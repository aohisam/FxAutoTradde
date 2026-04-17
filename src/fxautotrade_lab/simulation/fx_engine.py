"""Quote-aware FX backtest engine with conservative intrabar ordering."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide, OrderStatus
from fxautotrade_lab.core.symbols import split_fx_symbol
from fxautotrade_lab.execution.risk import RiskManager
from fxautotrade_lab.simulation.fills import apply_fill_model


@dataclass(slots=True)
class PendingEntry:
    symbol: str
    execute_at: pd.Timestamp
    quantity: int
    position_side: str
    entry_order_side: OrderSide
    exit_order_side: OrderSide
    trigger_price: float
    initial_stop_price: float
    initial_risk_price: float
    atr_at_entry: float
    breakout_level: float
    reason: str
    score: float
    signal_time: pd.Timestamp


@dataclass(slots=True)
class PendingExit:
    symbol: str
    execute_at: pd.Timestamp
    quantity: int
    order_side: OrderSide
    reason: str
    kind: str


@dataclass(slots=True)
class FxOpenPosition:
    symbol: str
    position_id: str
    position_side: str
    entry_order_side: OrderSide
    exit_order_side: OrderSide
    quantity: int
    initial_quantity: int
    entry_time: pd.Timestamp
    signal_time: pd.Timestamp
    entry_price: float
    highest_bid: float
    lowest_ask: float
    initial_stop_price: float
    trailing_stop_price: float
    initial_risk_price: float
    atr_at_entry: float
    breakout_level: float
    entry_reason: str
    entry_score: float
    partial_exit_done: bool = False
    lifecycle_state: str = "LONG_OPEN"


class FxQuotePortfolioSimulator:
    """Bid/Ask aware simulator for the FX rule-only strategy."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.risk_manager = RiskManager(config.risk)
        self.fx_cfg = config.strategy.fx_breakout_pullback
        self.intrabar_priority = self._intrabar_priority()

    def _intrabar_priority(self) -> tuple[str, ...]:
        policy = self.fx_cfg.intrabar_policy.strip().lower()
        if policy == "conservative_adverse":
            return (
                "protective_gap_exit",
                "protective_stop",
                "trailing_stop",
                "partial_exit",
                "new_entry",
                "favorable_exit",
            )
        raise ValueError(f"Unsupported FX intrabar policy: {self.fx_cfg.intrabar_policy}")

    @staticmethod
    def _as_bool(value: object, default: bool = False) -> bool:
        if value is None or value is pd.NA:
            return default
        if isinstance(value, float) and pd.isna(value):
            return default
        return bool(value)

    @staticmethod
    def _as_float(value: object, default: float = 0.0) -> float:
        try:
            if value is None or value is pd.NA:
                return default
            result = float(value)
        except (TypeError, ValueError):
            return default
        return default if pd.isna(result) else result

    @staticmethod
    def _normalize_position_side(value: object) -> str:
        return "short" if str(value or "").strip().lower() == "short" else "long"

    @staticmethod
    def _order_side_from_value(value: object, default: OrderSide) -> OrderSide:
        candidate = str(value or "").strip().lower()
        if candidate == OrderSide.SELL.value:
            return OrderSide.SELL
        if candidate == OrderSide.BUY.value:
            return OrderSide.BUY
        return default

    @staticmethod
    def _is_long(position_side: str) -> bool:
        return position_side == "long"

    @staticmethod
    def _quote_side_for_order(order_side: OrderSide) -> str:
        return "ask" if order_side == OrderSide.BUY else "bid"

    def _mark_to_market_price(self, row: pd.Series, position: FxOpenPosition) -> float:
        quote_side = "bid" if self._is_long(position.position_side) else "ask"
        return self._quote_price(row, quote_side, "close", position.entry_price)

    @staticmethod
    def _position_market_value(position: FxOpenPosition, current_price: float) -> float:
        multiplier = 1.0 if position.position_side == "long" else -1.0
        return multiplier * current_price * position.quantity

    def _quote_price(self, row: pd.Series, side: str, field: str, default: float = 0.0) -> float:
        raw_value = row.get(f"{side}_{field}")
        if raw_value is not None and raw_value is not pd.NA:
            mid = self._as_float(row.get(f"mid_{field}", row.get(field)), default)
            spread = self._as_float(row.get(f"spread_{field}", row.get("spread_close")), 0.0)
            multiplier = max(self.fx_cfg.spread_stress_multiplier, 0.0)
            if abs(multiplier - 1.0) < 1e-9:
                return self._as_float(raw_value, default)
            stressed_spread = spread * multiplier
            return mid + stressed_spread / 2.0 if side == "ask" else mid - stressed_spread / 2.0
        mid = self._as_float(row.get(f"mid_{field}", row.get(field)), default)
        spread = self._as_float(row.get(f"spread_{field}", row.get("spread_close")), 0.0) * max(
            self.fx_cfg.spread_stress_multiplier,
            0.0,
        )
        return mid + spread / 2.0 if side == "ask" else mid - spread / 2.0

    def _delayed_execute_at(self, frame: pd.DataFrame, timestamp: pd.Timestamp, delay_bars: int) -> pd.Timestamp | None:
        try:
            location = int(frame.index.get_loc(timestamp))
        except KeyError:
            return None
        target_index = location + delay_bars + 1
        if target_index >= len(frame.index):
            return None
        return pd.Timestamp(frame.index[target_index])

    def run(
        self,
        signal_frames: dict[str, pd.DataFrame],
        mode: BrokerMode = BrokerMode.LOCAL_SIM,
    ) -> dict[str, pd.DataFrame]:
        cash = float(self.config.risk.starting_cash)
        positions: dict[str, FxOpenPosition] = {}
        pending_entries: dict[str, PendingEntry] = {}
        pending_exits: dict[str, PendingExit] = {}
        order_records: list[dict[str, object]] = []
        fill_records: list[dict[str, object]] = []
        trade_records: list[dict[str, object]] = []
        equity_rows: list[dict[str, object]] = []
        position_rows: list[dict[str, object]] = []

        prepared: dict[str, pd.DataFrame] = {}
        all_timestamps: set[pd.Timestamp] = set()
        for symbol, frame in signal_frames.items():
            working = frame.copy().sort_index()
            working["next_timestamp"] = pd.Series(working.index, index=working.index).shift(-1)
            prepared[symbol] = working
            all_timestamps.update(working.index.tolist())

        for timestamp in sorted(all_timestamps):
            prices: dict[str, float] = {}
            for symbol, frame in prepared.items():
                if timestamp in frame.index:
                    position = positions.get(symbol)
                    if position is None:
                        prices[symbol] = self._quote_price(frame.loc[timestamp], "bid", "close", 0.0)
                    else:
                        prices[symbol] = self._mark_to_market_price(frame.loc[timestamp], position)
            for symbol, frame in prepared.items():
                if timestamp not in frame.index:
                    continue
                row = frame.loc[timestamp]
                if symbol in positions:
                    cash_delta, trade_row, fill_row, order_row, closed = self._process_protective_exit(
                        timestamp,
                        row,
                        positions[symbol],
                        mode,
                    )
                    cash += cash_delta
                    if order_row is not None:
                        order_records.append(order_row)
                    if fill_row is not None:
                        fill_records.append(fill_row)
                    if trade_row is not None:
                        trade_records.append(trade_row)
                    if closed:
                        positions.pop(symbol, None)
                        pending_exits.pop(symbol, None)
                if symbol in positions and symbol in pending_exits and pending_exits[symbol].execute_at == timestamp:
                    cash_delta, trade_row, fill_row, order_row = self._execute_scheduled_exit(
                        timestamp,
                        row,
                        positions[symbol],
                        pending_exits.pop(symbol),
                        mode,
                    )
                    cash += cash_delta
                    order_records.append(order_row)
                    fill_records.append(fill_row)
                    if trade_row is not None:
                        trade_records.append(trade_row)
                    if positions[symbol].quantity <= 0:
                        positions.pop(symbol, None)
                if symbol not in positions and symbol in pending_entries and pending_entries[symbol].execute_at == timestamp:
                    cash_delta, position, order_row, fill_row, trade_row = self._execute_pending_entry(
                        timestamp,
                        row,
                        pending_entries.pop(symbol),
                        cash,
                        mode,
                    )
                    cash += cash_delta
                    if order_row is not None:
                        order_records.append(order_row)
                    if fill_row is not None:
                        fill_records.append(fill_row)
                    if trade_row is not None:
                        trade_records.append(trade_row)
                    if position is not None:
                        positions[symbol] = position
                next_timestamp = row.get("next_timestamp")
                if pd.notna(next_timestamp):
                    if symbol not in positions and symbol not in pending_entries and self._as_bool(row.get("entry_signal", False)):
                        position_side = self._normalize_position_side(row.get("position_side"))
                        entry_order_side = self._order_side_from_value(
                            row.get("entry_order_side"),
                            OrderSide.BUY if position_side == "long" else OrderSide.SELL,
                        )
                        exit_order_side = self._order_side_from_value(
                            row.get("exit_order_side"),
                            OrderSide.SELL if position_side == "long" else OrderSide.BUY,
                        )
                        quantity = self._entry_quantity(symbol, row, cash, positions, prepared, timestamp, entry_order_side)
                        if quantity > 0:
                            execute_at = self._delayed_execute_at(frame, pd.Timestamp(timestamp), self.fx_cfg.entry_delay_bars)
                            if execute_at is None:
                                continue
                            pending_entries[symbol] = PendingEntry(
                                symbol=symbol,
                                execute_at=execute_at,
                                quantity=quantity,
                                position_side=position_side,
                                entry_order_side=entry_order_side,
                                exit_order_side=exit_order_side,
                                trigger_price=self._as_float(
                                    row.get("entry_trigger_price"),
                                    self._quote_price(
                                        row,
                                        self._quote_side_for_order(entry_order_side),
                                        "high" if entry_order_side == OrderSide.BUY else "low",
                                        self._as_float(row.get("close"), 0.0),
                                    ),
                                ),
                                initial_stop_price=self._as_float(row.get("initial_stop_price"), 0.0),
                                initial_risk_price=self._as_float(row.get("initial_risk_price"), 0.01),
                                atr_at_entry=max(self._as_float(row.get("breakout_atr_15m"), self._as_float(row.get("atr_15m"), 0.01)), 0.01),
                                breakout_level=self._as_float(row.get("breakout_level_15m"), self._as_float(row.get("close"), 0.0)),
                                reason=str(row.get("explanation_ja", "")),
                                score=self._as_float(row.get("signal_score"), 0.0),
                                signal_time=pd.Timestamp(timestamp),
                            )
                    if symbol in positions and symbol not in pending_exits:
                        if self._as_bool(row.get("exit_signal", False)):
                            pending_exits[symbol] = PendingExit(
                                symbol=symbol,
                                execute_at=pd.Timestamp(next_timestamp),
                                quantity=positions[symbol].quantity,
                                order_side=positions[symbol].exit_order_side,
                                reason="1時間足EMAクロスで全決済",
                                kind="favorable_exit",
                            )
                        elif self._as_bool(row.get("partial_exit_signal", False)) and not positions[symbol].partial_exit_done:
                            partial_quantity = max(
                                1,
                                int(round(positions[symbol].initial_quantity * self.fx_cfg.partial_exit_fraction)),
                            )
                            partial_quantity = min(partial_quantity, max(positions[symbol].quantity - 1, 0))
                            if partial_quantity > 0:
                                pending_exits[symbol] = PendingExit(
                                    symbol=symbol,
                                    execute_at=pd.Timestamp(next_timestamp),
                                    quantity=partial_quantity,
                                    order_side=positions[symbol].exit_order_side,
                                    reason="1時間足トレンド崩れで一部手仕舞い",
                                    kind="partial_exit",
                                )

            equity = cash
            exposure = 0.0
            for symbol, position in positions.items():
                current_price = prices.get(symbol, position.entry_price)
                market_value = self._position_market_value(position, current_price)
                exposure += abs(market_value)
                equity += market_value
                position_rows.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "position_side": position.position_side,
                        "quantity": position.quantity,
                        "entry_price": position.entry_price,
                        "initial_stop_price": position.initial_stop_price,
                        "trailing_stop_price": position.trailing_stop_price,
                        "partial_exit_done": position.partial_exit_done,
                        "strategy_state": position.lifecycle_state,
                    }
                )
            equity_rows.append(
                {
                    "timestamp": timestamp,
                    "cash": cash,
                    "equity": equity,
                    "exposure": exposure,
                }
            )

        return {
            "equity_curve": pd.DataFrame(equity_rows).set_index("timestamp") if equity_rows else pd.DataFrame(),
            "orders": pd.DataFrame(order_records),
            "fills": pd.DataFrame(fill_records),
            "trades": pd.DataFrame(trade_records),
            "positions": pd.DataFrame(position_rows),
        }

    def _entry_quantity(
        self,
        symbol: str,
        row: pd.Series,
        cash: float,
        positions: dict[str, FxOpenPosition],
        frames: dict[str, pd.DataFrame],
        timestamp: pd.Timestamp,
        entry_order_side: OrderSide,
    ) -> int:
        prices = {
            current_symbol: (
                self._mark_to_market_price(frames[current_symbol].loc[timestamp], position)
                if timestamp in frames[current_symbol].index
                else position.entry_price
            )
            for current_symbol, position in positions.items()
        }
        equity = cash + sum(
            self._position_market_value(position, prices.get(symbol_name, position.entry_price))
            for symbol_name, position in positions.items()
        )
        sizing = self.risk_manager.size_position(
            cash=cash,
            equity=equity,
            price=self._quote_price(
                row,
                self._quote_side_for_order(entry_order_side),
                "close",
                self._as_float(row.get("close"), 0.0),
            ),
            atr_value=max(self._as_float(row.get("breakout_atr_15m"), self._as_float(row.get("atr_15m"), 0.01)), 0.01),
        )
        if not self.risk_manager.can_open_position(
            open_positions=len(positions),
            current_exposure_ratio=0.0
            if equity <= 0
            else (
                sum(abs(self._position_market_value(position, prices.get(name, position.entry_price))) for name, position in positions.items())
                / equity
            ),
            quantity=sizing.quantity,
        ):
            return 0
        if self._jpy_cross_limit_reached(symbol, positions):
            return 0
        return sizing.quantity

    def _jpy_cross_limit_reached(self, symbol: str, positions: dict[str, FxOpenPosition]) -> bool:
        _, quote = split_fx_symbol(symbol)
        if quote != "JPY":
            return False
        open_jpy_crosses = sum(1 for current_symbol in positions if split_fx_symbol(current_symbol)[1] == "JPY")
        return open_jpy_crosses >= self.fx_cfg.max_jpy_cross_positions

    def _process_protective_exit(
        self,
        timestamp: pd.Timestamp,
        row: pd.Series,
        position: FxOpenPosition,
        mode: BrokerMode,
    ) -> tuple[float, dict[str, object] | None, dict[str, object] | None, dict[str, object] | None, bool]:
        current_atr = max(float(row.get("atr_15m") or position.atr_at_entry), 0.01)
        if self._is_long(position.position_side):
            position.highest_bid = max(position.highest_bid, self._quote_price(row, "bid", "high", position.highest_bid))
            trailing_candidate = position.highest_bid - self.fx_cfg.atr_trailing_mult * current_atr
            position.trailing_stop_price = max(position.trailing_stop_price, trailing_candidate)
            active_stop = max(position.initial_stop_price, position.trailing_stop_price)
            protective_open = self._quote_price(row, "bid", "open", position.entry_price)
            protective_extreme = self._quote_price(row, "bid", "low", position.entry_price)
            if protective_open <= active_stop:
                exit_price = protective_open
                reason = "protective_gap_exit"
            elif protective_extreme <= position.initial_stop_price:
                exit_price = position.initial_stop_price
                reason = "protective_stop"
            elif protective_extreme <= active_stop:
                exit_price = active_stop
                reason = "trailing_stop"
            else:
                return 0.0, None, None, None, False
        else:
            position.lowest_ask = min(position.lowest_ask, self._quote_price(row, "ask", "low", position.lowest_ask))
            trailing_candidate = position.lowest_ask + self.fx_cfg.atr_trailing_mult * current_atr
            position.trailing_stop_price = min(position.trailing_stop_price, trailing_candidate)
            active_stop = min(position.initial_stop_price, position.trailing_stop_price)
            protective_open = self._quote_price(row, "ask", "open", position.entry_price)
            protective_extreme = self._quote_price(row, "ask", "high", position.entry_price)
            if protective_open >= active_stop:
                exit_price = protective_open
                reason = "protective_gap_exit"
            elif protective_extreme >= position.initial_stop_price:
                exit_price = position.initial_stop_price
                reason = "protective_stop"
            elif protective_extreme >= active_stop:
                exit_price = active_stop
                reason = "trailing_stop"
            else:
                return 0.0, None, None, None, False
        fill = apply_fill_model(exit_price, position.exit_order_side, position.quantity, self.config.risk)
        order_id = str(uuid4())
        trade_row = self._trade_row(
            position=position,
            timestamp=timestamp,
            quantity=position.quantity,
            exit_price=fill.price,
            fee=fill.fee,
            reason=reason,
            mode=mode,
        )
        order_row = {
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": position.symbol,
            "side": position.exit_order_side.value,
            "quantity": position.quantity,
            "status": OrderStatus.FILLED.value,
            "reason": reason,
            "mode": mode.value,
        }
        fill_row = {
            "fill_id": str(uuid4()),
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": position.symbol,
            "side": position.exit_order_side.value,
            "quantity": position.quantity,
            "price": fill.price,
            "fee": fill.fee,
            "slippage": fill.slippage,
        }
        cash_delta = (
            fill.price * position.quantity - fill.fee
            if position.exit_order_side == OrderSide.SELL
            else -(fill.price * position.quantity + fill.fee)
        )
        return cash_delta, trade_row, fill_row, order_row, True

    def _execute_scheduled_exit(
        self,
        timestamp: pd.Timestamp,
        row: pd.Series,
        position: FxOpenPosition,
        pending_exit: PendingExit,
        mode: BrokerMode,
    ) -> tuple[float, dict[str, object] | None, dict[str, object], dict[str, object]]:
        exit_price = self._quote_price(
            row,
            self._quote_side_for_order(pending_exit.order_side),
            "open",
            position.entry_price,
        )
        fill = apply_fill_model(exit_price, pending_exit.order_side, pending_exit.quantity, self.config.risk)
        order_id = str(uuid4())
        position.quantity -= pending_exit.quantity
        if pending_exit.kind == "partial_exit":
            position.partial_exit_done = True
            position.lifecycle_state = "PARTIAL_EXIT_DONE"
        trade_row = self._trade_row(
            position=position,
            timestamp=timestamp,
            quantity=pending_exit.quantity,
            exit_price=fill.price,
            fee=fill.fee,
            reason=pending_exit.reason,
            mode=mode,
        )
        order_row = {
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": position.symbol,
            "side": pending_exit.order_side.value,
            "quantity": pending_exit.quantity,
            "status": OrderStatus.FILLED.value,
            "reason": pending_exit.reason,
            "mode": mode.value,
        }
        fill_row = {
            "fill_id": str(uuid4()),
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": position.symbol,
            "side": pending_exit.order_side.value,
            "quantity": pending_exit.quantity,
            "price": fill.price,
            "fee": fill.fee,
            "slippage": fill.slippage,
        }
        cash_delta = (
            fill.price * pending_exit.quantity - fill.fee
            if pending_exit.order_side == OrderSide.SELL
            else -(fill.price * pending_exit.quantity + fill.fee)
        )
        return cash_delta, trade_row, fill_row, order_row

    def _execute_pending_entry(
        self,
        timestamp: pd.Timestamp,
        row: pd.Series,
        pending_entry: PendingEntry,
        cash: float,
        mode: BrokerMode,
    ) -> tuple[float, FxOpenPosition | None, dict[str, object] | None, dict[str, object] | None, dict[str, object] | None]:
        if not self._as_bool(row.get("entry_context_ok", False)):
            return 0.0, None, None, None, None
        if pending_entry.entry_order_side == OrderSide.BUY:
            quote_open = self._quote_price(row, "ask", "open", 0.0)
            quote_extreme = self._quote_price(row, "ask", "high", 0.0)
            if quote_open >= pending_entry.trigger_price:
                entry_raw_price = quote_open
            elif quote_extreme >= pending_entry.trigger_price:
                entry_raw_price = pending_entry.trigger_price
            else:
                return 0.0, None, None, None, None
        else:
            quote_open = self._quote_price(row, "bid", "open", 0.0)
            quote_extreme = self._quote_price(row, "bid", "low", 0.0)
            if quote_open <= pending_entry.trigger_price:
                entry_raw_price = quote_open
            elif quote_extreme <= pending_entry.trigger_price:
                entry_raw_price = pending_entry.trigger_price
            else:
                return 0.0, None, None, None, None
        fill = apply_fill_model(entry_raw_price, pending_entry.entry_order_side, pending_entry.quantity, self.config.risk)
        total_value = fill.price * pending_entry.quantity
        if pending_entry.entry_order_side == OrderSide.BUY and cash < (total_value + fill.fee):
            return 0.0, None, None, None, None
        order_id = str(uuid4())
        order_row = {
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": pending_entry.symbol,
            "side": pending_entry.entry_order_side.value,
            "quantity": pending_entry.quantity,
            "status": OrderStatus.FILLED.value,
            "reason": pending_entry.reason,
            "mode": mode.value,
        }
        fill_row = {
            "fill_id": str(uuid4()),
            "order_id": order_id,
            "timestamp": timestamp,
            "symbol": pending_entry.symbol,
            "side": pending_entry.entry_order_side.value,
            "quantity": pending_entry.quantity,
            "price": fill.price,
            "fee": fill.fee,
            "slippage": fill.slippage,
        }
        position = FxOpenPosition(
            symbol=pending_entry.symbol,
            position_id=str(uuid4()),
            position_side=pending_entry.position_side,
            entry_order_side=pending_entry.entry_order_side,
            exit_order_side=pending_entry.exit_order_side,
            quantity=pending_entry.quantity,
            initial_quantity=pending_entry.quantity,
            entry_time=timestamp,
            signal_time=pending_entry.signal_time,
            entry_price=fill.price,
            highest_bid=self._quote_price(row, "bid", "open", fill.price),
            lowest_ask=self._quote_price(row, "ask", "open", fill.price),
            initial_stop_price=pending_entry.initial_stop_price,
            trailing_stop_price=pending_entry.initial_stop_price,
            initial_risk_price=max(pending_entry.initial_risk_price, 0.01),
            atr_at_entry=pending_entry.atr_at_entry,
            breakout_level=pending_entry.breakout_level,
            entry_reason=pending_entry.reason,
            entry_score=pending_entry.score,
            lifecycle_state="LONG_OPEN" if pending_entry.position_side == "long" else "SHORT_OPEN",
        )
        # conservative_adverse:
        # when the entry trigger and protective stop are both reachable inside the
        # same 1-minute bar, the engine assumes we are filled first and then stopped.
        if pending_entry.position_side == "long":
            protective_open = self._quote_price(row, "bid", "open", fill.price)
            protective_extreme = self._quote_price(row, "bid", "low", fill.price)
            if protective_open <= pending_entry.initial_stop_price:
                exit_raw = protective_open
                stop_reason = "protective_gap_exit"
            elif protective_extreme <= pending_entry.initial_stop_price:
                exit_raw = pending_entry.initial_stop_price
                stop_reason = "protective_stop"
            else:
                entry_cash_delta = -(total_value + fill.fee)
                return entry_cash_delta, position, order_row, fill_row, None
        else:
            protective_open = self._quote_price(row, "ask", "open", fill.price)
            protective_extreme = self._quote_price(row, "ask", "high", fill.price)
            if protective_open >= pending_entry.initial_stop_price:
                exit_raw = protective_open
                stop_reason = "protective_gap_exit"
            elif protective_extreme >= pending_entry.initial_stop_price:
                exit_raw = pending_entry.initial_stop_price
                stop_reason = "protective_stop"
            else:
                entry_cash_delta = total_value - fill.fee
                return entry_cash_delta, position, order_row, fill_row, None
        exit_fill = apply_fill_model(exit_raw, pending_entry.exit_order_side, pending_entry.quantity, self.config.risk)
        stop_order_id = str(uuid4())
        stop_trade = self._trade_row(
            position=position,
            timestamp=timestamp,
            quantity=pending_entry.quantity,
            exit_price=exit_fill.price,
            fee=exit_fill.fee,
            reason=stop_reason,
            mode=mode,
        )
        return (
            (
                -(total_value + fill.fee)
                if pending_entry.entry_order_side == OrderSide.BUY
                else total_value - fill.fee
            )
            + (
                exit_fill.price * pending_entry.quantity - exit_fill.fee
                if pending_entry.exit_order_side == OrderSide.SELL
                else -(exit_fill.price * pending_entry.quantity + exit_fill.fee)
            ),
            None,
            order_row,
            fill_row,
            {
                **stop_trade,
                "protective_fill_id": stop_order_id,
            },
        )

    def _trade_row(
        self,
        *,
        position: FxOpenPosition,
        timestamp: pd.Timestamp,
        quantity: int,
        exit_price: float,
        fee: float,
        reason: str,
        mode: BrokerMode,
    ) -> dict[str, object]:
        gross_pnl = (
            (exit_price - position.entry_price) * quantity
            if position.position_side == "long"
            else (position.entry_price - exit_price) * quantity
        )
        hold_bars = max(int((timestamp - position.entry_time) / pd.Timedelta(minutes=1)), 0)
        overnight_days = max((timestamp.normalize() - position.entry_time.normalize()).days, 0)
        carry_cost = overnight_days * self.fx_cfg.overnight_swap_per_unit * quantity
        net_pnl = gross_pnl - fee - carry_cost
        realized_r_net = net_pnl / max(position.initial_risk_price * quantity, 0.01)
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "signal_time": position.signal_time,
            "entry_time": position.entry_time,
            "exit_time": timestamp,
            "position_side": position.position_side,
            "strategy_state": "FLAT_EXITED",
            "position_state_before_exit": position.lifecycle_state,
            "quantity": quantity,
            "initial_quantity": position.initial_quantity,
            "entry_price": position.entry_price,
            "exit_price": exit_price,
            "entry_order_side": position.entry_order_side.value,
            "exit_order_side": position.exit_order_side.value,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "hold_bars": hold_bars,
            "entry_reason": position.entry_reason,
            "exit_reason": reason,
            "entry_score": position.entry_score,
            "initial_risk_price": position.initial_risk_price,
            "carry_cost": carry_cost,
            "realized_r_net": realized_r_net,
            "mode": mode.value,
        }
