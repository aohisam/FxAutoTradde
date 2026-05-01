"""Multi-symbol event-driven backtest simulator."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide, OrderStatus
from fxautotrade_lab.core.models import Position
from fxautotrade_lab.execution.risk import RiskManager
from fxautotrade_lab.execution.safety import DuplicateOrderGuard
from fxautotrade_lab.simulation.fills import apply_fill_model
from fxautotrade_lab.simulation.portfolio import PortfolioState


@dataclass(slots=True)
class PendingOrder:
    symbol: str
    side: OrderSide
    quantity: int
    execute_at: pd.Timestamp
    reason: str
    score: float
    entry_time: pd.Timestamp


class PortfolioSimulator:
    """Primary backtest engine with shared cash and exposure controls."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.risk_manager = RiskManager(config.risk)

    def run(
        self,
        signal_frames: dict[str, pd.DataFrame],
        mode: BrokerMode = BrokerMode.LOCAL_SIM,
    ) -> dict[str, pd.DataFrame]:
        portfolio = PortfolioState(cash=self.config.risk.starting_cash)
        guard = DuplicateOrderGuard()
        pending_orders: list[PendingOrder] = []
        order_records: list[dict[str, object]] = []
        fill_records: list[dict[str, object]] = []
        trade_records: list[dict[str, object]] = []
        equity_rows: list[dict[str, object]] = []
        position_rows: list[dict[str, object]] = []

        prepared = {}
        all_timestamps: set[pd.Timestamp] = set()
        for symbol, frame in signal_frames.items():
            working = frame.copy()
            working["next_timestamp"] = pd.Series(working.index, index=working.index).shift(-1)
            prepared[symbol] = working
            all_timestamps.update(working.index.tolist())

        for timestamp in sorted(all_timestamps):
            current_prices: dict[str, float] = {}
            for symbol, frame in prepared.items():
                if timestamp in frame.index:
                    row = frame.loc[timestamp]
                    current_prices[symbol] = float(row["close"])
            self._execute_pending_orders(
                timestamp,
                prepared,
                pending_orders,
                guard,
                portfolio,
                order_records,
                fill_records,
                trade_records,
                mode,
            )
            self._manage_open_positions(
                timestamp,
                prepared,
                guard,
                portfolio,
                pending_orders,
                order_records,
                fill_records,
                trade_records,
                mode,
            )
            self._execute_pending_orders(
                timestamp,
                prepared,
                pending_orders,
                guard,
                portfolio,
                order_records,
                fill_records,
                trade_records,
                mode,
            )
            self._evaluate_signals(
                timestamp,
                prepared,
                portfolio,
                guard,
                pending_orders,
            )
            equity_rows.append(
                {
                    "timestamp": timestamp,
                    "cash": portfolio.cash,
                    "equity": portfolio.equity(current_prices),
                    "exposure": portfolio.exposure_value(current_prices),
                }
            )
            for symbol, position in portfolio.positions.items():
                position_rows.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "quantity": position.quantity,
                        "entry_price": position.entry_price,
                        "stop_price": position.stop_price,
                        "trailing_stop_price": position.trailing_stop_price,
                        "bars_held": position.bars_held,
                    }
                )
        equity_curve = (
            pd.DataFrame(equity_rows).set_index("timestamp") if equity_rows else pd.DataFrame()
        )
        orders = pd.DataFrame(order_records)
        fills = pd.DataFrame(fill_records)
        trades = pd.DataFrame(trade_records)
        positions = pd.DataFrame(position_rows)
        return {
            "equity_curve": equity_curve,
            "orders": orders,
            "fills": fills,
            "trades": trades,
            "positions": positions,
        }

    def _execute_pending_orders(
        self,
        timestamp: pd.Timestamp,
        prepared: dict[str, pd.DataFrame],
        pending_orders: list[PendingOrder],
        guard: DuplicateOrderGuard,
        portfolio: PortfolioState,
        order_records: list[dict[str, object]],
        fill_records: list[dict[str, object]],
        trade_records: list[dict[str, object]],
        mode: BrokerMode,
    ) -> None:
        remaining: list[PendingOrder] = []
        for pending in pending_orders:
            if pending.execute_at != timestamp:
                remaining.append(pending)
                continue
            frame = prepared[pending.symbol]
            if timestamp not in frame.index:
                continue
            row = frame.loc[timestamp]
            fill = apply_fill_model(
                float(row["open"]), pending.side, pending.quantity, self.config.risk
            )
            order_id = str(uuid4())
            order_records.append(
                {
                    "order_id": order_id,
                    "timestamp": timestamp,
                    "symbol": pending.symbol,
                    "side": pending.side.value,
                    "quantity": pending.quantity,
                    "status": OrderStatus.FILLED.value,
                    "reason": pending.reason,
                    "mode": mode.value,
                }
            )
            fill_records.append(
                {
                    "fill_id": str(uuid4()),
                    "order_id": order_id,
                    "timestamp": timestamp,
                    "symbol": pending.symbol,
                    "side": pending.side.value,
                    "quantity": pending.quantity,
                    "price": fill.price,
                    "fee": fill.fee,
                    "slippage": fill.slippage,
                }
            )
            if pending.side == OrderSide.BUY:
                cost = fill.price * pending.quantity + fill.fee
                if portfolio.cash >= cost:
                    atr_value = float(row.get("entry_atr_14", row.get("daily_atr_14", 1.0)) or 1.0)
                    portfolio.cash -= cost
                    portfolio.positions[pending.symbol] = Position(
                        symbol=pending.symbol,
                        quantity=pending.quantity,
                        entry_price=fill.price,
                        entry_time=timestamp,
                        highest_price=fill.price,
                        stop_price=fill.price - atr_value * self.config.risk.atr_stop_multiple,
                        trailing_stop_price=fill.price
                        - atr_value * self.config.risk.trailing_stop_multiple,
                        max_hold_bars=self.config.risk.max_hold_bars,
                        entry_reason=pending.reason,
                        entry_score=pending.score,
                        metadata={"partial_taken": False, "atr": atr_value},
                    )
            else:
                position = portfolio.positions.get(pending.symbol)
                if position is not None:
                    proceeds = fill.price * pending.quantity - fill.fee
                    portfolio.cash += proceeds
                    trade_records.append(
                        {
                            "symbol": pending.symbol,
                            "entry_time": position.entry_time,
                            "exit_time": timestamp,
                            "quantity": pending.quantity,
                            "entry_price": position.entry_price,
                            "exit_price": fill.price,
                            "gross_pnl": (fill.price - position.entry_price) * pending.quantity,
                            "net_pnl": (fill.price - position.entry_price) * pending.quantity
                            - fill.fee,
                            "hold_bars": position.bars_held,
                            "entry_reason": position.entry_reason,
                            "exit_reason": pending.reason,
                            "entry_score": position.entry_score,
                            "mode": mode.value,
                        }
                    )
                    remaining_qty = position.quantity - pending.quantity
                    if remaining_qty <= 0:
                        portfolio.positions.pop(pending.symbol, None)
                    else:
                        position.quantity = remaining_qty
                        position.metadata["partial_taken"] = True
            guard.remove(pending.symbol)
        pending_orders[:] = remaining

    def _manage_open_positions(
        self,
        timestamp: pd.Timestamp,
        prepared: dict[str, pd.DataFrame],
        guard: DuplicateOrderGuard,
        portfolio: PortfolioState,
        pending_orders: list[PendingOrder],
        order_records: list[dict[str, object]],
        fill_records: list[dict[str, object]],
        trade_records: list[dict[str, object]],
        mode: BrokerMode,
    ) -> None:
        _ = order_records, fill_records, trade_records, mode
        for symbol, position in list(portfolio.positions.items()):
            frame = prepared.get(symbol)
            if frame is None or timestamp not in frame.index:
                continue
            row = frame.loc[timestamp]
            position.bars_held += 1
            position.highest_price = max(position.highest_price, float(row["high"]))
            atr_value = float(row.get("entry_atr_14", position.metadata.get("atr", 1.0)) or 1.0)
            trailing = position.highest_price - atr_value * self.config.risk.trailing_stop_multiple
            position.trailing_stop_price = max(position.trailing_stop_price or trailing, trailing)
            stop_level = max(
                position.stop_price or trailing, position.trailing_stop_price or trailing
            )
            if float(row["low"]) <= stop_level and guard.add(symbol):
                pending_orders.append(
                    PendingOrder(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        quantity=position.quantity,
                        execute_at=timestamp,
                        reason="トレーリングストップで利益確定",
                        score=0.0,
                        entry_time=timestamp,
                    )
                )
                continue
            if (
                self.config.risk.allow_partial_profit
                and not position.metadata.get("partial_taken", False)
                and float(row["high"])
                >= position.entry_price + atr_value * self.config.risk.partial_take_profit_r
                and position.quantity >= 2
                and guard.add(symbol)
            ):
                pending_orders.append(
                    PendingOrder(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        quantity=max(1, position.quantity // 2),
                        execute_at=timestamp,
                        reason="段階利確を実行",
                        score=0.0,
                        entry_time=timestamp,
                    )
                )
                continue
            if (
                position.max_hold_bars
                and position.bars_held >= position.max_hold_bars
                and guard.add(symbol)
            ):
                pending_orders.append(
                    PendingOrder(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        quantity=position.quantity,
                        execute_at=timestamp,
                        reason="最大保有期間に到達",
                        score=0.0,
                        entry_time=timestamp,
                    )
                )

    def _evaluate_signals(
        self,
        timestamp: pd.Timestamp,
        prepared: dict[str, pd.DataFrame],
        portfolio: PortfolioState,
        guard: DuplicateOrderGuard,
        pending_orders: list[PendingOrder],
    ) -> None:
        for symbol, frame in prepared.items():
            if timestamp not in frame.index:
                continue
            row = frame.loc[timestamp]
            next_timestamp = row.get("next_timestamp")
            if pd.isna(next_timestamp):
                continue
            current_prices = {
                current_symbol: (
                    float(prepared[current_symbol].loc[timestamp, "close"])
                    if timestamp in prepared[current_symbol].index
                    else position.entry_price
                )
                for current_symbol, position in portfolio.positions.items()
            }
            equity = portfolio.equity(current_prices)
            exposure_ratio = (
                0.0 if equity <= 0 else portfolio.exposure_value(current_prices) / equity
            )
            if (
                symbol not in portfolio.positions
                and bool(row["entry_signal"])
                and guard.add(symbol)
            ):
                sizing = self.risk_manager.size_position(
                    cash=portfolio.cash,
                    equity=equity,
                    price=float(row["close"]),
                    atr_value=float(row.get("entry_atr_14", 1.0) or 1.0),
                )
                if self.risk_manager.can_open_position(
                    open_positions=len(portfolio.positions),
                    current_exposure_ratio=exposure_ratio,
                    quantity=sizing.quantity,
                ):
                    pending_orders.append(
                        PendingOrder(
                            symbol=symbol,
                            side=OrderSide.BUY,
                            quantity=sizing.quantity,
                            execute_at=pd.Timestamp(next_timestamp),
                            reason=str(row["explanation_ja"]),
                            score=float(row["signal_score"]),
                            entry_time=timestamp,
                        )
                    )
                else:
                    guard.remove(symbol)
            elif symbol in portfolio.positions and bool(row["exit_signal"]) and guard.add(symbol):
                pending_orders.append(
                    PendingOrder(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        quantity=portfolio.positions[symbol].quantity,
                        execute_at=pd.Timestamp(next_timestamp),
                        reason="スコア低下または逆行シグナルで手仕舞い",
                        score=float(row["signal_score"]),
                        entry_time=timestamp,
                    )
                )
