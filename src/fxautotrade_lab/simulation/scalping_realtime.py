"""Realtime paper engine for GMO/JForex-compatible scalping ticks."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.ticks import resample_ticks_to_quote_bars, validate_tick_frame
from fxautotrade_lab.features.scalping import (
    build_directional_feature_frame,
    build_scalping_feature_frame,
)
from fxautotrade_lab.ml.scalping import ScalpingModelBundle, ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import ScalpingExecutionConfig


@dataclass(slots=True)
class ScalpingPaperPosition:
    position_id: str
    symbol: str
    side: str
    quantity: int
    entry_time: pd.Timestamp
    entry_price: float
    take_profit_price: float
    stop_loss_price: float
    probability: float


@dataclass(slots=True)
class ScalpingRealtimePaperEngine:
    symbol: str
    pip_size: float
    model_bundle: ScalpingModelBundle
    training_config: ScalpingTrainingConfig
    execution_config: ScalpingExecutionConfig
    bar_rule: str = "1s"
    min_buffer_ticks: int = 120
    max_buffer_ticks: int = 20_000
    cash: float = field(init=False)
    position: ScalpingPaperPosition | None = field(default=None, init=False)
    tick_buffer: list[dict[str, object]] = field(default_factory=list, init=False)
    events: list[dict[str, object]] = field(default_factory=list, init=False)
    trades: list[dict[str, object]] = field(default_factory=list, init=False)
    next_allowed_time: pd.Timestamp | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.cash = float(self.execution_config.starting_cash)

    def on_tick(
        self, *, timestamp: pd.Timestamp, bid: float, ask: float
    ) -> list[dict[str, object]]:
        ts = _tokyo(timestamp)
        tick = {
            "timestamp": ts,
            "symbol": self.symbol,
            "bid": float(bid),
            "ask": float(ask),
            "bid_volume": 0.0,
            "ask_volume": 0.0,
        }
        tick["mid"] = (tick["bid"] + tick["ask"]) / 2.0
        tick["spread"] = tick["ask"] - tick["bid"]
        if float(tick["bid"]) <= 0 or float(tick["ask"]) <= float(tick["bid"]):
            return []
        self.tick_buffer.append(tick)
        self.tick_buffer = self.tick_buffer[-self.max_buffer_ticks :]
        emitted: list[dict[str, object]] = []
        if self.position is not None:
            close_event = self._maybe_close_position(ts=ts, bid=float(bid), ask=float(ask))
            if close_event is not None:
                emitted.append(close_event)
        if self.position is None and len(self.tick_buffer) >= self.min_buffer_ticks:
            open_event = self._maybe_open_position(ts=ts, bid=float(bid), ask=float(ask))
            if open_event is not None:
                emitted.append(open_event)
        self.events.extend(emitted)
        self.events = self.events[-500:]
        return emitted

    def snapshot(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "cash": self.cash,
            "open_position": self.position.__dict__ if self.position is not None else None,
            "events": list(self.events[-100:]),
            "trades": list(self.trades[-100:]),
        }

    def _maybe_open_position(
        self, *, ts: pd.Timestamp, bid: float, ask: float
    ) -> dict[str, object] | None:
        if self.next_allowed_time is not None and ts < self.next_allowed_time:
            return None
        ticks = pd.DataFrame(self.tick_buffer).set_index("timestamp")
        tick_frame = validate_tick_frame(ticks, symbol=self.symbol)
        bars = resample_ticks_to_quote_bars(tick_frame, rule=self.bar_rule, symbol=self.symbol)
        if len(bars.index) < 40:
            return None
        features = build_scalping_feature_frame(bars, symbol=self.symbol, pip_size=self.pip_size)
        latest_ts = features.index[-1]
        latest = features.loc[[latest_ts]]
        spread = float(latest["spread_close_pips"].iloc[0])
        volatility = float(latest["micro_volatility_10_pips"].iloc[0])
        if (
            spread > self.training_config.max_spread_pips
            or volatility < self.training_config.min_volatility_pips
        ):
            return None
        long_probability = float(
            self.model_bundle.model.predict_proba(
                build_directional_feature_frame(latest, side="long")
            ).iloc[0]
        )
        short_probability = float(
            self.model_bundle.model.predict_proba(
                build_directional_feature_frame(latest, side="short")
            ).iloc[0]
        )
        side = "long" if long_probability >= short_probability else "short"
        probability = max(long_probability, short_probability)
        if probability < self.model_bundle.decision_threshold:
            return None
        price = ask if side == "long" else bid
        quantity = self._quantity(price)
        if quantity <= 0:
            return None
        slip = (self.training_config.round_trip_slippage_pips / 2.0) * self.pip_size
        entry_price = ask + slip if side == "long" else bid - slip
        take_profit = (
            entry_price + self.training_config.take_profit_pips * self.pip_size
            if side == "long"
            else entry_price - self.training_config.take_profit_pips * self.pip_size
        )
        stop_loss = (
            entry_price - self.training_config.stop_loss_pips * self.pip_size
            if side == "long"
            else entry_price + self.training_config.stop_loss_pips * self.pip_size
        )
        self.position = ScalpingPaperPosition(
            position_id=str(uuid4()),
            symbol=self.symbol,
            side=side,
            quantity=quantity,
            entry_time=ts,
            entry_price=entry_price,
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
            probability=probability,
        )
        return {
            "event": "paper_entry",
            "timestamp": ts.isoformat(),
            "symbol": self.symbol,
            "side": side,
            "quantity": quantity,
            "entry_price": entry_price,
            "probability": probability,
            "long_probability": long_probability,
            "short_probability": short_probability,
            "message_ja": "GMO tick互換のスキャルピング paper entry を記録しました。",
        }

    def _maybe_close_position(
        self, *, ts: pd.Timestamp, bid: float, ask: float
    ) -> dict[str, object] | None:
        position = self.position
        if position is None:
            return None
        max_exit_time = position.entry_time + pd.Timedelta(
            seconds=self.training_config.max_hold_seconds
        )
        slip = (self.training_config.round_trip_slippage_pips / 2.0) * self.pip_size
        exit_price = 0.0
        reason = ""
        if position.side == "long":
            if bid <= position.stop_loss_price:
                exit_price = position.stop_loss_price - slip
                reason = "stop_loss"
            elif bid >= position.take_profit_price:
                exit_price = position.take_profit_price - slip
                reason = "take_profit"
            elif ts >= max_exit_time:
                exit_price = bid - slip
                reason = "time_exit"
            pnl = (exit_price - position.entry_price) * position.quantity if exit_price > 0 else 0.0
            pips = (exit_price - position.entry_price) / self.pip_size if exit_price > 0 else 0.0
        else:
            if ask >= position.stop_loss_price:
                exit_price = position.stop_loss_price + slip
                reason = "stop_loss"
            elif ask <= position.take_profit_price:
                exit_price = position.take_profit_price + slip
                reason = "take_profit"
            elif ts >= max_exit_time:
                exit_price = ask + slip
                reason = "time_exit"
            pnl = (position.entry_price - exit_price) * position.quantity if exit_price > 0 else 0.0
            pips = (position.entry_price - exit_price) / self.pip_size if exit_price > 0 else 0.0
        if not reason:
            return None
        self.cash += pnl
        trade = {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "side": position.side,
            "quantity": position.quantity,
            "entry_time": position.entry_time.isoformat(),
            "exit_time": ts.isoformat(),
            "entry_price": position.entry_price,
            "exit_price": exit_price,
            "net_pnl": pnl,
            "realized_pips": pips,
            "exit_reason": reason,
            "probability": position.probability,
        }
        self.trades.append(trade)
        self.trades = self.trades[-500:]
        self.position = None
        self.next_allowed_time = ts + pd.Timedelta(seconds=self.execution_config.cooldown_seconds)
        return {
            "event": "paper_exit",
            "timestamp": ts.isoformat(),
            "symbol": self.symbol,
            "side": trade["side"],
            "quantity": trade["quantity"],
            "exit_price": exit_price,
            "net_pnl": pnl,
            "realized_pips": pips,
            "exit_reason": reason,
            "cash": self.cash,
            "message_ja": "スキャルピング paper position を決済しました。",
        }

    def _quantity(self, price: float) -> int:
        max_notional = self.cash * self.execution_config.max_position_notional_fraction
        target_notional = min(self.execution_config.fixed_order_amount, max_notional)
        if price <= 0 or target_notional <= 0:
            return 0
        raw_quantity = int(target_notional // price)
        step = max(1, int(self.execution_config.quantity_step))
        quantity = (raw_quantity // step) * step
        return quantity if quantity >= self.execution_config.minimum_order_quantity else 0


def _tokyo(value: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(ASIA_TOKYO)
    return ts.tz_convert(ASIA_TOKYO)
