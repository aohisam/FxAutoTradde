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
from fxautotrade_lab.simulation.scalping_policy import (
    ScalpingExecutionConfig,
    ScalpingExecutionPolicy,
    ScalpingRiskSnapshot,
    ScalpingRiskState,
    ScalpingSignalContext,
    ScalpingSignalPolicy,
)


@dataclass(slots=True)
class ScalpingPaperPosition:
    position_id: str
    signal_id: str
    symbol: str
    side: str
    quantity: int
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    entry_price: float
    take_profit_price: float
    stop_loss_price: float
    probability: float
    long_probability: float
    short_probability: float


@dataclass(slots=True)
class ScalpingPendingEntry:
    signal_id: str
    symbol: str
    side: str
    signal_time: pd.Timestamp
    target_time: pd.Timestamp
    probability: float
    long_probability: float
    short_probability: float
    threshold: float
    spread: float
    spread_mean: float
    spread_z: float
    volatility: float
    risk_snapshot: ScalpingRiskSnapshot


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
    pending_entry: ScalpingPendingEntry | None = field(default=None, init=False)
    tick_buffer: list[dict[str, object]] = field(default_factory=list, init=False)
    events: list[dict[str, object]] = field(default_factory=list, init=False)
    trades: list[dict[str, object]] = field(default_factory=list, init=False)
    signals: list[dict[str, object]] = field(default_factory=list, init=False)
    all_events: list[dict[str, object]] = field(default_factory=list, init=False)
    all_trades: list[dict[str, object]] = field(default_factory=list, init=False)
    all_signals: list[dict[str, object]] = field(default_factory=list, init=False)
    _drained_event_index: int = field(default=0, init=False)
    _drained_signal_index: int = field(default=0, init=False)
    _drained_trade_index: int = field(default=0, init=False)
    risk_state: ScalpingRiskState = field(init=False)
    signal_policy: ScalpingSignalPolicy = field(init=False)
    execution_policy: ScalpingExecutionPolicy = field(init=False)
    last_signal_time: pd.Timestamp | None = field(default=None, init=False)
    rejected_rows_recorded: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.risk_state = ScalpingRiskState(self.execution_config)
        self.signal_policy = ScalpingSignalPolicy(
            training_config=self.training_config,
            execution_config=self.execution_config,
        )
        self.execution_policy = ScalpingExecutionPolicy(self.execution_config)
        self.cash = float(self.risk_state.cash)

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
        if self.position is None and self.pending_entry is not None:
            entry_event = self._activate_pending_entry(ts=ts, bid=float(bid), ask=float(ask))
            if entry_event is not None:
                emitted.append(entry_event)
        if (
            self.position is None
            and self.pending_entry is None
            and len(self.tick_buffer) >= self.min_buffer_ticks
        ):
            signal_event = self._maybe_create_pending_entry()
            if signal_event is not None:
                emitted.append(signal_event)
            if self.position is None and self.pending_entry is not None:
                entry_event = self._activate_pending_entry(ts=ts, bid=float(bid), ask=float(ask))
                if entry_event is not None:
                    emitted.append(entry_event)
        self.events.extend(emitted)
        self.events = self.events[-500:]
        self.all_events.extend(emitted)
        return emitted

    def snapshot(self, *, include_history: bool = False) -> dict[str, object]:
        if include_history:
            events = list(self.all_events)
            signals = list(self.all_signals)
            trades = list(self.all_trades)
        else:
            events = list(self.events[-100:])
            signals = list(self.signals[-500:])
            trades = list(self.trades[-100:])
        return {
            "symbol": self.symbol,
            "cash": self.cash,
            "open_position": self.position.__dict__ if self.position is not None else None,
            "pending_entry": (
                self.pending_entry.__dict__ if self.pending_entry is not None else None
            ),
            "events": events,
            "signals": signals,
            "trades": trades,
        }

    def full_history(self) -> dict[str, object]:
        return self.snapshot(include_history=True)

    def drain_new_records(self) -> dict[str, list[dict[str, object]]]:
        """Return records created since the previous drain and advance cursors."""

        events = list(self.all_events[self._drained_event_index :])
        signals = list(self.all_signals[self._drained_signal_index :])
        trades = list(self.all_trades[self._drained_trade_index :])
        self._drained_event_index = len(self.all_events)
        self._drained_signal_index = len(self.all_signals)
        self._drained_trade_index = len(self.all_trades)
        return {"events": events, "signals": signals, "trades": trades}

    def _maybe_create_pending_entry(self) -> dict[str, object] | None:
        ticks = pd.DataFrame(self.tick_buffer).set_index("timestamp")
        tick_frame = validate_tick_frame(ticks, symbol=self.symbol)
        bars = resample_ticks_to_quote_bars(tick_frame, rule=self.bar_rule, symbol=self.symbol)
        if len(bars.index) < 40:
            return None
        features = build_scalping_feature_frame(bars, symbol=self.symbol, pip_size=self.pip_size)
        latest_ts = pd.Timestamp(features.index[-1])
        latest_ts = _tokyo(latest_ts)
        if self.last_signal_time is not None and latest_ts <= self.last_signal_time:
            return None
        latest = features.loc[[features.index[-1]]]
        spread = _feature_float(latest, "spread_close_pips")
        spread_mean = _feature_float(latest, "spread_mean_20_pips")
        spread_z = _feature_float(latest, "spread_z_120")
        volatility = _feature_float(latest, "micro_volatility_10_pips")
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
        threshold = float(self.model_bundle.decision_threshold)
        signal_id = str(uuid4())
        risk_snapshot = self.risk_state.snapshot(latest_ts)
        decision = self.signal_policy.decide_entry(
            ScalpingSignalContext(
                timestamp=latest_ts,
                tick_index=tick_frame.index,
                spread=spread,
                spread_mean=spread_mean,
                spread_z=spread_z,
                volatility=volatility,
                probability=probability,
                threshold=threshold,
                chosen_side=side,
                risk_reject_reason=self.risk_state.entry_reject_reason(latest_ts),
            )
        )
        self.last_signal_time = latest_ts
        if not decision.accepted:
            return self._record_signal(
                signal_id=signal_id,
                timestamp=latest_ts,
                side=side,
                accepted=False,
                reject_reason=decision.reject_reason,
                probability=probability,
                long_probability=long_probability,
                short_probability=short_probability,
                threshold=threshold,
                spread=spread,
                spread_mean=spread_mean,
                spread_z=spread_z,
                volatility=volatility,
                risk_snapshot_before=risk_snapshot,
                risk_snapshot_after=risk_snapshot,
            )
        self.pending_entry = ScalpingPendingEntry(
            signal_id=signal_id,
            symbol=self.symbol,
            side=side,
            signal_time=latest_ts,
            target_time=latest_ts
            + pd.Timedelta(milliseconds=max(0, int(self.execution_config.entry_latency_ms))),
            probability=probability,
            long_probability=long_probability,
            short_probability=short_probability,
            threshold=threshold,
            spread=spread,
            spread_mean=spread_mean,
            spread_z=spread_z,
            volatility=volatility,
            risk_snapshot=risk_snapshot,
        )
        return None

    def _activate_pending_entry(
        self, *, ts: pd.Timestamp, bid: float, ask: float
    ) -> dict[str, object] | None:
        pending = self.pending_entry
        if pending is None or ts < pending.target_time:
            return None
        price = ask if pending.side == "long" else bid
        quantity = self.execution_policy.quantity_for_price(price, cash=self.cash)
        tick_index = pd.DatetimeIndex([pd.Timestamp(row["timestamp"]) for row in self.tick_buffer])
        decision = self.signal_policy.decide_entry(
            ScalpingSignalContext(
                timestamp=pending.signal_time,
                tick_index=tick_index,
                spread=pending.spread,
                spread_mean=pending.spread_mean,
                spread_z=pending.spread_z,
                volatility=pending.volatility,
                probability=pending.probability,
                threshold=pending.threshold,
                chosen_side=pending.side,
                quantity=quantity,
            )
        )
        if not decision.accepted:
            self.pending_entry = None
            return self._record_signal(
                signal_id=pending.signal_id,
                timestamp=pending.signal_time,
                side=pending.side,
                accepted=False,
                reject_reason=decision.reject_reason,
                probability=pending.probability,
                long_probability=pending.long_probability,
                short_probability=pending.short_probability,
                threshold=pending.threshold,
                spread=pending.spread,
                spread_mean=pending.spread_mean,
                spread_z=pending.spread_z,
                volatility=pending.volatility,
                risk_snapshot_before=pending.risk_snapshot,
                risk_snapshot_after=pending.risk_snapshot,
            )
        slip = (self.training_config.round_trip_slippage_pips / 2.0) * self.pip_size
        entry_price = ask + slip if pending.side == "long" else bid - slip
        take_profit = (
            entry_price + self.training_config.take_profit_pips * self.pip_size
            if pending.side == "long"
            else entry_price - self.training_config.take_profit_pips * self.pip_size
        )
        stop_loss = (
            entry_price - self.training_config.stop_loss_pips * self.pip_size
            if pending.side == "long"
            else entry_price + self.training_config.stop_loss_pips * self.pip_size
        )
        self._record_signal(
            signal_id=pending.signal_id,
            timestamp=pending.signal_time,
            side=pending.side,
            accepted=True,
            reject_reason="accepted",
            probability=pending.probability,
            long_probability=pending.long_probability,
            short_probability=pending.short_probability,
            threshold=pending.threshold,
            spread=pending.spread,
            spread_mean=pending.spread_mean,
            spread_z=pending.spread_z,
            volatility=pending.volatility,
            risk_snapshot_before=pending.risk_snapshot,
            risk_snapshot_after=self.risk_state.snapshot(pending.signal_time),
        )
        self.position = ScalpingPaperPosition(
            position_id=str(uuid4()),
            signal_id=pending.signal_id,
            symbol=self.symbol,
            side=pending.side,
            quantity=quantity,
            signal_time=pending.signal_time,
            entry_time=ts,
            entry_price=entry_price,
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
            probability=pending.probability,
            long_probability=pending.long_probability,
            short_probability=pending.short_probability,
        )
        self.pending_entry = None
        return {
            "event": "paper_entry",
            "timestamp": ts.isoformat(),
            "signal_time": pending.signal_time.isoformat(),
            "symbol": self.symbol,
            "side": pending.side,
            "quantity": quantity,
            "entry_price": entry_price,
            "probability": pending.probability,
            "long_probability": pending.long_probability,
            "short_probability": pending.short_probability,
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
            gross_pnl = (
                (exit_price - position.entry_price) * position.quantity if exit_price > 0 else 0.0
            )
            gross_pips = (
                (exit_price - position.entry_price) / self.pip_size if exit_price > 0 else 0.0
            )
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
            gross_pnl = (
                (position.entry_price - exit_price) * position.quantity if exit_price > 0 else 0.0
            )
            gross_pips = (
                (position.entry_price - exit_price) / self.pip_size if exit_price > 0 else 0.0
            )
        if not reason:
            return None
        fee_pips = float(self.training_config.fee_pips)
        fee_amount = fee_pips * self.pip_size * position.quantity
        pnl = gross_pnl - fee_amount
        net_pips = gross_pips - fee_pips
        self.risk_state.record_trade(
            signal_time=position.signal_time,
            exit_time=ts,
            pnl=pnl,
        )
        self.cash = float(self.risk_state.cash)
        post_risk_snapshot = self.risk_state.snapshot(position.signal_time)
        self._update_signal_risk_after(position.signal_id, post_risk_snapshot)
        trade = {
            "trade_id": str(uuid4()),
            "position_id": position.position_id,
            "signal_id": position.signal_id,
            "symbol": position.symbol,
            "side": position.side,
            "quantity": position.quantity,
            "signal_time": position.signal_time.isoformat(),
            "entry_time": position.entry_time.isoformat(),
            "exit_time": ts.isoformat(),
            "entry_price": position.entry_price,
            "exit_price": exit_price,
            "gross_pnl": gross_pnl,
            "net_pnl": pnl,
            "fee_amount": fee_amount,
            "fee_pips": fee_pips,
            "realized_gross_pips": gross_pips,
            "realized_net_pips": net_pips,
            "realized_pips": net_pips,
            "exit_reason": reason,
            "probability": position.probability,
            "long_probability": position.long_probability,
            "short_probability": position.short_probability,
        }
        self.trades.append(trade)
        self.trades = self.trades[-500:]
        self.all_trades.append(trade)
        self.position = None
        return {
            "event": "paper_exit",
            "timestamp": ts.isoformat(),
            "symbol": self.symbol,
            "side": trade["side"],
            "quantity": trade["quantity"],
            "exit_price": exit_price,
            "net_pnl": pnl,
            "realized_pips": net_pips,
            "exit_reason": reason,
            "cash": self.cash,
            "message_ja": "スキャルピング paper position を決済しました。",
        }

    def _record_signal(
        self,
        *,
        signal_id: str,
        timestamp: pd.Timestamp,
        side: str,
        accepted: bool,
        reject_reason: str,
        probability: float,
        long_probability: float,
        short_probability: float,
        threshold: float,
        spread: float,
        spread_mean: float,
        spread_z: float,
        volatility: float,
        risk_snapshot_before: ScalpingRiskSnapshot,
        risk_snapshot_after: ScalpingRiskSnapshot,
    ) -> dict[str, object] | None:
        if not accepted:
            if not self.execution_config.record_rejected_signals:
                return None
            if (
                self.execution_config.max_rejected_signals is not None
                and self.rejected_rows_recorded >= int(self.execution_config.max_rejected_signals)
            ):
                return None
            self.rejected_rows_recorded += 1
        row = {
            "signal_id": signal_id,
            "timestamp": timestamp.isoformat(),
            "symbol": self.symbol,
            "chosen_side": side,
            "side": side if accepted else "",
            "probability": float(probability),
            "long_probability": float(long_probability),
            "short_probability": float(short_probability),
            "threshold": float(threshold),
            "spread_pips": float(spread),
            "spread_mean_20_pips": float(spread_mean),
            "spread_z_120": float(spread_z),
            "volatility_pips": float(volatility),
            "accepted": bool(accepted),
            "decision": "enter" if accepted else "reject",
            "reject_reason": reject_reason if reject_reason else ("accepted" if accepted else ""),
            "explanation_ja": _paper_signal_explanation(reject_reason, accepted=accepted),
            "trades_today": int(risk_snapshot_before.trades_today),
            "daily_pnl": float(risk_snapshot_before.daily_pnl),
            "consecutive_losses": int(risk_snapshot_before.consecutive_losses),
            "trades_today_before": int(risk_snapshot_before.trades_today),
            "daily_pnl_before": float(risk_snapshot_before.daily_pnl),
            "consecutive_losses_before": int(risk_snapshot_before.consecutive_losses),
            "trades_today_after": int(risk_snapshot_after.trades_today),
            "daily_pnl_after": float(risk_snapshot_after.daily_pnl),
            "consecutive_losses_after": int(risk_snapshot_after.consecutive_losses),
        }
        self.signals.append(row)
        self.signals = self.signals[-1_000:]
        self.all_signals.append(row)
        if accepted:
            return None
        return {
            "event": "paper_signal_rejected",
            "timestamp": timestamp.isoformat(),
            "symbol": self.symbol,
            "side": side,
            "reject_reason": reject_reason,
            "probability": probability,
            "message_ja": row["explanation_ja"],
        }

    def _update_signal_risk_after(
        self,
        signal_id: str,
        risk_snapshot_after: ScalpingRiskSnapshot,
    ) -> None:
        for row in self.all_signals:
            if row.get("signal_id") == signal_id:
                row["trades_today_after"] = int(risk_snapshot_after.trades_today)
                row["daily_pnl_after"] = float(risk_snapshot_after.daily_pnl)
                row["consecutive_losses_after"] = int(risk_snapshot_after.consecutive_losses)
                break


def _tokyo(value: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(ASIA_TOKYO)
    return ts.tz_convert(ASIA_TOKYO)


def _feature_float(features: pd.DataFrame, column: str) -> float:
    if column not in features.columns:
        return 0.0
    value = pd.to_numeric(features[column], errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else 0.0


def _paper_signal_explanation(reason: str, *, accepted: bool) -> str:
    if accepted:
        return "paper engine が backtest と同じentry条件を満たしたため採用"
    mapping = {
        "cooldown": "直前決済後のクールダウン中のためpaper entryを見送り",
        "max_trades_per_day": "1日の最大取引回数に達したためpaper entryを見送り",
        "daily_loss_halt": "当日損失停止に達したためpaper entryを停止",
        "consecutive_loss_halt": "当日連敗停止に達したためpaper entryを停止",
        "stale_tick": "直近tickが古く約定前提が不安定なためpaper entryを見送り",
        "spread_exceeded": "スプレッドが許容上限を超えたためpaper entryを見送り",
        "spread_z_exceeded": "スプレッドz-scoreが異常域のためpaper entryを見送り",
        "spread_to_mean_exceeded": "スプレッドが短期平均比で大きすぎるためpaper entryを見送り",
        "volatility_too_low": "短期ボラティリティが不足しているためpaper entryを見送り",
        "threshold_not_met": "予測確率がdecision threshold未満のためpaper entryを見送り",
        "quantity_too_small": "注文数量が最小数量を下回るためpaper entryを見送り",
    }
    if reason.startswith("blackout_window:"):
        window_reason = reason.split(":", 1)[1]
        return f"ブラックアウト時間帯({window_reason})のためpaper entryを見送り"
    return mapping.get(reason, f"paper entry条件を満たさないため見送り: {reason}")
