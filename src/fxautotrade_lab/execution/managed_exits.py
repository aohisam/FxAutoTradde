"""Shared forward-position exit management helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.config.models import RiskConfig


def _coerce_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value) if value not in {None, ""} else default
    except (TypeError, ValueError):
        return default


def recent_swing_low(frame: pd.DataFrame, lookback_bars: int) -> float | None:
    if frame is None or frame.empty or "low" not in frame.columns:
        return None
    lows = pd.to_numeric(frame["low"].tail(max(2, lookback_bars)), errors="coerce").dropna()
    if lows.empty:
        return None
    return float(lows.min())


def initial_stop_price(
    *,
    entry_price: float,
    atr_value: float,
    risk: RiskConfig,
    swing_low: float | None = None,
) -> float:
    atr_distance = max(atr_value * risk.atr_stop_multiple, entry_price * 0.01, 0.01)
    atr_stop = entry_price - atr_distance
    if swing_low is None or swing_low <= 0:
        return atr_stop
    swing_stop = float(swing_low) - atr_value * risk.swing_stop_buffer_atr
    return min(atr_stop, swing_stop)


@dataclass(slots=True)
class ManagedPositionState:
    symbol: str
    quantity: int
    initial_quantity: int
    entry_price: float
    entry_time: pd.Timestamp
    highest_price: float
    initial_stop_price: float
    stop_price: float
    trailing_stop_price: float
    partial_target_price: float
    next_trailing_price: float
    current_trailing_multiple: float
    atr_at_entry: float
    initial_risk_per_share: float
    bars_held: int = 0
    partial_taken: bool = False
    break_even_armed: bool = False
    last_bar_at: pd.Timestamp | None = None
    last_reference_high_price: float | None = None
    last_reference_low_price: float | None = None
    last_reference_close_price: float | None = None
    last_reference_timestamp: pd.Timestamp | None = None

    @property
    def active_stop_price(self) -> float:
        return max(self.stop_price, self.trailing_stop_price)


@dataclass(slots=True)
class ExitDecision:
    action: str
    quantity: int
    reason_ja: str


def build_managed_position(
    *,
    symbol: str,
    entry_price: float,
    quantity: int,
    entry_time: pd.Timestamp,
    atr_value: float,
    risk: RiskConfig,
    swing_low: float | None = None,
) -> ManagedPositionState:
    stop_price = initial_stop_price(
        entry_price=entry_price, atr_value=atr_value, risk=risk, swing_low=swing_low
    )
    trailing_stop = entry_price - atr_value * risk.trailing_stop_multiple
    initial_risk = max(float(entry_price) - float(stop_price), 0.01)
    partial_target = float(entry_price) + initial_risk * risk.partial_take_profit_r
    return ManagedPositionState(
        symbol=symbol.upper(),
        quantity=max(0, int(quantity)),
        initial_quantity=max(0, int(quantity)),
        entry_price=float(entry_price),
        entry_time=pd.Timestamp(entry_time),
        highest_price=float(entry_price),
        initial_stop_price=float(stop_price),
        stop_price=float(stop_price),
        trailing_stop_price=float(trailing_stop),
        partial_target_price=float(partial_target),
        next_trailing_price=float(trailing_stop),
        current_trailing_multiple=float(risk.trailing_stop_multiple),
        atr_at_entry=max(float(atr_value), 0.01),
        initial_risk_per_share=initial_risk,
        last_reference_high_price=float(entry_price),
        last_reference_low_price=float(entry_price),
        last_reference_close_price=float(entry_price),
        last_reference_timestamp=pd.Timestamp(entry_time),
    )


def _dynamic_trailing_multiple(latest: pd.Series, risk: RiskConfig) -> float:
    base = float(risk.trailing_stop_multiple)
    signal_score = _coerce_float(latest.get("signal_score"))
    daily_slope = _coerce_float(latest.get("daily_slope_20"))
    weekly_slope = _coerce_float(latest.get("weekly_slope_20"))
    monthly_slope = _coerce_float(latest.get("monthly_slope_20"))
    rsi_value = _coerce_float(latest.get("entry_rsi_14"), 50.0)
    gap_exhaustion = _coerce_float(latest.get("gap_exhaustion"))
    breakout = _coerce_float(latest.get("breakout_20"))
    strong = (
        signal_score >= 0.8
        and daily_slope > 0
        and weekly_slope > 0
        and monthly_slope >= 0
        and breakout > 0
    )
    stretched = gap_exhaustion > 0 or rsi_value >= 70 or signal_score <= 0.45
    if strong:
        return base + 0.5
    if stretched:
        return max(1.5, base - 0.8)
    return base


def evaluate_managed_position(
    *,
    state: ManagedPositionState,
    latest: pd.Series,
    timestamp: pd.Timestamp,
    risk: RiskConfig,
) -> ExitDecision | None:
    current_timestamp = pd.Timestamp(timestamp)
    if state.last_bar_at is not None and current_timestamp <= state.last_bar_at:
        return None
    state.last_bar_at = current_timestamp
    state.bars_held += 1

    high_price = _coerce_float(
        latest.get("high"), _coerce_float(latest.get("close"), state.entry_price)
    )
    low_price = _coerce_float(
        latest.get("low"), _coerce_float(latest.get("close"), state.entry_price)
    )
    close_price = _coerce_float(latest.get("close"), state.entry_price)
    atr_value = max(_coerce_float(latest.get("entry_atr_14"), state.atr_at_entry), 0.01)
    state.last_reference_high_price = high_price
    state.last_reference_low_price = low_price
    state.last_reference_close_price = close_price
    state.last_reference_timestamp = current_timestamp

    state.highest_price = max(state.highest_price, high_price)
    reward_multiple = (close_price - state.entry_price) / state.initial_risk_per_share
    if not state.break_even_armed and reward_multiple >= risk.break_even_trigger_r:
        state.break_even_armed = True
        state.stop_price = max(state.stop_price, state.entry_price + max(atr_value * 0.05, 0.01))

    trailing_multiple = _dynamic_trailing_multiple(latest, risk)
    state.current_trailing_multiple = trailing_multiple
    trailing_candidate = state.highest_price - atr_value * trailing_multiple
    state.next_trailing_price = max(state.trailing_stop_price, trailing_candidate)
    state.trailing_stop_price = max(state.trailing_stop_price, trailing_candidate)
    active_stop = state.active_stop_price
    if low_price <= active_stop:
        if active_stop >= state.entry_price:
            reason = "建値防衛またはトレーリングストップで利益確定"
        elif state.trailing_stop_price > state.stop_price:
            reason = "トレーリングストップで利益確定"
        else:
            reason = "初期防御ストップで撤退"
        return ExitDecision(action="full", quantity=state.quantity, reason_ja=reason)

    partial_target = state.partial_target_price
    if (
        risk.allow_partial_profit
        and not state.partial_taken
        and state.quantity >= 2
        and high_price >= partial_target
    ):
        desired = int(round(state.initial_quantity * risk.partial_take_profit_fraction))
        quantity = min(max(1, desired), state.quantity - 1)
        if quantity > 0:
            state.partial_taken = True
            state.break_even_armed = True
            state.stop_price = max(
                state.stop_price, state.entry_price + max(atr_value * 0.05, 0.01)
            )
            return ExitDecision(
                action="partial",
                quantity=quantity,
                reason_ja=f"{risk.partial_take_profit_r:.1f}R 到達で一部利確し、建値防衛へ移行",
            )

    progress = state.highest_price - state.entry_price
    if (
        risk.stagnation_bars > 0
        and state.bars_held >= risk.stagnation_bars
        and progress < state.initial_risk_per_share * risk.stagnation_min_r
    ):
        return ExitDecision(
            action="full", quantity=state.quantity, reason_ja="時間切れ撤退（進展不足）"
        )

    if state.bars_held >= risk.max_hold_bars > 0:
        return ExitDecision(action="full", quantity=state.quantity, reason_ja="最大保有期間に到達")
    return None
