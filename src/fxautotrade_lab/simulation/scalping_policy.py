"""Shared entry and risk policy for scalping backtest and paper replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig


class ScalpingRejectReason(StrEnum):
    ACCEPTED = "accepted"
    OUTSIDE_TICK_WINDOW = "outside_tick_window"
    COOLDOWN = "cooldown"
    MAX_TRADES_PER_DAY = "max_trades_per_day"
    DAILY_LOSS_HALT = "daily_loss_halt"
    CONSECUTIVE_LOSS_HALT = "consecutive_loss_halt"
    STALE_TICK = "stale_tick"
    SPREAD_EXCEEDED = "spread_exceeded"
    SPREAD_Z_EXCEEDED = "spread_z_exceeded"
    SPREAD_TO_MEAN_EXCEEDED = "spread_to_mean_exceeded"
    VOLATILITY_TOO_LOW = "volatility_too_low"
    THRESHOLD_NOT_MET = "threshold_not_met"
    ENTRY_TICK_NOT_FOUND = "entry_tick_not_found"
    EXIT_TICK_NOT_FOUND = "exit_tick_not_found"
    QUANTITY_TOO_SMALL = "quantity_too_small"


@dataclass(frozen=True, slots=True)
class BlackoutWindow:
    start: str
    end: str
    reason: str = "manual"


@dataclass(slots=True)
class ScalpingExecutionConfig:
    starting_cash: float = 5_000_000.0
    fixed_order_amount: float = 300_000.0
    minimum_order_quantity: int = 1_000
    quantity_step: int = 1_000
    max_position_notional_fraction: float = 0.25
    entry_latency_ms: int = 250
    cooldown_seconds: int = 5
    max_trades_per_day: int = 120
    mode: BrokerMode = BrokerMode.LOCAL_SIM
    max_daily_loss_amount: float | None = None
    max_consecutive_losses: int | None = None
    halt_for_day_on_daily_loss: bool = True
    halt_for_day_on_consecutive_losses: bool = True
    max_tick_gap_seconds: int | None = None
    reject_on_stale_ticks: bool = True
    max_spread_z: float | None = None
    max_spread_to_mean_ratio: float | None = None
    record_rejected_signals: bool = True
    max_rejected_signals: int | None = None
    blackout_windows_jst: tuple[BlackoutWindow, ...] = ()


@dataclass(frozen=True, slots=True)
class ScalpingRiskSnapshot:
    local_day: str
    trades_today: int
    daily_pnl: float
    consecutive_losses: int


@dataclass(slots=True)
class ScalpingRiskState:
    execution_config: ScalpingExecutionConfig
    cash: float = field(init=False)
    next_allowed_time: pd.Timestamp | None = None
    daily_start_cash: dict[str, float] = field(default_factory=dict)
    daily_halts: dict[str, str] = field(default_factory=dict)
    consecutive_losses_by_day: dict[str, int] = field(default_factory=dict)
    trades_per_day: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cash = float(self.execution_config.starting_cash)

    def snapshot(self, timestamp: pd.Timestamp) -> ScalpingRiskSnapshot:
        local_day = _local_day(timestamp)
        self.daily_start_cash.setdefault(local_day, self.cash)
        self.consecutive_losses_by_day.setdefault(local_day, 0)
        return ScalpingRiskSnapshot(
            local_day=local_day,
            trades_today=int(self.trades_per_day.get(local_day, 0)),
            daily_pnl=float(self.cash - self.daily_start_cash[local_day]),
            consecutive_losses=int(self.consecutive_losses_by_day.get(local_day, 0)),
        )

    def entry_reject_reason(self, timestamp: pd.Timestamp) -> str:
        snapshot = self.snapshot(timestamp)
        if self.next_allowed_time is not None and timestamp < self.next_allowed_time:
            return ScalpingRejectReason.COOLDOWN.value
        if snapshot.local_day in self.daily_halts:
            return self.daily_halts[snapshot.local_day]
        if snapshot.trades_today >= int(self.execution_config.max_trades_per_day):
            return ScalpingRejectReason.MAX_TRADES_PER_DAY.value
        return ""

    def record_trade(
        self,
        *,
        signal_time: pd.Timestamp,
        exit_time: pd.Timestamp,
        pnl: float,
    ) -> None:
        snapshot = self.snapshot(signal_time)
        local_day = snapshot.local_day
        self.cash += float(pnl)
        self.trades_per_day[local_day] = int(self.trades_per_day.get(local_day, 0)) + 1
        if pnl < 0.0:
            self.consecutive_losses_by_day[local_day] = (
                int(self.consecutive_losses_by_day.get(local_day, 0)) + 1
            )
        elif pnl > 0.0:
            self.consecutive_losses_by_day[local_day] = 0
        daily_pnl = self.cash - self.daily_start_cash[local_day]
        if (
            self.execution_config.max_daily_loss_amount is not None
            and self.execution_config.halt_for_day_on_daily_loss
            and daily_pnl <= -abs(float(self.execution_config.max_daily_loss_amount))
        ):
            self.daily_halts[local_day] = ScalpingRejectReason.DAILY_LOSS_HALT.value
        if (
            self.execution_config.max_consecutive_losses is not None
            and self.execution_config.halt_for_day_on_consecutive_losses
            and int(self.consecutive_losses_by_day.get(local_day, 0))
            >= int(self.execution_config.max_consecutive_losses)
        ):
            self.daily_halts[local_day] = ScalpingRejectReason.CONSECUTIVE_LOSS_HALT.value
        self.next_allowed_time = pd.Timestamp(exit_time) + pd.Timedelta(
            seconds=self.execution_config.cooldown_seconds
        )


@dataclass(frozen=True, slots=True)
class ScalpingSignalContext:
    timestamp: pd.Timestamp
    tick_index: pd.DatetimeIndex
    spread: float
    spread_mean: float
    spread_z: float
    volatility: float
    probability: float
    threshold: float
    chosen_side: str = ""
    quantity: int | None = None
    risk_reject_reason: str = ""


@dataclass(frozen=True, slots=True)
class ScalpingSignalDecision:
    accepted: bool
    reject_reason: str
    chosen_side: str = ""

    @property
    def decision(self) -> str:
        return "enter" if self.accepted else "reject"


@dataclass(frozen=True, slots=True)
class ScalpingSignalPolicy:
    training_config: ScalpingTrainingConfig
    execution_config: ScalpingExecutionConfig

    def decide_entry(self, context: ScalpingSignalContext) -> ScalpingSignalDecision:
        reject_reason = context.risk_reject_reason or self.entry_reject_reason(
            timestamp=context.timestamp,
            tick_index=context.tick_index,
            spread=context.spread,
            spread_mean=context.spread_mean,
            spread_z=context.spread_z,
            volatility=context.volatility,
            probability=context.probability,
            threshold=context.threshold,
        )
        if not reject_reason and context.quantity is not None and context.quantity <= 0:
            reject_reason = ScalpingRejectReason.QUANTITY_TOO_SMALL.value
        return ScalpingSignalDecision(
            accepted=not bool(reject_reason),
            reject_reason=reject_reason or ScalpingRejectReason.ACCEPTED.value,
            chosen_side=context.chosen_side,
        )

    def entry_reject_reason(
        self,
        *,
        timestamp: pd.Timestamp,
        tick_index: pd.DatetimeIndex,
        spread: float,
        spread_mean: float,
        spread_z: float,
        volatility: float,
        probability: float,
        threshold: float,
    ) -> str:
        blackout_reason = _blackout_reason(timestamp, self.execution_config.blackout_windows_jst)
        if blackout_reason:
            return f"blackout_window:{blackout_reason}"
        if _is_stale_tick(
            tick_index,
            timestamp,
            max_tick_gap_seconds=self.execution_config.max_tick_gap_seconds,
            reject_on_stale_ticks=self.execution_config.reject_on_stale_ticks,
        ):
            return ScalpingRejectReason.STALE_TICK.value
        if spread > self.training_config.max_spread_pips:
            return ScalpingRejectReason.SPREAD_EXCEEDED.value
        if self.execution_config.max_spread_z is not None and abs(spread_z) > float(
            self.execution_config.max_spread_z
        ):
            return ScalpingRejectReason.SPREAD_Z_EXCEEDED.value
        if (
            self.execution_config.max_spread_to_mean_ratio is not None
            and spread_mean > 0.0
            and spread / spread_mean > float(self.execution_config.max_spread_to_mean_ratio)
        ):
            return ScalpingRejectReason.SPREAD_TO_MEAN_EXCEEDED.value
        if volatility < self.training_config.min_volatility_pips:
            return ScalpingRejectReason.VOLATILITY_TOO_LOW.value
        if probability < threshold:
            return ScalpingRejectReason.THRESHOLD_NOT_MET.value
        return ""


@dataclass(frozen=True, slots=True)
class ScalpingExecutionPolicy:
    execution_config: ScalpingExecutionConfig

    def entry_index(self, tick_index: pd.DatetimeIndex, timestamp: pd.Timestamp) -> int:
        entry_target = timestamp + pd.Timedelta(milliseconds=self.execution_config.entry_latency_ms)
        return int(tick_index.searchsorted(entry_target, side="left"))

    def quantity_for_price(self, price: float, *, cash: float) -> int:
        max_notional = cash * self.execution_config.max_position_notional_fraction
        target_notional = min(float(self.execution_config.fixed_order_amount), max_notional)
        if price <= 0 or target_notional <= 0:
            return 0
        raw_quantity = int(target_notional // price)
        step = max(1, int(self.execution_config.quantity_step))
        quantity = (raw_quantity // step) * step
        if quantity < int(self.execution_config.minimum_order_quantity):
            return 0
        return quantity

    def quantity_for_tick(self, tick: pd.Series, *, side: str, cash: float) -> int:
        price = float(tick["ask"] if side == "long" else tick["bid"])
        return self.quantity_for_price(price, cash=cash)


def _is_stale_tick(
    tick_index: pd.DatetimeIndex,
    timestamp: pd.Timestamp,
    *,
    max_tick_gap_seconds: int | None,
    reject_on_stale_ticks: bool,
) -> bool:
    if not reject_on_stale_ticks or max_tick_gap_seconds is None:
        return False
    previous_index = tick_index.searchsorted(timestamp, side="right") - 1
    if previous_index < 0:
        return True
    previous_tick_time = pd.Timestamp(tick_index[previous_index])
    gap = (timestamp - previous_tick_time).total_seconds()
    return gap > float(max_tick_gap_seconds)


def _blackout_reason(timestamp: pd.Timestamp, windows: tuple[BlackoutWindow, ...]) -> str:
    if not windows:
        return ""
    local = (
        timestamp.tz_convert(ASIA_TOKYO)
        if timestamp.tzinfo is not None
        else timestamp.tz_localize(ASIA_TOKYO)
    )
    minutes = local.hour * 60 + local.minute
    for window in windows:
        start = _hhmm_to_minutes(window.start)
        end = _hhmm_to_minutes(window.end)
        if start == end:
            continue
        in_window = start <= minutes < end if start < end else minutes >= start or minutes < end
        if in_window:
            return window.reason
    return ""


def _hhmm_to_minutes(value: str) -> int:
    try:
        hour_text, minute_text = str(value).split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception as exc:
        raise ValueError(f"blackout window の時刻は HH:MM 形式で指定してください: {value}") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"blackout window の時刻が範囲外です: {value}")
    return hour * 60 + minute


def _local_day(timestamp: pd.Timestamp) -> str:
    local = (
        timestamp.tz_convert(ASIA_TOKYO)
        if timestamp.tzinfo is not None
        else timestamp.tz_localize(ASIA_TOKYO)
    )
    return local.date().isoformat()
