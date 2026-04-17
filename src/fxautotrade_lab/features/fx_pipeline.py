"""FX-specific quote-aware feature pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import logging

import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.context.economic_events import BaseEconomicEventProvider, build_event_provider
from fxautotrade_lab.core.constants import ASIA_TOKYO, UTC
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.core.symbols import split_fx_symbol
from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame
from fxautotrade_lab.features.indicators import adx, atr, ema

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FxFeatureSet:
    symbol: str
    execution_frame: pd.DataFrame
    signal_frame: pd.DataFrame
    trend_frame: pd.DataFrame


def _asof_join(base: pd.DataFrame, enrich: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    merged = pd.merge_asof(
        base.sort_index().reset_index().rename(columns={"index": "timestamp"}),
        enrich[columns].sort_index().reset_index().rename(columns={"index": "timestamp"}),
        on="timestamp",
        direction="backward",
    ).set_index("timestamp")
    merged.index = pd.DatetimeIndex(merged.index)
    return merged


def _consecutive_true_counts(series: pd.Series) -> pd.Series:
    counts: list[int] = []
    current = 0
    for value in series.fillna(False).astype(bool):
        current = current + 1 if value else 0
        counts.append(current)
    return pd.Series(counts, index=series.index, dtype="int64")


def _contextual_quantile_threshold(
    series: pd.Series,
    bucket_keys: pd.Series,
    *,
    lookback_days: int,
    quantile: float,
    min_periods: int = 30,
) -> pd.Series:
    result = pd.Series(index=series.index, dtype="float64")
    for _, bucket_series in series.groupby(bucket_keys):
        ordered = bucket_series.sort_index()
        threshold = ordered.rolling(window=f"{lookback_days}D", min_periods=min_periods, closed="left").quantile(quantile)
        result.loc[threshold.index] = threshold
    return result.sort_index()


def _rollover_blackout(index: pd.DatetimeIndex, rollover_hour_utc: int, blackout_minutes: int) -> pd.Series:
    utc_index = index.tz_convert(UTC)
    rollover = utc_index.normalize() + pd.to_timedelta(rollover_hour_utc, unit="h")
    delta_minutes = pd.Series((utc_index - rollover).total_seconds() / 60.0, index=index).abs()
    return delta_minutes <= blackout_minutes


def _tokyo_early_blackout(index: pd.DatetimeIndex, start_hour: int, end_hour: int) -> pd.Series:
    tokyo_index = index.tz_convert(ASIA_TOKYO)
    hours = tokyo_index.hour
    return pd.Series((hours >= start_hour) & (hours <= end_hour), index=index)


def _event_blackout_series(
    index: pd.DatetimeIndex,
    symbol: str,
    config: AppConfig,
    provider: BaseEconomicEventProvider,
    *,
    runtime_mode: bool,
) -> pd.Series:
    event_cfg = config.strategy.fx_breakout_pullback.event_filter
    if not event_cfg.enabled:
        return pd.Series(False, index=index)
    base, quote = split_fx_symbol(symbol)
    if index.empty:
        return pd.Series(dtype=bool)
    try:
        events = provider.list_events(index.min(), index.max(), {base, quote})
    except Exception as exc:
        failure_mode = (
            event_cfg.realtime_failure_mode if runtime_mode else event_cfg.backtest_failure_mode
        ).strip().lower()
        if failure_mode == "warn_and_disable":
            logger.warning(
                "経済イベント取得に失敗したためイベントフィルタを無効化します: symbol=%s runtime_mode=%s error=%s",
                symbol,
                runtime_mode,
                exc,
            )
            return pd.Series(False, index=index)
        if failure_mode == "fail_closed":
            return pd.Series(True, index=index)
        if failure_mode == "fail_open":
            return pd.Series(False, index=index)
        raise ValueError(f"未対応の event failure mode です: {failure_mode}")
    before = pd.Timedelta(minutes=event_cfg.event_blackout_before_minutes)
    after = pd.Timedelta(minutes=event_cfg.event_blackout_after_minutes)
    mask = pd.Series(False, index=index)
    for event in events:
        mask |= (index >= event.timestamp - before) & (index <= event.timestamp + after)
    return mask


def _prepare_swing_frame(frame: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    fx_cfg = config.strategy.fx_breakout_pullback
    working = validate_quote_bar_frame(frame.copy())
    low_column = "mid_low" if "mid_low" in working.columns else "low"
    high_column = "mid_high" if "mid_high" in working.columns else "high"
    working["swing_bar_timestamp"] = working.index
    working["swing_low_reference"] = (
        pd.to_numeric(working[low_column], errors="coerce")
        .rolling(fx_cfg.swing_lookback_bars, min_periods=1)
        .min()
    )
    working["swing_high_reference"] = (
        pd.to_numeric(working[high_column], errors="coerce")
        .rolling(fx_cfg.swing_lookback_bars, min_periods=1)
        .max()
    )
    return working


def _prepare_trend_frame(frame: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    fx_cfg = config.strategy.fx_breakout_pullback
    working = validate_quote_bar_frame(frame.copy())
    working["trend_bar_timestamp"] = working.index
    working["ema_fast_1h"] = ema(working["close"], fx_cfg.ema_fast)
    working["ema_slow_1h"] = ema(working["close"], fx_cfg.ema_slow)
    working["ema_fast_slope_1h"] = working["ema_fast_1h"] - working["ema_fast_1h"].shift(fx_cfg.ema_slope_lookback)
    working["atr_1h"] = atr(working[["open", "high", "low", "close"]], fx_cfg.atr_period)
    working["adx_1h"] = adx(working[["open", "high", "low", "close"]], fx_cfg.adx_period)
    working["atr_floor_1h"] = (
        working["atr_1h"]
        .rolling(window=fx_cfg.atr_percentile_lookback_bars, min_periods=fx_cfg.atr_period)
        .quantile(fx_cfg.min_atr_percentile)
        .shift(1)
    )
    working["atr_not_too_low_1h"] = (
        working["atr_floor_1h"].isna() | (working["atr_1h"] >= working["atr_floor_1h"])
    )
    working["trend_long_allowed_1h"] = (
        (working["ema_fast_1h"] > working["ema_slow_1h"])
        & (working["ema_fast_slope_1h"] > 0)
        & (
            (working["adx_1h"] >= fx_cfg.adx_threshold)
            | working["atr_not_too_low_1h"]
        )
    )
    working["trend_short_allowed_1h"] = (
        (working["ema_fast_1h"] < working["ema_slow_1h"])
        & (working["ema_fast_slope_1h"] < 0)
        & (
            (working["adx_1h"] >= fx_cfg.adx_threshold)
            | working["atr_not_too_low_1h"]
        )
    )
    working["ema_cross_down_1h"] = working["ema_fast_1h"] <= working["ema_slow_1h"]
    working["ema_cross_up_1h"] = working["ema_fast_1h"] >= working["ema_slow_1h"]
    working["slope_nonpos_1h"] = working["ema_fast_slope_1h"] <= 0
    working["slope_nonneg_1h"] = working["ema_fast_slope_1h"] >= 0
    working["close_below_fast_1h"] = working["close"] < working["ema_fast_1h"]
    working["close_above_fast_1h"] = working["close"] > working["ema_fast_1h"]
    working["slope_nonpos_count_1h"] = _consecutive_true_counts(working["slope_nonpos_1h"])
    working["slope_nonneg_count_1h"] = _consecutive_true_counts(working["slope_nonneg_1h"])
    working["close_below_fast_count_1h"] = _consecutive_true_counts(working["close_below_fast_1h"])
    working["close_above_fast_count_1h"] = _consecutive_true_counts(working["close_above_fast_1h"])
    working["partial_exit_trend_break_1h"] = (
        ~working["ema_cross_down_1h"]
        & (
            (working["slope_nonpos_count_1h"] >= fx_cfg.trend_break_confirm_bars)
            | (working["close_below_fast_count_1h"] >= fx_cfg.trend_break_confirm_bars)
        )
    )
    working["full_exit_trend_break_1h"] = working["ema_cross_down_1h"]
    working["partial_exit_short_trend_break_1h"] = (
        ~working["ema_cross_up_1h"]
        & (
            (working["slope_nonneg_count_1h"] >= fx_cfg.trend_break_confirm_bars)
            | (working["close_above_fast_count_1h"] >= fx_cfg.trend_break_confirm_bars)
        )
    )
    working["full_exit_short_trend_break_1h"] = working["ema_cross_up_1h"]
    working["trend_gap_ratio_1h"] = (
        (working["ema_fast_1h"] - working["ema_slow_1h"]) / working["close"].replace(0, pd.NA)
    ).fillna(0.0)
    return working


def _prepare_signal_frame(frame: pd.DataFrame, trend_frame: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    fx_cfg = config.strategy.fx_breakout_pullback
    working = validate_quote_bar_frame(frame.copy())
    working["signal_bar_timestamp"] = working.index
    working["atr_15m"] = atr(working[["open", "high", "low", "close"]], fx_cfg.atr_period)
    working["ema_fast_15m"] = ema(working["close"], 20)
    working["ema_slow_15m"] = ema(working["close"], 50)
    trend_cols = [
        "trend_bar_timestamp",
        "trend_long_allowed_1h",
        "trend_short_allowed_1h",
        "partial_exit_trend_break_1h",
        "full_exit_trend_break_1h",
        "partial_exit_short_trend_break_1h",
        "full_exit_short_trend_break_1h",
        "ema_fast_1h",
        "ema_slow_1h",
        "ema_fast_slope_1h",
        "adx_1h",
        "atr_1h",
        "trend_gap_ratio_1h",
    ]
    working = _asof_join(working, trend_frame, trend_cols)
    working["breakout_level_15m"] = working["high"].rolling(fx_cfg.breakout_lookback).max().shift(1)
    working["donchian_low_15m"] = working["low"].rolling(fx_cfg.breakout_lookback).min().shift(1)
    working["breakout_short_level_15m"] = working["donchian_low_15m"]
    working["donchian_width_15m"] = (working["breakout_level_15m"] - working["donchian_low_15m"]).fillna(0.0)
    working["breakout_atr_15m"] = working["atr_15m"].shift(1)
    working["breakout_strength_15m"] = (
        (working["close"] - working["breakout_level_15m"]) / working["breakout_atr_15m"].replace(0, pd.NA)
    ).fillna(0.0)
    working["breakout_strength_short_15m"] = (
        (working["breakout_short_level_15m"] - working["close"]) / working["breakout_atr_15m"].replace(0, pd.NA)
    ).fillna(0.0)
    working["breakout_signal_15m"] = (
        (working["close"] > (working["breakout_level_15m"] + fx_cfg.breakout_buffer_atr * working["breakout_atr_15m"]))
        & working["trend_long_allowed_1h"].fillna(False)
    )
    working["breakout_signal_short_15m"] = (
        (working["close"] < (working["breakout_short_level_15m"] - fx_cfg.breakout_buffer_atr * working["breakout_atr_15m"]))
        & working["trend_short_allowed_1h"].fillna(False)
    )
    return working


def build_fx_feature_set(
    symbol: str,
    bars_by_timeframe: dict[TimeFrame, pd.DataFrame],
    config: AppConfig,
    *,
    event_provider: BaseEconomicEventProvider | None = None,
    runtime_mode: bool = False,
) -> FxFeatureSet:
    fx_cfg = config.strategy.fx_breakout_pullback
    execution_frame = validate_quote_bar_frame(bars_by_timeframe[fx_cfg.execution_timeframe].copy())
    signal_frame = validate_quote_bar_frame(bars_by_timeframe[fx_cfg.signal_timeframe].copy())
    trend_frame = validate_quote_bar_frame(bars_by_timeframe[fx_cfg.trend_timeframe].copy())
    swing_base = bars_by_timeframe.get(fx_cfg.swing_timeframe, bars_by_timeframe[fx_cfg.execution_timeframe])
    swing_frame = validate_quote_bar_frame(swing_base.copy())
    provider = event_provider or build_event_provider(fx_cfg.event_filter)
    daily_frame = (
        validate_quote_bar_frame(bars_by_timeframe[TimeFrame.DAY_1].copy())
        if TimeFrame.DAY_1 in bars_by_timeframe
        else pd.DataFrame()
    )

    prepared_trend = _prepare_trend_frame(trend_frame, config)
    prepared_signal = _prepare_signal_frame(signal_frame, prepared_trend, config)
    prepared_swing = _prepare_swing_frame(swing_frame, config)

    execution = execution_frame.copy()
    signal_cols = [
        "signal_bar_timestamp",
        "breakout_signal_15m",
        "breakout_signal_short_15m",
        "breakout_level_15m",
        "breakout_short_level_15m",
        "breakout_atr_15m",
        "atr_15m",
        "ema_fast_15m",
        "ema_slow_15m",
        "donchian_width_15m",
        "breakout_strength_15m",
        "breakout_strength_short_15m",
    ]
    trend_cols = [
        "trend_bar_timestamp",
        "trend_long_allowed_1h",
        "trend_short_allowed_1h",
        "partial_exit_trend_break_1h",
        "full_exit_trend_break_1h",
        "partial_exit_short_trend_break_1h",
        "full_exit_short_trend_break_1h",
        "ema_fast_1h",
        "ema_slow_1h",
        "ema_fast_slope_1h",
        "adx_1h",
        "atr_1h",
        "trend_gap_ratio_1h",
    ]
    execution = _asof_join(execution, prepared_signal, signal_cols)
    execution = _asof_join(execution, prepared_trend, trend_cols)
    execution = _asof_join(
        execution,
        prepared_swing,
        ["swing_bar_timestamp", "swing_low_reference", "swing_high_reference"],
    )
    execution["symbol"] = symbol.upper()
    execution["swing_source_timeframe"] = fx_cfg.swing_timeframe.value
    execution["weekday"] = execution.index.weekday
    execution["hour"] = execution.index.hour
    execution["spread_context_bucket"] = (
        execution["symbol"].astype(str)
        + "_"
        + execution["weekday"].astype(str)
        + "_"
        + execution["hour"].astype(str)
    )
    execution["spread_context_limit"] = _contextual_quantile_threshold(
        execution["spread_close"],
        execution["spread_context_bucket"],
        lookback_days=fx_cfg.spread_context_lookback_days,
        quantile=fx_cfg.spread_percentile_threshold,
    )
    execution["spread_context_ok"] = (
        execution["spread_context_limit"].isna()
        | (execution["spread_close"] <= execution["spread_context_limit"])
    )
    execution["spread_context_ratio"] = (
        execution["spread_close"] / execution["spread_context_limit"].replace(0, pd.NA)
    ).fillna(0.0)
    execution["spread_to_atr"] = (
        execution["spread_close"] / execution["atr_15m"].replace(0, pd.NA)
    ).fillna(0.0)
    execution["spread_ratio_ok"] = execution["spread_to_atr"] <= fx_cfg.max_spread_to_atr_ratio
    execution["rollover_blackout"] = _rollover_blackout(
        execution.index,
        rollover_hour_utc=fx_cfg.rollover_hour_utc,
        blackout_minutes=fx_cfg.rollover_blackout_minutes,
    )
    execution["tokyo_early_blackout"] = (
        _tokyo_early_blackout(
            execution.index,
            start_hour=fx_cfg.tokyo_early_blackout_start_hour,
            end_hour=fx_cfg.tokyo_early_blackout_end_hour,
        )
        if fx_cfg.tokyo_early_blackout_enabled
        else False
    )
    execution["event_blackout"] = _event_blackout_series(
        execution.index,
        symbol,
        config,
        provider,
        runtime_mode=runtime_mode,
    )
    if not daily_frame.empty:
        daily_enrich = daily_frame.copy()
        daily_enrich["prev_day_high"] = daily_enrich["high"].shift(1)
        daily_enrich["prev_day_low"] = daily_enrich["low"].shift(1)
        execution = _asof_join(execution, daily_enrich, ["prev_day_high", "prev_day_low"])
        execution["breakout_distance_from_day_high"] = (
            (execution["close"] - execution["prev_day_high"]) / execution["atr_15m"].replace(0, pd.NA)
        ).fillna(0.0)
        execution["breakout_distance_from_day_low"] = (
            (execution["close"] - execution["prev_day_low"]) / execution["atr_15m"].replace(0, pd.NA)
        ).fillna(0.0)
    else:
        execution["prev_day_high"] = pd.NA
        execution["prev_day_low"] = pd.NA
        execution["breakout_distance_from_day_high"] = 0.0
        execution["breakout_distance_from_day_low"] = 0.0
    execution["entry_context_ok"] = (
        execution["spread_context_ok"].fillna(True)
        & execution["spread_ratio_ok"].fillna(True)
        & ~execution["rollover_blackout"].fillna(False)
        & ~pd.Series(execution["tokyo_early_blackout"], index=execution.index).fillna(False)
        & ~execution["event_blackout"].fillna(False)
    )
    execution["regime_label"] = "range_or_weak"
    moderate_long = execution["trend_long_allowed_1h"].fillna(False)
    strong_long = moderate_long & (execution["adx_1h"].fillna(0.0) >= fx_cfg.adx_threshold)
    moderate_short = execution["trend_short_allowed_1h"].fillna(False)
    strong_short = moderate_short & (execution["adx_1h"].fillna(0.0) >= fx_cfg.adx_threshold)
    blocked = ~execution["entry_context_ok"].fillna(False)
    execution.loc[moderate_long, "regime_label"] = "trend_up_moderate"
    execution.loc[strong_long, "regime_label"] = "trend_up_strong"
    execution.loc[moderate_short, "regime_label"] = "trend_down_moderate"
    execution.loc[strong_short, "regime_label"] = "trend_down_strong"
    execution.loc[blocked, "regime_label"] = "blocked"
    execution["accepted_data"] = True
    return FxFeatureSet(
        symbol=symbol.upper(),
        execution_frame=execution,
        signal_frame=prepared_signal,
        trend_frame=prepared_trend,
    )
