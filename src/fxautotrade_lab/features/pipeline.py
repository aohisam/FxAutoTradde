"""Feature pipeline for multi-timeframe strategies."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.context.market import MarketContextBuilder
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.quality import validate_bar_frame
from fxautotrade_lab.data.resample import resample_ohlcv
from fxautotrade_lab.features.candles import (
    bullish_engulfing,
    doji,
    gap_continuation,
    gap_exhaustion,
    gap_pct,
    hammer,
    inside_bar,
    inside_bar_breakout,
)
from fxautotrade_lab.features.indicators import atr, ema, rolling_return, rolling_zscore, rsi
from fxautotrade_lab.features.structure import (
    compression_score,
    higher_high_higher_low_score,
    slope,
)


@dataclass(slots=True)
class MultiTimeframeFeatureSet:
    symbol: str
    entry_frame: pd.DataFrame
    daily_frame: pd.DataFrame
    weekly_frame: pd.DataFrame
    monthly_frame: pd.DataFrame


def _prepare_common(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    working = validate_bar_frame(frame.copy())
    working[f"{prefix}_ema_20"] = ema(working["close"], 20)
    working[f"{prefix}_ema_50"] = ema(working["close"], 50)
    working[f"{prefix}_ema_200"] = ema(working["close"], 200)
    working[f"{prefix}_rsi_14"] = rsi(working["close"], 14)
    working[f"{prefix}_atr_14"] = atr(working, 14)
    working[f"{prefix}_slope_20"] = slope(working["close"], 20)
    working[f"{prefix}_return_20"] = rolling_return(working["close"], 20)
    working[f"{prefix}_hhhl_score"] = higher_high_higher_low_score(working, 5)
    working[f"{prefix}_compression"] = compression_score(working, 20)
    return working


def _asof_join(base: pd.DataFrame, enrich: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    merged = pd.merge_asof(
        base.sort_index().reset_index().rename(columns={"index": "timestamp"}),
        enrich[columns].sort_index().reset_index().rename(columns={"index": "timestamp"}),
        on="timestamp",
        direction="backward",
    ).set_index("timestamp")
    merged.index = pd.DatetimeIndex(merged.index)
    return merged


def build_multi_timeframe_feature_set(
    symbol: str,
    bars_by_timeframe: dict[TimeFrame, pd.DataFrame],
    benchmark_bars: dict[TimeFrame, pd.DataFrame] | None,
    sector_bars: dict[TimeFrame, pd.DataFrame] | None,
    config: AppConfig,
) -> MultiTimeframeFeatureSet:
    daily = _prepare_common(bars_by_timeframe[TimeFrame.DAY_1], "daily")
    weekly = _prepare_common(
        resample_ohlcv(daily[["open", "high", "low", "close", "volume"]], "W-FRI"), "weekly"
    )
    monthly = _prepare_common(
        resample_ohlcv(daily[["open", "high", "low", "close", "volume"]], "ME"),
        "monthly",
    )
    entry_tf = config.strategy.entry_timeframe
    entry = _prepare_common(bars_by_timeframe[entry_tf], "entry")
    entry["dollar_volume"] = entry["close"] * entry["volume"]
    entry["volume_zscore"] = rolling_zscore(entry["volume"], 20)
    entry["pullback_depth_atr"] = (
        (entry["entry_ema_20"] - entry["close"]) / entry["entry_atr_14"].replace(0, pd.NA)
    ).fillna(0.0)
    entry["breakout_20"] = (entry["close"] > entry["high"].rolling(20).max().shift(1)).astype(float)
    entry["continuation_ready"] = (
        (entry["close"] > entry["entry_ema_20"])
        & (entry["entry_rsi_14"] > 50)
        & (entry["volume_zscore"] > -0.5)
    ).astype(float)
    entry["bullish_engulfing"] = bullish_engulfing(entry)
    entry["hammer"] = hammer(entry)
    entry["inside_bar"] = inside_bar(entry)
    entry["inside_bar_breakout"] = inside_bar_breakout(entry)
    entry["doji"] = doji(entry)
    entry["gap_pct"] = gap_pct(entry)
    entry["gap_continuation"] = gap_continuation(entry)
    entry["gap_exhaustion"] = gap_exhaustion(entry)

    daily_cols = [
        "daily_ema_50",
        "daily_ema_200",
        "daily_rsi_14",
        "daily_atr_14",
        "daily_slope_20",
        "daily_hhhl_score",
        "daily_return_20",
    ]
    weekly_cols = [
        "weekly_ema_20",
        "weekly_slope_20",
        "weekly_hhhl_score",
        "weekly_return_20",
    ]
    monthly_cols = [
        "monthly_ema_20",
        "monthly_slope_20",
        "monthly_return_20",
    ]
    entry = _asof_join(entry, daily, daily_cols)
    entry = _asof_join(entry, weekly, weekly_cols)
    entry = _asof_join(entry, monthly, monthly_cols)

    benchmark_entry = None
    if benchmark_bars and entry_tf in benchmark_bars:
        benchmark_entry = _prepare_common(benchmark_bars[entry_tf], "benchmark_entry")
    sector_entry = None
    if sector_bars and entry_tf in sector_bars:
        sector_entry = _prepare_common(sector_bars[entry_tf], "sector_entry")
    entry = MarketContextBuilder().build(entry, benchmark_entry, sector_entry)
    entry["symbol"] = symbol.upper()
    entry["accepted_data"] = True
    return MultiTimeframeFeatureSet(
        symbol=symbol.upper(),
        entry_frame=entry,
        daily_frame=daily,
        weekly_frame=weekly,
        monthly_frame=monthly,
    )
