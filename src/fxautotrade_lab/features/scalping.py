"""Feature engineering for tick/second-bar FX scalping research."""

from __future__ import annotations

import numpy as np
import pandas as pd

from fxautotrade_lab.core.symbols import split_fx_symbol
from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame

SCALPING_FEATURE_COLUMNS = [
    "side_sign",
    "directional_return_1_pips",
    "directional_return_3_pips",
    "directional_return_10_pips",
    "directional_breakout_20_pips",
    "directional_range_position_20",
    "micro_volatility_10_pips",
    "micro_volatility_30_pips",
    "range_10_pips",
    "range_30_pips",
    "spread_close_pips",
    "spread_mean_20_pips",
    "spread_z_120",
    "spread_delta_1_pips",
    "spread_delta_3_pips",
    "spread_delta_10_pips",
    "spread_to_mean_20",
    "tick_count_log",
    "tick_count_mean_10",
    "tick_count_z_60",
    "bar_gap_seconds",
    "directional_up_tick_ratio_10",
    "directional_up_tick_ratio_30",
    "directional_signed_tick_imbalance_10",
    "directional_signed_tick_imbalance_30",
    "hour_sin",
    "hour_cos",
    "minute_sin",
    "minute_cos",
    "weekday_sin",
    "weekday_cos",
    "is_tokyo_session",
    "is_london_session",
    "is_newyork_session",
    "is_rollover_window",
    "is_weekend_or_illiquid_window",
    "is_month_start",
    "is_month_end",
]


def pip_size_for_symbol(symbol: str) -> float:
    _, quote = split_fx_symbol(symbol)
    return 0.01 if quote == "JPY" else 0.0001


def build_scalping_feature_frame(
    quote_bars: pd.DataFrame,
    *,
    symbol: str,
    pip_size: float | None = None,
) -> pd.DataFrame:
    """Build no-lookahead microstructure features from quote bars."""

    bars = validate_quote_bar_frame(quote_bars)
    if bars.empty:
        return pd.DataFrame()
    pip = float(pip_size or pip_size_for_symbol(symbol))
    close = pd.to_numeric(bars["close"], errors="coerce")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    spread_close = pd.to_numeric(
        bars.get("spread_close", bars["ask_close"] - bars["bid_close"]), errors="coerce"
    )
    tick_count = pd.to_numeric(bars.get("tick_count", 1), errors="coerce").fillna(0.0)

    returns_pips = close.diff().fillna(0.0) / pip
    rolling_high_20 = high.rolling(20, min_periods=2).max().shift(1)
    rolling_low_20 = low.rolling(20, min_periods=2).min().shift(1)
    rolling_width_20 = ((rolling_high_20 - rolling_low_20) / pip).replace(0.0, np.nan)
    range_position_20 = (
        (close - rolling_low_20) / (rolling_high_20 - rolling_low_20).replace(0.0, np.nan)
    ).clip(0.0, 1.0)

    features = pd.DataFrame(index=bars.index)
    features["return_1_pips"] = returns_pips
    features["return_3_pips"] = close.diff(3).fillna(0.0) / pip
    features["return_10_pips"] = close.diff(10).fillna(0.0) / pip
    features["breakout_up_20_pips"] = ((close - rolling_high_20) / pip).fillna(0.0)
    features["breakout_down_20_pips"] = ((rolling_low_20 - close) / pip).fillna(0.0)
    features["range_position_20"] = range_position_20.fillna(0.5)
    features["micro_volatility_10_pips"] = returns_pips.rolling(10, min_periods=2).std().fillna(0.0)
    features["micro_volatility_30_pips"] = returns_pips.rolling(30, min_periods=3).std().fillna(0.0)
    features["range_10_pips"] = (
        (high.rolling(10, min_periods=2).max() - low.rolling(10, min_periods=2).min()) / pip
    ).fillna(0.0)
    features["range_30_pips"] = (
        (high.rolling(30, min_periods=3).max() - low.rolling(30, min_periods=3).min()) / pip
    ).fillna(0.0)
    features["spread_close_pips"] = (spread_close / pip).fillna(0.0)
    features["spread_mean_20_pips"] = (
        features["spread_close_pips"]
        .rolling(20, min_periods=2)
        .mean()
        .fillna(features["spread_close_pips"])
    )
    features["spread_delta_1_pips"] = features["spread_close_pips"].diff(1).fillna(0.0)
    features["spread_delta_3_pips"] = features["spread_close_pips"].diff(3).fillna(0.0)
    features["spread_delta_10_pips"] = features["spread_close_pips"].diff(10).fillna(0.0)
    features["spread_to_mean_20"] = (
        (features["spread_close_pips"] / features["spread_mean_20_pips"].replace(0.0, np.nan))
        .replace([np.inf, -np.inf], 0.0)
        .fillna(0.0)
    )
    spread_std = (
        features["spread_close_pips"].rolling(120, min_periods=10).std().replace(0.0, np.nan)
    )
    spread_mean = features["spread_close_pips"].rolling(120, min_periods=10).mean()
    features["spread_z_120"] = (
        ((features["spread_close_pips"] - spread_mean) / spread_std).fillna(0.0).clip(-10.0, 10.0)
    )
    features["tick_count_log"] = np.log1p(tick_count)
    features["tick_count_mean_10"] = tick_count.rolling(10, min_periods=2).mean().fillna(tick_count)
    tick_mean_60 = tick_count.rolling(60, min_periods=10).mean()
    tick_std_60 = tick_count.rolling(60, min_periods=10).std().replace(0.0, np.nan)
    features["tick_count_z_60"] = (
        ((tick_count - tick_mean_60) / tick_std_60).fillna(0.0).clip(-10.0, 10.0)
    )
    gap_seconds = pd.Series(bars.index, index=bars.index).diff().dt.total_seconds()
    features["bar_gap_seconds"] = gap_seconds.fillna(0.0).clip(lower=0.0)
    up_tick = (returns_pips > 0.0).astype("float64")
    down_tick = (returns_pips < 0.0).astype("float64")
    features["up_tick_ratio_10"] = up_tick.rolling(10, min_periods=2).mean().fillna(0.5)
    features["up_tick_ratio_30"] = up_tick.rolling(30, min_periods=3).mean().fillna(0.5)
    signed_tick = up_tick - down_tick
    tick_weight = tick_count.replace(0.0, 1.0)
    signed_weighted = signed_tick * tick_weight
    weight_10 = tick_weight.rolling(10, min_periods=2).sum().replace(0.0, np.nan)
    weight_30 = tick_weight.rolling(30, min_periods=3).sum().replace(0.0, np.nan)
    features["signed_tick_imbalance_10"] = (
        signed_weighted.rolling(10, min_periods=2).sum() / weight_10
    ).fillna(0.0)
    features["signed_tick_imbalance_30"] = (
        signed_weighted.rolling(30, min_periods=3).sum() / weight_30
    ).fillna(0.0)

    local_index = bars.index.tz_convert("Asia/Tokyo") if bars.index.tz is not None else bars.index
    minutes_of_day = local_index.hour * 60 + local_index.minute
    features["hour_sin"] = np.sin(2.0 * np.pi * local_index.hour / 24.0)
    features["hour_cos"] = np.cos(2.0 * np.pi * local_index.hour / 24.0)
    features["minute_sin"] = np.sin(2.0 * np.pi * minutes_of_day / 1440.0)
    features["minute_cos"] = np.cos(2.0 * np.pi * minutes_of_day / 1440.0)
    features["weekday_sin"] = np.sin(2.0 * np.pi * local_index.weekday / 7.0)
    features["weekday_cos"] = np.cos(2.0 * np.pi * local_index.weekday / 7.0)
    features["is_tokyo_session"] = ((local_index.hour >= 9) & (local_index.hour < 15)).astype(
        "float64"
    )
    features["is_london_session"] = ((local_index.hour >= 16) | (local_index.hour < 1)).astype(
        "float64"
    )
    features["is_newyork_session"] = ((local_index.hour >= 21) | (local_index.hour < 6)).astype(
        "float64"
    )
    features["is_rollover_window"] = (
        (minutes_of_day >= 5 * 60 + 55) & (minutes_of_day < 6 * 60 + 10)
    ).astype("float64")
    features["is_weekend_or_illiquid_window"] = (
        (local_index.weekday >= 5)
        | (minutes_of_day < 7 * 60)
        | (minutes_of_day >= 5 * 60 + 55) & (minutes_of_day < 6 * 60 + 10)
    ).astype("float64")
    features["is_month_start"] = pd.Series(local_index.is_month_start, index=bars.index).astype(
        "float64"
    )
    features["is_month_end"] = pd.Series(local_index.is_month_end, index=bars.index).astype(
        "float64"
    )
    features["rolling_width_20_pips"] = rolling_width_20.fillna(0.0)
    features["mid_close"] = close
    features["bid_close"] = pd.to_numeric(bars["bid_close"], errors="coerce")
    features["ask_close"] = pd.to_numeric(bars["ask_close"], errors="coerce")
    return features.replace([np.inf, -np.inf], 0.0).fillna(0.0)


def build_directional_feature_frame(features: pd.DataFrame, *, side: str) -> pd.DataFrame:
    """Project neutral features into long/short candidate features."""

    side_key = side.strip().lower()
    if side_key not in {"long", "short"}:
        raise ValueError(f"side は long / short のいずれかです: {side}")
    sign = 1.0 if side_key == "long" else -1.0
    out = pd.DataFrame(index=features.index)
    out["side_sign"] = sign
    out["directional_return_1_pips"] = sign * features["return_1_pips"]
    out["directional_return_3_pips"] = sign * features["return_3_pips"]
    out["directional_return_10_pips"] = sign * features["return_10_pips"]
    if side_key == "long":
        out["directional_breakout_20_pips"] = features["breakout_up_20_pips"]
        out["directional_range_position_20"] = features["range_position_20"]
    else:
        out["directional_breakout_20_pips"] = features["breakout_down_20_pips"]
        out["directional_range_position_20"] = 1.0 - features["range_position_20"]
    for column in (
        "micro_volatility_10_pips",
        "micro_volatility_30_pips",
        "range_10_pips",
        "range_30_pips",
        "spread_close_pips",
        "spread_mean_20_pips",
        "spread_z_120",
        "spread_delta_1_pips",
        "spread_delta_3_pips",
        "spread_delta_10_pips",
        "spread_to_mean_20",
        "tick_count_log",
        "tick_count_mean_10",
        "tick_count_z_60",
        "bar_gap_seconds",
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        "weekday_sin",
        "weekday_cos",
        "is_tokyo_session",
        "is_london_session",
        "is_newyork_session",
        "is_rollover_window",
        "is_weekend_or_illiquid_window",
        "is_month_start",
        "is_month_end",
    ):
        out[column] = features[column]
    out["directional_up_tick_ratio_10"] = (
        features["up_tick_ratio_10"] if side_key == "long" else 1.0 - features["up_tick_ratio_10"]
    )
    out["directional_up_tick_ratio_30"] = (
        features["up_tick_ratio_30"] if side_key == "long" else 1.0 - features["up_tick_ratio_30"]
    )
    out["directional_signed_tick_imbalance_10"] = sign * features["signed_tick_imbalance_10"]
    out["directional_signed_tick_imbalance_30"] = sign * features["signed_tick_imbalance_30"]
    return out[SCALPING_FEATURE_COLUMNS].replace([np.inf, -np.inf], 0.0).fillna(0.0)
