"""Data quality helpers."""

from __future__ import annotations

import pandas as pd


REQUIRED_BAR_COLUMNS = ["open", "high", "low", "close", "volume"]


def repair_ohlc_relationships(
    frame: pd.DataFrame,
    *,
    open_column: str = "open",
    high_column: str = "high",
    low_column: str = "low",
    close_column: str = "close",
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    working = frame.copy()
    columns = [open_column, high_column, low_column, close_column]
    for column in columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    required_high = working[columns].max(axis=1)
    required_low = working[columns].min(axis=1)
    high_adjustment = (required_high - working[high_column]).clip(lower=0.0)
    low_adjustment = (working[low_column] - required_low).clip(lower=0.0)
    high_rows = high_adjustment > 0.0
    low_rows = low_adjustment > 0.0
    if high_rows.any():
        working.loc[high_rows, high_column] = required_high.loc[high_rows]
    if low_rows.any():
        working.loc[low_rows, low_column] = required_low.loc[low_rows]
    adjusted_rows = high_rows | low_rows
    return working, {
        "adjusted_rows": int(adjusted_rows.sum()),
        "adjusted_high_rows": int(high_rows.sum()),
        "adjusted_low_rows": int(low_rows.sum()),
        "max_high_adjustment": float(high_adjustment.max()) if len(high_adjustment.index) else 0.0,
        "max_low_adjustment": float(low_adjustment.max()) if len(low_adjustment.index) else 0.0,
    }


def validate_bar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    missing = [column for column in REQUIRED_BAR_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing OHLCV columns: {missing}")
    if not isinstance(working.index, pd.DatetimeIndex):
        raise TypeError("Bar frame index must be a DatetimeIndex")
    if working.index.tz is None:
        raise ValueError("Bar frame index must be timezone-aware")
    if not working.index.is_monotonic_increasing:
        working = working.sort_index()
    if working.index.has_duplicates:
        working = working.loc[~working.index.duplicated(keep="last")]
    for column in REQUIRED_BAR_COLUMNS:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    missing_rows = int(working[REQUIRED_BAR_COLUMNS].isna().any(axis=1).sum())
    if missing_rows:
        raise ValueError(f"OHLCV columns contain {missing_rows} incomplete rows.")
    if ((working["high"] < working["low"]) | (working["high"] < working["open"]) | (working["high"] < working["close"])).any():
        raise ValueError("Invalid OHLC relationship detected: high is below open/close/low.")
    if ((working["low"] > working["high"]) | (working["low"] > working["open"]) | (working["low"] > working["close"])).any():
        raise ValueError("Invalid OHLC relationship detected: low is above open/close/high.")
    return working


def summarize_bar_frame_quality(frame: pd.DataFrame) -> dict[str, object]:
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise TypeError("Bar frame index must be a DatetimeIndex")
    incomplete_rows = 0
    available_columns = [column for column in REQUIRED_BAR_COLUMNS if column in frame.columns]
    if available_columns:
        incomplete_rows = int(frame[available_columns].isna().any(axis=1).sum())
    return {
        "timezone": str(frame.index.tz) if frame.index.tz is not None else "",
        "timezone_aware": bool(frame.index.tz is not None),
        "monotonic": bool(frame.index.is_monotonic_increasing),
        "duplicate_timestamps": int(frame.index.duplicated().sum()),
        "incomplete_rows": incomplete_rows,
    }
