"""Data quality helpers."""

from __future__ import annotations

import pandas as pd


REQUIRED_BAR_COLUMNS = ["open", "high", "low", "close", "volume"]


def validate_bar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in REQUIRED_BAR_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing OHLCV columns: {missing}")
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise TypeError("Bar frame index must be a DatetimeIndex")
    if frame.index.tz is None:
        raise ValueError("Bar frame index must be timezone-aware")
    if not frame.index.is_monotonic_increasing:
        frame = frame.sort_index()
    if frame.index.has_duplicates:
        frame = frame.loc[~frame.index.duplicated(keep="last")]
    return frame
