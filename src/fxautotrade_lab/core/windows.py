"""Shared helpers for month/day/hour style time window strings."""

from __future__ import annotations

import pandas as pd


def offset_for_window(window: str) -> pd.DateOffset:
    normalized = window.strip().lower()
    if len(normalized) < 2:
        raise ValueError(f"未対応の期間指定です: {window}")
    count = int(normalized[:-1])
    unit = normalized[-1]
    if unit == "y":
        return pd.DateOffset(years=count)
    if unit == "m":
        return pd.DateOffset(months=count)
    if unit == "w":
        return pd.DateOffset(weeks=count)
    if unit == "d":
        return pd.DateOffset(days=count)
    if unit == "h":
        return pd.DateOffset(hours=count)
    raise ValueError(f"未対応の期間指定です: {window}")


def shift_timestamp(timestamp: pd.Timestamp, window: str, *, backward: bool) -> pd.Timestamp:
    offset = offset_for_window(window)
    return timestamp - offset if backward else timestamp + offset
