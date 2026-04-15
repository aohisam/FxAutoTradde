"""Timeframe resampling helpers."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.data.quality import validate_bar_frame


def resample_ohlcv(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    frame = validate_bar_frame(frame)
    aggregated = frame.resample(
        rule,
        label="right",
        closed="right",
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    aggregated = aggregated.dropna(subset=["open", "high", "low", "close"])
    return validate_bar_frame(aggregated)
