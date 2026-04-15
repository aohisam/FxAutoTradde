"""Trend structure and volatility helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def slope(series: pd.Series, window: int = 20) -> pd.Series:
    idx = np.arange(window)

    def _calc(values: np.ndarray) -> float:
        if np.isnan(values).any():
            return 0.0
        coef = np.polyfit(idx, values, 1)[0]
        return float(coef)

    return series.rolling(window).apply(_calc, raw=True).fillna(0.0)


def higher_high_higher_low_score(frame: pd.DataFrame, window: int = 5) -> pd.Series:
    higher_highs = (frame["high"] > frame["high"].shift(1)).rolling(window).mean().fillna(0.0)
    higher_lows = (frame["low"] > frame["low"].shift(1)).rolling(window).mean().fillna(0.0)
    return ((higher_highs + higher_lows) / 2).clip(0.0, 1.0)


def compression_score(frame: pd.DataFrame, window: int = 20) -> pd.Series:
    range_pct = ((frame["high"] - frame["low"]) / frame["close"]).rolling(window).mean()
    baseline = range_pct.rolling(window).mean().replace(0, np.nan)
    score = 1 - (range_pct / baseline)
    return score.clip(-1.0, 1.0).fillna(0.0)
