"""Compact candle/price action feature library."""

from __future__ import annotations

import pandas as pd


def candle_body(frame: pd.DataFrame) -> pd.Series:
    return (frame["close"] - frame["open"]).abs()


def candle_range(frame: pd.DataFrame) -> pd.Series:
    return (frame["high"] - frame["low"]).replace(0, pd.NA)


def bullish_engulfing(frame: pd.DataFrame) -> pd.Series:
    prev_open = frame["open"].shift(1)
    prev_close = frame["close"].shift(1)
    prev_bearish = prev_close < prev_open
    curr_bullish = frame["close"] > frame["open"]
    engulf = (frame["close"] >= prev_open) & (frame["open"] <= prev_close)
    return (prev_bearish & curr_bullish & engulf).astype(float)


def hammer(frame: pd.DataFrame) -> pd.Series:
    body = candle_body(frame)
    rng = candle_range(frame)
    lower_shadow = frame[["open", "close"]].min(axis=1) - frame["low"]
    upper_shadow = frame["high"] - frame[["open", "close"]].max(axis=1)
    return (
        (lower_shadow >= body * 2.2) & (upper_shadow <= body * 0.8) & (body / rng < 0.45)
    ).astype(float)


def inside_bar(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["high"] < frame["high"].shift(1)) & (frame["low"] > frame["low"].shift(1))
    ).astype(float)


def inside_bar_breakout(frame: pd.DataFrame) -> pd.Series:
    return ((inside_bar(frame).shift(1) > 0) & (frame["close"] > frame["high"].shift(1))).astype(
        float
    )


def doji(frame: pd.DataFrame) -> pd.Series:
    body = candle_body(frame)
    rng = candle_range(frame)
    return ((body / rng.fillna(1.0)) < 0.1).astype(float)


def gap_pct(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    return ((frame["open"] - prev_close) / prev_close).fillna(0.0)


def gap_continuation(frame: pd.DataFrame) -> pd.Series:
    gap = gap_pct(frame)
    return ((gap > 0.01) & (frame["close"] > frame["open"])).astype(float)


def gap_exhaustion(frame: pd.DataFrame) -> pd.Series:
    gap = gap_pct(frame)
    return ((gap > 0.02) & (frame["close"] < frame["open"])).astype(float)
