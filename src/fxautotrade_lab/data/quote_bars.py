"""Bid/Ask quote bar helpers for FX backtests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.quality import validate_bar_frame


QUOTE_SIDE_COLUMNS = ["open", "high", "low", "close", "volume"]


def validate_quote_bar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required: list[str] = []
    for side in ("bid", "ask"):
        required.extend(f"{side}_{column}" for column in QUOTE_SIDE_COLUMNS)
    working = frame.copy()
    missing = [column for column in required if column not in working.columns]
    if missing and {"open", "high", "low", "close"}.issubset(working.columns):
        base_volume = pd.to_numeric(working.get("volume", 0.0), errors="coerce").fillna(0.0)
        for price_column in ("open", "high", "low", "close"):
            numeric = pd.to_numeric(working[price_column], errors="coerce")
            working[f"bid_{price_column}"] = numeric
            working[f"ask_{price_column}"] = numeric
            working[f"mid_{price_column}"] = numeric
            working[f"spread_{price_column}"] = 0.0
        working["bid_volume"] = base_volume
        working["ask_volume"] = 0.0
        missing = [column for column in required if column not in working.columns]
    if missing:
        raise ValueError(f"Missing quote columns: {missing}")
    working["open"] = pd.to_numeric(working.get("open", working["bid_open"]), errors="coerce")
    working["high"] = pd.to_numeric(working.get("high", working["bid_high"]), errors="coerce")
    working["low"] = pd.to_numeric(working.get("low", working["bid_low"]), errors="coerce")
    working["close"] = pd.to_numeric(working.get("close", working["bid_close"]), errors="coerce")
    working["volume"] = pd.to_numeric(working.get("volume", working["bid_volume"]), errors="coerce").fillna(0.0)
    working = validate_bar_frame(working)
    spread_close = working.get("spread_close")
    if spread_close is not None and (pd.to_numeric(spread_close, errors="coerce") < 0).any():
        raise ValueError("Negative spread detected in quote bars.")
    return working


def read_jforex_quote_csv(file_path: str | Path, side: str) -> pd.DataFrame:
    side_name = side.strip().lower()
    if side_name not in {"bid", "ask"}:
        raise ValueError(f"Unsupported quote side: {side}")
    raw = pd.read_csv(Path(file_path))
    raw.columns = [str(column).strip() for column in raw.columns]
    renamed = raw.rename(
        columns={
            "Time (EET)": "timestamp",
            "Open": f"{side_name}_open",
            "High": f"{side_name}_high",
            "Low": f"{side_name}_low",
            "Close": f"{side_name}_close",
            "Volume": f"{side_name}_volume",
        }
    )
    timestamps = pd.to_datetime(renamed["timestamp"], format="%Y.%m.%d %H:%M:%S", errors="raise")
    frame = pd.DataFrame(
        {
            f"{side_name}_open": renamed[f"{side_name}_open"].astype(float),
            f"{side_name}_high": renamed[f"{side_name}_high"].astype(float),
            f"{side_name}_low": renamed[f"{side_name}_low"].astype(float),
            f"{side_name}_close": renamed[f"{side_name}_close"].astype(float),
            f"{side_name}_volume": renamed[f"{side_name}_volume"].astype(float),
        }
    )
    frame.index = (
        pd.DatetimeIndex(timestamps)
        .tz_localize("Europe/Helsinki", ambiguous="infer", nonexistent="shift_forward")
        .tz_convert(ASIA_TOKYO)
    )
    return frame.sort_index()


def build_quote_bar_frame(bid_frame: pd.DataFrame, ask_frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    merged = bid_frame.join(ask_frame, how="inner")
    if merged.empty:
        raise ValueError("Bid/Ask の結合結果が空です。時刻軸が一致しているか確認してください。")
    for price_column in ("open", "high", "low", "close"):
        merged[f"mid_{price_column}"] = (merged[f"bid_{price_column}"] + merged[f"ask_{price_column}"]) / 2.0
        merged[f"spread_{price_column}"] = merged[f"ask_{price_column}"] - merged[f"bid_{price_column}"]
        merged[price_column] = merged[f"mid_{price_column}"]
    merged["bid_volume"] = pd.to_numeric(merged["bid_volume"], errors="coerce").fillna(0.0)
    merged["ask_volume"] = pd.to_numeric(merged["ask_volume"], errors="coerce").fillna(0.0)
    merged["volume"] = merged["bid_volume"] + merged["ask_volume"]
    merged["symbol"] = symbol.upper()
    return validate_quote_bar_frame(merged)


def resample_quote_bars(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    working = validate_quote_bar_frame(frame)
    aggregated = working.resample(rule, label="right", closed="right").agg(
        {
            "bid_open": "first",
            "bid_high": "max",
            "bid_low": "min",
            "bid_close": "last",
            "bid_volume": "sum",
            "ask_open": "first",
            "ask_high": "max",
            "ask_low": "min",
            "ask_close": "last",
            "ask_volume": "sum",
        }
    )
    aggregated = aggregated.dropna(subset=["bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close"])
    symbol = str(working["symbol"].iloc[0]) if "symbol" in working.columns and not working.empty else ""
    return build_quote_bar_frame(
        aggregated[["bid_open", "bid_high", "bid_low", "bid_close", "bid_volume"]],
        aggregated[["ask_open", "ask_high", "ask_low", "ask_close", "ask_volume"]],
        symbol=symbol,
    )


def quote_spread_summary(frame: pd.DataFrame) -> dict[str, float]:
    working = validate_quote_bar_frame(frame)
    spreads = pd.to_numeric(working["spread_close"], errors="coerce").dropna()
    if spreads.empty:
        return {"spread_p95": 0.0, "spread_p99": 0.0, "spread_max": 0.0}
    return {
        "spread_p95": float(spreads.quantile(0.95)),
        "spread_p99": float(spreads.quantile(0.99)),
        "spread_max": float(spreads.max()),
    }
