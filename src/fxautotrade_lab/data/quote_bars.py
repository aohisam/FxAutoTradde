"""Bid/Ask quote bar helpers for FX backtests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.quality import summarize_bar_frame_quality, validate_bar_frame


QUOTE_SIDE_COLUMNS = ["open", "high", "low", "close", "volume"]
QUOTE_PRICE_COLUMNS = ["open", "high", "low", "close"]


def _normalize_quote_column_name(value: str) -> str:
    normalized = str(value).strip().lower()
    for source, target in {
        " ": "_",
        "-": "_",
        "/": "_",
        "(": "_",
        ")": "_",
        "[": "_",
        "]": "_",
    }.items():
        normalized = normalized.replace(source, target)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _has_combined_quote_columns(columns: list[str]) -> bool:
    normalized = {_normalize_quote_column_name(column) for column in columns}
    required = {
        *(f"bid_{column}" for column in QUOTE_PRICE_COLUMNS),
        *(f"ask_{column}" for column in QUOTE_PRICE_COLUMNS),
    }
    return required.issubset(normalized)


def _aligned_numeric_series(
    value: pd.Series | float | int | None,
    index: pd.Index,
    *,
    default: float = 0.0,
) -> pd.Series:
    if isinstance(value, pd.Series):
        return pd.to_numeric(value, errors="coerce").reindex(index).fillna(default)
    if value is None:
        scalar = default
    else:
        scalar = float(value)
    return pd.Series(scalar, index=index, dtype="float64")


def validate_quote_bar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required: list[str] = []
    for side in ("bid", "ask"):
        required.extend(f"{side}_{column}" for column in QUOTE_SIDE_COLUMNS)
    working = frame.copy()
    has_base_ohlc = {"open", "high", "low", "close"}.issubset(working.columns)
    missing = [column for column in required if column not in working.columns]
    if missing and has_base_ohlc:
        base_volume = _aligned_numeric_series(working.get("volume"), working.index, default=0.0)
        for price_column in ("open", "high", "low", "close"):
            numeric = pd.to_numeric(working[price_column], errors="coerce")
            working[f"bid_{price_column}"] = pd.to_numeric(working.get(f"bid_{price_column}", numeric), errors="coerce")
            working[f"ask_{price_column}"] = pd.to_numeric(working.get(f"ask_{price_column}", numeric), errors="coerce")
            working[f"mid_{price_column}"] = numeric
            working[f"spread_{price_column}"] = 0.0
        working["bid_volume"] = _aligned_numeric_series(working.get("bid_volume", base_volume), working.index, default=0.0)
        working["ask_volume"] = _aligned_numeric_series(working.get("ask_volume"), working.index, default=0.0)
        missing = [column for column in required if column not in working.columns]
    if missing:
        raise ValueError(f"Missing quote columns: {missing}")
    for column in required:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    if has_base_ohlc:
        base_volume = _aligned_numeric_series(working.get("volume"), working.index, default=0.0)
        for price_column in ("open", "high", "low", "close"):
            base_numeric = pd.to_numeric(working[price_column], errors="coerce")
            working[f"bid_{price_column}"] = working[f"bid_{price_column}"].fillna(base_numeric)
            working[f"ask_{price_column}"] = working[f"ask_{price_column}"].fillna(base_numeric)
        working["bid_volume"] = working["bid_volume"].fillna(base_volume)
        working["ask_volume"] = working["ask_volume"].fillna(0.0)
    null_rows = int(working[required].isna().any(axis=1).sum())
    if null_rows:
        raise ValueError(f"Quote columns contain {null_rows} incomplete rows.")
    for price_column in ("open", "high", "low", "close"):
        working[f"mid_{price_column}"] = (
            pd.to_numeric(working[f"bid_{price_column}"], errors="coerce")
            + pd.to_numeric(working[f"ask_{price_column}"], errors="coerce")
        ) / 2.0
        working[f"spread_{price_column}"] = (
            pd.to_numeric(working[f"ask_{price_column}"], errors="coerce")
            - pd.to_numeric(working[f"bid_{price_column}"], errors="coerce")
        )
        working[price_column] = working[f"mid_{price_column}"]
    working["volume"] = (
        pd.to_numeric(working["bid_volume"], errors="coerce").fillna(0.0)
        + pd.to_numeric(working["ask_volume"], errors="coerce").fillna(0.0)
    )
    working = validate_bar_frame(working)
    for price_column in ("open", "high", "low", "close"):
        spread = pd.to_numeric(working.get(f"spread_{price_column}"), errors="coerce")
        if (spread < 0).any():
            raise ValueError(f"Negative spread detected in quote bars ({price_column}).")
    return working


def read_jforex_quote_csv(file_path: str | Path, side: str) -> pd.DataFrame:
    side_name = side.strip().lower()
    if side_name not in {"bid", "ask"}:
        raise ValueError(f"Unsupported quote side: {side}")
    frame = pd.read_csv(
        Path(file_path),
        usecols=lambda column: str(column).strip() in {"Time (EET)", "Open", "High", "Low", "Close", "Volume"},
        dtype={
            "Open": "float64",
            "High": "float64",
            "Low": "float64",
            "Close": "float64",
            "Volume": "float64",
        },
        memory_map=True,
    )
    frame.columns = [str(column).strip() for column in frame.columns]
    timestamps = pd.to_datetime(frame.pop("Time (EET)"), format="%Y.%m.%d %H:%M:%S", errors="raise")
    frame = frame.rename(
        columns={
            "Open": f"{side_name}_open",
            "High": f"{side_name}_high",
            "Low": f"{side_name}_low",
            "Close": f"{side_name}_close",
            "Volume": f"{side_name}_volume",
        }
    )
    frame.index = (
        pd.DatetimeIndex(timestamps)
        .tz_localize("Europe/Helsinki", ambiguous="infer", nonexistent="shift_forward")
        .tz_convert(ASIA_TOKYO)
    )
    return frame.sort_index()


def read_combined_quote_csv(file_path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(Path(file_path))
    renamed = raw.rename(columns={column: _normalize_quote_column_name(column) for column in raw.columns})
    timestamp_column = next(
        (column for column in ("timestamp", "time_eet", "time", "datetime", "date_time") if column in renamed.columns),
        "",
    )
    if not timestamp_column:
        raise ValueError("結合 quote CSV に timestamp 列がありません。")
    required_prices = [
        *(f"bid_{column}" for column in QUOTE_PRICE_COLUMNS),
        *(f"ask_{column}" for column in QUOTE_PRICE_COLUMNS),
    ]
    missing = [column for column in required_prices if column not in renamed.columns]
    if missing:
        raise ValueError(f"結合 quote CSV に必要な列が不足しています: {missing}")
    timestamps = pd.to_datetime(renamed[timestamp_column], errors="raise")
    if timestamps.dt.tz is None:
        localized = pd.DatetimeIndex(timestamps).tz_localize(
            "Europe/Helsinki" if timestamp_column == "time_eet" else ASIA_TOKYO,
            ambiguous="infer",
            nonexistent="shift_forward",
        )
    else:
        localized = pd.DatetimeIndex(timestamps).tz_convert(ASIA_TOKYO)
    frame = pd.DataFrame(index=localized)
    for column in required_prices:
        frame[column] = pd.to_numeric(renamed[column], errors="coerce").to_numpy()
    for side in ("bid", "ask"):
        volume_column = f"{side}_volume"
        if volume_column in renamed.columns:
            frame[volume_column] = pd.to_numeric(renamed[volume_column], errors="coerce").fillna(0.0).to_numpy()
        else:
            frame[volume_column] = 0.0
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


def is_combined_quote_csv(file_path: str | Path) -> bool:
    sample = pd.read_csv(Path(file_path), nrows=1)
    return _has_combined_quote_columns([str(column) for column in sample.columns])


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


def summarize_quote_bar_quality(frame: pd.DataFrame) -> dict[str, object]:
    working = frame.copy()
    summary = summarize_bar_frame_quality(working)
    if "spread_close" not in working.columns:
        return {
            **summary,
            "negative_spread_rows": 0,
            "abnormal_spread_rows": 0,
            "spread_p95": 0.0,
            "spread_p99": 0.0,
            "spread_max": 0.0,
        }
    spreads = pd.to_numeric(working["spread_close"], errors="coerce")
    valid_spreads = spreads.dropna()
    if valid_spreads.empty:
        return {
            **summary,
            "negative_spread_rows": 0,
            "abnormal_spread_rows": 0,
            "spread_p95": 0.0,
            "spread_p99": 0.0,
            "spread_max": 0.0,
        }
    spread_p95 = float(valid_spreads.quantile(0.95))
    spread_p99 = float(valid_spreads.quantile(0.99))
    spread_max = float(valid_spreads.max())
    median = float(valid_spreads.median())
    abnormal_threshold = max(spread_p95 * 3.0, median * 10.0, 0.0001)
    abnormal_rows = int((valid_spreads > abnormal_threshold).sum())
    return {
        **summary,
        "negative_spread_rows": int((valid_spreads < 0).sum()),
        "abnormal_spread_rows": abnormal_rows,
        "spread_p95": spread_p95,
        "spread_p99": spread_p99,
        "spread_max": spread_max,
    }
