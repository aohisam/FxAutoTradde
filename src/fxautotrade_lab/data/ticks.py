"""Tick-level FX data import, cache, and resampling helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO, UTC
from fxautotrade_lab.core.symbols import infer_fx_symbol_from_filename, normalize_fx_symbol
from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame

TICK_PRICE_COLUMNS = ("bid", "ask")
TICK_OPTIONAL_COLUMNS = ("bid_volume", "ask_volume")
TICK_COLUMNS = ("bid", "ask", "bid_volume", "ask_volume", "mid", "spread")

_TIMESTAMP_CANDIDATES = (
    "time_eet",
    "time_utc",
    "local_time",
    "timestamp",
    "date_time",
    "datetime",
    "time",
)
_BID_CANDIDATES = ("bid", "bid_price", "bidprice")
_ASK_CANDIDATES = ("ask", "ask_price", "askprice")
_BID_VOLUME_CANDIDATES = ("bid_volume", "bidvolume", "bid_vol", "bidvol")
_ASK_VOLUME_CANDIDATES = ("ask_volume", "askvolume", "ask_vol", "askvol")


@dataclass(slots=True)
class TickQualitySummary:
    source_rows: int
    imported_rows: int
    dropped_rows: int
    duplicate_timestamps: int
    crossed_quotes: int
    start: str = ""
    end: str = ""
    messages: list[str] = field(default_factory=list)


@dataclass(slots=True)
class TickImportResult:
    symbol: str
    source_path: Path
    imported_rows: int
    dropped_rows: int
    duplicate_timestamps: int
    crossed_quotes: int
    start: str
    end: str
    cache_paths: list[str]
    messages: list[str] = field(default_factory=list)


def normalize_tick_column_name(value: object) -> str:
    normalized = str(value).strip().lower()
    for source, target in {
        " ": "_",
        "-": "_",
        "/": "_",
        "(": "_",
        ")": "_",
        "[": "_",
        "]": "_",
        ".": "_",
    }.items():
        normalized = normalized.replace(source, target)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _first_column(columns: list[str], candidates: tuple[str, ...], *, label_ja: str) -> str:
    normalized = {normalize_tick_column_name(column): column for column in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    raise ValueError(f"JForex tick CSV に {label_ja} 列がありません。候補: {list(candidates)}")


def _timestamp_timezone(column_name: str, default_tz: str) -> ZoneInfo:
    normalized = normalize_tick_column_name(column_name)
    if "utc" in normalized:
        return UTC
    if "eet" in normalized:
        return ZoneInfo("Europe/Helsinki")
    return ZoneInfo(default_tz)


def _to_tokyo_index(values: pd.Series, column_name: str, default_tz: str) -> pd.DatetimeIndex:
    cleaned = values.astype("string").str.strip()
    cleaned = cleaned.str.replace(" GMT", "", regex=False).str.replace(" UTC", "", regex=False)
    parsed = pd.to_datetime(cleaned, errors="coerce")
    index = pd.DatetimeIndex(parsed)
    if index.tz is not None:
        return index.tz_convert(ASIA_TOKYO)
    source_tz = _timestamp_timezone(column_name, default_tz)
    try:
        return index.tz_localize(
            source_tz, ambiguous="infer", nonexistent="shift_forward"
        ).tz_convert(ASIA_TOKYO)
    except Exception:
        return index.tz_localize(
            source_tz, ambiguous=False, nonexistent="shift_forward"
        ).tz_convert(ASIA_TOKYO)


def _numeric_or_default(raw: pd.DataFrame, column: str | None, default: float) -> pd.Series:
    if column and column in raw.columns:
        return pd.to_numeric(raw[column], errors="coerce")
    return pd.Series(default, index=raw.index, dtype="float64")


def normalize_tick_frame(
    raw: pd.DataFrame,
    symbol: str,
    *,
    default_tz: str = "Europe/Helsinki",
) -> tuple[pd.DataFrame, TickQualitySummary]:
    """Normalize a raw tick frame into bid/ask ticks indexed by Tokyo time."""

    normalized_symbol = normalize_fx_symbol(symbol)
    if raw.empty:
        return (
            pd.DataFrame(columns=[*TICK_COLUMNS, "symbol"]),
            TickQualitySummary(
                source_rows=0,
                imported_rows=0,
                dropped_rows=0,
                duplicate_timestamps=0,
                crossed_quotes=0,
            ),
        )

    source_rows = int(len(raw.index))
    columns = list(raw.columns)
    timestamp_column = _first_column(columns, _TIMESTAMP_CANDIDATES, label_ja="時刻")
    bid_column = _first_column(columns, _BID_CANDIDATES, label_ja="Bid")
    ask_column = _first_column(columns, _ASK_CANDIDATES, label_ja="Ask")
    renamed = raw.rename(
        columns={column: normalize_tick_column_name(column) for column in raw.columns}
    )
    timestamp_key = normalize_tick_column_name(timestamp_column)
    bid_key = normalize_tick_column_name(bid_column)
    ask_key = normalize_tick_column_name(ask_column)

    bid_volume_key = next((key for key in _BID_VOLUME_CANDIDATES if key in renamed.columns), None)
    ask_volume_key = next((key for key in _ASK_VOLUME_CANDIDATES if key in renamed.columns), None)

    frame = pd.DataFrame(
        index=_to_tokyo_index(renamed[timestamp_key], timestamp_column, default_tz)
    )
    frame["bid"] = pd.to_numeric(renamed[bid_key], errors="coerce").to_numpy()
    frame["ask"] = pd.to_numeric(renamed[ask_key], errors="coerce").to_numpy()
    frame["bid_volume"] = _numeric_or_default(renamed, bid_volume_key, 0.0).to_numpy()
    frame["ask_volume"] = _numeric_or_default(renamed, ask_volume_key, 0.0).to_numpy()
    frame["symbol"] = normalized_symbol

    valid = frame.index.notna() & frame["bid"].gt(0) & frame["ask"].gt(0)
    frame = frame.loc[valid].copy()
    crossed_quotes = int((frame["ask"] < frame["bid"]).sum())
    if crossed_quotes:
        frame = frame.loc[frame["ask"] >= frame["bid"]].copy()
    frame = frame.sort_index()
    duplicate_timestamps = int(frame.index.duplicated(keep="last").sum())
    if duplicate_timestamps:
        frame = frame.loc[~frame.index.duplicated(keep="last")].copy()
    frame["mid"] = (frame["bid"] + frame["ask"]) / 2.0
    frame["spread"] = frame["ask"] - frame["bid"]
    frame = frame[[*TICK_COLUMNS, "symbol"]]

    imported_rows = int(len(frame.index))
    dropped_rows = source_rows - imported_rows
    summary = TickQualitySummary(
        source_rows=source_rows,
        imported_rows=imported_rows,
        dropped_rows=dropped_rows,
        duplicate_timestamps=duplicate_timestamps,
        crossed_quotes=crossed_quotes,
        start=frame.index.min().isoformat() if not frame.empty else "",
        end=frame.index.max().isoformat() if not frame.empty else "",
    )
    if crossed_quotes:
        summary.messages.append(f"Ask が Bid を下回る tick を {crossed_quotes:,} 行除外しました。")
    if duplicate_timestamps:
        summary.messages.append(
            f"同一 timestamp の tick を {duplicate_timestamps:,} 行、後勝ちで統合しました。"
        )
    return frame, summary


def read_jforex_tick_csv(
    file_path: str | Path,
    *,
    symbol: str | None = None,
    default_tz: str = "Europe/Helsinki",
) -> tuple[pd.DataFrame, TickQualitySummary]:
    source_path = Path(file_path)
    normalized_symbol = (
        normalize_fx_symbol(symbol) if symbol else infer_fx_symbol_from_filename(source_path)
    )
    raw = pd.read_csv(source_path, memory_map=True)
    return normalize_tick_frame(raw, normalized_symbol, default_tz=default_tz)


def validate_tick_frame(frame: pd.DataFrame, *, symbol: str | None = None) -> pd.DataFrame:
    if frame.empty:
        columns = [*TICK_COLUMNS, "symbol"]
        return pd.DataFrame(columns=columns)
    missing = [column for column in TICK_PRICE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"tick frame に必要な列が不足しています: {missing}")
    working = frame.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        if "timestamp" not in working.columns:
            raise ValueError("tick frame には DatetimeIndex または timestamp 列が必要です。")
        working.index = pd.DatetimeIndex(pd.to_datetime(working.pop("timestamp"), errors="raise"))
    if working.index.tz is None:
        working.index = working.index.tz_localize(ASIA_TOKYO)
    else:
        working.index = working.index.tz_convert(ASIA_TOKYO)
    for column in ("bid", "ask", "bid_volume", "ask_volume"):
        working[column] = pd.to_numeric(working.get(column, 0.0), errors="coerce").fillna(0.0)
    working = working.loc[
        working["bid"].gt(0) & working["ask"].gt(0) & working["ask"].ge(working["bid"])
    ].copy()
    working = working.sort_index()
    working = working.loc[~working.index.duplicated(keep="last")].copy()
    working["mid"] = (working["bid"] + working["ask"]) / 2.0
    working["spread"] = working["ask"] - working["bid"]
    if symbol is not None:
        working["symbol"] = normalize_fx_symbol(symbol)
    elif "symbol" not in working.columns:
        working["symbol"] = ""
    return working[[*TICK_COLUMNS, "symbol"]]


class ParquetTickCache:
    """Stores tick data by symbol/day to avoid loading full histories."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _symbol_dir(self, symbol: str) -> Path:
        return self.cache_dir / "ticks" / normalize_fx_symbol(symbol).upper()

    def path_for_day(self, symbol: str, day: pd.Timestamp) -> Path:
        ts = _ensure_tokyo_timestamp(day)
        return (
            self._symbol_dir(symbol)
            / f"{ts.year:04d}"
            / f"{ts.month:02d}"
            / f"{ts.date().isoformat()}.parquet"
        )

    def save(self, symbol: str, frame: pd.DataFrame) -> list[Path]:
        ticks = validate_tick_frame(frame, symbol=symbol)
        if ticks.empty:
            return []
        written: list[Path] = []
        for day, day_frame in ticks.groupby(ticks.index.normalize()):
            path = self.path_for_day(symbol, pd.Timestamp(day))
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = day_frame.drop(columns=["symbol"], errors="ignore").copy()
            payload["timestamp"] = day_frame.index
            payload.to_parquet(path, index=False)
            written.append(path)
        return written

    def upsert(self, symbol: str, frame: pd.DataFrame) -> list[Path]:
        ticks = validate_tick_frame(frame, symbol=symbol)
        if ticks.empty:
            return []
        written: list[Path] = []
        for day, day_frame in ticks.groupby(ticks.index.normalize()):
            path = self.path_for_day(symbol, pd.Timestamp(day))
            if path.exists():
                existing = self._load_path(path, symbol)
                merged = pd.concat([existing, day_frame]).sort_index()
                merged = merged.loc[~merged.index.duplicated(keep="last")].copy()
            else:
                merged = day_frame
            written.extend(self.save(symbol, merged))
        return written

    def load_window(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        normalized_symbol = normalize_fx_symbol(symbol)
        start_ts = _ensure_tokyo_timestamp(start)
        end_ts = _ensure_tokyo_timestamp(end)
        if start_ts >= end_ts:
            return validate_tick_frame(pd.DataFrame(), symbol=normalized_symbol)
        frames: list[pd.DataFrame] = []
        for day in pd.date_range(start_ts.normalize(), end_ts.normalize(), freq="D", tz=ASIA_TOKYO):
            path = self.path_for_day(normalized_symbol, pd.Timestamp(day))
            if path.exists():
                frames.append(self._load_path(path, normalized_symbol))
        if not frames:
            return validate_tick_frame(pd.DataFrame(), symbol=normalized_symbol)
        merged = pd.concat(frames).sort_index()
        merged = merged.loc[(merged.index >= start_ts) & (merged.index < end_ts)].copy()
        return validate_tick_frame(merged, symbol=normalized_symbol)

    def _load_path(self, path: Path, symbol: str) -> pd.DataFrame:
        frame = pd.read_parquet(path)
        if "timestamp" not in frame.columns:
            raise ValueError(f"tick cache に timestamp 列がありません: {path}")
        frame.index = pd.DatetimeIndex(pd.to_datetime(frame.pop("timestamp"), errors="raise"))
        return validate_tick_frame(frame, symbol=symbol)


class JForexTickCsvImporter:
    def __init__(self, cache: ParquetTickCache) -> None:
        self.cache = cache

    def import_file(
        self,
        file_path: str | Path,
        *,
        symbol: str | None = None,
        default_tz: str = "Europe/Helsinki",
        chunk_size: int = 500_000,
    ) -> TickImportResult:
        source_path = Path(file_path)
        normalized_symbol = (
            normalize_fx_symbol(symbol) if symbol else infer_fx_symbol_from_filename(source_path)
        )
        imported_rows = 0
        dropped_rows = 0
        duplicate_timestamps = 0
        crossed_quotes = 0
        cache_paths: set[str] = set()
        start: pd.Timestamp | None = None
        end: pd.Timestamp | None = None
        messages: list[str] = []
        for raw_chunk in pd.read_csv(source_path, chunksize=chunk_size):
            ticks, quality = normalize_tick_frame(
                raw_chunk, normalized_symbol, default_tz=default_tz
            )
            imported_rows += quality.imported_rows
            dropped_rows += quality.dropped_rows
            duplicate_timestamps += quality.duplicate_timestamps
            crossed_quotes += quality.crossed_quotes
            messages.extend(quality.messages)
            if ticks.empty:
                continue
            start = ticks.index.min() if start is None else min(start, ticks.index.min())
            end = ticks.index.max() if end is None else max(end, ticks.index.max())
            for path in self.cache.upsert(normalized_symbol, ticks):
                cache_paths.add(str(path))
        unique_messages = list(dict.fromkeys(messages))
        return TickImportResult(
            symbol=normalized_symbol,
            source_path=source_path,
            imported_rows=imported_rows,
            dropped_rows=dropped_rows,
            duplicate_timestamps=duplicate_timestamps,
            crossed_quotes=crossed_quotes,
            start=start.isoformat() if start is not None else "",
            end=end.isoformat() if end is not None else "",
            cache_paths=sorted(cache_paths),
            messages=unique_messages,
        )


def resample_ticks_to_quote_bars(
    ticks: pd.DataFrame,
    *,
    rule: str = "1s",
    symbol: str | None = None,
) -> pd.DataFrame:
    validated = validate_tick_frame(ticks, symbol=symbol)
    if validated.empty:
        return pd.DataFrame()
    aggregated = validated.resample(rule, label="right", closed="right").agg(
        {
            "bid": ["first", "max", "min", "last"],
            "ask": ["first", "max", "min", "last"],
            "bid_volume": "sum",
            "ask_volume": "sum",
            "mid": ["first", "max", "min", "last"],
            "spread": ["first", "max", "min", "last", "mean"],
        }
    )
    aggregated.columns = [
        "_".join(str(part) for part in column if part)
        .replace("first", "open")
        .replace("last", "close")
        for column in aggregated.columns
    ]
    aggregated["tick_count"] = validated["bid"].resample(rule, label="right", closed="right").size()
    rename_map = {
        "bid_open": "bid_open",
        "bid_max": "bid_high",
        "bid_min": "bid_low",
        "bid_close": "bid_close",
        "ask_open": "ask_open",
        "ask_max": "ask_high",
        "ask_min": "ask_low",
        "ask_close": "ask_close",
        "mid_open": "open",
        "mid_max": "high",
        "mid_min": "low",
        "mid_close": "close",
        "spread_open": "spread_open",
        "spread_max": "spread_high",
        "spread_min": "spread_low",
        "spread_close": "spread_close",
        "spread_mean": "spread_mean",
    }
    aggregated = aggregated.rename(columns=rename_map)
    aggregated = aggregated.dropna(
        subset=[
            "bid_open",
            "bid_high",
            "bid_low",
            "bid_close",
            "ask_open",
            "ask_high",
            "ask_low",
            "ask_close",
        ]
    )
    if aggregated.empty:
        return pd.DataFrame()
    aggregated["volume"] = aggregated["bid_volume_sum"].fillna(0.0) + aggregated[
        "ask_volume_sum"
    ].fillna(0.0)
    aggregated["bid_volume"] = aggregated["bid_volume_sum"].fillna(0.0)
    aggregated["ask_volume"] = aggregated["ask_volume_sum"].fillna(0.0)
    aggregated["mid_open"] = aggregated["open"]
    aggregated["mid_high"] = aggregated["high"]
    aggregated["mid_low"] = aggregated["low"]
    aggregated["mid_close"] = aggregated["close"]
    aggregated = aggregated.drop(columns=["bid_volume_sum", "ask_volume_sum"], errors="ignore")
    if symbol is not None:
        aggregated["symbol"] = normalize_fx_symbol(symbol)
    return validate_quote_bar_frame(aggregated)


def _ensure_tokyo_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(ASIA_TOKYO)
    return ts.tz_convert(ASIA_TOKYO)
