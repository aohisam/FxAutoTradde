"""JForex CSV import helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.core.symbols import infer_fx_symbol_from_filename, normalize_fx_symbol
from fxautotrade_lab.data.cache import ParquetBarCache
from fxautotrade_lab.data.quality import validate_bar_frame
from fxautotrade_lab.data.quote_bars import (
    build_quote_bar_frame,
    quote_spread_summary,
    read_jforex_quote_csv,
    resample_quote_bars,
)
from fxautotrade_lab.data.resample import resample_ohlcv


TIMEFRAME_RULES: dict[TimeFrame, str] = {
    TimeFrame.MIN_1: "1min",
    TimeFrame.MIN_5: "5min",
    TimeFrame.MIN_10: "10min",
    TimeFrame.MIN_15: "15min",
    TimeFrame.MIN_30: "30min",
    TimeFrame.HOUR_1: "1h",
    TimeFrame.HOUR_4: "4h",
    TimeFrame.HOUR_8: "8h",
    TimeFrame.HOUR_12: "12h",
    TimeFrame.DAY_1: "1D",
    TimeFrame.WEEK_1: "1W",
    TimeFrame.MONTH_1: "1ME",
}


@dataclass(slots=True)
class JForexImportResult:
    symbol: str
    imported_rows: int
    source_path: Path
    cache_paths: dict[str, str]
    start: str
    end: str


@dataclass(slots=True)
class JForexBidAskImportResult:
    symbol: str
    imported_rows: int
    bid_source_path: Path
    ask_source_path: Path
    cache_paths: dict[str, str]
    start: str
    end: str


class JForexCsvImporter:
    def __init__(self, cache: ParquetBarCache) -> None:
        self.cache = cache

    def import_file(self, file_path: str | Path, symbol: str | None = None) -> JForexImportResult:
        source_path = Path(file_path)
        normalized_symbol = normalize_fx_symbol(symbol) if symbol else infer_fx_symbol_from_filename(source_path)
        raw = pd.read_csv(source_path)
        raw.columns = [str(column).strip() for column in raw.columns]
        renamed = raw.rename(
            columns={
                "Time (EET)": "timestamp",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )
        frame = pd.DataFrame(
            {
                "open": renamed["open"].astype(float),
                "high": renamed["high"].astype(float),
                "low": renamed["low"].astype(float),
                "close": renamed["close"].astype(float),
                "volume": renamed["volume"].astype(float),
            }
        )
        timestamps = pd.to_datetime(renamed["timestamp"], format="%Y.%m.%d %H:%M:%S", errors="raise")
        frame.index = (
            pd.DatetimeIndex(timestamps)
            .tz_localize("Europe/Helsinki", ambiguous="infer", nonexistent="shift_forward")
            .tz_convert(ASIA_TOKYO)
        )
        frame["symbol"] = normalized_symbol
        base = validate_bar_frame(frame)
        cache_paths: dict[str, str] = {}
        for timeframe, rule in TIMEFRAME_RULES.items():
            derived = base.copy() if timeframe == TimeFrame.MIN_1 else resample_ohlcv(base, rule)
            if derived.empty:
                continue
            derived["symbol"] = normalized_symbol
            path = self.cache.save(normalized_symbol, timeframe, derived)
            self.cache.save_metadata(
                normalized_symbol,
                timeframe,
                {
                    "source": "fx_cache",
                    "timeframe": timeframe.value,
                    "symbol": normalized_symbol,
                    "version": 1,
                },
            )
            cache_paths[timeframe.value] = str(path)
        return JForexImportResult(
            symbol=normalized_symbol,
            imported_rows=len(base.index),
            source_path=source_path,
            cache_paths=cache_paths,
            start=base.index.min().isoformat(),
            end=base.index.max().isoformat(),
        )

    def import_bid_ask_files(
        self,
        bid_file_path: str | Path,
        ask_file_path: str | Path,
        symbol: str | None = None,
    ) -> JForexBidAskImportResult:
        bid_source_path = Path(bid_file_path)
        ask_source_path = Path(ask_file_path)
        normalized_symbol = normalize_fx_symbol(symbol) if symbol else infer_fx_symbol_from_filename(bid_source_path)
        bid_symbol = infer_fx_symbol_from_filename(bid_source_path)
        ask_symbol = infer_fx_symbol_from_filename(ask_source_path)
        if bid_symbol != ask_symbol and symbol is None:
            raise ValueError("Bid/Ask CSV の通貨ペア名が一致しません。")
        bid_frame = read_jforex_quote_csv(bid_source_path, "bid")
        ask_frame = read_jforex_quote_csv(ask_source_path, "ask")
        base = build_quote_bar_frame(bid_frame, ask_frame, normalized_symbol)
        spread_stats = quote_spread_summary(base)
        cache_paths: dict[str, str] = {}
        for timeframe, rule in TIMEFRAME_RULES.items():
            derived = base.copy() if timeframe == TimeFrame.MIN_1 else resample_quote_bars(base, rule)
            if derived.empty:
                continue
            derived["symbol"] = normalized_symbol
            path = self.cache.save(normalized_symbol, timeframe, derived)
            self.cache.save_metadata(
                normalized_symbol,
                timeframe,
                {
                    "source": "fx_cache_bid_ask",
                    "timeframe": timeframe.value,
                    "symbol": normalized_symbol,
                    "version": 2,
                    "bid_source_path": str(bid_source_path),
                    "ask_source_path": str(ask_source_path),
                    **spread_stats,
                },
            )
            cache_paths[timeframe.value] = str(path)
        return JForexBidAskImportResult(
            symbol=normalized_symbol,
            imported_rows=len(base.index),
            bid_source_path=bid_source_path,
            ask_source_path=ask_source_path,
            cache_paths=cache_paths,
            start=base.index.min().isoformat(),
            end=base.index.max().isoformat(),
        )
