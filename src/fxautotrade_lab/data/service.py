"""Unified market data service for fixture, imported CSV, and GMO FX feeds."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, TimeFrame
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.data.cache import ParquetBarCache
from fxautotrade_lab.data.fixture import FixtureDataLoader
from fxautotrade_lab.data.gmo import GmoForexPublicClient
from fxautotrade_lab.data.quality import validate_bar_frame


SUPPORTED_DIRECT_TIMEFRAMES = set(TimeFrame)

RUNTIME_REFRESH_LOOKBACK: dict[TimeFrame, pd.Timedelta] = {
    TimeFrame.MIN_1: pd.Timedelta(days=2),
    TimeFrame.MIN_5: pd.Timedelta(days=3),
    TimeFrame.MIN_10: pd.Timedelta(days=4),
    TimeFrame.MIN_15: pd.Timedelta(days=5),
    TimeFrame.MIN_30: pd.Timedelta(days=8),
    TimeFrame.HOUR_1: pd.Timedelta(days=15),
    TimeFrame.HOUR_4: pd.Timedelta(days=40),
    TimeFrame.HOUR_8: pd.Timedelta(days=60),
    TimeFrame.HOUR_12: pd.Timedelta(days=90),
    TimeFrame.DAY_1: pd.Timedelta(days=180),
    TimeFrame.WEEK_1: pd.Timedelta(days=365),
    TimeFrame.MONTH_1: pd.Timedelta(days=365 * 2),
}

GMO_INTRADAY_START = pd.Timestamp("2023-10-28", tz=ASIA_TOKYO)


@dataclass(slots=True)
class MarketDataBundle:
    symbols: dict[str, dict[TimeFrame, pd.DataFrame]]
    benchmarks: dict[str, dict[TimeFrame, pd.DataFrame]]
    sectors: dict[str, dict[TimeFrame, pd.DataFrame]]


@dataclass(slots=True)
class MarketDataFrameLoad:
    frame: pd.DataFrame
    source: str
    cache_path: Path
    refreshed: bool


class MarketDataService:
    """Load and cache market data from fixture, imported CSV, or GMO public API."""

    def __init__(self, config: AppConfig, env: EnvironmentConfig | None = None) -> None:
        self.config = config
        self.env = env or EnvironmentConfig()
        self.cache = ParquetBarCache(config.data.cache_dir)
        self.fixture = FixtureDataLoader(config.data, self.cache)
        self.gmo = GmoForexPublicClient(self.env)

    def load_symbol_frames(
        self,
        symbol: str,
        timeframes: list[TimeFrame] | None = None,
        start: str | None = None,
        end: str | None = None,
        force_refresh: bool = False,
    ) -> dict[TimeFrame, pd.DataFrame]:
        results = self._load_symbol_frame_results(
            symbol,
            timeframes=timeframes,
            start=start,
            end=end,
            force_refresh=force_refresh,
        )
        return {timeframe: result.frame for timeframe, result in results.items()}

    def load_bundle(
        self,
        force_refresh: bool = False,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> MarketDataBundle:
        symbols = {
            symbol: self.load_symbol_frames(symbol, start=start, end=end, force_refresh=force_refresh)
            for symbol in self.config.watchlist.symbols
        }
        benchmarks = {
            symbol: self.load_symbol_frames(symbol, start=start, end=end, force_refresh=force_refresh)
            for symbol in self.config.watchlist.benchmark_symbols
        }
        sectors = {
            symbol: self.load_symbol_frames(symbol, start=start, end=end, force_refresh=force_refresh)
            for symbol in self.config.watchlist.sector_symbols
        }
        return MarketDataBundle(symbols=symbols, benchmarks=benchmarks, sectors=sectors)

    def load_runtime_symbol_frames(
        self,
        symbol: str,
        timeframes: list[TimeFrame] | None = None,
        as_of: pd.Timestamp | None = None,
    ) -> dict[TimeFrame, pd.DataFrame]:
        runtime_as_of = as_of or pd.Timestamp.now(tz=ASIA_TOKYO)
        results = self._load_symbol_frame_results(
            symbol,
            timeframes=timeframes,
            end=runtime_as_of.isoformat(),
            runtime_refresh=self._uses_gmo(),
        )
        return {
            timeframe: self._trim_for_runtime(result.frame)
            for timeframe, result in results.items()
        }

    def load_runtime_bundle(self, as_of: pd.Timestamp | None = None) -> MarketDataBundle:
        runtime_as_of = as_of or pd.Timestamp.now(tz=ASIA_TOKYO)
        symbols = {
            symbol: self.load_runtime_symbol_frames(symbol, as_of=runtime_as_of)
            for symbol in self.config.watchlist.symbols
        }
        benchmarks = {
            symbol: self.load_runtime_symbol_frames(symbol, as_of=runtime_as_of)
            for symbol in self.config.watchlist.benchmark_symbols
        }
        sectors = {
            symbol: self.load_runtime_symbol_frames(symbol, as_of=runtime_as_of)
            for symbol in self.config.watchlist.sector_symbols
        }
        return MarketDataBundle(symbols=symbols, benchmarks=benchmarks, sectors=sectors)

    def sync(self) -> dict[str, object]:
        force_refresh = self._uses_gmo()
        sync_mode = "incremental" if self._uses_gmo() else self.config.data.source
        symbols, symbol_details = self._sync_group("watchlist", self.config.watchlist.symbols, force_refresh)
        benchmarks, benchmark_details = self._sync_group(
            "benchmark",
            self.config.watchlist.benchmark_symbols,
            force_refresh,
        )
        sectors, sector_details = self._sync_group("sector", self.config.watchlist.sector_symbols, force_refresh)
        return {
            "symbols": len(symbols),
            "benchmarks": len(benchmarks),
            "sectors": len(sectors),
            "source": self.config.data.source,
            "force_refresh": force_refresh,
            "sync_mode": sync_mode,
            "start_date": self.config.data.start_date,
            "end_date": self.config.data.end_date,
            "timeframes": [timeframe.value for timeframe in self._normalized_timeframes(None)],
            "details": [*symbol_details, *benchmark_details, *sector_details],
        }

    def _sync_group(
        self,
        category: str,
        symbols: list[str],
        force_refresh: bool,
    ) -> tuple[dict[str, dict[TimeFrame, pd.DataFrame]], list[dict[str, object]]]:
        frames_by_symbol: dict[str, dict[TimeFrame, pd.DataFrame]] = {}
        details: list[dict[str, object]] = []
        for symbol in symbols:
            results = self._load_symbol_frame_results(symbol, force_refresh=force_refresh)
            frames_by_symbol[symbol] = {timeframe: result.frame for timeframe, result in results.items()}
            details.extend(self._build_sync_details(category, symbol, results))
        return frames_by_symbol, details

    def _build_sync_details(
        self,
        category: str,
        symbol: str,
        results: dict[TimeFrame, MarketDataFrameLoad],
    ) -> list[dict[str, object]]:
        details: list[dict[str, object]] = []
        for timeframe, result in results.items():
            frame = result.frame
            start = frame.index.min().isoformat() if not frame.empty else ""
            end = frame.index.max().isoformat() if not frame.empty else ""
            details.append(
                {
                    "category": category,
                    "symbol": symbol.upper(),
                    "timeframe": timeframe.value,
                    "rows": int(len(frame.index)),
                    "start": start,
                    "end": end,
                    "source": result.source,
                    "refreshed": result.refreshed,
                    "cache_path": str(result.cache_path),
                }
            )
        return details

    def _load_symbol_frame_results(
        self,
        symbol: str,
        timeframes: list[TimeFrame] | None = None,
        start: str | None = None,
        end: str | None = None,
        force_refresh: bool = False,
        runtime_refresh: bool = False,
    ) -> dict[TimeFrame, MarketDataFrameLoad]:
        requested = self._normalized_timeframes(timeframes)
        loaded: dict[TimeFrame, MarketDataFrameLoad] = {}
        for timeframe in requested:
            loaded[timeframe] = self._load_direct_frame(
                symbol,
                timeframe,
                start=start,
                end=end,
                force_refresh=force_refresh,
                runtime_refresh=runtime_refresh,
            )
        return {timeframe: loaded[timeframe] for timeframe in requested}

    def _normalized_timeframes(self, timeframes: list[TimeFrame] | None) -> list[TimeFrame]:
        selected = list(timeframes or self.config.data.timeframes)
        if self.config.strategy.name == "fx_breakout_pullback":
            required = [
                self.config.strategy.fx_breakout_pullback.execution_timeframe,
                self.config.strategy.fx_breakout_pullback.signal_timeframe,
                self.config.strategy.fx_breakout_pullback.trend_timeframe,
                self.config.strategy.fx_breakout_pullback.swing_timeframe,
                TimeFrame.DAY_1,
                TimeFrame.WEEK_1,
                TimeFrame.MONTH_1,
            ]
        else:
            required = [self.config.strategy.entry_timeframe, TimeFrame.DAY_1, TimeFrame.WEEK_1, TimeFrame.MONTH_1]
        ordered: list[TimeFrame] = []
        for timeframe in [*selected, *required]:
            if timeframe not in ordered:
                ordered.append(timeframe)
        return ordered

    def _load_direct_frame(
        self,
        symbol: str,
        timeframe: TimeFrame,
        *,
        start: str | None,
        end: str | None,
        force_refresh: bool,
        runtime_refresh: bool,
    ) -> MarketDataFrameLoad:
        if timeframe not in SUPPORTED_DIRECT_TIMEFRAMES:
            raise ValueError(f"未対応の直接取得時間足です: {timeframe.value}")
        if self.config.data.source == "fixture":
            return self._load_fixture(symbol, timeframe, start, end)
        if self._uses_gmo():
            return self._load_gmo(symbol, timeframe, start, end, force_refresh, runtime_refresh)
        return self._load_csv_cache(symbol, timeframe, start, end)

    def _load_fixture(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: str | None,
        end: str | None,
    ) -> MarketDataFrameLoad:
        cache_path = self.cache.path_for(symbol, timeframe)
        cache_existed = cache_path.exists()
        frame = self.fixture.load_bars(symbol, timeframe, start, end)
        return MarketDataFrameLoad(
            frame=frame,
            source="fixture" if not cache_existed else "fixture_cache",
            cache_path=cache_path,
            refreshed=not cache_existed,
        )

    def _load_csv_cache(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: str | None,
        end: str | None,
    ) -> MarketDataFrameLoad:
        normalized_symbol = normalize_fx_symbol(symbol)
        cache_path = self.cache.path_for(normalized_symbol, timeframe)
        start_ts, end_ts = self._requested_window(start, end)
        cached = self.cache.load(normalized_symbol, timeframe)
        if cached is None or cached.empty:
            return MarketDataFrameLoad(
                frame=self._empty_frame(start_ts),
                source="csv_missing",
                cache_path=cache_path,
                refreshed=False,
            )
        selection = self._slice_frame(cached, start_ts, end_ts)
        return MarketDataFrameLoad(
            frame=selection,
            source="csv_cache",
            cache_path=cache_path,
            refreshed=False,
        )

    def _load_gmo(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: str | None,
        end: str | None,
        force_refresh: bool,
        runtime_refresh: bool,
    ) -> MarketDataFrameLoad:
        normalized_symbol = normalize_fx_symbol(symbol)
        cache_path = self.cache.path_for(normalized_symbol, timeframe)
        start_ts, end_ts = self._requested_window(start, end)
        cached = self.cache.load(normalized_symbol, timeframe)
        merged = cached if cached is not None else self._empty_frame(start_ts)
        coverage = self.cache.load_coverage(normalized_symbol, timeframe)
        if not coverage and cached is not None and not cached.empty:
            coverage = [self._coverage_seed_from_cached(cached)]
        requested_start = max(start_ts, GMO_INTRADAY_START) if timeframe in {
            TimeFrame.MIN_1,
            TimeFrame.MIN_5,
            TimeFrame.MIN_10,
            TimeFrame.MIN_15,
            TimeFrame.MIN_30,
            TimeFrame.HOUR_1,
        } else start_ts
        missing_ranges = (
            [(requested_start, end_ts)]
            if force_refresh
            else self._missing_ranges(requested_start, end_ts, coverage)
        )
        runtime_ranges = (
            self._runtime_refresh_ranges(timeframe, requested_start, end_ts)
            if runtime_refresh and not force_refresh
            else []
        )
        fetch_ranges = self._merge_ranges([*missing_ranges, *runtime_ranges])
        fetched_frames: list[pd.DataFrame] = []
        for range_start, range_end in fetch_ranges:
            if range_start >= range_end:
                continue
            frame = self.gmo.fetch_bars(
                normalized_symbol,
                timeframe,
                range_start.to_pydatetime(),
                range_end.to_pydatetime(),
                price_type=self.config.data.gmo_price_type,
            )
            frame = self._slice_frame(frame, range_start, range_end)
            self.cache.record_coverage(normalized_symbol, timeframe, range_start, range_end)
            if not frame.empty:
                fetched_frames.append(frame)
        if fetched_frames:
            merged = self._merge_frames([merged, *fetched_frames], start_ts)
            self.cache.save(normalized_symbol, timeframe, merged)
            self.cache.save_metadata(
                normalized_symbol,
                timeframe,
                {
                    "source": "fx_cache",
                    "timeframe": timeframe.value,
                    "symbol": normalized_symbol,
                    "price_type": self.config.data.gmo_price_type,
                    "version": 1,
                },
            )
        return MarketDataFrameLoad(
            frame=self._slice_frame(merged, start_ts, end_ts),
            source=self._gmo_result_source(cached, fetched_frames, runtime_refresh),
            cache_path=cache_path,
            refreshed=bool(fetched_frames),
        )

    def _requested_window(self, start: str | None, end: str | None) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_ts = self._coerce_window_timestamp(start or self.config.data.start_date, is_end=False)
        end_ts = self._coerce_window_timestamp(end or self.config.data.end_date, is_end=True)
        return start_ts, end_ts

    def _slice_frame(self, frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        selection = frame.loc[(frame.index >= start) & (frame.index < end)]
        return selection.copy()

    def _empty_frame(self, start: pd.Timestamp) -> pd.DataFrame:
        return validate_bar_frame(
            pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"],
                index=pd.DatetimeIndex([], tz=start.tz),
            )
        )

    def _merge_frames(self, frames: list[pd.DataFrame], start_ts: pd.Timestamp) -> pd.DataFrame:
        non_empty = [frame for frame in frames if frame is not None and not frame.empty]
        if not non_empty:
            return self._empty_frame(start_ts)
        merged = pd.concat(non_empty).sort_index()
        if not isinstance(merged.index, pd.DatetimeIndex):
            merged.index = pd.to_datetime(merged.index, utc=True).tz_convert(ASIA_TOKYO)
        merged = merged.loc[~merged.index.duplicated(keep="last")]
        return validate_bar_frame(merged)

    def _coverage_seed_from_cached(self, cached: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
        return cached.index.min().normalize(), cached.index.max().normalize() + pd.Timedelta(days=1)

    def _coerce_window_timestamp(self, value: str | pd.Timestamp, *, is_end: bool) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(ASIA_TOKYO)
        else:
            timestamp = timestamp.tz_convert(ASIA_TOKYO)
        if is_end and self._is_date_only_value(value):
            return timestamp + pd.Timedelta(days=1)
        return timestamp

    def _is_date_only_value(self, value: object) -> bool:
        if not isinstance(value, str):
            return False
        normalized = value.strip()
        return "T" not in normalized and ":" not in normalized and " " not in normalized

    def _missing_ranges(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        coverage: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        if start >= end:
            return []
        normalized: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        for cover_start, cover_end in coverage:
            overlap_start = max(start, cover_start)
            overlap_end = min(end, cover_end)
            if overlap_start < overlap_end:
                normalized.append((overlap_start, overlap_end))
        if not normalized:
            return [(start, end)]
        missing: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        cursor = start
        for cover_start, cover_end in sorted(normalized, key=lambda item: item[0]):
            if cover_end <= cursor:
                continue
            if cover_start > cursor:
                missing.append((cursor, cover_start))
            cursor = max(cursor, cover_end)
            if cursor >= end:
                break
        if cursor < end:
            missing.append((cursor, end))
        return [(range_start, range_end) for range_start, range_end in missing if range_start < range_end]

    def _gmo_result_source(
        self,
        cached: pd.DataFrame | None,
        fetched_frames: list[pd.DataFrame],
        runtime_refresh: bool = False,
    ) -> str:
        if fetched_frames:
            if runtime_refresh:
                return "gmo_runtime_refresh"
            return "gmo" if cached is None or cached.empty else "gmo_incremental"
        return "gmo_cache" if cached is not None and not cached.empty else "gmo_empty"

    def _runtime_refresh_ranges(
        self,
        timeframe: TimeFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        overlap = RUNTIME_REFRESH_LOOKBACK.get(timeframe)
        if overlap is None:
            return []
        refresh_start = max(start, end - overlap)
        if refresh_start >= end:
            return []
        return [(refresh_start, end)]

    def _merge_ranges(
        self,
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        valid = [(start, end) for start, end in ranges if start < end]
        if not valid:
            return []
        ordered = sorted(valid, key=lambda item: item[0])
        merged: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        for start, end in ordered:
            if not merged:
                merged.append((start, end))
                continue
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged

    def _trim_for_runtime(self, frame: pd.DataFrame) -> pd.DataFrame:
        max_rows = max(1, int(self.config.data.max_bars_per_symbol))
        if len(frame.index) <= max_rows:
            return frame.copy()
        return frame.iloc[-max_rows:].copy()

    def _uses_gmo(self) -> bool:
        return self.config.data.source == "gmo" or self.config.broker.mode == BrokerMode.GMO_SIM
