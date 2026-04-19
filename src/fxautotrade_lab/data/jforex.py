"""JForex CSV import helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.core.symbols import display_fx_symbol, infer_fx_symbol_from_filename, normalize_fx_symbol
from fxautotrade_lab.data.cache import ParquetBarCache, timeframe_coverage_delta
from fxautotrade_lab.data.quality import repair_ohlc_relationships
from fxautotrade_lab.data.quote_bars import (
    build_quote_bar_frame,
    is_combined_quote_csv,
    quote_spread_summary,
    read_combined_quote_csv,
    read_jforex_quote_csv,
    resample_quote_bars,
    validate_quote_bar_frame,
)


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

_SIDE_PATTERNS = {
    "bid": re.compile(r"(^|[^a-z])bid([^a-z]|$)", re.IGNORECASE),
    "ask": re.compile(r"(^|[^a-z])ask([^a-z]|$)", re.IGNORECASE),
}


@dataclass(slots=True)
class JForexImportResult:
    symbol: str
    imported_rows: int
    skipped_rows: int
    source_path: Path
    cache_paths: dict[str, str]
    start: str
    end: str
    applied_start: str = ""
    applied_end: str = ""
    messages: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JForexBidAskImportResult:
    symbol: str
    imported_rows: int
    skipped_rows: int
    bid_source_path: Path
    ask_source_path: Path
    cache_paths: dict[str, str]
    start: str
    end: str
    applied_start: str = ""
    applied_end: str = ""
    bid_start: str = ""
    bid_end: str = ""
    ask_start: str = ""
    ask_end: str = ""
    messages: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class JForexBidAskSelection:
    symbol: str
    bid_source_path: Path
    ask_source_path: Path


def detect_quote_side_from_filename(file_path: str | Path) -> str:
    name = Path(file_path).name
    matches = [side for side, pattern in _SIDE_PATTERNS.items() if pattern.search(name)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) >= 2:
        raise ValueError(
            f"CSV ファイル名に Bid と Ask の両方が含まれています: {name}\n"
            "Bid 用と Ask 用は別ファイルに分けてください。"
        )
    raise ValueError(
        f"CSV ファイル名に Bid または Ask が含まれていません: {name}\n"
        "例: USDJPY_1 Min_Bid_....csv / USDJPY_1 Min_Ask_....csv"
    )


def resolve_bid_ask_csv_selection(
    file_paths: list[str | Path],
    symbol: str | None = None,
) -> JForexBidAskSelection:
    if len(file_paths) != 2:
        raise ValueError(
            f"CSV インポートは Bid / Ask の 2 ファイル必須です。現在の選択数: {len(file_paths)}\n"
            "ファイル名に Bid / Ask を含む 2 ファイルを同時に選択してください。"
        )
    side_to_path: dict[str, Path] = {}
    side_to_symbol: dict[str, str] = {}
    for raw_path in file_paths:
        path = Path(raw_path)
        side = detect_quote_side_from_filename(path)
        if side in side_to_path:
            raise ValueError(
                f"{side.upper()} 側の CSV が複数選択されています: {side_to_path[side].name}, {path.name}\n"
                "Bid と Ask を 1 ファイルずつ選択してください。"
            )
        side_to_path[side] = path
        try:
            side_to_symbol[side] = infer_fx_symbol_from_filename(path)
        except ValueError as exc:
            raise ValueError(
                f"CSV ファイル名から通貨ペアを判別できませんでした: {path.name}\n"
                "例: USDJPY_1 Min_Bid_....csv のように通貨ペアをファイル名へ含めてください。"
            ) from exc
    missing = [side for side in ("bid", "ask") if side not in side_to_path]
    if missing:
        missing_labels = " / ".join(side.upper() for side in missing)
        raise ValueError(
            f"{missing_labels} 側の CSV が不足しています。\n"
            "Bid と Ask を 1 ファイルずつ選択してください。"
        )
    bid_symbol = side_to_symbol["bid"]
    ask_symbol = side_to_symbol["ask"]
    if bid_symbol != ask_symbol:
        raise ValueError(
            "Bid / Ask CSV の通貨ペア名が一致しません。\n"
            f"Bid: {display_fx_symbol(bid_symbol)} ({side_to_path['bid'].name})\n"
            f"Ask: {display_fx_symbol(ask_symbol)} ({side_to_path['ask'].name})"
        )
    normalized_symbol = normalize_fx_symbol(symbol) if symbol else bid_symbol
    if normalized_symbol != bid_symbol:
        raise ValueError(
            "指定した通貨ペアと CSV ファイル名の通貨ペアが一致しません。\n"
            f"指定値: {display_fx_symbol(normalized_symbol)}\n"
            f"CSV: {display_fx_symbol(bid_symbol)}"
        )
    return JForexBidAskSelection(
        symbol=normalized_symbol,
        bid_source_path=side_to_path["bid"],
        ask_source_path=side_to_path["ask"],
    )


class JForexCsvImporter:
    def __init__(self, cache: ParquetBarCache) -> None:
        self.cache = cache

    def _repair_quote_side_source(self, frame: pd.DataFrame, side: str) -> tuple[pd.DataFrame, list[str]]:
        repaired, stats = repair_ohlc_relationships(
            frame,
            open_column=f"{side}_open",
            high_column=f"{side}_high",
            low_column=f"{side}_low",
            close_column=f"{side}_close",
        )
        if int(stats["adjusted_rows"]) <= 0:
            return repaired, []
        max_adjustment = max(float(stats["max_high_adjustment"]), float(stats["max_low_adjustment"]))
        message = (
            f"{side.upper()} CSV に OHLC の不整合が {int(stats['adjusted_rows']):,} 行あり、自動補正しました。"
            f" high 補正 {int(stats['adjusted_high_rows']):,} 行 / low 補正 {int(stats['adjusted_low_rows']):,} 行"
            f" / 最大補正幅 {max_adjustment:.6f}"
        )
        return repaired, [message]

    def _repair_crossed_quote_prices(
        self,
        bid_frame: pd.DataFrame,
        ask_frame: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        repaired_bid = bid_frame.copy()
        repaired_ask = ask_frame.copy()
        crossed_rows = 0
        max_adjustment = 0.0
        adjusted_columns: list[str] = []
        for price_column in ("open", "high", "low", "close"):
            bid_column = f"bid_{price_column}"
            ask_column = f"ask_{price_column}"
            adjustment = (repaired_bid[bid_column] - repaired_ask[ask_column]).clip(lower=0.0)
            crossed = adjustment > 0.0
            if not crossed.any():
                continue
            repaired_ask.loc[crossed, ask_column] = repaired_bid.loc[crossed, bid_column]
            crossed_rows += int(crossed.sum())
            max_adjustment = max(max_adjustment, float(adjustment.max()))
            adjusted_columns.append(price_column)
        if crossed_rows <= 0:
            return repaired_bid, repaired_ask, []
        labels = ", ".join(adjusted_columns)
        return repaired_bid, repaired_ask, [
            "Bid / Ask CSV に負のスプレッドがあり、自動補正しました。"
            f" 対象列: {labels} / 補正行数: {crossed_rows:,} / 最大補正幅 {max_adjustment:.6f}"
        ]

    def import_file(self, file_path: str | Path, symbol: str | None = None) -> JForexImportResult:
        source_path = Path(file_path)
        normalized_symbol = normalize_fx_symbol(symbol) if symbol else infer_fx_symbol_from_filename(source_path)
        if is_combined_quote_csv(source_path):
            quote_frame = read_combined_quote_csv(source_path)
            base = build_quote_bar_frame(
                quote_frame[["bid_open", "bid_high", "bid_low", "bid_close", "bid_volume"]],
                quote_frame[["ask_open", "ask_high", "ask_low", "ask_close", "ask_volume"]],
                normalized_symbol,
            )
            summary = self._import_quote_base(
                symbol=normalized_symbol,
                base=base,
                source_keys=["csv_combined_quote"],
                metadata={
                    "source": "fx_cache_combined_quote",
                    "symbol": normalized_symbol,
                    "version": 4,
                    "source_path": str(source_path),
                },
            )
            return JForexImportResult(
                symbol=normalized_symbol,
                imported_rows=summary["imported_rows"],
                skipped_rows=summary["skipped_rows"],
                source_path=source_path,
                cache_paths=summary["cache_paths"],
                start=base.index.min().isoformat(),
                end=base.index.max().isoformat(),
                applied_start=summary["applied_start"],
                applied_end=summary["applied_end"],
            )
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
        base = validate_quote_bar_frame(frame)
        summary = self._import_quote_base(
            symbol=normalized_symbol,
            base=base,
            source_keys=["csv_mid_only"],
            metadata={
                "source": "fx_cache_mid_only",
                "symbol": normalized_symbol,
                "version": 4,
                "source_path": str(source_path),
                "synthetic_quote": True,
            },
        )
        return JForexImportResult(
            symbol=normalized_symbol,
            imported_rows=summary["imported_rows"],
            skipped_rows=summary["skipped_rows"],
            source_path=source_path,
            cache_paths=summary["cache_paths"],
            start=base.index.min().isoformat(),
            end=base.index.max().isoformat(),
            applied_start=summary["applied_start"],
            applied_end=summary["applied_end"],
            messages=[],
        )

    def import_bid_ask_files(
        self,
        bid_file_path: str | Path,
        ask_file_path: str | Path,
        symbol: str | None = None,
    ) -> JForexBidAskImportResult:
        selection = resolve_bid_ask_csv_selection([bid_file_path, ask_file_path], symbol=symbol)
        bid_source_path = selection.bid_source_path
        ask_source_path = selection.ask_source_path
        normalized_symbol = selection.symbol
        bid_frame = read_jforex_quote_csv(bid_source_path, "bid")
        ask_frame = read_jforex_quote_csv(ask_source_path, "ask")
        bid_frame, bid_messages = self._repair_quote_side_source(bid_frame, "bid")
        ask_frame, ask_messages = self._repair_quote_side_source(ask_frame, "ask")
        bid_frame, ask_frame, spread_messages = self._repair_crossed_quote_prices(bid_frame, ask_frame)
        bid_start, bid_end = self._raw_frame_bounds(bid_frame)
        ask_start, ask_end = self._raw_frame_bounds(ask_frame)
        trimmed_bid, trimmed_ask, messages = self._align_bid_ask_frames(bid_frame, ask_frame)
        messages = [*bid_messages, *ask_messages, *spread_messages, *messages]
        try:
            base = build_quote_bar_frame(trimmed_bid, trimmed_ask, normalized_symbol)
        except ValueError as exc:
            if "Bid/Ask の結合結果が空です" in str(exc):
                raise ValueError(
                    "Bid / Ask CSV の共通期間では時刻が一致しません。\n"
                    "夏時間切替や欠損により 1 分足の timestamp がずれていないか確認してください。"
                ) from exc
            raise
        summary = self._import_quote_base(
            symbol=normalized_symbol,
            base=base,
            source_keys=["csv_bid", "csv_ask"],
            metadata={
                "source": "fx_cache_bid_ask",
                "symbol": normalized_symbol,
                "version": 4,
                "bid_source_path": str(bid_source_path),
                "ask_source_path": str(ask_source_path),
            },
        )
        return JForexBidAskImportResult(
            symbol=normalized_symbol,
            imported_rows=summary["imported_rows"],
            skipped_rows=summary["skipped_rows"],
            bid_source_path=bid_source_path,
            ask_source_path=ask_source_path,
            cache_paths=summary["cache_paths"],
            start=base.index.min().isoformat(),
            end=base.index.max().isoformat(),
            applied_start=summary["applied_start"],
            applied_end=summary["applied_end"],
            bid_start=bid_start.isoformat(),
            bid_end=bid_end.isoformat(),
            ask_start=ask_start.isoformat(),
            ask_end=ask_end.isoformat(),
            messages=messages,
        )

    def _import_quote_base(
        self,
        *,
        symbol: str,
        base: pd.DataFrame,
        source_keys: list[str],
        metadata: dict[str, object],
    ) -> dict[str, object]:
        base = validate_quote_bar_frame(base)
        min1_existing = self.cache.load(symbol, TimeFrame.MIN_1)
        min1_existing = validate_quote_bar_frame(min1_existing) if min1_existing is not None and not min1_existing.empty else None
        base_window = self._frame_coverage_range(base, TimeFrame.MIN_1)
        current_coverage = self.cache.load_coverage(symbol, TimeFrame.MIN_1)
        if not current_coverage and min1_existing is not None and not min1_existing.empty:
            seed = self._frame_coverage_range(min1_existing, TimeFrame.MIN_1)
            current_coverage = [seed] if seed is not None else []
        missing_ranges = (
            self._missing_ranges(base_window[0], base_window[1], current_coverage)
            if base_window is not None
            else []
        )
        accepted_base = self._slice_to_ranges(base, missing_ranges)
        imported_rows = int(len(accepted_base.index))
        skipped_rows = int(len(base.index) - imported_rows)
        merged_min1 = self._merge_frames(min1_existing, accepted_base, dedupe_keep="first")
        cache_paths: dict[str, str] = {}
        if merged_min1 is not None and not merged_min1.empty:
            path = self.cache.save(symbol, TimeFrame.MIN_1, merged_min1)
            cache_paths[TimeFrame.MIN_1.value] = str(path)
            self._save_import_metadata(
                symbol=symbol,
                timeframe=TimeFrame.MIN_1,
                metadata=metadata,
                imported_rows=imported_rows,
                skipped_rows=skipped_rows,
                spread_stats=quote_spread_summary(merged_min1),
                source_keys=source_keys,
            )
        if imported_rows > 0:
            for range_start, range_end in missing_ranges:
                for source_key in source_keys:
                    self.cache.record_coverage(symbol, TimeFrame.MIN_1, range_start, range_end, source_key=source_key)
        for timeframe, rule in TIMEFRAME_RULES.items():
            if timeframe == TimeFrame.MIN_1:
                continue
            existing_tf = self.cache.load(symbol, timeframe)
            existing_tf = validate_quote_bar_frame(existing_tf) if existing_tf is not None and not existing_tf.empty else None
            cache_path = self.cache.path_for(symbol, timeframe)
            if imported_rows <= 0:
                if cache_path.exists():
                    cache_paths[timeframe.value] = str(cache_path)
                continue
            expanded_ranges = self._expanded_ranges_for_timeframe(missing_ranges, timeframe)
            affected_base = self._expanded_rows_for_resample(merged_min1, expanded_ranges)
            if affected_base.empty:
                if cache_path.exists():
                    cache_paths[timeframe.value] = str(cache_path)
                continue
            derived = resample_quote_bars(affected_base, rule)
            merged_tf = self._merge_frames(existing_tf, derived, dedupe_keep="last")
            if merged_tf is None or merged_tf.empty:
                continue
            path = self.cache.save(symbol, timeframe, merged_tf)
            cache_paths[timeframe.value] = str(path)
            for expanded_start, expanded_end in expanded_ranges:
                for source_key in source_keys:
                    self.cache.record_coverage(
                        symbol,
                        timeframe,
                        expanded_start,
                        expanded_end,
                        source_key=source_key,
                    )
            self._save_import_metadata(
                symbol=symbol,
                timeframe=timeframe,
                metadata=metadata,
                imported_rows=imported_rows,
                skipped_rows=skipped_rows,
                spread_stats=quote_spread_summary(merged_tf),
                source_keys=source_keys,
            )
        applied_window = self._frame_coverage_range(accepted_base, TimeFrame.MIN_1)
        return {
            "imported_rows": imported_rows,
            "skipped_rows": skipped_rows,
            "cache_paths": cache_paths,
            "applied_start": applied_window[0].isoformat() if applied_window is not None else "",
            "applied_end": accepted_base.index.max().isoformat() if applied_window is not None and not accepted_base.empty else "",
        }

    def _merge_frames(
        self,
        existing: pd.DataFrame | None,
        incoming: pd.DataFrame | None,
        *,
        dedupe_keep: str,
    ) -> pd.DataFrame | None:
        frames = [frame for frame in (existing, incoming) if frame is not None and not frame.empty]
        if not frames:
            return None
        merged = pd.concat(frames).sort_index()
        merged = merged.loc[~merged.index.duplicated(keep=dedupe_keep)]
        return validate_quote_bar_frame(merged)

    def _frame_coverage_range(
        self,
        frame: pd.DataFrame,
        timeframe: TimeFrame,
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        if frame.empty:
            return None
        start = pd.Timestamp(frame.index.min())
        end = pd.Timestamp(frame.index.max()) + timeframe_coverage_delta(timeframe)
        return start, end

    def _raw_frame_bounds(
        self,
        frame: pd.DataFrame,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        return pd.Timestamp(frame.index.min()), pd.Timestamp(frame.index.max())

    def _align_bid_ask_frames(
        self,
        bid_frame: pd.DataFrame,
        ask_frame: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        bid_start, bid_end = self._raw_frame_bounds(bid_frame)
        ask_start, ask_end = self._raw_frame_bounds(ask_frame)
        overlap_start = max(bid_start, ask_start)
        overlap_end = min(bid_end, ask_end)
        if overlap_start > overlap_end:
            raise ValueError(
                "Bid / Ask CSV の期間が重なっていません。\n"
                f"Bid: {bid_start.isoformat()} - {bid_end.isoformat()}\n"
                f"Ask: {ask_start.isoformat()} - {ask_end.isoformat()}"
            )
        trimmed_bid = bid_frame.loc[(bid_frame.index >= overlap_start) & (bid_frame.index <= overlap_end)].copy()
        trimmed_ask = ask_frame.loc[(ask_frame.index >= overlap_start) & (ask_frame.index <= overlap_end)].copy()
        messages: list[str] = []
        if bid_start != overlap_start or bid_end != overlap_end or ask_start != overlap_start or ask_end != overlap_end:
            messages.append(
                "Bid / Ask の CSV 期間が一致しないため、共通期間のみを取り込みます。\n"
                f"共通期間: {overlap_start.isoformat()} - {overlap_end.isoformat()}"
            )
        return trimmed_bid, trimmed_ask, messages

    def _slice_to_ranges(
        self,
        frame: pd.DataFrame,
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> pd.DataFrame:
        if frame.empty or not ranges:
            return frame.iloc[0:0].copy()
        parts = [
            frame.loc[(frame.index >= range_start) & (frame.index < range_end)].copy()
            for range_start, range_end in ranges
        ]
        non_empty = [part for part in parts if not part.empty]
        if not non_empty:
            return frame.iloc[0:0].copy()
        merged = pd.concat(non_empty).sort_index()
        return merged.loc[~merged.index.duplicated(keep="last")].copy()

    def _expanded_rows_for_resample(
        self,
        merged_min1: pd.DataFrame | None,
        expanded_ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> pd.DataFrame:
        if merged_min1 is None or merged_min1.empty or not expanded_ranges:
            return merged_min1.iloc[0:0].copy() if merged_min1 is not None else pd.DataFrame()
        return self._slice_to_ranges(merged_min1, self._merge_ranges(expanded_ranges))

    def _expanded_ranges_for_timeframe(
        self,
        missing_ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
        timeframe: TimeFrame,
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        delta = timeframe_coverage_delta(timeframe)
        return self._merge_ranges(
            [
                (range_start - delta, range_end + delta)
                for range_start, range_end in missing_ranges
            ]
        )

    def _missing_ranges(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        coverage: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        if start >= end:
            return []
        normalized = self._merge_ranges(
            [
                (max(start, cover_start), min(end, cover_end))
                for cover_start, cover_end in coverage
                if max(start, cover_start) < min(end, cover_end)
            ]
        )
        if not normalized:
            return [(start, end)]
        missing: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        cursor = start
        for cover_start, cover_end in normalized:
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

    def _save_import_metadata(
        self,
        *,
        symbol: str,
        timeframe: TimeFrame,
        metadata: dict[str, object],
        imported_rows: int,
        skipped_rows: int,
        spread_stats: dict[str, float],
        source_keys: list[str],
    ) -> None:
        existing = self.cache.load_metadata(symbol, timeframe)
        history = list(existing.get("import_history", [])) if isinstance(existing.get("import_history", []), list) else []
        history.append(
            {
                "recorded_at": pd.Timestamp.now(tz=ASIA_TOKYO).isoformat(),
                "timeframe": timeframe.value,
                "imported_rows": imported_rows,
                "skipped_rows": skipped_rows,
                "source_keys": list(source_keys),
            }
        )
        self.cache.save_metadata(
            symbol,
            timeframe,
            {
                **existing,
                **metadata,
                "timeframe": timeframe.value,
                "symbol": symbol,
                "imported_rows_last_run": imported_rows,
                "skipped_rows_last_run": skipped_rows,
                "source_keys": list(source_keys),
                "import_history": history[-20:],
                **spread_stats,
            },
        )
