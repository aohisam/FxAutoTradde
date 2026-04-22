"""Parquet cache repository."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.quality import validate_bar_frame


TIMEFRAME_COVERAGE_DELTAS: dict[TimeFrame, pd.Timedelta] = {
    TimeFrame.MIN_1: pd.Timedelta(minutes=1),
    TimeFrame.MIN_5: pd.Timedelta(minutes=5),
    TimeFrame.MIN_10: pd.Timedelta(minutes=10),
    TimeFrame.MIN_15: pd.Timedelta(minutes=15),
    TimeFrame.MIN_30: pd.Timedelta(minutes=30),
    TimeFrame.HOUR_1: pd.Timedelta(hours=1),
    TimeFrame.HOUR_4: pd.Timedelta(hours=4),
    TimeFrame.HOUR_8: pd.Timedelta(hours=8),
    TimeFrame.HOUR_12: pd.Timedelta(hours=12),
    TimeFrame.DAY_1: pd.Timedelta(days=1),
    TimeFrame.WEEK_1: pd.Timedelta(weeks=1),
    TimeFrame.MONTH_1: pd.Timedelta(days=31),
}


def timeframe_coverage_delta(timeframe: TimeFrame) -> pd.Timedelta:
    return TIMEFRAME_COVERAGE_DELTAS[timeframe]


class ParquetBarCache:
    """Stores OHLCV bars by symbol and timeframe."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, symbol: str, timeframe: TimeFrame) -> Path:
        return self.cache_dir / symbol.upper() / f"{timeframe.value}.parquet"

    def path_for(self, symbol: str, timeframe: TimeFrame) -> Path:
        return self._path_for(symbol, timeframe)

    def _coverage_path_for(self, symbol: str, timeframe: TimeFrame) -> Path:
        return self.cache_dir / symbol.upper() / f"{timeframe.value}.coverage.json"

    def _metadata_path_for(self, symbol: str, timeframe: TimeFrame) -> Path:
        return self.cache_dir / symbol.upper() / f"{timeframe.value}.metadata.json"

    def load(self, symbol: str, timeframe: TimeFrame) -> pd.DataFrame | None:
        path = self._path_for(symbol, timeframe)
        if not path.exists():
            return None
        frame = pd.read_parquet(path)
        return self._normalize_loaded_frame(frame)

    def load_window(
        self,
        symbol: str,
        timeframe: TimeFrame,
        *,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame | None:
        path = self._path_for(symbol, timeframe)
        if not path.exists():
            return None
        if self._supports_timestamp_filter(path):
            frame = pd.read_parquet(
                path,
                filters=[
                    ("timestamp", ">=", start),
                    ("timestamp", "<", end),
                ],
            )
            return self._normalize_loaded_frame(frame)
        frame = self.load(symbol, timeframe)
        if frame is None:
            return None
        try:
            self.save(symbol, timeframe, frame)
        except Exception:
            pass
        selection = frame.loc[(frame.index >= start) & (frame.index < end)]
        return selection.copy()

    def save(self, symbol: str, timeframe: TimeFrame, frame: pd.DataFrame) -> Path:
        frame = validate_bar_frame(frame)
        path = self._path_for(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = frame.copy()
        payload["timestamp"] = frame.index
        payload.to_parquet(path)
        return path

    def upsert(self, symbol: str, timeframe: TimeFrame, frame: pd.DataFrame) -> Path:
        existing = self.load(symbol, timeframe)
        merged = frame if existing is None else pd.concat([existing, frame]).sort_index()
        merged = merged.loc[~merged.index.duplicated(keep="last")]
        return self.save(symbol, timeframe, merged)

    def load_coverage(
        self,
        symbol: str,
        timeframe: TimeFrame,
        *,
        source_key: str | None = None,
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        path = self._coverage_path_for(symbol, timeframe)
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, dict):
            return []
        if source_key:
            sources = payload.get("sources", {})
            source_payload = sources.get(source_key, []) if isinstance(sources, dict) else []
            return self._deserialize_ranges(source_payload)
        ranges = self._deserialize_ranges(payload.get("ranges", []))
        sources = payload.get("sources", {})
        if isinstance(sources, dict):
            for source_payload in sources.values():
                ranges.extend(self._deserialize_ranges(source_payload))
        return self._merge_ranges(ranges)

    def save_coverage(
        self,
        symbol: str,
        timeframe: TimeFrame,
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
        *,
        source_key: str | None = None,
    ) -> Path:
        path = self._coverage_path_for(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._load_coverage_payload(path)
        payload["version"] = 2
        merged_union = self._merge_ranges([*self._deserialize_ranges(payload.get("ranges", [])), *ranges])
        payload["ranges"] = self._serialize_ranges(merged_union)
        if source_key:
            sources = payload.setdefault("sources", {})
            existing_source_ranges = self._deserialize_ranges(sources.get(source_key, []))
            sources[source_key] = self._serialize_ranges([*existing_source_ranges, *ranges])
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def record_coverage(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        source_key: str | None = None,
    ) -> Path:
        if start >= end:
            return self._coverage_path_for(symbol, timeframe)
        return self.save_coverage(symbol, timeframe, [(start, end)], source_key=source_key)

    def load_metadata(self, symbol: str, timeframe: TimeFrame) -> dict[str, object]:
        path = self._metadata_path_for(symbol, timeframe)
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def save_metadata(self, symbol: str, timeframe: TimeFrame, metadata: dict[str, object]) -> Path:
        path = self._metadata_path_for(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def clear(self, symbol: str, timeframe: TimeFrame) -> None:
        for path in (
            self._path_for(symbol, timeframe),
            self._coverage_path_for(symbol, timeframe),
            self._metadata_path_for(symbol, timeframe),
        ):
            if path.exists():
                path.unlink()

    def _merge_ranges(
        self,
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        if not ranges:
            return []
        ordered = sorted(ranges, key=lambda item: item[0])
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

    def _load_coverage_payload(self, path: Path) -> dict[str, object]:
        if not path.exists():
            return {"version": 2, "ranges": [], "sources": {}}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"version": 2, "ranges": [], "sources": {}}
        if not isinstance(payload, dict):
            return {"version": 2, "ranges": [], "sources": {}}
        normalized = {
            "version": int(payload.get("version", 1) or 1),
            "ranges": payload.get("ranges", []),
            "sources": payload.get("sources", {}) if isinstance(payload.get("sources", {}), dict) else {},
        }
        return normalized

    def _deserialize_ranges(self, payload: object) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        if not isinstance(payload, list):
            return []
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            start = pd.Timestamp(item.get("start"))
            end = pd.Timestamp(item.get("end"))
            if start.tz is None or end.tz is None or start >= end:
                continue
            ranges.append((start, end))
        return self._merge_ranges(ranges)

    def _serialize_ranges(self, ranges: list[tuple[pd.Timestamp, pd.Timestamp]]) -> list[dict[str, str]]:
        return [
            {"start": start.isoformat(), "end": end.isoformat()}
            for start, end in self._merge_ranges(ranges)
        ]

    def _normalize_loaded_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        working = frame.copy()
        if "timestamp" in working.columns:
            timestamp = pd.to_datetime(working.pop("timestamp"), errors="coerce")
            index = pd.DatetimeIndex(timestamp)
        else:
            index = pd.DatetimeIndex(working.index)
        if index.tz is None:
            index = index.tz_localize("Asia/Tokyo")
        working.index = index
        return validate_bar_frame(working)

    def _supports_timestamp_filter(self, path: Path) -> bool:
        try:
            schema = pq.read_schema(path)
        except Exception:
            return False
        return "timestamp" in schema.names
