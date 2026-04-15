"""Parquet cache repository."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.quality import validate_bar_frame


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
        frame.index = pd.DatetimeIndex(frame.index)
        return validate_bar_frame(frame)

    def save(self, symbol: str, timeframe: TimeFrame, frame: pd.DataFrame) -> Path:
        frame = validate_bar_frame(frame)
        path = self._path_for(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path)
        return path

    def upsert(self, symbol: str, timeframe: TimeFrame, frame: pd.DataFrame) -> Path:
        existing = self.load(symbol, timeframe)
        merged = frame if existing is None else pd.concat([existing, frame]).sort_index()
        merged = merged.loc[~merged.index.duplicated(keep="last")]
        return self.save(symbol, timeframe, merged)

    def load_coverage(self, symbol: str, timeframe: TimeFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        path = self._coverage_path_for(symbol, timeframe)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        for item in payload.get("ranges", []):
            start = pd.Timestamp(item["start"])
            end = pd.Timestamp(item["end"])
            if start.tz is None or end.tz is None or start >= end:
                continue
            ranges.append((start, end))
        return self._merge_ranges(ranges)

    def save_coverage(
        self,
        symbol: str,
        timeframe: TimeFrame,
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]],
    ) -> Path:
        path = self._coverage_path_for(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        merged = self._merge_ranges(ranges)
        payload = {
            "ranges": [
                {"start": start.isoformat(), "end": end.isoformat()}
                for start, end in merged
            ]
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def record_coverage(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Path:
        if start >= end:
            return self._coverage_path_for(symbol, timeframe)
        ranges = self.load_coverage(symbol, timeframe)
        ranges.append((start, end))
        return self.save_coverage(symbol, timeframe, ranges)

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
