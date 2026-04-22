"""Chunk planning helpers for stateful backtests."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.core.windows import shift_timestamp


@dataclass(slots=True)
class TimeChunk:
    index: int
    total: int
    start: pd.Timestamp
    end: pd.Timestamp
    warmup_start: pd.Timestamp
    label: str


def format_chunk_label(start: pd.Timestamp, end: pd.Timestamp) -> str:
    inclusive_end = end - pd.Timedelta(minutes=1)
    return f"{start:%Y-%m-%d}〜{inclusive_end:%Y-%m-%d}"


def plan_time_chunks(
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    chunk_window: str,
    warmup_window: str = "0d",
    minimum_start: pd.Timestamp | None = None,
) -> list[TimeChunk]:
    if start >= end:
        return []
    spans: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = start
    while cursor < end:
        next_cursor = min(shift_timestamp(cursor, chunk_window, backward=False), end)
        if next_cursor <= cursor:
            break
        spans.append((cursor, next_cursor))
        cursor = next_cursor
    total = len(spans)
    chunks: list[TimeChunk] = []
    for index, (chunk_start, chunk_end) in enumerate(spans, start=1):
        warmup_start = shift_timestamp(chunk_start, warmup_window, backward=True)
        if minimum_start is not None:
            warmup_start = max(minimum_start, warmup_start)
        chunks.append(
            TimeChunk(
                index=index,
                total=total,
                start=chunk_start,
                end=chunk_end,
                warmup_start=warmup_start,
                label=format_chunk_label(chunk_start, chunk_end),
            )
        )
    return chunks
