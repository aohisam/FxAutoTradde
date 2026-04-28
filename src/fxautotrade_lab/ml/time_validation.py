"""Purged time-series validation helpers for scalping research."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO


@dataclass(frozen=True, slots=True)
class PurgedSplit:
    """Timestamp-indexed train/validation/test split with boundary purging."""

    train_index: pd.DatetimeIndex
    validation_index: pd.DatetimeIndex
    test_index: pd.DatetimeIndex
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    validation_start: pd.Timestamp
    validation_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    purge_seconds: int
    label_horizon_seconds: int


def effective_purge_seconds(
    *,
    max_hold_seconds: int,
    entry_latency_ms: int = 0,
    cooldown_seconds: int = 0,
    configured_purge_seconds: int | None = None,
) -> int:
    """Return a conservative purge window covering label horizon and execution delay."""

    automatic = (
        max(0, int(max_hold_seconds))
        + int(math.ceil(max(0, int(entry_latency_ms)) / 1000.0))
        + max(0, int(cooldown_seconds))
    )
    configured = 0 if configured_purge_seconds is None else max(0, int(configured_purge_seconds))
    return max(automatic, configured)


def purged_train_valid_test_split(
    index: pd.DatetimeIndex,
    *,
    train_ratio: float,
    validation_ratio: float,
    test_ratio: float,
    purge_seconds: int,
    label_horizon_seconds: int,
) -> PurgedSplit:
    """Split an ordered DatetimeIndex into train/validation/test without boundary leakage."""

    ordered = _normalize_index(index)
    if len(ordered) < 5:
        raise ValueError("purged split には最低5件以上のtimestampが必要です。")
    train_pos, validation_pos = _ratio_boundaries(
        len(ordered),
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        test_ratio=test_ratio,
    )
    validation_start = pd.Timestamp(ordered[train_pos])
    test_start = pd.Timestamp(ordered[validation_pos])
    train_start = pd.Timestamp(ordered[0])
    test_end = pd.Timestamp(ordered[-1])
    train_raw = ordered[:train_pos]
    validation_raw = ordered[train_pos:validation_pos]
    test_raw = ordered[validation_pos:]
    train_index = _eligible_before_boundary(
        train_raw,
        boundary=validation_start,
        purge_seconds=purge_seconds,
        label_horizon_seconds=label_horizon_seconds,
    )
    validation_index = _eligible_before_boundary(
        validation_raw,
        boundary=test_start,
        purge_seconds=purge_seconds,
        label_horizon_seconds=label_horizon_seconds,
    )
    test_index = _eligible_before_boundary(
        test_raw,
        boundary=test_end + pd.Timedelta(microseconds=1),
        purge_seconds=0,
        label_horizon_seconds=label_horizon_seconds,
    )
    if train_index.empty or validation_index.empty or test_index.empty:
        raise ValueError(
            "purged split 後の train/validation/test のいずれかが空です。"
            " 期間、validation_ratio、test_ratio、purge_seconds を見直してください。"
        )
    return PurgedSplit(
        train_index=train_index,
        validation_index=validation_index,
        test_index=test_index,
        train_start=train_start,
        train_end=pd.Timestamp(train_index[-1]),
        validation_start=validation_start,
        validation_end=pd.Timestamp(validation_index[-1]),
        test_start=test_start,
        test_end=pd.Timestamp(test_index[-1]),
        purge_seconds=int(purge_seconds),
        label_horizon_seconds=int(label_horizon_seconds),
    )


def purged_walk_forward_splits(
    index: pd.DatetimeIndex,
    *,
    train_days: int,
    validation_days: int,
    test_days: int,
    purge_seconds: int,
    label_horizon_seconds: int,
    min_folds: int = 1,
) -> list[PurgedSplit]:
    """Build rolling train < validation < test folds with purged boundaries."""

    ordered = _normalize_index(index)
    if ordered.empty:
        return []
    train_delta = pd.Timedelta(days=max(1, int(train_days)))
    validation_delta = pd.Timedelta(days=max(1, int(validation_days)))
    test_delta = pd.Timedelta(days=max(1, int(test_days)))
    folds: list[PurgedSplit] = []
    cursor = pd.Timestamp(ordered[0])
    final_time = pd.Timestamp(ordered[-1])
    while cursor + train_delta + validation_delta + test_delta <= final_time:
        train_start = cursor
        validation_start = train_start + train_delta
        test_start = validation_start + validation_delta
        test_end = test_start + test_delta
        train_raw = ordered[(ordered >= train_start) & (ordered < validation_start)]
        validation_raw = ordered[(ordered >= validation_start) & (ordered < test_start)]
        test_raw = ordered[(ordered >= test_start) & (ordered < test_end)]
        train_index = _eligible_before_boundary(
            train_raw,
            boundary=validation_start,
            purge_seconds=purge_seconds,
            label_horizon_seconds=label_horizon_seconds,
        )
        validation_index = _eligible_before_boundary(
            validation_raw,
            boundary=test_start,
            purge_seconds=purge_seconds,
            label_horizon_seconds=label_horizon_seconds,
        )
        test_index = _eligible_before_boundary(
            test_raw,
            boundary=test_end,
            purge_seconds=0,
            label_horizon_seconds=label_horizon_seconds,
        )
        if not train_index.empty and not validation_index.empty and not test_index.empty:
            folds.append(
                PurgedSplit(
                    train_index=train_index,
                    validation_index=validation_index,
                    test_index=test_index,
                    train_start=train_start,
                    train_end=pd.Timestamp(train_index[-1]),
                    validation_start=validation_start,
                    validation_end=pd.Timestamp(validation_index[-1]),
                    test_start=test_start,
                    test_end=pd.Timestamp(test_index[-1]),
                    purge_seconds=int(purge_seconds),
                    label_horizon_seconds=int(label_horizon_seconds),
                )
            )
        cursor = cursor + test_delta
    if len(folds) < max(0, int(min_folds)):
        raise ValueError(
            "purged walk-forward のfold数が不足しています。"
            f" 必要 {int(min_folds)} / 実際 {len(folds)}"
        )
    return folds


def _normalize_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if not isinstance(index, pd.DatetimeIndex):
        raise TypeError("時系列検証には DatetimeIndex が必要です。")
    ordered = pd.DatetimeIndex(index)
    if ordered.tz is None:
        ordered = ordered.tz_localize(ASIA_TOKYO)
    else:
        ordered = ordered.tz_convert(ASIA_TOKYO)
    ordered = ordered.sort_values().unique()
    return pd.DatetimeIndex(ordered)


def _ratio_boundaries(
    length: int,
    *,
    train_ratio: float,
    validation_ratio: float,
    test_ratio: float,
) -> tuple[int, int]:
    train = max(0.05, float(train_ratio))
    validation = max(0.0, float(validation_ratio))
    test = max(0.0, float(test_ratio))
    if validation <= 0.0 and test <= 0.0:
        remaining = max(0.0, 1.0 - train)
        validation = remaining / 2.0
        test = remaining / 2.0
    total = train + validation + test
    train = train / total
    validation = validation / total
    train_pos = max(1, min(length - 3, int(length * train)))
    validation_pos = max(train_pos + 1, min(length - 2, int(length * (train + validation))))
    return train_pos, validation_pos


def _eligible_before_boundary(
    index: pd.DatetimeIndex,
    *,
    boundary: pd.Timestamp,
    purge_seconds: int,
    label_horizon_seconds: int,
) -> pd.DatetimeIndex:
    if index.empty:
        return pd.DatetimeIndex([], tz=boundary.tz)
    purge_cutoff = boundary - pd.Timedelta(seconds=max(0, int(purge_seconds)))
    label_delta = pd.Timedelta(seconds=max(0, int(label_horizon_seconds)))
    mask = (index < purge_cutoff) & ((index + label_delta) < boundary)
    return pd.DatetimeIndex(index[mask])
