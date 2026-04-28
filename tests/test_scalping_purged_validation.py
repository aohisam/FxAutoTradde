from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.time_validation import (
    purged_train_valid_test_split,
    purged_walk_forward_splits,
)


def test_purged_split_removes_boundary_leaking_samples() -> None:
    index = pd.date_range("2026-02-02 09:00:00", periods=120, freq="1s", tz=ASIA_TOKYO)
    split = purged_train_valid_test_split(
        index,
        train_ratio=0.6,
        validation_ratio=0.2,
        test_ratio=0.2,
        purge_seconds=12,
        label_horizon_seconds=10,
    )

    assert split.train_index[-1] + pd.Timedelta(seconds=10) < split.validation_start
    assert split.validation_index[-1] + pd.Timedelta(seconds=10) < split.test_start
    assert split.train_index[-1] < split.validation_start - pd.Timedelta(seconds=12)
    assert split.validation_index[-1] < split.test_start - pd.Timedelta(seconds=12)


def test_walk_forward_folds_keep_chronological_order() -> None:
    index = pd.date_range("2026-02-01", periods=20 * 24, freq="1h", tz=ASIA_TOKYO)
    folds = purged_walk_forward_splits(
        index,
        train_days=5,
        validation_days=2,
        test_days=2,
        purge_seconds=3600,
        label_horizon_seconds=1800,
        min_folds=2,
    )

    assert len(folds) >= 2
    for fold in folds:
        assert fold.train_index.max() < fold.validation_index.min()
        assert fold.validation_index.max() < fold.test_index.min()
        assert fold.train_start < fold.validation_start < fold.test_start
