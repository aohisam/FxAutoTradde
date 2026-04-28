from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.features.scalping import SCALPING_FEATURE_COLUMNS
from fxautotrade_lab.ml import scalping as scalping_module
from fxautotrade_lab.ml.scalping import (
    ScalpingTrainingConfig,
    fit_scalping_model,
    select_decision_threshold,
)
from tests.scalping_helpers import neutral_features


class ProbabilityColumnModel:
    def predict_proba(self, features: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(features["probability"], errors="raise")


def test_threshold_selection_uses_validation_distribution() -> None:
    config = ScalpingTrainingConfig(
        min_threshold_trades=1,
        decision_threshold=0.5,
        threshold_grid=(0.55, 0.85),
    )
    validation_features = pd.DataFrame({"probability": [0.9, 0.6, 0.59]})
    validation_meta = pd.DataFrame({"net_pips": [-20.0, 4.0, 4.0]})

    threshold, metrics = select_decision_threshold(
        ProbabilityColumnModel(),  # type: ignore[arg-type]
        validation_features,
        validation_meta,
        config=config,
    )

    assert threshold == 0.55
    assert metrics["selected_net_pips"] == -12.0


def test_training_optimal_threshold_is_not_used_when_validation_is_supplied() -> None:
    config = ScalpingTrainingConfig(
        min_threshold_trades=1,
        decision_threshold=0.5,
        threshold_grid=(0.55, 0.85),
    )
    train_features = pd.DataFrame({"probability": [0.9, 0.6, 0.59]})
    train_meta = pd.DataFrame({"net_pips": [20.0, -4.0, -4.0]})
    validation_features = pd.DataFrame({"probability": [0.9, 0.6, 0.59]})
    validation_meta = pd.DataFrame({"net_pips": [-20.0, 4.0, 4.0]})

    train_threshold, _ = select_decision_threshold(
        ProbabilityColumnModel(),  # type: ignore[arg-type]
        train_features,
        train_meta,
        config=config,
    )
    validation_threshold, _ = select_decision_threshold(
        ProbabilityColumnModel(),  # type: ignore[arg-type]
        validation_features,
        validation_meta,
        config=config,
    )

    assert train_threshold == 0.85
    assert validation_threshold == 0.55


def test_fit_metadata_records_validation_threshold_source(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class DummyLogistic:
        feature_names = list(SCALPING_FEATURE_COLUMNS)
        metadata: dict[str, object] = {}

        def predict_proba(self, features: pd.DataFrame) -> pd.Series:
            values = (features["side_sign"] > 0).astype("float64") * 0.9
            return pd.Series(values.to_numpy(), index=features.index)

    def fake_fit(*args, **kwargs) -> DummyLogistic:  # type: ignore[no-untyped-def]
        return DummyLogistic()

    monkeypatch.setattr(scalping_module.NumpyLogisticRegression, "fit", fake_fit)
    train_index = pd.date_range("2026-02-02 09:00:00", periods=8, freq="1s", tz=ASIA_TOKYO)
    validation_index = pd.date_range("2026-02-02 09:01:00", periods=8, freq="1s", tz=ASIA_TOKYO)
    labels = pd.DataFrame(
        {
            "long_net_pips": 1.0,
            "short_net_pips": -1.0,
            "long_win": True,
            "short_win": False,
        },
        index=train_index,
    )
    validation_labels = labels.copy()
    validation_labels.index = validation_index

    bundle = fit_scalping_model(
        neutral_features(train_index),
        labels,
        validation_features=neutral_features(validation_index),
        validation_labels=validation_labels,
        config=ScalpingTrainingConfig(min_samples=1, min_threshold_trades=1),
    )

    assert bundle.train_metrics["threshold_selected_on"] == "validation"
    assert bundle.metadata["threshold_selected_on"] == "validation"


def test_validation_gate_fail_closed_sets_threshold_above_one(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class DummyLogistic:
        feature_names = list(SCALPING_FEATURE_COLUMNS)
        metadata: dict[str, object] = {}

        def predict_proba(self, features: pd.DataFrame) -> pd.Series:
            return pd.Series(0.9, index=features.index, dtype="float64")

    monkeypatch.setattr(
        scalping_module.NumpyLogisticRegression,
        "fit",
        lambda *args, **kwargs: DummyLogistic(),
    )
    train_index = pd.date_range("2026-02-02 09:00:00", periods=8, freq="1s", tz=ASIA_TOKYO)
    validation_index = pd.date_range("2026-02-02 09:01:00", periods=8, freq="1s", tz=ASIA_TOKYO)
    train_labels = pd.DataFrame(
        {
            "long_net_pips": 1.0,
            "short_net_pips": -1.0,
            "long_win": True,
            "short_win": False,
        },
        index=train_index,
    )
    validation_labels = pd.DataFrame(
        {
            "long_net_pips": -2.0,
            "short_net_pips": -2.0,
            "long_win": False,
            "short_win": False,
        },
        index=validation_index,
    )

    bundle = fit_scalping_model(
        neutral_features(train_index),
        train_labels,
        validation_features=neutral_features(validation_index),
        validation_labels=validation_labels,
        config=ScalpingTrainingConfig(
            min_samples=1,
            min_threshold_trades=1,
            min_validation_net_pips=0.0,
            min_validation_profit_factor=1.0,
            min_validation_trade_count=1,
            fail_closed_on_bad_validation=True,
        ),
    )

    assert bundle.decision_threshold == 1.01
    assert bundle.train_metrics["validation_gate_passed"] is False
    assert bundle.metadata["validation_gate_passed"] is False
    assert "validation gate未達" in str(bundle.train_metrics["warning_ja"])
