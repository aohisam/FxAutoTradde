from __future__ import annotations

import numpy as np
import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.features.scalping import (
    SCALPING_FEATURE_COLUMNS,
    build_directional_feature_frame,
    build_scalping_feature_frame,
)


def _bars(index: pd.DatetimeIndex) -> pd.DataFrame:
    mid = pd.Series(np.linspace(150.0, 150.2, len(index)), index=index)
    spread = 0.002
    return pd.DataFrame(
        {
            "bid_open": mid - spread / 2,
            "bid_high": mid + 0.001 - spread / 2,
            "bid_low": mid - 0.001 - spread / 2,
            "bid_close": mid - spread / 2,
            "bid_volume": 1.0,
            "ask_open": mid + spread / 2,
            "ask_high": mid + 0.001 + spread / 2,
            "ask_low": mid - 0.001 + spread / 2,
            "ask_close": mid + spread / 2,
            "ask_volume": 1.0,
            "tick_count": 2,
        },
        index=index,
    )


def test_future_bar_change_does_not_change_past_scalping_features() -> None:
    index = pd.date_range("2026-02-02 09:00:00", periods=120, freq="1s", tz=ASIA_TOKYO)
    bars = _bars(index)
    original = build_scalping_feature_frame(bars, symbol="USD_JPY")
    changed = bars.copy()
    for column in (
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
    ):
        changed.loc[index[90] :, column] += 5.0
    changed_features = build_scalping_feature_frame(changed, symbol="USD_JPY")

    pd.testing.assert_frame_equal(original.loc[: index[80]], changed_features.loc[: index[80]])


def test_added_features_are_finite_and_directional_order_is_stable() -> None:
    index = pd.date_range("2026-02-02 10:00:00", periods=80, freq="1s", tz=ASIA_TOKYO)
    features = build_scalping_feature_frame(_bars(index), symbol="USD_JPY")
    directional = build_directional_feature_frame(features, side="long")

    assert list(directional.columns) == SCALPING_FEATURE_COLUMNS
    assert np.isfinite(directional.to_numpy()).all()
    assert features["is_tokyo_session"].iloc[0] == 1.0
    assert features["is_london_session"].iloc[0] == 0.0


def test_naive_scalping_feature_index_is_localized_to_tokyo() -> None:
    naive_index = pd.date_range("2026-02-02 10:00:00", periods=20, freq="1s")
    aware_index = naive_index.tz_localize(ASIA_TOKYO)

    naive_features = build_scalping_feature_frame(_bars(naive_index), symbol="USD_JPY")
    aware_features = build_scalping_feature_frame(_bars(aware_index), symbol="USD_JPY")

    assert str(naive_features.index.tz) == str(ASIA_TOKYO)
    pd.testing.assert_frame_equal(naive_features, aware_features)
