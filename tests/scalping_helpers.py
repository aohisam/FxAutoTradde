from __future__ import annotations

import math
from collections.abc import Mapping

import pandas as pd

from fxautotrade_lab.features.scalping import SCALPING_FEATURE_COLUMNS
from fxautotrade_lab.ml.scalping import ScalpingModelBundle, ScalpingTrainingConfig


class ConstantProbabilityModel:
    feature_names = list(SCALPING_FEATURE_COLUMNS)
    metadata: dict[str, object] = {}

    def __init__(
        self, probability: float = 0.9, probabilities: Mapping[pd.Timestamp, float] | None = None
    ) -> None:
        self.probability = float(probability)
        self.probabilities = dict(probabilities or {})

    def predict_proba(self, features: pd.DataFrame) -> pd.Series:
        values = []
        for timestamp in features.index:
            values.append(float(self.probabilities.get(pd.Timestamp(timestamp), self.probability)))
        return pd.Series(values, index=features.index, dtype="float64")


def constant_bundle(probability: float = 0.9, threshold: float = 0.5) -> ScalpingModelBundle:
    return ScalpingModelBundle(
        model=ConstantProbabilityModel(probability),  # type: ignore[arg-type]
        decision_threshold=threshold,
        training_config=ScalpingTrainingConfig(),
        train_metrics={
            "threshold_selected_on": "validation",
            "train_sample_count": 10,
            "validation_sample_count": 5,
        },
    )


def neutral_features(
    index: pd.DatetimeIndex, *, spread: float = 0.1, volatility: float = 1.0
) -> pd.DataFrame:
    frame = pd.DataFrame(index=index)
    frame["return_1_pips"] = 0.2
    frame["return_3_pips"] = 0.2
    frame["return_10_pips"] = 0.2
    frame["breakout_up_20_pips"] = 0.1
    frame["breakout_down_20_pips"] = 0.1
    frame["range_position_20"] = 0.5
    frame["micro_volatility_10_pips"] = volatility
    frame["micro_volatility_30_pips"] = volatility
    frame["range_10_pips"] = 1.0
    frame["range_30_pips"] = 1.0
    frame["spread_close_pips"] = spread
    frame["spread_mean_20_pips"] = spread
    frame["spread_z_120"] = 0.0
    frame["spread_delta_1_pips"] = 0.0
    frame["spread_delta_3_pips"] = 0.0
    frame["spread_delta_10_pips"] = 0.0
    frame["spread_to_mean_20"] = 1.0
    frame["tick_count_log"] = math.log1p(1)
    frame["tick_count_mean_10"] = 1.0
    frame["tick_count_z_60"] = 0.0
    frame["bar_gap_seconds"] = 1.0
    frame["up_tick_ratio_10"] = 0.6
    frame["up_tick_ratio_30"] = 0.6
    frame["signed_tick_imbalance_10"] = 0.2
    frame["signed_tick_imbalance_30"] = 0.2
    frame["hour_sin"] = 0.0
    frame["hour_cos"] = 1.0
    frame["minute_sin"] = 0.0
    frame["minute_cos"] = 1.0
    frame["weekday_sin"] = 0.0
    frame["weekday_cos"] = 1.0
    frame["is_tokyo_session"] = 1.0
    frame["is_london_session"] = 0.0
    frame["is_newyork_session"] = 0.0
    frame["is_rollover_window"] = 0.0
    frame["is_weekend_or_illiquid_window"] = 0.0
    frame["is_month_start"] = 0.0
    frame["is_month_end"] = 0.0
    frame["mid_close"] = 150.0
    frame["bid_close"] = 149.999
    frame["ask_close"] = 150.001
    return frame


def simple_loss_ticks(index: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for timestamp in index:
        rows.append(
            {
                "timestamp": timestamp,
                "bid": 150.000,
                "ask": 150.001,
                "bid_volume": 1.0,
                "ask_volume": 1.0,
                "symbol": "USD_JPY",
            }
        )
        rows.append(
            {
                "timestamp": timestamp + pd.Timedelta(seconds=1),
                "bid": 149.999,
                "ask": 150.000,
                "bid_volume": 1.0,
                "ask_volume": 1.0,
                "symbol": "USD_JPY",
            }
        )
    frame = pd.DataFrame(rows).set_index("timestamp")
    return frame.sort_index()
