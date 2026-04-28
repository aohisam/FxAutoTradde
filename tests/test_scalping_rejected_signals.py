from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import (
    BlackoutWindow,
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from tests.scalping_helpers import ConstantProbabilityModel, constant_bundle, neutral_features


def _ticks_for(index: pd.DatetimeIndex) -> pd.DataFrame:
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
                "bid": 150.020,
                "ask": 150.021,
                "bid_volume": 1.0,
                "ask_volume": 1.0,
                "symbol": "USD_JPY",
            }
        )
    return pd.DataFrame(rows).set_index("timestamp").sort_index()


def test_rejected_signals_have_distinct_reasons_and_future_columns_only_in_backtest() -> None:
    signal_index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:10",
            "2026-02-02 09:02:00",
            "2026-02-02 09:03:00",
            "2026-02-02 09:04:00",
            "2026-02-03 05:56:00",
        ],
        tz=ASIA_TOKYO,
    )
    features = neutral_features(signal_index)
    features.loc[signal_index[2], "spread_close_pips"] = 9.0
    features.loc[signal_index[3], "micro_volatility_10_pips"] = 0.0
    probabilities = {signal_index[4]: 0.4}
    bundle = constant_bundle()
    bundle.model = ConstantProbabilityModel(0.9, probabilities=probabilities)  # type: ignore[assignment]
    labels = pd.DataFrame(
        {
            "long_net_pips": 1.0,
            "short_net_pips": -1.0,
            "long_exit_reason": "take_profit",
            "short_exit_reason": "stop_loss",
        },
        index=signal_index,
    )

    result = run_scalping_tick_backtest(
        _ticks_for(signal_index),
        features,
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=bundle,
        training_config=ScalpingTrainingConfig(
            take_profit_pips=1,
            stop_loss_pips=1,
            max_hold_seconds=2,
            max_spread_pips=1,
            min_volatility_pips=0.1,
            round_trip_slippage_pips=0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=60,
            blackout_windows_jst=(BlackoutWindow("05:55", "06:10", "rollover"),),
        ),
        labels=labels,
        include_future_outcomes=True,
    )

    reasons = set(result.signals["reject_reason"])
    assert "accepted" in reasons
    assert "cooldown" in reasons
    assert "spread_exceeded" in reasons
    assert "volatility_too_low" in reasons
    assert "threshold_not_met" in reasons
    assert "blackout_window:rollover" in reasons
    assert "future_long_net_pips" in result.signals.columns

    no_future = run_scalping_tick_backtest(
        _ticks_for(pd.DatetimeIndex([signal_index[0]])),
        neutral_features(pd.DatetimeIndex([signal_index[0]])),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(max_hold_seconds=2),
        execution_config=ScalpingExecutionConfig(),
        labels=labels,
        include_future_outcomes=False,
    )
    assert "future_long_net_pips" not in no_future.signals.columns


def test_blackout_window_supports_same_day_and_cross_midnight() -> None:
    signal_index = pd.DatetimeIndex(["2026-02-02 21:30:00", "2026-02-03 00:05:00"], tz=ASIA_TOKYO)
    result = run_scalping_tick_backtest(
        _ticks_for(signal_index),
        neutral_features(signal_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(max_hold_seconds=2),
        execution_config=ScalpingExecutionConfig(
            blackout_windows_jst=(
                BlackoutWindow("21:25", "21:40", "manual_news_window"),
                BlackoutWindow("23:55", "00:10", "cross_midnight"),
            )
        ),
    )

    assert set(result.signals["reject_reason"]) == {
        "blackout_window:manual_news_window",
        "blackout_window:cross_midnight",
    }
