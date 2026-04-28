from __future__ import annotations

import pandas as pd
import pytest

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig, build_tick_triple_barrier_labels
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from tests.scalping_helpers import constant_bundle, neutral_features


def test_tick_labels_use_latency_and_fee_like_tick_replay() -> None:
    index = pd.date_range("2026-02-03 09:00:00", periods=4, freq="1s", tz=ASIA_TOKYO)
    ticks = pd.DataFrame(
        {
            "bid": [150.000, 150.000, 150.015, 150.015],
            "ask": [150.002, 150.002, 150.017, 150.017],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )
    config = ScalpingTrainingConfig(
        take_profit_pips=1.0,
        stop_loss_pips=1.0,
        max_hold_seconds=5,
        round_trip_slippage_pips=0.0,
        fee_pips=0.2,
        max_spread_pips=1.0,
        min_volatility_pips=0.0,
    )
    sample_index = pd.DatetimeIndex([index[0]])

    labels = build_tick_triple_barrier_labels(
        ticks,
        sample_index=sample_index,
        pip_size=0.01,
        config=config,
        entry_latency_ms=1000,
        symbol="USD_JPY",
    )

    features = neutral_features(sample_index, spread=0.2, volatility=1.0)
    result = run_scalping_tick_backtest(
        ticks,
        features,
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(probability=0.9, threshold=0.5),
        training_config=config,
        execution_config=ScalpingExecutionConfig(
            starting_cash=10_000,
            fixed_order_amount=1_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=1000,
            cooldown_seconds=0,
            max_trades_per_day=10,
            record_rejected_signals=True,
        ),
        labels=labels,
        include_future_outcomes=True,
    )

    assert not result.signals.empty, result.signals.to_dict(orient="records")
    assert not result.trades.empty, result.signals.to_dict(orient="records")
    trade = result.trades.iloc[0]
    assert labels.loc[index[0], "long_exit_reason"] == "take_profit"
    assert labels.loc[index[0], "long_hold_seconds"] == 1.0
    assert labels.loc[index[0], "long_entry_time"] == index[1]
    assert labels.loc[index[0], "long_net_pips"] == pytest.approx(trade["realized_net_pips"])
    assert trade["realized_gross_pips"] > trade["realized_net_pips"]
    assert result.signals.loc[0, "future_long_net_pips"] == pytest.approx(
        trade["realized_net_pips"]
    )


def test_tick_labels_respect_real_time_horizon_with_sparse_ticks() -> None:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-02-03 09:00:00", tz=ASIA_TOKYO),
            pd.Timestamp("2026-02-03 09:00:01", tz=ASIA_TOKYO),
            pd.Timestamp("2026-02-03 09:00:10", tz=ASIA_TOKYO),
        ]
    )
    ticks = pd.DataFrame(
        {
            "bid": [150.000, 150.002, 150.050],
            "ask": [150.002, 150.004, 150.052],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )

    labels = build_tick_triple_barrier_labels(
        ticks,
        sample_index=pd.DatetimeIndex([index[0]]),
        pip_size=0.01,
        config=ScalpingTrainingConfig(
            take_profit_pips=5.0,
            stop_loss_pips=5.0,
            max_hold_seconds=2,
            round_trip_slippage_pips=0.0,
        ),
        entry_latency_ms=0,
        symbol="USD_JPY",
    )

    assert labels.loc[index[0], "long_exit_reason"] == "time_exit"
    assert labels.loc[index[0], "long_hold_seconds"] == 1.0
    assert labels.loc[index[0], "long_exit_time"] == index[1]
    assert labels.loc[index[0], "long_hold_ticks"] == 1
