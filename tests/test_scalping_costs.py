from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from tests.scalping_helpers import constant_bundle, neutral_features


def test_tick_replay_net_pnl_subtracts_round_trip_fee() -> None:
    index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:01",
            "2026-02-02 09:00:02",
        ],
        tz=ASIA_TOKYO,
    )
    ticks = pd.DataFrame(
        {
            "bid": [150.000, 150.020, 150.020],
            "ask": [150.001, 150.021, 150.021],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )
    result = run_scalping_tick_backtest(
        ticks,
        neutral_features(pd.DatetimeIndex([index[0]])),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(0.9, threshold=0.5),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=1.0,
            stop_loss_pips=1.0,
            max_hold_seconds=10,
            fee_pips=0.3,
            round_trip_slippage_pips=0.0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=0,
        ),
    )

    trade = result.trades.iloc[0]
    assert trade["net_pnl"] < trade["gross_pnl"]
    assert trade["realized_net_pips"] == trade["realized_gross_pips"] - 0.3
    assert result.metrics["total_fee_pips"] == 0.3
    assert result.metrics["total_fee_amount"] > 0
    assert "average_gross_pips" in result.metrics
    assert "average_net_pips" in result.metrics
