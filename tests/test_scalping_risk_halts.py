from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from tests.scalping_helpers import constant_bundle, neutral_features, simple_loss_ticks


def test_daily_loss_halt_stops_same_day_and_resets_next_day() -> None:
    signal_index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:10",
            "2026-02-03 09:00:00",
        ],
        tz=ASIA_TOKYO,
    )
    result = run_scalping_tick_backtest(
        simple_loss_ticks(signal_index),
        neutral_features(signal_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=5,
            stop_loss_pips=0.1,
            max_hold_seconds=5,
            round_trip_slippage_pips=0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=0,
            max_daily_loss_amount=0.1,
            max_trades_per_day=10,
        ),
    )

    assert "daily_loss_halt" in set(result.signals["reject_reason"])
    assert len(result.trades) == 2
    assert pd.Timestamp(result.trades.iloc[1]["entry_time"]).date().isoformat() == "2026-02-03"


def test_consecutive_loss_halt_stops_same_day_and_resets_next_day() -> None:
    signal_index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:10",
            "2026-02-03 09:00:00",
        ],
        tz=ASIA_TOKYO,
    )
    result = run_scalping_tick_backtest(
        simple_loss_ticks(signal_index),
        neutral_features(signal_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=5,
            stop_loss_pips=0.1,
            max_hold_seconds=5,
            round_trip_slippage_pips=0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=0,
            max_consecutive_losses=1,
            max_trades_per_day=10,
        ),
    )

    assert "consecutive_loss_halt" in set(result.signals["reject_reason"])
    assert len(result.trades) == 2


def test_stale_tick_rejects_entry() -> None:
    signal_index = pd.DatetimeIndex(["2026-02-02 09:00:10"], tz=ASIA_TOKYO)
    ticks = pd.DataFrame(
        {
            "bid": [150.000, 150.001],
            "ask": [150.001, 150.002],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=pd.DatetimeIndex(["2026-02-02 09:00:00", "2026-02-02 09:01:00"], tz=ASIA_TOKYO),
    )
    result = run_scalping_tick_backtest(
        ticks,
        neutral_features(signal_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(),
        execution_config=ScalpingExecutionConfig(
            max_tick_gap_seconds=5,
            reject_on_stale_ticks=True,
        ),
    )

    assert result.signals.iloc[0]["reject_reason"] == "stale_tick"
