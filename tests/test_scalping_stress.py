from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.ticks import resample_ticks_to_quote_bars
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import ScalpingExecutionConfig
from fxautotrade_lab.simulation.scalping_stress import (
    run_scalping_stress_grid,
    stress_tick_spread,
)
from tests.scalping_helpers import constant_bundle


def _ticks() -> pd.DataFrame:
    index = pd.date_range("2026-02-02 09:00:00", periods=80, freq="1s", tz=ASIA_TOKYO)
    mid = pd.Series(150.0, index=index) + pd.Series(range(len(index)), index=index) * 0.0002
    return pd.DataFrame(
        {
            "bid": mid - 0.001,
            "ask": mid + 0.001,
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )


def test_spread_stress_preserves_valid_bid_ask_order() -> None:
    stressed = stress_tick_spread(_ticks(), multiplier=2.0, symbol="USD_JPY")

    assert (stressed["bid"] < stressed["ask"]).all()


def test_latency_grid_returns_multiple_stress_rows() -> None:
    ticks = _ticks()
    bars = resample_ticks_to_quote_bars(ticks, rule="1s", symbol="USD_JPY")
    summary = run_scalping_stress_grid(
        ticks,
        bars,
        symbol="USD_JPY",
        pip_size=0.01,
        bar_rule="1s",
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=0.1,
            stop_loss_pips=0.1,
            max_hold_seconds=5,
            min_volatility_pips=0.0,
            max_spread_pips=1.0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
        ),
        spread_multipliers=[1.0, 1.5],
        latency_ms_grid=[0, 500],
    )

    assert len(summary) == 4
    assert {
        "spread_multiplier",
        "entry_latency_ms",
        "number_of_trades",
        "net_profit",
        "profit_factor",
        "average_net_pips",
    }.issubset(summary.columns)
