from __future__ import annotations

import pandas as pd

from fxautotrade_lab.backtest.scalping_backtest import (
    evaluate_scalping_model_promotion,
    select_decision_threshold_by_replay,
)
from fxautotrade_lab.config.models import FxScalpingConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.reporting.scalping_calibration import build_probability_calibration_report
from fxautotrade_lab.simulation.scalping_engine import ScalpingExecutionConfig
from tests.scalping_helpers import constant_bundle, neutral_features


def test_select_decision_threshold_by_replay_uses_tick_engine_constraints() -> None:
    index = pd.DatetimeIndex(
        ["2026-02-02 09:00:00", "2026-02-02 09:00:05"],
        tz=ASIA_TOKYO,
    )
    ticks = pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "bid": 150.000,
                "ask": 150.001,
                "bid_volume": 1.0,
                "ask_volume": 1.0,
                "symbol": "USD_JPY",
            }
            for timestamp in index
        ]
        + [
            {
                "timestamp": timestamp + pd.Timedelta(seconds=1),
                "bid": 150.020,
                "ask": 150.021,
                "bid_volume": 1.0,
                "ask_volume": 1.0,
                "symbol": "USD_JPY",
            }
            for timestamp in index
        ]
    ).set_index("timestamp")
    threshold, metrics = select_decision_threshold_by_replay(
        ticks.sort_index(),
        neutral_features(index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(0.9, threshold=0.5),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=1.0,
            stop_loss_pips=1.0,
            max_hold_seconds=2,
            round_trip_slippage_pips=0.0,
            threshold_grid=(0.5, 0.95),
            min_threshold_trades=1,
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

    assert threshold == 0.5
    assert metrics["selected_count"] > 0
    assert metrics["selected_net_pips"] > 0


def test_model_promotion_gate_rejects_failed_candidate() -> None:
    scalping = FxScalpingConfig(
        min_test_profit_factor=1.05,
        min_test_trade_count=50,
        min_test_net_profit=0.0,
        max_test_drawdown_amount=100.0,
        min_stress_profit_factor=1.0,
        min_stress_net_profit=0.0,
        min_walk_forward_pass_ratio=0.6,
    )
    stress = pd.DataFrame([{"profit_factor": 0.9, "net_profit": -10.0}])
    walk_forward = pd.DataFrame(
        [
            {"profit_factor": 1.1, "number_of_trades": 60, "net_profit": 1.0},
            {"profit_factor": 0.8, "number_of_trades": 60, "net_profit": -1.0},
        ]
    )

    promoted, reason, metrics = evaluate_scalping_model_promotion(
        test_metrics={
            "profit_factor": 0.8,
            "number_of_trades": 10,
            "net_profit": -1.0,
            "max_drawdown_amount": -120.0,
        },
        stress_results=stress,
        walk_forward_results=walk_forward,
        scalping_config=scalping,
    )

    assert promoted is False
    assert "test profit factor" in reason
    assert "stress min profit factor" in reason
    assert metrics["walk_forward_pass_ratio"] == 0.5


def test_probability_calibration_report_outputs_deciles_and_brier_score() -> None:
    signals = pd.DataFrame(
        {
            "probability": [0.55, 0.75, 0.85],
            "chosen_side": ["long", "short", "long"],
            "future_long_net_pips": [1.0, -1.0, -0.5],
            "future_short_net_pips": [-1.0, 1.2, 0.5],
        }
    )
    trades = pd.DataFrame(
        {
            "probability": [0.55, 0.75, 0.85],
            "realized_net_pips": [1.0, 1.2, -0.5],
        }
    )

    report = build_probability_calibration_report(signals, trades)

    assert int(report.deciles["trade_count"].sum()) == 3
    assert report.metrics["brier_score"] is not None
    assert report.curve["signal_count"].sum() == 3
