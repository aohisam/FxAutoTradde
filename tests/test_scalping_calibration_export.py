from __future__ import annotations

import json

import pandas as pd

from fxautotrade_lab.backtest.scalping_backtest import evaluate_scalping_model_promotion
from fxautotrade_lab.config.models import FxScalpingConfig
from fxautotrade_lab.reporting.scalping_calibration import (
    build_probability_calibration_report,
    write_probability_calibration_report,
)


def test_calibration_artifacts_and_promotion_gate(tmp_path) -> None:
    signals = pd.DataFrame(
        {
            "probability": [0.95, 0.85, 0.65],
            "chosen_side": ["long", "long", "short"],
            "future_long_net_pips": [-1.0, -0.5, 1.0],
            "future_short_net_pips": [1.0, 0.5, -1.0],
        }
    )
    trades = pd.DataFrame({"probability": [0.95, 0.85], "realized_net_pips": [-1.0, -0.5]})
    report = build_probability_calibration_report(signals, trades)
    artifacts = write_probability_calibration_report(report, tmp_path)

    assert (tmp_path / artifacts["probability_deciles"]).exists()
    assert (tmp_path / artifacts["calibration_curve"]).exists()
    summary_path = tmp_path / artifacts["calibration_summary"]
    assert summary_path.exists()
    assert json.loads(summary_path.read_text(encoding="utf-8"))["brier_score"] is not None

    config = FxScalpingConfig(
        min_test_trade_count=0,
        min_test_profit_factor=0.0,
        min_test_net_profit=-1.0,
        min_stress_profit_factor=0.0,
        min_stress_net_profit=None,
        min_walk_forward_pass_ratio=0.0,
        min_walk_forward_folds=0,
        max_brier_score=0.01,
        calibration_fail_closed=True,
    )
    promoted, reason, _ = evaluate_scalping_model_promotion(
        test_metrics={"profit_factor": 1.0, "number_of_trades": 1, "net_profit": 1.0},
        stress_results=pd.DataFrame(
            [
                {
                    "spread_multiplier": 1.5,
                    "entry_latency_ms": 500,
                    "profit_factor": 1.0,
                    "net_profit": 1.0,
                }
            ]
        ),
        walk_forward_results=pd.DataFrame(),
        scalping_config=config,
        calibration_report=report,
    )

    assert promoted is False
    assert "Brier" in reason
