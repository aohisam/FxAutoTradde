from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.backtest.scalping_backtest import evaluate_scalping_model_promotion
from fxautotrade_lab.config.models import FxScalpingConfig
from tests.test_application_scalping_methods import _write_scalping_config, _write_tick_csv


def _passing_stress() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "spread_multiplier": 1.5,
                "entry_latency_ms": 500,
                "profit_factor": 1.2,
                "net_profit": 100.0,
            }
        ]
    )


def _passing_walk_forward() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"profit_factor": 1.2, "number_of_trades": 120, "net_profit": 100.0},
            {"profit_factor": 1.1, "number_of_trades": 120, "net_profit": 50.0},
            {"profit_factor": 1.3, "number_of_trades": 120, "net_profit": 120.0},
        ]
    )


def test_promotion_success_and_validation_failure() -> None:
    config = FxScalpingConfig(
        min_test_trade_count=100,
        min_test_profit_factor=1.05,
        min_test_net_profit=0.0,
        min_stress_profit_factor=1.0,
        min_stress_net_profit=0.0,
        min_walk_forward_pass_ratio=0.6,
        min_walk_forward_folds=3,
    )

    promoted, reason, metrics = evaluate_scalping_model_promotion(
        test_metrics={
            "profit_factor": 1.2,
            "number_of_trades": 120,
            "net_profit": 10.0,
            "max_drawdown_amount": -10.0,
        },
        stress_results=_passing_stress(),
        walk_forward_results=_passing_walk_forward(),
        scalping_config=config,
    )
    assert promoted is True
    assert reason == ""
    assert metrics["required_stress_found"] is True

    promoted, reason, _ = evaluate_scalping_model_promotion(
        test_metrics={
            "profit_factor": 1.2,
            "number_of_trades": 120,
            "net_profit": 10.0,
            "max_drawdown_amount": -10.0,
        },
        stress_results=_passing_stress(),
        walk_forward_results=_passing_walk_forward(),
        scalping_config=config,
        validation_gate_passed=False,
    )
    assert promoted is False
    assert "validation gate failed" in reason


def test_failed_candidate_does_not_overwrite_previous_latest(tmp_path: Path) -> None:
    latest_path = tmp_path / "latest_scalping_model.json"
    previous_payload = {"previous": True}
    latest_path.write_text(json.dumps(previous_payload), encoding="utf-8")

    candidate_path = tmp_path / "candidates" / "run.json"
    candidate_path.parent.mkdir(parents=True)
    candidate_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "promoted_to_latest": False,
                    "promotion_gate_passed": False,
                    "promotion_reject_reasons_ja": ["test trade count が不足しています"],
                    "previous_latest_preserved": True,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert candidate_path.exists()
    assert json.loads(latest_path.read_text(encoding="utf-8")) == previous_payload


def test_run_scalping_pipeline_preserves_latest_when_promotion_fails(tmp_path: Path) -> None:
    config_path = _write_scalping_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    scalping = payload["strategy"]["fx_scalping"]
    scalping["model_promotion_enabled"] = True
    scalping["require_validation_gate_passed_for_promotion"] = False
    scalping["min_test_trade_count"] = 999
    scalping["outcome_store_enabled"] = False
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    latest_path = tmp_path / "models" / "latest_scalping_model.json"
    latest_payload = {"previous": True}
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(latest_payload), encoding="utf-8")

    app = LabApplication(config_path)
    app.import_jforex_tick_csv(file_path=_write_tick_csv(tmp_path), symbol="USD_JPY")
    result = app.run_scalping_backtest(symbol="USD_JPY")

    assert result["promoted_to_latest"] is False
    assert result["promotion_gate_passed"] is False
    assert result["promotion_reject_reason_ja"]
    assert Path(str(result["candidate_model_path"])).exists()
    assert json.loads(latest_path.read_text(encoding="utf-8")) == latest_payload
    assert result["metrics"]["promoted_to_latest"] is False
    assert result["metrics"]["promotion_gate_passed"] is False
    assert result["model_summary"]["metadata"]["promoted_to_latest"] is False


def test_run_scalping_pipeline_updates_latest_only_when_promotion_passes(tmp_path: Path) -> None:
    config_path = _write_scalping_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    scalping = payload["strategy"]["fx_scalping"]
    scalping.update(
        {
            "model_promotion_enabled": True,
            "require_validation_gate_passed_for_promotion": False,
            "min_test_trade_count": 0,
            "min_test_profit_factor": 0.0,
            "min_test_net_profit": -1_000_000.0,
            "min_stress_profit_factor": 0.0,
            "min_stress_net_profit": None,
            "required_stress_spread_multiplier": 1.0,
            "required_stress_latency_ms": 0,
            "min_walk_forward_pass_ratio": 0.0,
            "min_walk_forward_folds": 0,
            "spread_stress_multipliers": [1.0],
            "latency_ms_grid": [0],
            "outcome_store_enabled": False,
        }
    )
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    app = LabApplication(config_path)
    app.import_jforex_tick_csv(file_path=_write_tick_csv(tmp_path), symbol="USD_JPY")
    result = app.run_scalping_backtest(symbol="USD_JPY")

    latest_path = Path(str(result["latest_model_path"]))
    assert result["promoted_to_latest"] is True
    assert result["promotion_gate_passed"] is True
    assert latest_path.exists()
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["promoted_to_latest"] is True
