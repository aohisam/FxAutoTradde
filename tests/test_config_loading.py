from __future__ import annotations

import pytest
from pydantic import ValidationError

from fxautotrade_lab.backtest.scalping_backtest import training_config_from_app
from fxautotrade_lab.config.loader import load_app_config
from fxautotrade_lab.config.models import FxScalpingConfig
from tests.conftest import write_config


def test_load_app_config(tmp_path):
    config_path = write_config(tmp_path)
    config = load_app_config(config_path)
    assert config.watchlist.symbols == ["USD_JPY", "EUR_JPY"]
    assert config.strategy.name == "multi_timeframe_pattern_scoring"
    assert config.broker.mode.value == "local_sim"
    assert config.automation.reconnect_max_attempts == 3
    assert "log" in config.automation.notification_channels.channels


def test_load_scalping_strict_validation_config() -> None:
    config = load_app_config("configs/scalping_jforex.yaml")

    scalping = config.strategy.fx_scalping
    assert scalping.label_source == "tick"
    assert scalping.validation_ratio == 0.15
    assert scalping.test_ratio == 0.15
    assert scalping.walk_forward_enabled is True
    assert scalping.min_validation_profit_factor == 1.05
    assert scalping.min_validation_trade_count == 50
    assert scalping.min_test_profit_factor == 1.05
    assert scalping.min_walk_forward_pass_ratio == 0.6
    assert scalping.fail_closed_on_bad_validation is True
    assert scalping.threshold_grid == [
        0.52,
        0.54,
        0.56,
        0.58,
        0.60,
        0.62,
        0.64,
        0.66,
        0.68,
        0.70,
        0.72,
    ]
    assert training_config_from_app(config).threshold_grid == tuple(scalping.threshold_grid)
    assert scalping.record_rejected_signals is True
    assert scalping.blackout_windows_jst[0].start == "05:55"
    assert config.automation.enabled is False


def test_scalping_config_research_safety_warnings_are_japanese() -> None:
    loose = FxScalpingConfig(
        walk_forward_enabled=False,
        min_validation_profit_factor=1.0,
        min_validation_trade_count=1,
        spread_stress_multipliers=[1.0],
        latency_ms_grid=[250],
        threshold_selection_method="label",
        model_promotion_enabled=False,
    )
    warnings = loose.research_safety_warnings_ja()

    assert any("walk_forward_enabled=false" in warning for warning in warnings)
    assert any("min_validation_trade_count" in warning for warning in warnings)
    assert any("spread stress" in warning for warning in warnings)
    assert any("500ms" in warning for warning in warnings)

    safer = FxScalpingConfig(
        walk_forward_enabled=True,
        min_validation_profit_factor=1.05,
        min_validation_trade_count=50,
        spread_stress_multipliers=[1.0, 1.2, 1.5, 2.0],
        latency_ms_grid=[0, 250, 500, 1000],
        threshold_selection_method="replay",
        model_promotion_enabled=True,
    )
    assert safer.research_safety_warnings_ja() == []
    assert FxScalpingConfig().candidate_model_dir.as_posix() == "models/fx_scalping/candidates"


def test_scalping_threshold_grid_is_sorted_and_deduplicated() -> None:
    config = FxScalpingConfig(threshold_grid=[0.7, 0.54, 0.7, 0.52])

    assert config.threshold_grid == [0.52, 0.54, 0.7]


@pytest.mark.parametrize("threshold_grid", [[], [0.5, 1.0], [-0.1, 0.5], ["bad"]])
def test_scalping_threshold_grid_invalid_values_raise_japanese_error(
    threshold_grid: list[object],
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        FxScalpingConfig(threshold_grid=threshold_grid)

    message = str(exc_info.value)
    assert "スキャルピングML" in message
    assert "threshold_grid" in message
