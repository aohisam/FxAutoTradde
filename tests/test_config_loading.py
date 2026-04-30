from __future__ import annotations

from fxautotrade_lab.config.loader import load_app_config
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
    assert scalping.record_rejected_signals is True
    assert scalping.blackout_windows_jst[0].start == "05:55"
    assert config.automation.enabled is False
