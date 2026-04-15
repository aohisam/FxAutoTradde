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
