from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, TimeFrame

from tests.conftest import write_config


def test_application_test_gmo_connection_success(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    sample_frame = pd.DataFrame(
        {
            "open": [150.0, 150.1],
            "high": [150.2, 150.3],
            "low": [149.9, 150.0],
            "close": [150.1, 150.2],
            "volume": [1000.0, 1100.0],
        },
        index=pd.date_range("2026-04-14 09:00:00", periods=2, freq="15min", tz=ASIA_TOKYO),
    )

    monkeypatch.setattr(
        "fxautotrade_lab.application.GmoForexPublicClient.fetch_ticker_quotes",
        lambda self: [{"symbol": "USD_JPY", "bid": "150.10", "ask": "150.12"}],
    )
    monkeypatch.setattr(
        "fxautotrade_lab.application.GmoForexPublicClient.list_symbols",
        lambda self: [{"symbol": "USD_JPY", "minOpenOrderSize": "10000"}],
    )
    monkeypatch.setattr(
        "fxautotrade_lab.application.GmoForexPublicClient.fetch_bars",
        lambda self, symbol, timeframe, start, end, price_type="ASK": sample_frame,
    )

    result = app.test_gmo_connection()

    assert result["ok"] is True
    assert result["market_data_ok"] is True
    assert result["ticker_count"] == 1
    assert result["symbol_count"] == 1
    assert app.connection_test_results["public"]["ok"] is True


def test_application_load_credential_values_masks_private_credentials(tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.env = EnvironmentConfig().model_copy(
        update={
            "gmo_api_key": "GMO-12345678",
            "gmo_api_secret": "SECRET-5678",
        }
    )

    result = app.load_credential_values("private")

    assert result["configured"] is True
    assert result["source"] == "env"
    assert result["api_key_masked"] == "GMO-****5678"
    assert result["api_secret_masked"].endswith("5678")
    assert result["api_secret_masked"] != "SECRET-5678"


def test_application_update_runtime_mode_forces_gmo_source_in_runtime_mode(tmp_path):
    app = LabApplication(write_config(tmp_path))

    app.update_runtime_mode(
        broker_mode="gmo_sim",
        data_source="fixture",
        stream_enabled=True,
    )

    assert app.config.broker.mode.value == "gmo_sim"
    assert app.config.data.source == "gmo"
    assert app.config.data.stream_enabled is True

    app.update_runtime_mode(
        broker_mode="local_sim",
        data_source="fixture",
        stream_enabled=True,
    )

    assert app.config.broker.mode.value == "local_sim"
    assert app.config.data.source == "fixture"
    assert app.config.data.stream_enabled is False


def test_application_load_chart_dataset_reuses_cached_runtime_payload(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.config.data.source = "gmo"
    app.config.broker.mode = BrokerMode.GMO_SIM
    app.config.watchlist.symbols = ["USD_JPY"]
    app.config.watchlist.benchmark_symbols = []
    app.config.watchlist.sector_symbols = []
    calls = {"loads": 0, "features": 0, "signals": 0}

    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-04-14 10:00:00", tz=ASIA_TOKYO),
            pd.Timestamp("2026-04-14 10:15:00", tz=ASIA_TOKYO),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [150.0, 150.1],
            "high": [150.2, 150.3],
            "low": [149.9, 150.0],
            "close": [150.1, 150.2],
            "volume": [1000, 1200],
        },
        index=index,
    )

    class FakeMarketDataService:
        def __init__(self, config, env) -> None:  # noqa: ANN001
            _ = config, env

        def load_runtime_symbol_frames(self, symbol: str):  # noqa: ANN001
            calls["loads"] += 1
            _ = symbol
            return {
                app.config.strategy.entry_timeframe: frame,
                TimeFrame.DAY_1: frame,
            }

        load_symbol_frames = load_runtime_symbol_frames

    class DummyStrategy:
        def generate_signal_frame(self, working: pd.DataFrame) -> pd.DataFrame:
            calls["signals"] += 1
            return pd.DataFrame(
                {
                    "signal_action": ["hold"] * len(working.index),
                    "signal_score": [0.0] * len(working.index),
                },
                index=working.index,
            )

    def fake_build_features(symbol, bars_by_timeframe, benchmark_bars, sector_bars, config):  # noqa: ANN001
        calls["features"] += 1
        _ = symbol, bars_by_timeframe, benchmark_bars, sector_bars, config
        return SimpleNamespace(
            entry_frame=frame,
            daily_frame=frame,
            weekly_frame=frame,
            monthly_frame=frame,
        )

    monkeypatch.setattr("fxautotrade_lab.application.MarketDataService", FakeMarketDataService)
    monkeypatch.setattr("fxautotrade_lab.application.build_multi_timeframe_feature_set", fake_build_features)
    monkeypatch.setattr("fxautotrade_lab.application.create_strategy", lambda config: DummyStrategy())
    monkeypatch.setattr(
        LabApplication,
        "runtime_status_snapshot",
        lambda self, force_refresh=False, max_age_seconds=12.0: {
            "mode": "gmo_sim",
            "status": "running",
            "latest_market_bar_at": {"USD_JPY": "2026-04-14T10:15:00+09:00"},
            "recent_fills": [],
        },
    )

    first = app.load_chart_dataset("USD_JPY", app.config.strategy.entry_timeframe.value)
    second = app.load_chart_dataset("USD_JPY", app.config.strategy.entry_timeframe.value)
    third = app.load_chart_dataset("USD_JPY", app.config.strategy.entry_timeframe.value, force_refresh=True)

    assert not first["frame"].empty
    assert not second["frame"].empty
    assert not third["frame"].empty
    assert calls["loads"] == 2
    assert calls["features"] == 2
    assert calls["signals"] == 2
