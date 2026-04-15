from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import US_EASTERN
from fxautotrade_lab.core.enums import BrokerMode, TimeFrame

from tests.conftest import write_config


def test_application_test_alpaca_connection_success(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    env = EnvironmentConfig().model_copy(
        update={
            "alpaca_paper_api_key": "PK-1234",
            "alpaca_paper_api_secret": "PS-5678",
        }
    )
    monkeypatch.setattr("fxautotrade_lab.application.load_environment", lambda: env)

    monkeypatch.setattr(
        "fxautotrade_lab.application.AlpacaTradingGateway.get_account_summary",
        lambda self: {
            "status": "ACTIVE",
            "equity": "100000",
            "buying_power": "200000",
        },
    )
    monkeypatch.setattr(
        "fxautotrade_lab.application.AlpacaTradingGateway.list_recent_orders",
        lambda self, limit=3: [{"order_id": "1"}, {"order_id": "2"}],
    )
    monkeypatch.setattr(
        "fxautotrade_lab.application.AlpacaHistoricalDataClient.fetch_bars",
        lambda self, symbol, timeframe, start, end: pd.DataFrame(
            {"open": [1.0], "high": [1.1], "low": [0.9], "close": [1.05], "volume": [1000]}
        ),
    )

    result = app.test_alpaca_connection("paper")
    assert result["ok"] is True
    assert result["account_status"] == "ACTIVE"
    assert result["market_data_ok"] is True
    assert result["recent_order_count"] == 2
    assert app.connection_test_results["paper"]["ok"] is True


def test_application_test_alpaca_connection_requires_credentials(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    empty_env = EnvironmentConfig().model_copy(
        update={
            "alpaca_paper_api_key": "",
            "alpaca_paper_api_secret": "",
            "alpaca_api_key": "",
            "alpaca_api_secret": "",
        }
    )
    app.env = empty_env
    monkeypatch.setattr("fxautotrade_lab.application.load_environment", lambda: empty_env)
    with pytest.raises(RuntimeError) as exc:
        app.test_alpaca_connection("paper")
    assert "保存してください" in str(exc.value)


def test_application_load_credential_values_masks_existing_credentials(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.env = app.env.model_copy(
        update={
            "alpaca_paper_api_key": "PK-12345678",
            "alpaca_paper_api_secret": "SECRET-5678",
        }
    )
    monkeypatch.setattr(
        app.keychain,
        "load_credentials",
        lambda profile: type(
            "Record",
            (),
            {"configured": False, "api_key": "", "api_secret": "", "source": "none"},
        )(),
    )

    result = app.load_credential_values("paper")
    assert result["configured"] is True
    assert result["source"] == "env"
    assert result["api_key_masked"] == "PK-1***5678"
    assert result["api_secret_masked"].endswith("5678")
    assert result["api_secret_masked"] != "SECRET-5678"


def test_application_update_runtime_mode_forces_alpaca_source_in_paper_mode(tmp_path):
    app = LabApplication(write_config(tmp_path))

    app.update_runtime_mode(
        broker_mode="alpaca_paper",
        data_source="fixture",
        stream_enabled=True,
    )

    assert app.config.broker.mode.value == "alpaca_paper"
    assert app.config.data.source == "alpaca"
    assert app.config.data.stream_enabled is True

    app.update_runtime_mode(
        broker_mode="local_sim",
        data_source="fixture",
        stream_enabled=True,
    )

    assert app.config.broker.mode.value == "local_sim"
    assert app.config.data.source == "fixture"
    assert app.config.data.stream_enabled is False


def test_application_update_order_sizing_persists_values(tmp_path):
    app = LabApplication(write_config(tmp_path))

    app.update_order_sizing(
        order_size_mode="equity_fraction",
        fixed_order_amount=2500.0,
        equity_fraction_per_trade=0.15,
        risk_per_trade=0.02,
    )

    assert app.config.risk.order_size_mode.value == "equity_fraction"
    assert app.config.risk.fixed_order_amount == 2500.0
    assert app.config.risk.equity_fraction_per_trade == 0.15
    assert app.config.risk.risk_per_trade == 0.02


def test_application_runtime_status_snapshot_caches_broker_verification(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.config.broker.mode = BrokerMode.ALPACA_PAPER
    app.config.data.source = "alpaca"
    calls = {"count": 0}

    def fake_verify(self) -> dict[str, object]:  # noqa: ANN001
        calls["count"] += 1
        return {
            "account_summary": {"status": "ACTIVE"},
            "positions": [{"symbol": "NVDA", "qty": "1"}],
            "orders": [{"order_id": "1"}],
            "fills": [{"fill_id": "1"}],
        }

    monkeypatch.setattr(LabApplication, "verify_broker_runtime", fake_verify)

    first = app.runtime_status_snapshot()
    second = app.runtime_status_snapshot()
    third = app.runtime_status_snapshot(force_refresh=True)

    assert first["positions"]
    assert second["positions"]
    assert third["positions"]
    assert calls["count"] == 2


def test_application_load_chart_dataset_reuses_cached_runtime_payload(monkeypatch, tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.config.data.source = "alpaca"
    app.config.broker.mode = BrokerMode.ALPACA_PAPER
    app.config.watchlist.symbols = ["AAPL"]
    app.config.watchlist.benchmark_symbols = []
    app.config.watchlist.sector_symbols = []
    calls = {"loads": 0, "features": 0, "signals": 0}

    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN),
            pd.Timestamp("2026-04-14 10:15:00", tz=US_EASTERN),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
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
            "mode": "alpaca_paper",
            "status": "running",
            "latest_market_bar_at": {"AAPL": "2026-04-14T10:15:00-04:00"},
            "recent_fills": [],
        },
    )

    first = app.load_chart_dataset("AAPL", app.config.strategy.entry_timeframe.value)
    second = app.load_chart_dataset("AAPL", app.config.strategy.entry_timeframe.value)
    third = app.load_chart_dataset("AAPL", app.config.strategy.entry_timeframe.value, force_refresh=True)

    assert not first["frame"].empty
    assert not second["frame"].empty
    assert not third["frame"].empty
    assert calls["loads"] == 2
    assert calls["features"] == 2
    assert calls["signals"] == 2
