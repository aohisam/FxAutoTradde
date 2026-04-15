from __future__ import annotations

import pytest

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.brokers.alpaca_live import AlpacaLiveBroker
from fxautotrade_lab.config.loader import load_environment
from fxautotrade_lab.core.enums import OrderSide

from tests.conftest import write_config


def test_sqlite_store_persists_run_tables(tmp_path):
    app = LabApplication(write_config(tmp_path))
    result = app.run_backtest()
    trades = app.load_run_table(result.run_id, "trades")
    signals = app.load_run_table(result.run_id, "signals")
    assert signals is not None
    assert result.run_id in {row["run_id"] for row in app.list_runs()}
    assert app.store.load_config_snapshot(result.run_id)
    assert trades is not None


def test_alpaca_live_requires_all_safety_gates():
    env = load_environment()
    broker = AlpacaLiveBroker(env)
    with pytest.raises(RuntimeError) as exc:
        broker.submit_market_order("AAPL", 1, OrderSide.BUY, "test")
    assert "安全フラグが不足しています" in str(exc.value)


def test_verify_broker_runtime_local_sim(tmp_path):
    app = LabApplication(write_config(tmp_path))
    summary = app.verify_broker_runtime()
    assert "account_summary" in summary
    assert "positions" in summary
    assert "orders" in summary
