from __future__ import annotations

import pytest

from fxautotrade_lab.application import LabApplication
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


def test_verify_broker_runtime_local_sim(tmp_path):
    app = LabApplication(write_config(tmp_path))
    summary = app.verify_broker_runtime()
    assert "account_summary" in summary
    assert "positions" in summary
    assert "orders" in summary


def test_update_runtime_mode_rejects_unknown_mode(tmp_path):
    app = LabApplication(write_config(tmp_path))
    with pytest.raises(ValueError):
        app.update_runtime_mode(
            broker_mode="future_live", data_source="fixture", stream_enabled=False
        )
