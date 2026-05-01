from __future__ import annotations

from pathlib import Path

import pandas as pd

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.reporting.signal_snapshot import (
    build_signal_snapshot_payload,
    load_signal_snapshot_artifacts,
)
from tests.conftest import write_config


def test_backtest_and_report_export(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    result = app.run_backtest()
    assert result.output_dir is not None
    output_dir = Path(result.output_dir)
    assert (output_dir / "report.html").exists()
    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "trades.csv").exists()
    assert (output_dir / "orders.csv").exists()
    assert (output_dir / "fills.csv").exists()
    assert (output_dir / "positions.csv").exists()
    assert (output_dir / "equity_curve.csv").exists()
    assert (output_dir / "drawdown.csv").exists()
    assert (output_dir / "signal_log.csv").exists()
    assert (output_dir / "signal_snapshot.json").exists()
    assert (output_dir / "signal_recent.csv").exists()
    assert (output_dir / "signal_symbols.csv").exists()
    assert (output_dir / "config_snapshot.yaml").exists()
    assert (output_dir / "summary.md").exists()
    assert (output_dir / "monthly_returns.csv").exists()
    assert "total_return" in result.metrics
    assert "net_profit" in result.metrics
    assert "ending_equity" in result.metrics
    runs = app.list_runs()
    assert any(row["run_id"] == result.run_id for row in runs)
    loaded_signals = app.load_run_table(result.run_id, "signals")
    assert not loaded_signals.empty


def test_demo_run_produces_logs(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    summary = app.run_demo()
    assert summary["logs"]
    assert app.list_runs()


def test_backtest_uses_custom_window_and_exports_period(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    app.config.backtest.use_custom_window = True
    app.config.backtest.start_date = "2024-02-01"
    app.config.backtest.end_date = "2024-02-29"

    result = app.run_backtest()

    assert result.backtest_start == "2024-02-01"
    assert result.backtest_end == "2024-02-29"
    assert not result.equity_curve.empty
    assert result.equity_curve.index.min() >= pd.Timestamp("2024-02-01", tz="Asia/Tokyo")
    assert result.equity_curve.index.max() < pd.Timestamp("2024-03-01", tz="Asia/Tokyo")

    output_dir = Path(result.output_dir)
    summary_text = (output_dir / "summary.md").read_text(encoding="utf-8")
    report_text = (output_dir / "report.html").read_text(encoding="utf-8")
    assert "検証期間: 2024-02-01 - 2024-02-29" in summary_text
    assert "検証期間: 2024-02-01 - 2024-02-29" in report_text


def test_application_load_saved_backtest_result_restores_latest_run(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    result = app.run_backtest()
    assert len(result.signals) <= 300

    restarted = LabApplication(config_path)
    restored = restarted.load_saved_backtest_result()

    assert restored is not None
    assert restarted.last_result is not None
    assert restored.run_id == result.run_id
    assert restarted.last_result.metrics["number_of_trades"] == result.metrics["number_of_trades"]
    assert not restarted.last_result.signals.empty
    assert len(restarted.last_result.signals) <= 300


def test_list_backtest_runs_exposes_ml_usage_for_saved_run(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    result = app.run_backtest()

    runs = app.list_backtest_runs()

    latest = next(row for row in runs if row["run_id"] == result.run_id)
    assert latest["is_latest"] is True
    assert latest["display_label"]
    assert latest["strategy_label"]
    assert latest["ml_mode"] == "rule_only"
    assert latest["ml_model_label"] == "ML未使用"
    loaded_signals = app.load_saved_run_signals(result.run_id)
    assert not loaded_signals.empty


def test_saved_run_signal_snapshot_is_compact_and_aggregated(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    result = app.run_backtest()

    snapshot = app.load_saved_run_signal_snapshot(result.run_id)

    assert int(snapshot["summary"]["total"]) >= len(snapshot["recent_signals"])
    assert len(snapshot["recent_signals"]) <= 300
    assert set(snapshot["histogram"].keys()) == {"all", "accepted", "rejected"}
    assert len(snapshot["histogram"]["all"]) == 11
    assert snapshot["symbol_frame"] is not None


def test_saved_run_signal_snapshot_prefers_exported_artifacts(tmp_path):
    config_path = write_config(tmp_path)
    app = LabApplication(config_path)
    result = app.run_backtest()

    output_dir = Path(result.output_dir)
    assert (output_dir / "signal_snapshot.json").exists()
    assert (output_dir / "signal_recent.csv").exists()
    assert (output_dir / "signal_symbols.csv").exists()

    app._saved_signal_snapshot_cache.clear()
    snapshot = app.load_saved_run_signal_snapshot(result.run_id)

    assert int(snapshot["summary"]["total"]) >= len(snapshot["recent_signals"])
    assert len(snapshot["recent_signals"]) <= 300


def test_signal_snapshot_enriches_recent_signals_with_trade_sizing(tmp_path):
    signal_time = pd.Timestamp("2026-02-27T16:16:00+09:00")
    signals = pd.DataFrame(
        {
            "timestamp": [signal_time],
            "symbol": ["EUR_JPY"],
            "signal_action": ["buy"],
            "signal_score": [0.68],
        }
    )
    trades = pd.DataFrame(
        {
            "symbol": ["EUR_JPY"],
            "signal_time": [signal_time],
            "entry_time": [signal_time + pd.Timedelta(minutes=1)],
            "exit_time": [signal_time + pd.Timedelta(minutes=45)],
            "entry_order_side": ["buy"],
            "initial_quantity": [135],
            "entry_price": [184.13461605],
            "initial_risk_price": [0.1988903453174089],
            "net_pnl": [38.4367815],
        }
    )

    snapshot = build_signal_snapshot_payload(signals, trades=trades)
    recent = snapshot["recent_signals"]

    assert float(recent.loc[0, "trade_entry_notional_jpy"]) == 135 * 184.13461605
    assert float(recent.loc[0, "trade_initial_risk_jpy"]) == 135 * 0.1988903453174089
    assert float(recent.loc[0, "trade_net_pnl_jpy"]) == 38.4367815


def test_legacy_signal_snapshot_artifacts_are_enriched_from_trades_csv(tmp_path):
    output_dir = tmp_path / "report"
    output_dir.mkdir()
    signal_time = "2026-02-20 18:01:00+09:00"
    (output_dir / "signal_snapshot.json").write_text(
        '{"summary": {"total": 1}, "histogram": {"all": [1], "accepted": [1], "rejected": [0]}}',
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": [signal_time],
            "symbol": ["AUD_JPY"],
            "signal_action": ["buy"],
            "signal_score": [0.5],
        }
    ).to_csv(output_dir / "signal_recent.csv", index=False)
    pd.DataFrame({"通貨ペア": ["AUD_JPY"], "総数": [1], "採用": [1]}).to_csv(
        output_dir / "signal_symbols.csv",
        index=False,
    )
    pd.DataFrame(
        {
            "symbol": ["AUD_JPY"],
            "signal_time": [signal_time],
            "entry_order_side": ["buy"],
            "initial_quantity": [227],
            "entry_price": [109.82147075],
            "initial_risk_price": [0.2270604424435021],
            "net_pnl": [-36.0754302],
        }
    ).to_csv(output_dir / "trades.csv", index=False)

    snapshot = load_signal_snapshot_artifacts(output_dir)
    assert snapshot is not None
    recent = snapshot["recent_signals"]
    assert float(recent.loc[0, "trade_entry_notional_jpy"]) == 227 * 109.82147075
