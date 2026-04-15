from __future__ import annotations

from pathlib import Path

import pandas as pd

from fxautotrade_lab.application import LabApplication

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
    assert (output_dir / "config_snapshot.yaml").exists()
    assert (output_dir / "summary.md").exists()
    assert (output_dir / "monthly_returns.csv").exists()
    assert "total_return" in result.metrics
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
    assert result.equity_curve.index.min() >= pd.Timestamp("2024-02-01", tz="US/Eastern")
    assert result.equity_curve.index.max() < pd.Timestamp("2024-03-01", tz="US/Eastern")

    output_dir = Path(result.output_dir)
    summary_text = (output_dir / "summary.md").read_text(encoding="utf-8")
    report_text = (output_dir / "report.html").read_text(encoding="utf-8")
    assert "検証期間: 2024-02-01 - 2024-02-29" in summary_text
    assert "検証期間: 2024-02-01 - 2024-02-29" in report_text
