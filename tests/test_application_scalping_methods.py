from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from fxautotrade_lab.application import LabApplication


def _write_scalping_config(tmp_path: Path) -> Path:
    payload = {
        "app_name": "FXAutoTrade Lab Scalping Test",
        "watchlist": {
            "symbols": ["USD_JPY"],
            "benchmark_symbols": ["USD_JPY"],
            "sector_symbols": [],
        },
        "data": {
            "source": "csv",
            "cache_dir": str(tmp_path / "data_cache"),
            "start_date": "2026-02-02T09:00:00+09:00",
            "end_date": "2026-02-02T09:08:00+09:00",
            "timeframes": ["1Min"],
        },
        "strategy": {
            "name": "fx_scalping",
            "entry_timeframe": "1Min",
            "fx_scalping": {
                "enabled": True,
                "tick_cache_dir": str(tmp_path / "tick_cache"),
                "model_dir": str(tmp_path / "models"),
                "candidate_model_dir": str(tmp_path / "models" / "candidates"),
                "bar_rule": "1s",
                "max_hold_seconds": 3,
                "entry_latency_ms": 0,
                "cooldown_seconds": 0,
                "take_profit_pips": 0.1,
                "stop_loss_pips": 0.1,
                "round_trip_slippage_pips": 0.0,
                "max_spread_pips": 1.0,
                "min_volatility_pips": 0.0,
                "min_samples": 4,
                "min_threshold_trades": 1,
                "min_validation_trade_count": 0,
                "min_validation_profit_factor": 0.0,
                "threshold_selection_method": "label",
                "model_promotion_enabled": False,
                "walk_forward_enabled": False,
                "min_walk_forward_pass_ratio": 0.0,
                "min_walk_forward_folds": 0,
                "min_stress_profit_factor": 0.0,
                "min_stress_net_profit": None,
                "outcome_store_enabled": False,
                "calibration_report_enabled": True,
                "spread_stress_multipliers": [1.0],
                "latency_ms_grid": [0],
            },
        },
        "broker": {"mode": "local_sim"},
        "risk": {
            "starting_cash": 100000.0,
            "fixed_order_amount": 150000.0,
            "minimum_order_quantity": 1,
            "quantity_step": 1,
            "max_symbol_exposure": 1.0,
        },
        "automation": {"enabled": False},
        "reporting": {"output_dir": str(tmp_path / "reports")},
        "persistence": {"sqlite_path": str(tmp_path / "runtime" / "lab.sqlite")},
    }
    path = tmp_path / "scalping.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_tick_csv(tmp_path: Path) -> Path:
    start = pd.Timestamp("2026-02-02T09:00:00+09:00")
    rows = []
    for index in range(480):
        timestamp = start + pd.Timedelta(seconds=index)
        mid = 150.0 + ((index % 20) - 10) * 0.001
        rows.append({"timestamp": timestamp.isoformat(), "bid": mid - 0.001, "ask": mid + 0.001})
    path = tmp_path / "USD_JPY_ticks.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_real_lab_application_exposes_scalping_cli_methods(tmp_path: Path) -> None:
    app = LabApplication(_write_scalping_config(tmp_path))

    for method_name in (
        "import_jforex_bid_ask_csv",
        "import_jforex_tick_csv",
        "run_scalping_backtest",
        "run_scalping_realtime_sim",
        "record_gmo_scalping_ticks",
        "load_scalping_outcome_summary",
    ):
        assert callable(getattr(app, method_name))


def test_import_tick_csv_then_scalping_backtest_smoke(tmp_path: Path) -> None:
    app = LabApplication(_write_scalping_config(tmp_path))
    tick_csv = _write_tick_csv(tmp_path)

    import_summary = app.import_jforex_tick_csv(file_path=tick_csv, symbol="USD_JPY")
    result = app.run_scalping_backtest(tick_file_path=None, symbol="USD_JPY")

    assert import_summary["imported_rows"] > 0
    assert result["run_id"]
    assert Path(str(result["candidate_model_path"])).exists()
    assert result["promoted_to_latest"] is True
    assert app.env.live_trading_enabled is False
