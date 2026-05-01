from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from fxautotrade_lab import application as application_module
from fxautotrade_lab.application import LabApplication
from tests.scalping_helpers import constant_bundle


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
    assert import_summary["cache_dir"]
    assert result["run_id"]
    assert Path(str(result["candidate_model_path"])).exists()
    assert "promotion_gate_passed" in result
    assert result["promoted_to_latest"] is True
    assert app.env.live_trading_enabled is False


def test_scalping_realtime_sim_uses_latest_paper_engine_and_appends_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_scalping_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["strategy"]["fx_scalping"]["outcome_store_enabled"] = True
    payload["strategy"]["fx_scalping"]["outcome_store_dir"] = str(tmp_path / "paper_outcomes")
    payload["strategy"]["fx_scalping"]["outcome_store_format"] = "csv"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    latest_path = tmp_path / "models" / "latest_scalping_model.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text("{}", encoding="utf-8")
    bundle = constant_bundle()
    bundle.metadata.update({"promoted_to_latest": True, "model_id": "approved_model"})

    class FakeClient:
        def __init__(self, env) -> None:  # noqa: ANN001
            assert env.live_trading_enabled is False

        def fetch_ticker_quotes(self) -> dict[str, object]:
            return {
                "USD_JPY": SimpleNamespace(
                    timestamp=pd.Timestamp("2026-02-02T09:00:00+09:00"),
                    bid=150.0,
                    ask=150.001,
                )
            }

    class FakePaperEngine:
        def __init__(self, **kwargs: object) -> None:
            self.symbol = kwargs["symbol"]

        def on_tick(self, **kwargs: object) -> list[dict[str, object]]:
            return []

        def snapshot(self) -> dict[str, object]:
            return {
                "symbol": self.symbol,
                "events": [],
                "signals": [
                    {
                        "signal_id": "paper-s1",
                        "timestamp": "2026-02-02T09:00:00+09:00",
                        "symbol": self.symbol,
                        "probability": 0.7,
                        "chosen_side": "long",
                        "accepted": False,
                        "reject_reason": "threshold_not_met",
                    }
                ],
                "trades": [],
            }

    monkeypatch.setattr(application_module, "load_scalping_model_bundle", lambda *a, **k: bundle)
    monkeypatch.setattr(application_module, "GmoForexPublicClient", FakeClient)
    monkeypatch.setattr(application_module, "ScalpingRealtimePaperEngine", FakePaperEngine)

    result = LabApplication(config_path).run_scalping_realtime_sim(
        symbol="USD_JPY",
        max_ticks=1,
        poll_seconds=0,
    )

    assert result["model_path"] == str(latest_path)
    assert result["outcome_store_summary"]["enabled"] is True
    assert result["outcome_store_summary"]["signals"] == 1
    assert result["trades"] == []


def test_scalping_realtime_sim_persists_full_history_not_recent_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_scalping_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["strategy"]["fx_scalping"]["outcome_store_enabled"] = True
    payload["strategy"]["fx_scalping"]["outcome_store_dir"] = str(tmp_path / "paper_outcomes")
    payload["strategy"]["fx_scalping"]["outcome_store_format"] = "csv"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    latest_path = tmp_path / "models" / "latest_scalping_model.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text("{}", encoding="utf-8")
    bundle = constant_bundle()
    bundle.metadata.update({"promoted_to_latest": True, "model_id": "approved_model"})

    class FakeClient:
        def __init__(self, env) -> None:  # noqa: ANN001
            assert env.live_trading_enabled is False

        def fetch_ticker_quotes(self) -> dict[str, object]:
            return {
                "USD_JPY": SimpleNamespace(
                    timestamp=pd.Timestamp("2026-02-02T09:00:00+09:00"),
                    bid=150.0,
                    ask=150.001,
                )
            }

    class FakePaperEngine:
        def __init__(self, **kwargs: object) -> None:
            self.symbol = str(kwargs["symbol"])
            self.full_signals = [
                {
                    "signal_id": f"paper-s{index}",
                    "timestamp": "2026-02-02T09:00:00+09:00",
                    "symbol": self.symbol,
                    "probability": 0.7,
                    "chosen_side": "long",
                    "accepted": False,
                    "reject_reason": "threshold_not_met",
                }
                for index in range(1_005)
            ]

        def on_tick(self, **kwargs: object) -> list[dict[str, object]]:
            return []

        def snapshot(self) -> dict[str, object]:
            return {
                "symbol": self.symbol,
                "events": [],
                "signals": self.full_signals[-500:],
                "trades": [],
            }

        def full_history(self) -> dict[str, object]:
            return {"symbol": self.symbol, "events": [], "signals": self.full_signals, "trades": []}

    monkeypatch.setattr(application_module, "load_scalping_model_bundle", lambda *a, **k: bundle)
    monkeypatch.setattr(application_module, "GmoForexPublicClient", FakeClient)
    monkeypatch.setattr(application_module, "ScalpingRealtimePaperEngine", FakePaperEngine)

    result = LabApplication(config_path).run_scalping_realtime_sim(
        symbol="USD_JPY",
        max_ticks=1,
        poll_seconds=0,
    )

    assert len(result["signals"]) == 500
    assert result["outcome_store_summary"]["signals"] == 1_005


def test_realtime_sim_does_not_use_candidate_without_latest(tmp_path: Path) -> None:
    config_path = _write_scalping_config(tmp_path)
    candidate_dir = tmp_path / "models" / "candidates"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    (candidate_dir / "candidate.json").write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="latest|学習済みモデル"):
        LabApplication(config_path).run_scalping_realtime_sim(
            symbol="USD_JPY",
            max_ticks=1,
            poll_seconds=0,
        )


def test_record_gmo_scalping_ticks_uses_public_recorder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: dict[str, object] = {}

    class FakeRecorder:
        def __init__(self, env: object, cache: object, *, symbol: str) -> None:
            created["env"] = env
            created["cache"] = cache
            created["symbol"] = symbol

        def run(self, *, max_ticks: int | None = None) -> dict[str, object]:
            return {"symbol": created["symbol"], "recorded_ticks": max_ticks or 0}

    monkeypatch.setattr(application_module, "GmoPublicWebSocketTickRecorder", FakeRecorder)

    result = LabApplication(_write_scalping_config(tmp_path)).record_gmo_scalping_ticks(
        symbol="USD_JPY",
        max_ticks=2,
        output_path=tmp_path / "shadow_ticks",
    )

    assert result["symbol"] == "USD_JPY"
    assert result["recorded_ticks"] == 2
    assert result["cache_dir"] == str(tmp_path / "shadow_ticks")
    env = created["env"]
    assert env.live_trading_enabled is False


def test_load_scalping_outcome_summary_empty_store(tmp_path: Path) -> None:
    summary = LabApplication(_write_scalping_config(tmp_path)).load_scalping_outcome_summary()

    assert summary["total_runs"] == 0
    assert summary["total_trades"] == 0
    assert summary["total_signals"] == 0
