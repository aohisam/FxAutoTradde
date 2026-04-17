from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.automation.controller import AutomationController
from fxautotrade_lab.backtest.fx_backtest import run_fx_backtest
from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide, TimeFrame
from fxautotrade_lab.ml.fx_filter import FEATURE_COLUMNS, apply_fx_ml_filter, fit_fx_filter_model, load_filter_model
from fxautotrade_lab.research.pipeline import ResearchPipeline


def _make_fx_config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "app_name": "FX ML Test",
            "watchlist": {
                "symbols": ["USD_JPY"],
                "benchmark_symbols": ["USD_JPY"],
                "sector_symbols": [],
            },
            "data": {
                "source": "csv",
                "cache_dir": str(tmp_path / "cache"),
                "start_date": "2026-01-01",
                "end_date": "2026-01-05",
                "timeframes": ["1Min", "15Min", "1Hour", "1Day", "1Week", "1Month"],
                "preferred_entry_timeframe": "1Min",
            },
            "strategy": {
                "name": "fx_breakout_pullback",
                "entry_timeframe": "1Min",
                "fx_breakout_pullback": {
                    "ema_fast": 3,
                    "ema_slow": 6,
                    "ema_slope_lookback": 1,
                    "adx_period": 3,
                    "atr_period": 3,
                    "breakout_lookback": 3,
                    "pullback_window_bars": 3,
                    "swing_lookback_bars": 3,
                    "tokyo_early_blackout_enabled": False,
                    "ml_filter": {
                        "enabled": True,
                        "backtest_mode": "walk_forward_train",
                        "min_samples": 1,
                        "max_iter": 50,
                        "walk_forward": {
                            "mode": "anchored",
                            "train_window": "2d",
                            "test_window": "1d",
                            "retrain_frequency": "1d",
                        },
                        "model_dir": str(tmp_path / "models"),
                        "dataset_dir": str(tmp_path / "datasets"),
                    },
                },
            },
            "broker": {"mode": "local_sim"},
            "automation": {"enabled": True, "poll_interval_seconds": 0, "sync_broker_state_each_cycle": False},
            "reporting": {"output_dir": str(tmp_path / "reports")},
            "research": {
                "output_dir": str(tmp_path / "research"),
                "cache_dir": str(tmp_path / "research_cache"),
            },
            "persistence": {"sqlite_path": str(tmp_path / "runtime" / "trading_lab.sqlite")},
        }
    )


def _synthetic_dataset(rows: int = 32) -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=rows, freq="1h", tz="Asia/Tokyo")
    dataset = pd.DataFrame(
        {
            "symbol": "USD_JPY",
            "signal_time": index,
            "entry_time": index + pd.Timedelta(minutes=1),
            "exit_time": index + pd.Timedelta(minutes=15),
            "realized_r_net": np.where(np.arange(rows) % 2 == 0, 1.2, -0.8),
            "binary_label": np.where(np.arange(rows) % 2 == 0, 1, 0),
            "continuous_target": np.where(np.arange(rows) % 2 == 0, 1.2, -0.8),
            "net_pnl": np.where(np.arange(rows) % 2 == 0, 1200.0, -800.0),
        }
    )
    for idx, column in enumerate(FEATURE_COLUMNS):
        dataset[column] = np.where(np.arange(rows) % 2 == 0, 1.0 + idx * 0.01, -1.0 - idx * 0.01)
    return dataset


def test_fx_ml_train_save_load_and_apply(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    dataset = _synthetic_dataset()

    model = fit_fx_filter_model(dataset, config)
    model_path = config.strategy.fx_breakout_pullback.ml_filter.model_dir / "unit_test_model.json"
    saved = model.save(model_path)
    loaded = load_filter_model(saved)

    assert loaded is not None
    signal_frame = pd.DataFrame(index=dataset["signal_time"])
    for column in FEATURE_COLUMNS:
        signal_frame[column] = dataset[column].to_numpy()
    signal_frame["entry_signal"] = True
    signal_frame["signal_score"] = 0.25
    signal_frame["explanation_ja"] = "rule"
    filtered = apply_fx_ml_filter(signal_frame, loaded, config, model_label="unit")

    assert "ml_probability" in filtered.columns
    assert "entry_signal_rule_only" in filtered.columns
    assert filtered["entry_signal_rule_only"].all()
    assert filtered["ml_probability"].between(0, 1).all()
    assert loaded.metadata["hyperparameters"]["learning_rate"] == config.strategy.fx_breakout_pullback.ml_filter.learning_rate
    assert loaded.metadata["hyperparameters"]["max_iter"] == config.strategy.fx_breakout_pullback.ml_filter.max_iter


def test_fx_walk_forward_windows_do_not_look_ahead(tmp_path: Path, monkeypatch) -> None:
    config = _make_fx_config(tmp_path)
    config.data.start_date = "2026-01-03"
    config.data.end_date = "2026-01-05"
    index = pd.date_range("2026-01-01 00:00:00", periods=60 * 24 * 5, freq="1min", tz="Asia/Tokyo")
    signal_frame = pd.DataFrame(index=index)
    signal_frame["symbol"] = "USD_JPY"
    signal_frame["close"] = np.linspace(100.0, 102.0, len(index))
    signal_frame["entry_signal"] = False
    signal_frame.loc[index[::240], "entry_signal"] = True
    signal_frame["signal_action"] = np.where(signal_frame["entry_signal"], "buy", "hold")
    signal_frame["signal_score"] = 0.5
    signal_frame["explanation_ja"] = "test"
    for column in FEATURE_COLUMNS:
        signal_frame[column] = 0.1

    train_windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    class _DummyModel:
        def predict_proba(self, features: pd.DataFrame) -> pd.Series:
            return pd.Series(0.9, index=features.index)

    def fake_build_symbol_signals(*args, **kwargs):
        return {"USD_JPY": signal_frame}, {"USD_JPY": {"1Min": signal_frame.copy()}}, {"USD_JPY": signal_frame.copy()}

    def fake_train_model(*args, **kwargs):
        train_windows.append((kwargs["train_start"], kwargs["train_end"]))
        return _DummyModel(), _synthetic_dataset(4), {"model_path": "", "latest_model_path": "", "dataset_path": ""}

    def fake_sim_run(self, signal_frames, mode=BrokerMode.LOCAL_SIM):
        ordered = next(iter(signal_frames.values())).index[-10:]
        equity = pd.DataFrame(
            {
                "cash": np.linspace(1_000_000, 1_010_000, len(ordered)),
                "equity": np.linspace(1_000_000, 1_010_000, len(ordered)),
                "exposure": 0.0,
            },
            index=ordered,
        )
        return {
            "equity_curve": equity,
            "orders": pd.DataFrame(),
            "fills": pd.DataFrame(columns=["timestamp", "price", "quantity"]),
            "trades": pd.DataFrame(columns=["entry_time", "exit_time", "net_pnl", "realized_r_net", "symbol", "hold_bars"]),
            "positions": pd.DataFrame(),
        }

    monkeypatch.setattr("fxautotrade_lab.backtest.fx_backtest._build_symbol_signals", fake_build_symbol_signals)
    monkeypatch.setattr("fxautotrade_lab.backtest.fx_backtest._train_model_from_history", fake_train_model)
    monkeypatch.setattr("fxautotrade_lab.backtest.fx_backtest.FxQuotePortfolioSimulator.run", fake_sim_run)

    result = run_fx_backtest(
        config,
        EnvironmentConfig(),
        backtest_start=config.data.start_date,
        backtest_end=config.data.end_date,
    )

    assert result.walk_forward
    assert train_windows
    for row in result.walk_forward:
        assert pd.Timestamp(row["train_end"]) <= pd.Timestamp(row["start"])


def test_research_pipeline_minimal_integration(tmp_path: Path, monkeypatch) -> None:
    config = _make_fx_config(tmp_path)

    fake_result = SimpleNamespace(
        metrics={"total_return": 0.1, "profit_factor": 1.4, "average_r": 0.25},
        output_dir=str(tmp_path / "fake_backtest"),
        trades=pd.DataFrame(
            {
                "exit_time": pd.to_datetime(["2026-01-01T01:00:00+09:00"]),
                "net_pnl": [1000.0],
                "realized_r_net": [0.5],
                "symbol": ["USD_JPY"],
            }
        ),
        signals=pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-01-01T00:00:00+09:00"]),
                "entry_signal_rule_only": [True],
                "entry_signal": [True],
            }
        ),
        equity_curve=pd.DataFrame(
            {"equity": [1_000_000.0, 1_010_000.0]},
            index=pd.to_datetime(["2026-01-01T00:00:00+09:00", "2026-01-02T00:00:00+09:00"]),
        ),
    )

    monkeypatch.setattr(ResearchPipeline, "_validate_data", lambda self: {"symbols": [{"symbol": "USD_JPY"}]})
    monkeypatch.setattr(ResearchPipeline, "_train_summary", lambda self: {"trained_rows": 8})
    monkeypatch.setattr(ResearchPipeline, "_run_backtest_variant", lambda self, **kwargs: fake_result)
    monkeypatch.setattr(ResearchPipeline, "_robustness_runs", lambda self, mode: {"rows": [{"spread_multiplier": 1.2}]})
    monkeypatch.setattr(ResearchPipeline, "_parameter_sensitivity", lambda self, mode: {"rows": [{"breakout_lookback": 20}]})

    summary = ResearchPipeline(config, EnvironmentConfig(), mode="quick").run()

    assert summary["run_id"]
    assert Path(summary["output_dir"]).exists()
    assert (Path(summary["output_dir"]) / "research_summary.json").exists()
    assert (Path(summary["output_dir"]) / "regime_summary.csv").exists()
    assert summary["uplift"]["total_return_delta"] == 0.0

    monkeypatch.setattr(
        ResearchPipeline,
        "_run_backtest_variant",
        lambda self, **kwargs: (_ for _ in ()).throw(AssertionError("backtest cache should be reused")),
    )
    summary_cached = ResearchPipeline(config, EnvironmentConfig(), mode="quick").run()

    cached_steps = {step["step"]: step["status"] for step in summary_cached["steps"]}
    assert cached_steps["baseline_backtest"] == "cached"
    assert cached_steps["selected_backtest"] == "cached"


def test_fx_automation_controller_smoke(tmp_path: Path, monkeypatch) -> None:
    config = _make_fx_config(tmp_path)
    controller = AutomationController(config, EnvironmentConfig())
    index = pd.date_range("2026-01-01 00:00:00", periods=2, freq="1min", tz="Asia/Tokyo")
    execution_frame = pd.DataFrame(
        {
            "symbol": ["USD_JPY", "USD_JPY"],
            "entry_signal": [False, True],
            "exit_signal": [False, False],
            "partial_exit_signal": [False, False],
            "signal_action": ["hold", "buy"],
            "signal_score": [0.0, 0.8],
            "explanation_ja": ["idle", "entry"],
            "entry_context_ok": [True, True],
            "entry_trigger_price": [np.nan, 101.0],
            "initial_stop_price": [np.nan, 99.5],
            "initial_risk_price": [np.nan, 1.5],
            "breakout_atr_15m": [0.5, 0.5],
            "atr_15m": [0.5, 0.5],
            "breakout_level_15m": [100.5, 100.5],
            "bid_open": [100.0, 100.8],
            "bid_high": [100.1, 101.0],
            "bid_low": [99.9, 100.7],
            "bid_close": [100.0, 100.9],
            "ask_open": [100.04, 100.84],
            "ask_high": [100.14, 101.14],
            "ask_low": [99.94, 100.74],
            "ask_close": [100.04, 100.94],
            "mid_open": [100.02, 100.82],
            "mid_high": [100.12, 101.07],
            "mid_low": [99.92, 100.72],
            "mid_close": [100.02, 100.92],
            "spread_open": [0.04, 0.04],
            "spread_high": [0.04, 0.04],
            "spread_low": [0.04, 0.04],
            "spread_close": [0.04, 0.04],
        },
        index=index,
    )

    monkeypatch.setattr(
        "fxautotrade_lab.automation.controller.build_fx_feature_set",
        lambda *args, **kwargs: SimpleNamespace(execution_frame=execution_frame),
    )
    monkeypatch.setattr(controller.strategy, "generate_signal_frame", lambda frame: frame)

    snapshots = [{"USD_JPY": {TimeFrame.MIN_1: execution_frame}}, {"USD_JPY": {TimeFrame.MIN_1: execution_frame}}]

    def fake_load_cycle_market_data(self):
        return snapshots.pop(0), None, None

    monkeypatch.setattr(AutomationController, "_load_cycle_market_data", fake_load_cycle_market_data)
    controller.run(max_cycles=1)

    assert controller.recent_signals


def test_fx_automation_controller_respects_entry_delay_bars(tmp_path: Path, monkeypatch) -> None:
    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.entry_delay_bars = 1
    controller = AutomationController(config, EnvironmentConfig())
    index = pd.date_range("2026-01-01 00:00:00", periods=3, freq="1min", tz="Asia/Tokyo")
    signal_frame = pd.DataFrame(
        {
            "entry_context_ok": [True, True, True],
            "bid_open": [100.00, 100.20, 100.80],
            "bid_high": [100.10, 100.40, 101.00],
            "bid_low": [99.90, 100.10, 100.70],
            "bid_close": [100.00, 100.30, 100.90],
            "ask_open": [100.04, 100.24, 100.84],
            "ask_high": [100.14, 100.44, 101.20],
            "ask_low": [99.94, 100.14, 100.74],
            "ask_close": [100.04, 100.34, 100.94],
            "breakout_atr_15m": [0.5, 0.5, 0.5],
            "atr_15m": [0.5, 0.5, 0.5],
        },
        index=index,
    )
    submitted: list[dict[str, object]] = []

    class _Broker:
        def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict[str, object]:
            order = {
                "order_id": f"{symbol}-{side.value}-{len(submitted) + 1}",
                "symbol": symbol,
                "qty": str(qty),
                "filled_qty": str(qty),
                "side": side.value,
                "status": "filled_local_sim",
                "reason": reason,
                "filled_avg_price": "101.00",
            }
            submitted.append(order)
            return order

    monkeypatch.setattr(
        AutomationController,
        "_entry_quantity_fx",
        lambda self, symbol, latest, entry_order_side: (1000, "1,000 通貨"),
    )
    monkeypatch.setattr(AutomationController, "_has_pending_order", lambda self, symbol: False)
    controller.broker = _Broker()

    controller.fx_pending_entries["USD_JPY"] = {
        "signal_time": index[0],
        "position_side": "long",
        "entry_order_side": "buy",
        "exit_order_side": "sell",
        "trigger_price": 101.00,
        "initial_stop_price": 99.50,
        "initial_risk_price": 1.50,
        "atr_at_entry": 0.50,
        "breakout_level": 100.50,
        "reason": "delay test",
        "score": 0.80,
    }

    controller._execute_pending_fx_orders("USD_JPY", signal_frame.iloc[:2], signal_frame.iloc[1], index[1])

    assert not submitted
    assert "USD_JPY" in controller.fx_pending_entries

    controller._execute_pending_fx_orders("USD_JPY", signal_frame.iloc[:3], signal_frame.iloc[2], index[2])

    assert len(submitted) == 1
    assert submitted[0]["side"] == "buy"
    assert "USD_JPY" not in controller.fx_pending_entries
    assert "USD_JPY" in controller.open_symbols


def test_fx_automation_controller_retrains_model_on_schedule(tmp_path: Path, monkeypatch) -> None:
    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.ml_filter.enabled = True
    config.strategy.fx_breakout_pullback.ml_filter.realtime_retrain_enabled = True
    config.strategy.fx_breakout_pullback.ml_filter.realtime_retrain_frequency = "1d"
    controller = AutomationController(config, EnvironmentConfig())
    index = pd.date_range("2026-01-01 00:00:00", periods=2, freq="1min", tz="Asia/Tokyo")
    execution_frame = pd.DataFrame(
        {
            "symbol": ["USD_JPY", "USD_JPY"],
            "entry_signal": [False, False],
            "exit_signal": [False, False],
            "partial_exit_signal": [False, False],
            "signal_action": ["hold", "hold"],
            "signal_score": [0.0, 0.0],
            "explanation_ja": ["idle", "idle"],
            "entry_context_ok": [True, True],
            "bid_open": [100.0, 100.1],
            "bid_high": [100.2, 100.3],
            "bid_low": [99.9, 100.0],
            "bid_close": [100.1, 100.2],
            "ask_open": [100.04, 100.14],
            "ask_high": [100.24, 100.34],
            "ask_low": [99.94, 100.04],
            "ask_close": [100.14, 100.24],
            "mid_open": [100.02, 100.12],
            "mid_high": [100.22, 100.32],
            "mid_low": [99.92, 100.02],
            "mid_close": [100.12, 100.22],
            "spread_open": [0.04, 0.04],
            "spread_high": [0.04, 0.04],
            "spread_low": [0.04, 0.04],
            "spread_close": [0.04, 0.04],
            "regime_label": ["trend_strong", "trend_strong"],
        },
        index=index,
    )

    retrain_calls: list[str] = []

    monkeypatch.setattr(
        "fxautotrade_lab.automation.controller.build_fx_feature_set",
        lambda *args, **kwargs: SimpleNamespace(execution_frame=execution_frame),
    )
    monkeypatch.setattr(controller.strategy, "generate_signal_frame", lambda frame: frame)
    monkeypatch.setattr(
        "fxautotrade_lab.automation.controller.train_fx_filter_model_run",
        lambda config, env, as_of=None: retrain_calls.append(str(as_of)) or {"trained_rows": 4, "model_path": "", "latest_model_path": ""},
    )

    snapshots = [{"USD_JPY": {TimeFrame.MIN_1: execution_frame}}]

    def fake_load_cycle_market_data(self):
        return snapshots.pop(0), None, None

    monkeypatch.setattr(AutomationController, "_load_cycle_market_data", fake_load_cycle_market_data)
    controller.run(max_cycles=1)

    assert retrain_calls
    assert controller.fx_next_retrain_at is not None


def test_fx_automation_controller_enforces_jpy_cross_limit(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.watchlist.symbols = ["USD_JPY", "EUR_JPY"]
    config.risk.max_positions = 3
    config.risk.minimum_order_quantity = 1
    config.risk.fixed_order_amount = 500000
    controller = AutomationController(config, EnvironmentConfig())
    controller.account_summary = {
        "equity": "1000000",
        "portfolio_value": "1000000",
        "cash": "1000000",
        "buying_power": "1000000",
    }
    controller.open_symbols = set()
    controller.fx_position_state = {
        "USD_JPY": {
            "quantity": 1000,
            "position_side": "long",
        }
    }
    latest = pd.Series(
        {
            "ask_close": 101.04,
            "bid_close": 101.00,
            "breakout_atr_15m": 0.4,
            "atr_15m": 0.4,
        }
    )

    quantity, message = controller._entry_quantity_fx("EUR_JPY", latest, entry_order_side=OrderSide.BUY)

    assert quantity == 0
    assert "JPY クロス保有上限" in message


def test_application_exposes_research_and_training(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
app_name: FXApp
watchlist:
  symbols: ["USD_JPY"]
  benchmark_symbols: ["USD_JPY"]
  sector_symbols: []
data:
  source: csv
  cache_dir: data_cache
  start_date: "2026-01-01"
  end_date: "2026-01-02"
  timeframes: ["1Min", "15Min", "1Hour", "1Day", "1Week", "1Month"]
strategy:
  name: fx_breakout_pullback
  entry_timeframe: "1Min"
broker:
  mode: local_sim
reporting:
  output_dir: reports
research:
  output_dir: research_runs
  cache_dir: research_cache
""",
        encoding="utf-8",
    )
    app = LabApplication(config_path)
    monkeypatch.setattr("fxautotrade_lab.application.train_fx_filter_model_run", lambda *args, **kwargs: {"trained_rows": 1})
    monkeypatch.setattr("fxautotrade_lab.application.ResearchPipeline.run", lambda self: {"run_id": "r1", "output_dir": "x"})
    assert app.train_fx_model()["trained_rows"] == 1
    assert app.run_research()["run_id"] == "r1"
