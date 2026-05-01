from __future__ import annotations

import pandas as pd

from fxautotrade_lab.backtest import scalping_backtest
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import ScalpingBacktestResult
from fxautotrade_lab.simulation.scalping_policy import ScalpingExecutionConfig
from tests.scalping_helpers import constant_bundle, neutral_features


def test_replay_threshold_selection_uses_validation_tick_replay(monkeypatch) -> None:
    validation_index = pd.DatetimeIndex(
        ["2026-02-02 09:00:00", "2026-02-02 09:00:01"], tz=ASIA_TOKYO
    )
    validation_ticks = pd.DataFrame(
        {
            "bid": [150.0, 150.01],
            "ask": [150.001, 150.011],
            "bid_volume": [1.0, 1.0],
            "ask_volume": [1.0, 1.0],
            "symbol": ["USD_JPY", "USD_JPY"],
        },
        index=validation_index,
    )
    calls: list[float] = []

    def fake_replay(*args: object, **kwargs: object) -> ScalpingBacktestResult:
        assert args[0] is validation_ticks
        threshold = float(kwargs["model_bundle"].decision_threshold)
        calls.append(threshold)
        pips = [-3.0] if threshold == 0.5 else [2.0]
        trades = pd.DataFrame({"realized_net_pips": pips, "net_pnl": pips})
        return ScalpingBacktestResult(
            symbol="USD_JPY",
            metrics={"number_of_trades": 1},
            trades=trades,
            orders=pd.DataFrame(),
            fills=pd.DataFrame(),
            signals=pd.DataFrame(),
            equity_curve=pd.DataFrame(),
        )

    monkeypatch.setattr(scalping_backtest, "run_scalping_tick_backtest", fake_replay)

    threshold, metrics = scalping_backtest.select_decision_threshold_by_replay(
        validation_ticks,
        neutral_features(validation_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(
            threshold_grid=(0.5, 0.7),
            min_threshold_trades=1,
            min_validation_trade_count=1,
            min_validation_net_pips=0.0,
            min_validation_profit_factor=1.0,
        ),
        execution_config=ScalpingExecutionConfig(),
    )

    assert calls == [0.5, 0.7]
    assert threshold == 0.7
    assert metrics["threshold_candidates"]


def test_replay_threshold_selection_rejects_large_validation_drawdown(
    monkeypatch,
) -> None:
    validation_index = pd.DatetimeIndex(
        ["2026-02-02 09:00:00", "2026-02-02 09:00:01"], tz=ASIA_TOKYO
    )
    validation_ticks = pd.DataFrame(
        {
            "bid": [150.0, 150.01],
            "ask": [150.001, 150.011],
            "bid_volume": [1.0, 1.0],
            "ask_volume": [1.0, 1.0],
            "symbol": ["USD_JPY", "USD_JPY"],
        },
        index=validation_index,
    )

    def fake_replay(*args: object, **kwargs: object) -> ScalpingBacktestResult:
        threshold = float(kwargs["model_bundle"].decision_threshold)
        if threshold == 0.5:
            pips = [10.0]
            metrics = {"number_of_trades": 1, "max_drawdown_amount": -200.0}
        else:
            pips = [2.0]
            metrics = {"number_of_trades": 1, "max_drawdown_amount": -10.0}
        return ScalpingBacktestResult(
            symbol="USD_JPY",
            metrics=metrics,
            trades=pd.DataFrame({"realized_net_pips": pips, "net_pnl": pips}),
            orders=pd.DataFrame(),
            fills=pd.DataFrame(),
            signals=pd.DataFrame(),
            equity_curve=pd.DataFrame(),
        )

    monkeypatch.setattr(scalping_backtest, "run_scalping_tick_backtest", fake_replay)

    threshold, metrics = scalping_backtest.select_decision_threshold_by_replay(
        validation_ticks,
        neutral_features(validation_index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=ScalpingTrainingConfig(
            threshold_grid=(0.5, 0.7),
            min_threshold_trades=1,
            min_validation_trade_count=1,
            min_validation_net_pips=0.0,
            min_validation_profit_factor=1.0,
            max_validation_drawdown_amount=50.0,
        ),
        execution_config=ScalpingExecutionConfig(),
    )

    candidates = {
        candidate["threshold"]: candidate for candidate in metrics["threshold_candidates"]
    }
    assert threshold == 0.7
    assert candidates[0.5]["validation_gate_passed"] is False
    assert candidates[0.7]["validation_gate_passed"] is True


def test_replay_threshold_daily_loss_gate_fails_closed_with_warning(
    monkeypatch,
) -> None:
    validation_index = pd.DatetimeIndex(["2026-02-02 09:00:00"], tz=ASIA_TOKYO)
    features = neutral_features(validation_index)
    validation_ticks = pd.DataFrame(
        {
            "bid": [150.0],
            "ask": [150.001],
            "bid_volume": [1.0],
            "ask_volume": [1.0],
            "symbol": ["USD_JPY"],
        },
        index=validation_index,
    )

    def fake_select(*args: object, **kwargs: object) -> tuple[float, dict[str, object]]:
        return 0.6, {
            "candidate_count": 1,
            "selected_count": 1,
            "selected_net_pips": 10.0,
            "selected_mean_pips": 10.0,
            "selected_profit_factor": 2.0,
            "selected_max_drawdown_amount": 1.0,
            "selected_daily_max_loss": 100.0,
            "selected_max_drawdown_pips": -1.0,
            "objective": 10.0,
            "threshold_candidates": [],
        }

    monkeypatch.setattr(
        scalping_backtest,
        "select_decision_threshold_by_replay",
        fake_select,
    )

    updated = scalping_backtest._apply_validation_threshold_selection(
        constant_bundle(),
        train_features=features,
        train_labels=pd.DataFrame(),
        validation_ticks=validation_ticks,
        validation_features=features,
        validation_labels=pd.DataFrame(),
        symbol="USD_JPY",
        pip_size=0.01,
        training_config=ScalpingTrainingConfig(
            min_validation_trade_count=1,
            min_validation_net_pips=0.0,
            min_validation_profit_factor=1.0,
            max_validation_daily_loss_amount=50.0,
            fail_closed_on_bad_validation=True,
        ),
        execution_config=ScalpingExecutionConfig(),
        scalping_config=ScalpingTrainingConfig(threshold_selection_method="replay"),
    )

    assert updated.decision_threshold == 1.01
    assert updated.train_metrics["validation_gate_passed"] is False
    assert updated.train_metrics["validation_daily_loss_gate_passed"] is False
    assert "daily max loss" in str(updated.train_metrics["validation_drawdown_warning_ja"])
    assert "validation gate未達" in str(updated.train_metrics["warning_ja"])
    assert (
        updated.metadata["selected_threshold_validation_metrics"]["selected_daily_max_loss"]
        == 100.0
    )
