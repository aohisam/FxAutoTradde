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
