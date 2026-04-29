from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from fxautotrade_lab.backtest.scalping_backtest import run_scalping_pipeline
from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.features.scalping import SCALPING_FEATURE_COLUMNS
from fxautotrade_lab.ml import scalping as scalping_module
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from tests.scalping_helpers import constant_bundle, neutral_features


def test_tick_replay_net_pnl_subtracts_round_trip_fee() -> None:
    index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:01",
            "2026-02-02 09:00:02",
        ],
        tz=ASIA_TOKYO,
    )
    ticks = pd.DataFrame(
        {
            "bid": [150.000, 150.020, 150.020],
            "ask": [150.001, 150.021, 150.021],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )
    result = run_scalping_tick_backtest(
        ticks,
        neutral_features(pd.DatetimeIndex([index[0]])),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(0.9, threshold=0.5),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=1.0,
            stop_loss_pips=1.0,
            max_hold_seconds=10,
            fee_pips=0.3,
            round_trip_slippage_pips=0.0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100_000,
            fixed_order_amount=150_000,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=0,
        ),
    )

    trade = result.trades.iloc[0]
    assert int(trade["quantity"]) > 0
    assert abs(float(trade["gross_pnl"])) > 0.0
    assert float(trade["fee_amount"]) > 0.0
    assert abs(float(trade["net_pnl"])) > 0.0
    assert trade["net_pnl"] < trade["gross_pnl"]
    assert trade["realized_net_pips"] == trade["realized_gross_pips"] - 0.3
    assert result.metrics["total_fee_pips"] == 0.3
    assert result.metrics["total_fee_amount"] > 0
    assert "average_gross_pips" in result.metrics
    assert "average_net_pips" in result.metrics


def test_full_scalping_backtest_keeps_positive_quantity_and_fee_amount(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    class AlwaysLongModel:
        feature_names = list(SCALPING_FEATURE_COLUMNS)
        metadata: dict[str, object] = {}

        def predict_proba(self, features: pd.DataFrame) -> pd.Series:
            return pd.Series(0.9, index=features.index, dtype="float64")

        def save(self, path: str | Path) -> Path:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps({"metadata": self.metadata}), encoding="utf-8")
            return target

    monkeypatch.setattr(
        scalping_module.NumpyLogisticRegression,
        "fit",
        lambda *args, **kwargs: AlwaysLongModel(),
    )
    config = AppConfig()
    scalping = config.strategy.fx_scalping
    scalping.tick_cache_dir = tmp_path / "ticks"
    scalping.model_dir = tmp_path / "models"
    scalping.min_samples = 5
    scalping.min_threshold_trades = 1
    scalping.min_volatility_pips = 0.0
    scalping.max_spread_pips = 2.0
    scalping.take_profit_pips = 0.3
    scalping.stop_loss_pips = 3.0
    scalping.max_hold_seconds = 8
    scalping.round_trip_slippage_pips = 0.0
    scalping.fee_pips = 0.4
    scalping.train_ratio = 0.55
    scalping.validation_ratio = 0.20
    scalping.test_ratio = 0.25
    scalping.entry_latency_ms = 0
    scalping.cooldown_seconds = 0
    scalping.min_validation_net_pips = -1_000_000.0
    scalping.min_validation_profit_factor = 0.0
    scalping.min_validation_trade_count = 1
    config.risk.starting_cash = 1_000_000.0
    config.risk.fixed_order_amount = 200_000.0
    config.risk.minimum_order_quantity = 1
    config.risk.quantity_step = 1

    index = pd.date_range("2026-02-02 09:00:00", periods=260, freq="1s", tz=ASIA_TOKYO)
    mid = 150.0 + np.arange(len(index), dtype="float64") * 0.004
    spread = 0.002
    ticks = pd.DataFrame(
        {
            "bid": mid - spread / 2.0,
            "ask": mid + spread / 2.0,
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )

    result = run_scalping_pipeline(
        ticks,
        symbol="USD_JPY",
        config=config,
        output_dir=tmp_path / "reports",
    )

    assert int(result.backtest.metrics["number_of_trades"]) > 0
    assert float(result.backtest.metrics["total_fee_amount"]) > 0.0
    assert (result.backtest.trades["quantity"].astype(int) > 0).all()
    assert (result.backtest.trades["fee_amount"].astype(float) > 0.0).all()
    assert (result.backtest.trades["gross_pnl"].abs() > 0.0).all()
    assert (result.backtest.trades["net_pnl"].abs() > 0.0).all()
    assert (result.backtest.trades["net_pnl"] != result.backtest.trades["gross_pnl"]).all()
