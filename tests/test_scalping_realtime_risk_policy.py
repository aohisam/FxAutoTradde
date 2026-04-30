from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_policy import BlackoutWindow, ScalpingExecutionConfig
from fxautotrade_lab.simulation.scalping_realtime import ScalpingRealtimePaperEngine
from tests.scalping_helpers import constant_bundle


def _engine(execution_config: ScalpingExecutionConfig) -> ScalpingRealtimePaperEngine:
    return ScalpingRealtimePaperEngine(
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(probability=0.9, threshold=0.5),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=0.1,
            stop_loss_pips=0.1,
            max_hold_seconds=2,
            max_spread_pips=1.0,
            min_volatility_pips=0.0,
            round_trip_slippage_pips=0.0,
        ),
        execution_config=execution_config,
        min_buffer_ticks=45,
    )


def _feed(engine: ScalpingRealtimePaperEngine, *, start: str) -> None:
    ts = pd.Timestamp(start).tz_convert(ASIA_TOKYO)
    for index in range(60):
        mid = 150.0 + index * 0.001
        engine.on_tick(
            timestamp=ts + pd.Timedelta(seconds=index),
            bid=mid - 0.001,
            ask=mid + 0.001,
        )


def test_realtime_paper_records_max_trades_rejection() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            max_trades_per_day=0,
        )
    )

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == "max_trades_per_day" for signal in engine.signals)


def test_realtime_paper_records_blackout_rejection() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            blackout_windows_jst=(BlackoutWindow("09:00", "09:10", "news"),),
        )
    )

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == "blackout_window:news" for signal in engine.signals)
