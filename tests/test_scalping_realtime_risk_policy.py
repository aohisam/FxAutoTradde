from __future__ import annotations

import pandas as pd
import pytest

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation import scalping_realtime as realtime_module
from fxautotrade_lab.simulation.scalping_policy import BlackoutWindow, ScalpingExecutionConfig
from fxautotrade_lab.simulation.scalping_realtime import ScalpingRealtimePaperEngine
from tests.scalping_helpers import constant_bundle, neutral_features


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


def _feed_with_mids(
    engine: ScalpingRealtimePaperEngine,
    *,
    start: str,
    mids: list[float],
) -> None:
    ts = pd.Timestamp(start).tz_convert(ASIA_TOKYO)
    for index, mid in enumerate(mids):
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
    rejected = next(
        signal for signal in engine.signals if signal["reject_reason"] == "blackout_window:news"
    )
    assert rejected["trades_today_before"] == rejected["trades_today_after"]
    assert rejected["daily_pnl_before"] == rejected["daily_pnl_after"]
    assert rejected["consecutive_losses_before"] == rejected["consecutive_losses_after"]
    assert rejected["trades_today"] == rejected["trades_today_before"]


def test_realtime_paper_records_daily_loss_halt_rejection() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            cooldown_seconds=0,
            max_daily_loss_amount=1.0,
        )
    )
    loss_time = pd.Timestamp("2026-02-02T08:59:00+09:00")
    engine.risk_state.record_trade(signal_time=loss_time, exit_time=loss_time, pnl=-2.0)

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == "daily_loss_halt" for signal in engine.all_signals)


def test_realtime_paper_records_consecutive_loss_halt_rejection() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            cooldown_seconds=0,
            max_consecutive_losses=1,
        )
    )
    loss_time = pd.Timestamp("2026-02-02T08:59:00+09:00")
    engine.risk_state.record_trade(signal_time=loss_time, exit_time=loss_time, pnl=-1.0)

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == "consecutive_loss_halt" for signal in engine.all_signals)


def test_realtime_paper_records_stale_tick_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    def stale_features(bars: pd.DataFrame, *, symbol: str, pip_size: float) -> pd.DataFrame:
        index = pd.DatetimeIndex([bars.index[-1] + pd.Timedelta(seconds=10)])
        return neutral_features(index)

    monkeypatch.setattr(realtime_module, "build_scalping_feature_frame", stale_features)
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            max_tick_gap_seconds=2,
        )
    )

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == "stale_tick" for signal in engine.all_signals)


@pytest.mark.parametrize(
    ("feature_overrides", "execution_config", "reason"),
    [
        (
            {"spread_z_120": 3.0},
            ScalpingExecutionConfig(
                starting_cash=100000.0,
                fixed_order_amount=150000.0,
                minimum_order_quantity=1,
                quantity_step=1,
                max_spread_z=2.0,
            ),
            "spread_z_exceeded",
        ),
        (
            {"spread_close_pips": 0.2, "spread_mean_20_pips": 0.1},
            ScalpingExecutionConfig(
                starting_cash=100000.0,
                fixed_order_amount=150000.0,
                minimum_order_quantity=1,
                quantity_step=1,
                max_spread_to_mean_ratio=1.5,
            ),
            "spread_to_mean_exceeded",
        ),
    ],
)
def test_realtime_paper_records_spread_context_rejections(
    monkeypatch: pytest.MonkeyPatch,
    feature_overrides: dict[str, float],
    execution_config: ScalpingExecutionConfig,
    reason: str,
) -> None:
    def feature_frame(bars: pd.DataFrame, *, symbol: str, pip_size: float) -> pd.DataFrame:
        index = pd.DatetimeIndex([bars.index[-1]])
        frame = neutral_features(index)
        for column, value in feature_overrides.items():
            frame[column] = value
        return frame

    monkeypatch.setattr(realtime_module, "build_scalping_feature_frame", feature_frame)
    engine = _engine(execution_config)

    _feed(engine, start="2026-02-02T09:00:00+09:00")

    assert any(signal["reject_reason"] == reason for signal in engine.all_signals)


def test_realtime_paper_signal_history_keeps_all_records_while_snapshot_is_recent() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
        )
    )
    timestamp = pd.Timestamp("2026-02-02T09:00:00+09:00")
    snapshot = engine.risk_state.snapshot(timestamp)
    for index in range(1_005):
        engine._record_signal(
            signal_id=f"s{index}",
            timestamp=timestamp + pd.Timedelta(seconds=index),
            side="long",
            accepted=False,
            reject_reason="threshold_not_met",
            probability=0.4,
            long_probability=0.4,
            short_probability=0.3,
            threshold=0.5,
            spread=0.1,
            spread_mean=0.1,
            spread_z=0.0,
            volatility=1.0,
            risk_snapshot_before=snapshot,
            risk_snapshot_after=snapshot,
        )

    assert len(engine.signals) == 1_000
    assert len(engine.snapshot()["signals"]) == 500
    assert len(engine.full_history()["signals"]) == 1_005


def test_realtime_paper_accepted_signal_after_state_updates_after_exit() -> None:
    engine = _engine(
        ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            entry_latency_ms=0,
            cooldown_seconds=0,
        )
    )
    mids = [150.0 + index * 0.001 for index in range(80)]

    _feed_with_mids(engine, start="2026-02-02T09:00:00+09:00", mids=mids)

    accepted = next(signal for signal in engine.all_signals if bool(signal["accepted"]))
    assert accepted["trades_today_before"] == 0
    assert accepted["trades_today_after"] >= 1
    assert "daily_pnl_after" in accepted
