from __future__ import annotations

import pandas as pd
import pytest

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import run_scalping_tick_backtest
from fxautotrade_lab.simulation.scalping_policy import (
    BlackoutWindow,
    ScalpingExecutionConfig,
    ScalpingSignalContext,
    ScalpingSignalPolicy,
)
from fxautotrade_lab.simulation.scalping_realtime import ScalpingRealtimePaperEngine
from tests.scalping_helpers import constant_bundle, neutral_features, simple_loss_ticks


def _policy() -> ScalpingSignalPolicy:
    return ScalpingSignalPolicy(
        training_config=ScalpingTrainingConfig(
            max_spread_pips=0.5,
            min_volatility_pips=0.1,
        ),
        execution_config=ScalpingExecutionConfig(
            max_tick_gap_seconds=2,
            max_spread_z=2.0,
            max_spread_to_mean_ratio=1.5,
            blackout_windows_jst=(BlackoutWindow("09:00", "09:10", "news"),),
        ),
    )


def test_shared_policy_returns_same_reject_reason_for_backtest_and_paper_contexts() -> None:
    timestamp = pd.Timestamp("2026-02-02T09:30:00+09:00")
    tick_index = pd.DatetimeIndex([timestamp - pd.Timedelta(seconds=1)], tz=ASIA_TOKYO)
    policy = _policy()
    context = ScalpingSignalContext(
        timestamp=timestamp,
        tick_index=tick_index,
        spread=0.1,
        spread_mean=0.1,
        spread_z=3.0,
        volatility=1.0,
        probability=0.9,
        threshold=0.5,
        chosen_side="long",
    )

    decision = policy.decide_entry(context)

    assert decision.accepted is False
    assert decision.reject_reason == "spread_z_exceeded"


def test_policy_rejects_blackout_stale_threshold_and_quantity() -> None:
    policy = _policy()
    timestamp = pd.Timestamp("2026-02-02T09:05:00+09:00")
    tick_index = pd.DatetimeIndex([timestamp - pd.Timedelta(seconds=10)], tz=ASIA_TOKYO)
    fresh_time = pd.Timestamp("2026-02-02T09:30:00+09:00")
    fresh_index = pd.DatetimeIndex([fresh_time], tz=ASIA_TOKYO)

    assert (
        policy.decide_entry(
            ScalpingSignalContext(
                timestamp=timestamp,
                tick_index=tick_index,
                spread=0.1,
                spread_mean=0.1,
                spread_z=0.0,
                volatility=1.0,
                probability=0.9,
                threshold=0.5,
            )
        ).reject_reason
        == "blackout_window:news"
    )
    assert (
        policy.decide_entry(
            ScalpingSignalContext(
                timestamp=pd.Timestamp("2026-02-02T09:30:00+09:00"),
                tick_index=tick_index,
                spread=0.1,
                spread_mean=0.1,
                spread_z=0.0,
                volatility=1.0,
                probability=0.4,
                threshold=0.5,
            )
        ).reject_reason
        == "stale_tick"
    )
    assert (
        policy.decide_entry(
            ScalpingSignalContext(
                timestamp=fresh_time,
                tick_index=fresh_index,
                spread=0.1,
                spread_mean=0.1,
                spread_z=0.0,
                volatility=1.0,
                probability=0.4,
                threshold=0.5,
            )
        ).reject_reason
        == "threshold_not_met"
    )
    assert (
        policy.decide_entry(
            ScalpingSignalContext(
                timestamp=fresh_time,
                tick_index=fresh_index,
                spread=0.1,
                spread_mean=0.1,
                spread_z=0.0,
                volatility=1.0,
                probability=0.9,
                threshold=0.5,
                quantity=0,
            )
        ).reject_reason
        == "quantity_too_small"
    )


@pytest.mark.parametrize(
    ("context_kwargs", "reason"),
    [
        ({"risk_reject_reason": "cooldown"}, "cooldown"),
        ({"risk_reject_reason": "daily_loss_halt"}, "daily_loss_halt"),
        ({"risk_reject_reason": "consecutive_loss_halt"}, "consecutive_loss_halt"),
        ({"spread": 0.6}, "spread_exceeded"),
        ({"spread": 0.2, "spread_mean": 0.1}, "spread_to_mean_exceeded"),
        ({"volatility": 0.0}, "volatility_too_low"),
    ],
)
def test_decide_entry_covers_shared_reject_reasons(
    context_kwargs: dict[str, object],
    reason: str,
) -> None:
    policy = _policy()
    timestamp = pd.Timestamp("2026-02-02T09:30:00+09:00")
    base = {
        "timestamp": timestamp,
        "tick_index": pd.DatetimeIndex([timestamp], tz=ASIA_TOKYO),
        "spread": 0.1,
        "spread_mean": 0.1,
        "spread_z": 0.0,
        "volatility": 1.0,
        "probability": 0.9,
        "threshold": 0.5,
        "chosen_side": "long",
    }
    base.update(context_kwargs)

    assert policy.decide_entry(ScalpingSignalContext(**base)).reject_reason == reason


def test_backtest_and_realtime_engine_use_same_decide_entry_policy() -> None:
    training_config = ScalpingTrainingConfig(max_spread_pips=0.5, min_volatility_pips=0.1)
    execution_config = ScalpingExecutionConfig(max_spread_z=2.0)
    backtest_policy = ScalpingSignalPolicy(training_config, execution_config)
    realtime_engine = ScalpingRealtimePaperEngine(
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(),
        training_config=training_config,
        execution_config=execution_config,
    )
    timestamp = pd.Timestamp("2026-02-02T09:30:00+09:00")
    context = ScalpingSignalContext(
        timestamp=timestamp,
        tick_index=pd.DatetimeIndex([timestamp], tz=ASIA_TOKYO),
        spread=0.1,
        spread_mean=0.1,
        spread_z=3.0,
        volatility=1.0,
        probability=0.9,
        threshold=0.5,
        chosen_side="long",
    )

    assert backtest_policy.decide_entry(context) == realtime_engine.signal_policy.decide_entry(
        context
    )


def test_backtest_signal_rows_keep_risk_state_before_and_after() -> None:
    index = pd.DatetimeIndex(
        ["2026-02-02T09:00:00+09:00", "2026-02-02T09:00:03+09:00"],
        tz=ASIA_TOKYO,
    )
    result = run_scalping_tick_backtest(
        simple_loss_ticks(index),
        neutral_features(index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(probability=0.9, threshold=0.5),
        training_config=ScalpingTrainingConfig(
            take_profit_pips=0.05,
            stop_loss_pips=0.05,
            max_hold_seconds=2,
            max_spread_pips=1.0,
            min_volatility_pips=0.0,
            round_trip_slippage_pips=0.0,
        ),
        execution_config=ScalpingExecutionConfig(
            starting_cash=100000.0,
            fixed_order_amount=150000.0,
            minimum_order_quantity=1,
            quantity_step=1,
            cooldown_seconds=0,
        ),
    )

    accepted = result.signals.loc[result.signals["accepted"].astype(bool)].iloc[0]

    assert accepted["trades_today_before"] == 0
    assert accepted["trades_today_after"] == 1
    assert accepted["trades_today"] == accepted["trades_today_before"]


def test_rejected_signal_rows_keep_same_before_and_after_state() -> None:
    index = pd.DatetimeIndex(["2026-02-02T09:00:00+09:00"], tz=ASIA_TOKYO)
    result = run_scalping_tick_backtest(
        simple_loss_ticks(index),
        neutral_features(index),
        symbol="USD_JPY",
        pip_size=0.01,
        model_bundle=constant_bundle(probability=0.4, threshold=0.5),
        training_config=ScalpingTrainingConfig(max_spread_pips=1.0, min_volatility_pips=0.0),
        execution_config=ScalpingExecutionConfig(minimum_order_quantity=1, quantity_step=1),
    )

    rejected = result.signals.iloc[0]

    assert bool(rejected["accepted"]) is False
    assert rejected["trades_today_before"] == rejected["trades_today_after"]
    assert rejected["daily_pnl_before"] == rejected["daily_pnl_after"]
    assert rejected["consecutive_losses_before"] == rejected["consecutive_losses_after"]
    assert rejected["trades_today"] == rejected["trades_today_before"]
