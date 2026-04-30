from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_policy import (
    BlackoutWindow,
    ScalpingExecutionConfig,
    ScalpingSignalContext,
    ScalpingSignalPolicy,
)


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
