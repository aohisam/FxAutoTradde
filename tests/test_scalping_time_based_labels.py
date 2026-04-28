from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.ml.scalping import ScalpingTrainingConfig, build_triple_barrier_labels


def _quote_bars(index: pd.DatetimeIndex) -> pd.DataFrame:
    bid = pd.Series(150.000, index=index)
    ask = bid + 0.002
    return pd.DataFrame(
        {
            "bid_open": bid,
            "bid_high": bid + 0.0002,
            "bid_low": bid - 0.0002,
            "bid_close": bid,
            "bid_volume": 1.0,
            "ask_open": ask,
            "ask_high": ask + 0.0002,
            "ask_low": ask - 0.0002,
            "ask_close": ask,
            "ask_volume": 1.0,
        },
        index=index,
    )


def test_triple_barrier_uses_elapsed_time_for_irregular_bars() -> None:
    index = pd.DatetimeIndex(
        [
            "2026-02-02 09:00:00",
            "2026-02-02 09:00:10",
            "2026-02-02 09:00:40",
            "2026-02-02 09:01:20",
        ],
        tz=ASIA_TOKYO,
    )
    labels = build_triple_barrier_labels(
        _quote_bars(index),
        pip_size=0.01,
        config=ScalpingTrainingConfig(max_hold_seconds=30, take_profit_pips=10, stop_loss_pips=10),
    )

    assert labels.loc[index[0], "long_hold_seconds"] == 10.0
    assert labels.loc[index[1], "long_hold_seconds"] == 30.0
    assert labels.loc[index[0], "long_hold_bars"] == 1
    assert labels.loc[index[1], "long_hold_bars"] == 1


def test_missing_bars_do_not_turn_seconds_into_row_counts() -> None:
    index = pd.DatetimeIndex(
        ["2026-02-02 09:00:00", "2026-02-02 09:00:05", "2026-02-02 09:02:00"],
        tz=ASIA_TOKYO,
    )
    labels = build_triple_barrier_labels(
        _quote_bars(index),
        pip_size=0.01,
        config=ScalpingTrainingConfig(max_hold_seconds=30, take_profit_pips=10, stop_loss_pips=10),
    )

    assert labels.loc[index[0], "long_hold_seconds"] == 5.0
    assert pd.isna(labels.loc[index[1], "long_net_pips"])


def test_same_bar_take_profit_and_stop_loss_prefers_stop_loss() -> None:
    index = pd.DatetimeIndex(
        ["2026-02-02 09:00:00", "2026-02-02 09:00:01", "2026-02-02 09:00:02"],
        tz=ASIA_TOKYO,
    )
    bars = _quote_bars(index)
    bars.loc[index[1], "bid_high"] = 150.020
    bars.loc[index[1], "bid_low"] = 149.980
    bars.loc[index[1], "ask_high"] = 150.022
    bars.loc[index[1], "ask_low"] = 149.982

    labels = build_triple_barrier_labels(
        bars,
        pip_size=0.01,
        config=ScalpingTrainingConfig(max_hold_seconds=10, take_profit_pips=1, stop_loss_pips=1),
    )

    assert labels.loc[index[0], "long_exit_reason"] == "stop_loss"
    assert labels.loc[index[0], "long_hold_seconds"] == 1.0
    assert labels.loc[index[0], "long_net_pips"] < 0
