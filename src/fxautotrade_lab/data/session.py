"""FX market session helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.core.constants import (
    ASIA_TOKYO,
    FX_WEEKDAY_OPEN_LABEL_JA,
    FX_WEEKEND_CLOSED_LABEL_JA,
)


@dataclass(slots=True)
class SessionState:
    is_weekday: bool
    is_regular_session: bool
    is_pre_market: bool
    is_after_hours: bool
    label_ja: str


def get_session_state(timestamp: pd.Timestamp) -> SessionState:
    tokyo = pd.Timestamp(timestamp)
    tokyo = tokyo.tz_localize(ASIA_TOKYO) if tokyo.tzinfo is None else tokyo.tz_convert(ASIA_TOKYO)
    is_weekday = tokyo.weekday() < 5
    is_regular = is_weekday
    label = FX_WEEKDAY_OPEN_LABEL_JA if is_regular else FX_WEEKEND_CLOSED_LABEL_JA
    return SessionState(
        is_weekday=is_weekday,
        is_regular_session=is_regular,
        is_pre_market=False,
        is_after_hours=not is_regular,
        label_ja=label,
    )


def trading_days(start: str, end: str) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="B", tz=ASIA_TOKYO)
