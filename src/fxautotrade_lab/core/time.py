"""Timezone helpers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO, UTC


def ensure_eastern(ts: datetime | pd.Timestamp) -> pd.Timestamp:
    """Return a timezone-aware market timestamp in Asia/Tokyo."""
    value = pd.Timestamp(ts)
    if value.tzinfo is None:
        return value.tz_localize(ASIA_TOKYO)
    return value.tz_convert(ASIA_TOKYO)


def to_tokyo(ts: datetime | pd.Timestamp) -> pd.Timestamp:
    """Convert timestamp to Asia/Tokyo."""
    return ensure_eastern(ts).tz_convert(ASIA_TOKYO)


def utc_now_eastern() -> pd.Timestamp:
    """Current timestamp in Japan time."""
    return pd.Timestamp.utcnow().tz_convert(UTC).tz_convert(ASIA_TOKYO)


def format_dual_time(ts: datetime | pd.Timestamp) -> str:
    tokyo = ensure_eastern(ts)
    utc = tokyo.tz_convert(UTC)
    return (
        f"UTC {utc.strftime('%Y-%m-%d %H:%M:%S %Z')} / "
        f"日本時間 {tokyo.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
