"""Economic event provider abstraction for FX entry blackouts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.config.models import FxEventFilterConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO


@dataclass(slots=True)
class EconomicEvent:
    timestamp: pd.Timestamp
    currency: str
    importance: str
    title: str


class BaseEconomicEventProvider(ABC):
    @abstractmethod
    def list_events(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        currencies: set[str],
    ) -> list[EconomicEvent]:
        """Return events that overlap the requested range."""


class NullEconomicEventProvider(BaseEconomicEventProvider):
    def list_events(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        currencies: set[str],
    ) -> list[EconomicEvent]:
        _ = start, end, currencies
        return []


class StaticCsvEconomicEventProvider(BaseEconomicEventProvider):
    def __init__(self, calendar_path: str) -> None:
        self.calendar_path = calendar_path

    def list_events(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        currencies: set[str],
    ) -> list[EconomicEvent]:
        frame = pd.read_csv(self.calendar_path)
        if frame.empty:
            return []
        frame.columns = [str(column).strip().lower() for column in frame.columns]
        timestamps = pd.to_datetime(frame["timestamp"], errors="coerce")
        if timestamps.dt.tz is None:
            timestamps = timestamps.dt.tz_localize(ASIA_TOKYO)
        else:
            timestamps = timestamps.dt.tz_convert(ASIA_TOKYO)
        selected = frame.assign(timestamp=timestamps).dropna(subset=["timestamp"])
        selected["currency"] = selected["currency"].astype(str).str.upper()
        importance = selected["importance"] if "importance" in selected.columns else pd.Series("high", index=selected.index)
        selected["importance"] = importance.astype(str).str.lower()
        mask = (
            selected["timestamp"].between(start, end, inclusive="both")
            & selected["currency"].isin({currency.upper() for currency in currencies})
            & selected["importance"].isin({"high", "critical"})
        )
        return [
            EconomicEvent(
                timestamp=pd.Timestamp(row["timestamp"]),
                currency=str(row["currency"]).upper(),
                importance=str(row["importance"]).lower(),
                title=str(row.get("title", row.get("event", ""))),
            )
            for _, row in selected.loc[mask].iterrows()
        ]


def build_event_provider(config: FxEventFilterConfig) -> BaseEconomicEventProvider:
    if not config.enabled or config.provider == "disabled":
        return NullEconomicEventProvider()
    if config.provider == "static_csv":
        if config.calendar_path is None:
            raise ValueError("経済イベント CSV のパスが未設定です。")
        return StaticCsvEconomicEventProvider(str(config.calendar_path))
    raise ValueError(f"Unsupported economic event provider: {config.provider}")
