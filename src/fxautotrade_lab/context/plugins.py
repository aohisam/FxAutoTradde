"""Extensible context plugin interfaces for future news/macro sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class ContextPlugin(Protocol):
    name: str

    def enrich(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return the frame with additional context columns."""


@dataclass(slots=True)
class NewsContextPlugin:
    name: str = "news"

    def enrich(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        frame["news_context_available"] = False
        return frame


@dataclass(slots=True)
class MacroContextPlugin:
    name: str = "macro"

    def enrich(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        frame["macro_context_available"] = False
        return frame


@dataclass(slots=True)
class EarningsContextPlugin:
    name: str = "earnings"

    def enrich(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        frame["earnings_context_available"] = False
        return frame
