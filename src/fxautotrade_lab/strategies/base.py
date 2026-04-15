"""Base strategy interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from fxautotrade_lab.config.models import AppConfig


class BaseStrategy(ABC):
    """Abstract strategy."""

    name: str

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    @abstractmethod
    def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return a frame with signal columns."""
