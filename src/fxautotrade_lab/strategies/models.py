"""Strategy dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from fxautotrade_lab.core.enums import SignalAction


@dataclass(slots=True)
class StrategyDecision:
    timestamp: pd.Timestamp
    symbol: str
    action: SignalAction
    score: float
    accepted: bool
    reasons: list[str]
    explanation_ja: str
    sub_scores: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
