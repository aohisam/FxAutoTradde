"""Strategy factory."""

from __future__ import annotations

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.strategies.base import BaseStrategy
from fxautotrade_lab.strategies.baseline import BaselineTrendPullbackStrategy
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from fxautotrade_lab.strategies.scoring import MultiTimeframePatternScoringStrategy


def create_strategy(config: AppConfig) -> BaseStrategy:
    if config.strategy.name == BaselineTrendPullbackStrategy.name:
        return BaselineTrendPullbackStrategy(config)
    if config.strategy.name == FxBreakoutPullbackStrategy.name:
        return FxBreakoutPullbackStrategy(config)
    return MultiTimeframePatternScoringStrategy(config)
