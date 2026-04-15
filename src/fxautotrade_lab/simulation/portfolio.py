"""Portfolio state."""

from __future__ import annotations

from dataclasses import dataclass, field

from fxautotrade_lab.core.models import Position


@dataclass(slots=True)
class PortfolioState:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)

    def exposure_value(self, prices: dict[str, float]) -> float:
        value = 0.0
        for symbol, position in self.positions.items():
            value += position.quantity * prices.get(symbol, position.entry_price)
        return value

    def equity(self, prices: dict[str, float]) -> float:
        return self.cash + self.exposure_value(prices)
