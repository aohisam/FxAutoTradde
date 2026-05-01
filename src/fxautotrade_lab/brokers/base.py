"""Base broker interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from fxautotrade_lab.core.enums import BrokerMode, OrderSide

if TYPE_CHECKING:
    import pandas as pd


class BaseBroker(ABC):
    mode: BrokerMode

    @abstractmethod
    def submit_market_order(
        self, symbol: str, qty: int, side: OrderSide, reason: str
    ) -> dict[str, Any]:
        """Submit market order."""

    @abstractmethod
    def get_account_summary(self) -> dict[str, Any]:
        """Return account summary."""

    @abstractmethod
    def list_open_positions(self) -> list[dict[str, Any]]:
        """Return synchronized positions."""

    @abstractmethod
    def list_recent_orders(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent orders."""

    def list_recent_fills(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent fills."""
        _ = limit
        return []

    @abstractmethod
    def cancel_all_orders(self) -> dict[str, Any]:
        """Cancel all open orders."""

    @abstractmethod
    def close_all_positions(self) -> dict[str, Any]:
        """Close all positions."""

    def sync_runtime_state(self, order_limit: int = 50) -> dict[str, Any]:
        return {
            "account_summary": self.get_account_summary(),
            "positions": self.list_open_positions(),
            "orders": self.list_recent_orders(limit=order_limit),
            "fills": self.list_recent_fills(limit=order_limit),
        }

    def update_market_data(
        self,
        prices: dict[str, float],
        timestamp: pd.Timestamp | None = None,
    ) -> None:
        """Apply the latest observable market prices to the broker state."""
        _ = prices, timestamp

    def shutdown(self) -> None:
        """Close broker resources."""
        return None
