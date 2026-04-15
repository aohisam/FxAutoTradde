"""Alpaca paper trading broker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fxautotrade_lab.brokers.base import BaseBroker
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.data.alpaca import AlpacaTradingGateway


@dataclass(slots=True)
class AlpacaPaperBroker(BaseBroker):
    env: EnvironmentConfig
    mode: BrokerMode = BrokerMode.ALPACA_PAPER

    def _gateway(self) -> AlpacaTradingGateway:
        return AlpacaTradingGateway(self.env, self.env.alpaca_paper_base_url)

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict[str, Any]:
        result = self._gateway().submit_market_order(symbol, qty, side)
        result["reason"] = reason
        return result

    def get_account_summary(self) -> dict[str, Any]:
        summary = self._gateway().get_account_summary()
        summary["paper_notice_ja"] = "実市場データ連動 / 実売買は行いません / 約定はシミュレーションです"
        return summary

    def list_open_positions(self) -> list[dict[str, Any]]:
        return self._gateway().list_open_positions()

    def list_recent_orders(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._gateway().list_recent_orders(limit=limit)

    def list_recent_fills(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._gateway().list_recent_fills(limit=limit)

    def cancel_all_orders(self) -> dict[str, Any]:
        return self._gateway().cancel_all_orders()

    def close_all_positions(self) -> dict[str, Any]:
        return self._gateway().close_all_positions()
