"""Future live broker with hard safety gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fxautotrade_lab.brokers.base import BaseBroker
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.data.alpaca import AlpacaTradingGateway


@dataclass(slots=True)
class AlpacaLiveBroker(BaseBroker):
    env: EnvironmentConfig
    mode: BrokerMode = BrokerMode.ALPACA_LIVE

    def _assert_safety_gates(self) -> None:
        missing: list[str] = []
        if not self.env.live_trading_enabled:
            missing.append("LIVE_TRADING_ENABLED=true")
        if not self.env.i_understand_real_money_risk:
            missing.append("I_UNDERSTAND_REAL_MONEY_RISK=true")
        if self.env.confirm_broker_mode != "alpaca_live":
            missing.append("CONFIRM_BROKER_MODE=alpaca_live")
        if self.env.confirm_live_broker_class != "AlpacaLiveBroker":
            missing.append("CONFIRM_LIVE_BROKER_CLASS=AlpacaLiveBroker")
        if missing:
            required = " / ".join(missing)
            raise RuntimeError(
                "ライブ取引は無効です。安全フラグが不足しています。必要設定: " + required
            )

    def _gateway(self) -> AlpacaTradingGateway:
        self._assert_safety_gates()
        return AlpacaTradingGateway(self.env, self.env.alpaca_live_base_url)

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict[str, Any]:
        result = self._gateway().submit_market_order(symbol, qty, side)
        result["reason"] = reason
        return result

    def get_account_summary(self) -> dict[str, Any]:
        summary = self._gateway().get_account_summary()
        summary["warning_ja"] = "ライブ口座の設定が必要です"
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

    def sync_runtime_state(self, order_limit: int = 50) -> dict[str, Any]:
        self._assert_safety_gates()
        return super().sync_runtime_state(order_limit=order_limit)
