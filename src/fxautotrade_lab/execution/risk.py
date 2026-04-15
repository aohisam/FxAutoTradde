"""Risk and sizing rules."""

from __future__ import annotations

from dataclasses import dataclass

from fxautotrade_lab.config.models import RiskConfig
from fxautotrade_lab.core.enums import OrderSizingMode


@dataclass(slots=True)
class SizingResult:
    quantity: int
    notional: float
    risk_amount: float


class RiskManager:
    """Fixed-fraction and exposure controls."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def _stop_distance(self, price: float, atr_value: float) -> float:
        return max(atr_value * self.config.atr_stop_multiple, price * 0.01, 0.01)

    def _max_notional(self, cash: float, equity: float) -> float:
        return max(
            0.0,
            min(
                cash * (1 - self.config.min_cash_buffer),
                equity * self.config.max_symbol_exposure,
            ),
        )

    def size_position(self, cash: float, equity: float, price: float, atr_value: float) -> SizingResult:
        risk_budget = equity * self.config.risk_per_trade
        stop_distance = self._stop_distance(price, atr_value)
        qty_by_risk = int(risk_budget / stop_distance)
        max_notional = self._max_notional(cash, equity)
        qty_by_cap = int(max_notional / price)
        quantity = max(0, min(qty_by_risk, qty_by_cap))
        return SizingResult(
            quantity=quantity,
            notional=quantity * price,
            risk_amount=quantity * stop_distance,
        )

    def size_automation_position(self, cash: float, equity: float, price: float, atr_value: float) -> SizingResult:
        if price <= 0:
            return SizingResult(quantity=0, notional=0.0, risk_amount=0.0)
        max_notional = self._max_notional(cash, equity)
        stop_distance = self._stop_distance(price, atr_value)
        mode = self.config.order_size_mode
        if mode == OrderSizingMode.RISK_BASED:
            return self.size_position(cash=cash, equity=equity, price=price, atr_value=atr_value)
        if mode == OrderSizingMode.EQUITY_FRACTION:
            target_notional = min(equity * self.config.equity_fraction_per_trade, max_notional)
        else:
            target_notional = min(self.config.fixed_order_amount, max_notional)
        quantity = int(target_notional / price)
        step = max(1, int(self.config.quantity_step))
        quantity = (quantity // step) * step
        if quantity < int(self.config.minimum_order_quantity):
            quantity = 0
        notional = quantity * price
        return SizingResult(
            quantity=max(0, quantity),
            notional=notional,
            risk_amount=max(0, quantity) * stop_distance,
        )

    def can_open_position(
        self,
        open_positions: int,
        current_exposure_ratio: float,
        quantity: int,
    ) -> bool:
        if quantity <= 0:
            return False
        if open_positions >= self.config.max_positions:
            return False
        if current_exposure_ratio >= self.config.max_portfolio_exposure:
            return False
        return True
