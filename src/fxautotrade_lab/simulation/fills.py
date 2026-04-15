"""Fill and cost models."""

from __future__ import annotations

from dataclasses import dataclass

from fxautotrade_lab.config.models import RiskConfig
from fxautotrade_lab.core.enums import OrderSide


@dataclass(slots=True)
class FillPriceResult:
    price: float
    slippage: float
    fee: float


def apply_fill_model(price: float, side: OrderSide, quantity: int, config: RiskConfig) -> FillPriceResult:
    slip = price * (config.slippage_bps / 10_000)
    adjusted = price + slip if side == OrderSide.BUY else price - slip
    fee = config.fee_per_order
    return FillPriceResult(price=adjusted, slippage=slip, fee=fee)
