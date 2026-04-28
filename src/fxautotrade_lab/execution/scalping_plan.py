"""Safe order-plan helpers for future scalping broker integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from fxautotrade_lab.brokers.base import BaseBroker
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.core.symbols import normalize_fx_symbol


@dataclass(frozen=True, slots=True)
class ScalpingOrderPlan:
    """Broker-neutral scalping order intent.

    This object is deliberately an intent, not a live order. It can be handed to
    LocalSimBroker today and to a future private broker only after the broker-side
    safety gates explicitly opt in.
    """

    symbol: str
    side: str
    quantity: int
    entry_order_side: OrderSide
    exit_order_side: OrderSide
    entry_price: float
    take_profit_price: float
    stop_loss_price: float
    reason: str
    created_at: pd.Timestamp
    broker_mode: BrokerMode = BrokerMode.LOCAL_SIM
    live_submission_allowed: bool = False

    def to_entry_order_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "qty": self.quantity,
            "side": self.entry_order_side.value,
            "reason": self.reason,
            "broker_mode": self.broker_mode.value,
            "dry_run_only": not self.live_submission_allowed,
        }

    def to_exit_bracket_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "qty": self.quantity,
            "side": self.exit_order_side.value,
            "take_profit_price": self.take_profit_price,
            "stop_loss_price": self.stop_loss_price,
            "broker_mode": self.broker_mode.value,
            "dry_run_only": not self.live_submission_allowed,
        }


def create_scalping_order_plan(
    *,
    symbol: str,
    side: str,
    quantity: int,
    entry_price: float,
    take_profit_price: float,
    stop_loss_price: float,
    reason: str,
    broker_mode: BrokerMode = BrokerMode.LOCAL_SIM,
    created_at: pd.Timestamp | None = None,
    live_submission_allowed: bool = False,
) -> ScalpingOrderPlan:
    normalized_side = str(side).strip().lower()
    if normalized_side not in {"long", "short"}:
        raise ValueError("スキャルピング注文planの side は long または short を指定してください。")
    if int(quantity) <= 0:
        raise ValueError("スキャルピング注文planの数量は1以上である必要があります。")
    entry = float(entry_price)
    take = float(take_profit_price)
    stop = float(stop_loss_price)
    if entry <= 0 or take <= 0 or stop <= 0:
        raise ValueError("スキャルピング注文planの価格は正の値である必要があります。")
    if normalized_side == "long":
        if not (take > entry > stop):
            raise ValueError(
                "longの注文planは stop_loss < entry < take_profit である必要があります。"
            )
        entry_order_side = OrderSide.BUY
        exit_order_side = OrderSide.SELL
    else:
        if not (take < entry < stop):
            raise ValueError(
                "shortの注文planは take_profit < entry < stop_loss である必要があります。"
            )
        entry_order_side = OrderSide.SELL
        exit_order_side = OrderSide.BUY
    ts = pd.Timestamp.now(tz=ASIA_TOKYO) if created_at is None else pd.Timestamp(created_at)
    ts = ts.tz_localize(ASIA_TOKYO) if ts.tzinfo is None else ts.tz_convert(ASIA_TOKYO)
    return ScalpingOrderPlan(
        symbol=normalize_fx_symbol(symbol),
        side=normalized_side,
        quantity=int(quantity),
        entry_order_side=entry_order_side,
        exit_order_side=exit_order_side,
        entry_price=entry,
        take_profit_price=take,
        stop_loss_price=stop,
        reason=str(reason or "scalping_signal"),
        created_at=ts,
        broker_mode=broker_mode,
        live_submission_allowed=bool(live_submission_allowed),
    )


def validate_scalping_order_plan_for_submission(
    plan: ScalpingOrderPlan,
    *,
    broker_mode: BrokerMode,
    dry_run: bool = True,
    live_trading_enabled: bool = False,
) -> None:
    if broker_mode != plan.broker_mode:
        raise ValueError(
            "スキャルピング注文planのbroker_modeが現在のbrokerと一致しません。"
            f" plan={plan.broker_mode.value} / broker={broker_mode.value}"
        )
    if dry_run:
        return
    if broker_mode != BrokerMode.LOCAL_SIM:
        raise RuntimeError(
            "スキャルピングのprivate broker実注文送信は未実装です。"
            " このplanは研究用のdry-runとして扱ってください。"
        )
    if live_trading_enabled or plan.live_submission_allowed:
        raise RuntimeError(
            "スキャルピング注文planはlive tradingを有効化しません。"
            " 実売買には別途の安全ゲートとbroker実装が必要です。"
        )


def submit_scalping_entry_to_broker(
    broker: BaseBroker,
    plan: ScalpingOrderPlan,
    *,
    dry_run: bool = True,
    live_trading_enabled: bool = False,
) -> dict[str, Any]:
    """Submit or preview the entry leg through the common broker interface."""

    validate_scalping_order_plan_for_submission(
        plan,
        broker_mode=broker.mode,
        dry_run=dry_run,
        live_trading_enabled=live_trading_enabled,
    )
    payload = plan.to_entry_order_payload()
    if dry_run:
        return {
            "status": "dry_run",
            "message_ja": "dry-runのため注文は送信していません。",
            "order": payload,
        }
    return broker.submit_market_order(
        plan.symbol,
        qty=plan.quantity,
        side=plan.entry_order_side,
        reason=plan.reason,
    )
