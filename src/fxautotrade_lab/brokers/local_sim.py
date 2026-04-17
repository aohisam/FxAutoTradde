"""Local simulation broker."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.brokers.base import BaseBroker
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.core.time import ensure_eastern


def _format_decimal(value: float) -> str:
    return f"{value:.4f}"


@dataclass(slots=True)
class LocalSimBroker(BaseBroker):
    mode: BrokerMode = BrokerMode.LOCAL_SIM
    starting_equity: float = 5000000.0
    submitted_orders: list[dict[str, object]] = field(default_factory=list)
    open_positions: dict[str, dict[str, object]] = field(default_factory=dict)
    latest_prices: dict[str, float] = field(default_factory=dict)
    latest_bid_prices: dict[str, float] = field(default_factory=dict)
    latest_ask_prices: dict[str, float] = field(default_factory=dict)
    cash_balance: float = field(init=False)
    realized_pl: float = field(default=0.0, init=False)
    day_start_equity: float = field(init=False)
    day_marker: str = field(default="", init=False)
    latest_market_timestamp: pd.Timestamp | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.cash_balance = float(self.starting_equity)
        self.day_start_equity = float(self.starting_equity)

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict[str, object]:
        upper = symbol.upper()
        if side == OrderSide.BUY:
            fill_price = self.latest_ask_prices.get(upper, self.latest_prices.get(upper, 0.0))
        else:
            fill_price = self.latest_bid_prices.get(upper, self.latest_prices.get(upper, 0.0))
        timestamp = pd.Timestamp.now(tz=ASIA_TOKYO)
        payload = {
            "order_id": str(uuid4()),
            "symbol": upper,
            "qty": str(qty),
            "filled_qty": str(qty),
            "side": side.value,
            "reason": reason,
            "status": "filled_local_sim",
            "submitted_at": timestamp.isoformat(),
            "filled_at": timestamp.isoformat(),
            "filled_avg_price": _format_decimal(fill_price),
        }
        self.submitted_orders.append(payload)
        if side == OrderSide.BUY:
            self._fill_buy_order(upper, qty, fill_price)
        else:
            self._fill_sell_order(upper, qty, fill_price)
        return payload

    def get_account_summary(self) -> dict[str, object]:
        equity = self._current_equity()
        daily_pl = equity - self.day_start_equity
        daily_return = daily_pl / self.day_start_equity if self.day_start_equity else 0.0
        return {
            "mode": self.mode.value,
            "status": "local_sim_ready",
            "equity": _format_decimal(equity),
            "last_equity": _format_decimal(self.day_start_equity),
            "portfolio_value": _format_decimal(equity),
            "cash": _format_decimal(self.cash_balance),
            "buying_power": _format_decimal(self.cash_balance),
            "daily_pl": _format_decimal(daily_pl),
            "daily_return": _format_decimal(daily_return),
            "realized_pl": _format_decimal(self.realized_pl),
            "latest_market_timestamp": (
                self.latest_market_timestamp.isoformat() if self.latest_market_timestamp is not None else ""
            ),
            "message": "GMO/履歴データを使ったローカル約定です。実売買は行いません。",
        }

    def list_open_positions(self) -> list[dict[str, object]]:
        positions: list[dict[str, object]] = []
        for symbol in sorted(self.open_positions):
            positions.append(self._serialize_position(symbol, self.open_positions[symbol]))
        return positions

    def list_recent_orders(self, limit: int = 50) -> list[dict[str, object]]:
        return list(self.submitted_orders[-limit:])

    def list_recent_fills(self, limit: int = 50) -> list[dict[str, object]]:
        fills = []
        for order in self.submitted_orders[-limit:]:
            fills.append(
                {
                    "fill_id": order["order_id"],
                    "order_id": order["order_id"],
                    "symbol": order["symbol"],
                    "qty": order["filled_qty"],
                    "side": order["side"],
                    "price": order["filled_avg_price"],
                    "filled_at": order["filled_at"],
                }
            )
        return fills

    def cancel_all_orders(self) -> dict[str, object]:
        return {"cancelled_orders": 0, "message_ja": "ローカルシミュレーションでは未約定注文はありません"}

    def close_all_positions(self) -> dict[str, object]:
        count = len(self.open_positions)
        for symbol, position in list(self.open_positions.items()):
            qty = int(position.get("qty", 0))
            side = str(position.get("side", "long"))
            if side == "short":
                latest_price = self.latest_ask_prices.get(symbol, self.latest_prices.get(symbol, float(position.get("avg_entry_price", 0.0))))
                self._fill_buy_order(symbol, qty, latest_price)
            else:
                latest_price = self.latest_bid_prices.get(symbol, self.latest_prices.get(symbol, float(position.get("avg_entry_price", 0.0))))
                self._fill_sell_order(symbol, qty, latest_price)
        return {"closed_positions": count, "message_ja": "ローカルポジションを全てクローズしました"}

    def update_market_data(self, prices: dict[str, float], timestamp: pd.Timestamp | None = None) -> None:
        if not prices:
            return
        for symbol, payload in prices.items():
            upper = symbol.upper()
            if isinstance(payload, dict):
                bid = float(payload.get("bid", 0.0) or 0.0)
                ask = float(payload.get("ask", 0.0) or 0.0)
                mid = float(payload.get("mid", 0.0) or ((bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0))
                if bid > 0:
                    self.latest_bid_prices[upper] = bid
                if ask > 0:
                    self.latest_ask_prices[upper] = ask
                if mid > 0:
                    self.latest_prices[upper] = mid
                continue
            price = float(payload)
            if price > 0:
                self.latest_prices[upper] = price
        if timestamp is not None:
            eastern = ensure_eastern(timestamp)
            marker = eastern.strftime("%Y-%m-%d")
            if not self.day_marker:
                self.day_marker = marker
            elif marker != self.day_marker:
                self.day_marker = marker
                self.day_start_equity = self._current_equity()
            self.latest_market_timestamp = eastern

    def _fill_buy_order(self, symbol: str, qty: int, fill_price: float) -> None:
        cost = fill_price * qty
        self.cash_balance -= cost
        existing = self.open_positions.get(symbol)
        if existing is None:
            self.open_positions[symbol] = {
                "symbol": symbol,
                "qty": int(qty),
                "avg_entry_price": float(fill_price),
                "side": "long",
            }
            return
        if str(existing.get("side", "long")) == "short":
            current_qty = int(existing.get("qty", 0))
            avg_entry = float(existing.get("avg_entry_price", 0.0))
            cover_qty = min(qty, current_qty)
            if cover_qty > 0:
                self.realized_pl += (avg_entry - fill_price) * cover_qty
            remaining_short = current_qty - cover_qty
            remaining_buy = qty - cover_qty
            if remaining_short > 0:
                existing["qty"] = remaining_short
                return
            self.open_positions.pop(symbol, None)
            if remaining_buy > 0:
                self.open_positions[symbol] = {
                    "symbol": symbol,
                    "qty": int(remaining_buy),
                    "avg_entry_price": float(fill_price),
                    "side": "long",
                }
            return
        current_qty = int(existing.get("qty", 0))
        current_avg = float(existing.get("avg_entry_price", 0.0))
        new_qty = current_qty + qty
        weighted = ((current_avg * current_qty) + cost) / new_qty if new_qty else 0.0
        existing["qty"] = new_qty
        existing["avg_entry_price"] = weighted

    def _fill_sell_order(self, symbol: str, qty: int, fill_price: float) -> None:
        existing = self.open_positions.get(symbol)
        if existing is None:
            self.cash_balance += fill_price * qty
            self.open_positions[symbol] = {
                "symbol": symbol,
                "qty": int(qty),
                "avg_entry_price": float(fill_price),
                "side": "short",
            }
            return
        if str(existing.get("side", "long")) == "short":
            proceeds = fill_price * qty
            self.cash_balance += proceeds
            current_qty = int(existing.get("qty", 0))
            current_avg = float(existing.get("avg_entry_price", 0.0))
            new_qty = current_qty + qty
            weighted = ((current_avg * current_qty) + proceeds) / new_qty if new_qty else 0.0
            existing["qty"] = new_qty
            existing["avg_entry_price"] = weighted
            return
        current_qty = int(existing.get("qty", 0))
        avg_entry = float(existing.get("avg_entry_price", 0.0))
        sell_qty = min(qty, current_qty)
        if sell_qty <= 0:
            return
        self.cash_balance += fill_price * sell_qty
        self.realized_pl += (fill_price - avg_entry) * sell_qty
        remaining_qty = current_qty - sell_qty
        remaining_sell = qty - sell_qty
        if remaining_qty <= 0:
            self.open_positions.pop(symbol, None)
            if remaining_sell > 0:
                self.cash_balance += fill_price * remaining_sell
                self.open_positions[symbol] = {
                    "symbol": symbol,
                    "qty": int(remaining_sell),
                    "avg_entry_price": float(fill_price),
                    "side": "short",
                }
            return
        existing["qty"] = remaining_qty

    def _serialize_position(self, symbol: str, position: dict[str, object]) -> dict[str, object]:
        qty = int(position.get("qty", 0))
        avg_entry = float(position.get("avg_entry_price", 0.0))
        side = str(position.get("side", "long"))
        if side == "short":
            current_price = float(self.latest_ask_prices.get(symbol, self.latest_prices.get(symbol, avg_entry)))
            market_value = -current_price * qty
            unrealized = (avg_entry - current_price) * qty
        else:
            current_price = float(self.latest_bid_prices.get(symbol, self.latest_prices.get(symbol, avg_entry)))
            market_value = current_price * qty
            unrealized = (current_price - avg_entry) * qty
        return {
            "symbol": symbol,
            "qty": str(qty),
            "avg_entry_price": _format_decimal(avg_entry),
            "market_value": _format_decimal(market_value),
            "unrealized_pl": _format_decimal(unrealized),
            "side": side,
            "current_price": _format_decimal(current_price),
        }

    def _current_equity(self) -> float:
        market_value = 0.0
        for symbol, position in self.open_positions.items():
            qty = int(position.get("qty", 0))
            avg_entry = float(position.get("avg_entry_price", 0.0))
            side = str(position.get("side", "long"))
            if side == "short":
                current_price = float(self.latest_ask_prices.get(symbol, self.latest_prices.get(symbol, avg_entry)))
                market_value -= current_price * qty
            else:
                current_price = float(self.latest_bid_prices.get(symbol, self.latest_prices.get(symbol, avg_entry)))
                market_value += current_price * qty
        return self.cash_balance + market_value
