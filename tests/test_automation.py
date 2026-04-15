from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from fxautotrade_lab.automation.controller import AutomationController
from fxautotrade_lab.config.loader import load_app_config, load_environment
from fxautotrade_lab.core.enums import OrderSide

from tests.conftest import write_config


@dataclass
class FakeBroker:
    account_summary: dict = field(
        default_factory=lambda: {
            "status": "ready",
            "equity": "5000000",
            "last_equity": "5000000",
            "daily_pl": "0",
            "message": "local runtime",
        }
    )
    positions: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    fills: list[dict] = field(default_factory=list)
    cancelled: int = 0
    closed: int = 0

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict:
        payload = {
            "order_id": f"{symbol}-{side.value}-{len(self.orders) + 1}",
            "symbol": symbol,
            "qty": str(qty),
            "filled_qty": str(qty),
            "side": side.value,
            "status": "filled_local_sim",
            "reason": reason,
            "filled_avg_price": "150.00",
        }
        self.orders.append(payload)
        self.fills.append(
            {
                "fill_id": payload["order_id"],
                "order_id": payload["order_id"],
                "symbol": symbol,
                "qty": str(qty),
                "side": side.value,
                "price": "150.00",
            }
        )
        if side == OrderSide.BUY:
            self.positions = [{"symbol": symbol, "qty": str(qty), "side": "long", "avg_entry_price": "150.00"}]
        else:
            self.positions = []
        return payload

    def get_account_summary(self) -> dict:
        return dict(self.account_summary)

    def list_open_positions(self) -> list[dict]:
        return list(self.positions)

    def list_recent_orders(self, limit: int = 50) -> list[dict]:
        return list(self.orders[-limit:])

    def list_recent_fills(self, limit: int = 50) -> list[dict]:
        return list(self.fills[-limit:])

    def cancel_all_orders(self) -> dict:
        self.cancelled += 1
        return {"cancelled_orders": len(self.orders)}

    def close_all_positions(self) -> dict:
        self.closed += 1
        count = len(self.positions)
        self.positions = []
        return {"closed_positions": count}

    def sync_runtime_state(self, order_limit: int = 50) -> dict:
        return {
            "account_summary": self.get_account_summary(),
            "positions": self.list_open_positions(),
            "orders": self.list_recent_orders(order_limit),
            "fills": self.list_recent_fills(order_limit),
        }

    def update_market_data(self, prices: dict[str, float], timestamp=None) -> None:  # noqa: ANN001
        _ = prices, timestamp

    def shutdown(self) -> None:
        return None


def test_automation_controller_local_sim(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    controller = AutomationController(config, env)
    logs = controller.run(max_cycles=1)
    assert logs
    snapshot = controller.snapshot()
    assert snapshot["run_id"]
    assert snapshot["status"] == "stopped"
    assert "account_summary" in snapshot


def test_automation_syncs_broker_state(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(
        positions=[{"symbol": "USD_JPY", "qty": "10000", "side": "long"}],
        orders=[{"symbol": "USD_JPY", "status": "filled_local_sim", "order_id": "1"}],
        fills=[{"symbol": "USD_JPY", "order_id": "1", "fill_id": "1"}],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    assert controller._sync_broker_state() is True
    snapshot = controller.snapshot()
    assert snapshot["positions"]
    assert snapshot["recent_orders"]
    assert snapshot["recent_fills"]


def test_automation_daily_loss_triggers_kill_switch(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    config.risk.max_daily_loss_amount = 100.0
    env = load_environment()
    fake = FakeBroker(
        account_summary={
            "status": "ready",
            "equity": "4999800",
            "last_equity": "5000000",
            "daily_pl": "-200",
            "message": "loss",
        },
        positions=[{"symbol": "USD_JPY", "qty": "10000", "side": "long"}],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller.run(max_cycles=1)
    snapshot = controller.snapshot()
    assert snapshot["kill_switch_reason"]
    assert fake.cancelled == 1
    assert fake.closed == 1


def test_automation_manual_close_position(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(
        positions=[{"symbol": "USD_JPY", "qty": "10000", "side": "long", "avg_entry_price": "150.00"}]
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)

    order = controller.manual_close_position("USD_JPY")

    assert order["side"] == "sell"
    assert controller.snapshot()["positions"] == []


def test_automation_manual_close_position_requires_quantity(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(positions=[])
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)

    with pytest.raises(RuntimeError):
        controller.manual_close_position("USD_JPY")
