from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pandas as pd

from fxautotrade_lab.automation.controller import AutomationController
from fxautotrade_lab.brokers.local_sim import LocalSimBroker
from fxautotrade_lab.config.loader import load_app_config, load_environment
from fxautotrade_lab.core.constants import US_EASTERN
from fxautotrade_lab.core.enums import OrderSide, OrderSizingMode
from fxautotrade_lab.execution.managed_exits import (
    build_managed_position,
    evaluate_managed_position,
    recent_swing_low,
)
from fxautotrade_lab.execution.risk import RiskManager
from fxautotrade_lab.data.service import MarketDataBundle

from tests.conftest import write_config


@dataclass
class FakeBroker:
    account_summary: dict = field(
        default_factory=lambda: {
            "status": "ready",
            "equity": "100000",
            "last_equity": "100000",
            "daily_pl": "0",
        }
    )
    positions: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    fills: list[dict] = field(default_factory=list)
    cancelled: int = 0
    closed: int = 0

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict:
        payload = {
            "order_id": f"{symbol}-{side.value}",
            "symbol": symbol,
            "qty": str(qty),
            "filled_qty": str(qty),
            "side": side.value,
            "status": "filled",
            "reason": reason,
        }
        self.orders.append(payload)
        self.fills.append(
            {
                "fill_id": payload["order_id"],
                "order_id": payload["order_id"],
                "symbol": symbol,
                "qty": str(qty),
                "side": side.value,
            }
        )
        if side == OrderSide.BUY:
            self.positions = [{"symbol": symbol, "qty": str(qty), "side": "long"}]
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


@dataclass
class FailingBroker(FakeBroker):
    error_message: str = "offline"

    def get_account_summary(self) -> dict:
        raise RuntimeError(self.error_message)

    def sync_runtime_state(self, order_limit: int = 50) -> dict:
        raise RuntimeError(self.error_message)


@dataclass
class PartialAwareFakeBroker(FakeBroker):
    def submit_market_order(self, symbol: str, qty: int, side: OrderSide, reason: str) -> dict:
        existing = self.positions[0] if self.positions else {}
        payload = {
            "order_id": f"{symbol}-{side.value}-{len(self.orders) + 1}",
            "symbol": symbol,
            "qty": str(qty),
            "filled_qty": str(qty),
            "side": side.value,
            "status": "filled",
            "reason": reason,
            "filled_avg_price": existing.get("avg_entry_price", "100.0"),
        }
        self.orders.append(payload)
        self.fills.append(
            {
                "fill_id": payload["order_id"],
                "order_id": payload["order_id"],
                "symbol": symbol,
                "qty": str(qty),
                "side": side.value,
                "price": payload["filled_avg_price"],
            }
        )
        if side == OrderSide.SELL:
            current = int(float(existing.get("qty", "0") or 0))
            remaining = max(0, current - qty)
            if remaining > 0:
                self.positions = [
                    {
                        "symbol": symbol,
                        "qty": str(remaining),
                        "side": "long",
                        "avg_entry_price": existing.get("avg_entry_price", "100.0"),
                        "current_price": existing.get("current_price", "108.0"),
                    }
                ]
            else:
                self.positions = []
        else:
            self.positions = [
                {
                    "symbol": symbol,
                    "qty": str(qty),
                    "side": "long",
                    "avg_entry_price": payload["filled_avg_price"],
                    "current_price": payload["filled_avg_price"],
                }
            ]
        return payload


def test_automation_controller_local_sim(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    controller = AutomationController(config, env)
    logs = controller.run(max_cycles=2)
    assert len(logs) >= 2
    assert any("自動売買" in event.message_ja for event in logs)
    snapshot = controller.snapshot()
    assert snapshot["run_id"]
    assert snapshot["status"] == "stopped"
    assert "account_summary" in snapshot


def test_automation_syncs_broker_state(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(
        positions=[{"symbol": "AAPL", "qty": "1", "side": "long"}],
        orders=[{"symbol": "AAPL", "status": "filled", "order_id": "1"}],
        fills=[{"symbol": "AAPL", "order_id": "1", "fill_id": "1"}],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller._sync_broker_state()
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
            "equity": "99800",
            "last_equity": "100000",
            "daily_pl": "-200",
        },
        positions=[{"symbol": "AAPL", "qty": "1", "side": "long"}],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller.run(max_cycles=1)
    snapshot = controller.snapshot()
    assert snapshot["kill_switch_reason"]
    assert fake.cancelled == 1
    assert fake.closed == 1


def test_automation_recovers_connection_and_resyncs(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    config.automation.reconnect_seconds = 0
    config.automation.reconnect_max_attempts = 2
    env = load_environment()
    brokers = [
        FailingBroker(),
        FakeBroker(
            positions=[{"symbol": "MSFT", "qty": "1", "side": "long"}],
            orders=[{"symbol": "MSFT", "status": "filled", "order_id": "42"}],
            fills=[{"symbol": "MSFT", "order_id": "42", "fill_id": "42"}],
        ),
    ]
    monkeypatch.setattr(
        AutomationController,
        "_build_broker",
        lambda self: brokers.pop(0) if len(brokers) > 1 else brokers[0],
    )
    controller = AutomationController(config, env)
    assert controller._ensure_runtime_connectivity() is True
    snapshot = controller.snapshot()
    assert snapshot["connection_state"] == "connected"
    assert snapshot["reconnect_attempts"] >= 1
    assert snapshot["positions"]


def test_trade_update_updates_recent_orders_and_fills(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    controller = AutomationController(config, env)
    controller._on_trade_update(
        {
            "event": "fill",
            "order_id": "abc123",
            "symbol": "AAPL",
            "side": "buy",
            "status": "filled",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "185.20",
            "filled_at": "2026-04-14T09:31:00-04:00",
        }
    )
    snapshot = controller.snapshot()
    assert snapshot["recent_orders"]
    assert snapshot["recent_fills"]
    assert "AAPL" in snapshot["open_symbols"]


def test_local_sim_broker_marks_to_market_with_latest_price() -> None:
    broker = LocalSimBroker(starting_equity=1000.0)
    broker.update_market_data({"AAPL": 100.0}, pd.Timestamp("2026-04-14 09:45:00", tz=US_EASTERN))
    order = broker.submit_market_order("AAPL", 2, OrderSide.BUY, "テスト買い")
    assert order["filled_avg_price"] == "100.0000"
    summary = broker.get_account_summary()
    assert summary["cash"] == "800.0000"
    broker.update_market_data({"AAPL": 110.0}, pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN))
    positions = broker.list_open_positions()
    assert positions[0]["market_value"] == "220.0000"
    assert positions[0]["unrealized_pl"] == "20.0000"
    summary = broker.get_account_summary()
    assert summary["equity"] == "1020.0000"


def test_runtime_local_sim_skips_duplicate_decisions_for_same_bar(monkeypatch, tmp_path):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {"source": "alpaca"},
        },
    )
    env = load_environment()
    controller = AutomationController(config, env)

    entry_index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN),
            pd.Timestamp("2026-04-14 10:15:00", tz=US_EASTERN),
        ]
    )
    entry_frame = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1200],
        },
        index=entry_index,
    )

    bundle = MarketDataBundle(
        symbols={"AAPL": {config.strategy.entry_timeframe: entry_frame}},
        benchmarks={},
        sectors={},
    )
    monkeypatch.setattr(controller.data_service, "load_runtime_bundle", lambda as_of=None: bundle)
    monkeypatch.setattr(
        "fxautotrade_lab.automation.controller.build_multi_timeframe_feature_set",
        lambda symbol, bars_by_timeframe, benchmark_bars, sector_bars, config: SimpleNamespace(
            entry_frame=bars_by_timeframe[config.strategy.entry_timeframe]
        ),
    )

    class DummyStrategy:
        def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "signal_action": ["buy"] * len(frame.index),
                    "signal_score": [0.9] * len(frame.index),
                    "entry_signal": [True] * len(frame.index),
                    "exit_signal": [False] * len(frame.index),
                    "explanation_ja": ["押し目回復"] * len(frame.index),
                },
                index=frame.index,
            )

    controller.strategy = DummyStrategy()

    controller.run_cycle(1)
    assert len(controller.recent_orders) == 1
    assert controller.recent_orders[0]["filled_avg_price"] == "101.5000"

    controller.run_cycle(2)
    assert len(controller.recent_orders) == 1
    snapshot = controller.snapshot()
    assert snapshot["latest_market_bar_at"]["AAPL"] == entry_index[-1].isoformat()


def test_risk_manager_sizes_automation_positions_by_mode(tmp_path):
    config = load_app_config(write_config(tmp_path))
    manager = RiskManager(config.risk)

    config.risk.order_size_mode = OrderSizingMode.FIXED_AMOUNT
    config.risk.fixed_order_amount = 1000.0
    fixed = manager.size_automation_position(cash=100000.0, equity=100000.0, price=100.0, atr_value=2.0)
    assert fixed.quantity == 10

    config.risk.order_size_mode = OrderSizingMode.EQUITY_FRACTION
    config.risk.equity_fraction_per_trade = 0.10
    fraction = manager.size_automation_position(cash=100000.0, equity=100000.0, price=50.0, atr_value=2.0)
    assert fraction.quantity == 200

    config.risk.order_size_mode = OrderSizingMode.RISK_BASED
    config.risk.risk_per_trade = 0.01
    risk_based = manager.size_automation_position(cash=100000.0, equity=100000.0, price=100.0, atr_value=2.0)
    assert risk_based.quantity == 227


def test_manual_close_position_uses_synced_quantity(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(
        positions=[{"symbol": "NVDA", "qty": "3", "side": "long"}],
        orders=[],
        fills=[],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller._sync_broker_state()

    order = controller.manual_close_position("NVDA")

    assert order["side"] == "sell"
    assert order["qty"] == "3"
    assert fake.positions == []


def test_managed_exit_partial_profit_moves_stop_to_break_even(tmp_path):
    config = load_app_config(write_config(tmp_path))
    state = build_managed_position(
        symbol="AAPL",
        entry_price=100.0,
        quantity=6,
        entry_time=pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN),
        atr_value=2.0,
        risk=config.risk,
        swing_low=96.5,
    )
    latest = pd.Series(
        {
            "high": 108.5,
            "low": 103.0,
            "close": 107.5,
            "entry_atr_14": 2.0,
            "signal_score": 0.82,
            "daily_slope_20": 0.4,
            "weekly_slope_20": 0.3,
            "monthly_slope_20": 0.2,
            "breakout_20": 1.0,
        }
    )

    decision = evaluate_managed_position(
        state=state,
        latest=latest,
        timestamp=pd.Timestamp("2026-04-14 10:15:00", tz=US_EASTERN),
        risk=config.risk,
    )

    assert decision is not None
    assert decision.action == "partial"
    assert decision.quantity >= 1
    assert "一部利確" in decision.reason_ja
    assert state.partial_taken is True
    assert state.stop_price >= state.entry_price
    assert state.initial_stop_price < state.entry_price
    assert state.partial_target_price > state.entry_price
    assert state.next_trailing_price >= state.trailing_stop_price


def test_managed_exit_stagnation_time_stop(tmp_path):
    config = load_app_config(write_config(tmp_path))
    config.risk.stagnation_bars = 3
    config.risk.stagnation_min_r = 0.5
    state = build_managed_position(
        symbol="MSFT",
        entry_price=100.0,
        quantity=2,
        entry_time=pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN),
        atr_value=2.0,
        risk=config.risk,
        swing_low=97.0,
    )
    latest = pd.Series({"high": 101.0, "low": 99.7, "close": 100.6, "entry_atr_14": 2.0})

    first = evaluate_managed_position(
        state=state,
        latest=latest,
        timestamp=pd.Timestamp("2026-04-14 10:15:00", tz=US_EASTERN),
        risk=config.risk,
    )
    second = evaluate_managed_position(
        state=state,
        latest=latest,
        timestamp=pd.Timestamp("2026-04-14 10:30:00", tz=US_EASTERN),
        risk=config.risk,
    )
    third = evaluate_managed_position(
        state=state,
        latest=latest,
        timestamp=pd.Timestamp("2026-04-14 10:45:00", tz=US_EASTERN),
        risk=config.risk,
    )

    assert first is None
    assert second is None
    assert third is not None
    assert third.action == "full"
    assert "時間切れ" in third.reason_ja


def test_recent_swing_low_uses_latest_lookback_window() -> None:
    frame = pd.DataFrame({"low": [103.0, 101.5, 99.2, 100.1, 98.7]})
    assert recent_swing_low(frame, 3) == 98.7


def test_runtime_cycle_uses_partial_profit_exit(monkeypatch, tmp_path):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {"source": "alpaca"},
        },
    )
    env = load_environment()
    fake = PartialAwareFakeBroker(
        positions=[
            {
                "symbol": "AAPL",
                "qty": "4",
                "side": "long",
                "avg_entry_price": "100.0",
                "current_price": "108.0",
            }
        ],
        orders=[],
        fills=[],
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller._sync_broker_state()

    index = pd.DatetimeIndex([pd.Timestamp("2026-04-14 10:15:00", tz=US_EASTERN)])
    entry_frame = pd.DataFrame(
        {
            "open": [100.0],
            "high": [109.0],
            "low": [103.5],
            "close": [108.2],
            "volume": [1200],
            "entry_atr_14": [2.0],
                "signal_score": [0.85],
                "signal_action": ["hold"],
                "entry_signal": [False],
                "exit_signal": [False],
            "explanation_ja": ["強いトレンド継続"],
            "daily_slope_20": [0.4],
            "weekly_slope_20": [0.3],
            "monthly_slope_20": [0.2],
            "breakout_20": [1.0],
            "gap_exhaustion": [0.0],
            "entry_rsi_14": [63.0],
        },
        index=index,
    )
    bundle = MarketDataBundle(symbols={"AAPL": {config.strategy.entry_timeframe: entry_frame}}, benchmarks={}, sectors={})
    monkeypatch.setattr(controller.data_service, "load_runtime_bundle", lambda as_of=None: bundle)
    monkeypatch.setattr(
        "fxautotrade_lab.automation.controller.build_multi_timeframe_feature_set",
        lambda symbol, bars_by_timeframe, benchmark_bars, sector_bars, config: SimpleNamespace(
            entry_frame=bars_by_timeframe[config.strategy.entry_timeframe]
        ),
    )

    class HoldStrategy:
        def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
            return frame.copy()

    controller.strategy = HoldStrategy()
    controller.run_cycle(1)

    assert controller.recent_orders
    assert controller.recent_orders[-1]["side"] == "sell"
    assert controller.recent_orders[-1]["qty"] in {"2", "1"}
    assert "一部利確" in controller.recent_orders[-1]["reason"]


def test_snapshot_includes_managed_exit_levels(monkeypatch, tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    fake = FakeBroker(
        positions=[
            {
                "symbol": "AAPL",
                "qty": "2",
                "side": "long",
                "avg_entry_price": "100.0",
                "current_price": "104.0",
                "market_value": "208.0",
                "unrealized_pl": "8.0",
            }
        ]
    )
    monkeypatch.setattr(AutomationController, "_build_broker", lambda self: fake)
    controller = AutomationController(config, env)
    controller._sync_broker_state()
    controller.managed_positions["AAPL"] = build_managed_position(
        symbol="AAPL",
        entry_price=100.0,
        quantity=2,
        entry_time=pd.Timestamp("2026-04-14 10:00:00", tz=US_EASTERN),
        atr_value=2.0,
        risk=config.risk,
        swing_low=97.0,
    )

    snapshot = controller.snapshot()
    record = snapshot["positions"][0]

    assert record["managed_initial_stop_price"]
    assert record["managed_partial_target_price"]
    assert record["managed_partial_reference_price"]
    assert record["managed_reference_bar_at"]
    assert record["managed_next_trailing_price"]
