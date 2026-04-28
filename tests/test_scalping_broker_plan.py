from __future__ import annotations

import pandas as pd
import pytest

from fxautotrade_lab.brokers.local_sim import LocalSimBroker
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.execution.scalping_plan import (
    create_scalping_order_plan,
    submit_scalping_entry_to_broker,
    validate_scalping_order_plan_for_submission,
)


def test_scalping_order_plan_maps_long_and_short_sides() -> None:
    created_at = pd.Timestamp("2026-02-03 09:00:00", tz=ASIA_TOKYO)

    long_plan = create_scalping_order_plan(
        symbol="USDJPY",
        side="long",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=150.020,
        stop_loss_price=149.990,
        reason="test_long",
        created_at=created_at,
    )
    short_plan = create_scalping_order_plan(
        symbol="USD_JPY",
        side="short",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=149.980,
        stop_loss_price=150.010,
        reason="test_short",
        created_at=created_at,
    )

    assert long_plan.symbol == "USD_JPY"
    assert long_plan.entry_order_side == OrderSide.BUY
    assert long_plan.exit_order_side == OrderSide.SELL
    assert short_plan.entry_order_side == OrderSide.SELL
    assert short_plan.exit_order_side == OrderSide.BUY
    assert long_plan.to_entry_order_payload()["dry_run_only"] is True


def test_scalping_order_plan_dry_run_does_not_submit_order() -> None:
    broker = LocalSimBroker()
    broker.update_market_data({"USD_JPY": 150.0}, pd.Timestamp("2026-02-03", tz=ASIA_TOKYO))
    plan = create_scalping_order_plan(
        symbol="USD_JPY",
        side="long",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=150.020,
        stop_loss_price=149.990,
        reason="dry_run_test",
    )

    result = submit_scalping_entry_to_broker(broker, plan, dry_run=True)

    assert result["status"] == "dry_run"
    assert broker.submitted_orders == []


def test_scalping_order_plan_allows_local_sim_submission_only() -> None:
    broker = LocalSimBroker()
    broker.update_market_data({"USD_JPY": 150.0}, pd.Timestamp("2026-02-03", tz=ASIA_TOKYO))
    plan = create_scalping_order_plan(
        symbol="USD_JPY",
        side="long",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=150.020,
        stop_loss_price=149.990,
        reason="local_sim_test",
    )

    result = submit_scalping_entry_to_broker(broker, plan, dry_run=False)

    assert result["status"] == "filled_local_sim"
    assert len(broker.submitted_orders) == 1


def test_scalping_order_plan_rejects_future_private_submission() -> None:
    plan = create_scalping_order_plan(
        symbol="USD_JPY",
        side="long",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=150.020,
        stop_loss_price=149.990,
        reason="future_private_test",
        broker_mode=BrokerMode.GMO_SIM,
    )

    with pytest.raises(RuntimeError, match="private broker実注文送信は未実装"):
        validate_scalping_order_plan_for_submission(
            plan,
            broker_mode=BrokerMode.GMO_SIM,
            dry_run=False,
            live_trading_enabled=False,
        )


def test_scalping_order_plan_does_not_treat_live_flag_as_permission() -> None:
    plan = create_scalping_order_plan(
        symbol="USD_JPY",
        side="long",
        quantity=1000,
        entry_price=150.000,
        take_profit_price=150.020,
        stop_loss_price=149.990,
        reason="live_flag_test",
        live_submission_allowed=True,
    )

    with pytest.raises(RuntimeError, match="live tradingを有効化しません"):
        validate_scalping_order_plan_for_submission(
            plan,
            broker_mode=BrokerMode.LOCAL_SIM,
            dry_run=False,
            live_trading_enabled=True,
        )
