"""Dataclasses shared across modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from fxautotrade_lab.core.enums import BrokerMode, OrderSide, OrderStatus, RunKind, SignalAction


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    quantity: int
    submitted_at: datetime
    reason: str
    stop_price: float | None = None
    limit_price: float | None = None
    client_order_id: str | None = None


@dataclass(slots=True)
class OrderRecord:
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    submitted_at: datetime
    status: OrderStatus
    fill_price: float | None = None
    filled_at: datetime | None = None
    reason: str = ""
    mode: BrokerMode = BrokerMode.LOCAL_SIM


@dataclass(slots=True)
class FillRecord:
    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    price: float
    filled_at: datetime
    fee: float
    slippage: float


@dataclass(slots=True)
class Position:
    symbol: str
    quantity: int
    entry_price: float
    entry_time: datetime
    highest_price: float
    stop_price: float | None = None
    trailing_stop_price: float | None = None
    max_hold_bars: int | None = None
    bars_held: int = 0
    entry_reason: str = ""
    entry_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalRecord:
    timestamp: datetime
    symbol: str
    action: SignalAction
    score: float
    accepted: bool
    strategy_name: str
    reasons: list[str]
    explanation_ja: str
    sub_scores: dict[str, float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TradeRecord:
    symbol: str
    entry_time: datetime
    exit_time: datetime
    quantity: int
    entry_price: float
    exit_price: float
    gross_pnl: float
    net_pnl: float
    hold_bars: int
    entry_reason: str
    exit_reason: str
    entry_score: float
    mode: BrokerMode


@dataclass(slots=True)
class RunArtifact:
    run_id: str
    run_kind: RunKind
    mode: BrokerMode
    strategy_name: str
    started_at: datetime
    output_dir: str
    symbols: list[str]


@dataclass(slots=True)
class BacktestResult:
    run_id: str
    strategy_name: str
    mode: BrokerMode
    symbols: list[str]
    backtest_start: str
    backtest_end: str
    starting_cash: float
    metrics: dict[str, Any]
    equity_curve: pd.DataFrame
    drawdown_curve: pd.DataFrame
    trades: pd.DataFrame
    orders: pd.DataFrame
    fills: pd.DataFrame
    positions: pd.DataFrame
    signals: pd.DataFrame
    benchmark_curve: pd.DataFrame | None
    in_sample_metrics: dict[str, Any]
    out_of_sample_metrics: dict[str, Any]
    walk_forward: list[dict[str, Any]]
    chart_frames: dict[str, dict[str, pd.DataFrame]] = field(default_factory=dict)
    output_dir: str | None = None


@dataclass(slots=True)
class AutomationEvent:
    timestamp: datetime
    level: str
    message_ja: str
    metadata: dict[str, Any] = field(default_factory=dict)
