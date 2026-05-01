"""Project enums."""

from __future__ import annotations

from enum import StrEnum


class BrokerMode(StrEnum):
    LOCAL_SIM = "local_sim"
    GMO_SIM = "gmo_sim"


class TimeFrame(StrEnum):
    MIN_1 = "1Min"
    MIN_5 = "5Min"
    MIN_10 = "10Min"
    MIN_15 = "15Min"
    MIN_30 = "30Min"
    HOUR_1 = "1Hour"
    HOUR_4 = "4Hour"
    HOUR_8 = "8Hour"
    HOUR_12 = "12Hour"
    DAY_1 = "1Day"
    WEEK_1 = "1Week"
    MONTH_1 = "1Month"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderSizingMode(StrEnum):
    FIXED_AMOUNT = "fixed_amount"
    EQUITY_FRACTION = "equity_fraction"
    RISK_BASED = "risk_based"


class OrderStatus(StrEnum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class AutomationStatus(StrEnum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class SignalAction(StrEnum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    SKIP = "skip"


class RunKind(StrEnum):
    BACKTEST = "backtest"
    AUTOMATION = "automation"
    DEMO = "demo"
    TRAIN = "train"
    RESEARCH = "research"
