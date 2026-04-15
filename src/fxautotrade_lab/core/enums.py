"""Project enums."""

from __future__ import annotations

from enum import Enum


class BrokerMode(str, Enum):
    LOCAL_SIM = "local_sim"
    GMO_SIM = "gmo_sim"
    ALPACA_PAPER = "alpaca_paper"
    ALPACA_LIVE = "alpaca_live"


class TimeFrame(str, Enum):
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


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderSizingMode(str, Enum):
    FIXED_AMOUNT = "fixed_amount"
    EQUITY_FRACTION = "equity_fraction"
    RISK_BASED = "risk_based"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class AutomationStatus(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class SignalAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    SKIP = "skip"


class RunKind(str, Enum):
    BACKTEST = "backtest"
    AUTOMATION = "automation"
    DEMO = "demo"
    TRAIN = "train"
    RESEARCH = "research"
