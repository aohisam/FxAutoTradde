"""Event-driven tick replay engine for FX scalping research."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, OrderSide
from fxautotrade_lab.data.ticks import validate_tick_frame
from fxautotrade_lab.features.scalping import build_directional_feature_frame
from fxautotrade_lab.ml.scalping import ScalpingModelBundle, ScalpingTrainingConfig


@dataclass(slots=True)
class ScalpingExecutionConfig:
    starting_cash: float = 5_000_000.0
    fixed_order_amount: float = 300_000.0
    minimum_order_quantity: int = 1_000
    quantity_step: int = 1_000
    max_position_notional_fraction: float = 0.25
    entry_latency_ms: int = 250
    cooldown_seconds: int = 5
    max_trades_per_day: int = 120
    mode: BrokerMode = BrokerMode.LOCAL_SIM


@dataclass(slots=True)
class ScalpingBacktestResult:
    symbol: str
    metrics: dict[str, float | int]
    trades: pd.DataFrame
    orders: pd.DataFrame
    fills: pd.DataFrame
    signals: pd.DataFrame
    equity_curve: pd.DataFrame
    model_summary: dict[str, object] = field(default_factory=dict)


def run_scalping_tick_backtest(
    ticks: pd.DataFrame,
    features: pd.DataFrame,
    *,
    symbol: str,
    pip_size: float,
    model_bundle: ScalpingModelBundle,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
) -> ScalpingBacktestResult:
    tick_frame = validate_tick_frame(ticks, symbol=symbol)
    if tick_frame.empty or features.empty:
        return _empty_result(symbol, execution_config.starting_cash, model_bundle)

    long_features = build_directional_feature_frame(features, side="long")
    short_features = build_directional_feature_frame(features, side="short")
    long_prob = model_bundle.model.predict_proba(long_features)
    short_prob = model_bundle.model.predict_proba(short_features)

    threshold = float(model_bundle.decision_threshold)
    cash = float(execution_config.starting_cash)
    equity = cash
    next_allowed_time: pd.Timestamp | None = None
    trades_per_day: dict[str, int] = {}
    trade_rows: list[dict[str, object]] = []
    order_rows: list[dict[str, object]] = []
    fill_rows: list[dict[str, object]] = []
    signal_rows: list[dict[str, object]] = []
    equity_rows: list[dict[str, object]] = [
        {"timestamp": tick_frame.index.min(), "equity": equity, "cash": cash, "exposure": 0.0}
    ]

    tick_index = tick_frame.index
    for timestamp in features.index:
        ts = pd.Timestamp(timestamp)
        ts = ts.tz_localize(ASIA_TOKYO) if ts.tzinfo is None else ts.tz_convert(ASIA_TOKYO)
        if next_allowed_time is not None and ts < next_allowed_time:
            continue
        if ts < tick_index[0] or ts >= tick_index[-1]:
            continue
        local_day = ts.date().isoformat()
        if trades_per_day.get(local_day, 0) >= execution_config.max_trades_per_day:
            continue

        spread = float(features.loc[timestamp, "spread_close_pips"])
        volatility = float(features.loc[timestamp, "micro_volatility_10_pips"])
        if (
            spread > training_config.max_spread_pips
            or volatility < training_config.min_volatility_pips
        ):
            continue

        lp = float(long_prob.loc[timestamp])
        sp = float(short_prob.loc[timestamp])
        side = "long" if lp >= sp else "short"
        probability = max(lp, sp)
        if probability < threshold:
            continue
        entry_target = ts + pd.Timedelta(milliseconds=execution_config.entry_latency_ms)
        entry_index = tick_index.searchsorted(entry_target, side="left")
        if entry_index >= len(tick_index):
            break
        entry_tick = tick_frame.iloc[entry_index]
        quantity = _quantity_for_entry(
            entry_tick,
            side=side,
            cash=cash,
            execution_config=execution_config,
        )
        if quantity <= 0:
            continue
        trade = _simulate_trade_from_entry(
            tick_frame,
            entry_index=entry_index,
            symbol=symbol,
            side=side,
            quantity=quantity,
            pip_size=pip_size,
            training_config=training_config,
        )
        if trade is None:
            continue

        pnl = float(trade["net_pnl"])
        cash += pnl
        equity = cash
        trades_per_day[local_day] = trades_per_day.get(local_day, 0) + 1
        next_allowed_time = pd.Timestamp(trade["exit_time"]) + pd.Timedelta(
            seconds=execution_config.cooldown_seconds
        )
        signal_id = str(uuid4())
        signal_rows.append(
            {
                "signal_id": signal_id,
                "timestamp": ts.isoformat(),
                "symbol": symbol,
                "side": side,
                "probability": probability,
                "long_probability": lp,
                "short_probability": sp,
                "spread_pips": spread,
                "volatility_pips": volatility,
                "accepted": True,
                "explanation_ja": "tickスキャルピングMLが期待値条件を満たしたため採用",
            }
        )
        trade_id = str(uuid4())
        trade.update(
            {
                "trade_id": trade_id,
                "signal_id": signal_id,
                "probability": probability,
                "equity_after": equity,
                "mode": execution_config.mode.value,
            }
        )
        trade_rows.append(trade)
        entry_order, entry_fill = _order_and_fill_rows(trade, trade_id=trade_id, event="entry")
        exit_order, exit_fill = _order_and_fill_rows(trade, trade_id=trade_id, event="exit")
        order_rows.extend([entry_order, exit_order])
        fill_rows.extend([entry_fill, exit_fill])
        equity_rows.append(
            {
                "timestamp": pd.Timestamp(trade["exit_time"]),
                "equity": equity,
                "cash": cash,
                "exposure": 0.0,
            }
        )

    trades = pd.DataFrame(trade_rows)
    orders = pd.DataFrame(order_rows)
    fills = pd.DataFrame(fill_rows)
    signals = pd.DataFrame(signal_rows)
    equity_curve = pd.DataFrame(equity_rows)
    if not equity_curve.empty:
        equity_curve["timestamp"] = pd.to_datetime(equity_curve["timestamp"], errors="coerce")
        equity_curve = equity_curve.set_index("timestamp").sort_index()
    metrics = _compute_scalping_metrics(
        equity_curve, trades, fills, starting_cash=execution_config.starting_cash
    )
    return ScalpingBacktestResult(
        symbol=symbol,
        metrics=metrics,
        trades=trades,
        orders=orders,
        fills=fills,
        signals=signals,
        equity_curve=equity_curve,
        model_summary={
            "decision_threshold": model_bundle.decision_threshold,
            "train_metrics": model_bundle.train_metrics,
            "feature_names": list(model_bundle.model.feature_names),
        },
    )


def _quantity_for_entry(
    tick: pd.Series,
    *,
    side: str,
    cash: float,
    execution_config: ScalpingExecutionConfig,
) -> int:
    price = float(tick["ask"] if side == "long" else tick["bid"])
    max_notional = cash * execution_config.max_position_notional_fraction
    target_notional = min(float(execution_config.fixed_order_amount), max_notional)
    if price <= 0 or target_notional <= 0:
        return 0
    raw_quantity = int(target_notional // price)
    step = max(1, int(execution_config.quantity_step))
    quantity = (raw_quantity // step) * step
    if quantity < int(execution_config.minimum_order_quantity):
        return 0
    return quantity


def _simulate_trade_from_entry(
    tick_frame: pd.DataFrame,
    *,
    entry_index: int,
    symbol: str,
    side: str,
    quantity: int,
    pip_size: float,
    training_config: ScalpingTrainingConfig,
) -> dict[str, object] | None:
    entry_time = pd.Timestamp(tick_frame.index[entry_index])
    entry_tick = tick_frame.iloc[entry_index]
    slip = (training_config.round_trip_slippage_pips / 2.0) * pip_size
    if side == "long":
        entry_price = float(entry_tick["ask"]) + slip
        tp_price = entry_price + training_config.take_profit_pips * pip_size
        sl_price = entry_price - training_config.stop_loss_pips * pip_size
        exit_order_side = OrderSide.SELL
        entry_order_side = OrderSide.BUY
    else:
        entry_price = float(entry_tick["bid"]) - slip
        tp_price = entry_price - training_config.take_profit_pips * pip_size
        sl_price = entry_price + training_config.stop_loss_pips * pip_size
        exit_order_side = OrderSide.BUY
        entry_order_side = OrderSide.SELL

    max_exit_time = entry_time + pd.Timedelta(seconds=training_config.max_hold_seconds)
    end_index = tick_frame.index.searchsorted(max_exit_time, side="right") - 1
    end_index = max(entry_index + 1, min(end_index, len(tick_frame.index) - 1))
    exit_index = end_index
    exit_reason = "time_exit"
    exit_price = 0.0
    for index in range(entry_index + 1, end_index + 1):
        tick = tick_frame.iloc[index]
        if side == "long":
            bid = float(tick["bid"])
            if bid <= sl_price:
                exit_index = index
                exit_price = sl_price - slip
                exit_reason = "stop_loss"
                break
            if bid >= tp_price:
                exit_index = index
                exit_price = tp_price - slip
                exit_reason = "take_profit"
                break
        else:
            ask = float(tick["ask"])
            if ask >= sl_price:
                exit_index = index
                exit_price = sl_price + slip
                exit_reason = "stop_loss"
                break
            if ask <= tp_price:
                exit_index = index
                exit_price = tp_price + slip
                exit_reason = "take_profit"
                break
    if exit_price <= 0:
        exit_tick = tick_frame.iloc[exit_index]
        exit_price = float(exit_tick["bid"] - slip if side == "long" else exit_tick["ask"] + slip)
    exit_time = pd.Timestamp(tick_frame.index[exit_index])
    if side == "long":
        gross_pnl = (exit_price - entry_price) * quantity
        realized_pips = (exit_price - entry_price) / pip_size
    else:
        gross_pnl = (entry_price - exit_price) * quantity
        realized_pips = (entry_price - exit_price) / pip_size
    net_pnl = gross_pnl
    return {
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "realized_pips": realized_pips,
        "hold_seconds": max(0.0, (exit_time - entry_time).total_seconds()),
        "hold_bars": max(1, exit_index - entry_index),
        "entry_order_side": entry_order_side.value,
        "exit_order_side": exit_order_side.value,
        "exit_reason": exit_reason,
        "take_profit_price": tp_price,
        "stop_loss_price": sl_price,
    }


def _order_and_fill_rows(
    trade: dict[str, object],
    *,
    trade_id: str,
    event: str,
) -> tuple[dict[str, object], dict[str, object]]:
    order_id = str(uuid4())
    if event == "entry":
        timestamp = trade["entry_time"]
        side = trade["entry_order_side"]
        price = trade["entry_price"]
    else:
        timestamp = trade["exit_time"]
        side = trade["exit_order_side"]
        price = trade["exit_price"]
    order = {
        "order_id": order_id,
        "trade_id": trade_id,
        "symbol": trade["symbol"],
        "side": side,
        "quantity": trade["quantity"],
        "status": "filled_local_tick_sim",
        "submitted_at": timestamp,
        "filled_at": timestamp,
        "filled_avg_price": price,
        "reason": f"scalping_{event}",
    }
    fill = {
        "fill_id": str(uuid4()),
        "order_id": order_id,
        "trade_id": trade_id,
        "symbol": trade["symbol"],
        "side": side,
        "quantity": trade["quantity"],
        "price": price,
        "filled_at": timestamp,
    }
    return order, fill


def _compute_scalping_metrics(
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    fills: pd.DataFrame,
    *,
    starting_cash: float,
) -> dict[str, float | int]:
    if equity_curve.empty:
        return {}
    ending_equity = float(equity_curve["equity"].iloc[-1])
    net_profit = ending_equity - float(starting_cash)
    wins = trades["net_pnl"] > 0 if not trades.empty else pd.Series(dtype="bool")
    gross_profit = float(trades.loc[wins, "net_pnl"].sum()) if not trades.empty else 0.0
    gross_loss = (
        float(-trades.loc[~wins & (trades["net_pnl"] < 0), "net_pnl"].sum())
        if not trades.empty
        else 0.0
    )
    equity = pd.to_numeric(equity_curve["equity"], errors="coerce")
    drawdown = equity / equity.cummax() - 1.0
    turnover = (
        float(fills["price"].mul(fills["quantity"]).sum() / max(equity.mean(), 1.0))
        if not fills.empty
        else 0.0
    )
    return {
        "starting_equity": float(starting_cash),
        "ending_equity": ending_equity,
        "net_profit": net_profit,
        "total_return": ending_equity / float(starting_cash) - 1.0 if starting_cash else 0.0,
        "number_of_trades": int(len(trades.index)),
        "win_rate": float(wins.mean()) if not trades.empty else 0.0,
        "profit_factor": (
            gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
        ),
        "expectancy": float(trades["net_pnl"].mean()) if not trades.empty else 0.0,
        "average_pips": float(trades["realized_pips"].mean()) if not trades.empty else 0.0,
        "average_hold_seconds": float(trades["hold_seconds"].mean()) if not trades.empty else 0.0,
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "turnover": turnover,
    }


def _empty_result(
    symbol: str, starting_cash: float, model_bundle: ScalpingModelBundle
) -> ScalpingBacktestResult:
    equity_curve = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp.now(tz=ASIA_TOKYO),
                "equity": starting_cash,
                "cash": starting_cash,
                "exposure": 0.0,
            }
        ]
    ).set_index("timestamp")
    return ScalpingBacktestResult(
        symbol=symbol,
        metrics=_compute_scalping_metrics(
            equity_curve, pd.DataFrame(), pd.DataFrame(), starting_cash=starting_cash
        ),
        trades=pd.DataFrame(),
        orders=pd.DataFrame(),
        fills=pd.DataFrame(),
        signals=pd.DataFrame(),
        equity_curve=equity_curve,
        model_summary={
            "decision_threshold": model_bundle.decision_threshold,
            "train_metrics": model_bundle.train_metrics,
        },
    )
