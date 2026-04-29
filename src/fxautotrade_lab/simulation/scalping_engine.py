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


@dataclass(frozen=True, slots=True)
class BlackoutWindow:
    start: str
    end: str
    reason: str = "manual"


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
    max_daily_loss_amount: float | None = None
    max_consecutive_losses: int | None = None
    halt_for_day_on_daily_loss: bool = True
    halt_for_day_on_consecutive_losses: bool = True
    max_tick_gap_seconds: int | None = None
    reject_on_stale_ticks: bool = True
    max_spread_z: float | None = None
    max_spread_to_mean_ratio: float | None = None
    record_rejected_signals: bool = True
    max_rejected_signals: int | None = None
    blackout_windows_jst: tuple[BlackoutWindow, ...] = ()


@dataclass(slots=True)
class ScalpingBacktestResult:
    symbol: str
    metrics: dict[str, object]
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
    labels: pd.DataFrame | None = None,
    include_future_outcomes: bool = False,
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
    daily_start_cash: dict[str, float] = {}
    daily_halts: dict[str, str] = {}
    consecutive_losses_by_day: dict[str, int] = {}
    rejected_rows_recorded = 0
    for timestamp in features.index:
        ts = pd.Timestamp(timestamp)
        ts = ts.tz_localize(ASIA_TOKYO) if ts.tzinfo is None else ts.tz_convert(ASIA_TOKYO)
        local_day = ts.date().isoformat()
        daily_start_cash.setdefault(local_day, cash)
        consecutive_losses_by_day.setdefault(local_day, 0)
        trades_today = trades_per_day.get(local_day, 0)
        daily_pnl = cash - daily_start_cash[local_day]
        spread = _feature_float(features, timestamp, "spread_close_pips")
        spread_mean = _feature_float(features, timestamp, "spread_mean_20_pips")
        spread_z = _feature_float(features, timestamp, "spread_z_120")
        volatility = _feature_float(features, timestamp, "micro_volatility_10_pips")
        lp = float(long_prob.loc[timestamp])
        sp = float(short_prob.loc[timestamp])
        side = "long" if lp >= sp else "short"
        probability = max(lp, sp)
        signal_id = str(uuid4())
        reject_reason = ""
        quantity = 0
        entry_index = -1
        entry_tick: pd.Series | None = None
        trade: dict[str, object] | None = None
        if ts < tick_index[0] or ts >= tick_index[-1]:
            reject_reason = "outside_tick_window"
        elif next_allowed_time is not None and ts < next_allowed_time:
            reject_reason = "cooldown"
        elif local_day in daily_halts:
            reject_reason = daily_halts[local_day]
        elif trades_today >= execution_config.max_trades_per_day:
            reject_reason = "max_trades_per_day"
        elif _blackout_reason(ts, execution_config.blackout_windows_jst):
            reject_reason = (
                f"blackout_window:{_blackout_reason(ts, execution_config.blackout_windows_jst)}"
            )
        elif _is_stale_tick(
            tick_index,
            ts,
            max_tick_gap_seconds=execution_config.max_tick_gap_seconds,
            reject_on_stale_ticks=execution_config.reject_on_stale_ticks,
        ):
            reject_reason = "stale_tick"
        elif spread > training_config.max_spread_pips:
            reject_reason = "spread_exceeded"
        elif execution_config.max_spread_z is not None and abs(spread_z) > float(
            execution_config.max_spread_z
        ):
            reject_reason = "spread_z_exceeded"
        elif (
            execution_config.max_spread_to_mean_ratio is not None
            and spread_mean > 0.0
            and spread / spread_mean > float(execution_config.max_spread_to_mean_ratio)
        ):
            reject_reason = "spread_to_mean_exceeded"
        elif volatility < training_config.min_volatility_pips:
            reject_reason = "volatility_too_low"
        elif probability < threshold:
            reject_reason = "threshold_not_met"
        if not reject_reason:
            entry_target = ts + pd.Timedelta(milliseconds=execution_config.entry_latency_ms)
            entry_index = tick_index.searchsorted(entry_target, side="left")
            if entry_index >= len(tick_index):
                reject_reason = "entry_tick_not_found"
            elif entry_index >= len(tick_index) - 1:
                reject_reason = "exit_tick_not_found"
        if not reject_reason:
            entry_tick = tick_frame.iloc[entry_index]
            quantity = _quantity_for_entry(
                entry_tick,
                side=side,
                cash=cash,
                execution_config=execution_config,
            )
            if quantity <= 0:
                reject_reason = "quantity_too_small"
        if not reject_reason:
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
                reject_reason = "exit_tick_not_found"
        if reject_reason:
            if execution_config.record_rejected_signals and (
                execution_config.max_rejected_signals is None
                or rejected_rows_recorded < int(execution_config.max_rejected_signals)
            ):
                signal_rows.append(
                    _signal_row(
                        signal_id=signal_id,
                        timestamp=ts,
                        symbol=symbol,
                        chosen_side=side,
                        accepted=False,
                        reject_reason=reject_reason,
                        probability=probability,
                        long_probability=lp,
                        short_probability=sp,
                        threshold=threshold,
                        spread=spread,
                        spread_mean=spread_mean,
                        spread_z=spread_z,
                        volatility=volatility,
                        trades_today=trades_today,
                        daily_pnl=daily_pnl,
                        consecutive_losses=consecutive_losses_by_day[local_day],
                        labels=labels,
                        include_future_outcomes=include_future_outcomes,
                    )
                )
                rejected_rows_recorded += 1
            continue

        pnl = float(trade["net_pnl"])
        cash += pnl
        equity = cash
        trades_per_day[local_day] = trades_per_day.get(local_day, 0) + 1
        if pnl < 0.0:
            consecutive_losses_by_day[local_day] += 1
        elif pnl > 0.0:
            consecutive_losses_by_day[local_day] = 0
        daily_pnl = cash - daily_start_cash[local_day]
        if (
            execution_config.max_daily_loss_amount is not None
            and execution_config.halt_for_day_on_daily_loss
            and daily_pnl <= -abs(float(execution_config.max_daily_loss_amount))
        ):
            daily_halts[local_day] = "daily_loss_halt"
        if (
            execution_config.max_consecutive_losses is not None
            and execution_config.halt_for_day_on_consecutive_losses
            and consecutive_losses_by_day[local_day] >= int(execution_config.max_consecutive_losses)
        ):
            daily_halts[local_day] = "consecutive_loss_halt"
        next_allowed_time = pd.Timestamp(trade["exit_time"]) + pd.Timedelta(
            seconds=execution_config.cooldown_seconds
        )
        signal_rows.append(
            _signal_row(
                signal_id=signal_id,
                timestamp=ts,
                symbol=symbol,
                chosen_side=side,
                accepted=True,
                reject_reason="accepted",
                probability=probability,
                long_probability=lp,
                short_probability=sp,
                threshold=threshold,
                spread=spread,
                spread_mean=spread_mean,
                spread_z=spread_z,
                volatility=volatility,
                trades_today=trades_per_day[local_day],
                daily_pnl=daily_pnl,
                consecutive_losses=consecutive_losses_by_day[local_day],
                labels=labels,
                include_future_outcomes=include_future_outcomes,
            )
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
        equity_curve,
        trades,
        fills,
        signals,
        starting_cash=execution_config.starting_cash,
    )
    metrics.update(
        {
            "selected_threshold": float(model_bundle.decision_threshold),
            "threshold_selected_on": str(
                model_bundle.train_metrics.get("threshold_selected_on", "")
            ),
            "train_sample_count": int(model_bundle.train_metrics.get("train_sample_count", 0)),
            "validation_sample_count": int(
                model_bundle.train_metrics.get("validation_sample_count", 0)
            ),
            "validation_gate_passed": bool(
                model_bundle.train_metrics.get("validation_gate_passed", True)
            ),
            "validation_warning_ja": str(model_bundle.train_metrics.get("warning_ja", "")),
        }
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


def _feature_float(features: pd.DataFrame, timestamp: object, column: str) -> float:
    if column not in features.columns:
        return 0.0
    value = pd.to_numeric(pd.Series([features.loc[timestamp, column]]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else 0.0


def _signal_row(
    *,
    signal_id: str,
    timestamp: pd.Timestamp,
    symbol: str,
    chosen_side: str,
    accepted: bool,
    reject_reason: str,
    probability: float,
    long_probability: float,
    short_probability: float,
    threshold: float,
    spread: float,
    spread_mean: float,
    spread_z: float,
    volatility: float,
    trades_today: int,
    daily_pnl: float,
    consecutive_losses: int,
    labels: pd.DataFrame | None,
    include_future_outcomes: bool,
) -> dict[str, object]:
    row: dict[str, object] = {
        "signal_id": signal_id,
        "timestamp": timestamp.isoformat(),
        "symbol": symbol,
        "chosen_side": chosen_side,
        "side": chosen_side if accepted else "",
        "probability": float(probability),
        "long_probability": float(long_probability),
        "short_probability": float(short_probability),
        "threshold": float(threshold),
        "spread_pips": float(spread),
        "spread_mean_20_pips": float(spread_mean),
        "spread_z_120": float(spread_z),
        "volatility_pips": float(volatility),
        "accepted": bool(accepted),
        "reject_reason": reject_reason if reject_reason else ("accepted" if accepted else ""),
        "explanation_ja": _reject_explanation(reject_reason, accepted=accepted),
        "trades_today": int(trades_today),
        "daily_pnl": float(daily_pnl),
        "consecutive_losses": int(consecutive_losses),
    }
    if include_future_outcomes and labels is not None and not labels.empty:
        row.update(_future_label_outcome(labels, timestamp))
    return row


def _future_label_outcome(labels: pd.DataFrame, timestamp: pd.Timestamp) -> dict[str, object]:
    if timestamp not in labels.index:
        return {}
    label_row = labels.loc[timestamp]
    if isinstance(label_row, pd.DataFrame):
        label_row = label_row.iloc[0]
    return {
        "future_long_net_pips": float(label_row.get("long_net_pips", 0.0)),
        "future_short_net_pips": float(label_row.get("short_net_pips", 0.0)),
        "future_long_exit_reason": str(label_row.get("long_exit_reason", "")),
        "future_short_exit_reason": str(label_row.get("short_exit_reason", "")),
    }


def _reject_explanation(reason: str, *, accepted: bool) -> str:
    if accepted:
        return "tickスキャルピングMLが全ての検証条件を満たしたため採用"
    mapping = {
        "outside_tick_window": "検証対象のtick期間外のため不採用",
        "cooldown": "直前決済後のクールダウン中のため不採用",
        "max_trades_per_day": "1日の最大取引回数に達したため不採用",
        "daily_loss_halt": "当日損失停止に達したため新規エントリーを停止",
        "consecutive_loss_halt": "当日連敗停止に達したため新規エントリーを停止",
        "stale_tick": "直近tickが古く約定前提が不安定なため不採用",
        "spread_exceeded": "スプレッドが許容上限を超えたため不採用",
        "spread_z_exceeded": "スプレッドz-scoreが異常域のため不採用",
        "spread_to_mean_exceeded": "スプレッドが短期平均比で大きすぎるため不採用",
        "volatility_too_low": "短期ボラティリティが不足しているため不採用",
        "threshold_not_met": "予測確率がdecision threshold未満のため不採用",
        "entry_tick_not_found": "レイテンシ反映後のentry tickが見つからないため不採用",
        "quantity_too_small": "注文数量が最小数量を下回るため不採用",
        "exit_tick_not_found": "検証可能なexit tickが見つからないため不採用",
    }
    if reason.startswith("blackout_window:"):
        window_reason = reason.split(":", 1)[1]
        return f"ブラックアウト時間帯({window_reason})のため不採用"
    return mapping.get(reason, f"検証条件を満たさないため不採用: {reason}")


def _is_stale_tick(
    tick_index: pd.DatetimeIndex,
    timestamp: pd.Timestamp,
    *,
    max_tick_gap_seconds: int | None,
    reject_on_stale_ticks: bool,
) -> bool:
    if not reject_on_stale_ticks or max_tick_gap_seconds is None:
        return False
    previous_index = tick_index.searchsorted(timestamp, side="right") - 1
    if previous_index < 0:
        return True
    previous_tick_time = pd.Timestamp(tick_index[previous_index])
    gap = (timestamp - previous_tick_time).total_seconds()
    return gap > float(max_tick_gap_seconds)


def _blackout_reason(timestamp: pd.Timestamp, windows: tuple[BlackoutWindow, ...]) -> str:
    if not windows:
        return ""
    local = (
        timestamp.tz_convert(ASIA_TOKYO)
        if timestamp.tzinfo is not None
        else timestamp.tz_localize(ASIA_TOKYO)
    )
    minutes = local.hour * 60 + local.minute
    for window in windows:
        start = _hhmm_to_minutes(window.start)
        end = _hhmm_to_minutes(window.end)
        if start == end:
            continue
        in_window = start <= minutes < end if start < end else minutes >= start or minutes < end
        if in_window:
            return window.reason
    return ""


def _hhmm_to_minutes(value: str) -> int:
    try:
        hour_text, minute_text = str(value).split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception as exc:
        raise ValueError(f"blackout window の時刻は HH:MM 形式で指定してください: {value}") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"blackout window の時刻が範囲外です: {value}")
    return hour * 60 + minute


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
    if entry_index >= len(tick_frame.index) - 1:
        return None
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
    if end_index >= len(tick_frame.index):
        return None
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
        gross_pips = (exit_price - entry_price) / pip_size
    else:
        gross_pnl = (entry_price - exit_price) * quantity
        gross_pips = (entry_price - exit_price) / pip_size
    fee_pips = float(training_config.fee_pips)
    fee_amount = fee_pips * pip_size * quantity
    net_pnl = gross_pnl - fee_amount
    realized_net_pips = gross_pips - fee_pips
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
        "fee_pips": fee_pips,
        "fee_amount": fee_amount,
        "realized_gross_pips": gross_pips,
        "realized_net_pips": realized_net_pips,
        "realized_pips": realized_net_pips,
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
        "fee_amount": trade.get("fee_amount", 0.0) if event == "exit" else 0.0,
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
        "fee_amount": trade.get("fee_amount", 0.0) if event == "exit" else 0.0,
    }
    return order, fill


def _compute_scalping_metrics(
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    fills: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    starting_cash: float,
) -> dict[str, object]:
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
    gross_profit_amount = (
        float(trades.loc[trades["gross_pnl"] > 0, "gross_pnl"].sum())
        if not trades.empty and "gross_pnl" in trades.columns
        else 0.0
    )
    equity = pd.to_numeric(equity_curve["equity"], errors="coerce")
    drawdown = equity / equity.cummax() - 1.0
    turnover = (
        float(fills["price"].mul(fills["quantity"]).sum() / max(equity.mean(), 1.0))
        if not fills.empty
        else 0.0
    )
    realized_gross = (
        pd.to_numeric(trades["realized_gross_pips"], errors="coerce")
        if not trades.empty and "realized_gross_pips" in trades.columns
        else pd.Series(dtype="float64")
    )
    realized_net = (
        pd.to_numeric(trades["realized_net_pips"], errors="coerce")
        if not trades.empty and "realized_net_pips" in trades.columns
        else pd.Series(dtype="float64")
    )
    reject_counts = (
        signals.loc[~signals["accepted"].astype(bool), "reject_reason"].value_counts().to_dict()
        if not signals.empty
        and "accepted" in signals.columns
        and "reject_reason" in signals.columns
        else {}
    )
    accepted_signal_count = (
        int(signals["accepted"].astype(bool).sum())
        if not signals.empty and "accepted" in signals.columns
        else 0
    )
    rejected_signal_count = (
        int((~signals["accepted"].astype(bool)).sum())
        if not signals.empty and "accepted" in signals.columns
        else 0
    )
    monthly_pnl = _group_sum_by_period(
        trades, time_column="exit_time", value_column="net_pnl", freq="ME"
    )
    return {
        "starting_equity": float(starting_cash),
        "ending_equity": ending_equity,
        "net_profit": net_profit,
        "gross_profit_amount": gross_profit_amount,
        "total_fee_pips": (
            float(trades["fee_pips"].sum())
            if not trades.empty and "fee_pips" in trades.columns
            else 0.0
        ),
        "total_fee_amount": (
            float(trades["fee_amount"].sum())
            if not trades.empty and "fee_amount" in trades.columns
            else 0.0
        ),
        "total_return": ending_equity / float(starting_cash) - 1.0 if starting_cash else 0.0,
        "number_of_trades": int(len(trades.index)),
        "win_rate": float(wins.mean()) if not trades.empty else 0.0,
        "profit_factor": (
            gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
        ),
        "expectancy": float(trades["net_pnl"].mean()) if not trades.empty else 0.0,
        "average_gross_pips": float(realized_gross.mean()) if not realized_gross.empty else 0.0,
        "average_net_pips": float(realized_net.mean()) if not realized_net.empty else 0.0,
        "average_pips": float(realized_net.mean()) if not realized_net.empty else 0.0,
        "average_hold_seconds": float(trades["hold_seconds"].mean()) if not trades.empty else 0.0,
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "turnover": turnover,
        "accepted_signal_count": accepted_signal_count,
        "rejected_signal_count": rejected_signal_count,
        "reject_reason_counts": reject_counts,
        "session_performance": _session_performance(trades),
        "weekday_performance": _time_group_performance(trades, group="weekday"),
        "hour_performance": _time_group_performance(trades, group="hour"),
        "side_performance": _column_group_performance(trades, "side"),
        "exit_reason_performance": _column_group_performance(trades, "exit_reason"),
        "max_consecutive_losses": _max_consecutive_losses(trades),
        "daily_max_loss": _daily_max_loss(trades),
        "monthly_pnl": monthly_pnl,
    }


def _group_sum_by_period(
    trades: pd.DataFrame, *, time_column: str, value_column: str, freq: str
) -> dict[str, float]:
    if trades.empty or time_column not in trades.columns or value_column not in trades.columns:
        return {}
    timestamps = pd.to_datetime(trades[time_column], errors="coerce")
    values = pd.to_numeric(trades[value_column], errors="coerce").fillna(0.0)
    frame = pd.DataFrame({"timestamp": timestamps, "value": values}).dropna(subset=["timestamp"])
    if frame.empty:
        return {}
    grouped = frame.set_index("timestamp")["value"].resample(freq).sum()
    return {
        str(index.date() if hasattr(index, "date") else index): float(value)
        for index, value in grouped.items()
    }


def _column_group_performance(
    trades: pd.DataFrame, column: str
) -> dict[str, dict[str, float | int]]:
    if trades.empty or column not in trades.columns:
        return {}
    out: dict[str, dict[str, float | int]] = {}
    for key, group in trades.groupby(column, dropna=False):
        wins = pd.to_numeric(group["net_pnl"], errors="coerce") > 0
        out[str(key)] = {
            "trades": int(len(group.index)),
            "net_profit": float(pd.to_numeric(group["net_pnl"], errors="coerce").sum()),
            "average_net_pips": (
                float(
                    pd.to_numeric(
                        group.get("realized_net_pips", pd.Series(dtype="float64")), errors="coerce"
                    ).mean()
                )
                if "realized_net_pips" in group.columns
                else 0.0
            ),
            "win_rate": float(wins.mean()) if len(group.index) else 0.0,
        }
    return out


def _time_group_performance(
    trades: pd.DataFrame, *, group: str
) -> dict[str, dict[str, float | int]]:
    if trades.empty or "exit_time" not in trades.columns:
        return {}
    timestamps = pd.to_datetime(trades["exit_time"], errors="coerce")
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize(ASIA_TOKYO)
    else:
        timestamps = timestamps.dt.tz_convert(ASIA_TOKYO)
    working = trades.copy()
    if group == "weekday":
        working["_group"] = timestamps.dt.weekday.astype("string")
    else:
        working["_group"] = timestamps.dt.hour.astype("string")
    return _column_group_performance(working, "_group")


def _session_performance(trades: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    if trades.empty or "exit_time" not in trades.columns:
        return {}
    timestamps = pd.to_datetime(trades["exit_time"], errors="coerce")
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize(ASIA_TOKYO)
    else:
        timestamps = timestamps.dt.tz_convert(ASIA_TOKYO)
    hours = timestamps.dt.hour
    sessions = pd.Series("other", index=trades.index, dtype="object")
    sessions.loc[(hours >= 9) & (hours < 15)] = "tokyo"
    sessions.loc[(hours >= 16) | (hours < 1)] = "london"
    sessions.loc[(hours >= 21) | (hours < 6)] = "newyork"
    working = trades.copy()
    working["_session"] = sessions
    return _column_group_performance(working, "_session")


def _max_consecutive_losses(trades: pd.DataFrame) -> int:
    if trades.empty or "net_pnl" not in trades.columns:
        return 0
    current = 0
    longest = 0
    for value in pd.to_numeric(trades["net_pnl"], errors="coerce").fillna(0.0):
        if value < 0.0:
            current += 1
            longest = max(longest, current)
        elif value > 0.0:
            current = 0
    return longest


def _daily_max_loss(trades: pd.DataFrame) -> float:
    if trades.empty or "exit_time" not in trades.columns or "net_pnl" not in trades.columns:
        return 0.0
    timestamps = pd.to_datetime(trades["exit_time"], errors="coerce")
    values = pd.to_numeric(trades["net_pnl"], errors="coerce").fillna(0.0)
    frame = pd.DataFrame({"day": timestamps.dt.date, "net_pnl": values}).dropna(subset=["day"])
    if frame.empty:
        return 0.0
    daily = frame.groupby("day")["net_pnl"].sum()
    return float(min(0.0, daily.min()))


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
            equity_curve,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            starting_cash=starting_cash,
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
