"""Stress helpers for scalping tick replay."""

from __future__ import annotations

from dataclasses import replace

import pandas as pd

from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame
from fxautotrade_lab.data.ticks import validate_tick_frame
from fxautotrade_lab.features.scalping import build_scalping_feature_frame
from fxautotrade_lab.ml.scalping import ScalpingModelBundle, ScalpingTrainingConfig
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)


def stress_tick_spread(ticks: pd.DataFrame, *, multiplier: float, symbol: str) -> pd.DataFrame:
    """Widen bid/ask spread while preserving mid price."""

    frame = validate_tick_frame(ticks, symbol=symbol)
    if frame.empty:
        return frame
    factor = max(0.0, float(multiplier))
    mid = (frame["bid"] + frame["ask"]) / 2.0
    spread = (frame["ask"] - frame["bid"]).clip(lower=1e-12) * factor
    if factor == 0.0:
        spread = pd.Series(1e-12, index=frame.index)
    stressed = frame.copy()
    stressed["bid"] = mid - spread / 2.0
    stressed["ask"] = mid + spread / 2.0
    stressed["mid"] = mid
    stressed["spread"] = stressed["ask"] - stressed["bid"]
    return stressed


def stress_quote_spread(quote_bars: pd.DataFrame, *, multiplier: float) -> pd.DataFrame:
    """Widen quote-bar bid/ask spread while preserving each OHLC mid."""

    bars = validate_quote_bar_frame(quote_bars)
    factor = max(0.0, float(multiplier))
    stressed = bars.copy()
    for column in ("open", "high", "low", "close"):
        bid_column = f"bid_{column}"
        ask_column = f"ask_{column}"
        mid = (stressed[bid_column] + stressed[ask_column]) / 2.0
        spread = (stressed[ask_column] - stressed[bid_column]).clip(lower=1e-12) * factor
        if factor == 0.0:
            spread = pd.Series(1e-12, index=stressed.index)
        stressed[bid_column] = mid - spread / 2.0
        stressed[ask_column] = mid + spread / 2.0
    return validate_quote_bar_frame(stressed)


def run_scalping_stress_grid(
    ticks: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    symbol: str,
    pip_size: float,
    bar_rule: str,
    model_bundle: ScalpingModelBundle,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
    spread_multipliers: list[float],
    latency_ms_grid: list[int],
    evaluation_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    """Replay scalping backtest across spread and latency stress scenarios."""

    rows: list[dict[str, object]] = []
    base_ticks = validate_tick_frame(ticks, symbol=symbol)
    target_index = _normalized_evaluation_index(evaluation_index)
    for multiplier in spread_multipliers or [1.0]:
        stressed_ticks = stress_tick_spread(base_ticks, multiplier=float(multiplier), symbol=symbol)
        stressed_bars = stress_quote_spread(bars, multiplier=float(multiplier))
        stressed_features_full = build_scalping_feature_frame(
            stressed_bars, symbol=symbol, pip_size=pip_size
        )
        stressed_features = (
            stressed_features_full.loc[stressed_features_full.index.intersection(target_index)]
            if target_index is not None
            else stressed_features_full
        )
        if stressed_features.empty:
            rows.append(
                _empty_stress_row(
                    multiplier=float(multiplier),
                    latency_ms=int(execution_config.entry_latency_ms),
                    warning_ja="stress評価対象の特徴量が空です。",
                )
            )
            continue
        for latency_ms in latency_ms_grid or [execution_config.entry_latency_ms]:
            stressed_execution = replace(execution_config, entry_latency_ms=int(latency_ms))
            result = run_scalping_tick_backtest(
                stressed_ticks,
                stressed_features,
                symbol=symbol,
                pip_size=pip_size,
                model_bundle=model_bundle,
                training_config=training_config,
                execution_config=stressed_execution,
            )
            metrics = result.metrics
            rows.append(
                {
                    "spread_multiplier": float(multiplier),
                    "entry_latency_ms": int(latency_ms),
                    "number_of_trades": int(metrics.get("number_of_trades", 0)),
                    "net_profit": float(metrics.get("net_profit", 0.0)),
                    "total_return": float(metrics.get("total_return", 0.0)),
                    "profit_factor": float(metrics.get("profit_factor", 0.0)),
                    "average_net_pips": float(metrics.get("average_net_pips", 0.0)),
                    "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
                    "win_rate": float(metrics.get("win_rate", 0.0)),
                    "warning_ja": _stress_warning(
                        metrics, multiplier=float(multiplier), latency_ms=int(latency_ms)
                    ),
                }
            )
    return pd.DataFrame(rows)


def _normalized_evaluation_index(index: pd.DatetimeIndex | None) -> pd.DatetimeIndex | None:
    if index is None:
        return None
    if not isinstance(index, pd.DatetimeIndex):
        raise TypeError("stress評価のevaluation_indexには DatetimeIndex が必要です。")
    return pd.DatetimeIndex(index)


def _empty_stress_row(*, multiplier: float, latency_ms: int, warning_ja: str) -> dict[str, object]:
    return {
        "spread_multiplier": float(multiplier),
        "entry_latency_ms": int(latency_ms),
        "number_of_trades": 0,
        "net_profit": 0.0,
        "total_return": 0.0,
        "profit_factor": 0.0,
        "average_net_pips": 0.0,
        "max_drawdown": 0.0,
        "win_rate": 0.0,
        "warning_ja": warning_ja,
    }


def _stress_warning(metrics: dict[str, object], *, multiplier: float, latency_ms: int) -> str:
    net_profit = float(metrics.get("net_profit", 0.0))
    profit_factor = float(metrics.get("profit_factor", 0.0))
    if multiplier >= 1.5 and net_profit < 0.0:
        return "spread悪化シナリオで損益が大きく悪化しています。頑健性を追加確認してください。"
    if latency_ms >= 500 and profit_factor < 1.0:
        return "latency悪化シナリオでPFが1未満です。約定遅延への脆弱性があります。"
    return ""
