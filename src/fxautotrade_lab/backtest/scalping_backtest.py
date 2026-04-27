"""End-to-end tick scalping backtest pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.ticks import resample_ticks_to_quote_bars, validate_tick_frame
from fxautotrade_lab.features.scalping import build_scalping_feature_frame, pip_size_for_symbol
from fxautotrade_lab.ml.scalping import (
    ScalpingModelBundle,
    ScalpingTrainingConfig,
    build_triple_barrier_labels,
    fit_scalping_model,
)
from fxautotrade_lab.simulation.scalping_engine import (
    ScalpingBacktestResult,
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)


@dataclass(slots=True)
class ScalpingPipelineResult:
    run_id: str
    symbol: str
    output_dir: Path | None
    bars: pd.DataFrame
    features: pd.DataFrame
    labels: pd.DataFrame
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    model_bundle: ScalpingModelBundle
    backtest: ScalpingBacktestResult


def training_config_from_app(config: AppConfig) -> ScalpingTrainingConfig:
    scalping = config.strategy.fx_scalping
    return ScalpingTrainingConfig(
        take_profit_pips=float(scalping.take_profit_pips),
        stop_loss_pips=float(scalping.stop_loss_pips),
        max_hold_seconds=int(scalping.max_hold_seconds),
        round_trip_slippage_pips=float(scalping.round_trip_slippage_pips),
        fee_pips=float(scalping.fee_pips),
        max_spread_pips=float(scalping.max_spread_pips),
        min_volatility_pips=float(scalping.min_volatility_pips),
        min_samples=int(scalping.min_samples),
        min_threshold_trades=int(scalping.min_threshold_trades),
        decision_threshold=float(scalping.decision_threshold),
        learning_rate=float(scalping.learning_rate),
        max_iter=int(scalping.max_iter),
        l2_penalty=float(scalping.l2_penalty),
        feature_clip=float(scalping.feature_clip),
        seed=int(scalping.seed),
    )


def execution_config_from_app(config: AppConfig) -> ScalpingExecutionConfig:
    scalping = config.strategy.fx_scalping
    return ScalpingExecutionConfig(
        starting_cash=float(config.risk.starting_cash),
        fixed_order_amount=float(config.risk.fixed_order_amount),
        minimum_order_quantity=int(config.risk.minimum_order_quantity),
        quantity_step=int(config.risk.quantity_step),
        max_position_notional_fraction=float(config.risk.max_symbol_exposure),
        entry_latency_ms=int(scalping.entry_latency_ms),
        cooldown_seconds=int(scalping.cooldown_seconds),
        max_trades_per_day=int(scalping.max_trades_per_day),
        mode=config.broker.mode,
    )


def run_scalping_pipeline(
    ticks: pd.DataFrame,
    *,
    symbol: str,
    config: AppConfig,
    output_dir: Path | None = None,
) -> ScalpingPipelineResult:
    tick_frame = validate_tick_frame(ticks, symbol=symbol)
    if tick_frame.empty:
        raise ValueError("スキャルピングバックテスト用の tick データが空です。")
    scalping_cfg = config.strategy.fx_scalping
    pip_size = float(scalping_cfg.pip_size or pip_size_for_symbol(symbol))
    training_config = training_config_from_app(config)
    execution_config = execution_config_from_app(config)
    bars = resample_ticks_to_quote_bars(tick_frame, rule=scalping_cfg.bar_rule, symbol=symbol)
    if bars.empty:
        raise ValueError("tick からスキャルピング用の秒足を生成できませんでした。")
    features = build_scalping_feature_frame(bars, symbol=symbol, pip_size=pip_size)
    labels = build_triple_barrier_labels(bars, pip_size=pip_size, config=training_config)
    split_ts = _split_timestamp(features.index, float(scalping_cfg.train_ratio))
    train_features = features.loc[features.index <= split_ts].copy()
    train_labels = labels.loc[labels.index <= split_ts].copy()
    test_features = features.loc[features.index > split_ts].copy()
    if train_features.empty or test_features.empty:
        raise ValueError("スキャルピング検証の train/test 分割に必要な期間が不足しています。")

    model_bundle = fit_scalping_model(
        train_features,
        train_labels,
        config=training_config,
        metadata={
            "symbol": symbol,
            "bar_rule": scalping_cfg.bar_rule,
            "train_start": train_features.index.min().isoformat(),
            "train_end": train_features.index.max().isoformat(),
            "pip_size": pip_size,
        },
    )
    model_path = scalping_cfg.model_dir / scalping_cfg.latest_model_alias
    model_bundle.save(model_path)
    test_ticks = tick_frame.loc[tick_frame.index > split_ts].copy()
    backtest = run_scalping_tick_backtest(
        test_ticks,
        test_features,
        symbol=symbol,
        pip_size=pip_size,
        model_bundle=model_bundle,
        training_config=training_config,
        execution_config=execution_config,
    )
    run_id = datetime.now(tz=ASIA_TOKYO).strftime("%Y%m%d_%H%M%S_scalping")
    result = ScalpingPipelineResult(
        run_id=run_id,
        symbol=symbol,
        output_dir=output_dir,
        bars=bars,
        features=features,
        labels=labels,
        train_start=train_features.index.min().isoformat(),
        train_end=train_features.index.max().isoformat(),
        test_start=test_features.index.min().isoformat(),
        test_end=test_features.index.max().isoformat(),
        model_bundle=model_bundle,
        backtest=backtest,
    )
    if output_dir is not None:
        export_scalping_pipeline_result(result, output_dir)
    return result


def export_scalping_pipeline_result(result: ScalpingPipelineResult, output_dir: Path) -> Path:
    target = output_dir / result.run_id
    target.mkdir(parents=True, exist_ok=True)
    _write_frame(result.backtest.trades, target / "trades.csv")
    _write_frame(result.backtest.orders, target / "orders.csv")
    _write_frame(result.backtest.fills, target / "fills.csv")
    _write_frame(result.backtest.signals, target / "signals.csv")
    _write_frame(result.backtest.equity_curve, target / "equity_curve.csv")
    summary = {
        "run_id": result.run_id,
        "symbol": result.symbol,
        "train_start": result.train_start,
        "train_end": result.train_end,
        "test_start": result.test_start,
        "test_end": result.test_end,
        "metrics": result.backtest.metrics,
        "model_summary": result.backtest.model_summary,
        "artifacts": {
            "trades": "trades.csv",
            "orders": "orders.csv",
            "fills": "fills.csv",
            "signals": "signals.csv",
            "equity_curve": "equity_curve.csv",
        },
    }
    (target / "summary.json").write_text(
        json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return target


def _split_timestamp(index: pd.DatetimeIndex, train_ratio: float) -> pd.Timestamp:
    if len(index) < 3:
        raise ValueError("スキャルピング検証には最低3本以上の秒足が必要です。")
    ratio = min(max(train_ratio, 0.1), 0.9)
    position = max(1, min(len(index) - 2, int(len(index) * ratio)))
    return pd.Timestamp(index[position])


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    if frame.empty:
        frame.to_csv(path, index=False)
        return
    target = frame.copy()
    include_index = target.index.name is not None or not isinstance(target.index, pd.RangeIndex)
    target.to_csv(path, index=include_index)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, Path):
        return str(value)
    return value
