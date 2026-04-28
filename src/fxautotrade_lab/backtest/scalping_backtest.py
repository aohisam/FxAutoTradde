"""End-to-end tick scalping backtest pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
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
    build_tick_triple_barrier_labels,
    build_triple_barrier_labels,
    fit_scalping_model,
)
from fxautotrade_lab.ml.time_validation import (
    PurgedSplit,
    effective_purge_seconds,
    purged_train_valid_test_split,
    purged_walk_forward_splits,
)
from fxautotrade_lab.simulation.scalping_engine import (
    BlackoutWindow,
    ScalpingBacktestResult,
    ScalpingExecutionConfig,
    run_scalping_tick_backtest,
)
from fxautotrade_lab.simulation.scalping_stress import run_scalping_stress_grid


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
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str
    model_bundle: ScalpingModelBundle
    backtest: ScalpingBacktestResult
    split: PurgedSplit
    walk_forward_results: pd.DataFrame = field(default_factory=pd.DataFrame)
    stress_results: pd.DataFrame = field(default_factory=pd.DataFrame)


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
        max_daily_loss_amount=(
            float(scalping.max_daily_loss_amount)
            if scalping.max_daily_loss_amount is not None
            else float(config.risk.max_daily_loss_amount)
        ),
        max_consecutive_losses=scalping.max_consecutive_losses,
        halt_for_day_on_daily_loss=bool(scalping.halt_for_day_on_daily_loss),
        halt_for_day_on_consecutive_losses=bool(scalping.halt_for_day_on_consecutive_losses),
        max_tick_gap_seconds=scalping.max_tick_gap_seconds,
        reject_on_stale_ticks=bool(scalping.reject_on_stale_ticks),
        max_spread_z=scalping.max_spread_z,
        max_spread_to_mean_ratio=scalping.max_spread_to_mean_ratio,
        record_rejected_signals=bool(scalping.record_rejected_signals),
        max_rejected_signals=scalping.max_rejected_signals,
        blackout_windows_jst=tuple(
            BlackoutWindow(start=window.start, end=window.end, reason=window.reason)
            for window in scalping.blackout_windows_jst
        ),
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
    if scalping_cfg.label_source == "tick":
        labels = build_tick_triple_barrier_labels(
            tick_frame,
            sample_index=features.index,
            pip_size=pip_size,
            config=training_config,
            entry_latency_ms=execution_config.entry_latency_ms,
            symbol=symbol,
        )
    else:
        labels = build_triple_barrier_labels(bars, pip_size=pip_size, config=training_config)
    purge_seconds = effective_purge_seconds(
        max_hold_seconds=training_config.max_hold_seconds,
        entry_latency_ms=execution_config.entry_latency_ms,
        cooldown_seconds=execution_config.cooldown_seconds,
        configured_purge_seconds=scalping_cfg.purge_seconds,
    )
    split = purged_train_valid_test_split(
        features.index,
        train_ratio=float(scalping_cfg.train_ratio),
        validation_ratio=float(scalping_cfg.validation_ratio),
        test_ratio=float(scalping_cfg.test_ratio),
        purge_seconds=purge_seconds,
        label_horizon_seconds=training_config.max_hold_seconds,
    )
    train_features = features.loc[split.train_index].copy()
    train_labels = labels.loc[split.train_index].copy()
    validation_features = features.loc[split.validation_index].copy()
    validation_labels = labels.loc[split.validation_index].copy()
    test_features = features.loc[split.test_index].copy()
    test_labels = labels.loc[split.test_index].copy()
    if train_features.empty or validation_features.empty or test_features.empty:
        raise ValueError(
            "スキャルピング検証の train/validation/test 分割に必要な期間が不足しています。"
        )

    model_bundle = fit_scalping_model(
        train_features,
        train_labels,
        config=training_config,
        validation_features=validation_features,
        validation_labels=validation_labels,
        metadata={
            "symbol": symbol,
            "bar_rule": scalping_cfg.bar_rule,
            "train_start": train_features.index.min().isoformat(),
            "train_end": train_features.index.max().isoformat(),
            "validation_start": validation_features.index.min().isoformat(),
            "validation_end": validation_features.index.max().isoformat(),
            "test_start": test_features.index.min().isoformat(),
            "test_end": test_features.index.max().isoformat(),
            "pip_size": pip_size,
            "purge_seconds": purge_seconds,
            "label_source": scalping_cfg.label_source,
        },
    )
    model_path = scalping_cfg.model_dir / scalping_cfg.latest_model_alias
    model_bundle.save(model_path)
    test_ticks = tick_frame.loc[tick_frame.index >= split.test_start].copy()
    backtest = run_scalping_tick_backtest(
        test_ticks,
        test_features,
        symbol=symbol,
        pip_size=pip_size,
        model_bundle=model_bundle,
        training_config=training_config,
        execution_config=execution_config,
        labels=test_labels,
        include_future_outcomes=True,
    )
    walk_forward_results = _run_walk_forward_if_enabled(
        tick_frame,
        features,
        labels,
        symbol=symbol,
        pip_size=pip_size,
        config=config,
        training_config=training_config,
        execution_config=execution_config,
        purge_seconds=purge_seconds,
    )
    stress_results = run_scalping_stress_grid(
        tick_frame,
        bars,
        symbol=symbol,
        pip_size=pip_size,
        bar_rule=scalping_cfg.bar_rule,
        model_bundle=model_bundle,
        training_config=training_config,
        execution_config=execution_config,
        spread_multipliers=list(scalping_cfg.spread_stress_multipliers),
        latency_ms_grid=list(scalping_cfg.latency_ms_grid),
        evaluation_index=split.test_index,
    )
    backtest.metrics.update(
        {
            "test_sample_count": int(len(test_features.index)),
            "train_start": train_features.index.min().isoformat(),
            "train_end": train_features.index.max().isoformat(),
            "validation_start": validation_features.index.min().isoformat(),
            "validation_end": validation_features.index.max().isoformat(),
            "test_start": test_features.index.min().isoformat(),
            "test_end": test_features.index.max().isoformat(),
            "purge_seconds": int(purge_seconds),
            "label_source": scalping_cfg.label_source,
            "stress_test_summary": _stress_summary_records(stress_results),
        }
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
        validation_start=validation_features.index.min().isoformat(),
        validation_end=validation_features.index.max().isoformat(),
        test_start=test_features.index.min().isoformat(),
        test_end=test_features.index.max().isoformat(),
        model_bundle=model_bundle,
        backtest=backtest,
        split=split,
        walk_forward_results=walk_forward_results,
        stress_results=stress_results,
    )
    if output_dir is not None:
        export_scalping_pipeline_result(result, output_dir)
    return result


def _run_walk_forward_if_enabled(
    tick_frame: pd.DataFrame,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    symbol: str,
    pip_size: float,
    config: AppConfig,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
    purge_seconds: int,
) -> pd.DataFrame:
    scalping_cfg = config.strategy.fx_scalping
    if not scalping_cfg.walk_forward_enabled:
        return pd.DataFrame()
    folds = purged_walk_forward_splits(
        features.index,
        train_days=int(scalping_cfg.walk_forward_train_days),
        validation_days=int(scalping_cfg.walk_forward_validation_days),
        test_days=int(scalping_cfg.walk_forward_test_days),
        purge_seconds=purge_seconds,
        label_horizon_seconds=training_config.max_hold_seconds,
        min_folds=int(scalping_cfg.min_walk_forward_folds),
    )
    rows: list[dict[str, object]] = []
    for fold_number, split in enumerate(folds, start=1):
        train_features = features.loc[split.train_index]
        train_labels = labels.loc[split.train_index]
        validation_features = features.loc[split.validation_index]
        validation_labels = labels.loc[split.validation_index]
        test_features = features.loc[split.test_index]
        bundle = fit_scalping_model(
            train_features,
            train_labels,
            config=training_config,
            validation_features=validation_features,
            validation_labels=validation_labels,
            metadata={
                "symbol": symbol,
                "walk_forward_fold": fold_number,
                "train_start": split.train_start.isoformat(),
                "train_end": split.train_end.isoformat(),
                "validation_start": split.validation_start.isoformat(),
                "validation_end": split.validation_end.isoformat(),
                "test_start": split.test_start.isoformat(),
                "test_end": split.test_end.isoformat(),
                "label_source": scalping_cfg.label_source,
            },
        )
        test_ticks = tick_frame.loc[tick_frame.index >= split.test_start].copy()
        result = run_scalping_tick_backtest(
            test_ticks,
            test_features,
            symbol=symbol,
            pip_size=pip_size,
            model_bundle=bundle,
            training_config=training_config,
            execution_config=execution_config,
            labels=labels.loc[split.test_index],
            include_future_outcomes=True,
        )
        rows.append(
            {
                "fold": fold_number,
                "train_start": split.train_start.isoformat(),
                "train_end": split.train_end.isoformat(),
                "validation_start": split.validation_start.isoformat(),
                "validation_end": split.validation_end.isoformat(),
                "test_start": split.test_start.isoformat(),
                "test_end": split.test_end.isoformat(),
                "selected_threshold": bundle.decision_threshold,
                "number_of_trades": result.metrics.get("number_of_trades", 0),
                "net_profit": result.metrics.get("net_profit", 0.0),
                "total_return": result.metrics.get("total_return", 0.0),
                "profit_factor": result.metrics.get("profit_factor", 0.0),
                "average_net_pips": result.metrics.get("average_net_pips", 0.0),
                "max_drawdown": result.metrics.get("max_drawdown", 0.0),
                "win_rate": result.metrics.get("win_rate", 0.0),
            }
        )
    return pd.DataFrame(rows)


def export_scalping_pipeline_result(result: ScalpingPipelineResult, output_dir: Path) -> Path:
    target = output_dir / result.run_id
    target.mkdir(parents=True, exist_ok=True)
    _write_frame(result.backtest.trades, target / "trades.csv")
    _write_frame(result.backtest.orders, target / "orders.csv")
    _write_frame(result.backtest.fills, target / "fills.csv")
    _write_frame(result.backtest.signals, target / "signals.csv")
    _write_frame(result.backtest.equity_curve, target / "equity_curve.csv")
    _write_frame(result.walk_forward_results, target / "walk_forward.csv")
    _write_frame(result.stress_results, target / "stress_results.csv")
    (target / "stress_results.json").write_text(
        json.dumps(
            _jsonable(result.stress_results.to_dict(orient="records")),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    summary = {
        "run_id": result.run_id,
        "symbol": result.symbol,
        "train_start": result.train_start,
        "train_end": result.train_end,
        "validation_start": result.validation_start,
        "validation_end": result.validation_end,
        "test_start": result.test_start,
        "test_end": result.test_end,
        "purge_seconds": result.split.purge_seconds,
        "label_source": result.model_bundle.metadata.get(
            "label_source", result.backtest.metrics.get("label_source", "")
        ),
        "metrics": result.backtest.metrics,
        "model_summary": result.backtest.model_summary,
        "stress_summary": _jsonable(_stress_summary_records(result.stress_results)),
        "artifacts": {
            "trades": "trades.csv",
            "orders": "orders.csv",
            "fills": "fills.csv",
            "signals": "signals.csv",
            "equity_curve": "equity_curve.csv",
            "walk_forward": "walk_forward.csv",
            "stress_results": "stress_results.csv",
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


def _stress_summary_records(stress_results: pd.DataFrame) -> list[dict[str, object]]:
    if stress_results.empty:
        return []
    columns = [
        "spread_multiplier",
        "entry_latency_ms",
        "number_of_trades",
        "net_profit",
        "total_return",
        "profit_factor",
        "average_net_pips",
        "max_drawdown",
        "win_rate",
        "warning_ja",
    ]
    available = [column for column in columns if column in stress_results.columns]
    return _jsonable(stress_results[available].to_dict(orient="records"))


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
