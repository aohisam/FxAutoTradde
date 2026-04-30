"""End-to-end tick scalping backtest pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
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
    build_scalping_training_set,
    build_tick_triple_barrier_labels,
    build_triple_barrier_labels,
    fit_scalping_model,
    select_decision_threshold,
)
from fxautotrade_lab.ml.time_validation import (
    PurgedSplit,
    effective_purge_seconds,
    purged_train_valid_test_split,
    purged_walk_forward_splits,
)
from fxautotrade_lab.persistence.scalping_outcomes import ScalpingOutcomeStore
from fxautotrade_lab.reporting.scalping_calibration import (
    ScalpingCalibrationReport,
    build_probability_calibration_report,
    write_probability_calibration_report,
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
    candidate_model_path: Path | None = None
    latest_model_path: Path | None = None
    walk_forward_results: pd.DataFrame = field(default_factory=pd.DataFrame)
    stress_results: pd.DataFrame = field(default_factory=pd.DataFrame)
    probability_calibration: ScalpingCalibrationReport = field(
        default_factory=ScalpingCalibrationReport
    )
    promotion_metrics: dict[str, object] = field(default_factory=dict)
    outcome_store_summary: dict[str, object] = field(default_factory=dict)


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
        min_validation_net_pips=float(scalping.min_validation_net_pips),
        min_validation_profit_factor=float(scalping.min_validation_profit_factor),
        min_validation_trade_count=int(scalping.min_validation_trade_count),
        fail_closed_on_bad_validation=bool(scalping.fail_closed_on_bad_validation),
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

    run_id = datetime.now(tz=ASIA_TOKYO).strftime("%Y%m%d_%H%M%S_scalping")
    base_metadata = {
        "symbol": symbol,
        "run_id": run_id,
        "model_id": run_id,
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
    }
    model_bundle = fit_scalping_model(
        train_features,
        train_labels,
        config=training_config,
        metadata=base_metadata,
    )
    validation_ticks = _period_tick_window(
        tick_frame,
        start=split.validation_start,
        end=split.validation_end,
        training_config=training_config,
        execution_config=execution_config,
    )
    model_bundle = _apply_validation_threshold_selection(
        model_bundle,
        train_features=train_features,
        train_labels=train_labels,
        validation_ticks=validation_ticks,
        validation_features=validation_features,
        validation_labels=validation_labels,
        symbol=symbol,
        pip_size=pip_size,
        training_config=training_config,
        execution_config=execution_config,
    )
    candidate_model_path = scalping_cfg.model_dir / "candidates" / f"{run_id}.json"
    latest_model_path = scalping_cfg.model_dir / scalping_cfg.latest_model_alias
    model_bundle.metadata.update(
        {
            "candidate_model_path": str(candidate_model_path),
            "latest_model_path": str(latest_model_path),
            "promoted_to_latest": False,
            "promotion_reject_reason_ja": "promotion評価前です。",
            "promotion_metrics": {},
        }
    )
    model_bundle.save(candidate_model_path)
    test_ticks = evaluation_tick_window(
        tick_frame,
        split=split,
        training_config=training_config,
        execution_config=execution_config,
    )
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
    probability_calibration = build_probability_calibration_report(
        backtest.signals,
        backtest.trades,
    )
    promoted, promotion_reject_reason_ja, promotion_metrics = evaluate_scalping_model_promotion(
        test_metrics=backtest.metrics,
        stress_results=stress_results,
        walk_forward_results=walk_forward_results,
        scalping_config=scalping_cfg,
        calibration_report=probability_calibration,
    )
    model_bundle.metadata.update(
        {
            "promoted_to_latest": bool(promoted),
            "promotion_reject_reason_ja": promotion_reject_reason_ja,
            "promotion_metrics": promotion_metrics,
        }
    )
    model_bundle.train_metrics.update(
        {
            "promoted_to_latest": bool(promoted),
            "promotion_reject_reason_ja": promotion_reject_reason_ja,
        }
    )
    model_bundle.save(candidate_model_path)
    if promoted:
        model_bundle.save(latest_model_path)
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
            "probability_calibration": probability_calibration.metrics,
            "promoted_to_latest": bool(promoted),
            "promotion_reject_reason_ja": promotion_reject_reason_ja,
            "promotion_metrics": promotion_metrics,
            "candidate_model_path": str(candidate_model_path),
            "latest_model_path": str(latest_model_path),
        }
    )
    backtest.model_summary["metadata"] = dict(model_bundle.metadata)
    backtest.model_summary["train_metrics"] = dict(model_bundle.train_metrics)
    outcome_store_summary = _append_scalping_outcomes(
        config=config,
        run_id=run_id,
        model_id=run_id,
        symbol=symbol,
        backtest=backtest,
        features=test_features,
    )
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
        candidate_model_path=candidate_model_path,
        latest_model_path=latest_model_path,
        walk_forward_results=walk_forward_results,
        stress_results=stress_results,
        probability_calibration=probability_calibration,
        promotion_metrics=promotion_metrics,
        outcome_store_summary=outcome_store_summary,
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
        validation_ticks = _period_tick_window(
            tick_frame,
            start=split.validation_start,
            end=split.validation_end,
            training_config=training_config,
            execution_config=execution_config,
        )
        bundle = _apply_validation_threshold_selection(
            bundle,
            train_features=train_features,
            train_labels=train_labels,
            validation_ticks=validation_ticks,
            validation_features=validation_features,
            validation_labels=validation_labels,
            symbol=symbol,
            pip_size=pip_size,
            training_config=training_config,
            execution_config=execution_config,
        )
        test_ticks = evaluation_tick_window(
            tick_frame,
            split=split,
            training_config=training_config,
            execution_config=execution_config,
        )
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
                "threshold_selection_method": bundle.metadata.get(
                    "threshold_selection_method", ""
                ),
                "validation_gate_passed": bundle.train_metrics.get(
                    "validation_gate_passed", True
                ),
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


def select_decision_threshold_by_replay(
    validation_ticks: pd.DataFrame,
    validation_features: pd.DataFrame,
    *,
    symbol: str,
    pip_size: float,
    model_bundle: ScalpingModelBundle,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
    labels: pd.DataFrame | None = None,
) -> tuple[float, dict[str, float | int]]:
    """Select a decision threshold by replaying the validation tick window."""

    if validation_ticks.empty or validation_features.empty:
        raise ValueError("validation replay に必要な tick/features が空です。")
    best_threshold = float(training_config.decision_threshold)
    best_metrics: dict[str, float | int] = {
        "candidate_count": int(len(validation_features.index)),
        "selected_count": 0,
        "selected_net_pips": 0.0,
        "selected_mean_pips": 0.0,
        "selected_profit_factor": 0.0,
        "selected_max_drawdown_pips": 0.0,
        "objective": float("-inf"),
    }
    for threshold in training_config.threshold_grid:
        replay_bundle = replace(model_bundle, decision_threshold=float(threshold))
        replay = run_scalping_tick_backtest(
            validation_ticks,
            validation_features,
            symbol=symbol,
            pip_size=pip_size,
            model_bundle=replay_bundle,
            training_config=training_config,
            execution_config=execution_config,
            labels=labels,
            include_future_outcomes=True,
        )
        metrics = _threshold_metrics_from_replay(replay, validation_features)
        if metrics["selected_count"] < int(training_config.min_threshold_trades):
            continue
        if float(metrics["objective"]) > float(best_metrics["objective"]):
            best_threshold = float(threshold)
            best_metrics = metrics
    if best_metrics["selected_count"] == 0:
        best_metrics["objective"] = 0.0
    return best_threshold, best_metrics


def evaluate_scalping_model_promotion(
    *,
    test_metrics: dict[str, object],
    stress_results: pd.DataFrame,
    walk_forward_results: pd.DataFrame,
    scalping_config: Any,
    calibration_report: ScalpingCalibrationReport | None = None,
) -> tuple[bool, str, dict[str, object]]:
    """Return whether a candidate model can become the approved latest model."""

    test_profit_factor = _metric_float(test_metrics, "profit_factor")
    test_trade_count = int(_metric_float(test_metrics, "number_of_trades"))
    test_net_profit = _metric_float(test_metrics, "net_profit")
    test_drawdown_amount = abs(min(0.0, _metric_float(test_metrics, "max_drawdown_amount")))
    stress_profit_factor = _frame_min(stress_results, "profit_factor")
    stress_net_profit = _frame_min(stress_results, "net_profit")
    walk_forward_pass_ratio = _walk_forward_pass_ratio(
        walk_forward_results,
        min_profit_factor=float(getattr(scalping_config, "min_test_profit_factor", 0.0)),
        min_trade_count=int(getattr(scalping_config, "min_test_trade_count", 0)),
        min_net_profit=float(getattr(scalping_config, "min_test_net_profit", -1e18)),
    )
    metrics: dict[str, object] = {
        "test_profit_factor": test_profit_factor,
        "test_trade_count": test_trade_count,
        "test_net_profit": test_net_profit,
        "test_drawdown_amount": test_drawdown_amount,
        "stress_min_profit_factor": stress_profit_factor,
        "stress_min_net_profit": stress_net_profit,
        "walk_forward_pass_ratio": walk_forward_pass_ratio,
        "calibration": dict(calibration_report.metrics) if calibration_report is not None else {},
        "requirements": {
            "min_test_profit_factor": float(
                getattr(scalping_config, "min_test_profit_factor", 0.0)
            ),
            "min_test_trade_count": int(getattr(scalping_config, "min_test_trade_count", 0)),
            "min_test_net_profit": float(getattr(scalping_config, "min_test_net_profit", -1e18)),
            "max_test_drawdown_amount": getattr(
                scalping_config, "max_test_drawdown_amount", None
            ),
            "min_stress_profit_factor": float(
                getattr(scalping_config, "min_stress_profit_factor", 0.0)
            ),
            "min_stress_net_profit": float(
                getattr(scalping_config, "min_stress_net_profit", -1e18)
            ),
            "min_walk_forward_pass_ratio": float(
                getattr(scalping_config, "min_walk_forward_pass_ratio", 0.0)
            ),
        },
    }
    failures: list[str] = []
    min_test_pf = float(getattr(scalping_config, "min_test_profit_factor", 0.0))
    if test_profit_factor < min_test_pf:
        failures.append(
            f"test profit factor が基準未満です({test_profit_factor:.3f} < {min_test_pf:.3f})"
        )
    min_test_trades = int(getattr(scalping_config, "min_test_trade_count", 0))
    if test_trade_count < min_test_trades:
        failures.append(
            f"test trade count が不足しています({test_trade_count} < {min_test_trades})"
        )
    min_test_profit = float(getattr(scalping_config, "min_test_net_profit", -1e18))
    if test_net_profit < min_test_profit:
        failures.append(
            f"test net profit が基準未満です({test_net_profit:.3f} < {min_test_profit:.3f})"
        )
    max_drawdown_amount = getattr(scalping_config, "max_test_drawdown_amount", None)
    if max_drawdown_amount is not None and test_drawdown_amount > float(max_drawdown_amount):
        failures.append(
            "test max drawdown amount が上限超過です"
            f"({test_drawdown_amount:.3f} > {float(max_drawdown_amount):.3f})"
        )
    min_stress_pf = float(getattr(scalping_config, "min_stress_profit_factor", 0.0))
    if stress_profit_factor is None:
        if min_stress_pf > 0.0:
            failures.append("stress test 結果がありません")
    elif stress_profit_factor < min_stress_pf:
        failures.append(
            "stress min profit factor が基準未満です"
            f"({stress_profit_factor:.3f} < {min_stress_pf:.3f})"
        )
    min_stress_profit = float(getattr(scalping_config, "min_stress_net_profit", -1e18))
    if stress_net_profit is None:
        if min_stress_profit > -1e11:
            failures.append("stress test net profit 結果がありません")
    elif stress_net_profit < min_stress_profit:
        failures.append(
            "stress min net profit が基準未満です"
            f"({stress_net_profit:.3f} < {min_stress_profit:.3f})"
        )
    min_wf_ratio = float(getattr(scalping_config, "min_walk_forward_pass_ratio", 0.0))
    if walk_forward_pass_ratio is None:
        if min_wf_ratio > 0.0:
            failures.append("walk-forward 結果がありません")
    elif walk_forward_pass_ratio < min_wf_ratio:
        failures.append(
            "walk-forward pass ratio が基準未満です"
            f"({walk_forward_pass_ratio:.3f} < {min_wf_ratio:.3f})"
        )
    if failures:
        return False, " / ".join(failures), metrics
    return True, "", metrics


def evaluation_tick_window(
    tick_frame: pd.DataFrame,
    *,
    split: PurgedSplit,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
) -> pd.DataFrame:
    """Return only the ticks needed to evaluate a split's test period.

    Features already restrict new entries to ``split.test_index``.  The tick
    window includes enough future ticks to settle those entries, but does not
    expose later folds or later test periods to the replay engine.
    """

    return _period_tick_window(
        tick_frame,
        start=split.test_start,
        end=split.test_end,
        training_config=training_config,
        execution_config=execution_config,
    )


def _apply_validation_threshold_selection(
    model_bundle: ScalpingModelBundle,
    *,
    train_features: pd.DataFrame,
    train_labels: pd.DataFrame,
    validation_ticks: pd.DataFrame,
    validation_features: pd.DataFrame,
    validation_labels: pd.DataFrame,
    symbol: str,
    pip_size: float,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
) -> ScalpingModelBundle:
    method = "validation_replay"
    warning = ""
    try:
        threshold, selected_metrics = select_decision_threshold_by_replay(
            validation_ticks,
            validation_features,
            symbol=symbol,
            pip_size=pip_size,
            model_bundle=model_bundle,
            training_config=training_config,
            execution_config=execution_config,
            labels=validation_labels,
        )
    except Exception as exc:  # noqa: BLE001
        method = "label_fallback"
        warning = (
            "validation replay によるthreshold選択に失敗したため、"
            f"label集計fallbackを使いました: {exc}"
        )
        threshold, selected_metrics = _select_decision_threshold_by_labels(
            model_bundle,
            train_features=train_features,
            train_labels=train_labels,
            validation_features=validation_features,
            validation_labels=validation_labels,
            training_config=training_config,
        )
    selected_threshold_before_gate = float(threshold)
    validation_gate_passed, gate_warning = _validation_gate_status(
        selected_metrics,
        config=training_config,
    )
    if not validation_gate_passed and training_config.fail_closed_on_bad_validation:
        threshold = 1.01
    combined_warning = gate_warning or warning
    updated = replace(model_bundle, decision_threshold=float(threshold))
    updated.train_metrics.update(
        {
            "threshold_selected_on": "validation",
            "threshold_selection_method": method,
            "selected_threshold": float(threshold),
            "selected_threshold_before_validation_gate": selected_threshold_before_gate,
            "validation_gate_passed": bool(validation_gate_passed),
            "validation_sample_count": int(len(validation_features.index)),
            "validation_selected_count": int(selected_metrics.get("selected_count", 0)),
            "validation_net_pips": float(selected_metrics.get("selected_net_pips", 0.0)),
            "validation_mean_pips": float(selected_metrics.get("selected_mean_pips", 0.0)),
            "validation_profit_factor": float(
                selected_metrics.get("selected_profit_factor", 0.0)
            ),
            "validation_max_drawdown": float(
                selected_metrics.get("selected_max_drawdown_pips", 0.0)
            ),
            **selected_metrics,
        }
    )
    if combined_warning:
        updated.train_metrics["warning_ja"] = combined_warning
    updated.metadata.update(
        {
            "threshold_selected_on": "validation",
            "threshold_selection_method": method,
            "validation_gate_passed": bool(validation_gate_passed),
        }
    )
    if combined_warning:
        updated.metadata["warning_ja"] = combined_warning
    return updated


def _select_decision_threshold_by_labels(
    model_bundle: ScalpingModelBundle,
    *,
    train_features: pd.DataFrame,
    train_labels: pd.DataFrame,
    validation_features: pd.DataFrame,
    validation_labels: pd.DataFrame,
    training_config: ScalpingTrainingConfig,
) -> tuple[float, dict[str, float | int]]:
    threshold_features, _, threshold_meta = build_scalping_training_set(
        validation_features,
        validation_labels,
        config=training_config,
    )
    threshold_source = "validation_label"
    if threshold_features.empty:
        threshold_features, _, threshold_meta = build_scalping_training_set(
            train_features,
            train_labels,
            config=training_config,
        )
        threshold_source = "train_label"
    if threshold_features.empty:
        return float(training_config.decision_threshold), {
            "candidate_count": 0,
            "selected_count": 0,
            "selected_net_pips": 0.0,
            "selected_mean_pips": 0.0,
            "selected_profit_factor": 0.0,
            "selected_max_drawdown_pips": 0.0,
            "objective": 0.0,
            "label_threshold_source": threshold_source,
        }
    threshold, selected_metrics = select_decision_threshold(
        model_bundle.model,
        threshold_features,
        threshold_meta,
        config=training_config,
    )
    return threshold, {**selected_metrics, "label_threshold_source": threshold_source}


def _validation_gate_status(
    selected_metrics: dict[str, float | int],
    *,
    config: ScalpingTrainingConfig,
) -> tuple[bool, str]:
    selected_count = int(selected_metrics.get("selected_count", 0))
    selected_net_pips = float(selected_metrics.get("selected_net_pips", 0.0))
    selected_profit_factor = float(selected_metrics.get("selected_profit_factor", 0.0))
    failures: list[str] = []
    if selected_count < int(config.min_validation_trade_count):
        failures.append(
            "validation採用候補数が不足しています"
            f"({selected_count} < {int(config.min_validation_trade_count)})"
        )
    if selected_net_pips < float(config.min_validation_net_pips):
        failures.append(
            "validation net pips が基準未満です"
            f"({selected_net_pips:.3f} < {float(config.min_validation_net_pips):.3f})"
        )
    if selected_profit_factor < float(config.min_validation_profit_factor):
        failures.append(
            "validation profit factor が基準未満です"
            f"({selected_profit_factor:.3f} < {float(config.min_validation_profit_factor):.3f})"
        )
    if not failures:
        return True, ""
    warning = "validation gate未達: " + " / ".join(failures)
    if config.fail_closed_on_bad_validation:
        warning += "。decision_threshold=1.01 にして新規entryを停止します。"
    else:
        warning += "。fail_closed_on_bad_validation=false のため閾値は維持します。"
    return False, warning


def _threshold_metrics_from_replay(
    replay: ScalpingBacktestResult,
    validation_features: pd.DataFrame,
) -> dict[str, float | int]:
    trades = replay.trades
    net_pips = (
        pd.to_numeric(trades["realized_net_pips"], errors="coerce").fillna(0.0)
        if not trades.empty and "realized_net_pips" in trades.columns
        else pd.Series(dtype="float64")
    )
    count = int(len(net_pips.index))
    gross_profit = float(net_pips[net_pips > 0.0].sum()) if count else 0.0
    gross_loss = float(-net_pips[net_pips < 0.0].sum()) if count else 0.0
    profit_factor = (
        gross_profit / gross_loss if gross_loss > 0.0 else (99.0 if gross_profit else 0.0)
    )
    equity = net_pips.cumsum()
    drawdown = equity - equity.cummax()
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    total = float(net_pips.sum()) if count else 0.0
    mean = float(net_pips.mean()) if count else 0.0
    objective = total + mean * count * 0.25 + min(profit_factor, 5.0) * 2.0 + max_drawdown * 0.5
    return {
        "candidate_count": int(len(validation_features.index)),
        "selected_count": count,
        "selected_net_pips": total,
        "selected_mean_pips": mean,
        "selected_profit_factor": float(profit_factor),
        "selected_max_drawdown_pips": max_drawdown,
        "objective": float(objective),
    }


def _period_tick_window(
    tick_frame: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    training_config: ScalpingTrainingConfig,
    execution_config: ScalpingExecutionConfig,
) -> pd.DataFrame:
    window_start = pd.Timestamp(start)
    window_end = (
        pd.Timestamp(end)
        + pd.Timedelta(seconds=max(1, int(training_config.max_hold_seconds)))
        + pd.Timedelta(milliseconds=max(0, int(execution_config.entry_latency_ms)))
    )
    return tick_frame.loc[
        (tick_frame.index >= window_start) & (tick_frame.index <= window_end)
    ].copy()


def _append_scalping_outcomes(
    *,
    config: AppConfig,
    run_id: str,
    model_id: str,
    symbol: str,
    backtest: ScalpingBacktestResult,
    features: pd.DataFrame,
) -> dict[str, object]:
    scalping_cfg = config.strategy.fx_scalping
    if not scalping_cfg.outcome_store_enabled:
        return {"enabled": False}
    store_dir = scalping_cfg.outcome_store_dir or (scalping_cfg.model_dir / "outcomes")
    store = ScalpingOutcomeStore(store_dir)
    summary = store.append_backtest(
        run_id=run_id,
        model_id=model_id,
        symbol=symbol,
        signals=backtest.signals,
        trades=backtest.trades,
        features=features,
    )
    return {"enabled": True, "root_dir": str(store_dir), **summary}


def _metric_float(metrics: dict[str, object], key: str) -> float:
    try:
        return float(metrics.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _frame_min(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.min())


def _walk_forward_pass_ratio(
    frame: pd.DataFrame,
    *,
    min_profit_factor: float,
    min_trade_count: int,
    min_net_profit: float,
) -> float | None:
    if frame.empty:
        return None
    required = {"profit_factor", "number_of_trades", "net_profit"}
    if not required.issubset(frame.columns):
        return None
    working = frame.copy()
    passed = (
        (pd.to_numeric(working["profit_factor"], errors="coerce").fillna(0.0) >= min_profit_factor)
        & (
            pd.to_numeric(working["number_of_trades"], errors="coerce").fillna(0).astype(int)
            >= min_trade_count
        )
        & (pd.to_numeric(working["net_profit"], errors="coerce").fillna(0.0) >= min_net_profit)
    )
    return float(passed.mean()) if len(passed.index) else None


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
    calibration_artifacts = write_probability_calibration_report(
        result.probability_calibration,
        target,
    )
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
        "candidate_model_path": str(result.candidate_model_path or ""),
        "latest_model_path": str(result.latest_model_path or ""),
        "promotion_metrics": _jsonable(result.promotion_metrics),
        "outcome_store_summary": _jsonable(result.outcome_store_summary),
        "probability_calibration": _jsonable(result.probability_calibration.to_summary()),
        "artifacts": {
            "trades": "trades.csv",
            "orders": "orders.csv",
            "fills": "fills.csv",
            "signals": "signals.csv",
            "equity_curve": "equity_curve.csv",
            "walk_forward": "walk_forward.csv",
            "stress_results": "stress_results.csv",
            **calibration_artifacts,
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
