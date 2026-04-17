"""FX-specific backtest runner with optional ML participation filter."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.backtest.metrics import compute_drawdown, compute_metrics
from fxautotrade_lab.backtest.walk_forward import rolling_walk_forward, split_in_out_sample
from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.core.models import BacktestResult
from fxautotrade_lab.core.windows import shift_timestamp
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.ml.fx_filter import (
    apply_fx_ml_filter,
    build_labeled_dataset,
    fit_fx_filter_model,
    latest_model_path,
    load_filter_model,
    ml_filter_summary,
    save_labeled_dataset,
)
from fxautotrade_lab.reporting.exporters import export_backtest_artifacts
from fxautotrade_lab.simulation.fx_engine import FxQuotePortfolioSimulator
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy

def _build_test_windows(start: pd.Timestamp, end: pd.Timestamp, config: AppConfig) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    walk_cfg = config.strategy.fx_breakout_pullback.ml_filter.walk_forward
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = start
    while cursor < end:
        window_end = min(shift_timestamp(cursor, walk_cfg.test_window, backward=False), end)
        if cursor < window_end:
            windows.append((cursor, window_end))
        next_cursor = shift_timestamp(cursor, walk_cfg.retrain_frequency, backward=False)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    return windows


def _training_window_start(window_start: pd.Timestamp, history_start: pd.Timestamp, config: AppConfig) -> pd.Timestamp:
    walk_cfg = config.strategy.fx_breakout_pullback.ml_filter.walk_forward
    if walk_cfg.mode == "rolling":
        return max(history_start, shift_timestamp(window_start, walk_cfg.train_window, backward=True))
    initial_anchor = shift_timestamp(history_start, "0d", backward=True)
    return initial_anchor if history_start <= window_start else window_start


def _signal_log_frame(signal_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    logs: list[pd.DataFrame] = []
    for symbol, frame in signal_frames.items():
        if frame.empty:
            continue
        logs.append(frame.reset_index().rename(columns={"index": "timestamp"}).assign(symbol=symbol))
    return pd.concat(logs, ignore_index=True).sort_values("timestamp") if logs else pd.DataFrame()


def _slice_window(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    return frame.loc[(frame.index >= start) & (frame.index < end)].copy()


def _resolve_model_path(config: AppConfig) -> Path:
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    if ml_cfg.pretrained_model_path is not None:
        return ml_cfg.pretrained_model_path
    return latest_model_path(config)


def _load_model_for_backtest(config: AppConfig):
    model = load_filter_model(_resolve_model_path(config))
    if model is not None:
        return model
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    if ml_cfg.require_pretrained_model or ml_cfg.missing_model_behavior == "error":
        raise RuntimeError("学習済み ML モデルが見つかりません。")
    return None


def _save_model_and_dataset(model, dataset: pd.DataFrame, config: AppConfig) -> dict[str, str]:
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_dir = ml_cfg.model_dir
    dataset_dir = ml_cfg.dataset_dir
    model_path = model_dir / f"fx_filter_{timestamp}.json"
    saved_model_path = model.save(model_path) if ml_cfg.save_trained_model else None
    latest_path = None
    if saved_model_path is not None:
        latest_path = model.save(model_dir / ml_cfg.latest_model_alias)
    dataset_path = save_labeled_dataset(dataset, dataset_dir / f"labels_{timestamp}.parquet")
    return {
        "model_path": str(saved_model_path) if saved_model_path is not None else "",
        "latest_model_path": str(latest_path) if latest_path is not None else "",
        "dataset_path": str(dataset_path),
    }


def _build_symbol_signals(
    config: AppConfig,
    data_service: MarketDataService,
    *,
    history_start: str,
    backtest_end: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]:
    strategy = FxBreakoutPullbackStrategy(config)
    signal_frames: dict[str, pd.DataFrame] = {}
    chart_frames: dict[str, dict[str, pd.DataFrame]] = {}
    raw_execution_frames: dict[str, pd.DataFrame] = {}
    for symbol in config.watchlist.symbols:
        frames = data_service.load_symbol_frames(symbol, start=history_start, end=backtest_end)
        feature_set = build_fx_feature_set(symbol=symbol, bars_by_timeframe=frames, config=config, runtime_mode=False)
        signals = strategy.generate_signal_frame(feature_set.execution_frame)
        signal_frames[symbol] = signals
        raw_execution_frames[symbol] = feature_set.execution_frame
        chart_frames[symbol] = {
            config.strategy.fx_breakout_pullback.execution_timeframe.value: signals.copy(),
            config.strategy.fx_breakout_pullback.signal_timeframe.value: feature_set.signal_frame.copy(),
            config.strategy.fx_breakout_pullback.trend_timeframe.value: feature_set.trend_frame.copy(),
        }
    return signal_frames, chart_frames, raw_execution_frames


def _benchmark_curve(
    config: AppConfig,
    signal_frames: dict[str, pd.DataFrame],
    equity_curve: pd.DataFrame,
) -> pd.DataFrame | None:
    if equity_curve.empty or not config.watchlist.benchmark_symbols:
        return None
    benchmark_symbol = config.watchlist.benchmark_symbols[0]
    frame = signal_frames.get(benchmark_symbol)
    if frame is None or frame.empty:
        return None
    benchmark_prices = frame.reindex(equity_curve.index, method="ffill")
    return pd.DataFrame(
        {
            "benchmark_equity": config.risk.starting_cash
            * (benchmark_prices["close"] / benchmark_prices["close"].iloc[0])
        },
        index=equity_curve.index,
    )


def _train_model_from_history(
    signal_frames: dict[str, pd.DataFrame],
    config: AppConfig,
    *,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
) -> tuple[object, pd.DataFrame, dict[str, str]]:
    training_slices = {
        symbol: _slice_window(frame, train_start, train_end)
        for symbol, frame in signal_frames.items()
    }
    baseline_outputs = FxQuotePortfolioSimulator(config).run(training_slices)
    datasets = [
        build_labeled_dataset(frame, baseline_outputs["trades"], config, require_exit_before=train_end)
        for frame in training_slices.values()
    ]
    dataset = pd.concat([item for item in datasets if not item.empty], ignore_index=True) if datasets else pd.DataFrame()
    model = fit_fx_filter_model(dataset, config)
    paths = _save_model_and_dataset(model, dataset, config)
    return model, dataset, paths


def _apply_walk_forward_filter(
    signal_frames: dict[str, pd.DataFrame],
    config: AppConfig,
    *,
    backtest_start: pd.Timestamp,
    backtest_end: pd.Timestamp,
    history_start: pd.Timestamp,
) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]], dict[str, str]]:
    windows = _build_test_windows(backtest_start, backtest_end, config)
    filtered: dict[str, pd.DataFrame] = {
        symbol: _slice_window(frame, backtest_start, backtest_end)
        for symbol, frame in signal_frames.items()
    }
    walk_rows: list[dict[str, object]] = []
    latest_paths: dict[str, str] = {"model_path": "", "latest_model_path": "", "dataset_path": ""}
    for window_start, window_end in windows:
        train_start = _training_window_start(window_start, history_start, config)
        model, dataset, paths = _train_model_from_history(
            signal_frames,
            config,
            train_start=train_start,
            train_end=window_start,
        )
        latest_paths = paths
        accepted = 0
        candidates = 0
        for symbol, frame in signal_frames.items():
            test_slice = _slice_window(frame, window_start, window_end)
            if test_slice.empty:
                continue
            filtered_slice = apply_fx_ml_filter(test_slice, model, config, model_label=Path(paths["latest_model_path"] or paths["model_path"]).name)
            summary = ml_filter_summary(filtered_slice)
            accepted += int(summary["accepted_candidates"])
            candidates += int(summary["rule_candidates"])
            target_frame = filtered[symbol]
            overlap = target_frame.index.intersection(filtered_slice.index)
            target_frame.loc[overlap, filtered_slice.columns] = filtered_slice.loc[overlap, filtered_slice.columns]
        walk_rows.append(
            {
                "window": len(walk_rows) + 1,
                "start": str(window_start),
                "end": str(window_end),
                "train_start": str(train_start),
                "train_end": str(window_start),
                "training_rows": int(len(dataset.index)),
                "accepted_candidates": accepted,
                "rule_candidates": candidates,
                "coverage": accepted / candidates if candidates else 0.0,
            }
        )
    return filtered, walk_rows, latest_paths


def run_fx_backtest(
    config: AppConfig,
    env: EnvironmentConfig,
    *,
    backtest_start: str,
    backtest_end: str,
) -> BacktestResult:
    data_service = MarketDataService(config, env)
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    requested_start = pd.Timestamp(backtest_start, tz="Asia/Tokyo")
    requested_end = pd.Timestamp(backtest_end, tz="Asia/Tokyo")
    history_start = requested_start
    if ml_cfg.enabled and ml_cfg.backtest_mode in {"train_from_scratch", "walk_forward_train"}:
        history_start = shift_timestamp(
            requested_start,
            ml_cfg.walk_forward.train_window,
            backward=True,
        )
    signal_frames, chart_frames, _ = _build_symbol_signals(
        config,
        data_service,
        history_start=history_start.isoformat(),
        backtest_end=requested_end.isoformat(),
    )

    active_frames: dict[str, pd.DataFrame]
    model_paths = {"model_path": "", "latest_model_path": "", "dataset_path": ""}
    walk_forward_rows: list[dict[str, object]]
    if not ml_cfg.enabled or ml_cfg.backtest_mode == "rule_only":
        active_frames = {
            symbol: _slice_window(frame, requested_start, requested_end)
            for symbol, frame in signal_frames.items()
        }
        walk_forward_rows = []
    elif ml_cfg.backtest_mode == "load_pretrained":
        model = _load_model_for_backtest(config)
        active_frames = {
            symbol: apply_fx_ml_filter(
                _slice_window(frame, requested_start, requested_end),
                model,
                config,
                model_label=Path(_resolve_model_path(config)).name if model is not None else "",
            )
            for symbol, frame in signal_frames.items()
        }
        walk_forward_rows = []
    elif ml_cfg.backtest_mode == "train_from_scratch":
        model, dataset, model_paths = _train_model_from_history(
            signal_frames,
            config,
            train_start=history_start,
            train_end=requested_start,
        )
        active_frames = {
            symbol: apply_fx_ml_filter(
                _slice_window(frame, requested_start, requested_end),
                model,
                config,
                model_label=Path(model_paths["latest_model_path"] or model_paths["model_path"]).name,
            )
            for symbol, frame in signal_frames.items()
        }
        walk_forward_rows = [
            {
                "window": 1,
                "start": str(requested_start),
                "end": str(requested_end),
                "train_start": str(history_start),
                "train_end": str(requested_start),
                "training_rows": int(len(dataset.index)),
            }
        ]
    elif ml_cfg.backtest_mode == "walk_forward_train":
        active_frames, walk_forward_rows, model_paths = _apply_walk_forward_filter(
            signal_frames,
            config,
            backtest_start=requested_start,
            backtest_end=requested_end,
            history_start=history_start,
        )
    else:
        raise ValueError(f"未対応の FX ML backtest mode です: {ml_cfg.backtest_mode}")

    sim_outputs = FxQuotePortfolioSimulator(config).run(active_frames, mode=config.broker.mode)
    equity_curve = sim_outputs["equity_curve"]
    if not equity_curve.empty:
        equity_curve["drawdown"] = compute_drawdown(equity_curve["equity"])
    drawdown_curve = equity_curve[["drawdown"]].copy() if "drawdown" in equity_curve.columns else pd.DataFrame()
    benchmark_curve = _benchmark_curve(config, active_frames, equity_curve)
    signals_frame = _signal_log_frame(active_frames)
    metrics = compute_metrics(
        equity_curve=equity_curve,
        trades=sim_outputs["trades"],
        fills=sim_outputs["fills"],
        benchmark_curve=benchmark_curve,
    )
    metrics["ml_filter"] = {
        symbol: ml_filter_summary(frame)
        for symbol, frame in active_frames.items()
    }
    if model_paths["latest_model_path"] or model_paths["model_path"]:
        metrics["model_artifacts"] = {key: value for key, value in model_paths.items() if value}
    in_sample_equity, out_sample_equity = split_in_out_sample(equity_curve, config.validation.in_sample_ratio)
    in_sample_metrics = compute_metrics(in_sample_equity, sim_outputs["trades"], sim_outputs["fills"])
    out_of_sample_metrics = compute_metrics(out_sample_equity, sim_outputs["trades"], sim_outputs["fills"])
    walk_forward_summary = (
        walk_forward_rows
        if walk_forward_rows
        else rolling_walk_forward(
            equity_curve=equity_curve,
            trades=sim_outputs["trades"],
            fills=sim_outputs["fills"],
            windows=config.validation.rolling_windows,
        )
    )
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
    result = BacktestResult(
        run_id=run_id,
        strategy_name=FxBreakoutPullbackStrategy.name,
        mode=config.broker.mode,
        symbols=config.watchlist.symbols,
        backtest_start=backtest_start,
        backtest_end=backtest_end,
        starting_cash=config.risk.starting_cash,
        metrics=metrics,
        equity_curve=equity_curve,
        drawdown_curve=drawdown_curve,
        trades=sim_outputs["trades"],
        orders=sim_outputs["orders"],
        fills=sim_outputs["fills"],
        positions=sim_outputs["positions"],
        signals=signals_frame,
        benchmark_curve=benchmark_curve,
        in_sample_metrics=in_sample_metrics,
        out_of_sample_metrics=out_of_sample_metrics,
        walk_forward=walk_forward_summary,
        chart_frames=chart_frames,
    )
    result.output_dir = str(export_backtest_artifacts(result, config))
    return result


def train_fx_filter_model_run(
    config: AppConfig,
    env: EnvironmentConfig,
    *,
    as_of: str | None = None,
) -> dict[str, object]:
    data_service = MarketDataService(config, env)
    train_end = pd.Timestamp(as_of or config.data.end_date, tz="Asia/Tokyo")
    history_start = shift_timestamp(
        train_end,
        config.strategy.fx_breakout_pullback.ml_filter.walk_forward.train_window,
        backward=True,
    )
    signal_frames, _, _ = _build_symbol_signals(
        config,
        data_service,
        history_start=history_start.isoformat(),
        backtest_end=train_end.isoformat(),
    )
    model, dataset, paths = _train_model_from_history(
        signal_frames,
        config,
        train_start=history_start,
        train_end=train_end,
    )
    return {
        "trained_rows": int(len(dataset.index)),
        "train_start": history_start.isoformat(),
        "train_end": train_end.isoformat(),
        "positive_rate": float(dataset["binary_label"].mean()) if not dataset.empty else 0.0,
        "feature_names": list(model.feature_names),
        **paths,
    }
