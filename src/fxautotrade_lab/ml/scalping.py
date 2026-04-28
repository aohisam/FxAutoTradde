"""Cost-aware ML helpers for tick/second-bar scalping."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame
from fxautotrade_lab.data.ticks import validate_tick_frame
from fxautotrade_lab.features.scalping import (
    SCALPING_FEATURE_COLUMNS,
    build_directional_feature_frame,
)
from fxautotrade_lab.ml.logistic import NumpyLogisticRegression


@dataclass(slots=True)
class ScalpingTrainingConfig:
    take_profit_pips: float = 1.6
    stop_loss_pips: float = 1.2
    max_hold_seconds: int = 90
    round_trip_slippage_pips: float = 0.15
    fee_pips: float = 0.0
    max_spread_pips: float = 0.6
    min_volatility_pips: float = 0.03
    min_samples: int = 200
    min_threshold_trades: int = 20
    decision_threshold: float = 0.58
    learning_rate: float = 0.08
    max_iter: int = 500
    l2_penalty: float = 0.002
    feature_clip: float = 8.0
    seed: int = 23
    min_validation_net_pips: float = 0.0
    min_validation_profit_factor: float = 1.0
    min_validation_trade_count: int = 1
    fail_closed_on_bad_validation: bool = True
    threshold_grid: tuple[float, ...] = (
        0.52,
        0.54,
        0.56,
        0.58,
        0.60,
        0.62,
        0.64,
        0.66,
        0.68,
        0.70,
        0.72,
    )


@dataclass(slots=True)
class ScalpingModelBundle:
    model: NumpyLogisticRegression
    decision_threshold: float
    training_config: ScalpingTrainingConfig
    train_metrics: dict[str, float | int | str | bool] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def save(self, path: str | Path) -> Path:
        self.model.metadata = {
            **self.model.metadata,
            **self.metadata,
            "decision_threshold": self.decision_threshold,
            "train_metrics": self.train_metrics,
            "scalping": True,
        }
        return self.model.save(path)


def load_scalping_model_bundle(
    path: str | Path, config: ScalpingTrainingConfig
) -> ScalpingModelBundle:
    model = NumpyLogisticRegression.load(path)
    if list(model.feature_names) != SCALPING_FEATURE_COLUMNS:
        missing = [
            column for column in SCALPING_FEATURE_COLUMNS if column not in model.feature_names
        ]
        extra = [column for column in model.feature_names if column not in SCALPING_FEATURE_COLUMNS]
        raise ValueError(
            "スキャルピングモデルの特徴量定義が現在のコードと一致しません。"
            " 旧モデルをそのまま新しい特徴量列で使うことはできません。"
            f" 不足: {missing} / 余分: {extra}"
        )
    threshold = float(model.metadata.get("decision_threshold", config.decision_threshold))
    train_metrics = model.metadata.get("train_metrics", {})
    return ScalpingModelBundle(
        model=model,
        decision_threshold=threshold,
        training_config=config,
        train_metrics=dict(train_metrics) if isinstance(train_metrics, dict) else {},
        metadata=dict(model.metadata),
    )


def build_triple_barrier_labels(
    quote_bars: pd.DataFrame,
    *,
    pip_size: float,
    config: ScalpingTrainingConfig,
) -> pd.DataFrame:
    """Label each bar by first TP/SL touch after costs using real elapsed time.

    If take-profit and stop-loss can both be touched inside the same bar, stop-loss is
    chosen first as a conservative bar-based assumption.
    """

    bars = validate_quote_bar_frame(quote_bars)
    if bars.empty:
        return pd.DataFrame()
    horizon_seconds = max(1, int(config.max_hold_seconds))
    horizon_delta = pd.Timedelta(seconds=horizon_seconds)
    pip = float(pip_size)
    entry_slip = (config.round_trip_slippage_pips / 2.0) * pip
    exit_slip = (config.round_trip_slippage_pips / 2.0) * pip
    fee_pips = float(config.fee_pips)

    ask_close = bars["ask_close"].to_numpy(dtype="float64")
    bid_close = bars["bid_close"].to_numpy(dtype="float64")
    bid_high = bars["bid_high"].to_numpy(dtype="float64")
    bid_low = bars["bid_low"].to_numpy(dtype="float64")
    ask_high = bars["ask_high"].to_numpy(dtype="float64")
    ask_low = bars["ask_low"].to_numpy(dtype="float64")
    ask_exit = bars["ask_close"].to_numpy(dtype="float64")
    bid_exit = bars["bid_close"].to_numpy(dtype="float64")

    long_net = np.full(len(bars.index), np.nan, dtype="float64")
    short_net = np.full(len(bars.index), np.nan, dtype="float64")
    long_hold = np.full(len(bars.index), 0, dtype="int64")
    short_hold = np.full(len(bars.index), 0, dtype="int64")
    long_hold_seconds = np.full(len(bars.index), 0.0, dtype="float64")
    short_hold_seconds = np.full(len(bars.index), 0.0, dtype="float64")
    long_reason: list[str] = [""] * len(bars.index)
    short_reason: list[str] = [""] * len(bars.index)

    tp = float(config.take_profit_pips) * pip
    sl = float(config.stop_loss_pips) * pip
    for i in range(len(bars.index) - 1):
        max_exit_time = pd.Timestamp(bars.index[i]) + horizon_delta
        last = int(bars.index.searchsorted(max_exit_time, side="right") - 1)
        last = min(len(bars.index) - 1, last)
        if last <= i:
            continue

        long_entry = ask_close[i] + entry_slip
        long_tp = long_entry + tp
        long_sl = long_entry - sl
        short_entry = bid_close[i] - entry_slip
        short_tp = short_entry - tp
        short_sl = short_entry + sl

        long_exit_price = bid_exit[last] - exit_slip
        short_exit_price = ask_exit[last] + exit_slip
        long_reason[i] = "time_exit"
        short_reason[i] = "time_exit"
        long_hold[i] = last - i
        short_hold[i] = last - i
        long_hold_seconds[i] = max(
            0.0, (pd.Timestamp(bars.index[last]) - pd.Timestamp(bars.index[i])).total_seconds()
        )
        short_hold_seconds[i] = long_hold_seconds[i]

        for j in range(i + 1, last + 1):
            long_stop_hit = bid_low[j] <= long_sl
            long_take_hit = bid_high[j] >= long_tp
            if long_stop_hit or long_take_hit:
                long_hold[i] = j - i
                long_hold_seconds[i] = max(
                    0.0,
                    (pd.Timestamp(bars.index[j]) - pd.Timestamp(bars.index[i])).total_seconds(),
                )
                if long_stop_hit:
                    long_exit_price = long_sl - exit_slip
                    long_reason[i] = "stop_loss"
                else:
                    long_exit_price = long_tp - exit_slip
                    long_reason[i] = "take_profit"
                break
        for j in range(i + 1, last + 1):
            short_stop_hit = ask_high[j] >= short_sl
            short_take_hit = ask_low[j] <= short_tp
            if short_stop_hit or short_take_hit:
                short_hold[i] = j - i
                short_hold_seconds[i] = max(
                    0.0,
                    (pd.Timestamp(bars.index[j]) - pd.Timestamp(bars.index[i])).total_seconds(),
                )
                if short_stop_hit:
                    short_exit_price = short_sl + exit_slip
                    short_reason[i] = "stop_loss"
                else:
                    short_exit_price = short_tp + exit_slip
                    short_reason[i] = "take_profit"
                break

        long_net[i] = ((long_exit_price - long_entry) / pip) - fee_pips
        short_net[i] = ((short_entry - short_exit_price) / pip) - fee_pips

    labels = pd.DataFrame(index=bars.index)
    labels["long_net_pips"] = long_net
    labels["short_net_pips"] = short_net
    labels["long_win"] = labels["long_net_pips"] > 0.0
    labels["short_win"] = labels["short_net_pips"] > 0.0
    labels["long_hold_bars"] = long_hold
    labels["short_hold_bars"] = short_hold
    labels["long_hold_seconds"] = long_hold_seconds
    labels["short_hold_seconds"] = short_hold_seconds
    labels["long_exit_reason"] = long_reason
    labels["short_exit_reason"] = short_reason
    return labels


def build_tick_triple_barrier_labels(
    ticks: pd.DataFrame,
    *,
    sample_index: pd.DatetimeIndex | None = None,
    pip_size: float,
    config: ScalpingTrainingConfig,
    entry_latency_ms: int = 0,
    symbol: str | None = None,
) -> pd.DataFrame:
    """Label samples with tick-level execution assumptions.

    The entry tick is the first quote observed at or after
    ``sample_timestamp + entry_latency_ms``. Long exits use Bid, short exits use
    Ask, and stop-loss is checked before take-profit on the same tick. Fees are
    treated as round-trip cost, matching the tick replay net-pips convention.
    """

    tick_frame = validate_tick_frame(ticks, symbol=symbol)
    if tick_frame.empty:
        index = _normalize_label_sample_index(sample_index, None)
        return pd.DataFrame(index=index)
    samples = _normalize_label_sample_index(sample_index, tick_frame.index)
    if samples.empty:
        return pd.DataFrame(index=samples)

    horizon_delta = pd.Timedelta(seconds=max(1, int(config.max_hold_seconds)))
    latency_delta = pd.Timedelta(milliseconds=max(0, int(entry_latency_ms)))
    pip = float(pip_size)
    slip = (float(config.round_trip_slippage_pips) / 2.0) * pip
    fee_pips = float(config.fee_pips)
    take_distance = float(config.take_profit_pips) * pip
    stop_distance = float(config.stop_loss_pips) * pip

    tick_index = tick_frame.index
    bid = tick_frame["bid"].to_numpy(dtype="float64")
    ask = tick_frame["ask"].to_numpy(dtype="float64")

    long_net = np.full(len(samples), np.nan, dtype="float64")
    short_net = np.full(len(samples), np.nan, dtype="float64")
    long_hold_ticks = np.zeros(len(samples), dtype="int64")
    short_hold_ticks = np.zeros(len(samples), dtype="int64")
    long_hold_seconds = np.zeros(len(samples), dtype="float64")
    short_hold_seconds = np.zeros(len(samples), dtype="float64")
    long_reason: list[str] = [""] * len(samples)
    short_reason: list[str] = [""] * len(samples)
    long_entry_times: list[object] = [pd.NaT] * len(samples)
    short_entry_times: list[object] = [pd.NaT] * len(samples)
    long_exit_times: list[object] = [pd.NaT] * len(samples)
    short_exit_times: list[object] = [pd.NaT] * len(samples)

    for sample_position, sample_time in enumerate(samples):
        entry_target = pd.Timestamp(sample_time) + latency_delta
        entry_position = int(tick_index.searchsorted(entry_target, side="left"))
        if entry_position >= len(tick_index) - 1:
            continue
        entry_time = pd.Timestamp(tick_index[entry_position])
        end_position = int(tick_index.searchsorted(entry_time + horizon_delta, side="right") - 1)
        end_position = max(entry_position + 1, min(end_position, len(tick_index) - 1))
        if end_position <= entry_position:
            continue

        long_entry = float(ask[entry_position]) + slip
        long_tp = long_entry + take_distance
        long_sl = long_entry - stop_distance
        short_entry = float(bid[entry_position]) - slip
        short_tp = short_entry - take_distance
        short_sl = short_entry + stop_distance

        long_exit_position = end_position
        short_exit_position = end_position
        long_exit_price = float(bid[end_position]) - slip
        short_exit_price = float(ask[end_position]) + slip
        long_reason[sample_position] = "time_exit"
        short_reason[sample_position] = "time_exit"

        for exit_position in range(entry_position + 1, end_position + 1):
            observed_bid = float(bid[exit_position])
            if observed_bid <= long_sl:
                long_exit_position = exit_position
                long_exit_price = long_sl - slip
                long_reason[sample_position] = "stop_loss"
                break
            if observed_bid >= long_tp:
                long_exit_position = exit_position
                long_exit_price = long_tp - slip
                long_reason[sample_position] = "take_profit"
                break

        for exit_position in range(entry_position + 1, end_position + 1):
            observed_ask = float(ask[exit_position])
            if observed_ask >= short_sl:
                short_exit_position = exit_position
                short_exit_price = short_sl + slip
                short_reason[sample_position] = "stop_loss"
                break
            if observed_ask <= short_tp:
                short_exit_position = exit_position
                short_exit_price = short_tp + slip
                short_reason[sample_position] = "take_profit"
                break

        long_exit_time = pd.Timestamp(tick_index[long_exit_position])
        short_exit_time = pd.Timestamp(tick_index[short_exit_position])
        long_net[sample_position] = ((long_exit_price - long_entry) / pip) - fee_pips
        short_net[sample_position] = ((short_entry - short_exit_price) / pip) - fee_pips
        long_hold_ticks[sample_position] = max(1, long_exit_position - entry_position)
        short_hold_ticks[sample_position] = max(1, short_exit_position - entry_position)
        long_hold_seconds[sample_position] = max(0.0, (long_exit_time - entry_time).total_seconds())
        short_hold_seconds[sample_position] = max(
            0.0, (short_exit_time - entry_time).total_seconds()
        )
        long_entry_times[sample_position] = entry_time
        short_entry_times[sample_position] = entry_time
        long_exit_times[sample_position] = long_exit_time
        short_exit_times[sample_position] = short_exit_time

    labels = pd.DataFrame(index=samples)
    labels["long_net_pips"] = long_net
    labels["short_net_pips"] = short_net
    labels["long_win"] = labels["long_net_pips"] > 0.0
    labels["short_win"] = labels["short_net_pips"] > 0.0
    labels["long_hold_bars"] = long_hold_ticks
    labels["short_hold_bars"] = short_hold_ticks
    labels["long_hold_ticks"] = long_hold_ticks
    labels["short_hold_ticks"] = short_hold_ticks
    labels["long_hold_seconds"] = long_hold_seconds
    labels["short_hold_seconds"] = short_hold_seconds
    labels["long_exit_reason"] = long_reason
    labels["short_exit_reason"] = short_reason
    labels["long_entry_time"] = long_entry_times
    labels["short_entry_time"] = short_entry_times
    labels["long_exit_time"] = long_exit_times
    labels["short_exit_time"] = short_exit_times
    return labels


def _normalize_label_sample_index(
    sample_index: pd.DatetimeIndex | None,
    fallback_index: pd.DatetimeIndex | None,
) -> pd.DatetimeIndex:
    source_index = sample_index if sample_index is not None else fallback_index
    if source_index is None:
        return pd.DatetimeIndex([], tz=ASIA_TOKYO)
    normalized = pd.DatetimeIndex(source_index)
    if normalized.tz is None:
        return normalized.tz_localize(ASIA_TOKYO)
    return normalized.tz_convert(ASIA_TOKYO)


def build_scalping_training_set(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    config: ScalpingTrainingConfig,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    label_parts: list[pd.Series] = []
    meta_parts: list[pd.DataFrame] = []
    for side in ("long", "short"):
        side_features = build_directional_feature_frame(features, side=side)
        net_column = f"{side}_net_pips"
        win_column = f"{side}_win"
        if net_column not in labels.columns or win_column not in labels.columns:
            continue
        aligned = side_features.join(labels[[net_column, win_column]], how="inner")
        aligned = aligned.loc[aligned[net_column].notna()].copy()
        aligned = aligned.loc[
            (aligned["spread_close_pips"] <= config.max_spread_pips)
            & (aligned["micro_volatility_10_pips"] >= config.min_volatility_pips)
        ].copy()
        if aligned.empty:
            continue
        frames.append(aligned[SCALPING_FEATURE_COLUMNS])
        label_parts.append(aligned[win_column].astype(bool))
        meta = pd.DataFrame(index=aligned.index)
        meta["side"] = side
        meta["net_pips"] = aligned[net_column]
        meta["spread_close_pips"] = aligned["spread_close_pips"]
        meta_parts.append(meta)
    if not frames:
        return (
            pd.DataFrame(columns=SCALPING_FEATURE_COLUMNS),
            pd.Series(dtype="bool"),
            pd.DataFrame(),
        )
    feature_frame = pd.concat(frames).sort_index(kind="stable")
    label_series = pd.concat(label_parts).sort_index(kind="stable")
    meta_frame = pd.concat(meta_parts).sort_index(kind="stable")
    return feature_frame, label_series, meta_frame


def fit_scalping_model(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    config: ScalpingTrainingConfig,
    validation_features: pd.DataFrame | None = None,
    validation_labels: pd.DataFrame | None = None,
    metadata: dict[str, Any] | None = None,
) -> ScalpingModelBundle:
    training_features, training_labels, training_meta = build_scalping_training_set(
        features, labels, config=config
    )
    if len(training_features.index) < config.min_samples:
        raise ValueError(
            "スキャルピングMLの学習サンプルが不足しています。"
            f" 必要 {config.min_samples:,} 件 / 実際 {len(training_features.index):,} 件"
        )
    model = NumpyLogisticRegression.fit(
        training_features,
        training_labels.astype(int),
        learning_rate=config.learning_rate,
        max_iter=config.max_iter,
        l2_penalty=config.l2_penalty,
        feature_clip=config.feature_clip,
        seed=config.seed,
        metadata={
            "feature_family": "fx_scalping_tick",
            "feature_names": SCALPING_FEATURE_COLUMNS,
            **dict(metadata or {}),
        },
    )
    threshold_source = "train"
    threshold_features = training_features
    threshold_meta = training_meta
    validation_sample_count = 0
    if validation_features is not None and validation_labels is not None:
        validation_training_features, _, validation_meta = build_scalping_training_set(
            validation_features, validation_labels, config=config
        )
        if validation_training_features.empty:
            raise ValueError(
                "スキャルピングMLのvalidationサンプルがありません。"
                " 閾値選択をvalidation期間で行うため、期間やフィルタ条件を見直してください。"
            )
        threshold_source = "validation"
        threshold_features = validation_training_features
        threshold_meta = validation_meta
        validation_sample_count = int(len(validation_training_features.index))
    threshold, selected_metrics = select_decision_threshold(
        model, threshold_features, threshold_meta, config=config
    )
    selected_threshold_before_gate = float(threshold)
    validation_gate_passed = True
    validation_gate_warning = ""
    if threshold_source == "validation":
        validation_gate_passed, validation_gate_warning = _evaluate_validation_gate(
            selected_metrics, config=config
        )
        if not validation_gate_passed and config.fail_closed_on_bad_validation:
            threshold = 1.01

    metrics: dict[str, float | int | str | bool] = {
        "train_sample_count": int(len(training_features.index)),
        "validation_sample_count": validation_sample_count,
        "threshold_selected_on": threshold_source,
        "selected_threshold": float(threshold),
        "selected_threshold_before_validation_gate": selected_threshold_before_gate,
        "validation_gate_passed": bool(validation_gate_passed),
        "validation_selected_count": (
            int(selected_metrics.get("selected_count", 0))
            if threshold_source == "validation"
            else 0
        ),
        "validation_net_pips": (
            float(selected_metrics.get("selected_net_pips", 0.0))
            if threshold_source == "validation"
            else 0.0
        ),
        "validation_mean_pips": (
            float(selected_metrics.get("selected_mean_pips", 0.0))
            if threshold_source == "validation"
            else 0.0
        ),
        "validation_profit_factor": (
            float(selected_metrics.get("selected_profit_factor", 0.0))
            if threshold_source == "validation"
            else 0.0
        ),
        "validation_max_drawdown": (
            float(selected_metrics.get("selected_max_drawdown_pips", 0.0))
            if threshold_source == "validation"
            else 0.0
        ),
        **selected_metrics,
    }
    if validation_gate_warning:
        metrics["warning_ja"] = validation_gate_warning
    elif threshold_source == "train":
        metrics["warning_ja"] = (
            "decision_threshold は学習データ上で選択されています。"
            "検証用にはvalidation期間を指定してください。"
        )
    bundle_metadata = {
        **dict(metadata or {}),
        "threshold_selected_on": threshold_source,
        "validation_gate_passed": bool(validation_gate_passed),
    }
    if validation_gate_warning:
        bundle_metadata["warning_ja"] = validation_gate_warning
    return ScalpingModelBundle(
        model=model,
        decision_threshold=threshold,
        training_config=config,
        train_metrics=metrics,
        metadata=bundle_metadata,
    )


def _evaluate_validation_gate(
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


def select_decision_threshold(
    model: NumpyLogisticRegression,
    features: pd.DataFrame,
    meta: pd.DataFrame,
    *,
    config: ScalpingTrainingConfig,
) -> tuple[float, dict[str, float | int]]:
    probabilities = model.predict_proba(features)
    best_threshold = float(config.decision_threshold)
    best_metrics: dict[str, float | int] = {
        "candidate_count": int(len(features.index)),
        "selected_count": 0,
        "selected_net_pips": 0.0,
        "selected_mean_pips": 0.0,
        "selected_profit_factor": 0.0,
        "selected_max_drawdown_pips": 0.0,
        "objective": float("-inf"),
    }
    for threshold in config.threshold_grid:
        selected = probabilities >= threshold
        count = int(selected.sum())
        if count < config.min_threshold_trades:
            continue
        selected_net = pd.to_numeric(
            meta.iloc[selected.to_numpy()]["net_pips"], errors="coerce"
        ).fillna(0.0)
        gross_profit = float(selected_net[selected_net > 0].sum())
        gross_loss = float(-selected_net[selected_net < 0].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.0
        equity = selected_net.cumsum()
        drawdown = equity - equity.cummax()
        max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
        total = float(selected_net.sum())
        mean = float(selected_net.mean()) if count else 0.0
        objective = total + mean * count * 0.25 + min(profit_factor, 5.0) * 2.0 + max_drawdown * 0.5
        if objective > float(best_metrics["objective"]):
            best_threshold = float(threshold)
            best_metrics = {
                "candidate_count": int(len(features.index)),
                "selected_count": count,
                "selected_net_pips": total,
                "selected_mean_pips": mean,
                "selected_profit_factor": float(profit_factor),
                "selected_max_drawdown_pips": max_drawdown,
                "objective": float(objective),
            }
    if best_metrics["selected_count"] == 0:
        best_metrics["objective"] = 0.0
    return best_threshold, best_metrics
