"""Cost-aware ML helpers for tick/second-bar scalping."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fxautotrade_lab.data.quote_bars import validate_quote_bar_frame
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
    train_metrics: dict[str, float | int] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def save(self, path: str | Path) -> Path:
        self.model.metadata = {
            **self.model.metadata,
            "decision_threshold": self.decision_threshold,
            "train_metrics": self.train_metrics,
            "scalping": True,
        }
        return self.model.save(path)


def load_scalping_model_bundle(
    path: str | Path, config: ScalpingTrainingConfig
) -> ScalpingModelBundle:
    model = NumpyLogisticRegression.load(path)
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
    """Label each bar by first TP/SL touch after spread and slippage costs."""

    bars = validate_quote_bar_frame(quote_bars)
    if bars.empty:
        return pd.DataFrame()
    horizon = max(1, int(config.max_hold_seconds))
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
    long_reason: list[str] = [""] * len(bars.index)
    short_reason: list[str] = [""] * len(bars.index)

    tp = float(config.take_profit_pips) * pip
    sl = float(config.stop_loss_pips) * pip
    for i in range(len(bars.index) - 1):
        last = min(len(bars.index) - 1, i + horizon)
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

        for j in range(i + 1, last + 1):
            long_stop_hit = bid_low[j] <= long_sl
            long_take_hit = bid_high[j] >= long_tp
            if long_stop_hit or long_take_hit:
                long_hold[i] = j - i
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
    labels["long_exit_reason"] = long_reason
    labels["short_exit_reason"] = short_reason
    return labels


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
    threshold, metrics = select_decision_threshold(
        model, training_features, training_meta, config=config
    )
    return ScalpingModelBundle(
        model=model,
        decision_threshold=threshold,
        training_config=config,
        train_metrics=metrics,
        metadata=dict(metadata or {}),
    )


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
