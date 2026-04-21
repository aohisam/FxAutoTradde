"""Feature extraction, labeling, and filtering helpers for FX ML participation filter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.ml.logistic import NumpyLogisticRegression


FEATURE_COLUMNS = [
    "ema_fast_slope_1h",
    "adx_1h",
    "atr_1h",
    "atr_15m",
    "trend_gap_ratio_1h",
    "donchian_width_15m",
    "breakout_strength_15m",
    "spread_to_atr",
    "spread_context_ratio",
    "pullback_depth_atr",
    "breakout_distance_from_day_high",
    "breakout_distance_from_day_low",
    "entry_trigger_distance_atr",
    "weekday",
    "hour",
]

STORAGE_COLUMNS = [
    "symbol",
    "signal_time",
    "realized_r_net",
    "binary_label",
    "continuous_target",
    "net_pnl",
    *FEATURE_COLUMNS,
]


def _as_float_series(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.replace([np.inf, -np.inf], np.nan)


def candidate_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = pd.DataFrame(index=frame.index)
    for column in FEATURE_COLUMNS:
        if column in frame.columns:
            prepared[column] = _as_float_series(frame[column])
        else:
            prepared[column] = 0.0
    return prepared.fillna(0.0)


def aggregate_trade_labels(trades: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    if trades is None or trades.empty:
        return pd.DataFrame(
            columns=[
                "signal_time",
                "symbol",
                "entry_time",
                "exit_time",
                "realized_r_net",
                "binary_label",
                "continuous_target",
                "net_pnl",
                "gross_pnl",
            ]
        )
    required = {"position_id", "signal_time", "symbol", "entry_time", "exit_time", "net_pnl", "gross_pnl"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"トレードラベル集計に必要な列が不足しています: {sorted(missing)}")
    grouped = (
        trades.groupby(["position_id", "signal_time", "symbol"], as_index=False)
        .agg(
            entry_time=("entry_time", "min"),
            exit_time=("exit_time", "max"),
            net_pnl=("net_pnl", "sum"),
            gross_pnl=("gross_pnl", "sum"),
            initial_risk_price=("initial_risk_price", "first"),
            initial_quantity=("initial_quantity", "first"),
        )
        .copy()
    )
    denominator = (grouped["initial_risk_price"] * grouped["initial_quantity"]).replace(0, np.nan)
    grouped["realized_r_net"] = (grouped["net_pnl"] / denominator).fillna(0.0)
    grouped["continuous_target"] = grouped["realized_r_net"].clip(
        lower=config.strategy.fx_breakout_pullback.ml_filter.label_clip_lower,
        upper=config.strategy.fx_breakout_pullback.ml_filter.label_clip_upper,
    )
    grouped["binary_label"] = (
        grouped["realized_r_net"] >= config.strategy.fx_breakout_pullback.positive_r_threshold
    ).astype("int64")
    return grouped


def build_labeled_dataset(
    signal_frame: pd.DataFrame,
    trades: pd.DataFrame,
    config: AppConfig,
    *,
    require_exit_before: pd.Timestamp | None = None,
) -> pd.DataFrame:
    candidates = signal_frame.loc[signal_frame.get("entry_signal_rule_only", signal_frame.get("entry_signal", False)).fillna(False)].copy()
    if candidates.empty:
        return pd.DataFrame()
    labels = aggregate_trade_labels(trades, config)
    if require_exit_before is not None and not labels.empty:
        labels = labels.loc[pd.to_datetime(labels["exit_time"]) < require_exit_before].copy()
    label_join = labels.set_index(["signal_time", "symbol"])
    candidates = candidates.copy()
    candidates["signal_time"] = pd.to_datetime(candidates.index)
    candidates["symbol"] = candidates["symbol"].astype(str).str.upper()
    joined = candidates.join(label_join, on=["signal_time", "symbol"], how="inner", rsuffix="_trade")
    if joined.empty:
        return pd.DataFrame()
    features = candidate_feature_frame(joined)
    dataset = pd.concat(
        [
            joined[["symbol", "signal_time", "entry_time", "exit_time", "realized_r_net", "binary_label", "continuous_target", "net_pnl"]],
            features,
        ],
        axis=1,
    )
    return dataset.reset_index(drop=True)


def fit_fx_filter_model(dataset: pd.DataFrame, config: AppConfig) -> NumpyLogisticRegression:
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    if dataset.empty:
        raise ValueError("学習用データセットが空です。")
    if len(dataset.index) < ml_cfg.min_samples:
        raise ValueError(f"学習サンプル数が不足しています。必要={ml_cfg.min_samples}, 実際={len(dataset.index)}")
    x = dataset.loc[:, FEATURE_COLUMNS]
    y = dataset["binary_label"]
    positive_rate = float(y.mean()) if len(y.index) else 0.0
    metadata = {
        "backend": ml_cfg.backend,
        "strategy_name": config.strategy.name,
        "feature_names": FEATURE_COLUMNS,
        "training_rows": int(len(dataset.index)),
        "positive_rate": positive_rate,
        "train_start": str(pd.Timestamp(dataset["signal_time"].min()).isoformat()),
        "train_end": str(pd.Timestamp(dataset["signal_time"].max()).isoformat()),
        "seed": ml_cfg.seed,
        "hyperparameters": {
            "learning_rate": ml_cfg.learning_rate,
            "max_iter": ml_cfg.max_iter,
            "l2_penalty": ml_cfg.l2_penalty,
            "feature_clip": ml_cfg.feature_clip,
            "decision_threshold": ml_cfg.decision_threshold,
            "min_samples": ml_cfg.min_samples,
        },
    }
    return NumpyLogisticRegression.fit(
        x,
        y,
        learning_rate=ml_cfg.learning_rate,
        max_iter=ml_cfg.max_iter,
        l2_penalty=ml_cfg.l2_penalty,
        feature_clip=ml_cfg.feature_clip,
        seed=ml_cfg.seed,
        metadata=metadata,
    )


def _storage_ready_dataset(dataset: pd.DataFrame) -> pd.DataFrame:
    available = [column for column in STORAGE_COLUMNS if column in dataset.columns]
    compact = dataset.loc[:, available].copy()
    if compact.empty:
        return compact
    if "symbol" in compact.columns:
        compact["symbol"] = compact["symbol"].astype("string").astype("category")
    if "signal_time" in compact.columns:
        compact["signal_time"] = pd.to_datetime(compact["signal_time"], errors="coerce", utc=True)
    float_columns = [
        "realized_r_net",
        "continuous_target",
        "net_pnl",
        *FEATURE_COLUMNS,
    ]
    for column in float_columns:
        if column in compact.columns:
            compact[column] = pd.to_numeric(compact[column], errors="coerce").astype("float32")
    if "binary_label" in compact.columns:
        compact["binary_label"] = pd.to_numeric(compact["binary_label"], errors="coerce").fillna(0).astype("int8")
    return compact


def save_labeled_dataset(dataset: pd.DataFrame, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    _storage_ready_dataset(dataset).to_parquet(target, index=False)
    return target


def load_filter_model(path: str | Path | None) -> NumpyLogisticRegression | None:
    if path is None:
        return None
    target = Path(path)
    if not target.exists():
        return None
    return NumpyLogisticRegression.load(target)


def latest_model_path(config: AppConfig) -> Path:
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    return ml_cfg.model_dir / ml_cfg.latest_model_alias


def apply_fx_ml_filter(
    signal_frame: pd.DataFrame,
    model: NumpyLogisticRegression | None,
    config: AppConfig,
    *,
    model_label: str = "",
) -> pd.DataFrame:
    working = signal_frame.copy()
    rule_only_entries = working.get("entry_signal", pd.Series(False, index=working.index)).fillna(False).astype(bool)
    working["entry_signal_rule_only"] = rule_only_entries
    working["ml_probability"] = np.nan
    working["ml_decision"] = True
    working["ml_model_label"] = model_label
    if model is None:
        working["entry_signal"] = rule_only_entries
        return working
    candidate_index = working.index[rule_only_entries]
    if len(candidate_index) == 0:
        working["entry_signal"] = rule_only_entries
        return working
    probabilities = model.predict_proba(candidate_feature_frame(working.loc[candidate_index]))
    threshold = config.strategy.fx_breakout_pullback.ml_filter.decision_threshold
    decisions = probabilities >= threshold
    working.loc[candidate_index, "ml_probability"] = probabilities
    working.loc[candidate_index, "ml_decision"] = decisions
    working["entry_signal"] = rule_only_entries & working["ml_decision"].fillna(True).astype(bool)
    working["signal_score"] = np.where(
        working["ml_probability"].notna(),
        np.maximum(_as_float_series(working["signal_score"]).fillna(0.0), working["ml_probability"].fillna(0.0)),
        _as_float_series(working["signal_score"]).fillna(0.0),
    )
    explanations: list[str] = []
    for timestamp, row in working.iterrows():
        explanation = str(row.get("explanation_ja", ""))
        if timestamp in candidate_index:
            probability = float(row.get("ml_probability") or 0.0)
            decision = bool(row.get("ml_decision", False))
            explanation = (
                f"{explanation} / ML確率={probability:.2f} / {'参加許可' if decision else '参加見送り'}"
                if explanation
                else f"ML確率={probability:.2f} / {'参加許可' if decision else '参加見送り'}"
            )
        explanations.append(explanation)
    working["explanation_ja"] = explanations
    return working


def ml_filter_summary(signal_frame: pd.DataFrame) -> dict[str, float | int]:
    if signal_frame.empty or "entry_signal_rule_only" not in signal_frame.columns:
        return {"rule_candidates": 0, "accepted_candidates": 0, "coverage": 0.0}
    rule_candidates = int(signal_frame["entry_signal_rule_only"].fillna(False).sum())
    accepted_candidates = int(signal_frame["entry_signal"].fillna(False).sum())
    coverage = accepted_candidates / rule_candidates if rule_candidates else 0.0
    avg_probability = (
        float(pd.to_numeric(signal_frame["ml_probability"], errors="coerce").dropna().mean())
        if "ml_probability" in signal_frame.columns
        else 0.0
    )
    return {
        "rule_candidates": rule_candidates,
        "accepted_candidates": accepted_candidates,
        "coverage": coverage,
        "average_ml_probability": avg_probability,
    }
