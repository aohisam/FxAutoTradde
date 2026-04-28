"""Small numpy-based logistic regression for the FX ML filter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -60.0, 60.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _logit(probability: float) -> float:
    bounded = min(max(probability, 1e-6), 1 - 1e-6)
    return float(np.log(bounded / (1 - bounded)))


@dataclass(slots=True)
class NumpyLogisticRegression:
    """Simple binary logistic regression with feature standardization."""

    feature_names: list[str]
    weights: np.ndarray
    bias: float
    mean: np.ndarray
    scale: np.ndarray
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def fit(
        cls,
        features: pd.DataFrame,
        labels: pd.Series,
        *,
        learning_rate: float,
        max_iter: int,
        l2_penalty: float,
        feature_clip: float,
        seed: int,
        metadata: dict[str, Any] | None = None,
    ) -> "NumpyLogisticRegression":
        if features.empty:
            raise ValueError("学習対象の特徴量が空です。")
        if len(features.index) != len(labels.index):
            raise ValueError("特徴量とラベルの件数が一致しません。")
        x = features.astype("float64").to_numpy(copy=True)
        y = labels.astype("float64").to_numpy(copy=True)
        mean = np.nanmean(x, axis=0)
        mean = np.where(np.isfinite(mean), mean, 0.0)
        nan_mask = np.isnan(x)
        if nan_mask.any():
            x[nan_mask] = np.take(mean, np.where(nan_mask)[1])
        scale = np.nanstd(x, axis=0)
        scale = np.where((scale > 1e-9) & np.isfinite(scale), scale, 1.0)
        x = np.clip((x - mean) / scale, -feature_clip, feature_clip)
        rng = np.random.default_rng(seed)
        weights = rng.normal(loc=0.0, scale=0.01, size=x.shape[1])
        bias = _logit(float(y.mean())) if len(y) else 0.0
        sample_count = max(len(y), 1)
        for _ in range(max_iter):
            logits = x @ weights + bias
            probs = _sigmoid(logits)
            error = probs - y
            gradient_w = (x.T @ error) / sample_count + (l2_penalty * weights)
            gradient_b = float(error.mean())
            weights -= learning_rate * gradient_w
            bias -= learning_rate * gradient_b
        return cls(
            feature_names=list(features.columns),
            weights=weights,
            bias=float(bias),
            mean=mean.astype("float64"),
            scale=scale.astype("float64"),
            metadata=dict(metadata or {}),
        )

    def predict_proba(self, features: pd.DataFrame) -> pd.Series:
        if list(features.columns) != self.feature_names:
            missing = [column for column in self.feature_names if column not in features.columns]
            extra = [column for column in features.columns if column not in self.feature_names]
            raise ValueError(
                "推論時の特徴量定義が学習時と一致しません。"
                f" 不足: {missing} / 余分: {extra}"
            )
        x = features.astype("float64").to_numpy(copy=True)
        nan_mask = np.isnan(x)
        if nan_mask.any():
            x[nan_mask] = np.take(self.mean, np.where(nan_mask)[1])
        normalized = np.clip((x - self.mean) / self.scale, -10.0, 10.0)
        probs = _sigmoid(normalized @ self.weights + self.bias)
        return pd.Series(probs, index=features.index, dtype="float64")

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "feature_names": self.feature_names,
            "weights": self.weights.tolist(),
            "bias": self.bias,
            "mean": self.mean.tolist(),
            "scale": self.scale.tolist(),
            "metadata": {
                **self.metadata,
                "saved_at": datetime.utcnow().isoformat() + "Z",
            },
        }
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return target

    @classmethod
    def load(cls, path: str | Path) -> "NumpyLogisticRegression":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            feature_names=list(payload["feature_names"]),
            weights=np.asarray(payload["weights"], dtype="float64"),
            bias=float(payload["bias"]),
            mean=np.asarray(payload["mean"], dtype="float64"),
            scale=np.asarray(payload["scale"], dtype="float64"),
            metadata=dict(payload.get("metadata", {})),
        )
