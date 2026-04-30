"""Probability calibration summaries for scalping model operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class ScalpingCalibrationReport:
    deciles: pd.DataFrame = field(default_factory=pd.DataFrame)
    curve: pd.DataFrame = field(default_factory=pd.DataFrame)
    metrics: dict[str, object] = field(default_factory=dict)

    def to_summary(self) -> dict[str, object]:
        return {
            "metrics": dict(self.metrics),
            "deciles": self.deciles.to_dict(orient="records"),
            "curve": self.curve.to_dict(orient="records"),
        }


def build_probability_calibration_report(
    signals: pd.DataFrame,
    trades: pd.DataFrame,
) -> ScalpingCalibrationReport:
    """Build decile and Brier score diagnostics from replay outputs."""

    deciles = _trade_deciles(trades)
    curve, brier_score = _calibration_curve(signals)
    metrics: dict[str, object] = {
        "brier_score": brier_score,
        "calibration_sample_count": int(curve["signal_count"].sum()) if not curve.empty else 0,
        "trade_sample_count": int(deciles["trade_count"].sum()) if not deciles.empty else 0,
    }
    metrics.update(_decile_extremes(deciles))
    return ScalpingCalibrationReport(deciles=deciles, curve=curve, metrics=metrics)


def write_probability_calibration_report(
    report: ScalpingCalibrationReport,
    output_dir: str | Path,
) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    decile_path = target / "probability_deciles.csv"
    curve_path = target / "calibration_curve.csv"
    report.deciles.to_csv(decile_path, index=False)
    report.curve.to_csv(curve_path, index=False)
    summary_path = target / "calibration_summary.json"
    summary_path.write_text(
        pd.Series(report.metrics, dtype="object").to_json(force_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "probability_deciles": decile_path.name,
        "calibration_curve": curve_path.name,
        "calibration_summary": summary_path.name,
    }


def _decile_extremes(deciles: pd.DataFrame) -> dict[str, object]:
    if deciles.empty or "probability_decile" not in deciles.columns:
        return {
            "best_decile": None,
            "worst_decile": None,
            "high_probability_decile_win_rate": None,
            "high_probability_decile_average_net_pips": None,
        }
    working = deciles.copy()
    working["average_net_pips"] = pd.to_numeric(working.get("average_net_pips"), errors="coerce")
    working["win_rate"] = pd.to_numeric(working.get("win_rate"), errors="coerce")
    valid = working.dropna(subset=["average_net_pips"])
    if valid.empty:
        best_decile = None
        worst_decile = None
    else:
        best_decile = int(
            valid.sort_values("average_net_pips", ascending=False).iloc[0]["probability_decile"]
        )
        worst_decile = int(
            valid.sort_values("average_net_pips", ascending=True).iloc[0]["probability_decile"]
        )
    high_decile = working.sort_values("probability_decile", ascending=False).head(1)
    if high_decile.empty:
        high_win_rate = None
        high_average = None
    else:
        high_win_rate_value = high_decile.iloc[0].get("win_rate")
        high_average_value = high_decile.iloc[0].get("average_net_pips")
        high_win_rate = float(high_win_rate_value) if pd.notna(high_win_rate_value) else None
        high_average = float(high_average_value) if pd.notna(high_average_value) else None
    return {
        "best_decile": best_decile,
        "worst_decile": worst_decile,
        "high_probability_decile_win_rate": high_win_rate,
        "high_probability_decile_average_net_pips": high_average,
    }


def _trade_deciles(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "probability" not in trades.columns:
        return pd.DataFrame(
            columns=[
                "probability_decile",
                "trade_count",
                "win_rate",
                "average_net_pips",
                "total_net_pips",
                "profit_factor",
            ]
        )
    working = trades.copy()
    working["probability"] = pd.to_numeric(working["probability"], errors="coerce")
    pips_column = "realized_net_pips" if "realized_net_pips" in working.columns else "realized_pips"
    working["net_pips"] = pd.to_numeric(working.get(pips_column), errors="coerce")
    working = working.dropna(subset=["probability", "net_pips"])
    if working.empty:
        return _trade_deciles(pd.DataFrame())
    working["probability_decile"] = _decile_series(working["probability"])
    rows: list[dict[str, object]] = []
    for decile, group in working.groupby("probability_decile", dropna=False):
        net_pips = pd.to_numeric(group["net_pips"], errors="coerce").fillna(0.0)
        rows.append(
            {
                "probability_decile": int(decile),
                "trade_count": int(len(group.index)),
                "win_rate": float((net_pips > 0.0).mean()) if len(group.index) else 0.0,
                "average_net_pips": float(net_pips.mean()) if len(group.index) else 0.0,
                "total_net_pips": float(net_pips.sum()),
                "profit_factor": _profit_factor(net_pips),
            }
        )
    return pd.DataFrame(rows).sort_values("probability_decile").reset_index(drop=True)


def _calibration_curve(signals: pd.DataFrame) -> tuple[pd.DataFrame, float | None]:
    required = {"probability", "chosen_side", "future_long_net_pips", "future_short_net_pips"}
    if signals.empty or not required.issubset(signals.columns):
        return (
            pd.DataFrame(
                columns=[
                    "probability_decile",
                    "signal_count",
                    "mean_probability",
                    "observed_win_rate",
                ]
            ),
            None,
        )
    working = signals.copy()
    working["probability"] = pd.to_numeric(working["probability"], errors="coerce")
    long_pips = pd.to_numeric(working["future_long_net_pips"], errors="coerce")
    short_pips = pd.to_numeric(working["future_short_net_pips"], errors="coerce")
    working["future_net_pips"] = long_pips.where(working["chosen_side"] == "long", short_pips)
    working = working.dropna(subset=["probability", "future_net_pips"])
    if working.empty:
        empty, _ = _calibration_curve(pd.DataFrame())
        return empty, None
    working["actual_win"] = (working["future_net_pips"] > 0.0).astype(float)
    working["probability_decile"] = _decile_series(working["probability"])
    brier = float(((working["probability"] - working["actual_win"]) ** 2).mean())
    curve = (
        working.groupby("probability_decile", dropna=False)
        .agg(
            signal_count=("probability", "size"),
            mean_probability=("probability", "mean"),
            observed_win_rate=("actual_win", "mean"),
        )
        .reset_index()
    )
    curve["probability_decile"] = curve["probability_decile"].astype(int)
    return curve.sort_values("probability_decile").reset_index(drop=True), brier


def _decile_series(probabilities: pd.Series) -> pd.Series:
    bounded = pd.to_numeric(probabilities, errors="coerce").clip(lower=0.0, upper=1.0)
    deciles = (bounded * 10).fillna(0.0).astype("float64").astype(int)
    deciles = deciles.mask(deciles >= 10, 9)
    deciles = deciles.mask(deciles < 0, 0)
    return deciles.astype(int)


def _profit_factor(values: pd.Series) -> float:
    net = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gross_profit = float(net[net > 0.0].sum())
    gross_loss = float(-net[net < 0.0].sum())
    if gross_loss > 0.0:
        return gross_profit / gross_loss
    return 99.0 if gross_profit > 0.0 else 0.0


def _jsonable(value: Any) -> Any:
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
