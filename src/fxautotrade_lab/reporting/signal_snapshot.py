"""Compact persisted signal snapshot helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _score_series(frame: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(frame.get("signal_score"), errors="coerce")


def enrich_signals_with_trade_context(signals: pd.DataFrame, trades: pd.DataFrame | None) -> pd.DataFrame:
    """Attach compact trade sizing fields to signal rows when a signal became a trade."""
    if signals is None or signals.empty or trades is None or trades.empty:
        return signals
    if "symbol" not in signals.columns or "symbol" not in trades.columns or "signal_time" not in trades.columns:
        return signals
    working = signals.copy()
    signal_time_source = working["timestamp"] if "timestamp" in working.columns else pd.Series(working.index, index=working.index)
    working["_signal_time_key"] = pd.to_datetime(signal_time_source, errors="coerce", utc=True)
    working["_symbol_key"] = working["symbol"].astype(str).str.upper()

    trade_working = trades.copy()
    trade_working["_signal_time_key"] = pd.to_datetime(trade_working["signal_time"], errors="coerce", utc=True)
    trade_working["_symbol_key"] = trade_working["symbol"].astype(str).str.upper()
    for column in ("quantity", "initial_quantity", "entry_price", "initial_risk_price", "net_pnl"):
        if column in trade_working.columns:
            trade_working[column] = pd.to_numeric(trade_working[column], errors="coerce")
    quantity = trade_working.get("initial_quantity", trade_working.get("quantity", pd.Series(0, index=trade_working.index)))
    trade_working["trade_quantity"] = quantity.fillna(trade_working.get("quantity", 0)).fillna(0)
    entry_price = trade_working.get("entry_price", pd.Series(0.0, index=trade_working.index)).fillna(0.0)
    initial_risk = trade_working.get("initial_risk_price", pd.Series(0.0, index=trade_working.index)).fillna(0.0)
    trade_working["trade_entry_notional_jpy"] = trade_working["trade_quantity"] * entry_price
    trade_working["trade_initial_risk_jpy"] = trade_working["trade_quantity"] * initial_risk
    trade_working["trade_net_pnl_jpy"] = trade_working.get("net_pnl", pd.Series(0.0, index=trade_working.index)).fillna(0.0)
    if "entry_order_side" in trade_working.columns:
        trade_working["trade_entry_side"] = trade_working["entry_order_side"].astype(str).str.lower()
    elif "position_side" in trade_working.columns:
        sides = trade_working["position_side"].astype(str).str.lower()
        trade_working["trade_entry_side"] = sides.map({"long": "buy", "short": "sell"}).fillna("")
    else:
        trade_working["trade_entry_side"] = ""

    aggregations: dict[str, tuple[str, str]] = {
        "trade_entry_side": ("trade_entry_side", "first"),
        "trade_quantity": ("trade_quantity", "first"),
        "trade_entry_notional_jpy": ("trade_entry_notional_jpy", "first"),
        "trade_initial_risk_jpy": ("trade_initial_risk_jpy", "first"),
        "trade_net_pnl_jpy": ("trade_net_pnl_jpy", "sum"),
    }
    if "entry_time" in trade_working.columns:
        aggregations["trade_entry_time"] = ("entry_time", "first")
    if "exit_time" in trade_working.columns:
        aggregations["trade_exit_time"] = ("exit_time", "last")
    if "entry_price" in trade_working.columns:
        aggregations["trade_entry_price"] = ("entry_price", "first")
    trade_context = (
        trade_working.dropna(subset=["_signal_time_key"])
        .groupby(["_signal_time_key", "_symbol_key"], as_index=False)
        .agg(**aggregations)
    )
    if trade_context.empty:
        return working.drop(columns=["_signal_time_key", "_symbol_key"])
    enriched = working.merge(trade_context, on=["_signal_time_key", "_symbol_key"], how="left")
    return enriched.drop(columns=["_signal_time_key", "_symbol_key"])


def build_signal_snapshot_payload(
    frame: pd.DataFrame | None,
    *,
    trades: pd.DataFrame | None = None,
    threshold: float = 0.55,
    recent_limit: int = 300,
    bins: int = 11,
    symbol_limit: int = 5,
) -> dict[str, object]:
    if frame is None or frame.empty:
        return {
            "recent_signals": pd.DataFrame(),
            "summary": {
                "total": 0,
                "accepted": 0,
                "buy_accepted": 0,
                "sell_accepted": 0,
                "mean_score": float("nan"),
            },
            "histogram": {"all": [0] * bins, "accepted": [0] * bins, "rejected": [0] * bins},
            "symbol_frame": pd.DataFrame(columns=["通貨ペア", "総数", "採用", "採用率", "平均スコア"]),
        }

    score_series = _score_series(frame)
    accepted = score_series.fillna(0.0) >= threshold
    actions = frame.get("signal_action", pd.Series([""] * len(frame), index=frame.index)).astype(str).str.lower()

    histogram = {"all": [0] * bins, "accepted": [0] * bins, "rejected": [0] * bins}
    for value, is_accepted in zip(score_series.tolist(), accepted.tolist()):
        if value != value:
            continue
        bounded = max(0.0, min(1.0, float(value)))
        bucket = min(bins - 1, int(bounded * bins))
        histogram["all"][bucket] += 1
        histogram["accepted" if is_accepted else "rejected"][bucket] += 1

    work = pd.DataFrame(
        {
            "symbol": frame.get("symbol", pd.Series([""] * len(frame), index=frame.index)).astype(str),
            "score": score_series,
            "accepted": accepted,
        }
    )
    grouped = (
        work.groupby("symbol")
        .agg(total=("symbol", "size"), accepted=("accepted", "sum"), mean_score=("score", "mean"))
        .reset_index()
        .sort_values(["accepted", "total", "symbol"], ascending=[False, False, True])
        .head(max(1, int(symbol_limit)))
    )
    grouped["採用率"] = (
        grouped["accepted"] / grouped["total"].clip(lower=1) * 100
    ).map(lambda value: f"{value:.1f}%")
    grouped["平均スコア"] = grouped["mean_score"].map(
        lambda value: "-" if pd.isna(value) else f"{value:.2f}"
    )
    symbol_frame = grouped.rename(
        columns={"symbol": "通貨ペア", "total": "総数", "accepted": "採用"}
    )[["通貨ペア", "総数", "採用", "採用率", "平均スコア"]].reset_index(drop=True)

    recent_signals = enrich_signals_with_trade_context(
        frame.tail(max(1, int(recent_limit))).reset_index(drop=True),
        trades,
    )
    return {
        "recent_signals": recent_signals,
        "summary": {
            "total": int(len(frame)),
            "accepted": int(accepted.sum()),
            "buy_accepted": int(((actions == "buy") & accepted).sum()),
            "sell_accepted": int(((actions == "sell") & accepted).sum()),
            "mean_score": float(score_series.mean()) if score_series.notna().any() else float("nan"),
        },
        "histogram": histogram,
        "symbol_frame": symbol_frame,
    }


def write_signal_snapshot_artifacts(output_dir: Path, snapshot: dict[str, object]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "summary": dict(snapshot.get("summary") or {}),
        "histogram": dict(snapshot.get("histogram") or {}),
    }
    (output_dir / "signal_snapshot.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    recent = snapshot.get("recent_signals")
    recent_frame = recent if isinstance(recent, pd.DataFrame) else pd.DataFrame()
    recent_frame.to_csv(output_dir / "signal_recent.csv", index=False)
    symbol = snapshot.get("symbol_frame")
    symbol_frame = symbol if isinstance(symbol, pd.DataFrame) else pd.DataFrame()
    symbol_frame.to_csv(output_dir / "signal_symbols.csv", index=False)


def load_signal_snapshot_artifacts(output_dir: Path) -> dict[str, object] | None:
    summary_path = output_dir / "signal_snapshot.json"
    recent_path = output_dir / "signal_recent.csv"
    symbol_path = output_dir / "signal_symbols.csv"
    if not summary_path.exists():
        return None
    try:
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    try:
        recent = pd.read_csv(recent_path) if recent_path.exists() else pd.DataFrame()
    except Exception:  # noqa: BLE001
        recent = pd.DataFrame()
    if not recent.empty and not {"trade_quantity", "trade_entry_notional_jpy"}.issubset(recent.columns):
        try:
            trades = pd.read_csv(output_dir / "trades.csv")
        except Exception:  # noqa: BLE001
            trades = pd.DataFrame()
        recent = enrich_signals_with_trade_context(recent, trades)
    try:
        symbol_frame = pd.read_csv(symbol_path) if symbol_path.exists() else pd.DataFrame()
    except Exception:  # noqa: BLE001
        symbol_frame = pd.DataFrame()
    return {
        "recent_signals": recent,
        "summary": dict(summary_payload.get("summary") or {}),
        "histogram": dict(summary_payload.get("histogram") or {}),
        "symbol_frame": symbol_frame,
    }
