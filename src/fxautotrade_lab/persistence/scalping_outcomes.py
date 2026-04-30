"""Cross-run storage for scalping signals, trades, and realized outcomes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


class ScalpingOutcomeStore:
    """Append run-scoped scalping outputs as Parquet partitions.

    Each call writes a separate run/source file under ``signals/``, ``trades/``,
    and ``outcomes/``.  This keeps appends simple and makes later research jobs
    able to read the store by concatenating partitions.
    """

    def __init__(self, root_dir: str | Path, *, storage_format: str = "parquet") -> None:
        self.root_dir = Path(root_dir)
        self.storage_format = storage_format if storage_format in {"parquet", "csv"} else "parquet"

    def append_backtest(
        self,
        *,
        run_id: str,
        model_id: str,
        symbol: str,
        signals: pd.DataFrame,
        trades: pd.DataFrame,
        model_path: str = "",
        model_promoted: bool = False,
        features: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        return self._append(
            source="backtest",
            run_id=run_id,
            model_id=model_id,
            model_path=model_path,
            model_promoted=model_promoted,
            symbol=symbol,
            signals=signals,
            trades=trades,
            features=features,
        )

    def append_paper(
        self,
        *,
        run_id: str,
        model_id: str,
        symbol: str,
        signals: pd.DataFrame,
        trades: pd.DataFrame,
        model_path: str = "",
        model_promoted: bool = False,
    ) -> dict[str, object]:
        return self._append(
            source="paper",
            run_id=run_id,
            model_id=model_id,
            model_path=model_path,
            model_promoted=model_promoted,
            symbol=symbol,
            signals=signals,
            trades=trades,
            features=None,
        )

    def load_signals(self) -> pd.DataFrame:
        return self._load_partition("signals")

    def load_trades(self) -> pd.DataFrame:
        return self._load_partition("trades")

    def load_outcomes(self) -> pd.DataFrame:
        return self._load_partition("outcomes")

    def load_summary(self) -> dict[str, object]:
        outcomes = self.load_outcomes()
        signals = self.load_signals()
        trades = self.load_trades()
        return {
            "signal_count": int(len(signals.index)),
            "trade_count": int(len(trades.index)),
            "outcome_count": int(len(outcomes.index)),
            "by_probability_decile": _probability_decile_summary(outcomes),
            "by_session": _session_summary(outcomes),
            "by_reject_reason": _column_summary(signals, "reject_reason"),
            "by_model_id": _column_summary(outcomes, "model_id"),
            "paper_vs_backtest": _paper_vs_backtest_summary(outcomes),
        }

    def _append(
        self,
        *,
        source: str,
        run_id: str,
        model_id: str,
        model_path: str,
        model_promoted: bool,
        symbol: str,
        signals: pd.DataFrame,
        trades: pd.DataFrame,
        features: pd.DataFrame | None,
    ) -> dict[str, object]:
        signal_frame = _prepare_signals(
            signals,
            source=source,
            run_id=run_id,
            model_id=model_id,
            model_path=model_path,
            model_promoted=model_promoted,
            symbol=symbol,
            features=features,
        )
        trade_frame = _prepare_trades(
            trades,
            source=source,
            run_id=run_id,
            model_id=model_id,
            symbol=symbol,
        )
        outcome_frame = _prepare_outcomes(signal_frame, trade_frame)
        signal_path = self._write_partition("signals", run_id, source, signal_frame)
        trade_path = self._write_partition("trades", run_id, source, trade_frame)
        outcome_path = self._write_partition("outcomes", run_id, source, outcome_frame)
        return {
            "signals": int(len(signal_frame.index)),
            "trades": int(len(trade_frame.index)),
            "outcomes": int(len(outcome_frame.index)),
            "signal_path": str(signal_path),
            "trade_path": str(trade_path),
            "outcome_path": str(outcome_path),
        }

    def _write_partition(
        self,
        table: str,
        run_id: str,
        source: str,
        frame: pd.DataFrame,
    ) -> Path:
        directory = self.root_dir / table
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{run_id}_{source}.parquet"
        if self.storage_format == "csv":
            csv_path = path.with_suffix(".csv")
            frame.to_csv(csv_path, index=False)
            return csv_path
        try:
            frame.to_parquet(path, index=False)
            return path
        except (ImportError, ValueError, OSError):
            csv_path = path.with_suffix(".csv")
            frame.to_csv(csv_path, index=False)
            return csv_path

    def _load_partition(self, table: str) -> pd.DataFrame:
        directory = self.root_dir / table
        if not directory.exists():
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        for path in sorted(directory.glob("*.parquet")):
            try:
                frames.append(pd.read_parquet(path))
            except (ImportError, ValueError, OSError):
                continue
        for path in sorted(directory.glob("*.csv")):
            try:
                frames.append(pd.read_csv(path))
            except OSError:
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, sort=False)


def _prepare_signals(
    signals: pd.DataFrame,
    *,
    source: str,
    run_id: str,
    model_id: str,
    model_path: str,
    model_promoted: bool,
    symbol: str,
    features: pd.DataFrame | None,
) -> pd.DataFrame:
    frame = signals.copy().reset_index(drop=True)
    if frame.empty:
        frame = pd.DataFrame(columns=["signal_id", "timestamp", "symbol"])
    frame.insert(0, "source", source)
    frame.insert(0, "model_promoted", bool(model_promoted))
    frame.insert(0, "model_path", model_path)
    frame.insert(0, "model_id", model_id)
    frame.insert(0, "run_id", run_id)
    if "symbol" not in frame.columns:
        frame["symbol"] = symbol
    if features is not None and not features.empty and "timestamp" in frame.columns:
        frame["features_json"] = _feature_json_for_signals(frame, features)
    elif "features_json" not in frame.columns:
        frame["features_json"] = ""
    return _stringify_complex_values(frame)


def _prepare_trades(
    trades: pd.DataFrame,
    *,
    source: str,
    run_id: str,
    model_id: str,
    symbol: str,
) -> pd.DataFrame:
    frame = trades.copy().reset_index(drop=True)
    if frame.empty:
        frame = pd.DataFrame(columns=["trade_id", "signal_id", "symbol"])
    frame.insert(0, "source", source)
    frame.insert(0, "model_id", model_id)
    frame.insert(0, "run_id", run_id)
    if "symbol" not in frame.columns:
        frame["symbol"] = symbol
    return _stringify_complex_values(frame)


def _prepare_outcomes(signals: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame(columns=["run_id", "model_id", "source", "signal_id"])
    signal_columns = [
        column
        for column in [
            "run_id",
            "model_id",
            "model_path",
            "model_promoted",
            "source",
            "symbol",
            "signal_id",
            "timestamp",
            "features_json",
            "probability",
            "long_probability",
            "short_probability",
            "chosen_side",
            "decision",
            "accepted",
            "reject_reason",
            "spread_pips",
            "spread_mean_20_pips",
            "spread_z_120",
            "volatility_pips",
            "future_long_net_pips",
            "future_short_net_pips",
        ]
        if column in signals.columns
    ]
    base = signals[signal_columns].copy()
    if trades.empty or "signal_id" not in trades.columns or "signal_id" not in base.columns:
        return base
    trade_columns = [
        column
        for column in [
            "signal_id",
            "trade_id",
            "position_id",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "quantity",
            "gross_pnl",
            "fee_amount",
            "net_pnl",
            "realized_net_pips",
            "realized_pips",
            "exit_reason",
        ]
        if column in trades.columns
    ]
    return base.merge(trades[trade_columns], on="signal_id", how="left")


def _probability_decile_summary(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty or "probability" not in frame.columns:
        return []
    working = frame.copy()
    working["probability"] = pd.to_numeric(working["probability"], errors="coerce")
    working["probability_decile"] = (
        (working["probability"].clip(0.0, 1.0) * 10).fillna(0).astype(int).clip(0, 9)
    )
    working["net_pnl"] = _numeric_column(working, "net_pnl")
    working["realized_net_pips"] = _numeric_column(working, "realized_net_pips")
    rows: list[dict[str, object]] = []
    for decile, group in working.groupby("probability_decile", dropna=False):
        rows.append(_summary_row(group, "probability_decile", int(decile)))
    return rows


def _session_summary(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty or "timestamp" not in frame.columns:
        return []
    timestamps = pd.to_datetime(frame["timestamp"], errors="coerce")
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize("Asia/Tokyo")
    else:
        timestamps = timestamps.dt.tz_convert("Asia/Tokyo")
    hours = timestamps.dt.hour
    working = frame.copy()
    working["session"] = "other"
    working.loc[(hours >= 9) & (hours < 15), "session"] = "tokyo"
    working.loc[(hours >= 16) | (hours < 1), "session"] = "london"
    working.loc[(hours >= 21) | (hours < 6), "session"] = "newyork"
    return [
        _summary_row(group, "session", str(session))
        for session, group in working.groupby("session")
    ]


def _column_summary(frame: pd.DataFrame, column: str) -> list[dict[str, object]]:
    if frame.empty or column not in frame.columns:
        return []
    return [
        _summary_row(group, column, str(key)) for key, group in frame.groupby(column, dropna=False)
    ]


def _paper_vs_backtest_summary(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty or "source" not in frame.columns:
        return []
    return _column_summary(frame, "source")


def _summary_row(frame: pd.DataFrame, key_name: str, key_value: object) -> dict[str, object]:
    accepted = (
        frame["accepted"].astype(bool)
        if "accepted" in frame.columns
        else pd.Series(False, index=frame.index)
    )
    net_pnl = _numeric_column(frame, "net_pnl")
    net_pips = _numeric_column(frame, "realized_net_pips")
    return {
        key_name: key_value,
        "signals": int(len(frame.index)),
        "accepted": int(accepted.sum()) if len(accepted.index) else 0,
        "trades": int(net_pnl.ne(0.0).sum()) if len(net_pnl.index) else 0,
        "net_pnl": float(net_pnl.sum()) if len(net_pnl.index) else 0.0,
        "average_net_pips": float(net_pips.mean()) if len(net_pips.index) else 0.0,
        "win_rate": float((net_pnl > 0.0).mean()) if len(net_pnl.index) else 0.0,
    }


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(0.0, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


def _feature_json_for_signals(signals: pd.DataFrame, features: pd.DataFrame) -> list[str]:
    feature_frame = features.copy()
    if not isinstance(feature_frame.index, pd.DatetimeIndex):
        feature_frame.index = pd.to_datetime(feature_frame.index, errors="coerce")
    feature_frame.index = _normalize_index(feature_frame.index)
    out: list[str] = []
    timestamps = pd.to_datetime(signals["timestamp"], errors="coerce")
    for timestamp in timestamps:
        if pd.isna(timestamp):
            out.append("")
            continue
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(feature_frame.index.tz)
        else:
            ts = ts.tz_convert(feature_frame.index.tz)
        if ts not in feature_frame.index:
            out.append("")
            continue
        row = feature_frame.loc[ts]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        payload = {str(key): _jsonable(value) for key, value in row.to_dict().items()}
        out.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return out


def _normalize_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if index.tz is None:
        return index.tz_localize("Asia/Tokyo")
    return index.tz_convert("Asia/Tokyo")


def _stringify_complex_values(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in output.columns:
        if output[column].map(lambda value: isinstance(value, dict | list | tuple)).any():
            output[column] = output[column].map(
                lambda value: (
                    json.dumps(_jsonable(value), ensure_ascii=False)
                    if isinstance(value, dict | list | tuple)
                    else value
                )
            )
    for column in output.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
        output[column] = output[column].astype(str)
    return output


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
