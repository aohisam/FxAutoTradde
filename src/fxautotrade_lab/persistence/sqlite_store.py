"""SQLite repository for backtest and automation history."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd
import yaml

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.enums import RunKind
from fxautotrade_lab.core.models import AutomationEvent, BacktestResult


def _json_default(value):
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def _sanitize_value(value):
    if isinstance(value, dict):
        return {key: _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


class SQLiteStore:
    """Persist runs, artifacts, and automation events."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    run_kind TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    output_dir TEXT,
                    symbols_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    in_sample_metrics_json TEXT,
                    out_of_sample_metrics_json TEXT,
                    walk_forward_json TEXT,
                    config_snapshot_yaml TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trades (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS fills (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS signals (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS automation_events (
                    run_id TEXT NOT NULL,
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message_ja TEXT NOT NULL,
                    metadata_json TEXT
                );
                """
            )

    def save_backtest_result(self, result: BacktestResult, config: AppConfig) -> None:
        config_snapshot_yaml = yaml.safe_dump(
            config.model_dump(mode="json"),
            allow_unicode=True,
            sort_keys=False,
        )
        with self.connection() as conn:
            conn.execute("DELETE FROM runs WHERE run_id = ?", (result.run_id,))
            for table in ("trades", "orders", "fills", "positions", "signals", "automation_events"):
                conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (result.run_id,))
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, run_kind, mode, strategy_name, started_at, finished_at, output_dir,
                    symbols_json, metrics_json, in_sample_metrics_json, out_of_sample_metrics_json,
                    walk_forward_json, config_snapshot_yaml
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.run_id,
                    RunKind.BACKTEST.value,
                    result.mode.value,
                    result.strategy_name,
                    str(result.equity_curve.index[0]) if not result.equity_curve.empty else "",
                    str(result.equity_curve.index[-1]) if not result.equity_curve.empty else "",
                    result.output_dir or "",
                    json.dumps(result.symbols, ensure_ascii=False),
                    json.dumps(result.metrics, ensure_ascii=False, default=_json_default),
                    json.dumps(result.in_sample_metrics, ensure_ascii=False, default=_json_default),
                    json.dumps(result.out_of_sample_metrics, ensure_ascii=False, default=_json_default),
                    json.dumps(result.walk_forward, ensure_ascii=False, default=_json_default),
                    config_snapshot_yaml,
                ),
            )
            self._insert_frame(conn, "trades", result.run_id, result.trades)
            self._insert_frame(conn, "orders", result.run_id, result.orders)
            self._insert_frame(conn, "fills", result.run_id, result.fills)
            self._insert_frame(conn, "positions", result.run_id, result.positions)
            self._insert_frame(conn, "signals", result.run_id, _compact_signals(result.signals))

    def save_automation_events(
        self,
        run_id: str,
        mode: str,
        strategy_name: str,
        symbols: list[str],
        events: list[AutomationEvent],
        config: AppConfig,
        output_dir: str = "",
    ) -> None:
        started_at = events[0].timestamp.isoformat() if events else ""
        finished_at = events[-1].timestamp.isoformat() if events else ""
        with self.connection() as conn:
            conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM automation_events WHERE run_id = ?", (run_id,))
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, run_kind, mode, strategy_name, started_at, finished_at, output_dir,
                    symbols_json, metrics_json, in_sample_metrics_json, out_of_sample_metrics_json,
                    walk_forward_json, config_snapshot_yaml
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    RunKind.AUTOMATION.value,
                    mode,
                    strategy_name,
                    started_at,
                    finished_at,
                    output_dir,
                    json.dumps(symbols, ensure_ascii=False),
                    json.dumps({"event_count": len(events)}, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                    json.dumps([], ensure_ascii=False),
                    yaml.safe_dump(config.model_dump(mode="json"), allow_unicode=True, sort_keys=False),
                ),
            )
            conn.executemany(
                """
                INSERT INTO automation_events (run_id, timestamp, level, message_ja, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        event.timestamp.isoformat(),
                        event.level,
                        event.message_ja,
                        json.dumps(event.metadata, ensure_ascii=False, default=_json_default),
                    )
                    for event in events
                ],
            )

    def list_runs(self, limit: int = 100) -> list[dict[str, object]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT run_id, run_kind, mode, strategy_name, started_at, finished_at, output_dir, metrics_json
                FROM runs
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "run_id": row["run_id"],
                "run_kind": row["run_kind"],
                "mode": row["mode"],
                "strategy_name": row["strategy_name"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "output_dir": row["output_dir"],
                "metrics": json.loads(row["metrics_json"]),
            }
            for row in rows
        ]

    def load_table(self, run_id: str, table: str) -> pd.DataFrame:
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT payload_json FROM {table} WHERE run_id = ? ORDER BY row_id ASC",  # noqa: S608
                (run_id,),
            ).fetchall()
        records = [json.loads(row["payload_json"]) for row in rows]
        return pd.DataFrame(records)

    def load_automation_events(self, run_id: str) -> pd.DataFrame:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, level, message_ja, metadata_json
                FROM automation_events
                WHERE run_id = ?
                ORDER BY row_id ASC
                """,
                (run_id,),
            ).fetchall()
        return pd.DataFrame(
            [
                {
                    "timestamp": row["timestamp"],
                    "level": row["level"],
                    "message_ja": row["message_ja"],
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
                for row in rows
            ]
        )

    def load_config_snapshot(self, run_id: str) -> str | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT config_snapshot_yaml FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return None if row is None else row["config_snapshot_yaml"]

    def _insert_frame(self, conn: sqlite3.Connection, table: str, run_id: str, frame: pd.DataFrame) -> None:
        if frame is None:
            return
        normalized = frame.reset_index(drop=True) if "timestamp" in frame.columns else frame.copy()
        records = [
            (run_id, json.dumps({key: _sanitize_value(value) for key, value in row.items()}, ensure_ascii=False))
            for row in normalized.to_dict(orient="records")
        ]
        if records:
            conn.executemany(
                f"INSERT INTO {table} (run_id, payload_json) VALUES (?, ?)",  # noqa: S608
                records,
            )


def _compact_signals(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "signal_action",
                "signal_score",
                "entry_signal",
                "exit_signal",
                "explanation_ja",
                "strategy_name",
                "reasons_ja",
            ]
        )
    keep_columns = [
        "timestamp",
        "symbol",
        "signal_action",
        "signal_score",
        "entry_signal",
        "entry_signal_rule_only",
        "exit_signal",
        "partial_exit_signal",
        "ml_probability",
        "ml_decision",
        "explanation_ja",
        "strategy_name",
        "reasons_ja",
        "sub_score_trend_regime",
        "sub_score_pullback_continuation",
        "sub_score_breakout_compression",
        "sub_score_candle_price_action",
        "sub_score_multi_timeframe_alignment",
        "sub_score_market_context",
    ]
    available = [column for column in keep_columns if column in frame.columns]
    return frame[available].copy()
