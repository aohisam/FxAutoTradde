"""Shared orchestration for CLI and desktop."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import os
from pathlib import Path
import shutil
import sys
from time import monotonic, sleep

import pandas as pd
import yaml

from fxautotrade_lab.automation.controller import AutomationController
from fxautotrade_lab.backtest.fx_backtest import train_fx_filter_model_run
from fxautotrade_lab.backtest.runner import BacktestRunner
from fxautotrade_lab.backtest.scalping_backtest import execution_config_from_app, run_scalping_pipeline, training_config_from_app
from fxautotrade_lab.config.loader import load_app_config, load_environment, save_app_config
from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import BrokerMode, OrderSizingMode, RunKind, TimeFrame
from fxautotrade_lab.core.models import BacktestResult
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.data.cache import timeframe_coverage_delta
from fxautotrade_lab.data.gmo import GmoForexPublicClient
from fxautotrade_lab.data.gmo_tick_stream import GmoPublicWebSocketTickRecorder
from fxautotrade_lab.data.jforex import JForexCsvImporter
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.data.ticks import JForexTickCsvImporter, ParquetTickCache
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from fxautotrade_lab.features.scalping import pip_size_for_symbol
from fxautotrade_lab.ml.scalping import load_scalping_model_bundle
from fxautotrade_lab.persistence.sqlite_store import SQLiteStore
from fxautotrade_lab.research.pipeline import ResearchPipeline
from fxautotrade_lab.reporting.signal_snapshot import (
    load_signal_snapshot_artifacts,
    write_signal_snapshot_artifacts,
)
from fxautotrade_lab.security.keychain import (
    delete_private_gmo_credentials,
    resolve_private_gmo_credentials,
    save_private_gmo_credentials,
)
from fxautotrade_lab.simulation.scalping_realtime import ScalpingRealtimePaperEngine
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from fxautotrade_lab.strategies.registry import create_strategy


_TIMEFRAME_ALIASES: dict[str, TimeFrame] = {
    "1m": TimeFrame.MIN_1,
    "1min": TimeFrame.MIN_1,
    "5m": TimeFrame.MIN_5,
    "5min": TimeFrame.MIN_5,
    "10m": TimeFrame.MIN_10,
    "10min": TimeFrame.MIN_10,
    "15m": TimeFrame.MIN_15,
    "15min": TimeFrame.MIN_15,
    "30m": TimeFrame.MIN_30,
    "30min": TimeFrame.MIN_30,
    "1h": TimeFrame.HOUR_1,
    "1hour": TimeFrame.HOUR_1,
    "4h": TimeFrame.HOUR_4,
    "4hour": TimeFrame.HOUR_4,
    "8h": TimeFrame.HOUR_8,
    "8hour": TimeFrame.HOUR_8,
    "12h": TimeFrame.HOUR_12,
    "12hour": TimeFrame.HOUR_12,
    "1d": TimeFrame.DAY_1,
    "1day": TimeFrame.DAY_1,
    "1w": TimeFrame.WEEK_1,
    "1week": TimeFrame.WEEK_1,
    "1month": TimeFrame.MONTH_1,
    "1mo": TimeFrame.MONTH_1,
}

_ML_MODE_LABELS: dict[str, str] = {
    "rule_only": "ルールのみ",
    "load_pretrained": "学習済みモデルを使う",
    "train_from_scratch": "その場で再学習して使う",
    "walk_forward_train": "期間をずらしながら逐次学習する",
}

_STRATEGY_LABELS: dict[str, str] = {
    "fx_breakout_pullback": "FX ブレイクアウト押し目",
    "baseline_trend_pullback": "ベースライン順張り押し目",
    "multi_timeframe_pattern_scoring": "マルチタイムフレーム総合スコア",
}

_REPORT_TABLE_FILES: dict[str, str] = {
    "trades": "trades.csv",
    "orders": "orders.csv",
    "fills": "fills.csv",
    "positions": "positions.csv",
    "signals": "signal_log.csv",
}


def _safe_yaml_mapping(payload: str | None) -> dict[str, object]:
    if not payload:
        return {}
    try:
        parsed = yaml.safe_load(payload) or {}
    except Exception:  # noqa: BLE001
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _mapping_get(mapping: dict[str, object], *keys: str) -> object:
    current: object = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _load_report_snapshot(output_dir: Path | None) -> dict[str, object]:
    if output_dir is None:
        return {}
    snapshot_path = output_dir / "config_snapshot.yaml"
    if not snapshot_path.exists():
        return {}
    try:
        return _safe_yaml_mapping(snapshot_path.read_text(encoding="utf-8"))
    except OSError:
        return {}


def _saved_run_period(snapshot: dict[str, object], row: dict[str, object]) -> tuple[str, str]:
    backtest_cfg = _mapping_get(snapshot, "backtest")
    data_cfg = _mapping_get(snapshot, "data")
    backtest = backtest_cfg if isinstance(backtest_cfg, dict) else {}
    data = data_cfg if isinstance(data_cfg, dict) else {}
    use_custom_window = bool(backtest.get("use_custom_window"))
    start = str((backtest if use_custom_window else data).get("start_date") or "") if isinstance(backtest if use_custom_window else data, dict) else ""
    end = str((backtest if use_custom_window else data).get("end_date") or "") if isinstance(backtest if use_custom_window else data, dict) else ""
    if not start:
        start = str(row.get("started_at", ""))[:10]
    if not end:
        end = str(row.get("finished_at", ""))[:10]
    return start, end


def _saved_run_starting_cash(snapshot: dict[str, object]) -> float:
    raw = _mapping_get(snapshot, "risk", "starting_cash")
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _saved_run_symbols(snapshot: dict[str, object], row: dict[str, object]) -> list[str]:
    raw = row.get("symbols")
    if isinstance(raw, list) and raw:
        return [str(symbol) for symbol in raw]
    watchlist_cfg = _mapping_get(snapshot, "watchlist")
    if isinstance(watchlist_cfg, dict):
        symbols = watchlist_cfg.get("symbols")
        if isinstance(symbols, list) and symbols:
            return [str(symbol) for symbol in symbols]
    return []


def _saved_run_ml_details(snapshot: dict[str, object]) -> dict[str, object]:
    ml_cfg = _mapping_get(snapshot, "strategy", "fx_breakout_pullback", "ml_filter")
    ml = ml_cfg if isinstance(ml_cfg, dict) else {}
    enabled = bool(ml.get("enabled", False))
    mode = str(ml.get("backtest_mode") or "rule_only")
    pretrained_model_path = str(ml.get("pretrained_model_path") or "").strip()
    latest_alias = str(ml.get("latest_model_alias") or "latest_model.json")
    if not enabled or mode == "rule_only":
        label = "ML未使用"
    elif mode == "load_pretrained":
        model_name = Path(pretrained_model_path).name if pretrained_model_path else latest_alias
        label = f"学習済みモデル: {model_name}"
    elif mode == "train_from_scratch":
        label = "今回その場で再学習したモデル"
    elif mode == "walk_forward_train":
        label = "各期間でその都度学習したモデル"
    else:
        label = f"MLモード: {mode}"
    return {
        "enabled": enabled,
        "mode": mode,
        "mode_label": _ML_MODE_LABELS.get(mode, mode or "-"),
        "model_path": pretrained_model_path,
        "model_label": label,
    }


def _saved_run_label(row: dict[str, object]) -> str:
    finished_at = str(row.get("finished_at") or "")
    try:
        finished_label = pd.Timestamp(finished_at).strftime("%Y-%m-%d %H:%M")
    except Exception:  # noqa: BLE001
        finished_label = finished_at or "-"
    strategy_name = str(row.get("strategy_name") or "")
    strategy_label = _STRATEGY_LABELS.get(strategy_name, strategy_name or "-")
    start = str(row.get("start_date") or "")
    end = str(row.get("end_date") or "")
    if start and end:
        return f"{finished_label} / {strategy_label} / {start} → {end}"
    return f"{finished_label} / {strategy_label}"


def _load_report_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()
    if not frame.empty:
        first_column = str(frame.columns[0])
        if first_column.startswith("Unnamed:"):
            index_series = pd.to_datetime(frame.iloc[:, 0], errors="coerce")
            if index_series.notna().any():
                frame = frame.iloc[:, 1:].copy()
                frame.index = index_series
    return frame


def _emit_progress(
    progress_callback,
    *,
    task: str,
    current: int,
    total: int,
    message: str,
    phase: str = "running",
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        {
            "task": task,
            "phase": phase,
            "current": current,
            "total": total,
            "message": message,
        }
    )


def _resolve_timeframe(value: TimeFrame | str) -> TimeFrame:
    if isinstance(value, TimeFrame):
        return value
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("時間足が指定されていません。")
    try:
        return TimeFrame(normalized)
    except ValueError:
        alias = _TIMEFRAME_ALIASES.get(normalized.lower())
        if alias is not None:
            return alias
        raise ValueError(f"未対応の時間足です: {normalized}") from None


def _saved_fx_model_entries(config: AppConfig) -> list[dict[str, object]]:
    ml_cfg = config.strategy.fx_breakout_pullback.ml_filter
    model_dir = ml_cfg.model_dir
    latest_path = model_dir / ml_cfg.latest_model_alias
    entries: list[dict[str, object]] = [
        {
            "key": "__LATEST__",
            "label": "最新モデル (latest_model.json)",
            "path": str(latest_path),
            "exists": latest_path.exists(),
            "is_latest": True,
        }
    ]
    if not model_dir.exists():
        return entries
    for path in sorted(model_dir.glob("fx_filter_*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        modified_at = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        entries.append(
            {
                "key": str(path),
                "label": f"{modified_at} / {path.name}",
                "path": str(path),
                "exists": True,
                "is_latest": False,
            }
        )
    return entries


@dataclass(slots=True)
class LabApplication:
    config_path: Path | None = None
    overrides: dict | None = None
    config: AppConfig = field(init=False)
    env: object = field(init=False)
    store: SQLiteStore = field(init=False)
    last_result: BacktestResult | None = field(default=None, init=False)
    last_research_result: dict[str, object] | None = field(default=None, init=False)
    automation_controller: AutomationController | None = field(default=None, init=False)
    connection_test_results: dict[str, dict[str, object]] = field(default_factory=dict, init=False)
    _runtime_status_cache: dict[str, object] | None = field(default=None, init=False, repr=False)
    _runtime_status_cached_at: float = field(default=0.0, init=False, repr=False)
    _chart_dataset_cache: dict[tuple[str, str, bool], dict[str, object]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _chart_dataset_signatures: dict[tuple[str, str, bool], tuple[object, ...]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _runs_cache: list[dict[str, object]] | None = field(default=None, init=False, repr=False)
    _backtest_runs_cache: list[dict[str, object]] | None = field(default=None, init=False, repr=False)
    _saved_signals_cache: dict[str, pd.DataFrame] = field(default_factory=dict, init=False, repr=False)
    _saved_signal_snapshot_cache: dict[str, dict[str, object]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self.config_path = self._prepare_writable_config_path(self.config_path)
        self.reload_config()

    def reload_config(self) -> None:
        self.config = load_app_config(self.config_path, self.overrides)
        self.env = load_environment()
        self.store = SQLiteStore(self.config.persistence.sqlite_path)
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()
        self._invalidate_run_listing_cache()
        self._saved_signals_cache.clear()
        self._saved_signal_snapshot_cache.clear()
        self.last_result = None

    def save_config(self) -> None:
        if self.config_path is not None:
            save_app_config(self.config, self.config_path)

    def sync_data(self) -> dict[str, object]:
        return MarketDataService(self.config, self.env).sync()

    def sync_market_data(
        self,
        *,
        source: str,
        start_date: str,
        end_date: str,
        timeframes: list[TimeFrame],
        symbols: list[str] | None = None,
        progress_callback=None,
    ) -> dict[str, object]:
        sync_source = str(source).strip().lower()
        if sync_source not in {"gmo", "fixture"}:
            raise ValueError("同期ソースは GMO または fixture を指定してください。")
        sync_config = self.config.model_copy(deep=True)
        sync_config.data.source = sync_source
        sync_config.data.start_date = start_date
        sync_config.data.end_date = end_date
        sync_config.data.timeframes = list(timeframes)
        return MarketDataService(sync_config, self.env).sync(
            symbols=symbols,
            progress_callback=progress_callback,
        )

    def run_backtest(self, *, progress_callback=None) -> BacktestResult:
        self.last_result = BacktestRunner(self.config, self.env).run(progress_callback=progress_callback)
        if self.config.persistence.enabled:
            self.store.save_backtest_result(self.last_result, self.config)
            self._invalidate_run_listing_cache()
            self._saved_signals_cache.pop(self.last_result.run_id, None)
            self._saved_signal_snapshot_cache.pop(self.last_result.run_id, None)
        self._compact_backtest_result_for_ui()
        _emit_progress(
            progress_callback,
            task="backtest",
            current=5,
            total=5,
            phase="done",
            message="バックテストが完了しました。",
        )
        return self.last_result

    def train_fx_model(self, *, as_of: str | None = None, progress_callback=None) -> dict[str, object]:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            raise RuntimeError("FX breakout 戦略以外では FX ML 学習は利用できません。")
        summary = train_fx_filter_model_run(self.config, self.env, as_of=as_of, progress_callback=progress_callback)
        _emit_progress(
            progress_callback,
            task="train",
            current=4,
            total=4,
            phase="done",
            message="ML モデル学習が完了しました。",
        )
        return summary

    def model_status(self) -> dict[str, object]:
        ml_cfg = self.config.strategy.fx_breakout_pullback.ml_filter
        selected_key = "__LATEST__" if ml_cfg.pretrained_model_path is None else str(ml_cfg.pretrained_model_path)
        available_models = _saved_fx_model_entries(self.config)
        selected_entry = next((entry for entry in available_models if entry["key"] == selected_key), None)
        model_path = Path(str(selected_entry["path"])) if selected_entry is not None else (
            ml_cfg.pretrained_model_path or (ml_cfg.model_dir / ml_cfg.latest_model_alias)
        )
        return {
            "enabled": ml_cfg.enabled,
            "backtest_mode": ml_cfg.backtest_mode,
            "model_path": str(model_path),
            "exists": model_path.exists(),
            "selected_model_key": selected_key,
            "selected_model_label": str(selected_entry["label"]) if selected_entry is not None else str(model_path.name),
            "available_models": available_models,
        }

    def run_research(self, *, mode: str | None = None, progress_callback=None) -> dict[str, object]:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            raise RuntimeError("FX breakout 戦略以外では research_run は未対応です。")
        self.last_research_result = ResearchPipeline(self.config, self.env, mode=mode).run(
            progress_callback=progress_callback
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=7,
            total=7,
            phase="done",
            message="研究パイプラインが完了しました。",
        )
        return self.last_research_result

    def run_demo(self) -> dict[str, object]:
        demo_overrides = {
            "broker": {"mode": BrokerMode.LOCAL_SIM.value},
            "data": {"source": "fixture"},
            "automation": {"enabled": True},
        }
        self.overrides = demo_overrides if self.overrides is None else {**self.overrides, **demo_overrides}
        self.reload_config()
        result = self.run_backtest()
        automation = AutomationController(self.config, self.env)
        logs = automation.run(max_cycles=min(8, self.config.automation.max_cycles_for_demo))
        self.automation_controller = automation
        if self.config.persistence.enabled:
            self.store.save_automation_events(
                run_id=automation.run_id,
                mode=self.config.broker.mode.value,
                strategy_name=self.config.strategy.name,
                symbols=self.config.watchlist.symbols,
                events=logs,
                config=self.config,
                output_dir=result.output_dir or "",
            )
            self._invalidate_run_listing_cache()
        return {"result": result, "logs": logs}

    def run_realtime_sim(self, max_cycles: int | None = None) -> list:
        self.automation_controller = AutomationController(self.config, self.env)
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()
        events = self.automation_controller.run(max_cycles=max_cycles)
        if self.config.persistence.enabled:
            self.store.save_automation_events(
                run_id=self.automation_controller.run_id,
                mode=self.config.broker.mode.value,
                strategy_name=self.config.strategy.name,
                symbols=self.config.watchlist.symbols,
                events=events,
                config=self.config,
            )
            self._invalidate_run_listing_cache()
        return events

    def start_automation(self) -> AutomationController:
        self.automation_controller = AutomationController(self.config, self.env)
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()
        return self.automation_controller

    def stop_automation(self) -> None:
        if self.automation_controller is not None:
            self.automation_controller.stop()
            self._invalidate_runtime_cache()
            self._invalidate_chart_cache()

    def shutdown(self) -> None:
        if self.automation_controller is not None:
            self.automation_controller.stop()
            self.automation_controller.broker.shutdown()
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()

    def verify_broker_runtime(self) -> dict[str, object]:
        controller = AutomationController(self.config, self.env)
        try:
            return controller.broker.sync_runtime_state(order_limit=self.config.automation.reconcile_orders_limit)
        finally:
            controller.broker.shutdown()

    def persist_automation_events(self, events: list) -> None:
        if self.config.persistence.enabled and self.automation_controller is not None:
            self.store.save_automation_events(
                run_id=self.automation_controller.run_id,
                mode=self.config.broker.mode.value,
                strategy_name=self.config.strategy.name,
                symbols=self.config.watchlist.symbols,
                events=events,
                config=self.config,
                output_dir=self.last_result.output_dir if self.last_result else "",
            )
            self._invalidate_run_listing_cache()

    def update_watchlist(
        self,
        symbols: list[str],
        benchmarks: list[str] | None = None,
        sectors: list[str] | None = None,
    ) -> None:
        self.config.watchlist.symbols = [normalize_fx_symbol(symbol) for symbol in symbols]
        if benchmarks is not None:
            self.config.watchlist.benchmark_symbols = [normalize_fx_symbol(symbol) for symbol in benchmarks]
        if sectors is not None:
            self.config.watchlist.sector_symbols = [normalize_fx_symbol(symbol) for symbol in sectors]
        self.save_config()

    def update_runtime_mode(
        self,
        *,
        broker_mode: str,
        data_source: str,
        stream_enabled: bool,
    ) -> None:
        normalized_mode = broker_mode.strip().lower()
        normalized_source = data_source.strip().lower()
        if normalized_mode not in {BrokerMode.LOCAL_SIM.value, BrokerMode.GMO_SIM.value}:
            raise ValueError(f"Unsupported broker mode: {broker_mode}")
        if normalized_source not in {"fixture", "csv", "gmo"}:
            raise ValueError(f"Unsupported data source: {data_source}")
        if self.automation_controller is not None:
            self.automation_controller.stop()
        selected_mode = BrokerMode(normalized_mode)
        effective_source = "gmo" if selected_mode == BrokerMode.GMO_SIM else normalized_source
        self.config.broker.mode = selected_mode
        self.config.data.source = effective_source
        self.config.data.stream_enabled = bool(stream_enabled and effective_source == "gmo")
        self.save_config()
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()

    def update_order_sizing(
        self,
        *,
        order_size_mode: str,
        fixed_order_amount: float,
        equity_fraction_per_trade: float,
        risk_per_trade: float,
    ) -> None:
        self.config.risk.order_size_mode = OrderSizingMode(order_size_mode.strip().lower())
        self.config.risk.fixed_order_amount = max(0.0, float(fixed_order_amount))
        self.config.risk.equity_fraction_per_trade = max(0.0, float(equity_fraction_per_trade))
        self.config.risk.risk_per_trade = max(0.0, float(risk_per_trade))
        self.save_config()
        self._invalidate_chart_cache()

    def update_account_settings(self, *, starting_cash: float) -> None:
        normalized_cash = float(starting_cash)
        if normalized_cash <= 0:
            raise ValueError("初期資産は 0 より大きい値を指定してください。")
        self.config.risk.starting_cash = normalized_cash
        self.save_config()
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()

    def runtime_status_snapshot(
        self,
        *,
        force_refresh: bool = False,
        max_age_seconds: float = 12.0,
    ) -> dict[str, object]:
        if self.automation_controller is not None:
            snapshot = self.automation_controller.snapshot()
            self._runtime_status_cache = snapshot
            self._runtime_status_cached_at = monotonic()
            return snapshot
        cache_is_fresh = (
            not force_refresh
            and self._runtime_status_cache is not None
            and (monotonic() - self._runtime_status_cached_at) <= max_age_seconds
        )
        if cache_is_fresh:
            return self._runtime_status_cache
        snapshot = {
            "run_id": "",
            "status": "stopped",
            "mode": self.config.broker.mode.value,
            "cycle_count": 0,
            "heartbeat": "",
            "open_symbols": [],
            "positions": [],
            "recent_orders": [],
            "recent_fills": [],
            "recent_signals": [],
            "recent_events": [],
            "last_actions": {},
            "account_summary": {
                "status": "stopped",
                "message": (
                    "GMO 実時間シミュレーションは停止中です。"
                    if self.config.broker.mode == BrokerMode.GMO_SIM or self.config.data.source == "gmo"
                    else "ローカルシミュレーションは停止中です。"
                ),
            },
            "kill_switch_reason": "",
            "connection_state": "idle",
            "stream_state": {"enabled": self.config.data.stream_enabled, "connected": False, "healthy": False},
            "reconnect_attempts": 0,
            "last_reconnect_at": "",
            "data_source": self.config.data.source,
            "entry_timeframe": (
                self.config.strategy.fx_breakout_pullback.execution_timeframe.value
                if self.config.strategy.name == FxBreakoutPullbackStrategy.name
                else self.config.strategy.entry_timeframe.value
            ),
            "latest_market_bar_at": {},
        }
        self._runtime_status_cache = snapshot
        self._runtime_status_cached_at = monotonic()
        return snapshot

    def load_chart_dataset(
        self,
        symbol: str,
        timeframe: str,
        *,
        force_refresh: bool = False,
    ) -> dict[str, object]:
        selected_symbol = normalize_fx_symbol(symbol)
        selected_timeframe = _resolve_timeframe(timeframe)
        runtime_mode = (
            self.automation_controller is not None
            or self.config.data.source == "gmo"
            or self.config.broker.mode == BrokerMode.GMO_SIM
        )
        runtime_snapshot = self.runtime_status_snapshot(force_refresh=force_refresh)
        cache_key = (selected_symbol, selected_timeframe.value, runtime_mode)
        cache_signature = self._chart_dataset_signature(
            symbol=selected_symbol,
            timeframe=selected_timeframe,
            runtime_mode=runtime_mode,
            runtime_snapshot=runtime_snapshot,
        )
        if not force_refresh and self._chart_dataset_signatures.get(cache_key) == cache_signature:
            cached = self._chart_dataset_cache.get(cache_key)
            if cached is not None:
                return cached
        data_service = MarketDataService(self.config, self.env)
        loader = (
            data_service.load_runtime_symbol_frames
            if runtime_mode
            else data_service.load_symbol_frames
        )
        start_ts, end_ts = self._chart_dataset_window(selected_timeframe, runtime_mode=runtime_mode)
        loader_kwargs: dict[str, object] = {
            "timeframes": [selected_timeframe],
            "start": start_ts.isoformat(),
        }
        if runtime_mode:
            loader_kwargs["as_of"] = end_ts
        else:
            loader_kwargs["end"] = end_ts.isoformat()
        try:
            symbol_frames = loader(selected_symbol, **loader_kwargs)
        except TypeError as exc:
            if "unexpected keyword" not in str(exc):
                raise
            symbol_frames = loader(selected_symbol)
        selected_frame = symbol_frames.get(selected_timeframe, pd.DataFrame())
        selected_frame = self._trim_chart_frame_for_ui(selected_frame)
        fills_frame = pd.DataFrame(runtime_snapshot.get("recent_fills", []))
        payload = {
            "frame": selected_frame,
            "fills": fills_frame,
            "runtime": runtime_mode,
            "entry_timeframe": self.config.strategy.entry_timeframe.value,
        }
        self._chart_dataset_cache[cache_key] = payload
        self._chart_dataset_signatures[cache_key] = cache_signature
        return payload

    def manual_close_position(self, symbol: str, quantity: int | None = None) -> dict[str, object]:
        upper = normalize_fx_symbol(symbol)
        if not upper:
            raise ValueError("通貨ペアが指定されていません。")
        if self.automation_controller is not None:
            result = self.automation_controller.manual_close_position(upper, quantity=quantity)
            self._invalidate_runtime_cache()
            self._invalidate_chart_cache()
            return result
        raise RuntimeError("停止後のローカルシミュレーションポジションは保持されません。稼働中に決済してください。")

    def manual_close_all_positions(self) -> dict[str, object]:
        if self.automation_controller is not None:
            result = self.automation_controller.manual_close_all_positions()
            self._invalidate_runtime_cache()
            self._invalidate_chart_cache()
            return result
        raise RuntimeError("停止後のローカルシミュレーションポジションは保持されません。稼働中に決済してください。")

    def list_reports(self) -> list[Path]:
        output_dir = self.config.reporting.output_dir
        if not output_dir.exists():
            return []
        return sorted(output_dir.iterdir(), reverse=True)

    def list_runs(self) -> list[dict[str, object]]:
        if self._runs_cache is None:
            self._runs_cache = self.store.list_runs()
        return [dict(row) for row in self._runs_cache]

    def list_backtest_runs(self) -> list[dict[str, object]]:
        if self._backtest_runs_cache is not None:
            return [dict(row) for row in self._backtest_runs_cache]
        rows = [row for row in self.list_runs() if str(row.get("run_kind", "")) == RunKind.BACKTEST.value]
        enriched: list[dict[str, object]] = []
        for index, row in enumerate(rows):
            output_dir_value = str(row.get("output_dir") or "").strip()
            output_dir = Path(output_dir_value) if output_dir_value else None
            snapshot = _safe_yaml_mapping(self.store.load_config_snapshot(str(row.get("run_id", "")))) or _load_report_snapshot(output_dir)
            start_date, end_date = _saved_run_period(snapshot, row)
            ml_details = _saved_run_ml_details(snapshot)
            enriched_row = dict(row)
            enriched_row.update(
                {
                    "is_latest": index == 0,
                    "output_dir_path": output_dir,
                    "config_snapshot": snapshot,
                    "strategy_label": _STRATEGY_LABELS.get(str(row.get("strategy_name") or ""), str(row.get("strategy_name") or "-")),
                    "start_date": start_date,
                    "end_date": end_date,
                    "starting_cash": _saved_run_starting_cash(snapshot),
                    "symbols": _saved_run_symbols(snapshot, row),
                    "ml_enabled": ml_details["enabled"],
                    "ml_mode": ml_details["mode"],
                    "ml_mode_label": ml_details["mode_label"],
                    "ml_model_path": ml_details["model_path"],
                    "ml_model_label": ml_details["model_label"],
                }
            )
            enriched_row["display_label"] = _saved_run_label(enriched_row)
            enriched.append(enriched_row)
        self._backtest_runs_cache = [dict(row) for row in enriched]
        return [dict(row) for row in self._backtest_runs_cache]

    def load_saved_backtest_result(self, run_id: str | None = None) -> BacktestResult | None:
        runs = self.list_backtest_runs()
        if not runs:
            self.last_result = None
            return None
        selected = runs[0] if run_id is None else next((row for row in runs if str(row.get("run_id")) == run_id), None)
        if selected is None:
            raise ValueError("指定された保存済みバックテスト結果が見つかりません。")
        if self.last_result is not None and self.last_result.run_id == str(selected.get("run_id")):
            return self.last_result

        run_id_value = str(selected.get("run_id") or "")
        record = self.store.load_run_record(run_id_value) or selected
        output_dir_value = str(record.get("output_dir") or "").strip()
        output_dir = Path(output_dir_value) if output_dir_value else None
        snapshot = _safe_yaml_mapping(str(record.get("config_snapshot_yaml") or "")) or _load_report_snapshot(output_dir)
        start_date, end_date = _saved_run_period(snapshot, record)

        def _table(table_name: str) -> pd.DataFrame:
            frame = self.store.load_table(run_id_value, table_name)
            if frame.empty and output_dir is not None:
                file_name = _REPORT_TABLE_FILES.get(table_name)
                if file_name:
                    return _load_report_frame(output_dir / file_name)
            return frame

        def _signals_table() -> pd.DataFrame:
            frame = self.store.load_recent_table(run_id_value, "signals", 300)
            if frame.empty and output_dir is not None:
                report_frame = _load_report_frame(output_dir / _REPORT_TABLE_FILES["signals"])
                if not report_frame.empty:
                    frame = report_frame.tail(300).reset_index(drop=True)
            return frame

        equity_curve = _load_report_frame(output_dir / "equity_curve.csv") if output_dir is not None else pd.DataFrame()
        drawdown_curve = _load_report_frame(output_dir / "drawdown.csv") if output_dir is not None else pd.DataFrame()
        self.last_result = BacktestResult(
            run_id=run_id_value,
            strategy_name=str(record.get("strategy_name") or ""),
            mode=BrokerMode(str(record.get("mode") or BrokerMode.LOCAL_SIM.value)),
            symbols=_saved_run_symbols(snapshot, record),
            backtest_start=start_date,
            backtest_end=end_date,
            starting_cash=_saved_run_starting_cash(snapshot),
            metrics=dict(record.get("metrics") or {}),
            equity_curve=equity_curve,
            drawdown_curve=drawdown_curve,
            trades=_table("trades"),
            # 起動直後の UI では orders / fills / positions を参照しないため、
            # 保存済み結果の復元では読み込まずにメインスレッド負荷を下げる。
            orders=pd.DataFrame(),
            fills=pd.DataFrame(),
            positions=pd.DataFrame(),
            signals=_signals_table(),
            benchmark_curve=None,
            in_sample_metrics=dict(record.get("in_sample_metrics") or {}),
            out_of_sample_metrics=dict(record.get("out_of_sample_metrics") or {}),
            walk_forward=list(record.get("walk_forward") or []),
            chart_frames={},
            output_dir=output_dir_value or None,
        )
        self._invalidate_chart_cache()
        return self.last_result

    def load_run_table(self, run_id: str, table: str):
        return self.store.load_table(run_id, table)

    def load_saved_run_signals(self, run_id: str) -> pd.DataFrame:
        cached = self._saved_signals_cache.get(run_id)
        if cached is not None:
            return cached.copy()
        frame = self.store.load_table(run_id, "signals")
        if frame.empty:
            record = self.store.load_run_record(run_id)
            output_dir_value = str(record.get("output_dir") or "").strip() if record is not None else ""
            output_dir = Path(output_dir_value) if output_dir_value else None
            if output_dir is not None:
                frame = _load_report_frame(output_dir / _REPORT_TABLE_FILES["signals"])
        self._saved_signals_cache[run_id] = frame.copy()
        return frame

    def load_saved_run_signal_snapshot(self, run_id: str) -> dict[str, object]:
        cached = self._saved_signal_snapshot_cache.get(run_id)
        if cached is not None:
            snapshot = dict(cached)
            snapshot["recent_signals"] = cached.get("recent_signals", pd.DataFrame()).copy()
            snapshot["symbol_frame"] = cached.get("symbol_frame", pd.DataFrame()).copy()
            return snapshot
        snapshot: dict[str, object] | None = None
        record = self.store.load_run_record(run_id)
        output_dir_value = str(record.get("output_dir") or "").strip() if record is not None else ""
        output_dir = Path(output_dir_value) if output_dir_value else None
        if output_dir is not None and output_dir.exists():
            snapshot = load_signal_snapshot_artifacts(output_dir)
        if snapshot is None:
            snapshot = self.store.load_signal_snapshot(run_id, threshold=0.55, recent_limit=300, bins=11, symbol_limit=5)
            if output_dir is not None and output_dir.exists():
                try:
                    write_signal_snapshot_artifacts(output_dir, snapshot)
                except OSError:
                    pass
        self._saved_signal_snapshot_cache[run_id] = {
            "recent_signals": snapshot.get("recent_signals", pd.DataFrame()).copy(),
            "summary": dict(snapshot.get("summary") or {}),
            "histogram": dict(snapshot.get("histogram") or {}),
            "symbol_frame": snapshot.get("symbol_frame", pd.DataFrame()).copy(),
        }
        return {
            "recent_signals": self._saved_signal_snapshot_cache[run_id]["recent_signals"].copy(),
            "summary": dict(self._saved_signal_snapshot_cache[run_id]["summary"]),
            "histogram": dict(self._saved_signal_snapshot_cache[run_id]["histogram"]),
            "symbol_frame": self._saved_signal_snapshot_cache[run_id]["symbol_frame"].copy(),
        }

    def _compact_backtest_result_for_ui(self) -> None:
        if self.last_result is None:
            return
        if self.last_result.signals is not None and not self.last_result.signals.empty:
            self.last_result.signals = self.last_result.signals.tail(300).reset_index(drop=True)
        if self.last_result.chart_frames:
            self.last_result.chart_frames = {
                symbol: {
                    timeframe: self._trim_chart_frame_for_ui(frame)
                    for timeframe, frame in frames.items()
                }
                for symbol, frames in self.last_result.chart_frames.items()
            }

    def _chart_dataset_window(
        self,
        timeframe: TimeFrame,
        *,
        runtime_mode: bool,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        end_ts = (
            pd.Timestamp.now(tz=ASIA_TOKYO)
            if runtime_mode
            else self._coerce_chart_window_timestamp(self.config.data.end_date, is_end=True)
        )
        config_start = self._coerce_chart_window_timestamp(
            self.config.data.start_date,
            is_end=False,
        )
        max_bars = max(500, int(self.config.data.max_bars_per_symbol or 5000))
        start_ts = end_ts - timeframe_coverage_delta(timeframe) * max_bars
        return max(config_start, start_ts), end_ts

    def _coerce_chart_window_timestamp(
        self,
        value: str | pd.Timestamp,
        *,
        is_end: bool,
    ) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(ASIA_TOKYO)
        else:
            timestamp = timestamp.tz_convert(ASIA_TOKYO)
        if is_end and isinstance(value, str):
            normalized = value.strip()
            if "T" not in normalized and ":" not in normalized and " " not in normalized:
                timestamp += pd.Timedelta(days=1)
        return timestamp

    def _trim_chart_frame_for_ui(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame() if frame is None else frame.copy()
        max_rows = max(500, int(self.config.data.max_bars_per_symbol or 5000))
        return frame.tail(max_rows).copy()

    def load_automation_events(self, run_id: str):
        return self.store.load_automation_events(run_id)

    def locate_report(self, run_id: str) -> Path | None:
        for path in self.list_reports():
            if run_id in path.name:
                return path
        return None

    def available_timeframes(self) -> list[str]:
        return [timeframe.value for timeframe in TimeFrame]

    def credential_statuses(self) -> dict[str, dict[str, object]]:
        private_credentials = self._resolved_private_credentials()
        return {
            "public": {
                "configured": True,
                "source": "public_api",
                "key_hint": "認証不要",
                "keychain_available": private_credentials.keychain_available,
            },
            "private": {
                "configured": private_credentials.configured,
                "source": private_credentials.source,
                "key_hint": self._mask_api_key(private_credentials.api_key) if private_credentials.configured else "未設定",
                "keychain_available": private_credentials.keychain_available,
            },
        }

    def load_credential_values(self, profile: str) -> dict[str, object]:
        normalized = profile.lower().strip()
        if normalized == "private":
            credentials = self._resolved_private_credentials()
            return {
                "profile": "private",
                "configured": credentials.configured,
                "source": credentials.source,
                "api_key": credentials.api_key,
                "api_secret": credentials.api_secret,
                "api_key_masked": self._mask_api_key(credentials.api_key),
                "api_secret_masked": self._mask_api_secret(credentials.api_secret),
                "keychain_available": credentials.keychain_available,
            }
        return {
            "profile": "public",
            "configured": True,
            "source": "public_api",
            "api_key": "",
            "api_secret": "",
            "api_key_masked": "",
            "api_secret_masked": "",
            "keychain_available": self._resolved_private_credentials().keychain_available,
        }

    def save_gmo_credentials(self, profile: str, api_key: str, api_secret: str) -> dict[str, object]:
        normalized = profile.lower().strip()
        if normalized != "private":
            raise ValueError("GMO private API の資格情報だけ保存できます。")
        save_private_gmo_credentials(api_key=api_key, api_secret=api_secret)
        self.env = load_environment()
        return self.load_credential_values("private")

    def delete_gmo_credentials(self, profile: str) -> bool:
        normalized = profile.lower().strip()
        if normalized != "private":
            return False
        deleted = delete_private_gmo_credentials()
        self.env = load_environment()
        return deleted

    def import_jforex_csv(self, file_path: str, symbol: str | None = None) -> dict[str, object]:
        _ = file_path, symbol
        raise RuntimeError(
            "単一 CSV のインポートは無効です。\n"
            "Bid / Ask の 2 ファイルを指定してください。"
        )

    def import_jforex_bid_ask_csv(
        self,
        bid_file_path: str,
        ask_file_path: str,
        symbol: str | None = None,
    ) -> dict[str, object]:
        result = JForexCsvImporter(MarketDataService(self.config, self.env).cache).import_bid_ask_files(
            bid_file_path=bid_file_path,
            ask_file_path=ask_file_path,
            symbol=symbol,
        )
        self._invalidate_chart_cache()
        return {
            "symbol": result.symbol,
            "bid_source_path": str(result.bid_source_path),
            "ask_source_path": str(result.ask_source_path),
            "imported_rows": result.imported_rows,
            "skipped_rows": result.skipped_rows,
            "start": result.start,
            "end": result.end,
            "applied_start": result.applied_start,
            "applied_end": result.applied_end,
            "bid_start": result.bid_start,
            "bid_end": result.bid_end,
            "ask_start": result.ask_start,
            "ask_end": result.ask_end,
            "messages": list(result.messages),
            "timeframes": sorted(result.cache_paths),
            "cache_paths": result.cache_paths,
        }

    def import_jforex_tick_csv(
        self,
        file_path: str,
        symbol: str | None = None,
    ) -> dict[str, object]:
        tick_cache = ParquetTickCache(self.config.strategy.fx_scalping.tick_cache_dir)
        result = JForexTickCsvImporter(tick_cache).import_file(file_path, symbol=symbol)
        return {
            "symbol": result.symbol,
            "source_path": str(result.source_path),
            "imported_rows": result.imported_rows,
            "dropped_rows": result.dropped_rows,
            "duplicate_timestamps": result.duplicate_timestamps,
            "crossed_quotes": result.crossed_quotes,
            "start": result.start,
            "end": result.end,
            "cache_paths": result.cache_paths,
            "messages": list(result.messages),
        }

    def run_scalping_backtest(
        self,
        *,
        tick_file_path: str | None = None,
        symbol: str | None = None,
        start: str | None = None,
        end: str | None = None,
        progress_callback=None,
    ) -> dict[str, object]:
        normalized_symbol = normalize_fx_symbol(symbol or self.config.watchlist.symbols[0])
        tick_cache = ParquetTickCache(self.config.strategy.fx_scalping.tick_cache_dir)
        import_summary: dict[str, object] | None = None
        if tick_file_path:
            import_summary = self.import_jforex_tick_csv(tick_file_path, normalized_symbol)
        start_ts, end_ts = self._scalping_window(start=start, end=end)
        _emit_progress(
            progress_callback,
            task="scalping_backtest",
            current=1,
            total=4,
            phase="load_ticks",
            message="tick データを読み込んでいます。",
        )
        ticks = tick_cache.load_window(normalized_symbol, start_ts, end_ts)
        if ticks.empty:
            raise RuntimeError(
                "指定期間の tick キャッシュが空です。"
                " JForex tick CSV を import-jforex-tick-csv で取り込んでください。"
            )
        _emit_progress(
            progress_callback,
            task="scalping_backtest",
            current=2,
            total=4,
            phase="train",
            message="スキャルピング係数を学習しています。",
        )
        output_root = self.config.reporting.output_dir / "scalping"
        result = run_scalping_pipeline(
            ticks,
            symbol=normalized_symbol,
            config=self.config,
            output_dir=output_root,
        )
        _emit_progress(
            progress_callback,
            task="scalping_backtest",
            current=4,
            total=4,
            phase="done",
            message="スキャルピングバックテストが完了しました。",
        )
        return {
            "run_id": result.run_id,
            "symbol": normalized_symbol,
            "output_dir": str((output_root / result.run_id) if result.output_dir is not None else ""),
            "import_summary": import_summary or {},
            "train_start": result.train_start,
            "train_end": result.train_end,
            "test_start": result.test_start,
            "test_end": result.test_end,
            "metrics": result.backtest.metrics,
            "model_summary": result.backtest.model_summary,
        }

    def run_scalping_realtime_sim(
        self,
        *,
        symbol: str | None = None,
        max_ticks: int = 120,
        poll_seconds: float = 1.0,
    ) -> dict[str, object]:
        normalized_symbol = normalize_fx_symbol(symbol or self.config.watchlist.symbols[0])
        scalping_cfg = self.config.strategy.fx_scalping
        model_path = scalping_cfg.model_dir / scalping_cfg.latest_model_alias
        if not model_path.exists():
            raise RuntimeError(
                "スキャルピング用の学習済みモデルがありません。"
                " 先に scalping-backtest を実行して係数を作成してください。"
            )
        training_config = training_config_from_app(self.config)
        model_bundle = load_scalping_model_bundle(model_path, training_config)
        engine = ScalpingRealtimePaperEngine(
            symbol=normalized_symbol,
            pip_size=float(scalping_cfg.pip_size or pip_size_for_symbol(normalized_symbol)),
            model_bundle=model_bundle,
            training_config=training_config,
            execution_config=execution_config_from_app(self.config),
            bar_rule=scalping_cfg.bar_rule,
        )
        client = GmoForexPublicClient(self.env)
        observed_ticks = 0
        for _ in range(max(1, int(max_ticks))):
            quotes = client.fetch_ticker_quotes()
            quote = quotes.get(normalized_symbol)
            if quote is None:
                raise RuntimeError(f"GMO ticker に {normalized_symbol} が含まれていません。")
            engine.on_tick(timestamp=quote.timestamp, bid=quote.bid, ask=quote.ask)
            observed_ticks += 1
            if observed_ticks < max_ticks and poll_seconds > 0:
                sleep(float(poll_seconds))
        snapshot = engine.snapshot()
        snapshot["observed_ticks"] = observed_ticks
        snapshot["mode_note_ja"] = (
            "GMO public REST ticker をスキャルピング paper engine へ流し込む簡易リアルタイムシミュレーションです。"
            " 本番運用前は WebSocket ticker 記録で shadow 検証してください。"
        )
        return snapshot

    def record_gmo_scalping_ticks(
        self,
        *,
        symbol: str | None = None,
        max_ticks: int | None = None,
    ) -> dict[str, object]:
        normalized_symbol = normalize_fx_symbol(symbol or self.config.watchlist.symbols[0])
        tick_cache = ParquetTickCache(self.config.strategy.fx_scalping.tick_cache_dir)
        return GmoPublicWebSocketTickRecorder(
            self.env,
            tick_cache,
            symbol=normalized_symbol,
        ).run(max_ticks=max_ticks)

    def _scalping_window(self, *, start: str | None, end: str | None) -> tuple[pd.Timestamp, pd.Timestamp]:
        start_value = start or (self.config.backtest.start_date if self.config.backtest.use_custom_window else self.config.data.start_date)
        end_value = end or (self.config.backtest.end_date if self.config.backtest.use_custom_window else self.config.data.end_date)
        start_ts = pd.Timestamp(start_value)
        end_ts = pd.Timestamp(end_value)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize(ASIA_TOKYO)
        else:
            start_ts = start_ts.tz_convert(ASIA_TOKYO)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize(ASIA_TOKYO)
        else:
            end_ts = end_ts.tz_convert(ASIA_TOKYO)
        if start_ts >= end_ts:
            raise ValueError("スキャルピング検証の開始日時は終了日時より前にしてください。")
        return start_ts, end_ts

    def available_gmo_symbols(self) -> list[dict[str, object]]:
        rules = GmoForexPublicClient(self.env).list_symbols()
        return [
            {
                "symbol": normalize_fx_symbol(str(item.get("symbol", ""))),
                "min_open_order_size": str(item.get("minOpenOrderSize", "")),
                "max_order_size": str(item.get("maxOrderSize", "")),
                "size_step": str(item.get("sizeStep", "")),
                "tick_size": str(item.get("tickSize", "")),
            }
            for item in rules
        ]

    def _invalidate_runtime_cache(self) -> None:
        self._runtime_status_cache = None
        self._runtime_status_cached_at = 0.0

    def _invalidate_chart_cache(self) -> None:
        self._chart_dataset_cache.clear()
        self._chart_dataset_signatures.clear()

    def _invalidate_run_listing_cache(self) -> None:
        self._runs_cache = None
        self._backtest_runs_cache = None

    def _chart_dataset_signature(
        self,
        *,
        symbol: str,
        timeframe: TimeFrame,
        runtime_mode: bool,
        runtime_snapshot: dict[str, object],
    ) -> tuple[object, ...]:
        if not runtime_mode:
            return (
                "backtest",
                getattr(self.last_result, "run_id", ""),
                symbol,
                timeframe.value,
            )
        latest_market_bar_at = runtime_snapshot.get("latest_market_bar_at", {})
        fills = runtime_snapshot.get("recent_fills", [])
        last_fill_token = ""
        if fills:
            latest_fill = fills[-1]
            last_fill_token = str(
                latest_fill.get("fill_id")
                or latest_fill.get("order_id")
                or latest_fill.get("filled_at")
                or latest_fill.get("submitted_at")
                or ""
            )
        return (
            "runtime",
            runtime_snapshot.get("mode", self.config.broker.mode.value),
            runtime_snapshot.get("status", "stopped"),
            symbol,
            timeframe.value,
            self.config.strategy.name,
            self.config.strategy.entry_timeframe.value,
            tuple(sorted((str(key), str(value)) for key, value in dict(latest_market_bar_at).items())),
            len(fills),
            last_fill_token,
        )

    def update_notification_settings(
        self,
        *,
        enabled: bool,
        channels: list[str],
        sound_name: str,
        webhook_url: str,
    ) -> None:
        self.config.automation.notifications_enabled = enabled
        self.config.automation.notification_channels.channels = channels or ["log"]
        self.config.automation.notification_channels.sound_name = sound_name.strip() or "Glass"
        self.config.automation.notification_channels.webhook_url = webhook_url.strip()
        self.save_config()

    def test_gmo_connection(self) -> dict[str, object]:
        client = GmoForexPublicClient(self.env)
        warnings: list[str] = []
        quotes = client.fetch_ticker_quotes()
        rules = client.list_symbols()
        market_data_rows = 0
        test_symbol = (self.config.watchlist.benchmark_symbols or self.config.watchlist.symbols or ["USD_JPY"])[0]
        try:
            start = pd.Timestamp.now(tz="Asia/Tokyo") - pd.Timedelta(days=3)
            end = pd.Timestamp.now(tz="Asia/Tokyo")
            frame = client.fetch_bars(
                test_symbol,
                TimeFrame.MIN_15,
                start.to_pydatetime(),
                end.to_pydatetime(),
                price_type=self.config.data.gmo_price_type,
            )
            market_data_rows = len(frame)
            if frame.empty:
                warnings.append("市場データ応答が空でした。")
        except Exception as exc:
            warnings.append(f"市場データ確認に失敗しました: {exc}")
        result = {
            "profile": "public",
            "ok": bool(quotes),
            "account_ok": True,
            "market_data_ok": market_data_rows > 0,
            "account_status": "public_api_open" if quotes else "unavailable",
            "equity": "",
            "buying_power": "",
            "recent_order_count": 0,
            "market_data_symbol": test_symbol,
            "market_data_rows": market_data_rows,
            "warning_count": len(warnings),
            "warnings_ja": warnings,
            "tested_at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
            "mode_label_ja": "GMO public API 接続確認",
            "note_ja": "この確認は GMO の public API を使った read-only の疎通確認です。",
            "symbol_count": len(rules),
            "ticker_count": len(quotes),
        }
        self.connection_test_results["public"] = result
        return result

    @staticmethod
    def _mask_api_key(api_key: str) -> str:
        normalized = api_key.strip()
        if not normalized:
            return ""
        if len(normalized) <= 8:
            return f"{'*' * max(1, len(normalized) - 4)}{normalized[-4:]}"
        return f"{normalized[:4]}{'*' * max(1, len(normalized) - 8)}{normalized[-4:]}"

    @staticmethod
    def _mask_api_secret(api_secret: str) -> str:
        normalized = api_secret.strip()
        if not normalized:
            return ""
        if len(normalized) <= 4:
            return "*" * len(normalized)
        return f"{'*' * max(4, len(normalized) - 4)}{normalized[-4:]}"

    def _resolved_private_credentials(self):
        return resolve_private_gmo_credentials(
            env_api_key=getattr(self.env, "gmo_api_key", ""),
            env_api_secret=getattr(self.env, "gmo_api_secret", ""),
        )

    @staticmethod
    def _prepare_writable_config_path(path: Path | None) -> Path | None:
        if path is None:
            return None
        candidate = Path(path)
        if not candidate.exists():
            return candidate
        resolved = candidate.resolve()
        needs_user_copy = getattr(sys, "frozen", False) or "/Contents/Resources/" in str(resolved)
        needs_user_copy = needs_user_copy or not os.access(candidate, os.W_OK)
        if not needs_user_copy:
            return candidate
        target = Path.home() / "Library" / "Application Support" / "FXAutoTradeLab" / "configs" / candidate.name
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(candidate, target)
        return target
