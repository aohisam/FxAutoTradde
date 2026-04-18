"""Shared orchestration for CLI and desktop."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import os
from pathlib import Path
import shutil
import sys
from time import monotonic

import pandas as pd

from fxautotrade_lab.automation.controller import AutomationController
from fxautotrade_lab.backtest.fx_backtest import train_fx_filter_model_run
from fxautotrade_lab.backtest.runner import BacktestRunner
from fxautotrade_lab.config.loader import load_app_config, load_environment, save_app_config
from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSizingMode, TimeFrame
from fxautotrade_lab.core.models import BacktestResult
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.data.gmo import GmoForexPublicClient
from fxautotrade_lab.data.jforex import JForexCsvImporter
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from fxautotrade_lab.persistence.sqlite_store import SQLiteStore
from fxautotrade_lab.research.pipeline import ResearchPipeline
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from fxautotrade_lab.strategies.registry import create_strategy


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

    def __post_init__(self) -> None:
        self.config_path = self._prepare_writable_config_path(self.config_path)
        self.reload_config()

    def reload_config(self) -> None:
        self.config = load_app_config(self.config_path, self.overrides)
        self.env = load_environment()
        self.store = SQLiteStore(self.config.persistence.sqlite_path)
        self._invalidate_runtime_cache()
        self._invalidate_chart_cache()

    def save_config(self) -> None:
        if self.config_path is not None:
            save_app_config(self.config, self.config_path)

    def sync_data(self) -> dict[str, object]:
        return MarketDataService(self.config, self.env).sync()

    def run_backtest(self) -> BacktestResult:
        self.last_result = BacktestRunner(self.config, self.env).run()
        if self.config.persistence.enabled:
            self.store.save_backtest_result(self.last_result, self.config)
        return self.last_result

    def train_fx_model(self, *, as_of: str | None = None) -> dict[str, object]:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            raise RuntimeError("FX breakout 戦略以外では FX ML 学習は利用できません。")
        summary = train_fx_filter_model_run(self.config, self.env, as_of=as_of)
        return summary

    def model_status(self) -> dict[str, object]:
        ml_cfg = self.config.strategy.fx_breakout_pullback.ml_filter
        model_path = ml_cfg.pretrained_model_path or (ml_cfg.model_dir / ml_cfg.latest_model_alias)
        return {
            "enabled": ml_cfg.enabled,
            "backtest_mode": ml_cfg.backtest_mode,
            "model_path": str(model_path),
            "exists": model_path.exists(),
        }

    def run_research(self, *, mode: str | None = None) -> dict[str, object]:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            raise RuntimeError("FX breakout 戦略以外では research_run は未対応です。")
        self.last_research_result = ResearchPipeline(self.config, self.env, mode=mode).run()
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
        selected_timeframe = TimeFrame(timeframe)
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
        loader = data_service.load_runtime_symbol_frames if runtime_mode else data_service.load_symbol_frames
        symbol_frames = loader(selected_symbol)
        benchmark_frames = loader(self.config.watchlist.benchmark_symbols[0]) if self.config.watchlist.benchmark_symbols else None
        sector_frames = loader(self.config.watchlist.sector_symbols[0]) if self.config.watchlist.sector_symbols else None
        selected_frame = symbol_frames.get(selected_timeframe, pd.DataFrame()).copy()
        if self.config.strategy.name == FxBreakoutPullbackStrategy.name:
            fx_feature_set = build_fx_feature_set(
                symbol=selected_symbol,
                bars_by_timeframe=symbol_frames,
                config=self.config,
                runtime_mode=runtime_mode,
            )
            if selected_timeframe == self.config.strategy.fx_breakout_pullback.execution_timeframe:
                selected_frame = create_strategy(self.config).generate_signal_frame(fx_feature_set.execution_frame)
        else:
            feature_set = build_multi_timeframe_feature_set(
                symbol=selected_symbol,
                bars_by_timeframe=symbol_frames,
                benchmark_bars=benchmark_frames,
                sector_bars=sector_frames,
                config=self.config,
            )
            if selected_timeframe == self.config.strategy.entry_timeframe:
                signal_frame = create_strategy(self.config).generate_signal_frame(feature_set.entry_frame)
                for column in feature_set.entry_frame.columns:
                    if column not in signal_frame.columns:
                        signal_frame[column] = feature_set.entry_frame[column]
                selected_frame = signal_frame
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
        return self.store.list_runs()

    def load_run_table(self, run_id: str, table: str):
        return self.store.load_table(run_id, table)

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
        return {
            "public": {
                "configured": True,
                "source": "public_api",
                "key_hint": "認証不要",
                "keychain_available": False,
            },
            "private": {
                "configured": bool(self.env.has_credentials("private")),
                "source": "env" if self.env.has_credentials("private") else "unset",
                "key_hint": self._mask_api_key(self.env.gmo_api_key) if self.env.has_credentials("private") else "未設定",
                "keychain_available": False,
            },
        }

    def load_credential_values(self, profile: str) -> dict[str, object]:
        normalized = profile.lower().strip()
        if normalized == "private":
            api_key = self.env.gmo_api_key.strip()
            api_secret = self.env.gmo_api_secret.strip()
            return {
                "profile": "private",
                "configured": bool(api_key and api_secret),
                "source": "env" if api_key and api_secret else "unset",
                "api_key": api_key,
                "api_secret": api_secret,
                "api_key_masked": self._mask_api_key(api_key),
                "api_secret_masked": self._mask_api_secret(api_secret),
            }
        return {
            "profile": "public",
            "configured": True,
            "source": "public_api",
            "api_key": "",
            "api_secret": "",
            "api_key_masked": "",
            "api_secret_masked": "",
        }

    def save_gmo_credentials(self, profile: str, api_key: str, api_secret: str) -> dict[str, object]:
        _ = profile, api_key, api_secret
        raise RuntimeError("現行版では UI から GMO 認証情報を保存しません。.env に設定してください。")

    def delete_gmo_credentials(self, profile: str) -> bool:
        _ = profile
        return False

    def import_jforex_csv(self, file_path: str, symbol: str | None = None) -> dict[str, object]:
        result = JForexCsvImporter(MarketDataService(self.config, self.env).cache).import_file(file_path, symbol=symbol)
        self._invalidate_chart_cache()
        return {
            "symbol": result.symbol,
            "source_path": str(result.source_path),
            "imported_rows": result.imported_rows,
            "skipped_rows": result.skipped_rows,
            "start": result.start,
            "end": result.end,
            "applied_start": result.applied_start,
            "applied_end": result.applied_end,
            "timeframes": sorted(result.cache_paths),
            "cache_paths": result.cache_paths,
        }

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
            "timeframes": sorted(result.cache_paths),
            "cache_paths": result.cache_paths,
        }

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
