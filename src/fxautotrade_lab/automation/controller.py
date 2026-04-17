"""Automated forward simulation loop for FX runtime validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
from uuid import uuid4

import pandas as pd

from fxautotrade_lab.automation.notifications import MultiChannelNotifier
from fxautotrade_lab.backtest.fx_backtest import train_fx_filter_model_run
from fxautotrade_lab.brokers.base import BaseBroker
from fxautotrade_lab.brokers.local_sim import LocalSimBroker
from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import AutomationStatus, BrokerMode, OrderSide
from fxautotrade_lab.core.models import AutomationEvent
from fxautotrade_lab.core.symbols import split_fx_symbol
from fxautotrade_lab.core.windows import shift_timestamp
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.data.session import get_session_state
from fxautotrade_lab.execution.safety import DuplicateOrderGuard
from fxautotrade_lab.execution.managed_exits import (
    ExitDecision,
    ManagedPositionState,
    build_managed_position,
    evaluate_managed_position,
    recent_swing_low,
)
from fxautotrade_lab.execution.risk import RiskManager
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from fxautotrade_lab.ml.fx_filter import apply_fx_ml_filter, load_filter_model
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from fxautotrade_lab.strategies.registry import create_strategy


TERMINAL_ORDER_STATUSES = {"filled", "cancelled", "canceled", "rejected", "expired"}


@dataclass(slots=True)
class AutomationController:
    config: AppConfig
    env: EnvironmentConfig
    stop_event: Event = field(init=False)
    state: list = field(init=False)
    data_service: MarketDataService = field(init=False)
    strategy: object = field(init=False)
    logs: list[AutomationEvent] = field(init=False)
    local_indices: dict[str, int] = field(init=False)
    broker: BaseBroker = field(init=False)
    run_id: str = field(init=False)
    status: AutomationStatus = field(init=False)
    cycle_count: int = field(init=False)
    last_heartbeat_at: pd.Timestamp | None = field(init=False)
    open_symbols: set[str] = field(init=False)
    recent_orders: list[dict[str, object]] = field(init=False)
    recent_fills: list[dict[str, object]] = field(init=False)
    recent_signals: list[dict[str, object]] = field(init=False)
    synced_positions: list[dict[str, object]] = field(init=False)
    last_actions: dict[str, str] = field(init=False)
    account_summary: dict[str, object] = field(init=False)
    duplicate_guard: DuplicateOrderGuard = field(init=False)
    risk_manager: RiskManager = field(init=False)
    notifier: MultiChannelNotifier = field(init=False)
    session_start_equity: float | None = field(init=False)
    kill_switch_reason: str = field(init=False)
    stream_client: object | None = field(init=False)
    connection_state: str = field(init=False)
    stream_state: dict[str, object] = field(init=False)
    reconnect_attempts: int = field(init=False)
    last_reconnect_at: pd.Timestamp | None = field(init=False)
    last_processed_entry_bar_at: dict[str, pd.Timestamp] = field(init=False)
    latest_market_bar_at: dict[str, str] = field(init=False)
    managed_positions: dict[str, ManagedPositionState] = field(init=False)
    pending_entry_contexts: dict[str, dict[str, object]] = field(init=False)
    fx_loaded_model: object | None = field(init=False)
    fx_pending_entries: dict[str, dict[str, object]] = field(init=False)
    fx_pending_exits: dict[str, dict[str, object]] = field(init=False)
    fx_position_state: dict[str, dict[str, object]] = field(init=False)
    fx_last_retrain_at: pd.Timestamp | None = field(init=False)
    fx_next_retrain_at: pd.Timestamp | None = field(init=False)
    fx_last_retrain_summary: dict[str, object] = field(init=False)

    def __post_init__(self) -> None:
        self.stop_event = Event()
        self.state = []
        self.data_service = MarketDataService(self.config, self.env)
        self.strategy = create_strategy(self.config)
        self.logs = []
        self.local_indices = {symbol: 120 for symbol in self.config.watchlist.symbols}
        self.broker = self._build_broker()
        self.run_id = pd.Timestamp.now(tz=ASIA_TOKYO).strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
        self.status = AutomationStatus.STOPPED
        self.cycle_count = 0
        self.last_heartbeat_at = None
        self.open_symbols = set()
        self.recent_orders = []
        self.recent_fills = []
        self.recent_signals = []
        self.synced_positions = []
        self.last_actions = {}
        self.account_summary = {}
        self.duplicate_guard = DuplicateOrderGuard()
        self.risk_manager = RiskManager(self.config.risk)
        self.notifier = MultiChannelNotifier(
            enabled=self.config.automation.notifications_enabled,
            config=self.config.automation.notification_channels,
        )
        self.session_start_equity = None
        self.kill_switch_reason = ""
        self.stream_client = self._build_stream_client()
        self.connection_state = "idle"
        self.stream_state = {"enabled": False, "connected": False, "healthy": False}
        self.reconnect_attempts = 0
        self.last_reconnect_at = None
        self.last_processed_entry_bar_at = {}
        self.latest_market_bar_at = {}
        self.managed_positions = {}
        self.pending_entry_contexts = {}
        self.fx_loaded_model = self._load_fx_model_if_needed()
        self.fx_pending_entries = {}
        self.fx_pending_exits = {}
        self.fx_position_state = {}
        self.fx_last_retrain_at = None
        self.fx_next_retrain_at = None
        self.fx_last_retrain_summary = {}

    def _build_broker(self) -> BaseBroker:
        return LocalSimBroker(starting_equity=self.config.risk.starting_cash)

    def _build_stream_client(self) -> object | None:
        return None

    def _load_fx_model_if_needed(self) -> object | None:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            return None
        ml_cfg = self.config.strategy.fx_breakout_pullback.ml_filter
        if not ml_cfg.enabled:
            return None
        model_path = ml_cfg.pretrained_model_path or (ml_cfg.model_dir / ml_cfg.latest_model_alias)
        model = load_filter_model(model_path)
        if model is None and (ml_cfg.require_pretrained_model or ml_cfg.missing_model_behavior == "error"):
            raise RuntimeError(f"学習済み FX ML モデルが見つかりません: {model_path}")
        return model

    def _fx_ml_retrain_enabled(self) -> bool:
        if self.config.strategy.name != FxBreakoutPullbackStrategy.name:
            return False
        ml_cfg = self.config.strategy.fx_breakout_pullback.ml_filter
        return bool(ml_cfg.enabled and ml_cfg.realtime_retrain_enabled)

    def _schedule_next_fx_retrain(self, base_time: pd.Timestamp) -> None:
        ml_cfg = self.config.strategy.fx_breakout_pullback.ml_filter
        self.fx_last_retrain_at = pd.Timestamp(base_time)
        self.fx_next_retrain_at = shift_timestamp(
            pd.Timestamp(base_time),
            ml_cfg.realtime_retrain_frequency,
            backward=False,
        )

    def _maybe_retrain_fx_model(self, as_of: pd.Timestamp) -> None:
        if not self._fx_ml_retrain_enabled():
            return
        current_time = pd.Timestamp(as_of)
        if self.fx_loaded_model is not None and self.fx_last_retrain_at is None:
            self._schedule_next_fx_retrain(current_time)
            return
        if self.fx_loaded_model is not None and self.fx_next_retrain_at is not None and current_time < self.fx_next_retrain_at:
            return
        try:
            summary = train_fx_filter_model_run(
                self.config,
                self.env,
                as_of=current_time.isoformat(),
            )
            model_path = summary.get("latest_model_path") or summary.get("model_path") or ""
            loaded = load_filter_model(model_path)
            if loaded is not None:
                self.fx_loaded_model = loaded
            self.fx_last_retrain_summary = summary
            self._schedule_next_fx_retrain(current_time)
            self._log(
                "info",
                f"FX ML を再学習しました。trained_rows={summary.get('trained_rows', 0)} / 次回={self.fx_next_retrain_at.isoformat() if self.fx_next_retrain_at is not None else '-'}",
            )
        except Exception as exc:
            failure_mode = self.config.strategy.fx_breakout_pullback.ml_filter.realtime_retrain_failure_mode.strip().lower()
            if failure_mode == "disable_ml":
                self.fx_loaded_model = None
            self._schedule_next_fx_retrain(current_time)
            self._log("warning", f"FX ML の再学習に失敗したため既存モデルを維持します: {exc}")

    def stop(self) -> None:
        self.status = AutomationStatus.STOPPING
        self.stop_event.set()

    def run(self, max_cycles: int | None = None) -> list[AutomationEvent]:
        cycle_limit = max_cycles
        self.status = AutomationStatus.STARTING
        try:
            self.connection_state = "connecting"
            startup_ok = self._refresh_account_summary()
            if self.config.automation.sync_broker_state_on_start:
                startup_ok = self._sync_broker_state() and startup_ok
            stream_ok = self._connect_streaming_if_needed(force=True)
            if not startup_ok:
                self._recover_runtime_connection("起動時の接続確認")
            elif self.stream_client is not None and not stream_ok:
                self.connection_state = "polling_only"
            self.session_start_equity = self._coerce_float(
                self.account_summary.get("equity") or self.account_summary.get("last_equity")
            )
            if self.connection_state == "connecting":
                self.connection_state = "connected"
            self._log("info", "自動売買ループを開始します")
            self._notify(
                enabled=self.config.automation.notify_on_start_stop,
                title="FXAutoTrade Lab",
                message="自動売買ループを開始しました。",
                subtitle=self.config.broker.mode.value,
            )
            self.status = AutomationStatus.RUNNING
            cycle = 0
            while cycle_limit is None or cycle < cycle_limit:
                cycle += 1
                if self.stop_event.is_set():
                    self._log("info", "停止要求を受け付けました")
                    break
                self.run_cycle(cycle)
                if self.stop_event.is_set():
                    self._log("info", "停止要求を受け付けました")
                    break
                should_wait = not (
                    self.config.broker.mode == BrokerMode.LOCAL_SIM and not self._uses_runtime_market_data()
                )
                if (
                    should_wait
                    and (cycle_limit is None or cycle < cycle_limit)
                    and self.config.automation.poll_interval_seconds > 0
                ):
                    interrupted = self.stop_event.wait(timeout=self.config.automation.poll_interval_seconds)
                    if interrupted:
                        self._log("info", "停止要求を受け付けました")
                        break
        finally:
            self.status = AutomationStatus.STOPPED
            self._log("info", "自動売買ループを終了しました")
            self._notify(
                enabled=self.config.automation.notify_on_start_stop,
                title="FXAutoTrade Lab",
                message="自動売買ループを終了しました。",
                subtitle=self.config.broker.mode.value,
            )
            if self.stream_client is not None:
                self.stream_client.disconnect()
            self.broker.shutdown()
        return self.logs

    def run_cycle(self, cycle_number: int) -> None:
        try:
            self.cycle_count = cycle_number
            self.last_heartbeat_at = pd.Timestamp.now(tz=ASIA_TOKYO)
            self._log("info", f"自動売買サイクル {cycle_number} を実行中")
            if not self._ensure_runtime_connectivity():
                return
            if self._daily_loss_breached():
                return
            try:
                snapshots, benchmarks, sector_frames = self._load_cycle_market_data()
            except Exception as exc:
                self._log("warning", f"市場データ取得に失敗しました。再接続を試みます: {exc}")
                recovered = self._recover_runtime_connection("市場データ取得エラー")
                if not recovered:
                    return
                snapshots, benchmarks, sector_frames = self._load_cycle_market_data()
            self._update_broker_market_snapshot(snapshots)
            if self.config.strategy.name == FxBreakoutPullbackStrategy.name:
                self._run_fx_cycle(snapshots)
                return
            for symbol, frames in snapshots.items():
                feature_set = build_multi_timeframe_feature_set(
                    symbol=symbol,
                    bars_by_timeframe=frames,
                    benchmark_bars=benchmarks,
                    sector_bars=sector_frames,
                    config=self.config,
                )
                signal_frame = self.strategy.generate_signal_frame(feature_set.entry_frame)
                if signal_frame.empty:
                    continue
                for column in ("close", "entry_atr_14", "volume"):
                    if column not in signal_frame.columns and column in feature_set.entry_frame.columns:
                        signal_frame[column] = feature_set.entry_frame[column]
                latest = signal_frame.iloc[-1]
                latest_ts = pd.Timestamp(signal_frame.index[-1])
                self.latest_market_bar_at[symbol.upper()] = latest_ts.isoformat()
                if not self._is_new_entry_bar(symbol, latest_ts):
                    continue
                session = get_session_state(pd.Timestamp(latest_ts))
                self.recent_signals.append(
                    {
                        "timestamp": str(latest_ts),
                        "symbol": symbol,
                        "signal_action": latest["signal_action"],
                        "signal_score": float(latest["signal_score"]),
                        "accepted": bool(latest["entry_signal"]),
                        "explanation_ja": str(latest["explanation_ja"]),
                        "session_label_ja": session.label_ja,
                    }
                )
                self.recent_signals = self.recent_signals[-100:]
                managed = self._ensure_managed_position(symbol, latest, latest_ts, signal_frame)
                if managed is not None:
                    protective_exit = evaluate_managed_position(
                        state=managed,
                        latest=latest,
                        timestamp=latest_ts,
                        risk=self.config.risk,
                    )
                    if protective_exit is not None:
                        if not session.is_regular_session:
                            self._log("warning", f"{symbol}: {session.label_ja} のため {protective_exit.reason_ja} を保留しました")
                        else:
                            self._handle_exit(symbol, latest, reason=protective_exit.reason_ja, quantity=protective_exit.quantity)
                        continue
                if not session.is_regular_session:
                    self._log("debug", f"{symbol}: {session.label_ja} のため新規注文を見送りました")
                    continue
                if latest["entry_signal"]:
                    self._handle_entry(symbol, latest, latest_ts, signal_frame)
                elif latest["exit_signal"]:
                    self._handle_exit(symbol, latest, reason="スコア低下または逆行シグナルで手仕舞い")
                else:
                    self._log("debug", f"{symbol}: {latest['explanation_ja']}")
        except Exception as exc:  # pragma: no cover - protective path
            self.status = AutomationStatus.ERROR
            self._log("error", f"自動売買ループでエラーが発生しました: {exc}")
            self._notify(
                enabled=self.config.automation.notify_on_errors,
                title="FXAutoTrade Lab",
                message=f"自動売買エラー: {exc}",
                subtitle=self.config.broker.mode.value,
            )
            raise

    def _load_cycle_market_data(
        self,
    ) -> tuple[
        dict[str, dict],
        dict | None,
        dict | None,
    ]:
        if self.config.broker.mode == BrokerMode.LOCAL_SIM and not self._uses_runtime_market_data():
            snapshots = self._local_replay_snapshot()
            benchmark_frames = self._load_reference_frames(self.config.watchlist.benchmark_symbols)
            sector_frames = self._load_reference_frames(self.config.watchlist.sector_symbols)
            return snapshots, benchmark_frames, sector_frames
        bundle = self.data_service.load_runtime_bundle() if self._uses_runtime_market_data() else self.data_service.load_bundle()
        benchmark_frames = None
        if self.config.watchlist.benchmark_symbols:
            benchmark_frames = bundle.benchmarks.get(self.config.watchlist.benchmark_symbols[0])
        sector_frames = None
        if self.config.watchlist.sector_symbols:
            sector_frames = bundle.sectors.get(self.config.watchlist.sector_symbols[0])
        return bundle.symbols, benchmark_frames, sector_frames

    def _load_reference_frames(self, symbols: list[str]) -> dict | None:
        if not symbols:
            return None
        return self.data_service.load_symbol_frames(symbols[0])

    def _uses_runtime_market_data(self) -> bool:
        return self.config.data.source == "gmo" or self.config.broker.mode == BrokerMode.GMO_SIM

    def _update_broker_market_snapshot(self, snapshots: dict[str, dict]) -> None:
        entry_timeframe = (
            self.config.strategy.fx_breakout_pullback.execution_timeframe
            if self.config.strategy.name == FxBreakoutPullbackStrategy.name
            else self.config.strategy.entry_timeframe
        )
        latest_prices: dict[str, object] = {}
        latest_timestamps: list[pd.Timestamp] = []
        for symbol, frames in snapshots.items():
            entry_frame = frames.get(entry_timeframe)
            if entry_frame is None or entry_frame.empty:
                continue
            latest_row = entry_frame.iloc[-1]
            latest_ts = pd.Timestamp(entry_frame.index[-1])
            latest_price = self._coerce_float(latest_row.get("close"))
            self.latest_market_bar_at[symbol.upper()] = latest_ts.isoformat()
            if self.config.strategy.name == FxBreakoutPullbackStrategy.name and {"bid_close", "ask_close"}.issubset(entry_frame.columns):
                bid_price = self._coerce_float(latest_row.get("bid_close"))
                ask_price = self._coerce_float(latest_row.get("ask_close"))
                if bid_price > 0 and ask_price > 0:
                    latest_prices[symbol.upper()] = {
                        "bid": bid_price,
                        "ask": ask_price,
                        "mid": latest_price,
                    }
                    latest_timestamps.append(latest_ts)
            elif latest_price > 0:
                latest_prices[symbol.upper()] = latest_price
                latest_timestamps.append(latest_ts)
        if not latest_prices:
            return
        latest_timestamp = max(latest_timestamps) if latest_timestamps else None
        self.broker.update_market_data(latest_prices, latest_timestamp)
        if self.config.broker.mode == BrokerMode.LOCAL_SIM:
            self._sync_broker_state()

    def _is_new_entry_bar(self, symbol: str, latest_ts: pd.Timestamp) -> bool:
        key = symbol.upper()
        previous = self.last_processed_entry_bar_at.get(key)
        if previous is not None and latest_ts <= previous:
            return False
        self.last_processed_entry_bar_at[key] = latest_ts
        return True

    def _local_replay_snapshot(self) -> dict[str, dict]:
        snapshots = {}
        for symbol in self.config.watchlist.symbols:
            frames = self.data_service.load_symbol_frames(symbol)
            entry_frame = frames[self.config.strategy.entry_timeframe]
            pointer = min(self.local_indices[symbol], max(1, len(entry_frame) - 1))
            cutoff = entry_frame.index[pointer - 1]
            clipped = {
                timeframe: frame.loc[frame.index <= cutoff].copy()
                for timeframe, frame in frames.items()
            }
            self.local_indices[symbol] = min(pointer + 1, len(entry_frame))
            snapshots[symbol] = clipped
        return snapshots

    def _fx_quote_price(self, row: pd.Series, side: str, field: str) -> float:
        raw = self._coerce_float(row.get(f"{side}_{field}") or row.get(f"{side}_{field}".replace("__", "_")))
        mid = self._coerce_float(row.get(f"mid_{field}") or row.get(field))
        spread = self._coerce_float(row.get(f"spread_{field}") or row.get("spread_close"))
        multiplier = max(self.config.strategy.fx_breakout_pullback.spread_stress_multiplier, 0.0)
        if raw > 0 and abs(multiplier - 1.0) < 1e-9:
            return raw
        stressed_spread = spread * multiplier
        if mid > 0:
            return mid + stressed_spread / 2.0 if side == "ask" else mid - stressed_spread / 2.0
        return raw

    @staticmethod
    def _fx_delayed_execute_at(frame: pd.DataFrame, timestamp: pd.Timestamp, delay_bars: int) -> pd.Timestamp | None:
        try:
            location = int(frame.index.get_loc(timestamp))
        except KeyError:
            return None
        target_index = location + delay_bars + 1
        if target_index >= len(frame.index):
            return None
        return pd.Timestamp(frame.index[target_index])

    @staticmethod
    def _fx_position_side(value: object) -> str:
        return "short" if str(value or "").strip().lower() == "short" else "long"

    @staticmethod
    def _fx_order_side(value: object, default: OrderSide) -> OrderSide:
        candidate = str(value or "").strip().lower()
        if candidate == OrderSide.SELL.value:
            return OrderSide.SELL
        if candidate == OrderSide.BUY.value:
            return OrderSide.BUY
        return default

    def _fx_breakout_level(self, latest: pd.Series, position_side: str) -> float:
        breakout_column = "breakout_level_15m" if position_side == "long" else "breakout_short_level_15m"
        return self._coerce_float(latest.get(breakout_column) or latest.get("close"))

    def _fx_jpy_cross_limit_reached(self, symbol: str) -> bool:
        _, quote = split_fx_symbol(symbol)
        if quote != "JPY":
            return False
        active_symbols = {
            str(position.get("symbol", "")).upper()
            for position in self.synced_positions
            if str(position.get("qty", "0")) not in {"0", "0.0", ""}
        }
        active_symbols.update(self.open_symbols)
        active_symbols.update(
            symbol_name
            for symbol_name, state in self.fx_position_state.items()
            if int(float(state.get("quantity", 0) or 0)) > 0
        )
        open_jpy_crosses = sum(1 for current_symbol in active_symbols if split_fx_symbol(current_symbol)[1] == "JPY")
        return open_jpy_crosses >= self.config.strategy.fx_breakout_pullback.max_jpy_cross_positions

    def _record_fx_signal(self, symbol: str, latest: pd.Series, latest_ts: pd.Timestamp) -> None:
        session = get_session_state(pd.Timestamp(latest_ts))
        self.recent_signals.append(
            {
                "timestamp": str(latest_ts),
                "symbol": symbol,
                "signal_action": latest["signal_action"],
                "signal_score": float(latest.get("signal_score", 0.0) or 0.0),
                "accepted": bool(latest.get("entry_signal", False)),
                "explanation_ja": str(latest.get("explanation_ja", "")),
                "session_label_ja": session.label_ja,
            }
        )
        self.recent_signals = self.recent_signals[-100:]

    def _run_fx_cycle(self, snapshots: dict[str, dict]) -> None:
        cycle_as_of: pd.Timestamp | None = None
        for frames in snapshots.values():
            execution_frame = frames.get(self.config.strategy.fx_breakout_pullback.execution_timeframe)
            if execution_frame is None or execution_frame.empty:
                continue
            latest_ts = pd.Timestamp(execution_frame.index[-1])
            cycle_as_of = latest_ts if cycle_as_of is None else max(cycle_as_of, latest_ts)
        if cycle_as_of is not None:
            self._maybe_retrain_fx_model(cycle_as_of)
        for symbol, frames in snapshots.items():
            feature_set = build_fx_feature_set(
                symbol=symbol,
                bars_by_timeframe=frames,
                config=self.config,
                runtime_mode=True,
            )
            signal_frame = self.strategy.generate_signal_frame(feature_set.execution_frame)
            if self.fx_loaded_model is not None and self.config.strategy.fx_breakout_pullback.ml_filter.enabled:
                signal_frame = apply_fx_ml_filter(signal_frame, self.fx_loaded_model, self.config, model_label="realtime")
            if signal_frame.empty:
                continue
            latest = signal_frame.iloc[-1]
            latest_ts = pd.Timestamp(signal_frame.index[-1])
            self.latest_market_bar_at[symbol.upper()] = latest_ts.isoformat()
            if not self._is_new_entry_bar(symbol, latest_ts):
                continue
            self._record_fx_signal(symbol, latest, latest_ts)
            self._execute_pending_fx_orders(symbol, signal_frame, latest, latest_ts)
            self._evaluate_fx_runtime_protection(symbol, latest, latest_ts)
            if symbol in self.open_symbols:
                self._schedule_fx_exit_if_needed(symbol, latest, latest_ts)
            elif bool(latest.get("entry_signal", False)):
                position_side = self._fx_position_side(latest.get("position_side"))
                entry_order_side = self._fx_order_side(
                    latest.get("entry_order_side"),
                    OrderSide.BUY if position_side == "long" else OrderSide.SELL,
                )
                exit_order_side = self._fx_order_side(
                    latest.get("exit_order_side"),
                    OrderSide.SELL if position_side == "long" else OrderSide.BUY,
                )
                self.fx_pending_entries[symbol.upper()] = {
                    "signal_time": latest_ts,
                    "position_side": position_side,
                    "entry_order_side": entry_order_side.value,
                    "exit_order_side": exit_order_side.value,
                    "trigger_price": self._coerce_float(latest.get("entry_trigger_price")),
                    "initial_stop_price": self._coerce_float(latest.get("initial_stop_price")),
                    "initial_risk_price": self._coerce_float(latest.get("initial_risk_price")) or 0.01,
                    "atr_at_entry": max(self._coerce_float(latest.get("breakout_atr_15m") or latest.get("atr_15m")), 0.01),
                    "breakout_level": self._fx_breakout_level(latest, position_side),
                    "reason": str(latest.get("explanation_ja", "")),
                    "score": float(latest.get("signal_score", 0.0) or 0.0),
                }

    def _execute_pending_fx_orders(
        self,
        symbol: str,
        signal_frame: pd.DataFrame,
        latest: pd.Series,
        latest_ts: pd.Timestamp,
    ) -> None:
        upper = symbol.upper()
        pending_entry = self.fx_pending_entries.get(upper)
        if pending_entry is not None:
            execute_at = self._fx_delayed_execute_at(
                signal_frame,
                pd.Timestamp(pending_entry["signal_time"]),
                self.config.strategy.fx_breakout_pullback.entry_delay_bars,
            )
            if execute_at is None or latest_ts < execute_at:
                pass
            else:
                entry_order_side = self._fx_order_side(pending_entry.get("entry_order_side"), OrderSide.BUY)
                exit_order_side = self._fx_order_side(pending_entry.get("exit_order_side"), OrderSide.SELL)
                position_side = self._fx_position_side(pending_entry.get("position_side"))
                trigger_price = float(pending_entry["trigger_price"])
                if entry_order_side == OrderSide.BUY:
                    quote_open = self._fx_quote_price(latest, "ask", "open")
                    quote_extreme = self._fx_quote_price(latest, "ask", "high")
                    trigger_hit = quote_open >= trigger_price or quote_extreme >= trigger_price
                else:
                    quote_open = self._fx_quote_price(latest, "bid", "open")
                    quote_extreme = self._fx_quote_price(latest, "bid", "low")
                    trigger_hit = quote_open <= trigger_price or quote_extreme <= trigger_price
                if bool(latest.get("entry_context_ok", False)) and trigger_hit:
                    quantity, sizing_message = self._entry_quantity_fx(symbol, latest, entry_order_side=entry_order_side)
                    if quantity > 0 and not self._has_pending_order(upper):
                        order = self.broker.submit_market_order(
                            symbol=upper,
                            qty=quantity,
                            side=entry_order_side,
                            reason=str(pending_entry["reason"]),
                        )
                        fill_price = self._coerce_float(order.get("filled_avg_price") or order.get("price"))
                        self.recent_orders.append(order)
                        self.recent_orders = self.recent_orders[-100:]
                        self.open_symbols.add(upper)
                        self.last_actions[upper] = entry_order_side.value
                        self.fx_position_state[upper] = {
                            "position_side": position_side,
                            "entry_order_side": entry_order_side.value,
                            "exit_order_side": exit_order_side.value,
                            "quantity": quantity,
                            "initial_quantity": quantity,
                            "entry_price": fill_price,
                            "entry_time": latest_ts,
                            "signal_time": pd.Timestamp(pending_entry["signal_time"]),
                            "initial_stop_price": float(pending_entry["initial_stop_price"]),
                            "trailing_stop_price": float(pending_entry["initial_stop_price"]),
                            "highest_bid": self._fx_quote_price(latest, "bid", "close"),
                            "lowest_ask": self._fx_quote_price(latest, "ask", "close"),
                            "initial_risk_price": float(pending_entry["initial_risk_price"]),
                            "atr_at_entry": float(pending_entry["atr_at_entry"]),
                            "breakout_level": float(pending_entry["breakout_level"]),
                            "partial_exit_done": False,
                        }
                        self._log("info", f"{upper}: FX エントリーを実行しました ({sizing_message})")
                        if self.config.automation.sync_broker_state_each_cycle:
                            self._sync_broker_state()
                self.fx_pending_entries.pop(upper, None)
        pending_exit = self.fx_pending_exits.get(upper)
        if pending_exit is not None and latest_ts > pd.Timestamp(pending_exit["signal_time"]):
            exit_quantity = int(pending_exit["quantity"])
            exit_order_side = self._fx_order_side(pending_exit.get("order_side"), OrderSide.SELL)
            if exit_quantity > 0 and upper in self.open_symbols and not self._has_pending_order(upper):
                order = self.broker.submit_market_order(
                    symbol=upper,
                    qty=exit_quantity,
                    side=exit_order_side,
                    reason=str(pending_exit["reason"]),
                )
                self.recent_orders.append(order)
                self.recent_orders = self.recent_orders[-100:]
                self.last_actions[upper] = exit_order_side.value
                state = self.fx_position_state.get(upper)
                if state is not None:
                    if pending_exit["kind"] == "partial_exit":
                        state["partial_exit_done"] = True
                        state["quantity"] = max(0, int(state["quantity"]) - exit_quantity)
                    else:
                        state["quantity"] = 0
                if self.config.automation.sync_broker_state_each_cycle:
                    self._sync_broker_state()
            self.fx_pending_exits.pop(upper, None)

    def _evaluate_fx_runtime_protection(self, symbol: str, latest: pd.Series, latest_ts: pd.Timestamp) -> None:
        upper = symbol.upper()
        state = self.fx_position_state.get(upper)
        if state is None or upper not in self.open_symbols:
            return
        current_atr = max(self._coerce_float(latest.get("atr_15m")), float(state["atr_at_entry"]))
        position_side = self._fx_position_side(state.get("position_side"))
        exit_order_side = self._fx_order_side(
            state.get("exit_order_side"),
            OrderSide.SELL if position_side == "long" else OrderSide.BUY,
        )
        if position_side == "long":
            state["highest_bid"] = max(float(state["highest_bid"]), self._fx_quote_price(latest, "bid", "high"))
            trailing_candidate = float(state["highest_bid"]) - self.config.strategy.fx_breakout_pullback.atr_trailing_mult * current_atr
            state["trailing_stop_price"] = max(float(state["trailing_stop_price"]), trailing_candidate)
            active_stop = max(float(state["initial_stop_price"]), float(state["trailing_stop_price"]))
            protective_open = self._fx_quote_price(latest, "bid", "open")
            protective_extreme = self._fx_quote_price(latest, "bid", "low")
            stop_hit = protective_open <= active_stop or protective_extreme <= active_stop
        else:
            state["lowest_ask"] = min(float(state["lowest_ask"]), self._fx_quote_price(latest, "ask", "low"))
            trailing_candidate = float(state["lowest_ask"]) + self.config.strategy.fx_breakout_pullback.atr_trailing_mult * current_atr
            state["trailing_stop_price"] = min(float(state["trailing_stop_price"]), trailing_candidate)
            active_stop = min(float(state["initial_stop_price"]), float(state["trailing_stop_price"]))
            protective_open = self._fx_quote_price(latest, "ask", "open")
            protective_extreme = self._fx_quote_price(latest, "ask", "high")
            stop_hit = protective_open >= active_stop or protective_extreme >= active_stop
        if stop_hit:
            order = self.broker.submit_market_order(
                symbol=upper,
                qty=int(state["quantity"]),
                side=exit_order_side,
                reason="protective_stop",
            )
            self.recent_orders.append(order)
            self.recent_orders = self.recent_orders[-100:]
            self.open_symbols.discard(upper)
            self.fx_position_state.pop(upper, None)
            self.fx_pending_exits.pop(upper, None)
            self.last_actions[upper] = exit_order_side.value
            if self.config.automation.sync_broker_state_each_cycle:
                self._sync_broker_state()
            self._log("info", f"{upper}: FX 防御ストップで決済しました")

    def _schedule_fx_exit_if_needed(self, symbol: str, latest: pd.Series, latest_ts: pd.Timestamp) -> None:
        upper = symbol.upper()
        state = self.fx_position_state.get(upper)
        if state is None or upper in self.fx_pending_exits:
            return
        if bool(latest.get("exit_signal", False)):
            self.fx_pending_exits[upper] = {
                "signal_time": latest_ts,
                "quantity": int(state["quantity"]),
                "order_side": state.get("exit_order_side", OrderSide.SELL.value),
                "reason": "1時間足EMAクロスで全決済",
                "kind": "full_exit",
            }
        elif bool(latest.get("partial_exit_signal", False)) and not bool(state.get("partial_exit_done", False)):
            partial_quantity = max(
                1,
                int(round(int(state["initial_quantity"]) * self.config.strategy.fx_breakout_pullback.partial_exit_fraction)),
            )
            partial_quantity = min(partial_quantity, max(int(state["quantity"]) - 1, 0))
            if partial_quantity > 0:
                self.fx_pending_exits[upper] = {
                    "signal_time": latest_ts,
                    "quantity": partial_quantity,
                    "order_side": state.get("exit_order_side", OrderSide.SELL.value),
                    "reason": "1時間足トレンド崩れで一部手仕舞い",
                    "kind": "partial_exit",
                }

    def _entry_quantity_fx(self, symbol: str, latest: pd.Series, *, entry_order_side: OrderSide) -> tuple[int, str]:
        equity = self._coerce_float(self.account_summary.get("equity") or self.account_summary.get("portfolio_value"))
        cash = self._coerce_float(self.account_summary.get("cash") or self.account_summary.get("buying_power"))
        price = self._fx_quote_price(latest, "ask" if entry_order_side == OrderSide.BUY else "bid", "close")
        atr_value = max(self._coerce_float(latest.get("breakout_atr_15m") or latest.get("atr_15m")), 0.01)
        sizing = self.risk_manager.size_automation_position(
            cash=cash,
            equity=equity or cash,
            price=price,
            atr_value=atr_value,
        )
        if not self.risk_manager.can_open_position(
            open_positions=len(self.open_symbols),
            current_exposure_ratio=self._current_exposure_ratio(),
            quantity=sizing.quantity,
        ):
            return 0, "リスク制約により新規エントリーを見送りました"
        if self._fx_jpy_cross_limit_reached(symbol):
            return 0, "JPY クロス保有上限に達しているため新規エントリーを見送りました"
        if sizing.quantity <= 0:
            return 0, "数量を確保できないため見送りました"
        return sizing.quantity, f"{sizing.quantity:,} 通貨"

    def _positions_with_management(self) -> list[dict[str, object]]:
        enriched: list[dict[str, object]] = []
        for position in self.synced_positions:
            record = dict(position)
            symbol = str(record.get("symbol", "")).upper()
            if symbol in self.fx_position_state:
                fx_state = self.fx_position_state[symbol]
                position_side = self._fx_position_side(fx_state.get("position_side"))
                if position_side == "short":
                    active_stop = min(float(fx_state["initial_stop_price"]), float(fx_state["trailing_stop_price"]))
                    reference_price = float(fx_state.get("lowest_ask", fx_state["entry_price"]))
                else:
                    active_stop = max(float(fx_state["initial_stop_price"]), float(fx_state["trailing_stop_price"]))
                    reference_price = float(fx_state.get("highest_bid", fx_state["entry_price"]))
                record["managed_initial_stop_price"] = f"{float(fx_state['initial_stop_price']):.4f}"
                record["managed_stop_price"] = f"{float(fx_state['initial_stop_price']):.4f}"
                record["managed_trailing_stop_price"] = f"{float(fx_state['trailing_stop_price']):.4f}"
                record["managed_active_stop_price"] = f"{active_stop:.4f}"
                record["managed_partial_target_price"] = ""
                record["managed_partial_reference_price"] = f"{reference_price:.4f}"
                record["managed_reference_bar_at"] = str(self.latest_market_bar_at.get(symbol, ""))
                record["managed_next_trailing_price"] = f"{float(fx_state['trailing_stop_price']):.4f}"
                record["managed_trailing_multiple"] = f"{self.config.strategy.fx_breakout_pullback.atr_trailing_mult:.2f}"
                record["managed_bars_held"] = (
                    max((pd.Timestamp.now(tz=ASIA_TOKYO) - pd.Timestamp(fx_state["entry_time"])).seconds // 60, 0)
                )
                record["managed_partial_taken"] = bool(fx_state.get("partial_exit_done", False))
                record["managed_break_even_armed"] = False
                record["side"] = position_side
                enriched.append(record)
                continue
            managed = self.managed_positions.get(symbol)
            if managed is not None:
                record["managed_initial_stop_price"] = f"{managed.initial_stop_price:.4f}"
                record["managed_stop_price"] = f"{managed.stop_price:.4f}"
                record["managed_trailing_stop_price"] = f"{managed.trailing_stop_price:.4f}"
                record["managed_active_stop_price"] = f"{managed.active_stop_price:.4f}"
                record["managed_partial_target_price"] = f"{managed.partial_target_price:.4f}"
                record["managed_partial_reference_price"] = (
                    f"{managed.last_reference_high_price:.4f}"
                    if managed.last_reference_high_price is not None
                    else ""
                )
                record["managed_reference_bar_at"] = (
                    managed.last_reference_timestamp.isoformat()
                    if managed.last_reference_timestamp is not None
                    else ""
                )
                record["managed_next_trailing_price"] = f"{managed.next_trailing_price:.4f}"
                record["managed_trailing_multiple"] = f"{managed.current_trailing_multiple:.2f}"
                record["managed_bars_held"] = managed.bars_held
                record["managed_partial_taken"] = managed.partial_taken
                record["managed_break_even_armed"] = managed.break_even_armed
            enriched.append(record)
        return enriched

    def snapshot(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "mode": self.config.broker.mode.value,
            "cycle_count": self.cycle_count,
            "heartbeat": self.last_heartbeat_at.isoformat() if self.last_heartbeat_at is not None else "",
            "open_symbols": sorted(self.open_symbols),
            "positions": self._positions_with_management(),
            "recent_orders": list(self.recent_orders[-50:]),
            "recent_fills": list(self.recent_fills[-50:]),
            "recent_signals": list(self.recent_signals[-50:]),
            "recent_events": [
                {
                    "timestamp": event.timestamp.isoformat(),
                    "level": event.level,
                    "message_ja": event.message_ja,
                }
                for event in self.logs[-100:]
            ],
            "last_actions": dict(self.last_actions),
            "account_summary": dict(self.account_summary),
            "kill_switch_reason": self.kill_switch_reason,
            "connection_state": self.connection_state,
            "stream_state": dict(self.stream_state),
            "reconnect_attempts": self.reconnect_attempts,
            "last_reconnect_at": self.last_reconnect_at.isoformat() if self.last_reconnect_at is not None else "",
            "data_source": self.config.data.source,
            "entry_timeframe": (
                self.config.strategy.fx_breakout_pullback.execution_timeframe.value
                if self.config.strategy.name == FxBreakoutPullbackStrategy.name
                else self.config.strategy.entry_timeframe.value
            ),
            "order_size_mode": self.config.risk.order_size_mode.value,
            "latest_market_bar_at": dict(self.latest_market_bar_at),
            "fx_ml_status": {
                "enabled": bool(self.config.strategy.fx_breakout_pullback.ml_filter.enabled),
                "model_loaded": self.fx_loaded_model is not None,
                "last_retrain_at": self.fx_last_retrain_at.isoformat() if self.fx_last_retrain_at is not None else "",
                "next_retrain_at": self.fx_next_retrain_at.isoformat() if self.fx_next_retrain_at is not None else "",
                "last_retrain_summary": dict(self.fx_last_retrain_summary),
            },
        }

    def _handle_entry(self, symbol: str, latest: pd.Series, latest_ts: pd.Timestamp, signal_frame: pd.DataFrame) -> None:
        if self.kill_switch_reason:
            self._log("warning", f"{symbol}: キルスイッチ中のため新規買いを停止しました")
            return
        if symbol in self.open_symbols:
            self._log("debug", f"{symbol}: 既存ポジションがあるため重複買いを防止しました")
            return
        if self._has_pending_order(symbol):
            self._log("debug", f"{symbol}: 既存の未完了注文があるため買いを見送りました")
            return
        if not self.duplicate_guard.add(symbol):
            self._log("debug", f"{symbol}: 重複注文防止により買いを見送りました")
            return
        try:
            if self.config.automation.dry_decision_mode:
                self._log("info", f"{symbol}: ドライモードのため買い判断のみ記録しました")
                self.last_actions[symbol] = OrderSide.BUY.value
                return
            quantity, sizing_message = self._entry_quantity(latest)
            if quantity <= 0:
                self._log("info", f"{symbol}: {sizing_message}")
                return
            if not self.risk_manager.can_open_position(
                open_positions=len(self.open_symbols),
                current_exposure_ratio=self._current_exposure_ratio(),
                quantity=quantity,
            ):
                self._log("info", f"{symbol}: リスク制約により新規買いを見送りました")
                return
            atr_value = self._coerce_float(latest.get("entry_atr_14")) or 1.0
            swing_low = recent_swing_low(signal_frame, self.config.risk.swing_stop_lookback_bars)
            order = self.broker.submit_market_order(
                symbol=symbol,
                qty=quantity,
                side=OrderSide.BUY,
                reason=str(latest["explanation_ja"]),
            )
            fill_price = self._coerce_float(order.get("filled_avg_price") or order.get("price"))
            self.pending_entry_contexts[symbol.upper()] = {
                "entry_time": pd.Timestamp(latest_ts),
                "entry_price": fill_price or self._coerce_float(latest.get("close")),
                "atr_value": atr_value,
                "swing_low": swing_low,
            }
            self.open_symbols.add(symbol)
            self.last_actions[symbol] = OrderSide.BUY.value
            self.recent_orders.append(order)
            self.recent_orders = self.recent_orders[-100:]
            managed = self._ensure_managed_position(
                symbol,
                latest,
                latest_ts,
                signal_frame,
                quantity_hint=quantity,
                entry_price_hint=fill_price if fill_price > 0 else None,
            )
            stop_note = ""
            if managed is not None:
                stop_note = (
                    f" / 初期ストップ {managed.initial_stop_price:.2f}"
                    f" / 一部利確 {managed.partial_target_price:.2f}"
                )
            self._log("info", f"{symbol}: 自動買い判断を送信しました ({sizing_message}{stop_note})")
            self._notify(
                enabled=self.config.automation.notify_on_orders,
                title="FXAutoTrade Lab",
                message=f"{symbol} を {quantity:,} 通貨で自動買いしました。",
                subtitle=self.config.broker.mode.value,
            )
            if self.config.automation.sync_broker_state_each_cycle:
                self._sync_broker_state()
        finally:
            self.duplicate_guard.remove(symbol)

    def _handle_exit(self, symbol: str, latest: pd.Series, *, reason: str, quantity: int | None = None) -> None:
        if symbol not in self.open_symbols:
            self._log("debug", f"{symbol}: 保有がないため売りを送信しませんでした")
            return
        if self._has_pending_order(symbol):
            self._log("debug", f"{symbol}: 既存の未完了注文があるため売りを見送りました")
            return
        if not self.duplicate_guard.add(symbol):
            self._log("debug", f"{symbol}: 重複注文防止により売りを見送りました")
            return
        try:
            if self.config.automation.dry_decision_mode:
                self._log("info", f"{symbol}: ドライモードのため手仕舞い判断のみ記録しました")
                self.last_actions[symbol] = OrderSide.SELL.value
                return
            exit_quantity = quantity or self._position_quantity(symbol)
            if exit_quantity <= 0:
                self._log("warning", f"{symbol}: 保有数量を特定できないため売却できませんでした")
                return
            order = self.broker.submit_market_order(
                symbol=symbol,
                qty=exit_quantity,
                side=OrderSide.SELL,
                reason=reason,
            )
            self.last_actions[symbol] = OrderSide.SELL.value
            self.recent_orders.append(order)
            self.recent_orders = self.recent_orders[-100:]
            managed = self.managed_positions.get(symbol.upper())
            current_qty = self._position_quantity(symbol) or (managed.quantity if managed is not None else exit_quantity)
            if exit_quantity >= current_qty > 0:
                self.open_symbols.discard(symbol)
            if managed is not None and exit_quantity < managed.quantity:
                managed.partial_taken = True
                managed.quantity -= exit_quantity
                managed.stop_price = max(managed.stop_price, managed.entry_price)
            elif managed is not None and exit_quantity >= managed.quantity:
                self.managed_positions.pop(symbol.upper(), None)
                self.pending_entry_contexts.pop(symbol.upper(), None)
            self._log("info", f"{symbol}: 自動売り判断を送信しました ({exit_quantity:,} 通貨 / {reason})")
            self._notify(
                enabled=self.config.automation.notify_on_orders,
                title="FXAutoTrade Lab",
                message=f"{symbol} を {exit_quantity:,} 通貨で自動売却しました。",
                subtitle=self.config.broker.mode.value,
            )
            if self.config.automation.sync_broker_state_each_cycle:
                self._sync_broker_state()
        finally:
            self.duplicate_guard.remove(symbol)

    def manual_close_position(self, symbol: str, quantity: int | None = None) -> dict[str, object]:
        upper = symbol.strip().upper()
        self._sync_broker_state()
        qty = quantity or self._position_quantity(upper)
        if qty <= 0:
            raise RuntimeError(f"{upper} の保有数量を特定できませんでした。")
        order = self.broker.submit_market_order(
            symbol=upper,
            qty=qty,
            side=OrderSide.SELL,
            reason="手動決済",
        )
        self.recent_orders.append(order)
        self.recent_orders = self.recent_orders[-100:]
        self.open_symbols.discard(upper)
        self.last_actions[upper] = OrderSide.SELL.value
        self.managed_positions.pop(upper, None)
        self.pending_entry_contexts.pop(upper, None)
        self.fx_position_state.pop(upper, None)
        self.fx_pending_entries.pop(upper, None)
        self.fx_pending_exits.pop(upper, None)
        self._sync_broker_state()
        self._log("info", f"{upper}: 手動決済を送信しました ({qty:,} 通貨)")
        return order

    def manual_close_all_positions(self) -> dict[str, object]:
        self._sync_broker_state()
        result = self.broker.close_all_positions()
        self._sync_broker_state()
        self.open_symbols.clear()
        self.managed_positions.clear()
        self.pending_entry_contexts.clear()
        self.fx_position_state.clear()
        self.fx_pending_entries.clear()
        self.fx_pending_exits.clear()
        self._log("info", "全ポジションの手動決済を送信しました")
        return result

    def _refresh_account_summary(self) -> bool:
        try:
            self.account_summary = self.broker.get_account_summary()
            return True
        except Exception as exc:  # pragma: no cover - network/config path
            self.account_summary = {"status": "unavailable", "message": str(exc)}
            self._log("warning", f"口座情報の取得に失敗しました: {exc}")
            return False

    def _sync_broker_state(self) -> bool:
        try:
            state = self.broker.sync_runtime_state(order_limit=self.config.automation.reconcile_orders_limit)
        except Exception as exc:  # pragma: no cover - network/config path
            self._log("warning", f"ブローカー状態の再同期に失敗しました: {exc}")
            return False
        self.account_summary = dict(state.get("account_summary", self.account_summary))
        self.synced_positions = list(state.get("positions", []))
        self.recent_orders = list(state.get("orders", []))[-100:]
        self.recent_fills = list(state.get("fills", []))[-100:]
        self.open_symbols = {
            str(position.get("symbol", "")).upper()
            for position in self.synced_positions
            if str(position.get("qty", "0")) not in {"0", "0.0", ""}
        }
        self._reconcile_fx_positions()
        self._reconcile_managed_positions()
        return True

    def _ensure_runtime_connectivity(self) -> bool:
        account_ok = self._refresh_account_summary()
        sync_ok = True
        if self.config.automation.sync_broker_state_each_cycle:
            sync_ok = self._sync_broker_state()
        if account_ok and sync_ok:
            self.connection_state = "connected"
            return True
        issues: list[str] = []
        if not account_ok:
            issues.append("シミュレーション状態")
        if not sync_ok:
            issues.append("内部状態再同期")
        return self._recover_runtime_connection(" / ".join(issues) if issues else "接続状態")

    def _recover_runtime_connection(self, reason: str) -> bool:
        max_attempts = max(1, self.config.automation.reconnect_max_attempts)
        self.connection_state = "reconnecting"
        last_error = ""
        for attempt in range(1, max_attempts + 1):
            self.reconnect_attempts += 1
            self.last_reconnect_at = pd.Timestamp.now(tz=ASIA_TOKYO)
            self._log("warning", f"{reason} のため再接続を試行します ({attempt}/{max_attempts})")
            self.data_service = MarketDataService(self.config, self.env)
            account_ok = self._refresh_account_summary()
            sync_ok = True
            if self.config.automation.sync_broker_state_on_start or self.config.automation.sync_broker_state_each_cycle:
                sync_ok = self._sync_broker_state()
            if account_ok and sync_ok:
                self.connection_state = "connected"
                self._log("info", "接続を復旧しました (connected)")
                self._notify(
                    enabled=self.config.automation.notify_on_reconnect,
                    title="FXAutoTrade Lab",
                    message="接続を復旧しました。",
                    subtitle="connected",
                )
                return True
            last_error = self.account_summary.get("message", "") or last_error or "再接続失敗"
            if attempt < max_attempts:
                self.stop_event.wait(timeout=self.config.automation.reconnect_seconds)
                if self.stop_event.is_set():
                    break
        self.connection_state = "degraded"
        self._log("error", f"再接続に失敗しました: {reason} / {last_error}")
        self._notify(
            enabled=self.config.automation.notify_on_errors,
            title="FXAutoTrade Lab",
            message="再接続に失敗しました。ポーリング/発注状態を確認してください。",
            subtitle=reason,
        )
        return False

    def _connect_streaming_if_needed(self, force: bool = False) -> bool:
        _ = force
        self.stream_state = {"enabled": False, "connected": False, "healthy": False}
        return True

    def _on_stream_bar(self, payload: dict[str, object]) -> None:
        _ = payload
        self.stream_state = {"enabled": False, "connected": False, "healthy": False}

    def _on_trade_update(self, payload: dict[str, object]) -> None:
        if self.stream_client is None:
            return
        event = str(payload.get("event", "")).lower()
        order_id = str(payload.get("order_id") or payload.get("client_order_id") or "")
        symbol = str(payload.get("symbol", "")).upper()
        side = str(payload.get("side", "")).lower()
        status = str(payload.get("status", "")).lower()
        order_record = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "status": status or event,
            "qty": payload.get("qty", ""),
            "filled_qty": payload.get("filled_qty", ""),
            "filled_avg_price": payload.get("filled_avg_price", ""),
            "event": event,
            "submitted_at": payload.get("submitted_at", ""),
            "filled_at": payload.get("filled_at", ""),
        }
        self._upsert_recent_order(order_record)
        if event in {"fill", "partial_fill", "partial-fill"} or status in {"filled", "partially_filled"}:
            filled_qty = int(self._coerce_float(payload.get("filled_qty") or payload.get("qty")))
            fill_id = order_id or f"{symbol}-{event}-{len(self.recent_fills) + 1}"
            self.recent_fills.append(
                {
                    "fill_id": fill_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": side,
                    "qty": str(filled_qty),
                    "price": payload.get("filled_avg_price", ""),
                    "event": event,
                    "filled_at": payload.get("filled_at", ""),
                }
            )
            self.recent_fills = self.recent_fills[-100:]
            if side == OrderSide.BUY.value:
                self.open_symbols.add(symbol)
            elif side == OrderSide.SELL.value:
                managed = self.managed_positions.get(symbol)
                if managed is not None and filled_qty > 0:
                    if filled_qty >= managed.quantity:
                        self.managed_positions.pop(symbol, None)
                        self.pending_entry_contexts.pop(symbol, None)
                        self.open_symbols.discard(symbol)
                    else:
                        managed.quantity -= filled_qty
                        managed.partial_taken = True
        self.stream_state = {"enabled": False, "connected": False, "healthy": False}

    def _upsert_recent_order(self, payload: dict[str, object]) -> None:
        order_id = str(payload.get("order_id", ""))
        if order_id:
            for index, record in enumerate(self.recent_orders):
                existing_id = str(record.get("order_id") or record.get("id") or "")
                if existing_id and existing_id == order_id:
                    merged = dict(record)
                    merged.update(payload)
                    self.recent_orders[index] = merged
                    self.recent_orders = self.recent_orders[-100:]
                    return
        self.recent_orders.append(payload)
        self.recent_orders = self.recent_orders[-100:]

    def _daily_loss_breached(self) -> bool:
        if self.kill_switch_reason:
            return True
        current_equity = self._coerce_float(self.account_summary.get("equity"))
        last_equity = self._coerce_float(self.account_summary.get("last_equity"))
        daily_pl = self._coerce_float(self.account_summary.get("daily_pl"))
        if daily_pl == 0.0 and current_equity and self.session_start_equity is not None:
            daily_pl = current_equity - self.session_start_equity
        daily_loss = max(0.0, -daily_pl)
        reference_equity = last_equity or self.session_start_equity or current_equity or 0.0
        pct_loss = daily_loss / reference_equity if reference_equity else 0.0
        amount_hit = self.config.risk.max_daily_loss_amount > 0 and daily_loss >= self.config.risk.max_daily_loss_amount
        pct_hit = self.config.risk.max_daily_loss_pct > 0 and pct_loss >= self.config.risk.max_daily_loss_pct
        if amount_hit or pct_hit:
            reason = (
                f"日次損失制限に到達しました。損失額={daily_loss:.2f}, "
                f"損失率={pct_loss:.2%}, 許容額={self.config.risk.max_daily_loss_amount:.2f}, "
                f"許容率={self.config.risk.max_daily_loss_pct:.2%}"
            )
            self._trigger_kill_switch(reason)
            return True
        return False

    def _trigger_kill_switch(self, reason: str) -> None:
        if self.kill_switch_reason:
            return
        self.kill_switch_reason = reason
        self.status = AutomationStatus.STOPPING
        self._log("warning", reason)
        self._notify(
            enabled=self.config.automation.notify_on_risk_events,
            title="FXAutoTrade Lab",
            message="日次損失制限に到達したため自動売買を停止します。",
            subtitle=self.config.broker.mode.value,
        )
        try:
            cancelled = self.broker.cancel_all_orders()
            self._log("warning", f"未完了注文をキャンセルしました: {cancelled}")
        except Exception as exc:  # pragma: no cover - network/config path
            self._log("error", f"未完了注文のキャンセルに失敗しました: {exc}")
        if self.config.automation.close_positions_on_kill_switch:
            try:
                closed = self.broker.close_all_positions()
                self._log("warning", f"ポジションを強制クローズしました: {closed}")
                self.open_symbols.clear()
                self.synced_positions = []
                self.managed_positions.clear()
                self.pending_entry_contexts.clear()
            except Exception as exc:  # pragma: no cover - network/config path
                self._log("error", f"ポジションの強制クローズに失敗しました: {exc}")
        self.stop_event.set()

    def _has_pending_order(self, symbol: str) -> bool:
        upper = symbol.upper()
        for order in self.recent_orders[-self.config.automation.reconcile_orders_limit :]:
            if str(order.get("symbol", "")).upper() != upper:
                continue
            status = str(order.get("status", "")).lower()
            if status and "filled" not in status and status not in TERMINAL_ORDER_STATUSES:
                return True
        return False

    def _position_quantity(self, symbol: str) -> int:
        upper = symbol.upper()
        for position in self.synced_positions:
            if str(position.get("symbol", "")).upper() != upper:
                continue
            try:
                return int(float(position.get("qty", "0") or 0))
            except (TypeError, ValueError):
                return 0
        return 0

    def _position_entry_price(self, symbol: str) -> float:
        upper = symbol.upper()
        for position in self.synced_positions:
            if str(position.get("symbol", "")).upper() != upper:
                continue
            return self._coerce_float(position.get("avg_entry_price") or position.get("current_price"))
        return 0.0

    def _reconcile_managed_positions(self) -> None:
        active_symbols = {str(position.get("symbol", "")).upper() for position in self.synced_positions}
        for symbol in list(self.managed_positions):
            if symbol not in active_symbols:
                self.managed_positions.pop(symbol, None)
                self.pending_entry_contexts.pop(symbol, None)
        for position in self.synced_positions:
            symbol = str(position.get("symbol", "")).upper()
            qty = self._position_quantity(symbol)
            if qty <= 0:
                self.managed_positions.pop(symbol, None)
                continue
            state = self.managed_positions.get(symbol)
            if state is None:
                continue
            if qty < state.quantity:
                state.partial_taken = True
            state.quantity = qty
            state.initial_quantity = max(state.initial_quantity, qty)
            avg_entry = self._position_entry_price(symbol)
            if avg_entry > 0 and state.entry_price <= 0:
                state.entry_price = avg_entry

    def _reconcile_fx_positions(self) -> None:
        active_symbols = {str(position.get("symbol", "")).upper() for position in self.synced_positions}
        for symbol in list(self.fx_position_state):
            if symbol not in active_symbols:
                self.fx_position_state.pop(symbol, None)
                self.fx_pending_exits.pop(symbol, None)
        for position in self.synced_positions:
            symbol = str(position.get("symbol", "")).upper()
            if symbol not in self.fx_position_state:
                continue
            qty = self._position_quantity(symbol)
            if qty <= 0:
                self.fx_position_state.pop(symbol, None)
                continue
            self.fx_position_state[symbol]["quantity"] = qty
            self.fx_position_state[symbol]["position_side"] = self._fx_position_side(position.get("side"))

    def _ensure_managed_position(
        self,
        symbol: str,
        latest: pd.Series,
        latest_ts: pd.Timestamp,
        signal_frame: pd.DataFrame,
        *,
        quantity_hint: int | None = None,
        entry_price_hint: float | None = None,
    ) -> ManagedPositionState | None:
        upper = symbol.upper()
        quantity = quantity_hint if quantity_hint is not None else self._position_quantity(upper)
        if quantity <= 0:
            self.managed_positions.pop(upper, None)
            return None
        existing = self.managed_positions.get(upper)
        if existing is not None:
            existing.quantity = quantity
            existing.initial_quantity = max(existing.initial_quantity, quantity)
            return existing
        context = self.pending_entry_contexts.get(upper, {})
        entry_price = entry_price_hint or self._position_entry_price(upper) or self._coerce_float(context.get("entry_price")) or self._coerce_float(latest.get("close"))
        atr_value = self._coerce_float(context.get("atr_value")) or self._coerce_float(latest.get("entry_atr_14")) or 1.0
        swing_low = context.get("swing_low")
        if swing_low is None:
            swing_low = recent_swing_low(signal_frame, self.config.risk.swing_stop_lookback_bars)
        state = build_managed_position(
            symbol=upper,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=pd.Timestamp(context.get("entry_time") or latest_ts),
            atr_value=atr_value,
            risk=self.config.risk,
            swing_low=swing_low,
        )
        self.managed_positions[upper] = state
        return state

    def _current_exposure_ratio(self) -> float:
        equity = self._coerce_float(self.account_summary.get("equity") or self.account_summary.get("portfolio_value"))
        if equity <= 0:
            return 0.0
        exposure = 0.0
        for position in self.synced_positions:
            market_value = self._coerce_float(position.get("market_value"))
            if abs(market_value) <= 0:
                qty = self._coerce_float(position.get("qty"))
                price = self._coerce_float(position.get("current_price") or position.get("avg_entry_price"))
                side = str(position.get("side", "long")).lower()
                market_value = qty * price * (-1.0 if side == "short" else 1.0)
            exposure += abs(market_value)
        return exposure / equity if equity else 0.0

    def _entry_quantity(self, latest: pd.Series) -> tuple[int, str]:
        equity = self._coerce_float(self.account_summary.get("equity") or self.account_summary.get("portfolio_value"))
        cash = self._coerce_float(self.account_summary.get("cash") or self.account_summary.get("buying_power"))
        price = self._coerce_float(latest.get("close"))
        atr_value = self._coerce_float(latest.get("entry_atr_14")) or 1.0
        sizing = self.risk_manager.size_automation_position(
            cash=cash,
            equity=equity or cash,
            price=price,
            atr_value=atr_value,
        )
        mode = self.config.risk.order_size_mode.value
        currency = self.config.risk.account_currency
        if sizing.quantity <= 0:
            if mode == "fixed_amount":
                return 0, f"定額指定 {self.config.risk.fixed_order_amount:,.0f} {currency} では数量を確保できないため見送りました"
            if mode == "equity_fraction":
                return 0, f"資産比率 {self.config.risk.equity_fraction_per_trade:.2%} では数量を確保できないため見送りました"
            return 0, f"リスク率 {self.config.risk.risk_per_trade:.2%} と ATR 条件では数量を確保できないため見送りました"
        if mode == "fixed_amount":
            return sizing.quantity, f"定額 {self.config.risk.fixed_order_amount:,.0f} {currency} 相当"
        if mode == "equity_fraction":
            return sizing.quantity, f"資産比率 {self.config.risk.equity_fraction_per_trade:.2%} 相当"
        return sizing.quantity, f"リスク率 {self.config.risk.risk_per_trade:.2%} / 想定損失 {sizing.risk_amount:,.0f} {currency}"

    def _notify(self, enabled: bool, title: str, message: str, subtitle: str = "") -> None:
        if enabled:
            self.notifier.notify(title=title, message=message, subtitle=subtitle)

    def _log(self, level: str, message_ja: str) -> None:
        self.logs.append(
            AutomationEvent(
                timestamp=pd.Timestamp.now(tz=ASIA_TOKYO),
                level=level,
                message_ja=message_ja,
            )
        )

    @staticmethod
    def _coerce_float(value: object) -> float:
        try:
            return float(value) if value not in {None, ""} else 0.0
        except (TypeError, ValueError):
            return 0.0
