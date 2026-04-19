"""Pydantic config models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from fxautotrade_lab.core.constants import DEFAULT_BENCHMARK_SYMBOLS, DEFAULT_CURRENCY, DEFAULT_SECTOR_SYMBOLS
from fxautotrade_lab.core.enums import BrokerMode, OrderSizingMode, TimeFrame
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.security.keychain import resolve_private_gmo_credentials


class EnvironmentConfig(BaseSettings):
    """Secrets and safety flags from environment."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gmo_public_base_url: str = "https://forex-api.coin.z.com/public"
    gmo_public_ws_url: str = "wss://forex-api.coin.z.com/ws/public/v1"
    gmo_private_base_url: str = "https://forex-api.coin.z.com/private"
    gmo_api_key: str = ""
    gmo_api_secret: str = ""
    live_trading_enabled: bool = False
    i_understand_real_money_risk: bool = False
    confirm_broker_mode: str = ""
    confirm_live_broker_class: str = ""

    def credentials_for_profile(self, profile: str) -> tuple[str, str]:
        normalized = profile.lower().strip()
        if normalized == "public":
            return ("", "")
        credentials = resolve_private_gmo_credentials(
            env_api_key=self.gmo_api_key,
            env_api_secret=self.gmo_api_secret,
        )
        return (credentials.api_key, credentials.api_secret)

    def has_credentials(self, profile: str) -> bool:
        if profile.lower().strip() == "public":
            return True
        api_key, api_secret = self.credentials_for_profile(profile)
        return bool(api_key and api_secret)


class WatchlistConfig(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: ["USD_JPY"])
    benchmark_symbols: list[str] = Field(default_factory=lambda: list(DEFAULT_BENCHMARK_SYMBOLS))
    sector_symbols: list[str] = Field(default_factory=lambda: list(DEFAULT_SECTOR_SYMBOLS))

    @field_validator("symbols", "benchmark_symbols", "sector_symbols")
    @classmethod
    def normalize_symbols(cls, values: list[str]) -> list[str]:
        normalized = []
        seen: set[str] = set()
        for value in values:
            symbol = normalize_fx_symbol(value)
            if symbol and symbol not in seen:
                normalized.append(symbol)
                seen.add(symbol)
        return normalized


class DataConfig(BaseModel):
    source: str = "csv"
    cache_dir: Path = Path("data_cache")
    import_dir: Path = Path("imports")
    fixture_seed: int = 11
    start_date: str = "2024-01-01"
    end_date: str = "2026-04-15"
    timeframes: list[TimeFrame] = Field(
        default_factory=lambda: [TimeFrame.DAY_1, TimeFrame.HOUR_1, TimeFrame.MIN_15, TimeFrame.MIN_1]
    )
    preferred_entry_timeframe: TimeFrame = TimeFrame.MIN_1
    use_incremental_cache: bool = True
    max_bars_per_symbol: int = 5000
    stream_enabled: bool = False
    stream_reconnect_seconds: int = 10
    gmo_price_type: str = "ASK"


class ScoreWeights(BaseModel):
    trend_regime: float = 0.28
    pullback_continuation: float = 0.24
    breakout_compression: float = 0.16
    candle_price_action: float = 0.12
    multi_timeframe_alignment: float = 0.12
    market_context: float = 0.08


class BaselineStrategyConfig(BaseModel):
    daily_fast_ema: int = 50
    daily_slow_ema: int = 200
    lower_fast_ema: int = 20
    rsi_period: int = 14
    rsi_recovery_level: int = 45
    min_daily_slope: float = 0.0
    require_volume_confirmation: bool = True


class ScoringStrategyConfig(BaseModel):
    entry_score_threshold: float = 0.64
    exit_score_threshold: float = 0.34
    overheat_score_threshold: float = 0.85
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class FxEventFilterConfig(BaseModel):
    enabled: bool = False
    provider: str = "disabled"
    calendar_path: Path | None = None
    event_blackout_before_minutes: int = 30
    event_blackout_after_minutes: int = 15
    backtest_failure_mode: str = "warn_and_disable"
    realtime_failure_mode: str = "fail_closed"

    @field_validator("backtest_failure_mode", "realtime_failure_mode", mode="before")
    @classmethod
    def _normalize_failure_mode(cls, value: object) -> str:
        mode = str(value or "").strip().lower()
        allowed = {"warn_and_disable", "fail_closed", "fail_open"}
        if mode not in allowed:
            raise ValueError(f"event failure mode は {sorted(allowed)} のいずれかを指定してください: {value}")
        return mode


class FxWalkForwardConfig(BaseModel):
    mode: str = "anchored"
    train_window: str = "2y"
    test_window: str = "1m"
    retrain_frequency: str = "1m"


class FxMlConfig(BaseModel):
    enabled: bool = False
    backend: str = "numpy_logistic"
    backtest_mode: str = "rule_only"
    decision_threshold: float = 0.5
    require_pretrained_model: bool = False
    missing_model_behavior: str = "rule_only"
    save_trained_model: bool = True
    model_dir: Path = Path("models/fx_ml")
    dataset_dir: Path = Path("datasets/fx_ml")
    latest_model_alias: str = "latest_model.json"
    seed: int = 17
    min_samples: int = 25
    max_iter: int = 400
    learning_rate: float = 0.1
    l2_penalty: float = 0.001
    feature_clip: float = 6.0
    label_clip_lower: float = -5.0
    label_clip_upper: float = 10.0
    pretrained_model_path: Path | None = None
    realtime_retrain_enabled: bool = False
    realtime_retrain_frequency: str = "1d"
    realtime_retrain_failure_mode: str = "keep_current"
    walk_forward: FxWalkForwardConfig = Field(default_factory=FxWalkForwardConfig)


class FxBreakoutPullbackConfig(BaseModel):
    ema_fast: int = 50
    ema_slow: int = 200
    ema_slope_lookback: int = 3
    adx_period: int = 14
    adx_threshold: float = 20.0
    atr_period: int = 14
    atr_percentile_lookback_bars: int = 240
    min_atr_percentile: float = 0.20
    breakout_lookback: int = 20
    breakout_buffer_atr: float = 0.0
    pullback_window_bars: int = 5
    min_pullback_atr_ratio: float = 0.05
    shallow_pullback_max_ratio: float = 0.50
    pullback_break_below_buffer_atr: float = 0.10
    swing_lookback_bars: int = 10
    swing_timeframe: TimeFrame = TimeFrame.MIN_1
    atr_stop_mult: float = 2.0
    swing_buffer_atr: float = 0.10
    atr_trailing_mult: float = 2.5
    partial_exit_fraction: float = 0.5
    trend_break_confirm_bars: int = 2
    spread_percentile_threshold: float = 0.80
    spread_context_lookback_days: int = 20
    max_spread_to_atr_ratio: float = 0.15
    rollover_blackout_minutes: int = 10
    rollover_hour_utc: int = 22
    tokyo_early_blackout_enabled: bool = False
    tokyo_early_blackout_start_hour: int = 6
    tokyo_early_blackout_end_hour: int = 8
    signal_timeframe: TimeFrame = TimeFrame.MIN_15
    trend_timeframe: TimeFrame = TimeFrame.HOUR_1
    execution_timeframe: TimeFrame = TimeFrame.MIN_1
    intrabar_policy: str = "conservative_adverse"
    positive_r_threshold: float = 0.0
    max_jpy_cross_positions: int = 1
    short_enabled: bool = False
    entry_delay_bars: int = 0
    spread_stress_multiplier: float = 1.0
    overnight_swap_per_unit: float = 0.0
    event_filter: FxEventFilterConfig = Field(default_factory=FxEventFilterConfig)
    ml_filter: FxMlConfig = Field(default_factory=FxMlConfig)


class StrategyConfig(BaseModel):
    name: str = "fx_breakout_pullback"
    entry_timeframe: TimeFrame = TimeFrame.MIN_1
    baseline: BaselineStrategyConfig = Field(default_factory=BaselineStrategyConfig)
    scoring: ScoringStrategyConfig = Field(default_factory=ScoringStrategyConfig)
    fx_breakout_pullback: FxBreakoutPullbackConfig = Field(default_factory=FxBreakoutPullbackConfig)


class RiskConfig(BaseModel):
    account_currency: str = DEFAULT_CURRENCY
    starting_cash: float = 5000000.0
    order_size_mode: OrderSizingMode = OrderSizingMode.FIXED_AMOUNT
    fixed_order_amount: float = 300000.0
    minimum_order_quantity: int = 10000
    quantity_step: int = 1000
    equity_fraction_per_trade: float = 0.1
    risk_per_trade: float = 0.01
    max_positions: int = 4
    max_symbol_exposure: float = 0.25
    max_portfolio_exposure: float = 0.9
    min_cash_buffer: float = 0.05
    max_daily_loss_pct: float = 0.02
    max_daily_loss_amount: float = 100000.0
    slippage_bps: float = 1.5
    fee_per_order: float = 0.0
    atr_stop_multiple: float = 2.2
    trailing_stop_multiple: float = 2.8
    partial_take_profit_r: float = 1.8
    partial_take_profit_fraction: float = 0.5
    break_even_trigger_r: float = 1.0
    swing_stop_lookback_bars: int = 6
    swing_stop_buffer_atr: float = 0.2
    stagnation_bars: int = 8
    stagnation_min_r: float = 0.5
    max_hold_bars: int = 30
    allow_partial_profit: bool = True


class BrokerConfig(BaseModel):
    mode: BrokerMode = BrokerMode.LOCAL_SIM
    feed: str = "gmo_public"
    allow_live_class_import: bool = False
    live_trading_hard_disabled: bool = True


class NotificationChannelConfig(BaseModel):
    channels: list[str] = Field(default_factory=lambda: ["desktop", "log"])
    sound_name: str = "Glass"
    log_path: Path = Path("runtime/notifications.log")
    webhook_url: str = ""
    webhook_timeout_seconds: float = 2.5

    @field_validator("channels")
    @classmethod
    def normalize_channels(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            channel = value.strip().lower()
            if channel and channel not in seen:
                normalized.append(channel)
                seen.add(channel)
        return normalized or ["desktop", "log"]


class AutomationConfig(BaseModel):
    enabled: bool = False
    poll_interval_seconds: int = 30
    heartbeat_timeout_seconds: int = 120
    reconnect_seconds: int = 10
    reconnect_max_attempts: int = 3
    dry_decision_mode: bool = False
    max_cycles_for_demo: int = 80
    log_every_cycle: bool = True
    sync_broker_state_on_start: bool = True
    sync_broker_state_each_cycle: bool = True
    reconcile_orders_limit: int = 50
    notifications_enabled: bool = True
    notification_channels: NotificationChannelConfig = Field(default_factory=NotificationChannelConfig)
    notify_on_start_stop: bool = True
    notify_on_orders: bool = True
    notify_on_errors: bool = True
    notify_on_risk_events: bool = True
    notify_on_reconnect: bool = True
    close_positions_on_kill_switch: bool = True


class ReportingConfig(BaseModel):
    output_dir: Path = Path("reports")
    export_html: bool = True
    export_csv: bool = True
    export_json: bool = True


class PersistenceConfig(BaseModel):
    sqlite_path: Path = Path("runtime/trading_lab.sqlite")
    enabled: bool = True


class BacktestConfig(BaseModel):
    use_custom_window: bool = False
    start_date: str = ""
    end_date: str = ""


class UIConfig(BaseModel):
    language: str = "ja"
    theme: str = "light"
    default_page: str = "概要"
    width: int = 1440
    height: int = 920
    show_dark_theme_option: bool = True


class ValidationConfig(BaseModel):
    walk_forward_enabled: bool = True
    in_sample_ratio: float = 0.7
    rolling_windows: int = 3


class ResearchConfig(BaseModel):
    mode: str = "standard"
    output_dir: Path = Path("research_runs")
    cache_dir: Path = Path("research_cache")
    reuse_cached_steps: bool = True
    spread_stress_multipliers: list[float] = Field(default_factory=lambda: [1.0, 1.2, 1.5])
    entry_delay_scenarios: list[int] = Field(default_factory=lambda: [0, 1, 2])
    parameter_sensitivity_breakout: list[int] = Field(default_factory=lambda: [18, 20, 22])
    parameter_sensitivity_stop: list[float] = Field(default_factory=lambda: [1.8, 2.0, 2.2])


class AppConfig(BaseModel):
    app_name: str = "FXAutoTrade Lab"
    watchlist: WatchlistConfig = Field(default_factory=WatchlistConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    broker: BrokerConfig = Field(default_factory=BrokerConfig)
    automation: AutomationConfig = Field(default_factory=AutomationConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    research: ResearchConfig = Field(default_factory=ResearchConfig)
    notes: dict[str, Any] = Field(default_factory=dict)
