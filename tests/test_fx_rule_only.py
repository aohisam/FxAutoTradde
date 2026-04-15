from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.enums import BrokerMode, TimeFrame
from fxautotrade_lab.data.cache import ParquetBarCache
from fxautotrade_lab.data.jforex import JForexCsvImporter
from fxautotrade_lab.data.quote_bars import build_quote_bar_frame, resample_quote_bars
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.simulation.fx_engine import FxQuotePortfolioSimulator
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy


def _make_fx_config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "app_name": "FX Rule Test",
            "watchlist": {
                "symbols": ["USD_JPY"],
                "benchmark_symbols": ["USD_JPY"],
                "sector_symbols": [],
            },
            "data": {
                "source": "csv",
                "cache_dir": str(tmp_path / "cache"),
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "timeframes": ["1Min", "15Min", "1Hour", "1Day", "1Week", "1Month"],
                "preferred_entry_timeframe": "1Min",
            },
            "strategy": {
                "name": "fx_breakout_pullback",
                "entry_timeframe": "1Min",
                "fx_breakout_pullback": {
                    "signal_timeframe": "15Min",
                    "trend_timeframe": "1Hour",
                    "execution_timeframe": "1Min",
                    "tokyo_early_blackout_enabled": False,
                    "spread_context_lookback_days": 2,
                    "atr_percentile_lookback_bars": 40,
                },
            },
            "broker": {"mode": "local_sim"},
            "reporting": {"output_dir": str(tmp_path / "reports")},
            "persistence": {"sqlite_path": str(tmp_path / "runtime" / "trading_lab.sqlite")},
        }
    )


def _make_quote_frame(
    index: pd.DatetimeIndex,
    mid_prices: np.ndarray,
    spreads: np.ndarray,
) -> pd.DataFrame:
    bid_close = mid_prices - spreads / 2.0
    ask_close = mid_prices + spreads / 2.0
    bid_open = np.concatenate(([bid_close[0]], bid_close[:-1]))
    ask_open = np.concatenate(([ask_close[0]], ask_close[:-1]))
    bid_high = np.maximum(bid_open, bid_close) + 0.02
    ask_high = np.maximum(ask_open, ask_close) + 0.02
    bid_low = np.minimum(bid_open, bid_close) - 0.02
    ask_low = np.minimum(ask_open, ask_close) - 0.02
    bid = pd.DataFrame(
        {
            "bid_open": bid_open,
            "bid_high": bid_high,
            "bid_low": bid_low,
            "bid_close": bid_close,
            "bid_volume": 100.0,
        },
        index=index,
    )
    ask = pd.DataFrame(
        {
            "ask_open": ask_open,
            "ask_high": ask_high,
            "ask_low": ask_low,
            "ask_close": ask_close,
            "ask_volume": 120.0,
        },
        index=index,
    )
    return build_quote_bar_frame(bid, ask, "USD_JPY")


def _write_jforex_csv(path: Path, frame: pd.DataFrame, side: str) -> None:
    renamed = pd.DataFrame(
        {
            "Time (EET)": frame.index.tz_convert("Europe/Helsinki").strftime("%Y.%m.%d %H:%M:%S"),
            "Open": frame[f"{side}_open"],
            "High": frame[f"{side}_high"],
            "Low": frame[f"{side}_low"],
            "Close": frame[f"{side}_close"],
            "Volume": frame[f"{side}_volume"],
        }
    )
    renamed.to_csv(path, index=False)


def test_bid_ask_import_and_resample(tmp_path: Path) -> None:
    index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.04)
    quote_frame = _make_quote_frame(index, mid_prices, spreads)
    bid_path = tmp_path / "USDJPY_1 Min_Bid_test.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_test.csv"
    _write_jforex_csv(bid_path, quote_frame, "bid")
    _write_jforex_csv(ask_path, quote_frame, "ask")

    importer = JForexCsvImporter(ParquetBarCache(tmp_path / "cache"))
    result = importer.import_bid_ask_files(bid_path, ask_path)

    assert result.symbol == "USD_JPY"
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")
    min15 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "15Min.parquet")
    assert {"bid_open", "ask_open", "mid_close", "spread_close"}.issubset(min1.columns)
    assert {"bid_open", "ask_open", "mid_close", "spread_close"}.issubset(min15.columns)
    assert float(min1["spread_close"].min()) > 0


def test_fx_strategy_requires_pullback_before_entry(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    strategy = FxBreakoutPullbackStrategy(config)
    index = pd.date_range("2026-01-05 10:00:00", periods=6, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "close": [100.0, 100.6, 100.45, 100.55, 100.65, 100.70],
            "high": [100.1, 100.7, 100.55, 100.60, 100.75, 100.80],
            "low": [99.9, 100.4, 100.48, 100.45, 100.55, 100.60],
            "mid_high": [100.1, 100.7, 100.55, 100.60, 100.75, 100.80],
            "mid_low": [99.9, 100.4, 100.48, 100.45, 100.55, 100.60],
            "ask_high": [100.12, 100.72, 100.57, 100.62, 100.77, 100.82],
            "trend_long_allowed_1h": [True, True, True, True, True, True],
            "spread_context_ok": [True, True, True, True, True, True],
            "spread_ratio_ok": [True, True, True, True, True, True],
            "entry_context_ok": [True, True, True, True, True, True],
            "event_blackout": [False] * 6,
            "rollover_blackout": [False] * 6,
            "tokyo_early_blackout": [False] * 6,
            "breakout_signal_15m": [False, True, False, False, False, False],
            "signal_bar_timestamp": [pd.NaT, index[1], index[1], index[1], index[4], index[4]],
            "breakout_level_15m": [pd.NA, 100.5, 100.5, 100.5, 100.6, 100.6],
            "breakout_atr_15m": [pd.NA, 0.5, 0.5, 0.5, 0.5, 0.5],
            "atr_15m": [0.5] * 6,
            "trend_bar_timestamp": [index[0], index[0], index[0], index[3], index[3], index[3]],
            "full_exit_trend_break_1h": [False] * 6,
            "partial_exit_trend_break_1h": [False] * 6,
        },
        index=index,
    )

    signal_frame = strategy.generate_signal_frame(frame)

    assert not bool(signal_frame.loc[index[1], "entry_signal"])
    assert bool(signal_frame.loc[index[2], "entry_signal"])
    assert signal_frame.loc[index[2], "strategy_state"] == "ENTRY_ARMED"
    assert float(signal_frame.loc[index[2], "initial_stop_price"]) < float(signal_frame.loc[index[2], "entry_trigger_price"])


def test_fx_engine_uses_ask_entry_and_bid_exit(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.risk.slippage_bps = 0.0
    simulator = FxQuotePortfolioSimulator(config)
    index = pd.date_range("2026-01-06 09:00:00", periods=3, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "ask_open": [100.0, 100.8, 101.8],
            "ask_high": [100.2, 101.1, 102.0],
            "ask_low": [99.9, 100.7, 101.7],
            "ask_close": [100.1, 100.95, 101.9],
            "bid_open": [99.96, 100.76, 101.7],
            "bid_high": [100.1, 100.9, 101.9],
            "bid_low": [99.9, 100.6, 101.6],
            "bid_close": [100.0, 100.85, 101.8],
            "open": [99.98, 100.78, 101.75],
            "high": [100.15, 101.0, 101.95],
            "low": [99.9, 100.6, 101.6],
            "close": [100.05, 100.9, 101.85],
            "volume": [220.0, 220.0, 220.0],
            "entry_signal": [True, False, False],
            "entry_trigger_price": [101.0, pd.NA, pd.NA],
            "initial_stop_price": [99.5, pd.NA, pd.NA],
            "initial_risk_price": [1.5, pd.NA, pd.NA],
            "breakout_atr_15m": [0.5, 0.5, 0.5],
            "breakout_level_15m": [100.7, 100.7, 100.7],
            "atr_15m": [0.5, 0.5, 0.5],
            "signal_score": [0.8, 0.0, 0.0],
            "explanation_ja": ["entry", "", ""],
            "entry_context_ok": [True, True, True],
            "exit_signal": [False, True, False],
            "partial_exit_signal": [False, False, False],
        },
        index=index,
    )

    outputs = simulator.run({"USD_JPY": frame}, mode=BrokerMode.LOCAL_SIM)

    trades = outputs["trades"]
    assert len(trades.index) == 1
    assert float(trades.iloc[0]["entry_price"]) == 101.0
    assert float(trades.iloc[0]["exit_price"]) == 101.7


def test_fx_engine_conservative_same_bar_stop_after_entry(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.risk.slippage_bps = 0.0
    simulator = FxQuotePortfolioSimulator(config)
    index = pd.date_range("2026-01-06 10:00:00", periods=2, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "ask_open": [100.0, 100.8],
            "ask_high": [100.2, 101.2],
            "ask_low": [99.9, 100.7],
            "ask_close": [100.1, 100.9],
            "bid_open": [99.96, 100.6],
            "bid_high": [100.1, 100.8],
            "bid_low": [99.9, 99.4],
            "bid_close": [100.0, 99.8],
            "open": [99.98, 100.7],
            "high": [100.15, 101.0],
            "low": [99.9, 99.4],
            "close": [100.05, 100.0],
            "volume": [220.0, 220.0],
            "entry_signal": [True, False],
            "entry_trigger_price": [101.0, pd.NA],
            "initial_stop_price": [99.5, pd.NA],
            "initial_risk_price": [1.5, pd.NA],
            "breakout_atr_15m": [0.5, 0.5],
            "breakout_level_15m": [100.7, 100.7],
            "atr_15m": [0.5, 0.5],
            "signal_score": [0.8, 0.0],
            "explanation_ja": ["entry", ""],
            "entry_context_ok": [True, True],
            "exit_signal": [False, False],
            "partial_exit_signal": [False, False],
        },
        index=index,
    )

    outputs = simulator.run({"USD_JPY": frame}, mode=BrokerMode.LOCAL_SIM)

    trades = outputs["trades"]
    positions = outputs["positions"]
    assert len(trades.index) == 1
    assert trades.iloc[0]["exit_reason"] == "protective_stop"
    assert positions.empty


def test_fx_pipeline_spread_filter_excludes_current_bar(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    index = pd.date_range("2026-01-01 00:00:00", periods=60 * 24 * 3, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 103.0, len(index))
    spreads = np.full(len(index), 0.02)
    spreads[-1] = 0.50
    frame_1m = _make_quote_frame(index, mid_prices, spreads)
    frames = {
        TimeFrame.MIN_1: frame_1m,
        TimeFrame.MIN_15: resample_quote_bars(frame_1m, "15min"),
        TimeFrame.HOUR_1: resample_quote_bars(frame_1m, "1h"),
        TimeFrame.DAY_1: resample_quote_bars(frame_1m, "1D"),
        TimeFrame.WEEK_1: resample_quote_bars(frame_1m, "1W"),
        TimeFrame.MONTH_1: resample_quote_bars(frame_1m, "1ME"),
    }

    feature_set = build_fx_feature_set("USD_JPY", frames, config)

    assert bool(feature_set.execution_frame["spread_context_ok"].iloc[-2])
    assert not bool(feature_set.execution_frame["spread_context_ok"].iloc[-1])
