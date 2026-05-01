from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.context.economic_events import BaseEconomicEventProvider
from fxautotrade_lab.core.enums import BrokerMode, TimeFrame
from fxautotrade_lab.data.cache import ParquetBarCache
from fxautotrade_lab.data.jforex import JForexCsvImporter, resolve_bid_ask_csv_selection
from fxautotrade_lab.data.quote_bars import build_quote_bar_frame, resample_quote_bars
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.simulation.fx_engine import FxQuotePortfolioSimulator
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from tests.conftest import write_config


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


def _write_combined_quote_csv(path: Path, frame: pd.DataFrame) -> None:
    combined = pd.DataFrame(
        {
            "timestamp": frame.index,
            "bid_open": frame["bid_open"],
            "bid_high": frame["bid_high"],
            "bid_low": frame["bid_low"],
            "bid_close": frame["bid_close"],
            "ask_open": frame["ask_open"],
            "ask_high": frame["ask_high"],
            "ask_low": frame["ask_low"],
            "ask_close": frame["ask_close"],
            "bid_volume": frame["bid_volume"],
            "ask_volume": frame["ask_volume"],
        }
    )
    combined.to_csv(path, index=False)


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


def test_bid_ask_import_repairs_minor_ohlc_inconsistencies(tmp_path: Path) -> None:
    index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.04)
    quote_frame = _make_quote_frame(index, mid_prices, spreads).copy()
    quote_frame.loc[index[5], "bid_high"] = quote_frame.loc[index[5], "bid_open"] - 0.001
    quote_frame.loc[index[8], "ask_low"] = quote_frame.loc[index[8], "ask_close"] + 0.002
    quote_frame.loc[index[10], "ask_low"] = quote_frame.loc[index[10], "bid_low"] - 0.002
    bid_path = tmp_path / "USDJPY_1 Min_Bid_invalid.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_invalid.csv"
    _write_jforex_csv(bid_path, quote_frame, "bid")
    _write_jforex_csv(ask_path, quote_frame, "ask")

    importer = JForexCsvImporter(ParquetBarCache(tmp_path / "cache"))
    result = importer.import_bid_ask_files(bid_path, ask_path)

    assert result.imported_rows == 20
    assert any("BID CSV に OHLC の不整合" in message for message in result.messages)
    assert any("ASK CSV に OHLC の不整合" in message for message in result.messages)
    assert any("負のスプレッド" in message for message in result.messages)
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")
    assert float(min1.loc[index[5], "bid_high"]) >= float(min1.loc[index[5], "bid_open"])
    assert float(min1.loc[index[8], "ask_low"]) <= float(min1.loc[index[8], "ask_close"])
    assert float(min1.loc[index[10], "ask_low"]) >= float(min1.loc[index[10], "bid_low"])


def test_combined_quote_csv_import_preserves_bid_ask(tmp_path: Path) -> None:
    index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.05)
    quote_frame = _make_quote_frame(index, mid_prices, spreads)
    combined_path = tmp_path / "usd_jpy_combined_quote.csv"
    _write_combined_quote_csv(combined_path, quote_frame)

    importer = JForexCsvImporter(ParquetBarCache(tmp_path / "cache"))
    result = importer.import_file(combined_path, symbol="USD_JPY")

    assert result.symbol == "USD_JPY"
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")
    assert {"bid_open", "ask_open", "spread_close"}.issubset(min1.columns)
    assert float(min1["spread_close"].median()) > 0.0


def test_bid_ask_selection_requires_exactly_two_files(tmp_path: Path) -> None:
    path = tmp_path / "USDJPY_1 Min_Bid_only.csv"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="2 ファイル必須"):
        resolve_bid_ask_csv_selection([path])


def test_bid_ask_selection_rejects_mismatched_symbols(tmp_path: Path) -> None:
    bid_path = tmp_path / "USDJPY_1 Min_Bid_test.csv"
    ask_path = tmp_path / "EURJPY_1 Min_Ask_test.csv"
    bid_path.write_text("", encoding="utf-8")
    ask_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="通貨ペア名が一致しません"):
        resolve_bid_ask_csv_selection([bid_path, ask_path])


def test_bid_ask_selection_requires_side_name_in_filename(tmp_path: Path) -> None:
    bid_like = tmp_path / "USDJPY_1 Min_left.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_test.csv"
    bid_like.write_text("", encoding="utf-8")
    ask_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Bid または Ask"):
        resolve_bid_ask_csv_selection([bid_like, ask_path])


def test_bid_ask_import_skips_overlapping_rows_and_only_appends_new_range(tmp_path: Path) -> None:
    cache = ParquetBarCache(tmp_path / "cache")
    importer = JForexCsvImporter(cache)
    first_index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    second_index = pd.date_range("2026-01-01 00:10:00", periods=20, freq="1min", tz="Asia/Tokyo")
    first_frame = _make_quote_frame(
        first_index, np.linspace(100.0, 100.5, len(first_index)), np.full(len(first_index), 0.04)
    )
    second_frame = _make_quote_frame(
        second_index, np.linspace(100.3, 100.9, len(second_index)), np.full(len(second_index), 0.04)
    )
    bid_first = tmp_path / "USDJPY_1 Min_Bid_first.csv"
    ask_first = tmp_path / "USDJPY_1 Min_Ask_first.csv"
    bid_second = tmp_path / "USDJPY_1 Min_Bid_second.csv"
    ask_second = tmp_path / "USDJPY_1 Min_Ask_second.csv"
    _write_jforex_csv(bid_first, first_frame, "bid")
    _write_jforex_csv(ask_first, first_frame, "ask")
    _write_jforex_csv(bid_second, second_frame, "bid")
    _write_jforex_csv(ask_second, second_frame, "ask")

    first_result = importer.import_bid_ask_files(bid_first, ask_first)
    second_result = importer.import_bid_ask_files(bid_second, ask_second)

    assert first_result.imported_rows == 20
    assert second_result.imported_rows == 10
    assert second_result.skipped_rows == 10
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")
    assert len(min1.index) == 30
    assert min1.index.min() == first_index.min()
    assert min1.index.max() == second_index.max()
    coverage = cache.load_coverage("USD_JPY", TimeFrame.MIN_1)
    assert coverage == [(first_index.min(), second_index.max() + pd.Timedelta(minutes=1))]


def test_bid_ask_import_only_fills_gap_between_existing_csv_and_gmo_ranges(tmp_path: Path) -> None:
    cache = ParquetBarCache(tmp_path / "cache")
    importer = JForexCsvImporter(cache)
    leading_index = pd.date_range("2026-01-01 00:00:00", periods=10, freq="1min", tz="Asia/Tokyo")
    trailing_index = pd.date_range("2026-01-01 00:20:00", periods=10, freq="1min", tz="Asia/Tokyo")
    full_index = pd.date_range("2026-01-01 00:00:00", periods=30, freq="1min", tz="Asia/Tokyo")
    leading_frame = _make_quote_frame(
        leading_index,
        np.linspace(100.0, 100.2, len(leading_index)),
        np.full(len(leading_index), 0.04),
    )
    trailing_frame = _make_quote_frame(
        trailing_index,
        np.linspace(101.0, 101.2, len(trailing_index)),
        np.full(len(trailing_index), 0.06),
    )
    full_frame = _make_quote_frame(
        full_index, np.linspace(100.0, 101.2, len(full_index)), np.full(len(full_index), 0.05)
    )
    bid_path = tmp_path / "USDJPY_1 Min_Bid_gapfill.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_gapfill.csv"
    _write_jforex_csv(bid_path, full_frame, "bid")
    _write_jforex_csv(ask_path, full_frame, "ask")

    cache.save("USD_JPY", TimeFrame.MIN_1, pd.concat([leading_frame, trailing_frame]).sort_index())
    cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        leading_index.min(),
        leading_index.max() + pd.Timedelta(minutes=1),
        source_key="csv_bid",
    )
    cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        leading_index.min(),
        leading_index.max() + pd.Timedelta(minutes=1),
        source_key="csv_ask",
    )
    cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        trailing_index.min(),
        trailing_index.max() + pd.Timedelta(minutes=1),
        source_key="gmo_bid",
    )
    cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        trailing_index.min(),
        trailing_index.max() + pd.Timedelta(minutes=1),
        source_key="gmo_ask",
    )

    result = importer.import_bid_ask_files(bid_path, ask_path)

    assert result.imported_rows == 10
    assert result.skipped_rows == 20
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")
    assert len(min1.index) == 30
    gap_slice = min1.loc[
        (min1.index >= pd.Timestamp("2026-01-01 00:10:00", tz="Asia/Tokyo"))
        & (min1.index < pd.Timestamp("2026-01-01 00:20:00", tz="Asia/Tokyo"))
    ]
    assert len(gap_slice.index) == 10
    existing_gmo_row = min1.loc[trailing_index.min()]
    assert float(existing_gmo_row["spread_close"]) == pytest.approx(
        float(trailing_frame.loc[trailing_index.min(), "spread_close"])
    )


def test_bid_ask_reimport_skips_fully_covered_period_without_reloading_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ParquetBarCache(tmp_path / "cache")
    importer = JForexCsvImporter(cache)
    index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    frame = _make_quote_frame(
        index, np.linspace(100.0, 100.5, len(index)), np.full(len(index), 0.04)
    )
    bid_path = tmp_path / "USDJPY_1 Min_Bid_reimport.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_reimport.csv"
    _write_jforex_csv(bid_path, frame, "bid")
    _write_jforex_csv(ask_path, frame, "ask")

    first = importer.import_bid_ask_files(bid_path, ask_path)

    def fail_load(symbol: str, timeframe: TimeFrame):  # noqa: ANN001
        raise AssertionError(
            f"cache.load should not be called for fully covered re-import: {symbol} {timeframe.value}"
        )

    monkeypatch.setattr(cache, "load", fail_load)

    second = importer.import_bid_ask_files(bid_path, ask_path)

    assert first.imported_rows == 20
    assert second.imported_rows == 0
    assert second.skipped_rows == 20
    assert any("既存キャッシュで期間が埋まっている" in message for message in second.messages)


def test_bid_ask_import_trims_to_common_period_when_ranges_differ(tmp_path: Path) -> None:
    importer = JForexCsvImporter(ParquetBarCache(tmp_path / "cache"))
    bid_index = pd.date_range("2026-01-01 00:00:00", periods=20, freq="1min", tz="Asia/Tokyo")
    ask_index = pd.date_range("2026-01-01 00:05:00", periods=10, freq="1min", tz="Asia/Tokyo")
    bid_frame = _make_quote_frame(
        bid_index, np.linspace(100.0, 100.5, len(bid_index)), np.full(len(bid_index), 0.04)
    )
    ask_frame = _make_quote_frame(
        ask_index, np.linspace(100.1, 100.4, len(ask_index)), np.full(len(ask_index), 0.05)
    )
    bid_path = tmp_path / "USDJPY_1 Min_Bid_trim.csv"
    ask_path = tmp_path / "USDJPY_1 Min_Ask_trim.csv"
    _write_jforex_csv(bid_path, bid_frame, "bid")
    _write_jforex_csv(ask_path, ask_frame, "ask")

    result = importer.import_bid_ask_files(bid_path, ask_path)
    min1 = pd.read_parquet(tmp_path / "cache" / "USD_JPY" / "1Min.parquet")

    assert result.start == ask_index.min().isoformat()
    assert result.end == ask_index.max().isoformat()
    assert result.bid_start == bid_index.min().isoformat()
    assert result.bid_end == bid_index.max().isoformat()
    assert result.ask_start == ask_index.min().isoformat()
    assert result.ask_end == ask_index.max().isoformat()
    assert result.imported_rows == len(ask_index)
    assert min1.index.min() == ask_index.min()
    assert min1.index.max() == ask_index.max()
    assert any("共通期間のみ" in message for message in result.messages)


def test_application_rejects_single_csv_import(tmp_path: Path) -> None:
    app = LabApplication(write_config(tmp_path))

    with pytest.raises(RuntimeError, match="単一 CSV のインポートは無効"):
        app.import_jforex_csv(str(tmp_path / "USDJPY_1 Min_Bid_only.csv"))


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
    assert float(signal_frame.loc[index[2], "initial_stop_price"]) < float(
        signal_frame.loc[index[2], "entry_trigger_price"]
    )


def test_fx_strategy_uses_swing_reference_from_selected_timeframe(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    strategy = FxBreakoutPullbackStrategy(config)
    index = pd.date_range("2026-01-05 10:00:00", periods=3, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "close": [100.0, 100.6, 100.66],
            "high": [100.1, 100.7, 100.68],
            "low": [100.0, 100.5, 100.62],
            "mid_high": [100.1, 100.7, 100.68],
            "mid_low": [100.0, 100.5, 100.62],
            "ask_high": [100.12, 100.72, 100.70],
            "trend_long_allowed_1h": [True, True, True],
            "spread_context_ok": [True, True, True],
            "spread_ratio_ok": [True, True, True],
            "entry_context_ok": [True, True, True],
            "event_blackout": [False] * 3,
            "rollover_blackout": [False] * 3,
            "tokyo_early_blackout": [False] * 3,
            "breakout_signal_15m": [False, True, False],
            "signal_bar_timestamp": [pd.NaT, index[1], index[1]],
            "breakout_level_15m": [pd.NA, 100.4, 100.4],
            "breakout_atr_15m": [pd.NA, 0.2, 0.2],
            "atr_15m": [0.2, 0.2, 0.2],
            "swing_low_reference": [pd.NA, 99.9, 99.9],
            "trend_bar_timestamp": [index[0], index[0], index[0]],
            "full_exit_trend_break_1h": [False] * 3,
            "partial_exit_trend_break_1h": [False] * 3,
        },
        index=index,
    )

    signal_frame = strategy.generate_signal_frame(frame)

    assert bool(signal_frame.loc[index[2], "entry_signal"])
    assert float(signal_frame.loc[index[2], "initial_stop_price"]) == pytest.approx(99.88, abs=1e-9)


def test_fx_strategy_generates_short_entry_after_shallow_pullback(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.short_enabled = True
    strategy = FxBreakoutPullbackStrategy(config)
    index = pd.date_range("2026-01-05 11:00:00", periods=4, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "close": [100.0, 99.30, 99.22, 99.05],
            "high": [100.1, 99.40, 99.35, 99.12],
            "low": [99.9, 99.20, 99.15, 98.98],
            "mid_high": [100.1, 99.40, 99.35, 99.12],
            "mid_low": [99.9, 99.20, 99.15, 98.98],
            "bid_low": [99.88, 99.18, 99.10, 98.95],
            "trend_long_allowed_1h": [False, False, False, False],
            "trend_short_allowed_1h": [True, True, True, True],
            "spread_context_ok": [True, True, True, True],
            "spread_ratio_ok": [True, True, True, True],
            "entry_context_ok": [True, True, True, True],
            "event_blackout": [False] * 4,
            "rollover_blackout": [False] * 4,
            "tokyo_early_blackout": [False] * 4,
            "breakout_signal_15m": [False] * 4,
            "breakout_signal_short_15m": [False, True, False, False],
            "signal_bar_timestamp": [pd.NaT, index[1], index[1], index[1]],
            "breakout_short_level_15m": [pd.NA, 99.50, 99.50, 99.50],
            "breakout_atr_15m": [pd.NA, 0.50, 0.50, 0.50],
            "atr_15m": [0.50, 0.50, 0.50, 0.50],
            "swing_high_reference": [pd.NA, 99.90, 99.90, 99.90],
            "trend_bar_timestamp": [index[0], index[0], index[0], index[0]],
            "full_exit_trend_break_1h": [False] * 4,
            "partial_exit_trend_break_1h": [False] * 4,
            "full_exit_short_trend_break_1h": [False] * 4,
            "partial_exit_short_trend_break_1h": [False] * 4,
        },
        index=index,
    )

    signal_frame = strategy.generate_signal_frame(frame)

    assert not bool(signal_frame.loc[index[1], "entry_signal"])
    assert bool(signal_frame.loc[index[2], "entry_signal"])
    assert signal_frame.loc[index[2], "position_side"] == "short"
    assert signal_frame.loc[index[2], "entry_order_side"] == "sell"
    assert signal_frame.loc[index[2], "exit_order_side"] == "buy"
    assert signal_frame.loc[index[2], "strategy_state"] == "ENTRY_ARMED"
    assert float(signal_frame.loc[index[2], "initial_stop_price"]) > float(
        signal_frame.loc[index[2], "entry_trigger_price"]
    )


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


def test_fx_engine_can_skip_heavy_account_history_for_label_runs(tmp_path: Path) -> None:
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

    outputs = simulator.run(
        {"USD_JPY": frame},
        mode=BrokerMode.LOCAL_SIM,
        collect_equity=False,
        collect_orders=False,
        collect_fills=False,
        collect_positions=False,
    )

    assert outputs["equity_curve"].empty
    assert outputs["orders"].empty
    assert outputs["fills"].empty
    assert outputs["positions"].empty
    assert len(outputs["trades"].index) == 1


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


def test_fx_engine_chunked_runs_preserve_position_and_pending_exit_state(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.risk.slippage_bps = 0.0
    simulator = FxQuotePortfolioSimulator(config)
    index = pd.date_range("2026-01-06 09:00:00", periods=4, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "ask_open": [100.0, 100.8, 101.8, 102.0],
            "ask_high": [100.2, 101.1, 102.0, 102.1],
            "ask_low": [99.9, 100.7, 101.7, 101.9],
            "ask_close": [100.1, 100.95, 101.9, 102.0],
            "bid_open": [99.96, 100.76, 101.7, 101.9],
            "bid_high": [100.1, 100.9, 101.9, 102.0],
            "bid_low": [99.9, 100.6, 101.6, 101.8],
            "bid_close": [100.0, 100.85, 101.8, 101.95],
            "open": [99.98, 100.78, 101.75, 101.95],
            "high": [100.15, 101.0, 101.95, 102.05],
            "low": [99.9, 100.6, 101.6, 101.8],
            "close": [100.05, 100.9, 101.85, 101.98],
            "volume": [220.0, 220.0, 220.0, 220.0],
            "entry_signal": [True, False, False, False],
            "entry_trigger_price": [101.0, pd.NA, pd.NA, pd.NA],
            "initial_stop_price": [99.5, pd.NA, pd.NA, pd.NA],
            "initial_risk_price": [1.5, pd.NA, pd.NA, pd.NA],
            "breakout_atr_15m": [0.5, 0.5, 0.5, 0.5],
            "breakout_level_15m": [100.7, 100.7, 100.7, 100.7],
            "atr_15m": [0.5, 0.5, 0.5, 0.5],
            "signal_score": [0.8, 0.0, 0.0, 0.0],
            "explanation_ja": ["entry", "", "", ""],
            "entry_context_ok": [True, True, True, True],
            "exit_signal": [False, True, False, False],
            "partial_exit_signal": [False, False, False, False],
        },
        index=index,
    )

    full_outputs = simulator.run({"USD_JPY": frame}, mode=BrokerMode.LOCAL_SIM)
    first_chunk = simulator.run(
        {"USD_JPY": frame.iloc[:3]},
        mode=BrokerMode.LOCAL_SIM,
        process_until=index[2],
    )
    second_chunk = simulator.run(
        {"USD_JPY": frame.iloc[2:]},
        mode=BrokerMode.LOCAL_SIM,
        initial_state=first_chunk["state"],
    )

    chunked_trades = pd.concat([first_chunk["trades"], second_chunk["trades"]], ignore_index=True)
    chunked_orders = pd.concat([first_chunk["orders"], second_chunk["orders"]], ignore_index=True)
    chunked_fills = pd.concat([first_chunk["fills"], second_chunk["fills"]], ignore_index=True)

    comparable_columns = [
        "symbol",
        "signal_time",
        "entry_time",
        "exit_time",
        "position_side",
        "quantity",
        "initial_quantity",
        "entry_price",
        "exit_price",
        "entry_order_side",
        "exit_order_side",
        "net_pnl",
        "exit_reason",
    ]
    assert chunked_trades[comparable_columns].to_dict("records") == full_outputs[
        "trades"
    ].reset_index(drop=True)[comparable_columns].to_dict("records")
    assert len(chunked_orders.index) == len(full_outputs["orders"].index)
    assert len(chunked_fills.index) == len(full_outputs["fills"].index)


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
    assert feature_set.execution_frame["spread_context_bucket"].iloc[-1].startswith("USD_JPY_")


def test_event_blackout_uses_runtime_failure_mode(tmp_path: Path) -> None:
    class _RaisingProvider(BaseEconomicEventProvider):
        def list_events(self, start: pd.Timestamp, end: pd.Timestamp, currencies: set[str]):
            _ = start, end, currencies
            raise RuntimeError("provider down")

    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.event_filter.enabled = True
    config.strategy.fx_breakout_pullback.event_filter.provider = "static_csv"
    config.strategy.fx_breakout_pullback.event_filter.backtest_failure_mode = "warn_and_disable"
    config.strategy.fx_breakout_pullback.event_filter.realtime_failure_mode = "fail_closed"
    index = pd.date_range("2026-01-01 00:00:00", periods=60, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.02)
    frame_1m = _make_quote_frame(index, mid_prices, spreads)
    frames = {
        TimeFrame.MIN_1: frame_1m,
        TimeFrame.MIN_15: resample_quote_bars(frame_1m, "15min"),
        TimeFrame.HOUR_1: resample_quote_bars(frame_1m, "1h"),
        TimeFrame.DAY_1: resample_quote_bars(frame_1m, "1D"),
        TimeFrame.WEEK_1: resample_quote_bars(frame_1m, "1W"),
        TimeFrame.MONTH_1: resample_quote_bars(frame_1m, "1ME"),
    }

    backtest_feature_set = build_fx_feature_set(
        "USD_JPY", frames, config, event_provider=_RaisingProvider(), runtime_mode=False
    )
    realtime_feature_set = build_fx_feature_set(
        "USD_JPY", frames, config, event_provider=_RaisingProvider(), runtime_mode=True
    )

    assert not bool(backtest_feature_set.execution_frame["event_blackout"].iloc[-1])
    assert bool(realtime_feature_set.execution_frame["event_blackout"].iloc[-1])


def test_event_blackout_warn_and_disable_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    class _RaisingProvider(BaseEconomicEventProvider):
        def list_events(self, start: pd.Timestamp, end: pd.Timestamp, currencies: set[str]):
            _ = start, end, currencies
            raise RuntimeError("provider down")

    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.event_filter.enabled = True
    config.strategy.fx_breakout_pullback.event_filter.backtest_failure_mode = "warn_and_disable"
    index = pd.date_range("2026-01-01 00:00:00", periods=60, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.02)
    frame_1m = _make_quote_frame(index, mid_prices, spreads)
    frames = {
        TimeFrame.MIN_1: frame_1m,
        TimeFrame.MIN_15: resample_quote_bars(frame_1m, "15min"),
        TimeFrame.HOUR_1: resample_quote_bars(frame_1m, "1h"),
        TimeFrame.DAY_1: resample_quote_bars(frame_1m, "1D"),
        TimeFrame.WEEK_1: resample_quote_bars(frame_1m, "1W"),
        TimeFrame.MONTH_1: resample_quote_bars(frame_1m, "1ME"),
    }

    caplog.set_level(logging.WARNING)
    feature_set = build_fx_feature_set(
        "USD_JPY", frames, config, event_provider=_RaisingProvider(), runtime_mode=False
    )

    assert not bool(feature_set.execution_frame["event_blackout"].iloc[-1])
    assert "イベントフィルタを無効化します" in caplog.text


def test_event_blackout_fail_open_and_validation_are_explicit(tmp_path: Path) -> None:
    class _RaisingProvider(BaseEconomicEventProvider):
        def list_events(self, start: pd.Timestamp, end: pd.Timestamp, currencies: set[str]):
            _ = start, end, currencies
            raise RuntimeError("provider down")

    config = _make_fx_config(tmp_path)
    config.strategy.fx_breakout_pullback.event_filter.enabled = True
    config.strategy.fx_breakout_pullback.event_filter.realtime_failure_mode = "fail_open"
    index = pd.date_range("2026-01-01 00:00:00", periods=60, freq="1min", tz="Asia/Tokyo")
    mid_prices = np.linspace(100.0, 100.5, len(index))
    spreads = np.full(len(index), 0.02)
    frame_1m = _make_quote_frame(index, mid_prices, spreads)
    frames = {
        TimeFrame.MIN_1: frame_1m,
        TimeFrame.MIN_15: resample_quote_bars(frame_1m, "15min"),
        TimeFrame.HOUR_1: resample_quote_bars(frame_1m, "1h"),
        TimeFrame.DAY_1: resample_quote_bars(frame_1m, "1D"),
        TimeFrame.WEEK_1: resample_quote_bars(frame_1m, "1W"),
        TimeFrame.MONTH_1: resample_quote_bars(frame_1m, "1ME"),
    }

    realtime_feature_set = build_fx_feature_set(
        "USD_JPY", frames, config, event_provider=_RaisingProvider(), runtime_mode=True
    )

    assert not bool(realtime_feature_set.execution_frame["event_blackout"].iloc[-1])

    payload = config.model_dump(mode="python")
    payload["strategy"]["fx_breakout_pullback"]["event_filter"][
        "realtime_failure_mode"
    ] = "unexpected_mode"
    with pytest.raises(ValidationError):
        AppConfig.model_validate(payload)


def test_fx_engine_supports_short_entry_and_buy_to_cover_exit(tmp_path: Path) -> None:
    config = _make_fx_config(tmp_path)
    config.risk.slippage_bps = 0.0
    simulator = FxQuotePortfolioSimulator(config)
    index = pd.date_range("2026-01-06 11:00:00", periods=3, freq="1min", tz="Asia/Tokyo")
    frame = pd.DataFrame(
        {
            "ask_open": [100.04, 99.84, 99.24],
            "ask_high": [100.10, 99.90, 99.30],
            "ask_low": [99.98, 99.70, 99.10],
            "ask_close": [100.02, 99.78, 99.18],
            "bid_open": [100.00, 99.80, 99.20],
            "bid_high": [100.06, 99.86, 99.26],
            "bid_low": [99.94, 99.60, 99.00],
            "bid_close": [99.98, 99.74, 99.14],
            "open": [100.02, 99.82, 99.22],
            "high": [100.08, 99.88, 99.28],
            "low": [99.96, 99.65, 99.05],
            "close": [100.00, 99.76, 99.16],
            "volume": [220.0, 220.0, 220.0],
            "entry_signal": [True, False, False],
            "position_side": ["short", pd.NA, pd.NA],
            "entry_order_side": ["sell", pd.NA, pd.NA],
            "exit_order_side": ["buy", pd.NA, pd.NA],
            "entry_trigger_price": [99.70, pd.NA, pd.NA],
            "initial_stop_price": [100.50, pd.NA, pd.NA],
            "initial_risk_price": [0.80, pd.NA, pd.NA],
            "breakout_atr_15m": [0.4, 0.4, 0.4],
            "breakout_level_15m": [99.80, 99.80, 99.80],
            "atr_15m": [0.4, 0.4, 0.4],
            "signal_score": [0.8, 0.0, 0.0],
            "explanation_ja": ["short entry", "", ""],
            "entry_context_ok": [True, True, True],
            "exit_signal": [False, True, False],
            "partial_exit_signal": [False, False, False],
        },
        index=index,
    )

    outputs = simulator.run({"USD_JPY": frame}, mode=BrokerMode.LOCAL_SIM)

    trades = outputs["trades"]
    assert len(trades.index) == 1
    assert trades.iloc[0]["position_side"] == "short"
    assert trades.iloc[0]["entry_order_side"] == "sell"
    assert trades.iloc[0]["exit_order_side"] == "buy"
    assert float(trades.iloc[0]["entry_price"]) == 99.7
    assert float(trades.iloc[0]["exit_price"]) == 99.24
    assert float(trades.iloc[0]["net_pnl"]) > 0.0
