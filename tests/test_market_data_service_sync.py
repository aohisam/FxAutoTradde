from __future__ import annotations

import numpy as np
import pandas as pd

from fxautotrade_lab.config.loader import load_app_config
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.quote_bars import build_quote_bar_frame, summarize_quote_bar_quality, validate_quote_bar_frame
from fxautotrade_lab.data.service import MarketDataFrameLoad, MarketDataService

from tests.conftest import write_config


def _make_quote_frame(symbol: str, start: str, periods: int, freq: str = "1min") -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq=freq, tz=ASIA_TOKYO)
    mid = np.linspace(150.0, 150.5, periods)
    spread = np.full(periods, 0.02)
    bid = pd.DataFrame(
        {
            "bid_open": mid - spread / 2.0,
            "bid_high": mid + 0.03,
            "bid_low": mid - 0.04,
            "bid_close": mid - spread / 2.0,
            "bid_volume": 1000.0,
        },
        index=index,
    )
    ask = pd.DataFrame(
        {
            "ask_open": mid + spread / 2.0,
            "ask_high": mid + 0.05,
            "ask_low": mid - 0.02,
            "ask_close": mid + spread / 2.0,
            "ask_volume": 1200.0,
        },
        index=index,
    )
    return build_quote_bar_frame(bid, ask, symbol)


def test_sync_refreshes_gmo_cache_and_reports_quote_details(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path, strategy_name="fx_breakout_pullback"),
        overrides={
            "watchlist": {"symbols": ["USD_JPY"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
            },
            "strategy": {"entry_timeframe": "1Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    cached = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 3)
    refreshed = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 5)
    service.cache.save("USD_JPY", TimeFrame.MIN_1, cached)

    calls: list[tuple[str, TimeFrame]] = []

    def fake_fetch_bars(symbol, timeframe, start, end, price_type="ASK"):  # noqa: ANN001
        calls.append((symbol, timeframe))
        return refreshed

    monkeypatch.setattr(service.gmo, "fetch_bars", fake_fetch_bars)

    summary = service.sync()

    cached_after = service.cache.load("USD_JPY", TimeFrame.MIN_1)
    assert cached_after is not None
    assert cached_after.index.max() == refreshed.index.max()
    assert ("USD_JPY", TimeFrame.MIN_1) in calls
    assert len(calls) >= 1
    assert summary["force_refresh"] is True
    assert summary["sync_mode"] == "incremental"
    assert summary["symbols"] == 1
    assert summary["details"][0]["source"] == "gmo_incremental"


def test_sync_reuses_loaded_results_when_watchlist_and_benchmark_overlap(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path, strategy_name="fx_breakout_pullback"),
        overrides={
            "watchlist": {"symbols": ["USD_JPY"], "benchmark_symbols": ["USD_JPY"], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
            },
            "strategy": {"entry_timeframe": "1Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    frame = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 3)

    calls: list[str] = []
    cache_path = service.cache.path_for("USD_JPY", TimeFrame.MIN_1)

    def fake_load_symbol_frame_results(symbol, **kwargs):  # noqa: ANN001
        _ = kwargs
        calls.append(symbol)
        return {
            TimeFrame.MIN_1: MarketDataFrameLoad(
                frame=frame,
                source="gmo_cache",
                cache_path=cache_path,
                refreshed=False,
            )
        }

    monkeypatch.setattr(service, "_load_symbol_frame_results", fake_load_symbol_frame_results)
    summary = service.sync()

    assert calls == ["USD_JPY"]
    assert summary["symbols"] == 1
    assert summary["benchmarks"] == 1
    detail_categories = {detail["category"] for detail in summary["details"]}
    assert detail_categories == {"watchlist", "benchmark"}
    assert str(cache_path) == summary["details"][0]["cache_path"]


def test_sync_filters_symbols_and_emits_progress_updates(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path, strategy_name="fx_breakout_pullback"),
        overrides={
            "watchlist": {"symbols": ["USD_JPY", "EUR_JPY"], "benchmark_symbols": ["USD_JPY"], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
            },
            "strategy": {"entry_timeframe": "1Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    frame = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 3)
    cache_path = service.cache.path_for("USD_JPY", TimeFrame.MIN_1)
    calls: list[str] = []
    progress_events: list[dict[str, object]] = []

    def fake_load_symbol_frame_results(symbol, **kwargs):  # noqa: ANN001
        _ = kwargs
        calls.append(symbol)
        return {
            TimeFrame.MIN_1: MarketDataFrameLoad(
                frame=frame,
                source="gmo_cache",
                cache_path=cache_path,
                refreshed=False,
            )
        }

    monkeypatch.setattr(service, "_load_symbol_frame_results", fake_load_symbol_frame_results)

    summary = service.sync(symbols=["USD/JPY"], progress_callback=progress_events.append)

    assert calls == ["USD_JPY"]
    assert summary["selected_symbols"] == ["USD_JPY"]
    assert summary["symbols"] == 1
    assert summary["benchmarks"] == 1
    assert summary["sectors"] == 0
    assert {detail["symbol"] for detail in summary["details"]} == {"USD_JPY"}
    assert [event["phase"] for event in progress_events] == ["start", "loading", "loaded", "done"]
    assert progress_events[-1]["current"] == 1
    assert progress_events[-1]["total"] == 1


def test_runtime_load_refreshes_recent_intraday_window(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["USD_JPY"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["15Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
                "max_bars_per_symbol": 3,
            },
            "strategy": {"entry_timeframe": "15Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    cached = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 4, freq="15min")
    refreshed = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 5, freq="15min")
    service.cache.save("USD_JPY", TimeFrame.MIN_15, cached)

    calls: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    def fake_fetch_bars(symbol, timeframe, start, end, price_type="ASK"):  # noqa: ANN001
        _ = symbol, timeframe, price_type
        calls.append((pd.Timestamp(start), pd.Timestamp(end)))
        return refreshed

    monkeypatch.setattr(service.gmo, "fetch_bars", fake_fetch_bars)

    frames = service.load_runtime_symbol_frames(
        "USD_JPY",
        timeframes=[TimeFrame.MIN_15],
        as_of=pd.Timestamp("2026-04-14 10:45:00", tz=ASIA_TOKYO),
    )

    assert calls
    assert len(frames[TimeFrame.MIN_15].index) == 3
    assert frames[TimeFrame.MIN_15].index.max() == refreshed.index.max()


def test_gmo_load_only_fetches_missing_gap_between_existing_coverages(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path, strategy_name="fx_breakout_pullback"),
        overrides={
            "watchlist": {"symbols": ["USD_JPY"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
            },
            "strategy": {"entry_timeframe": "1Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    leading = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 10)
    gap = _make_quote_frame("USD_JPY", "2026-04-14 09:10:00", 10)
    trailing = _make_quote_frame("USD_JPY", "2026-04-14 09:20:00", 10)
    service.cache.save("USD_JPY", TimeFrame.MIN_1, pd.concat([leading, trailing]).sort_index())
    service.cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        leading.index.min(),
        leading.index.max() + pd.Timedelta(minutes=1),
        source_key="csv_bid",
    )
    service.cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        leading.index.min(),
        leading.index.max() + pd.Timedelta(minutes=1),
        source_key="csv_ask",
    )
    service.cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        trailing.index.min(),
        trailing.index.max() + pd.Timedelta(minutes=1),
        source_key="gmo_bid",
    )
    service.cache.record_coverage(
        "USD_JPY",
        TimeFrame.MIN_1,
        trailing.index.min(),
        trailing.index.max() + pd.Timedelta(minutes=1),
        source_key="gmo_ask",
    )

    calls: list[tuple[pd.Timestamp, pd.Timestamp, str]] = []

    def fake_fetch_bars(symbol, timeframe, start, end, price_type="ASK"):  # noqa: ANN001
        _ = symbol, timeframe
        calls.append((pd.Timestamp(start), pd.Timestamp(end), str(price_type)))
        side = "ask" if str(price_type).upper() == "ASK" else "bid"
        return pd.DataFrame(
            {
                "open": gap[f"{side}_open"],
                "high": gap[f"{side}_high"],
                "low": gap[f"{side}_low"],
                "close": gap[f"{side}_close"],
                "volume": gap[f"{side}_volume"],
                "symbol": "USD_JPY",
            },
            index=gap.index,
        )

    monkeypatch.setattr(service.gmo, "fetch_bars", fake_fetch_bars)

    frames = service.load_symbol_frames(
        "USD_JPY",
        timeframes=[TimeFrame.MIN_1],
        start="2026-04-14T09:00:00+09:00",
        end="2026-04-14T09:30:00+09:00",
    )

    gap_calls = [
        (start, end, price_type)
        for start, end, price_type in calls
        if start == pd.Timestamp("2026-04-14 09:10:00", tz=ASIA_TOKYO)
        and end == pd.Timestamp("2026-04-14 09:20:00", tz=ASIA_TOKYO)
    ]
    assert len(gap_calls) == 2
    assert {price_type for _, _, price_type in gap_calls} == {"ASK", "BID"}
    frame = frames[TimeFrame.MIN_1]
    assert len(frame.index) == 30
    assert {"bid_open", "ask_open", "spread_close"}.issubset(frame.columns)


def test_quote_quality_summary_detects_abnormal_spread():
    frame = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 20)
    frame.loc[frame.index[-1], "spread_close"] = 0.8
    summary = summarize_quote_bar_quality(frame)

    assert summary["timezone_aware"] is True
    assert summary["duplicate_timestamps"] == 0
    assert summary["negative_spread_rows"] == 0
    assert summary["abnormal_spread_rows"] == 1
    assert summary["spread_max"] >= 0.8


def test_validate_quote_bar_frame_accepts_legacy_ohlcv_without_side_volumes():
    index = pd.date_range("2026-04-14 09:00:00", periods=2, freq="1h", tz=ASIA_TOKYO)
    legacy = pd.DataFrame(
        {
            "open": [150.0, 150.1],
            "high": [150.2, 150.3],
            "low": [149.9, 150.0],
            "close": [150.1, 150.2],
            "volume": [1000.0, 1100.0],
        },
        index=index,
    )

    normalized = validate_quote_bar_frame(legacy)

    assert {"bid_volume", "ask_volume", "spread_close"}.issubset(normalized.columns)
    assert normalized["bid_volume"].tolist() == [1000.0, 1100.0]
    assert normalized["ask_volume"].tolist() == [0.0, 0.0]
    assert normalized["spread_close"].tolist() == [0.0, 0.0]


def test_gmo_sync_recovers_from_invalid_cached_frame(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path, strategy_name="fx_breakout_pullback"),
        overrides={
            "watchlist": {"symbols": ["USD_JPY"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "gmo",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Min"],
                "start_date": "2026-04-14",
                "end_date": "2026-04-14",
            },
            "strategy": {"entry_timeframe": "1Min"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    invalid = pd.DataFrame(
        {
            "open": [150.0],
            "high": [149.8],
            "low": [149.9],
            "close": [150.1],
            "volume": [1000.0],
            "symbol": ["USD_JPY"],
        },
        index=pd.date_range("2026-04-14 09:00:00", periods=1, freq="1min", tz=ASIA_TOKYO),
    )
    cache_path = service.cache.path_for("USD_JPY", TimeFrame.MIN_1)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    invalid.to_parquet(cache_path)

    refreshed = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 5)
    calls: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    def fake_fetch_quote(symbol, timeframe, start, end):  # noqa: ANN001
        _ = symbol, timeframe
        calls.append((pd.Timestamp(start), pd.Timestamp(end)))
        return refreshed

    monkeypatch.setattr(service, "_fetch_gmo_quote_bars", fake_fetch_quote)

    summary = service.sync()
    recovered = service.cache.load("USD_JPY", TimeFrame.MIN_1)

    assert calls
    assert summary["details"][0]["source"] == "gmo"
    assert recovered is not None
    assert recovered.index.max() == refreshed.index.max()
