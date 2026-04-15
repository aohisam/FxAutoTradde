from __future__ import annotations

import numpy as np
import pandas as pd

from fxautotrade_lab.config.loader import load_app_config
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.quote_bars import build_quote_bar_frame, summarize_quote_bar_quality
from fxautotrade_lab.data.service import MarketDataService

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
        write_config(tmp_path),
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


def test_quote_quality_summary_detects_abnormal_spread():
    frame = _make_quote_frame("USD_JPY", "2026-04-14 09:00:00", 20)
    frame.loc[frame.index[-1], "spread_close"] = 0.8
    summary = summarize_quote_bar_quality(frame)

    assert summary["timezone_aware"] is True
    assert summary["duplicate_timestamps"] == 0
    assert summary["negative_spread_rows"] == 0
    assert summary["abnormal_spread_rows"] == 1
    assert summary["spread_max"] >= 0.8
