from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from fxautotrade_lab.config.models import BrokerConfig
from fxautotrade_lab.config.loader import load_app_config
from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import US_EASTERN
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.alpaca import AlpacaHistoricalDataClient
from fxautotrade_lab.data.service import MarketDataService

from tests.conftest import write_config


def _make_daily_frame(symbol: str, start: str, periods: int) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq="B", tz=US_EASTERN) + pd.Timedelta(hours=16)
    frame = pd.DataFrame(
        {
            "open": [100.0 + step for step in range(periods)],
            "high": [101.0 + step for step in range(periods)],
            "low": [99.0 + step for step in range(periods)],
            "close": [100.5 + step for step in range(periods)],
            "volume": [1_000_000 + step * 1_000 for step in range(periods)],
            "symbol": [symbol.upper()] * periods,
        },
        index=index,
    )
    return frame


def _make_intraday_frame(symbol: str, start: str, periods: int, freq: str = "15min") -> pd.DataFrame:
    index = pd.date_range(start=start, periods=periods, freq=freq, tz=US_EASTERN)
    frame = pd.DataFrame(
        {
            "open": [100.0 + step for step in range(periods)],
            "high": [101.0 + step for step in range(periods)],
            "low": [99.0 + step for step in range(periods)],
            "close": [100.5 + step for step in range(periods)],
            "volume": [100_000 + step * 500 for step in range(periods)],
            "symbol": [symbol.upper()] * periods,
        },
        index=index,
    )
    return frame


def test_sync_force_refreshes_alpaca_cache_and_reports_details(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "alpaca",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Day", "1Week", "1Month"],
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
            },
            "strategy": {"entry_timeframe": "1Day"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    cached_middle = _make_daily_frame("AAPL", "2024-01-10", 8)
    expected_before = _make_daily_frame("AAPL", "2024-01-02", 6)
    expected_after = _make_daily_frame("AAPL", "2024-01-22", 50)
    service.cache.save("AAPL", TimeFrame.DAY_1, cached_middle)
    service.cache.save_metadata(
        "AAPL",
        TimeFrame.DAY_1,
        {"source": "alpaca", "timeframe": "1Day", "version": 2, "adjustment": "split", "feed": "iex"},
    )

    calls: list[tuple[str, TimeFrame]] = []

    def fake_fetch(self, symbol: str, timeframe: TimeFrame, start, end) -> pd.DataFrame:  # noqa: ANN001
        calls.append((symbol, timeframe))
        if pd.Timestamp(end) <= cached_middle.index.min().normalize():
            return expected_before
        return expected_after

    monkeypatch.setattr(AlpacaHistoricalDataClient, "fetch_bars", fake_fetch)

    summary = service.sync()

    cached = service.cache.load("AAPL", TimeFrame.DAY_1)
    assert cached is not None
    assert cached.index.min() == expected_before.index.min()
    assert cached.index.max() == expected_after.index.max()
    assert calls == [("AAPL", TimeFrame.DAY_1), ("AAPL", TimeFrame.DAY_1)]
    assert summary["force_refresh"] is False
    assert summary["sync_mode"] == "incremental"
    assert summary["symbols"] == 1
    details = {row["timeframe"]: row for row in summary["details"]}
    assert details["1Day"]["source"] == "alpaca_incremental"
    assert details["1Week"]["source"] == "derived_from_1Day"
    assert details["1Month"]["source"] == "derived_from_1Day"
    assert details["1Day"]["rows"] == len(cached.index)
    assert details["1Week"]["rows"] >= 1


def test_load_symbol_frames_derives_week_and_month_timeframes(tmp_path):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "fixture",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Week", "1Month"],
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
            },
            "strategy": {"entry_timeframe": "1Day"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())

    frames = service.load_symbol_frames("AAPL", timeframes=[TimeFrame.WEEK_1, TimeFrame.MONTH_1])

    assert TimeFrame.DAY_1 in frames
    assert TimeFrame.WEEK_1 in frames
    assert TimeFrame.MONTH_1 in frames
    assert not frames[TimeFrame.DAY_1].empty
    assert not frames[TimeFrame.WEEK_1].empty
    assert not frames[TimeFrame.MONTH_1].empty
    assert service.cache.path_for("AAPL", TimeFrame.WEEK_1).exists()
    assert service.cache.path_for("AAPL", TimeFrame.MONTH_1).exists()


def test_runtime_load_uses_cache_seed_and_refreshes_recent_intraday_window(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "alpaca",
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
    cached = _make_intraday_frame("AAPL", "2026-04-14 09:30:00", 4)
    service.cache.save("AAPL", TimeFrame.MIN_15, cached)

    refreshed = _make_intraday_frame("AAPL", "2026-04-14 09:30:00", 5)
    calls: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    def fake_fetch(self, symbol: str, timeframe: TimeFrame, start, end) -> pd.DataFrame:  # noqa: ANN001
        calls.append((pd.Timestamp(start), pd.Timestamp(end)))
        return refreshed

    monkeypatch.setattr(AlpacaHistoricalDataClient, "fetch_bars", fake_fetch)

    frames = service.load_runtime_symbol_frames(
        "AAPL",
        timeframes=[TimeFrame.MIN_15],
        as_of=pd.Timestamp("2026-04-14 10:45:00", tz=US_EASTERN),
    )

    assert calls
    assert TimeFrame.MIN_15 in frames
    assert len(frames[TimeFrame.MIN_15].index) == 3
    assert frames[TimeFrame.MIN_15].index.max() == refreshed.index.max()
    cached_after = service.cache.load("AAPL", TimeFrame.MIN_15)
    assert cached_after is not None
    assert cached_after.index.max() == refreshed.index.max()


def test_alpaca_requests_split_adjustment(monkeypatch):
    client = AlpacaHistoricalDataClient(EnvironmentConfig(), BrokerConfig())
    captured: dict[str, object] = {}
    expected = _make_daily_frame("AAPL", "2024-01-02", 3)

    class FakeHistoricalClient:
        def get_stock_bars(self, request):  # noqa: ANN001
            captured["adjustment"] = getattr(request, "adjustment", None)
            return SimpleNamespace(df=pd.DataFrame())

    monkeypatch.setattr(AlpacaHistoricalDataClient, "_historical_client", lambda self: FakeHistoricalClient())
    monkeypatch.setattr(
        AlpacaHistoricalDataClient,
        "_normalize_bar_response",
        lambda self, response, symbol, start: expected,  # noqa: ARG005
    )

    frame = client.fetch_bars(
        "AAPL",
        TimeFrame.DAY_1,
        pd.Timestamp("2024-01-01", tz=US_EASTERN).to_pydatetime(),
        pd.Timestamp("2024-01-31", tz=US_EASTERN).to_pydatetime(),
    )

    assert captured["adjustment"] is not None
    assert str(captured["adjustment"]) == "Adjustment.SPLIT" or getattr(captured["adjustment"], "value", "") == "split"
    pd.testing.assert_frame_equal(frame, expected)


def test_alpaca_cache_metadata_invalidates_unadjusted_cache(tmp_path, monkeypatch):
    config = load_app_config(
        write_config(tmp_path),
        overrides={
            "watchlist": {"symbols": ["AAPL"], "benchmark_symbols": [], "sector_symbols": []},
            "data": {
                "source": "alpaca",
                "cache_dir": str(tmp_path / "cache"),
                "timeframes": ["1Day", "1Week"],
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
            },
            "strategy": {"entry_timeframe": "1Day"},
        },
    )
    service = MarketDataService(config, EnvironmentConfig())
    stale_daily = _make_daily_frame("AAPL", "2024-01-02", 5)
    service.cache.save("AAPL", TimeFrame.DAY_1, stale_daily)
    service.cache.save_metadata(
        "AAPL",
        TimeFrame.DAY_1,
        {"source": "alpaca", "timeframe": "1Day", "version": 1, "adjustment": "raw", "feed": "iex"},
    )
    service.cache.save(
        "AAPL",
        TimeFrame.WEEK_1,
        pd.DataFrame(
            {
                "open": [999.0],
                "high": [999.0],
                "low": [999.0],
                "close": [999.0],
                "volume": [999.0],
                "symbol": ["AAPL"],
            },
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-05 16:00:00", tz=US_EASTERN)]),
        ),
    )
    service.cache.save_metadata(
        "AAPL",
        TimeFrame.WEEK_1,
        {"source": "derived", "timeframe": "1Week", "base_timeframe": "1Day", "version": 1, "adjustment": "raw"},
    )
    refreshed_daily = _make_daily_frame("AAPL", "2024-01-02", 20)
    calls: list[tuple[str, TimeFrame]] = []

    def fake_fetch(self, symbol: str, timeframe: TimeFrame, start, end) -> pd.DataFrame:  # noqa: ANN001
        calls.append((symbol, timeframe))
        return refreshed_daily

    monkeypatch.setattr(AlpacaHistoricalDataClient, "fetch_bars", fake_fetch)

    frames = service.load_symbol_frames("AAPL", timeframes=[TimeFrame.DAY_1, TimeFrame.WEEK_1])

    assert calls == [("AAPL", TimeFrame.DAY_1)]
    assert frames[TimeFrame.DAY_1].iloc[-1]["close"] == refreshed_daily.iloc[-1]["close"]
    assert frames[TimeFrame.WEEK_1].iloc[0]["close"] != 999.0
    assert service.cache.load_metadata("AAPL", TimeFrame.DAY_1)["adjustment"] == "split"
    assert service.cache.load_metadata("AAPL", TimeFrame.WEEK_1)["adjustment"] == "split"
