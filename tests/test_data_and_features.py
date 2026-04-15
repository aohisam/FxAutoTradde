from __future__ import annotations

from fxautotrade_lab.config.loader import load_app_config, load_environment
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set

from tests.conftest import write_config


def test_fixture_data_and_feature_pipeline(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    service = MarketDataService(config, env)
    frames = service.load_symbol_frames("AAPL")
    assert "close" in frames[config.strategy.entry_timeframe].columns
    feature_set = build_multi_timeframe_feature_set(
        symbol="AAPL",
        bars_by_timeframe=frames,
        benchmark_bars=service.load_symbol_frames("SPY"),
        sector_bars=service.load_symbol_frames("XLK"),
        config=config,
    )
    frame = feature_set.entry_frame
    assert "daily_ema_50" in frame.columns
    assert "relative_strength" in frame.columns
    assert frame.index.is_monotonic_increasing
