from __future__ import annotations

from fxautotrade_lab.config.loader import load_app_config, load_environment
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from fxautotrade_lab.strategies.registry import create_strategy

from tests.conftest import write_config


def test_scoring_strategy_generates_columns(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    service = MarketDataService(config, env)
    frames = service.load_symbol_frames("AAPL")
    feature_set = build_multi_timeframe_feature_set(
        symbol="AAPL",
        bars_by_timeframe=frames,
        benchmark_bars=service.load_symbol_frames("SPY"),
        sector_bars=service.load_symbol_frames("XLK"),
        config=config,
    )
    strategy = create_strategy(config)
    signal_frame = strategy.generate_signal_frame(feature_set.entry_frame)
    assert "signal_score" in signal_frame.columns
    assert "explanation_ja" in signal_frame.columns
    assert "sub_score_market_context" in signal_frame.columns
