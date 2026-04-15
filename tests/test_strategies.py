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
    frames = service.load_symbol_frames("USD_JPY")
    feature_set = build_multi_timeframe_feature_set(
        symbol="USD_JPY",
        bars_by_timeframe=frames,
        benchmark_bars=service.load_symbol_frames("USD_JPY"),
        sector_bars=None,
        config=config,
    )
    strategy = create_strategy(config)
    signal_frame = strategy.generate_signal_frame(feature_set.entry_frame)
    assert "signal_score" in signal_frame.columns
    assert "explanation_ja" in signal_frame.columns
    assert "sub_score_market_context" in signal_frame.columns
