from __future__ import annotations

from fxautotrade_lab.config.loader import load_app_config, load_environment
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.candles import bullish_engulfing, doji, hammer, inside_bar
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from tests.conftest import write_config


def test_daily_features_are_backward_aligned(tmp_path):
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
    aligned = feature_set.entry_frame.dropna(subset=["daily_ema_50"])
    sample = aligned.iloc[min(50, len(aligned) - 1)]
    daily_up_to_point = feature_set.daily_frame.loc[feature_set.daily_frame.index <= sample.name]
    assert not daily_up_to_point.empty
    expected = daily_up_to_point.iloc[-1]["daily_ema_50"]
    assert sample["daily_ema_50"] == expected


def test_candle_feature_functions_return_numeric_series(tmp_path):
    config = load_app_config(write_config(tmp_path))
    env = load_environment()
    service = MarketDataService(config, env)
    frame = service.load_symbol_frames("USD_JPY")[config.strategy.entry_timeframe].head(100)
    for series in [bullish_engulfing(frame), hammer(frame), inside_bar(frame), doji(frame)]:
        assert len(series) == len(frame)
        assert series.dtype.kind in {"f", "i", "b"}
