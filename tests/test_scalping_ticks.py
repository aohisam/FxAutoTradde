from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from fxautotrade_lab.backtest.scalping_backtest import run_scalping_pipeline
from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.data.ticks import (
    JForexTickCsvImporter,
    ParquetTickCache,
    resample_ticks_to_quote_bars,
)


def test_jforex_tick_import_cache_and_resample(tmp_path: Path) -> None:
    csv_path = tmp_path / "USDJPY_tick_sample.csv"
    pd.DataFrame(
        {
            "Time (EET)": [
                "2026.02.01 00:00:00.000",
                "2026.02.01 00:00:00.500",
                "2026.02.01 00:00:01.000",
                "2026.02.01 00:00:02.000",
            ],
            "Ask": [150.003, 150.004, 150.000, 150.006],
            "Bid": [150.000, 150.001, 150.002, 150.003],
            "AskVolume": [1.0, 1.0, 1.0, 1.0],
            "BidVolume": [1.0, 1.0, 1.0, 1.0],
        }
    ).to_csv(csv_path, index=False)
    cache = ParquetTickCache(tmp_path / "cache")
    result = JForexTickCsvImporter(cache).import_file(csv_path, chunk_size=2)

    assert result.symbol == "USD_JPY"
    assert result.imported_rows == 3
    assert result.crossed_quotes == 1
    loaded = cache.load_window(
        "USD_JPY",
        pd.Timestamp("2026-02-01 06:00:00", tz=ASIA_TOKYO),
        pd.Timestamp("2026-02-01 08:00:00", tz=ASIA_TOKYO),
    )
    assert len(loaded) == 3
    bars = resample_ticks_to_quote_bars(loaded, rule="1s", symbol="USD_JPY")
    assert {"bid_open", "ask_open", "spread_close", "tick_count"}.issubset(bars.columns)
    assert bars["spread_close"].min() >= 0


def test_scalping_pipeline_trains_and_exports(tmp_path: Path) -> None:
    config = AppConfig()
    config.strategy.fx_scalping.tick_cache_dir = tmp_path / "tick_cache"
    config.strategy.fx_scalping.model_dir = tmp_path / "models"
    config.strategy.fx_scalping.candidate_model_dir = tmp_path / "models" / "candidates"
    config.strategy.fx_scalping.outcome_store_dir = tmp_path / "outcomes"
    config.strategy.fx_scalping.min_samples = 20
    config.strategy.fx_scalping.min_threshold_trades = 2
    config.strategy.fx_scalping.min_volatility_pips = 0.0
    config.strategy.fx_scalping.max_spread_pips = 1.0
    config.strategy.fx_scalping.take_profit_pips = 0.25
    config.strategy.fx_scalping.stop_loss_pips = 0.25
    config.strategy.fx_scalping.max_hold_seconds = 8
    config.strategy.fx_scalping.train_ratio = 0.6
    config.strategy.fx_scalping.model_promotion_enabled = False
    config.risk.starting_cash = 100_000
    config.risk.fixed_order_amount = 20_000
    config.risk.minimum_order_quantity = 1
    config.risk.quantity_step = 1

    index = pd.date_range("2026-02-01 09:00:00", periods=360, freq="1s", tz=ASIA_TOKYO)
    mid = [150.0 + 0.018 * math.sin(i / 6.0) + 0.00005 * i for i in range(len(index))]
    spread = 0.003
    ticks = pd.DataFrame(
        {
            "bid": [value - spread / 2 for value in mid],
            "ask": [value + spread / 2 for value in mid],
            "bid_volume": 1.0,
            "ask_volume": 1.0,
            "symbol": "USD_JPY",
        },
        index=index,
    )

    result = run_scalping_pipeline(
        ticks,
        symbol="USD_JPY",
        config=config,
        output_dir=tmp_path / "reports",
    )

    assert result.train_start
    assert result.test_start
    assert result.backtest.metrics["starting_equity"] == 100_000
    assert result.backtest.metrics["label_source"] == "tick"
    assert result.backtest.metrics["test_sample_count"] > 0
    assert "stress_test_summary" in result.backtest.metrics
    assert not result.stress_results.empty
    assert (tmp_path / "reports" / result.run_id / "summary.json").exists()
    assert (tmp_path / "reports" / result.run_id / "stress_results.json").exists()
    assert (tmp_path / "reports" / result.run_id / "probability_deciles.csv").exists()
    assert (tmp_path / "reports" / result.run_id / "calibration_curve.csv").exists()
    assert result.candidate_model_path is not None
    assert result.candidate_model_path.exists()
    assert result.model_bundle.metadata["threshold_selection_method"] == "replay"
    assert result.model_bundle.metadata["promoted_to_latest"] is True
    assert (tmp_path / "models" / "latest_scalping_model.json").exists()
    payload = json.loads(result.candidate_model_path.read_text(encoding="utf-8"))
    metadata = payload["metadata"]
    assert metadata["promoted_to_latest"] is True
    assert metadata["promotion_metrics"]["test_trade_count"] >= 0
    assert result.outcome_store_summary["enabled"] is True
