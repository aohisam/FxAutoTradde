"""Market context from benchmarks, session, and liquidity."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.data.session import get_session_state
from fxautotrade_lab.features.indicators import atr, ema, rolling_return
from fxautotrade_lab.features.relative_strength import relative_strength


class MarketContextBuilder:
    """Build explainable context columns for strategies."""

    def build(
        self,
        symbol_frame: pd.DataFrame,
        benchmark_frame: pd.DataFrame | None,
        sector_frame: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        enriched = symbol_frame.copy()
        enriched["session_label_ja"] = [get_session_state(ts).label_ja for ts in enriched.index]
        enriched["is_regular_session"] = [get_session_state(ts).is_regular_session for ts in enriched.index]
        enriched["gap_risk"] = enriched["gap_pct"].abs().clip(0.0, 0.1)
        enriched["liquidity_score"] = (
            (enriched["dollar_volume"].rolling(20).mean() / 5_000_000).clip(0.0, 2.0) / 2.0
        ).fillna(0.0)
        if benchmark_frame is not None and not benchmark_frame.empty:
            benchmark = benchmark_frame.reindex(enriched.index, method="ffill")
            enriched["benchmark_trend"] = (
                (benchmark["close"] > ema(benchmark["close"], 50))
                & (ema(benchmark["close"], 50) > ema(benchmark["close"], 200))
            ).astype(float)
            enriched["benchmark_volatility"] = (atr(benchmark, 14) / benchmark["close"]).fillna(0.0)
            enriched["relative_strength"] = relative_strength(enriched["close"], benchmark["close"], 20)
            enriched["breadth_proxy"] = rolling_return(benchmark["close"], 5).gt(0).astype(float)
        else:
            enriched["benchmark_trend"] = 0.0
            enriched["benchmark_volatility"] = 0.0
            enriched["relative_strength"] = 0.0
            enriched["breadth_proxy"] = 0.0
        if sector_frame is not None and not sector_frame.empty:
            sector = sector_frame.reindex(enriched.index, method="ffill")
            enriched["sector_relative_strength"] = relative_strength(enriched["close"], sector["close"], 15)
        else:
            enriched["sector_relative_strength"] = 0.0
        return enriched
