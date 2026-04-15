"""Relative strength features."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.features.indicators import rolling_return


def relative_strength(symbol_close: pd.Series, benchmark_close: pd.Series, periods: int = 20) -> pd.Series:
    aligned = pd.concat(
        [
            rolling_return(symbol_close, periods).rename("symbol"),
            rolling_return(benchmark_close, periods).rename("benchmark"),
        ],
        axis=1,
    ).dropna()
    rs = aligned["symbol"] - aligned["benchmark"]
    return rs.reindex(symbol_close.index).ffill().fillna(0.0)
