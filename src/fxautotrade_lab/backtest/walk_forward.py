"""Walk-forward summary helpers."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.backtest.metrics import compute_metrics


def split_in_out_sample(
    equity_curve: pd.DataFrame, ratio: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if equity_curve.empty:
        return equity_curve, equity_curve
    split_index = max(1, int(len(equity_curve) * ratio))
    return equity_curve.iloc[:split_index], equity_curve.iloc[split_index:]


def rolling_walk_forward(
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    fills: pd.DataFrame,
    windows: int,
) -> list[dict[str, object]]:
    if equity_curve.empty or windows <= 0:
        return []
    window_size = max(1, len(equity_curve) // windows)
    summaries: list[dict[str, object]] = []
    for idx in range(windows):
        start = idx * window_size
        end = len(equity_curve) if idx == windows - 1 else (idx + 1) * window_size
        slice_equity = equity_curve.iloc[start:end]
        if slice_equity.empty:
            continue
        start_ts = slice_equity.index[0]
        end_ts = slice_equity.index[-1]
        slice_trades = (
            trades.loc[(trades["entry_time"] >= start_ts) & (trades["exit_time"] <= end_ts)]
            if not trades.empty
            else trades
        )
        slice_fills = (
            fills.loc[(fills["timestamp"] >= start_ts) & (fills["timestamp"] <= end_ts)]
            if not fills.empty
            else fills
        )
        summaries.append(
            {
                "window": idx + 1,
                "start": str(start_ts),
                "end": str(end_ts),
                "metrics": compute_metrics(slice_equity, slice_trades, slice_fills),
            }
        )
    return summaries
