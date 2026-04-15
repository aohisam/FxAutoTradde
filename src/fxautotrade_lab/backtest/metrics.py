"""Performance metrics."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def compute_drawdown(equity: pd.Series) -> pd.Series:
    peak = equity.cummax()
    return equity / peak - 1


def compute_metrics(
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    fills: pd.DataFrame,
    benchmark_curve: pd.DataFrame | None = None,
) -> dict[str, float | int | dict[str, float]]:
    if equity_curve.empty:
        return {}
    equity = equity_curve["equity"]
    returns = equity.pct_change(fill_method=None).dropna()
    daily_returns = equity.resample("1D").last().pct_change(fill_method=None).dropna()
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1)
    elapsed_days = max((equity.index[-1] - equity.index[0]).days, 1)
    annualized_return = float((1 + total_return) ** (365 / elapsed_days) - 1)
    daily_std = float(daily_returns.std()) if not daily_returns.empty else 0.0
    sharpe = float(np.sqrt(252) * daily_returns.mean() / daily_std) if daily_std and math.isfinite(daily_std) else 0.0
    downside = daily_returns[daily_returns < 0]
    downside_std = float(downside.std()) if not downside.empty else 0.0
    sortino = (
        float(np.sqrt(252) * daily_returns.mean() / downside_std)
        if downside_std and math.isfinite(downside_std)
        else 0.0
    )
    drawdown = compute_drawdown(equity)
    win_rate = float((trades["net_pnl"] > 0).mean()) if not trades.empty else 0.0
    gross_profit = float(trades.loc[trades["net_pnl"] > 0, "net_pnl"].sum()) if not trades.empty else 0.0
    gross_loss = float(-trades.loc[trades["net_pnl"] < 0, "net_pnl"].sum()) if not trades.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss else math.inf
    expectancy = float(trades["net_pnl"].mean()) if not trades.empty else 0.0
    average_r = float(trades["realized_r_net"].mean()) if not trades.empty and "realized_r_net" in trades.columns else 0.0
    avg_hold = float(trades["hold_bars"].mean()) if not trades.empty else 0.0
    exposure = float((equity_curve["exposure"] / equity_curve["equity"]).fillna(0.0).mean())
    turnover = float(fills["price"].mul(fills["quantity"]).sum() / equity.mean()) if not fills.empty else 0.0
    per_symbol = (
        trades.groupby("symbol")["net_pnl"].sum().sort_values(ascending=False).to_dict()
        if not trades.empty
        else {}
    )
    benchmark_relative = 0.0
    if benchmark_curve is not None and not benchmark_curve.empty:
        benchmark_relative = total_return - float(
            benchmark_curve["benchmark_equity"].iloc[-1] / benchmark_curve["benchmark_equity"].iloc[0] - 1
        )
    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": float(drawdown.min()),
        "win_rate": win_rate,
        "profit_factor": float(profit_factor if math.isfinite(profit_factor) else 999.0),
        "expectancy": expectancy,
        "average_r": average_r,
        "average_hold_bars": avg_hold,
        "exposure": exposure,
        "turnover": turnover,
        "number_of_trades": int(len(trades)),
        "per_symbol_contribution": per_symbol,
        "best_trade": float(trades["net_pnl"].max()) if not trades.empty else 0.0,
        "worst_trade": float(trades["net_pnl"].min()) if not trades.empty else 0.0,
        "benchmark_relative_performance": benchmark_relative,
    }
