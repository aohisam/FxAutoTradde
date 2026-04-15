"""High-level backtest runner."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.backtest.metrics import compute_drawdown, compute_metrics
from fxautotrade_lab.backtest.fx_backtest import run_fx_backtest
from fxautotrade_lab.backtest.walk_forward import rolling_walk_forward, split_in_out_sample
from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.core.enums import BrokerMode
from fxautotrade_lab.core.models import BacktestResult
from fxautotrade_lab.data.service import MarketDataService
from fxautotrade_lab.features.fx_pipeline import build_fx_feature_set
from fxautotrade_lab.features.pipeline import build_multi_timeframe_feature_set
from fxautotrade_lab.reporting.exporters import export_backtest_artifacts
from fxautotrade_lab.simulation.fx_engine import FxQuotePortfolioSimulator
from fxautotrade_lab.simulation.engine import PortfolioSimulator
from fxautotrade_lab.strategies.fx_breakout_pullback import FxBreakoutPullbackStrategy
from fxautotrade_lab.strategies.registry import create_strategy


@dataclass(slots=True)
class BacktestRunner:
    config: AppConfig
    env: EnvironmentConfig

    def run(self) -> BacktestResult:
        backtest_start, backtest_end = self._backtest_window()
        if self.config.strategy.name == FxBreakoutPullbackStrategy.name:
            return run_fx_backtest(
                self.config,
                self.env,
                backtest_start=backtest_start,
                backtest_end=backtest_end,
            )
        data_service = MarketDataService(self.config, self.env)
        bundle = data_service.load_bundle(start=backtest_start, end=backtest_end)
        strategy = create_strategy(self.config)
        signal_frames: dict[str, pd.DataFrame] = {}
        signal_logs: list[pd.DataFrame] = []
        chart_frames: dict[str, dict[str, pd.DataFrame]] = {}
        benchmark_curve = None
        benchmark_symbol = self.config.watchlist.benchmark_symbols[0] if self.config.watchlist.benchmark_symbols else None
        benchmark_entry_frame = None
        if benchmark_symbol:
            benchmark_frames = bundle.benchmarks.get(benchmark_symbol, {})
            if benchmark_frames:
                benchmark_entry_frame = benchmark_frames[self.config.strategy.entry_timeframe]
        sector_symbol = self.config.watchlist.sector_symbols[0] if self.config.watchlist.sector_symbols else None
        sector_frames = bundle.sectors.get(sector_symbol, {}) if sector_symbol else None

        for symbol, frames in bundle.symbols.items():
            if strategy.name == FxBreakoutPullbackStrategy.name:
                fx_feature_set = build_fx_feature_set(
                    symbol=symbol,
                    bars_by_timeframe=frames,
                    config=self.config,
                )
                signals = strategy.generate_signal_frame(fx_feature_set.execution_frame)
                signal_frames[symbol] = signals
                chart_frames[symbol] = {
                    self.config.strategy.fx_breakout_pullback.execution_timeframe.value: signals.copy(),
                    self.config.strategy.fx_breakout_pullback.signal_timeframe.value: fx_feature_set.signal_frame.copy(),
                    self.config.strategy.fx_breakout_pullback.trend_timeframe.value: fx_feature_set.trend_frame.copy(),
                }
            else:
                feature_set = build_multi_timeframe_feature_set(
                    symbol=symbol,
                    bars_by_timeframe=frames,
                    benchmark_bars=bundle.benchmarks.get(benchmark_symbol, {}) if benchmark_symbol else None,
                    sector_bars=sector_frames,
                    config=self.config,
                )
                signals = strategy.generate_signal_frame(feature_set.entry_frame)
                signal_frames[symbol] = signals
                chart_frames[symbol] = {
                    self.config.strategy.entry_timeframe.value: feature_set.entry_frame.copy(),
                    "1Day": feature_set.daily_frame.copy(),
                    "1Week": feature_set.weekly_frame.copy(),
                    "1Month": feature_set.monthly_frame.copy(),
                }
            signal_logs.append(
                signals.reset_index()
                .rename(columns={"index": "timestamp"})
                .assign(symbol=symbol, strategy_name=strategy.name)
            )
        sim = (
            FxQuotePortfolioSimulator(self.config)
            if strategy.name == FxBreakoutPullbackStrategy.name
            else PortfolioSimulator(self.config)
        )
        sim_outputs = sim.run(signal_frames, mode=self.config.broker.mode)
        equity_curve = sim_outputs["equity_curve"]
        if not equity_curve.empty:
            equity_curve["drawdown"] = compute_drawdown(equity_curve["equity"])
        drawdown_curve = equity_curve[["drawdown"]].copy() if "drawdown" in equity_curve.columns else pd.DataFrame()
        signals_frame = (
            pd.concat(signal_logs, ignore_index=True).sort_values("timestamp")
            if signal_logs
            else pd.DataFrame()
        )
        if benchmark_entry_frame is not None and not benchmark_entry_frame.empty and not equity_curve.empty:
            benchmark_prices = benchmark_entry_frame.reindex(equity_curve.index, method="ffill")
            benchmark_curve = pd.DataFrame(
                {
                    "benchmark_equity": self.config.risk.starting_cash
                    * (benchmark_prices["close"] / benchmark_prices["close"].iloc[0])
                },
                index=equity_curve.index,
            )
        metrics = compute_metrics(
            equity_curve=equity_curve,
            trades=sim_outputs["trades"],
            fills=sim_outputs["fills"],
            benchmark_curve=benchmark_curve,
        )
        in_sample_equity, out_sample_equity = split_in_out_sample(
            equity_curve, self.config.validation.in_sample_ratio
        )
        in_sample_metrics = compute_metrics(
            in_sample_equity,
            sim_outputs["trades"],
            sim_outputs["fills"],
            benchmark_curve=None,
        )
        out_of_sample_metrics = compute_metrics(
            out_sample_equity,
            sim_outputs["trades"],
            sim_outputs["fills"],
            benchmark_curve=None,
        )
        walk_forward = rolling_walk_forward(
            equity_curve=equity_curve,
            trades=sim_outputs["trades"],
            fills=sim_outputs["fills"],
            windows=self.config.validation.rolling_windows,
        )
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
        result = BacktestResult(
            run_id=run_id,
            strategy_name=strategy.name,
            mode=self.config.broker.mode,
            symbols=self.config.watchlist.symbols,
            backtest_start=backtest_start,
            backtest_end=backtest_end,
            starting_cash=self.config.risk.starting_cash,
            metrics=metrics,
            equity_curve=equity_curve,
            drawdown_curve=drawdown_curve,
            trades=sim_outputs["trades"],
            orders=sim_outputs["orders"],
            fills=sim_outputs["fills"],
            positions=sim_outputs["positions"],
            signals=signals_frame,
            benchmark_curve=benchmark_curve,
            in_sample_metrics=in_sample_metrics,
            out_of_sample_metrics=out_of_sample_metrics,
            walk_forward=walk_forward,
            chart_frames=chart_frames,
        )
        output_dir = export_backtest_artifacts(result, self.config)
        result.output_dir = str(output_dir)
        return result

    def _backtest_window(self) -> tuple[str, str]:
        if self.config.backtest.use_custom_window:
            start = self.config.backtest.start_date or self.config.data.start_date
            end = self.config.backtest.end_date or self.config.data.end_date
            return start, end
        return self.config.data.start_date, self.config.data.end_date
