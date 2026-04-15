"""One-click FX research pipeline with resumable step cache."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml

from fxautotrade_lab.backtest.fx_backtest import run_fx_backtest, train_fx_filter_model_run
from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig
from fxautotrade_lab.data.quote_bars import summarize_quote_bar_quality
from fxautotrade_lab.data.service import MarketDataService


def _json_default(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _sanitize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in metrics.items():
        if isinstance(value, dict):
            sanitized[key] = _sanitize_metrics(value)
        elif isinstance(value, (list, tuple)):
            sanitized[key] = [item if not isinstance(item, Path) else str(item) for item in value]
        elif isinstance(value, Path):
            sanitized[key] = str(value)
        else:
            sanitized[key] = value
    return sanitized


def _yearly_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades is None or trades.empty:
        return pd.DataFrame(columns=["year", "net_pnl", "trade_count", "average_r"])
    working = trades.copy()
    working["year"] = pd.to_datetime(working["exit_time"]).dt.year
    return (
        working.groupby("year", as_index=False)
        .agg(
            net_pnl=("net_pnl", "sum"),
            trade_count=("symbol", "count"),
            average_r=("realized_r_net", "mean"),
        )
        .sort_values("year")
    )


def _hourly_summary(signals: pd.DataFrame) -> pd.DataFrame:
    if signals is None or signals.empty:
        return pd.DataFrame(columns=["hour", "rule_candidates", "accepted_candidates"])
    working = signals.copy()
    working["hour"] = pd.to_datetime(working["timestamp"]).dt.hour
    return (
        working.groupby("hour", as_index=False)
        .agg(
            rule_candidates=("entry_signal_rule_only", "sum"),
            accepted_candidates=("entry_signal", "sum"),
        )
        .sort_values("hour")
    )


def _monthly_equity_summary(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve is None or equity_curve.empty:
        return pd.DataFrame(columns=["month", "return"])
    monthly = equity_curve["equity"].resample("ME").last().pct_change(fill_method=None)
    frame = monthly.rename("return").reset_index()
    return frame.rename(columns={frame.columns[0]: "month"})


@dataclass(slots=True)
class ResearchPipeline:
    config: AppConfig
    env: EnvironmentConfig
    mode: str | None = None
    logs: list[str] = field(default_factory=list)
    steps: list[dict[str, object]] = field(default_factory=list)

    def run(self) -> dict[str, object]:
        selected_mode = (self.mode or self.config.research.mode).strip().lower()
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_research"
        output_dir = self.config.research.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir = self._cache_dir(selected_mode)
        cache_dir.mkdir(parents=True, exist_ok=True)

        validation = self._step(
            "validate",
            cache_dir / "validate.json",
            lambda: self._validate_data(),
        )
        train_summary = self._step(
            "train",
            cache_dir / "train.json",
            lambda: self._train_summary(),
        )
        baseline_result = self._run_backtest_variant(
            mode_name="rule_only",
            output_dir=output_dir,
            ml_enabled=False,
            backtest_mode="rule_only",
        )
        selected_result = self._run_backtest_variant(
            mode_name="walk_forward_train",
            output_dir=output_dir,
            ml_enabled=True,
            backtest_mode="walk_forward_train",
        )
        robustness = self._step(
            "robustness",
            cache_dir / f"robustness_{selected_mode}.json",
            lambda: self._robustness_runs(selected_mode),
        )
        sensitivity = self._step(
            "sensitivity",
            cache_dir / f"sensitivity_{selected_mode}.json",
            lambda: self._parameter_sensitivity(selected_mode),
        )

        summary = {
            "run_id": run_id,
            "mode": selected_mode,
            "output_dir": str(output_dir),
            "cache_dir": str(cache_dir),
            "steps": self.steps,
            "logs": self.logs,
            "validation": validation,
            "train_summary": train_summary,
            "baseline_metrics": _sanitize_metrics(baseline_result.metrics),
            "selected_metrics": _sanitize_metrics(selected_result.metrics),
            "uplift": {
                "total_return_delta": selected_result.metrics.get("total_return", 0.0)
                - baseline_result.metrics.get("total_return", 0.0),
                "profit_factor_delta": selected_result.metrics.get("profit_factor", 0.0)
                - baseline_result.metrics.get("profit_factor", 0.0),
                "average_r_delta": selected_result.metrics.get("average_r", 0.0)
                - baseline_result.metrics.get("average_r", 0.0),
            },
            "artifacts": {
                "baseline_output_dir": baseline_result.output_dir,
                "selected_output_dir": selected_result.output_dir,
            },
            "robustness": robustness,
            "sensitivity": sensitivity,
        }
        self._write_reports(output_dir, summary, baseline_result, selected_result)
        return summary

    def _cache_dir(self, selected_mode: str) -> Path:
        payload = json.dumps(
            {
                "symbols": self.config.watchlist.symbols,
                "start": self.config.data.start_date,
                "end": self.config.data.end_date,
                "mode": selected_mode,
                "strategy": self.config.strategy.fx_breakout_pullback.model_dump(mode="json"),
            },
            ensure_ascii=False,
            default=_json_default,
            sort_keys=True,
        )
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
        return self.config.research.cache_dir / digest

    def _step(self, name: str, cache_path: Path, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        use_cache = self.config.research.reuse_cached_steps and cache_path.exists()
        if use_cache:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            self.logs.append(f"{name}: 既存キャッシュを再利用しました")
            self.steps.append({"step": name, "status": "cached", "path": str(cache_path)})
            return payload
        self.logs.append(f"{name}: 実行を開始します")
        try:
            payload = fn()
        except Exception as exc:
            self.steps.append({"step": name, "status": "failed", "error": str(exc)})
            raise
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        self.steps.append({"step": name, "status": "completed", "path": str(cache_path)})
        return payload

    def _validate_data(self) -> dict[str, Any]:
        data_service = MarketDataService(self.config, self.env)
        rows: list[dict[str, Any]] = []
        for symbol in self.config.watchlist.symbols:
            frames = data_service.load_symbol_frames(
                symbol,
                start=self.config.data.start_date,
                end=self.config.data.end_date,
            )
            one_min = frames[self.config.strategy.fx_breakout_pullback.execution_timeframe]
            rows.append(
                {
                    "symbol": symbol,
                    "rows": int(len(one_min.index)),
                    "start": one_min.index.min().isoformat() if not one_min.empty else "",
                    "end": one_min.index.max().isoformat() if not one_min.empty else "",
                    "has_bid_ask": bool({"bid_open", "ask_open", "spread_close"}.issubset(one_min.columns)),
                    **summarize_quote_bar_quality(one_min),
                }
            )
        return {"symbols": rows}

    def _train_summary(self) -> dict[str, Any]:
        train_config = self.config.model_copy(deep=True)
        train_config.strategy.fx_breakout_pullback.ml_filter.enabled = True
        return train_fx_filter_model_run(train_config, self.env)

    def _run_backtest_variant(
        self,
        *,
        mode_name: str,
        output_dir: Path,
        ml_enabled: bool,
        backtest_mode: str,
        spread_multiplier: float = 1.0,
        entry_delay_bars: int = 0,
        breakout_lookback: int | None = None,
        atr_stop_mult: float | None = None,
    ):
        variant = self.config.model_copy(deep=True)
        variant.reporting.output_dir = output_dir / mode_name
        variant.strategy.fx_breakout_pullback.ml_filter.enabled = ml_enabled
        variant.strategy.fx_breakout_pullback.ml_filter.backtest_mode = backtest_mode
        variant.strategy.fx_breakout_pullback.spread_stress_multiplier = spread_multiplier
        variant.strategy.fx_breakout_pullback.entry_delay_bars = entry_delay_bars
        if breakout_lookback is not None:
            variant.strategy.fx_breakout_pullback.breakout_lookback = breakout_lookback
        if atr_stop_mult is not None:
            variant.strategy.fx_breakout_pullback.atr_stop_mult = atr_stop_mult
        return run_fx_backtest(
            variant,
            self.env,
            backtest_start=variant.backtest.start_date or variant.data.start_date,
            backtest_end=variant.backtest.end_date or variant.data.end_date,
        )

    def _robustness_runs(self, selected_mode: str) -> dict[str, Any]:
        spread_values = self.config.research.spread_stress_multipliers
        delay_values = self.config.research.entry_delay_scenarios
        if selected_mode == "quick":
            spread_values = spread_values[:2]
            delay_values = delay_values[:2]
        rows: list[dict[str, Any]] = []
        for spread_multiplier in spread_values:
            for delay_bars in delay_values:
                result = self._run_backtest_variant(
                    mode_name=f"robustness_s{spread_multiplier:.1f}_d{delay_bars}",
                    output_dir=self.config.research.output_dir / "tmp",
                    ml_enabled=True,
                    backtest_mode="walk_forward_train",
                    spread_multiplier=spread_multiplier,
                    entry_delay_bars=delay_bars,
                )
                rows.append(
                    {
                        "spread_multiplier": spread_multiplier,
                        "entry_delay_bars": delay_bars,
                        "total_return": result.metrics.get("total_return", 0.0),
                        "profit_factor": result.metrics.get("profit_factor", 0.0),
                        "average_r": result.metrics.get("average_r", 0.0),
                        "max_drawdown": result.metrics.get("max_drawdown", 0.0),
                        "output_dir": result.output_dir,
                    }
                )
        return {"rows": rows}

    def _parameter_sensitivity(self, selected_mode: str) -> dict[str, Any]:
        if selected_mode == "quick":
            return {"rows": []}
        rows: list[dict[str, Any]] = []
        breakout_values = self.config.research.parameter_sensitivity_breakout
        stop_values = self.config.research.parameter_sensitivity_stop
        if selected_mode == "standard":
            breakout_values = breakout_values[:2]
            stop_values = stop_values[:2]
        for breakout_lookback in breakout_values:
            for atr_stop_mult in stop_values:
                result = self._run_backtest_variant(
                    mode_name=f"sensitivity_b{breakout_lookback}_s{atr_stop_mult:.1f}",
                    output_dir=self.config.research.output_dir / "tmp",
                    ml_enabled=True,
                    backtest_mode="walk_forward_train",
                    breakout_lookback=breakout_lookback,
                    atr_stop_mult=atr_stop_mult,
                )
                rows.append(
                    {
                        "breakout_lookback": breakout_lookback,
                        "atr_stop_mult": atr_stop_mult,
                        "total_return": result.metrics.get("total_return", 0.0),
                        "profit_factor": result.metrics.get("profit_factor", 0.0),
                        "average_r": result.metrics.get("average_r", 0.0),
                        "output_dir": result.output_dir,
                    }
                )
        return {"rows": rows}

    def _write_reports(self, output_dir: Path, summary: dict[str, Any], baseline_result, selected_result) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "research_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )
        (output_dir / "research_summary.yaml").write_text(
            yaml.safe_dump(summary, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        pd.DataFrame(summary["robustness"].get("rows", [])).to_csv(output_dir / "robustness.csv", index=False)
        pd.DataFrame(summary["sensitivity"].get("rows", [])).to_csv(output_dir / "sensitivity.csv", index=False)
        _yearly_summary(selected_result.trades).to_csv(output_dir / "yearly_summary.csv", index=False)
        _monthly_equity_summary(selected_result.equity_curve).to_csv(output_dir / "monthly_summary.csv", index=False)
        _hourly_summary(selected_result.signals).to_csv(output_dir / "hourly_signal_summary.csv", index=False)
        (output_dir / "summary.md").write_text(
            "\n".join(
                [
                    "# FX Research Summary",
                    "",
                    f"- Run ID: {summary['run_id']}",
                    f"- Mode: {summary['mode']}",
                    f"- Data Range: {self.config.data.start_date} - {self.config.data.end_date}",
                    f"- Symbols: {', '.join(self.config.watchlist.symbols)}",
                    f"- Baseline Total Return: {baseline_result.metrics.get('total_return', 0.0):.2%}",
                    f"- Selected Total Return: {selected_result.metrics.get('total_return', 0.0):.2%}",
                    f"- Uplift: {summary['uplift']['total_return_delta']:.2%}",
                    f"- Baseline Output: {baseline_result.output_dir}",
                    f"- Selected Output: {selected_result.output_dir}",
                    "",
                    "## Notes",
                    "",
                    "- ML は参加許可フィルタとしてのみ使っています。",
                    "- ラベルは realized_r_net を基準に作成しています。",
                    "- quick / standard / exhaustive は頑健性シナリオ数のみを変えます。",
                ]
            ),
            encoding="utf-8",
        )
