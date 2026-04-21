"""One-click FX research pipeline with resumable step cache."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
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


def _regime_summary(signals: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if signals is None or signals.empty:
        return pd.DataFrame(columns=["regime", "rule_candidates", "accepted_candidates", "trade_count", "net_pnl", "average_r"])
    signal_working = signals.copy()
    regime_source = signal_working["regime_label"] if "regime_label" in signal_working.columns else pd.Series("unknown", index=signal_working.index)
    signal_working["regime"] = regime_source.fillna("unknown").astype(str)
    signal_summary = (
        signal_working.groupby("regime", as_index=False)
        .agg(
            rule_candidates=("entry_signal_rule_only", "sum"),
            accepted_candidates=("entry_signal", "sum"),
        )
        .sort_values("regime")
    )
    if trades is None or trades.empty:
        signal_summary["trade_count"] = 0
        signal_summary["net_pnl"] = 0.0
        signal_summary["average_r"] = 0.0
        return signal_summary
    trade_working = trades.copy()
    if "signal_time" not in trade_working.columns:
        signal_summary["trade_count"] = 0
        signal_summary["net_pnl"] = 0.0
        signal_summary["average_r"] = 0.0
        return signal_summary
    trade_working["signal_time"] = pd.to_datetime(trade_working["signal_time"], errors="coerce")
    signal_regimes = (
        signal_working.reset_index()
        .rename(columns={"index": "timestamp"})
        .assign(timestamp=lambda frame: pd.to_datetime(frame["timestamp"], errors="coerce"))
        .loc[:, ["timestamp", "regime"]]
    )
    trade_with_regime = trade_working.merge(
        signal_regimes,
        left_on="signal_time",
        right_on="timestamp",
        how="left",
    )
    trade_with_regime["regime"] = trade_with_regime["regime"].fillna("unknown")
    trade_summary = (
        trade_with_regime.groupby("regime", as_index=False)
        .agg(
            trade_count=("symbol", "count"),
            net_pnl=("net_pnl", "sum"),
            average_r=("realized_r_net", "mean"),
        )
    )
    return signal_summary.merge(trade_summary, on="regime", how="left").fillna(
        {"trade_count": 0, "net_pnl": 0.0, "average_r": 0.0}
    )


def _load_backtest_frame(path: Path, *, index_col: int | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, index_col=index_col)
    if index_col is not None and not frame.empty:
        frame.index = pd.to_datetime(frame.index, errors="coerce")
    for column in ("timestamp", "signal_time", "entry_time", "exit_time"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _emit_progress(
    progress_callback,
    *,
    task: str,
    current: int,
    total: int,
    message: str,
    phase: str = "running",
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        {
            "task": task,
            "phase": phase,
            "current": current,
            "total": total,
            "message": message,
        }
    )


@dataclass(slots=True)
class ResearchPipeline:
    config: AppConfig
    env: EnvironmentConfig
    mode: str | None = None
    logs: list[str] = field(default_factory=list)
    steps: list[dict[str, object]] = field(default_factory=list)

    def run(self, *, progress_callback=None) -> dict[str, object]:
        selected_mode = (self.mode or self.config.research.mode).strip().lower()
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_research"
        output_dir = self.config.research.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir = self._cache_dir(selected_mode)
        cache_dir.mkdir(parents=True, exist_ok=True)
        total_steps = 7

        def nested(step_index: int, label: str):
            def _callback(payload: dict[str, object]) -> None:
                message = str(payload.get("message", "")).strip()
                if message:
                    full_message = f"{label}: {message}"
                else:
                    full_message = f"{label}を実行しています。"
                _emit_progress(
                    progress_callback,
                    task="research",
                    current=step_index,
                    total=total_steps,
                    message=full_message,
                    phase=str(payload.get("phase", "running") or "running"),
                )

            return _callback

        _emit_progress(
            progress_callback,
            task="research",
            current=1,
            total=total_steps,
            message="データ検証を実行しています。",
        )
        validation = self._step(
            "validate",
            cache_dir / "validate.json",
            lambda: self._validate_data(),
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=2,
            total=total_steps,
            message="ML 学習ステップを実行しています。",
        )
        train_summary = self._step(
            "train",
            cache_dir / "train.json",
            lambda: self._train_summary(progress_callback=nested(2, "ML 学習")),
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=3,
            total=total_steps,
            message="ベースラインのバックテストを実行しています。",
        )
        baseline_result = self._backtest_step(
            "baseline_backtest",
            cache_dir / "baseline_backtest.json",
            lambda: self._run_backtest_variant(
                mode_name="rule_only",
                output_dir=output_dir,
                ml_enabled=False,
                backtest_mode="rule_only",
                progress_callback=nested(3, "ベースラインバックテスト"),
            ),
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=4,
            total=total_steps,
            message="選択モードのバックテストを実行しています。",
        )
        selected_result = self._backtest_step(
            "selected_backtest",
            cache_dir / "selected_backtest.json",
            lambda: self._run_backtest_variant(
                mode_name="walk_forward_train",
                output_dir=output_dir,
                ml_enabled=True,
                backtest_mode="walk_forward_train",
                progress_callback=nested(4, "選択モードバックテスト"),
            ),
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=5,
            total=total_steps,
            message="頑健性チェックを実行しています。",
        )
        robustness = self._step(
            "robustness",
            cache_dir / f"robustness_{selected_mode}.json",
            lambda: self._robustness_runs(selected_mode, progress_callback=nested(5, "頑健性チェック")),
        )
        _emit_progress(
            progress_callback,
            task="research",
            current=6,
            total=total_steps,
            message="パラメータ感度分析を実行しています。",
        )
        sensitivity = self._step(
            "sensitivity",
            cache_dir / f"sensitivity_{selected_mode}.json",
            lambda: self._parameter_sensitivity(selected_mode, progress_callback=nested(6, "感度分析")),
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
        _emit_progress(
            progress_callback,
            task="research",
            current=7,
            total=total_steps,
            message="レポートを書き出しています。",
        )
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

    def _backtest_step(self, name: str, cache_path: Path, fn: Callable[[], Any]):
        use_cache = self.config.research.reuse_cached_steps and cache_path.exists()
        if use_cache:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            output_dir = Path(payload["output_dir"])
            self.logs.append(f"{name}: 既存キャッシュを再利用しました")
            self.steps.append({"step": name, "status": "cached", "path": str(cache_path)})
            return SimpleNamespace(
                metrics=payload["metrics"],
                output_dir=str(output_dir),
                trades=_load_backtest_frame(output_dir / "trades.csv"),
                signals=_load_backtest_frame(output_dir / "signal_log.csv"),
                equity_curve=_load_backtest_frame(output_dir / "equity_curve.csv", index_col=0),
            )
        self.logs.append(f"{name}: 実行を開始します")
        try:
            result = fn()
        except Exception as exc:
            self.steps.append({"step": name, "status": "failed", "error": str(exc)})
            raise
        payload = {
            "output_dir": result.output_dir,
            "metrics": _sanitize_metrics(result.metrics),
        }
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        self.steps.append({"step": name, "status": "completed", "path": str(cache_path)})
        return result

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

    def _train_summary(self, progress_callback=None) -> dict[str, Any]:
        train_config = self.config.model_copy(deep=True)
        train_config.strategy.fx_breakout_pullback.ml_filter.enabled = True
        return train_fx_filter_model_run(train_config, self.env, progress_callback=progress_callback)

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
        progress_callback=None,
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
            persist_ml_artifacts=False,
            progress_callback=progress_callback,
        )

    def _robustness_runs(self, selected_mode: str, progress_callback=None) -> dict[str, Any]:
        spread_values = self.config.research.spread_stress_multipliers
        delay_values = self.config.research.entry_delay_scenarios
        if selected_mode == "quick":
            spread_values = spread_values[:2]
            delay_values = delay_values[:2]
        rows: list[dict[str, Any]] = []
        total_runs = max(len(spread_values) * len(delay_values), 1)
        for spread_multiplier in spread_values:
            for delay_bars in delay_values:
                current_run = len(rows) + 1
                _emit_progress(
                    progress_callback,
                    task="research",
                    current=current_run,
                    total=total_runs,
                    message=(
                        f"頑健性シナリオ {current_run}/{total_runs}: "
                        f"spread x{spread_multiplier:.1f}, 遅延 {delay_bars} 本"
                    ),
                )
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

    def _parameter_sensitivity(self, selected_mode: str, progress_callback=None) -> dict[str, Any]:
        if selected_mode == "quick":
            return {"rows": []}
        rows: list[dict[str, Any]] = []
        breakout_values = self.config.research.parameter_sensitivity_breakout
        stop_values = self.config.research.parameter_sensitivity_stop
        if selected_mode == "standard":
            breakout_values = breakout_values[:2]
            stop_values = stop_values[:2]
        total_runs = max(len(breakout_values) * len(stop_values), 1)
        for breakout_lookback in breakout_values:
            for atr_stop_mult in stop_values:
                current_run = len(rows) + 1
                _emit_progress(
                    progress_callback,
                    task="research",
                    current=current_run,
                    total=total_runs,
                    message=(
                        f"感度分析 {current_run}/{total_runs}: "
                        f"breakout={breakout_lookback}, stop={atr_stop_mult:.1f}"
                    ),
                )
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
        _regime_summary(selected_result.signals, selected_result.trades).to_csv(output_dir / "regime_summary.csv", index=False)
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
                    "- regime_summary.csv ではトレンド強弱とエントリー可否の局面別集計を確認できます。",
                ]
            ),
            encoding="utf-8",
        )
