"""Artifact exporters."""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

from fxautotrade_lab.config.models import AppConfig
from fxautotrade_lab.core.models import BacktestResult
from fxautotrade_lab.reporting.html import write_html_report


def _sanitize_scalar(value):
    if isinstance(value, dict):
        return {key: _sanitize_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _sanitize_mapping(payload):
    return {key: _sanitize_scalar(value) for key, value in payload.items()}


def export_backtest_artifacts(result: BacktestResult, config: AppConfig) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = config.reporting.output_dir / f"{timestamp}_{result.run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    if config.reporting.export_html:
        write_html_report(result, output_dir)
    if config.reporting.export_json:
        (output_dir / "metrics.json").write_text(
            json.dumps(_sanitize_mapping(result.metrics), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if config.reporting.export_csv:
        _write_frame_csv(result.trades, output_dir / "trades.csv")
        _write_frame_csv(result.orders, output_dir / "orders.csv")
        _write_frame_csv(result.fills, output_dir / "fills.csv")
        _write_frame_csv(result.positions, output_dir / "positions.csv")
        _write_frame_csv(result.equity_curve, output_dir / "equity_curve.csv")
        _write_frame_csv(result.drawdown_curve, output_dir / "drawdown.csv")
        _write_frame_csv(result.signals, output_dir / "signal_log.csv")
        _write_frame_csv(_monthly_returns(result.equity_curve), output_dir / "monthly_returns.csv")
    (output_dir / "summary.md").write_text(
        _summary_markdown(result),
        encoding="utf-8",
    )
    (output_dir / "config_snapshot.yaml").write_text(
        yaml.safe_dump(config.model_dump(mode="json"), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return output_dir


def _write_frame_csv(frame: pd.DataFrame, path: Path) -> None:
    target = frame if frame is not None else pd.DataFrame()
    include_index = not isinstance(target.index, pd.RangeIndex)
    target.to_csv(path, index=include_index)


def _monthly_returns(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve is None or equity_curve.empty:
        return pd.DataFrame(columns=["month", "return"])
    monthly = equity_curve["equity"].resample("ME").last().pct_change(fill_method=None)
    frame = monthly.rename("return").reset_index()
    first_column = frame.columns[0]
    return frame.rename(columns={first_column: "month"})


def _summary_markdown(result: BacktestResult) -> str:
    lines = [
        "# 実行サマリー",
        "",
        f"- 実行ID: {result.run_id}",
        f"- モード: {result.mode.value}",
        f"- 戦略: {result.strategy_name}",
        f"- 対象銘柄: {', '.join(result.symbols)}",
        f"- 検証期間: {result.backtest_start} - {result.backtest_end}",
        f"- 初期資産: {result.starting_cash:,.2f} JPY",
        f"- 総損益: {result.metrics.get('total_return', 0):.2%}",
        f"- 最大ドローダウン: {result.metrics.get('max_drawdown', 0):.2%}",
        f"- 勝率: {result.metrics.get('win_rate', 0):.2%}",
        f"- In-Sample シャープ: {result.in_sample_metrics.get('sharpe', 0) or 0:.2f}",
        f"- Out-of-Sample シャープ: {result.out_of_sample_metrics.get('sharpe', 0) or 0:.2f}",
        "",
        "## 注意事項",
        "",
        "- シミュレーションおよび実時間シミュレーションの結果は将来を保証しません。",
        "- 実市場では流動性、スリッページ、注文制約が異なる場合があります。",
        "- v1 では実売買は既定で無効です。",
    ]
    return "\n".join(lines)
