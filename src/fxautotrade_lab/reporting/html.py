"""HTML report generation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot

from fxautotrade_lab.core.models import BacktestResult


def _equity_chart_div(result: BacktestResult) -> str:
    if result.equity_curve.empty:
        return "<p>データがありません。</p>"
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=result.equity_curve.index,
            y=result.equity_curve["equity"],
            mode="lines",
            name="資産曲線",
            line={"color": "#1f77b4", "width": 2},
        )
    )
    if result.benchmark_curve is not None and not result.benchmark_curve.empty:
        figure.add_trace(
            go.Scatter(
                x=result.benchmark_curve.index,
                y=result.benchmark_curve["benchmark_equity"],
                mode="lines",
                name="ベンチマーク",
                line={"color": "#8c8c8c", "dash": "dot"},
            )
        )
    figure.update_layout(
        title="資産曲線",
        template="plotly_white",
        margin={"l": 30, "r": 20, "t": 50, "b": 30},
        height=360,
    )
    return plot(figure, include_plotlyjs="cdn", output_type="div")


def render_html_report(result: BacktestResult) -> str:
    monthly_returns = _monthly_returns_table(result.equity_curve)
    walk_forward_table = _walk_forward_table(result.walk_forward)
    metrics_rows = "".join(
        f"<tr><th>{label}</th><td>{value}</td></tr>"
        for label, value in {
            "総損益": f"{result.metrics.get('total_return', 0):.2%}",
            "年率換算": f"{result.metrics.get('annualized_return', 0):.2%}",
            "シャープレシオ": f"{result.metrics.get('sharpe', 0):.2f}",
            "最大ドローダウン": f"{result.metrics.get('max_drawdown', 0):.2%}",
            "勝率": f"{result.metrics.get('win_rate', 0):.2%}",
            "取引回数": result.metrics.get("number_of_trades", 0),
            "ベンチマーク差分": f"{result.metrics.get('benchmark_relative_performance', 0):.2%}",
        }.items()
    )
    caution = """
    <ul>
      <li>この結果はヒストリカル検証またはシミュレーションであり、将来利益を保証しません。</li>
      <li>実時間シミュレーションの約定、スリッページ、流動性は将来の実運用と乖離する場合があります。</li>
      <li>実売買は既定で無効化されており、現行版は GMO データを用いた検証系が主系です。</li>
    </ul>
    """
    recent_signals = (
        result.signals.tail(20)[["timestamp", "symbol", "signal_action", "signal_score", "explanation_ja"]]
        .to_html(index=False, classes="table")
        if not result.signals.empty
        else "<p>シグナル履歴はありません。</p>"
    )
    return f"""
<!doctype html>
<html lang="ja">
  <head>
    <meta charset="utf-8">
    <title>FXAutoTrade Lab レポート</title>
    <style>
      body {{
        font-family: -apple-system, BlinkMacSystemFont, "Hiragino Sans", sans-serif;
        background: #f6f8fb;
        color: #18212f;
        margin: 0;
        padding: 32px;
      }}
      .card {{
        background: white;
        border-radius: 18px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 12px 30px rgba(32, 55, 88, 0.08);
      }}
      h1, h2 {{
        margin-top: 0;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        border-bottom: 1px solid #e6ebf2;
        padding: 10px 12px;
        text-align: left;
      }}
      .badge {{
        display: inline-block;
        padding: 6px 12px;
        border-radius: 999px;
        background: #e7eef8;
        color: #294d7b;
        font-size: 12px;
      }}
    </style>
  </head>
  <body>
    <div class="card">
      <h1>FXAutoTrade Lab レポート</h1>
      <p>
        <span class="badge">モード: {result.mode.value}</span>
        <span class="badge">戦略: {result.strategy_name}</span>
      </p>
      <p>対象銘柄: {", ".join(result.symbols)}</p>
      <p>検証期間: {result.backtest_start} - {result.backtest_end}</p>
      <p>初期資産: {result.starting_cash:,.2f} JPY</p>
      <p>実行ID: {result.run_id}</p>
    </div>
    <div class="card">
      <h2>主要指標</h2>
      <table>{metrics_rows}</table>
    </div>
    <div class="card">
      <h2>資産曲線</h2>
      {_equity_chart_div(result)}
    </div>
    <div class="card">
      <h2>In-Sample / Out-of-Sample</h2>
      <table>
        <tr><th>期間</th><th>総損益</th><th>シャープレシオ</th><th>最大ドローダウン</th></tr>
        <tr><td>In-Sample</td><td>{result.in_sample_metrics.get('total_return', 0):.2%}</td><td>{(result.in_sample_metrics.get('sharpe') or 0):.2f}</td><td>{result.in_sample_metrics.get('max_drawdown', 0):.2%}</td></tr>
        <tr><td>Out-of-Sample</td><td>{result.out_of_sample_metrics.get('total_return', 0):.2%}</td><td>{(result.out_of_sample_metrics.get('sharpe') or 0):.2f}</td><td>{result.out_of_sample_metrics.get('max_drawdown', 0):.2%}</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>月次リターン</h2>
      {monthly_returns}
    </div>
    <div class="card">
      <h2>ウォークフォワード評価</h2>
      {walk_forward_table}
    </div>
    <div class="card">
      <h2>最近のシグナル</h2>
      {recent_signals}
    </div>
    <div class="card">
      <h2>実行前提</h2>
      <table>
        <tr><th>実行モード</th><td>{result.mode.value}</td></tr>
        <tr><th>戦略</th><td>{result.strategy_name}</td></tr>
        <tr><th>対象銘柄</th><td>{", ".join(result.symbols)}</td></tr>
        <tr><th>検証期間</th><td>{result.backtest_start} - {result.backtest_end}</td></tr>
        <tr><th>初期資産</th><td>{result.starting_cash:,.2f} JPY</td></tr>
        <tr><th>スリッページ/手数料</th><td>設定ファイルに基づく推定モデル</td></tr>
        <tr><th>注文種別</th><td>v1 は Bid/Ask 前提のローカル約定・実時間シミュレーションが中心です</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>前提と注意事項</h2>
      <p>スリッページ/手数料仮定を含む推定結果です。バックテスト、実時間シミュレーション、将来の実運用では挙動が異なります。</p>
      {caution}
    </div>
  </body>
</html>
"""


def write_html_report(result: BacktestResult, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "report.html"
    path.write_text(render_html_report(result), encoding="utf-8")
    return path


def _monthly_returns_table(equity_curve: pd.DataFrame) -> str:
    if equity_curve.empty:
        return "<p>月次データはありません。</p>"
    monthly = equity_curve["equity"].resample("ME").last().pct_change(fill_method=None).dropna()
    if monthly.empty:
        return "<p>月次データはありません。</p>"
    table = monthly.rename("return").reset_index()
    return table.to_html(index=False, classes="table")


def _walk_forward_table(walk_forward: list[dict[str, object]]) -> str:
    if not walk_forward:
        return "<p>ウォークフォワード結果はありません。</p>"
    rows = []
    for row in walk_forward:
        metrics = row.get("metrics", {})
        rows.append(
            {
                "window": row.get("window"),
                "start": row.get("start"),
                "end": row.get("end"),
                "return": f"{metrics.get('total_return', 0):.2%}",
                "sharpe": f"{(metrics.get('sharpe') or 0):.2f}",
                "max_drawdown": f"{metrics.get('max_drawdown', 0):.2%}",
            }
        )
    return pd.DataFrame(rows).to_html(index=False, classes="table")
