"""Desktop chart rendering helpers."""

# ruff: noqa: E501, I001

from __future__ import annotations

import os

import pandas as pd


def _load_plotly_modules() -> tuple[object | None, object | None]:
    if os.environ.get("FXAUTOTRADE_DISABLE_PLOTLY") == "1":
        return None, None
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return None, None
    return go, make_subplots


def _render_symbol_chart_fallback_html(symbol: str, frame: pd.DataFrame, title_suffix: str) -> str:
    display_columns = [
        column for column in ["open", "high", "low", "close", "volume"] if column in frame.columns
    ]
    latest = (
        frame.loc[:, display_columns].tail(120).reset_index().to_html(index=False, classes="table")
        if display_columns
        else "<p>チャート表示に必要な価格データがありません。</p>"
    )
    return f"""
    <html>
      <head>
        <style>
          body {{
            font-family: -apple-system, BlinkMacSystemFont, "Hiragino Sans", sans-serif;
            color: #0f172a;
            padding: 18px;
          }}
          .notice {{
            background: #fff7ed;
            border: 1px solid #fed7aa;
            border-radius: 12px;
            color: #9a3412;
            padding: 12px;
            margin-bottom: 14px;
          }}
          .table {{
            width: 100%;
            border-collapse: collapse;
          }}
          .table th, .table td {{
            border-bottom: 1px solid #e5e7eb;
            padding: 8px 10px;
            text-align: left;
          }}
        </style>
      </head>
      <body>
        <h2>{symbol} チャート{title_suffix}</h2>
        <div class="notice">インタラクティブチャート部品を読み込めないため、直近データの表で表示しています。</div>
        {latest}
      </body>
    </html>
    """


def render_symbol_chart_html(
    symbol: str, frame, trades_frame=None, fills_frame=None, title_suffix: str = ""
) -> str:
    go, make_subplots = _load_plotly_modules()
    if go is None or make_subplots is None:
        return _render_symbol_chart_fallback_html(symbol, frame, title_suffix)

    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=[0.58, 0.18, 0.24],
        specs=[[{"secondary_y": True}], [{}], [{}]],
    )
    figure.add_trace(
        go.Candlestick(
            x=frame.index,
            open=frame["open"],
            high=frame["high"],
            low=frame["low"],
            close=frame["close"],
            name="価格",
        ),
        row=1,
        col=1,
    )
    for column, label, color in [
        ("entry_ema_20", "EMA20", "#2b6cb0"),
        ("daily_ema_50", "日足EMA50", "#f59e0b"),
        ("daily_ema_200", "日足EMA200", "#7f1d1d"),
    ]:
        if column in frame.columns:
            figure.add_trace(
                go.Scatter(
                    x=frame.index,
                    y=frame[column],
                    mode="lines",
                    name=label,
                    line={"width": 1.6, "color": color},
                ),
                row=1,
                col=1,
            )
    if "signal_score" in frame.columns:
        figure.add_trace(
            go.Scatter(
                x=frame.index,
                y=frame["signal_score"],
                mode="lines",
                name="スコア",
                line={"width": 1.2, "color": "#0f766e", "dash": "dot"},
            ),
            row=1,
            col=1,
            secondary_y=True,
        )
    if trades_frame is not None and not trades_frame.empty:
        symbol_trades = trades_frame[trades_frame["symbol"] == symbol].copy()
        if not symbol_trades.empty:
            figure.add_trace(
                go.Scatter(
                    x=symbol_trades["entry_time"],
                    y=symbol_trades["entry_price"],
                    mode="markers",
                    name="エントリー",
                    marker={"symbol": "triangle-up", "size": 10, "color": "#16a34a"},
                ),
                row=1,
                col=1,
            )
            figure.add_trace(
                go.Scatter(
                    x=symbol_trades["exit_time"],
                    y=symbol_trades["exit_price"],
                    mode="markers",
                    name="イグジット",
                    marker={"symbol": "triangle-down", "size": 10, "color": "#dc2626"},
                ),
                row=1,
                col=1,
            )
    if fills_frame is not None and not fills_frame.empty:
        symbol_fills = fills_frame[
            fills_frame["symbol"].astype(str).str.upper() == symbol.upper()
        ].copy()
        if not symbol_fills.empty:
            if "filled_at" in symbol_fills.columns:
                symbol_fills["timestamp"] = pd.to_datetime(
                    symbol_fills["filled_at"], errors="coerce"
                )
            elif "submitted_at" in symbol_fills.columns:
                symbol_fills["timestamp"] = pd.to_datetime(
                    symbol_fills["submitted_at"], errors="coerce"
                )
            else:
                symbol_fills["timestamp"] = pd.NaT
            if "price" not in symbol_fills.columns and "filled_avg_price" in symbol_fills.columns:
                symbol_fills["price"] = pd.to_numeric(
                    symbol_fills["filled_avg_price"], errors="coerce"
                )
            symbol_fills["price"] = pd.to_numeric(symbol_fills.get("price"), errors="coerce")
            symbol_fills = symbol_fills.dropna(subset=["timestamp", "price"])
            buy_fills = symbol_fills[symbol_fills["side"].astype(str).str.lower() == "buy"]
            sell_fills = symbol_fills[symbol_fills["side"].astype(str).str.lower() == "sell"]
            if not buy_fills.empty:
                figure.add_trace(
                    go.Scatter(
                        x=buy_fills["timestamp"],
                        y=buy_fills["price"],
                        mode="markers",
                        name="買い約定",
                        marker={"symbol": "triangle-up", "size": 10, "color": "#16a34a"},
                    ),
                    row=1,
                    col=1,
                )
            if not sell_fills.empty:
                figure.add_trace(
                    go.Scatter(
                        x=sell_fills["timestamp"],
                        y=sell_fills["price"],
                        mode="markers",
                        name="売り約定",
                        marker={"symbol": "triangle-down", "size": 10, "color": "#dc2626"},
                    ),
                    row=1,
                    col=1,
                )
    figure.add_trace(
        go.Bar(x=frame.index, y=frame["volume"], name="出来高", marker={"color": "#94a3b8"}),
        row=2,
        col=1,
    )
    if "entry_rsi_14" in frame.columns:
        figure.add_trace(
            go.Scatter(
                x=frame.index,
                y=frame["entry_rsi_14"],
                mode="lines",
                name="RSI",
                line={"color": "#7c3aed"},
            ),
            row=3,
            col=1,
        )
    figure.update_layout(
        title=f"{symbol} チャート{title_suffix}",
        template="plotly_white",
        margin={"l": 20, "r": 20, "t": 48, "b": 20},
        height=860,
        legend={"orientation": "h"},
        xaxis_rangeslider_visible=False,
    )
    figure.update_yaxes(title_text="価格", row=1, col=1)
    figure.update_yaxes(title_text="スコア", row=1, col=1, secondary_y=True, range=[0, 1])
    figure.update_yaxes(title_text="出来高", row=2, col=1)
    figure.update_yaxes(title_text="RSI", row=3, col=1, range=[0, 100])
    return figure.to_html(include_plotlyjs=True, full_html=False)


def render_backtest_dashboard_html(result) -> str:
    go, make_subplots = _load_plotly_modules()
    if go is None or make_subplots is None:
        return render_backtest_dashboard_fallback_html(result)

    figure = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=(
            "資産曲線",
            "ドローダウン",
            "月次リターン",
            "Rolling Sharpe",
            "取引損益ヒストグラム",
            "保有期間ヒストグラム",
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )
    if result.equity_curve is not None and not result.equity_curve.empty:
        figure.add_trace(
            go.Scatter(
                x=result.equity_curve.index,
                y=result.equity_curve["equity"],
                mode="lines",
                name="資産曲線",
                line={"color": "#2563eb", "width": 2},
            ),
            row=1,
            col=1,
        )
        if result.benchmark_curve is not None and not result.benchmark_curve.empty:
            figure.add_trace(
                go.Scatter(
                    x=result.benchmark_curve.index,
                    y=result.benchmark_curve["benchmark_equity"],
                    mode="lines",
                    name="ベンチマーク",
                    line={"color": "#6b7280", "dash": "dot"},
                ),
                row=1,
                col=1,
            )
        if "drawdown" in result.equity_curve.columns:
            figure.add_trace(
                go.Scatter(
                    x=result.equity_curve.index,
                    y=result.equity_curve["drawdown"],
                    fill="tozeroy",
                    name="ドローダウン",
                    line={"color": "#dc2626"},
                ),
                row=1,
                col=2,
            )
        monthly = (
            result.equity_curve["equity"]
            .resample("ME")
            .last()
            .pct_change(fill_method=None)
            .dropna()
        )
        if not monthly.empty:
            monthly_df = monthly.to_frame("return")
            monthly_df["year"] = monthly_df.index.year
            monthly_df["month"] = monthly_df.index.month
            pivot = monthly_df.pivot(index="year", columns="month", values="return").sort_index()
            figure.add_trace(
                go.Heatmap(
                    z=pivot.values,
                    x=[f"{month}月" for month in pivot.columns],
                    y=[str(year) for year in pivot.index],
                    colorscale="RdYlGn",
                    zmid=0,
                    colorbar={"title": "return"},
                ),
                row=2,
                col=1,
            )
        daily_returns = (
            result.equity_curve["equity"].resample("1D").last().pct_change(fill_method=None)
        )
        rolling_mean = daily_returns.rolling(20).mean()
        rolling_std = daily_returns.rolling(20).std().replace(0, pd.NA)
        rolling_sharpe = (
            ((rolling_mean / rolling_std) * (252**0.5))
            .replace([float("inf"), float("-inf")], 0)
            .fillna(0)
        )
        figure.add_trace(
            go.Scatter(
                x=rolling_sharpe.index,
                y=rolling_sharpe,
                mode="lines",
                name="Rolling Sharpe",
                line={"color": "#0f766e"},
            ),
            row=2,
            col=2,
        )
    if result.trades is not None and not result.trades.empty:
        figure.add_trace(
            go.Histogram(
                x=result.trades["net_pnl"],
                nbinsx=30,
                name="PnL",
                marker={"color": "#3b82f6"},
            ),
            row=3,
            col=1,
        )
        figure.add_trace(
            go.Histogram(
                x=result.trades["hold_bars"],
                nbinsx=20,
                name="保有期間",
                marker={"color": "#8b5cf6"},
            ),
            row=3,
            col=2,
        )
    figure.update_layout(
        template="plotly_white",
        height=1100,
        margin={"l": 30, "r": 20, "t": 60, "b": 30},
        showlegend=False,
        title="バックテスト分析ダッシュボード",
    )
    return figure.to_html(include_plotlyjs=True, full_html=False)


def render_backtest_dashboard_fallback_html(result) -> str:
    per_symbol = result.metrics.get("per_symbol_contribution", {}) or {}
    per_symbol_frame = (
        pd.DataFrame(
            [{"通貨ペア": symbol, "純損益": value} for symbol, value in per_symbol.items()]
        ).sort_values("純損益", ascending=False)
        if per_symbol
        else pd.DataFrame(columns=["通貨ペア", "純損益"])
    )
    monthly = pd.DataFrame(columns=["月", "リターン"])
    if result.equity_curve is not None and not result.equity_curve.empty:
        monthly_series = (
            result.equity_curve["equity"]
            .resample("ME")
            .last()
            .pct_change(fill_method=None)
            .dropna()
            .tail(12)
        )
        if not monthly_series.empty:
            monthly = pd.DataFrame(
                {
                    "月": [stamp.strftime("%Y-%m") for stamp in monthly_series.index],
                    "リターン": [f"{value:.2%}" for value in monthly_series.values],
                }
            )
    walk_forward = pd.DataFrame(
        columns=["window", "start", "end", "return", "sharpe", "max_drawdown"]
    )
    if result.walk_forward:
        rows = []
        for row in result.walk_forward:
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
        walk_forward = pd.DataFrame(rows)
    summary_cards = [
        ("総損益", f"{result.metrics.get('total_return', 0):.2%}"),
        ("最大ドローダウン", f"{result.metrics.get('max_drawdown', 0):.2%}"),
        ("勝率", f"{result.metrics.get('win_rate', 0):.2%}"),
        ("シャープレシオ", f"{(result.metrics.get('sharpe') or 0):.2f}"),
        ("取引回数", str(result.metrics.get("number_of_trades", 0))),
        ("平均保有期間", f"{result.metrics.get('average_hold_bars', 0):.2f}"),
    ]
    card_html = "".join(
        f"<div class='metric-card'><div class='metric-label'>{label}</div><div class='metric-value'>{value}</div></div>"
        for label, value in summary_cards
    )
    per_symbol_html = (
        per_symbol_frame.to_html(index=False, classes="table")
        if not per_symbol_frame.empty
        else "<p>通貨ペア別寄与はありません。</p>"
    )
    monthly_html = (
        monthly.to_html(index=False, classes="table")
        if not monthly.empty
        else "<p>月次データはありません。</p>"
    )
    walk_forward_html = (
        walk_forward.to_html(index=False, classes="table")
        if not walk_forward.empty
        else "<p>Walk-Forward 結果はありません。</p>"
    )
    latest_equity_html = "<p>資産曲線データはありません。</p>"
    if result.equity_curve is not None and not result.equity_curve.empty:
        latest_equity_html = (
            "<div class='note-box'>"
            f"開始資産: {result.equity_curve['equity'].iloc[0]:,.2f}<br>"
            f"終了資産: {result.equity_curve['equity'].iloc[-1]:,.2f}<br>"
            f"最終更新: {result.equity_curve.index.max()}"
            "</div>"
        )
    return f"""
    <html>
      <head>
        <style>
          body {{
            font-family: -apple-system, BlinkMacSystemFont, "Hiragino Sans", sans-serif;
            color: #0f172a;
            background: white;
            margin: 0;
            padding: 18px;
          }}
          .metrics {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
            margin-bottom: 18px;
          }}
          .metric-card {{
            border: 1px solid #dbe3ee;
            border-radius: 14px;
            padding: 14px;
            background: #f8fafc;
          }}
          .metric-label {{
            color: #64748b;
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 6px;
          }}
          .metric-value {{
            color: #0f172a;
            font-size: 22px;
            font-weight: 700;
          }}
          .section {{
            border: 1px solid #dbe3ee;
            border-radius: 16px;
            padding: 16px;
            margin-bottom: 16px;
            background: #ffffff;
          }}
          .section h3 {{
            margin: 0 0 10px 0;
            font-size: 17px;
          }}
          .table {{
            width: 100%;
            border-collapse: collapse;
          }}
          .table th, .table td {{
            border-bottom: 1px solid #e5e7eb;
            padding: 8px 10px;
            text-align: left;
          }}
          .table th {{
            background: #eff6ff;
          }}
          .note-box {{
            background: #eef6ff;
            border-radius: 12px;
            padding: 12px;
            color: #0f3c78;
            line-height: 1.6;
          }}
        </style>
      </head>
      <body>
        <div class="metrics">{card_html}</div>
        <div class="section">
          <h3>資産曲線サマリー</h3>
          {latest_equity_html}
        </div>
        <div class="section">
          <h3>通貨ペア別寄与</h3>
          {per_symbol_html}
        </div>
        <div class="section">
          <h3>直近12か月の月次リターン</h3>
          {monthly_html}
        </div>
        <div class="section">
          <h3>Walk-Forward</h3>
          {walk_forward_html}
        </div>
      </body>
    </html>
    """


def load_native_symbol_chart_widget_class():  # pragma: no cover - UI helper
    from PySide6.QtCharts import (
        QChart,
        QChartView,
        QCandlestickSeries,
        QCandlestickSet,
        QDateTimeAxis,
        QLineSeries,
        QScatterSeries,
        QValueAxis,
    )
    from PySide6.QtCore import QDateTime, QMargins, Qt
    from PySide6.QtGui import QColor, QPainter, QPen
    from PySide6.QtWidgets import QLabel, QSizePolicy, QVBoxLayout, QWidget

    class NativeSymbolChartWidget(QWidget):
        def __init__(self, parent=None) -> None:
            super().__init__(parent)
            layout = QVBoxLayout(self)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(18)
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.MinimumExpanding)
            self.status_label = QLabel("チャートデータを表示します。")
            self.status_label.setObjectName("nativeChartStatus")
            self.status_label.setProperty("role", "muted")
            self.status_label.setWordWrap(True)
            layout.addWidget(self.status_label)

            self.price_chart = QChart()
            self.price_view = self._build_view(self.price_chart)
            self.price_view.setMinimumHeight(560)
            layout.addWidget(self.price_view, 5)

            self.volume_chart = QChart()
            self.volume_view = self._build_view(self.volume_chart)
            self.volume_view.setMinimumHeight(240)
            layout.addWidget(self.volume_view, 2)

            self.rsi_chart = QChart()
            self.rsi_view = self._build_view(self.rsi_chart)
            self.rsi_view.setMinimumHeight(240)
            layout.addWidget(self.rsi_view, 2)
            self.setMinimumHeight(1120)

        def _build_view(self, chart: QChart) -> QChartView:
            view = QChartView(chart)
            view.setObjectName("nativeChartView")
            view.setRenderHint(QPainter.Antialiasing, True)
            view.setRubberBand(QChartView.RectangleRubberBand)
            chart.legend().setVisible(True)
            chart.legend().setAlignment(Qt.AlignBottom)
            chart.setMargins(QMargins(8, 8, 8, 8))
            return view

        def clear(self, message: str) -> None:
            self.status_label.setText(message)
            for chart in (self.price_chart, self.volume_chart, self.rsi_chart):
                chart.removeAllSeries()
                for axis in list(chart.axes()):
                    chart.removeAxis(axis)
                chart.setTitle("")

        def render(
            self,
            symbol: str,
            frame: pd.DataFrame,
            *,
            trades_frame: pd.DataFrame | None = None,
            fills_frame: pd.DataFrame | None = None,
            title_suffix: str = "",
        ) -> None:
            if frame is None or frame.empty:
                self.clear(f"{symbol} のチャートデータがありません。")
                return
            working = frame.copy().tail(240)
            if not isinstance(working.index, pd.DatetimeIndex):
                working.index = pd.to_datetime(working.index, utc=True)
            if working.index.tz is None:
                working.index = working.index.tz_localize("UTC")
            timestamps = [pd.Timestamp(index).to_pydatetime() for index in working.index]
            x_values = [QDateTime(value).toMSecsSinceEpoch() for value in timestamps]
            self.status_label.setText(
                f"{symbol} / {len(working)}本 / 期間: {working.index.min()} - {working.index.max()}"
            )
            self._render_price_chart(
                symbol,
                working,
                x_values,
                trades_frame=trades_frame,
                fills_frame=fills_frame,
                title_suffix=title_suffix,
            )
            self._render_volume_chart(working, x_values)
            self._render_rsi_chart(working, x_values)

        def _axis_format_for_frame(self, working: pd.DataFrame) -> str:
            start = working.index.min()
            end = working.index.max()
            if pd.isna(start) or pd.isna(end):
                return "MM/dd HH:mm"
            crosses_year = start.year != end.year
            span = end - start
            if crosses_year:
                return "yy/MM/dd" if span.days >= 7 else "yy/MM/dd HH:mm"
            return "MM/dd" if span.days >= 7 else "MM/dd HH:mm"

        def _reset_chart(
            self, chart: QChart, title: str, working: pd.DataFrame
        ) -> tuple[QDateTimeAxis, QValueAxis]:
            chart.removeAllSeries()
            for axis in list(chart.axes()):
                chart.removeAxis(axis)
            chart.setTitle(title)
            axis_x = QDateTimeAxis()
            axis_x.setFormat(self._axis_format_for_frame(working))
            axis_x.setLabelsAngle(-25)
            axis_x.setTickCount(6)
            axis_y = QValueAxis()
            axis_y.setLabelFormat("%.2f")
            chart.addAxis(axis_x, Qt.AlignBottom)
            chart.addAxis(axis_y, Qt.AlignLeft)
            return axis_x, axis_y

        def _render_price_chart(
            self,
            symbol: str,
            working: pd.DataFrame,
            x_values: list[int],
            *,
            trades_frame: pd.DataFrame | None,
            fills_frame: pd.DataFrame | None,
            title_suffix: str,
        ) -> None:
            axis_x, axis_y = self._reset_chart(
                self.price_chart, f"{symbol} チャート{title_suffix}", working
            )
            candle = QCandlestickSeries()
            candle.setIncreasingColor(QColor("#16a34a"))
            candle.setDecreasingColor(QColor("#dc2626"))
            for (_, row), x_value in zip(working.iterrows(), x_values, strict=False):
                candle.append(
                    QCandlestickSet(
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        x_value,
                    )
                )
            self.price_chart.addSeries(candle)
            candle.attachAxis(axis_x)
            candle.attachAxis(axis_y)

            for column, label, color in [
                ("entry_ema_20", "EMA20", "#2563eb"),
                ("daily_ema_50", "日足EMA50", "#f59e0b"),
                ("daily_ema_200", "日足EMA200", "#7f1d1d"),
            ]:
                if column not in working.columns:
                    continue
                series = QLineSeries()
                series.setName(label)
                pen = QPen(QColor(color))
                pen.setWidth(2)
                series.setPen(pen)
                for value, y_value in zip(x_values, working[column], strict=False):
                    if pd.notna(y_value):
                        series.append(float(value), float(y_value))
                self.price_chart.addSeries(series)
                series.attachAxis(axis_x)
                series.attachAxis(axis_y)

            self._add_trade_markers(
                symbol,
                working,
                x_values,
                axis_x,
                axis_y,
                trades_frame=trades_frame,
                fills_frame=fills_frame,
            )
            low = float(working["low"].min())
            high = float(working["high"].max())
            padding = max((high - low) * 0.08, 0.5)
            axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(x_values[0]),
                QDateTime.fromMSecsSinceEpoch(x_values[-1]),
            )
            axis_y.setRange(low - padding, high + padding)

        def _add_trade_markers(
            self,
            symbol: str,
            working: pd.DataFrame,
            x_values: list[int],
            axis_x: QDateTimeAxis,
            axis_y: QValueAxis,
            *,
            trades_frame: pd.DataFrame | None,
            fills_frame: pd.DataFrame | None,
        ) -> None:
            marker_specs: list[tuple[str, str, QColor, pd.Series, str, str]] = []
            if (
                trades_frame is not None
                and not trades_frame.empty
                and "symbol" in trades_frame.columns
            ):
                symbol_trades = trades_frame[
                    trades_frame["symbol"].astype(str).str.upper() == symbol.upper()
                ].copy()
                if not symbol_trades.empty:
                    marker_specs.extend(
                        [
                            (
                                "エントリー",
                                "entry_time",
                                QColor("#16a34a"),
                                symbol_trades["entry_price"],
                                "entry_time",
                                "entry_price",
                            ),
                            (
                                "イグジット",
                                "exit_time",
                                QColor("#dc2626"),
                                symbol_trades["exit_price"],
                                "exit_time",
                                "exit_price",
                            ),
                        ]
                    )
                    for name, _, color, _, time_column, price_column in marker_specs[-2:]:
                        scatter = QScatterSeries()
                        scatter.setName(name)
                        scatter.setMarkerSize(11.0)
                        scatter.setColor(color)
                        for _, trade in symbol_trades.iterrows():
                            timestamp = pd.to_datetime(
                                trade.get(time_column), errors="coerce", utc=True
                            )
                            price = pd.to_numeric(trade.get(price_column), errors="coerce")
                            if pd.isna(timestamp) or pd.isna(price):
                                continue
                            scatter.append(
                                float(QDateTime(timestamp.to_pydatetime()).toMSecsSinceEpoch()),
                                float(price),
                            )
                        if scatter.count():
                            self.price_chart.addSeries(scatter)
                            scatter.attachAxis(axis_x)
                            scatter.attachAxis(axis_y)

            if (
                fills_frame is not None
                and not fills_frame.empty
                and "symbol" in fills_frame.columns
            ):
                symbol_fills = fills_frame[
                    fills_frame["symbol"].astype(str).str.upper() == symbol.upper()
                ].copy()
                if not symbol_fills.empty:
                    buy_series = QScatterSeries()
                    buy_series.setName("買い約定")
                    buy_series.setMarkerSize(10.0)
                    buy_series.setColor(QColor("#16a34a"))
                    sell_series = QScatterSeries()
                    sell_series.setName("売り約定")
                    sell_series.setMarkerSize(10.0)
                    sell_series.setColor(QColor("#dc2626"))
                    for _, fill in symbol_fills.iterrows():
                        timestamp = pd.to_datetime(
                            fill.get("filled_at") or fill.get("submitted_at"),
                            errors="coerce",
                            utc=True,
                        )
                        price = pd.to_numeric(
                            fill.get("price") or fill.get("filled_avg_price"), errors="coerce"
                        )
                        if pd.isna(timestamp) or pd.isna(price):
                            continue
                        point_x = float(QDateTime(timestamp.to_pydatetime()).toMSecsSinceEpoch())
                        if str(fill.get("side", "")).lower() == "sell":
                            sell_series.append(point_x, float(price))
                        else:
                            buy_series.append(point_x, float(price))
                    for scatter in (buy_series, sell_series):
                        if scatter.count():
                            self.price_chart.addSeries(scatter)
                            scatter.attachAxis(axis_x)
                            scatter.attachAxis(axis_y)

        def _render_volume_chart(self, working: pd.DataFrame, x_values: list[int]) -> None:
            axis_x, axis_y = self._reset_chart(self.volume_chart, "出来高", working)
            series = QLineSeries()
            series.setName("出来高")
            pen = QPen(QColor("#64748b"))
            pen.setWidth(2)
            series.setPen(pen)
            for value, volume in zip(x_values, working["volume"], strict=False):
                series.append(float(value), float(volume))
            self.volume_chart.addSeries(series)
            series.attachAxis(axis_x)
            series.attachAxis(axis_y)
            axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(x_values[0]),
                QDateTime.fromMSecsSinceEpoch(x_values[-1]),
            )
            axis_y.setRange(0.0, max(float(working["volume"].max()) * 1.1, 1.0))

        def _render_rsi_chart(self, working: pd.DataFrame, x_values: list[int]) -> None:
            latest_rsi = pd.to_numeric(working.get("entry_rsi_14"), errors="coerce").dropna()
            title = f"RSI（現在 {latest_rsi.iloc[-1]:.1f}）" if not latest_rsi.empty else "RSI"
            axis_x, axis_y = self._reset_chart(self.rsi_chart, title, working)
            axis_y.setRange(0.0, 100.0)
            axis_y.setLabelFormat("%.0f")
            axis_x.setRange(
                QDateTime.fromMSecsSinceEpoch(x_values[0]),
                QDateTime.fromMSecsSinceEpoch(x_values[-1]),
            )
            if "entry_rsi_14" in working.columns:
                series = QLineSeries()
                series.setName("RSI14")
                pen = QPen(QColor("#7c3aed"))
                pen.setWidth(3)
                series.setPen(pen)
                for value, rsi in zip(x_values, working["entry_rsi_14"], strict=False):
                    if pd.notna(rsi):
                        series.append(float(value), float(rsi))
                self.rsi_chart.addSeries(series)
                series.attachAxis(axis_x)
                series.attachAxis(axis_y)
                valid_rsi = pd.to_numeric(working["entry_rsi_14"], errors="coerce").dropna()
                if not valid_rsi.empty:
                    last_index = working.index.get_loc(valid_rsi.index[-1])
                    marker = QScatterSeries()
                    marker.setName("最新RSI")
                    marker.setMarkerSize(11.0)
                    marker.setColor(QColor("#7c3aed"))
                    marker.append(float(x_values[last_index]), float(valid_rsi.iloc[-1]))
                    self.rsi_chart.addSeries(marker)
                    marker.attachAxis(axis_x)
                    marker.attachAxis(axis_y)
            for threshold, color, label in [
                (30.0, "#94a3b8", "売られ過ぎ"),
                (70.0, "#f59e0b", "買われ過ぎ"),
            ]:
                line = QLineSeries()
                line.setName(label)
                pen = QPen(QColor(color))
                pen.setWidth(1)
                pen.setStyle(Qt.DashLine)
                line.setPen(pen)
                line.append(float(x_values[0]), threshold)
                line.append(float(x_values[-1]), threshold)
                self.rsi_chart.addSeries(line)
                line.attachAxis(axis_x)
                line.attachAxis(axis_y)

    return NativeSymbolChartWidget
