"""Additional desktop pages."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import yaml


def _optional_web_view():  # pragma: no cover - UI helper
    try:
        if os.environ.get("QT_QPA_PLATFORM") == "offscreen":
            raise ImportError("offscreen mode")
        if getattr(sys, "frozen", False):
            raise ImportError("qtwebengine disabled in packaged app")
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except ImportError:
        return None
    return QWebEngineView


def build_chart_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    import re

    from PySide6.QtCore import QTimer
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QFrame,
        QHBoxLayout,
        QLabel,
        QPushButton,
        QScrollArea,
        QTextBrowser,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.charts import load_native_symbol_chart_widget_class, render_symbol_chart_html
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip

    QWebEngineView = _optional_web_view()
    try:
        NativeSymbolChartWidget = load_native_symbol_chart_widget_class()
    except ImportError:
        NativeSymbolChartWidget = None

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("チャート")
    title.setProperty("role", "h1")
    subtitle = QLabel("価格・出来高・RSI をまとめて確認")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    refresh_button = QPushButton("チャート更新")
    refresh_button.setProperty("variant", "primary")
    header_row.addWidget(refresh_button)
    layout.addLayout(header_row)

    banner = Card(sunken=True)
    helper = QLabel(
        "バックテスト結果、または GMO 実時間シミュレーション中の最新チャートを表示します。"
        " 負荷を抑えるため、自動更新は必要な時だけ有効化してください。"
    )
    helper.setWordWrap(True)
    helper.setProperty("role", "muted")
    banner.addBodyWidget(helper)
    layout.addWidget(banner)

    controls_card = Card(title="表示設定", subtitle="通貨ペア / 足種 / オプション")
    symbol_combo = QComboBox()
    timeframe_combo = QComboBox()
    auto_refresh = QCheckBox("自動更新")
    auto_refresh.setChecked(False)
    controls_row = QHBoxLayout()
    controls_row.setSpacing(10)
    controls_row.addWidget(symbol_combo, 1)
    controls_row.addWidget(timeframe_combo)
    controls_row.addWidget(auto_refresh)
    controls_card.addBodyLayout(controls_row)
    source_note = QLabel()
    source_note.setWordWrap(True)
    source_note.setProperty("role", "muted")
    controls_card.addBodyWidget(source_note)
    layout.addWidget(controls_card)

    live_chip = Chip("静的", "neutral")
    chart_card = Card(title="価格チャート", subtitle="OHLC / 指標 / 約定マーカー", header_right=live_chip)
    web = QWebEngineView() if QWebEngineView is not None else None
    native_chart = NativeSymbolChartWidget() if NativeSymbolChartWidget is not None else None
    fallback = QTextBrowser()
    if native_chart is not None:
        chart_card.addBodyWidget(native_chart)
    elif web is not None:
        web.setMinimumHeight(1280)
        chart_card.addBodyWidget(web, 1)
    else:
        fallback.setMinimumHeight(1280)
        chart_card.addBodyWidget(fallback, 1)
    layout.addWidget(chart_card)
    layout.addStretch(1)

    refresh_timer = QTimer(page)
    refresh_timer.setInterval(max(10000, app_state.config.automation.poll_interval_seconds * 1000))
    page._chart_request_id = 0
    page._chart_loading = False

    def is_runtime_chart() -> bool:
        return (
            app_state.automation_controller is not None
            or app_state.config.data.source == "gmo"
            or app_state.config.broker.mode.value == "gmo_sim"
        )

    def supported_timeframes() -> list[str]:
        entry = app_state.config.strategy.entry_timeframe.value
        configured = [timeframe.value for timeframe in app_state.config.data.timeframes]
        ordered = [entry, *configured, "1Day", "1Week", "1Month"]
        return list(dict.fromkeys(ordered))

    def plain_message(content: str) -> str:
        return re.sub(r"<[^>]+>", "", content).strip() or "表示できるチャートがありません。"

    def set_content(content: str) -> None:
        if native_chart is not None:
            native_chart.clear(plain_message(content))
        elif web is not None:
            web.setHtml(content)
        else:
            fallback.setHtml(content)

    def render_native(symbol: str, frame: pd.DataFrame, *, trades_frame: pd.DataFrame | None = None, fills_frame: pd.DataFrame | None = None, title_suffix: str = "") -> bool:
        if native_chart is None:
            return False
        native_chart.render(
            symbol,
            frame.tail(400),
            trades_frame=trades_frame,
            fills_frame=fills_frame,
            title_suffix=title_suffix,
        )
        return True

    def on_chart_loaded(request_id: int, symbol: str, timeframe: str, dataset: dict[str, object]) -> None:
        if request_id != page._chart_request_id:
            return
        page._chart_loading = False
        refresh_button.setText("チャート更新")
        set_button_enabled(refresh_button, True)
        frame = dataset["frame"]
        fills = dataset["fills"]
        if frame is None or frame.empty:
            set_content(f"<h3>{symbol} / {timeframe} の runtime チャートデータがありません。</h3>")
            return
        if not render_native(symbol, frame, fills_frame=fills, title_suffix="（実時間シミュレーション）"):
            set_content(
                render_symbol_chart_html(
                    symbol,
                    frame.tail(400),
                    fills_frame=fills,
                    title_suffix="（実時間シミュレーション）",
                )
            )

    def on_chart_error(request_id: int, message: str) -> None:
        if request_id != page._chart_request_id:
            return
        page._chart_loading = False
        refresh_button.setText("チャート更新")
        set_button_enabled(refresh_button, True)
        set_content(f"<h3>チャート表示に失敗しました。</h3><p>{message}</p>")
        log_message(f"チャート更新エラー: {message}")

    def request_runtime_render(*, force_refresh: bool = False) -> None:
        symbol = symbol_combo.currentText()
        timeframe = timeframe_combo.currentText()
        if not symbol or symbol == "データなし":
            set_content("<h3>表示対象の通貨ペアがありません。</h3>")
            return
        if page._chart_loading:
            return
        page._chart_request_id += 1
        request_id = page._chart_request_id
        page._chart_loading = True
        refresh_button.setText("更新中...")
        set_button_enabled(refresh_button, False, busy=True)
        set_content("<h3>チャートを更新しています…</h3>")
        submit_task(
            lambda: app_state.load_chart_dataset(symbol, timeframe, force_refresh=force_refresh),
            lambda dataset, rid=request_id, current_symbol=symbol, current_timeframe=timeframe: on_chart_loaded(
                rid,
                current_symbol,
                current_timeframe,
                dataset,
            ),
            lambda message, rid=request_id: on_chart_error(rid, message),
        )

    def refresh() -> None:
        symbol_combo.blockSignals(True)
        timeframe_combo.blockSignals(True)
        symbol_combo.clear()
        timeframe_combo.clear()
        timeframe_combo.addItems(supported_timeframes())
        runtime = is_runtime_chart()
        if runtime:
            symbols = list(dict.fromkeys(app_state.config.watchlist.symbols))
            source_note.setText("表示ソース: GMO / ローカル実時間シミュレーションの runtime データ")
            live_chip.set_tone("running")
            live_chip.set_text("live")
            if symbols:
                symbol_combo.addItems(symbols)
            else:
                symbol_combo.addItem("データなし")
            content = "<h3>runtime チャートを読み込み中です。</h3>"
        elif app_state.last_result is None or not app_state.last_result.chart_frames:
            symbol_combo.addItem("データなし")
            source_note.setText("表示ソース: バックテスト結果")
            live_chip.set_tone("neutral")
            live_chip.set_text("未実行")
            content = "<h3>バックテスト後にチャートを表示できます。</h3>"
            set_content(content)
            symbol_combo.blockSignals(False)
            timeframe_combo.blockSignals(False)
            refresh_button.setText("チャート更新")
            set_button_enabled(refresh_button, True)
            return
        else:
            symbols = list(app_state.last_result.chart_frames.keys())
            symbol_combo.addItems(symbols)
            source_note.setText("表示ソース: バックテスト結果")
            live_chip.set_tone("info")
            live_chip.set_text("backtest")
            content = "<h3>バックテストチャートを読み込み中です。</h3>"
        set_content(content)
        symbol_combo.blockSignals(False)
        timeframe_combo.blockSignals(False)
        if not page._chart_loading:
            refresh_button.setText("チャート更新")
            set_button_enabled(refresh_button, True)
        render_current()

    def render_current() -> None:
        symbol = symbol_combo.currentText()
        timeframe = timeframe_combo.currentText()
        if not symbol or symbol == "データなし":
            content = "<h3>表示対象の通貨ペアがありません。</h3>"
        elif is_runtime_chart():
            request_runtime_render(force_refresh=False)
            return
        elif app_state.last_result is None or not app_state.last_result.chart_frames:
            content = "<h3>バックテスト後にチャートを表示できます。</h3>"
        else:
            frame = app_state.last_result.chart_frames.get(symbol, {}).get(timeframe)
            if frame is None or frame.empty:
                content = f"<h3>{symbol} / {timeframe} のチャートデータがありません。</h3>"
            else:
                if render_native(symbol, frame, trades_frame=app_state.last_result.trades):
                    return
                content = render_symbol_chart_html(symbol, frame.tail(400), app_state.last_result.trades)
        set_content(content)

    symbol_combo.currentTextChanged.connect(lambda _: render_current())
    timeframe_combo.currentTextChanged.connect(lambda _: render_current())
    refresh_button.clicked.connect(
        lambda: request_runtime_render(force_refresh=True) if is_runtime_chart() else refresh()
    )
    refresh_timer.timeout.connect(
        lambda: request_runtime_render(force_refresh=False)
        if auto_refresh.isChecked() and page.isVisible() and is_runtime_chart()
        else None
    )
    refresh_timer.start()
    page.refresh = refresh
    return page


def build_history_page(app_state):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import (
        QComboBox,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QTabWidget,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile

    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("取引履歴")
    title.setProperty("role", "h1")
    subtitle = QLabel("バックテスト / 実時間シミュレーションで採取した履歴")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    kpi_grid = QGridLayout()
    kpi_grid.setHorizontalSpacing(12)
    kpi_grid.setVerticalSpacing(12)
    kpi_specs = [
        ("trade_count", "累計取引"),
        ("total_pl", "累計損益"),
        ("win_rate", "勝率"),
        ("profit_factor", "Profit Factor"),
    ]
    kpi_tiles: dict[str, KpiTile] = {}
    for index, (key, label_text) in enumerate(kpi_specs):
        tile = KpiTile(label=label_text, value="-")
        kpi_grid.addWidget(tile, 0, index)
        kpi_tiles[key] = tile
        kpi_grid.setColumnStretch(index, 1)
    layout.addLayout(kpi_grid)

    card = Card(title="取引ログ", subtitle="通貨ペア / 売買フィルタあり")
    filter_row = QHBoxLayout()
    symbol_filter = QLineEdit()
    symbol_filter.setPlaceholderText("通貨ペアフィルタ")
    side_filter = QComboBox()
    side_filter.addItems(["すべて", "buy", "sell"])
    filter_row.addWidget(symbol_filter, 1)
    filter_row.addWidget(side_filter)
    card.addBodyLayout(filter_row)

    tabs = QTabWidget()
    views: dict[str, QTableView] = {}
    models: dict[str, DataFrameTableModel] = {}
    raw_frames: dict[str, pd.DataFrame] = {}
    for key, label in [("trades", "取引"), ("orders", "注文"), ("fills", "約定")]:
        table = QTableView()
        table.setAlternatingRowColors(False)
        table.setShowGrid(False)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        table.horizontalHeader().setStretchLastSection(True)
        model = DataFrameTableModel()
        table.setModel(model)
        tabs.addTab(table, label)
        views[key] = table
        models[key] = model
        raw_frames[key] = pd.DataFrame()
    card.addBodyWidget(tabs, 1)
    layout.addWidget(card, 1)

    def apply_filters() -> None:
        needle = symbol_filter.text().strip().upper()
        selected_side = side_filter.currentText()
        for key, model in models.items():
            frame = raw_frames[key]
            filtered = frame.copy()
            if not filtered.empty and needle and "symbol" in filtered.columns:
                filtered = filtered[filtered["symbol"].astype(str).str.upper().str.contains(needle, na=False)]
            if not filtered.empty and selected_side != "すべて":
                side_column = "side" if "side" in filtered.columns else None
                if side_column is not None:
                    filtered = filtered[filtered[side_column].astype(str) == selected_side]
            model.set_frame(filtered.tail(300) if not filtered.empty else None)

    def refresh() -> None:
        if app_state.last_result is None:
            for key in raw_frames:
                raw_frames[key] = pd.DataFrame()
            for tile in kpi_tiles.values():
                tile.set_value("-")
            apply_filters()
            return
        raw_frames["trades"] = app_state.last_result.trades.copy()
        raw_frames["orders"] = app_state.last_result.orders.copy()
        raw_frames["fills"] = app_state.last_result.fills.copy()
        metrics = app_state.last_result.metrics
        trades_count = metrics.get("number_of_trades", 0)
        total_return = metrics.get("total_return", 0)
        win_rate = metrics.get("win_rate", 0)
        profit_factor = metrics.get("profit_factor")
        kpi_tiles["trade_count"].set_value(str(trades_count))
        kpi_tiles["total_pl"].set_value(
            f"{total_return:.2%}",
            tone="pos" if total_return >= 0 else "neg",
        )
        kpi_tiles["win_rate"].set_value(f"{win_rate:.2%}")
        kpi_tiles["profit_factor"].set_value(
            f"{profit_factor:.2f}" if profit_factor is not None else "-"
        )
        apply_filters()

    symbol_filter.textChanged.connect(lambda _: apply_filters())
    side_filter.currentTextChanged.connect(lambda _: apply_filters())
    page.refresh = refresh
    return page


def build_reports_page(app_state):  # pragma: no cover - UI helper
    from PySide6.QtCore import QUrl, Qt
    from PySide6.QtWidgets import (
        QFrame,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QPushButton,
        QSplitter,
        QTableView,
        QTextBrowser,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.widgets.card import Card

    QWebEngineView = _optional_web_view()
    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("レポート")
    title.setProperty("role", "h1")
    subtitle = QLabel("実行ごとの詳細サマリーとプレビュー")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    list_card = Card(title="全ての実行", subtitle="直近の run を上から表示")
    splitter = QSplitter(Qt.Horizontal)
    table = QTableView()
    table.setAlternatingRowColors(False)
    table.setShowGrid(False)
    table.verticalHeader().setVisible(False)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
    table.horizontalHeader().setStretchLastSection(True)
    detail_splitter = QSplitter(Qt.Vertical)
    detail = QTextEdit()
    detail.setReadOnly(True)
    detail.setProperty("role", "mono")
    preview = QWebEngineView() if QWebEngineView is not None else QTextBrowser()
    model = DataFrameTableModel()
    table.setModel(model)
    splitter.addWidget(table)
    detail_splitter.addWidget(detail)
    detail_splitter.addWidget(preview)
    detail_splitter.setStretchFactor(0, 1)
    detail_splitter.setStretchFactor(1, 3)
    splitter.addWidget(detail_splitter)
    splitter.setStretchFactor(0, 2)
    splitter.setStretchFactor(1, 5)
    list_card.addBodyWidget(splitter, 1)
    layout.addWidget(list_card, 1)

    def set_preview_content(report_dir: Path | None, run_id: str) -> None:
        if report_dir is None:
            content = "<h3>出力ディレクトリが見つかりません。</h3>"
            preview.setHtml(content)
            return
        html_path = report_dir / "report.html"
        summary_path = report_dir / "summary.md"
        if html_path.exists():
            html = html_path.read_text(encoding="utf-8")
            if QWebEngineView is not None and isinstance(preview, QWebEngineView):
                preview.setHtml(html, QUrl.fromLocalFile(str(html_path)))
            else:
                preview.setHtml(html)
            return
        if summary_path.exists():
            markdown = summary_path.read_text(encoding="utf-8").replace("\n", "<br>")
            preview.setHtml(markdown)
            return
        events = app_state.load_automation_events(run_id)
        if not events.empty:
            preview.setHtml(events.tail(50).to_html(index=False))
            return
        preview.setHtml("<h3>プレビュー可能な成果物がありません。</h3>")

    def on_clicked(index) -> None:  # noqa: ANN001
        frame = model._frame
        if frame.empty:
            return
        row = frame.iloc[index.row()]
        report_dir = app_state.locate_report(str(row.get("run_id", "")))
        automation_events = app_state.load_automation_events(str(row.get("run_id", "")))
        detail_lines = [
            f"実行ID: {row.get('run_id', '')}",
            f"種別: {row.get('run_kind', '')}",
            f"モード: {row.get('mode', '')}",
            f"戦略: {row.get('strategy_name', '')}",
            f"終了時刻: {row.get('finished_at', '')}",
            f"出力先: {row.get('output_dir', '')}",
        ]
        if pd.notna(row.get("total_return")):
            detail_lines.append(f"総損益: {row.get('total_return', 0):.2%}")
        if pd.notna(row.get("max_drawdown")):
            detail_lines.append(f"最大ドローダウン: {row.get('max_drawdown', 0):.2%}")
        if not automation_events.empty:
            detail_lines.append(f"自動売買イベント数: {len(automation_events)}")
            detail_lines.append("直近イベント:")
            detail_lines.extend(
                f"- {record['timestamp']} / {record['level']} / {record['message_ja']}"
                for record in automation_events.tail(5).to_dict(orient="records")
            )
        config_snapshot = app_state.store.load_config_snapshot(str(row.get("run_id", "")))
        if config_snapshot:
            detail_lines.extend(["", "設定スナップショット", config_snapshot[:1500]])
        detail.setPlainText("\n".join(detail_lines))
        set_preview_content(report_dir, str(row.get("run_id", "")))

    table.clicked.connect(on_clicked)

    def refresh() -> None:
        rows = app_state.list_runs()
        if not rows:
            model.set_frame(None)
            detail.setPlainText("レポートはまだありません。")
            preview.setHtml("<h3>レポートはまだありません。</h3>")
            return
        frame = pd.DataFrame(
            [
                {
                    "run_id": row["run_id"],
                    "run_kind": row["run_kind"],
                    "mode": row["mode"],
                    "strategy_name": row["strategy_name"],
                    "finished_at": row["finished_at"],
                    "output_dir": row["output_dir"],
                    "total_return": row["metrics"].get("total_return"),
                    "max_drawdown": row["metrics"].get("max_drawdown"),
                }
                for row in rows
            ]
        )
        model.set_frame(frame)
        detail.setPlainText("行を選択するとレポート概要とプレビューを表示します。")
        first_report_dir = app_state.locate_report(frame.iloc[0]["run_id"])
        set_preview_content(first_report_dir, frame.iloc[0]["run_id"])

    page.refresh = refresh
    return page


def build_settings_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMessageBox,
        QPushButton,
        QScrollArea,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip

    def _set_combo_value(combo: QComboBox, value: str) -> None:
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def credential_source_text(source: str) -> str:
        return {
            "public_api": "public API",
            "keychain": "macOS キーチェーン",
            "env": ".env",
            "unset": "未設定",
        }.get(source, source)

    def number_input(placeholder: str) -> QLineEdit:
        editor = QLineEdit()
        editor.setPlaceholderText(placeholder)
        editor.setClearButtonEnabled(True)
        editor.setAlignment(Qt.AlignRight)
        editor.setProperty("align", "num")
        return editor

    def format_number(value: float, decimals: int = 2) -> str:
        text = f"{float(value):,.{decimals}f}"
        if decimals and "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    def parse_number_input(editor: QLineEdit, default: float) -> float:
        text_value = editor.text().strip()
        if not text_value:
            return default
        return float(text_value.replace(",", "").replace("JPY", "").strip())

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    page.last_test_result = None
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    # Header
    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("設定")
    title.setProperty("role", "h1")
    subtitle = QLabel("運用モード / 資金管理 / 通知 / GMO 接続")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    # Banner
    banner = Card(sunken=True)
    helper = QLabel(
        "FX 版では JForex CSV の履歴データと GMO の実時間データを切り替えて運用します。"
        " 現在の自動売買はすべてローカル約定で、バックテストとフォワード検証はローカルで完結します。"
    )
    helper.setWordWrap(True)
    helper.setProperty("role", "muted")
    banner.addBodyWidget(helper)
    warning = QLabel()
    warning.setWordWrap(True)
    warning.setProperty("tone", "warn")
    banner.addBodyWidget(warning)
    layout.addWidget(banner)

    def refresh_all_pages() -> None:
        window = page.window()
        if hasattr(window, "refresh_all_pages"):
            window.refresh_all_pages()
        else:
            refresh()

    # Runtime mode card
    mode_card = Card(
        title="運用モード",
        subtitle="発注はすべてローカルでシミュレーションします",
    )
    mode_combo = QComboBox()
    mode_combo.addItem("ローカルシミュレーション", "local_sim")
    mode_combo.addItem("GMO 実時間シミュレーション", "gmo_sim")
    source_combo = QComboBox()
    source_combo.addItem("JForex CSV キャッシュ", "csv")
    source_combo.addItem("GMO 実時間データ", "gmo")
    source_combo.addItem("fixture / デモ履歴", "fixture")
    stream_box = QCheckBox("実時間更新を有効化")
    mode_status = QLabel()
    mode_status.setWordWrap(True)
    mode_status.setProperty("role", "muted")
    mode_form = QGridLayout()
    mode_form.setHorizontalSpacing(14)
    mode_form.setVerticalSpacing(8)
    label_mode = QLabel("運用モード")
    label_mode.setProperty("role", "muted2")
    label_source = QLabel("市場データ")
    label_source.setProperty("role", "muted2")
    mode_form.addWidget(label_mode, 0, 0)
    mode_form.addWidget(label_source, 0, 1)
    mode_form.addWidget(mode_combo, 1, 0)
    mode_form.addWidget(source_combo, 1, 1)
    mode_form.setColumnStretch(0, 1)
    mode_form.setColumnStretch(1, 1)
    mode_card.addBodyLayout(mode_form)
    mode_card.addBodyWidget(stream_box)
    mode_card.addBodyWidget(mode_status)
    mode_button_row = QHBoxLayout()
    mode_button_row.addStretch(1)
    save_mode_button = QPushButton("運用モードを保存")
    save_mode_button.setProperty("variant", "primary")
    mode_button_row.addWidget(save_mode_button)
    mode_card.addBodyLayout(mode_button_row)
    layout.addWidget(mode_card)

    # Sizing card
    sizing_card = Card(title="資金 / 注文サイズ", subtitle="JPY 建ての資金量とリスク")
    sizing_combo = QComboBox()
    sizing_combo.addItem("定額", "fixed_amount")
    sizing_combo.addItem("資産比率", "equity_fraction")
    sizing_combo.addItem("リスク率", "risk_based")
    fixed_amount_input = number_input("例: 300000")
    equity_fraction_input = number_input("例: 0.10")
    risk_fraction_input = number_input("例: 0.01")
    starting_cash_input = number_input("例: 5000000")
    sizing_status = QLabel()
    sizing_status.setWordWrap(True)
    sizing_status.setProperty("role", "muted")
    sizing_form = QGridLayout()
    sizing_form.setHorizontalSpacing(14)
    sizing_form.setVerticalSpacing(8)
    labels = [
        ("初期資産 (JPY)", 0, 0),
        ("数量モード", 0, 1),
        ("定額 (JPY)", 2, 0),
        ("資産比率", 2, 1),
        ("リスク率", 4, 0),
    ]
    for text, row, column in labels:
        lbl = QLabel(text)
        lbl.setProperty("role", "muted2")
        sizing_form.addWidget(lbl, row, column)
    sizing_form.addWidget(starting_cash_input, 1, 0)
    sizing_form.addWidget(sizing_combo, 1, 1)
    sizing_form.addWidget(fixed_amount_input, 3, 0)
    sizing_form.addWidget(equity_fraction_input, 3, 1)
    sizing_form.addWidget(risk_fraction_input, 5, 0, 1, 2)
    sizing_form.setColumnStretch(0, 1)
    sizing_form.setColumnStretch(1, 1)
    sizing_card.addBodyLayout(sizing_form)
    sizing_card.addBodyWidget(sizing_status)
    sizing_button_row = QHBoxLayout()
    sizing_button_row.addStretch(1)
    save_sizing_button = QPushButton("資金 / 注文サイズを保存")
    save_sizing_button.setProperty("variant", "primary")
    sizing_button_row.addWidget(save_sizing_button)
    sizing_card.addBodyLayout(sizing_button_row)
    layout.addWidget(sizing_card)

    # Notifications card
    notifications_card = Card(
        title="通知チャネル",
        subtitle="注文・エラー・再接続・停止理由の通知先",
    )
    notify_enabled = QCheckBox("通知を有効化")
    desktop_box = QCheckBox("デスクトップ通知")
    sound_box = QCheckBox("サウンド")
    log_box = QCheckBox("ログ保存")
    webhook_box = QCheckBox("Webhook")
    sound_name = QLineEdit()
    sound_name.setClearButtonEnabled(True)
    webhook_url = QLineEdit()
    webhook_url.setClearButtonEnabled(True)
    webhook_url.setEchoMode(QLineEdit.Password)
    log_path_label = QLabel()
    log_path_label.setWordWrap(True)
    log_path_label.setProperty("role", "muted")
    channels_row = QHBoxLayout()
    for widget in (desktop_box, sound_box, log_box, webhook_box):
        channels_row.addWidget(widget)
    channels_row.addStretch(1)
    notifications_card.addBodyWidget(notify_enabled)
    label_channels = QLabel("チャネル")
    label_channels.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_channels)
    notifications_card.addBodyLayout(channels_row)
    label_sound = QLabel("サウンド名")
    label_sound.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_sound)
    notifications_card.addBodyWidget(sound_name)
    label_webhook = QLabel("Webhook URL")
    label_webhook.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_webhook)
    notifications_card.addBodyWidget(webhook_url)
    label_logpath = QLabel("ログ出力先")
    label_logpath.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_logpath)
    notifications_card.addBodyWidget(log_path_label)
    notif_button_row = QHBoxLayout()
    notif_button_row.addStretch(1)
    save_notifications_button = QPushButton("通知設定を保存")
    save_notifications_button.setProperty("variant", "primary")
    notif_button_row.addWidget(save_notifications_button)
    notifications_card.addBodyLayout(notif_button_row)
    layout.addWidget(notifications_card)

    # Connection card
    conn_chip = Chip("接続テスト未実行", "neutral")
    connection_card = Card(
        title="GMO 接続確認",
        subtitle="public API の疎通確認 / private API キー管理",
        header_right=conn_chip,
    )
    api_key_input = QLineEdit()
    api_key_input.setPlaceholderText("GMO private API Key")
    api_key_input.setClearButtonEnabled(True)
    api_secret_input = QLineEdit()
    api_secret_input.setPlaceholderText("GMO private API Secret")
    api_secret_input.setClearButtonEnabled(True)
    api_secret_input.setEchoMode(QLineEdit.Password)
    credential_status = QLabel()
    credential_status.setWordWrap(True)
    credential_status.setProperty("role", "muted")
    connection_status = QLabel("接続テストは未実行です。")
    connection_status.setWordWrap(True)
    connection_status.setProperty("role", "muted")
    credential_form = QGridLayout()
    credential_form.setHorizontalSpacing(14)
    credential_form.setVerticalSpacing(8)
    label_key = QLabel("private API Key")
    label_key.setProperty("role", "muted2")
    label_secret = QLabel("private API Secret")
    label_secret.setProperty("role", "muted2")
    credential_form.addWidget(label_key, 0, 0)
    credential_form.addWidget(label_secret, 0, 1)
    credential_form.addWidget(api_key_input, 1, 0)
    credential_form.addWidget(api_secret_input, 1, 1)
    credential_form.setColumnStretch(0, 1)
    credential_form.setColumnStretch(1, 1)
    connection_card.addBodyLayout(credential_form)
    connection_card.addBodyWidget(credential_status)
    connection_card.addBodyWidget(connection_status)
    connection_buttons = QHBoxLayout()
    save_credentials_button = QPushButton("private API を保存")
    save_credentials_button.setProperty("variant", "primary")
    clear_credentials_button = QPushButton("保存済みキーを削除")
    clear_credentials_button.setProperty("variant", "ghost")
    test_connection_button = QPushButton("GMO 接続テスト")
    test_connection_button.setProperty("variant", "ghost")
    connection_buttons.addStretch(1)
    connection_buttons.addWidget(clear_credentials_button)
    connection_buttons.addWidget(test_connection_button)
    connection_buttons.addWidget(save_credentials_button)
    connection_card.addBodyLayout(connection_buttons)

    # Test result area inline under connection card
    test_output = QTextEdit()
    test_output.setReadOnly(True)
    test_output.setMinimumHeight(180)
    test_output.setProperty("role", "mono")
    connection_card.addBodyWidget(test_output)
    layout.addWidget(connection_card)

    # Summary + config snapshot card
    summary_card = Card(title="現在の設定スナップショット", subtitle="YAML 形式の全量表示")
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setProperty("role", "muted")
    summary_card.addBodyWidget(summary_label)
    config_text = QTextEdit()
    config_text.setReadOnly(True)
    config_text.setMinimumHeight(260)
    config_text.setProperty("role", "mono")
    summary_card.addBodyWidget(config_text)
    layout.addWidget(summary_card)

    layout.addStretch(1)

    def update_mode_status() -> None:
        selected_mode = str(mode_combo.currentData() or "local_sim")
        selected_source = str(source_combo.currentData() or "csv")
        if selected_mode == "gmo_sim":
            _set_combo_value(source_combo, "gmo")
            source_combo.setEnabled(False)
            stream_box.setEnabled(True)
            mode_status.setText(
                "GMO 実時間シミュレーション: 市場データは GMO public API に固定されます。"
                " 発注はすべてローカル約定です。"
            )
            return
        source_combo.setEnabled(True)
        stream_allowed = selected_source == "gmo"
        stream_box.setEnabled(stream_allowed)
        if not stream_allowed:
            stream_box.setChecked(False)
        source_text = {
            "csv": "JForex CSV から作成した複数時間足キャッシュを使うオフライン検証です。",
            "gmo": "GMO の価格を取得しつつ、注文はローカルで約定させます。",
            "fixture": "fixture の生成データを使う軽量な検証モードです。",
        }.get(selected_source, selected_source)
        mode_status.setText(
            f"ローカルシミュレーション: {source_text}"
            f"  実時間更新: {'有効化できます' if stream_allowed else 'このソースでは無効です'}"
        )

    def save_runtime_mode() -> None:
        selected_mode = str(mode_combo.currentData() or "local_sim")
        selected_source = str(source_combo.currentData() or "csv")
        try:
            app_state.update_runtime_mode(
                broker_mode=selected_mode,
                data_source=selected_source,
                stream_enabled=stream_box.isChecked(),
            )
        except Exception as exc:  # pragma: no cover - config/runtime guard
            QMessageBox.critical(page, "エラー", f"運用モードの保存に失敗しました。\n{exc}")
            return
        mode_label = {
            "local_sim": "ローカルシミュレーション",
            "gmo_sim": "GMO 実時間シミュレーション",
        }.get(selected_mode, selected_mode)
        QMessageBox.information(page, "完了", f"運用モードを保存しました。\n{mode_label}")
        log_message(f"運用モードを保存しました: {selected_mode} / {app_state.config.data.source}")
        refresh_all_pages()

    def update_sizing_status() -> None:
        selected_mode = str(sizing_combo.currentData() or "fixed_amount")
        fixed_amount_input.setEnabled(selected_mode == "fixed_amount")
        equity_fraction_input.setEnabled(selected_mode == "equity_fraction")
        risk_fraction_input.setEnabled(selected_mode == "risk_based")
        if selected_mode == "fixed_amount":
            sizing_status.setText(
                "定額モード: 指定した JPY 金額に近い数量を計算します。"
                f" 最小数量 {app_state.config.risk.minimum_order_quantity:,} / "
                f"数量ステップ {app_state.config.risk.quantity_step:,} に合わせて丸めます。"
            )
            return
        if selected_mode == "equity_fraction":
            sizing_status.setText(
                "資産比率モード: 現在資産の一定割合を 1 回の新規エントリーに使います。"
                " 例: 0.10 なら約 10% です。"
            )
            return
        sizing_status.setText(
            "リスク率モード: ATR ベースのストップ距離と許容損失率から数量を計算します。"
        )

    def save_order_sizing() -> None:
        try:
            app_state.update_account_settings(
                starting_cash=parse_number_input(starting_cash_input, app_state.config.risk.starting_cash)
            )
            app_state.update_order_sizing(
                order_size_mode=str(sizing_combo.currentData() or "fixed_amount"),
                fixed_order_amount=parse_number_input(
                    fixed_amount_input,
                    app_state.config.risk.fixed_order_amount,
                ),
                equity_fraction_per_trade=parse_number_input(
                    equity_fraction_input,
                    app_state.config.risk.equity_fraction_per_trade,
                ),
                risk_per_trade=parse_number_input(
                    risk_fraction_input,
                    app_state.config.risk.risk_per_trade,
                ),
            )
        except Exception as exc:  # pragma: no cover - UI validation
            QMessageBox.critical(page, "エラー", f"資金 / 注文サイズ設定の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "資金 / 注文サイズ設定を保存しました。")
        log_message(
            "資金 / 注文サイズ設定を保存しました: "
            f"{app_state.config.risk.starting_cash:,.0f} {app_state.config.risk.account_currency} / "
            f"{app_state.config.risk.order_size_mode.value}"
        )
        refresh_all_pages()

    def save_private_credentials() -> None:
        try:
            values = app_state.save_gmo_credentials(
                "private",
                api_key_input.text(),
                api_secret_input.text(),
            )
        except Exception as exc:  # pragma: no cover - OS credential store
            QMessageBox.critical(page, "エラー", f"GMO private API の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "GMO private API を macOS キーチェーンへ保存しました。")
        log_message(f"GMO private API を保存しました: {credential_source_text(str(values.get('source', 'keychain')))}")
        refresh_all_pages()

    def clear_private_credentials() -> None:
        answer = QMessageBox.question(
            page,
            "確認",
            "macOS キーチェーンに保存済みの GMO private API 資格情報を削除します。よろしいですか？",
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            deleted = app_state.delete_gmo_credentials("private")
        except Exception as exc:  # pragma: no cover - OS credential store
            QMessageBox.critical(page, "エラー", f"GMO private API の削除に失敗しました。\n{exc}")
            return
        if deleted:
            QMessageBox.information(page, "完了", "GMO private API を macOS キーチェーンから削除しました。")
            log_message("GMO private API を削除しました。")
        else:
            QMessageBox.information(page, "情報", "削除対象の GMO private API は見つかりませんでした。")
        refresh_all_pages()

    def save_notifications() -> None:
        channels = []
        if desktop_box.isChecked():
            channels.append("desktop")
        if sound_box.isChecked():
            channels.append("sound")
        if log_box.isChecked():
            channels.append("log")
        if webhook_box.isChecked():
            channels.append("webhook")
        try:
            app_state.update_notification_settings(
                enabled=notify_enabled.isChecked(),
                channels=channels,
                sound_name=sound_name.text(),
                webhook_url=webhook_url.text(),
            )
        except Exception as exc:  # pragma: no cover - file system path
            QMessageBox.critical(page, "エラー", f"通知設定の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "通知設定を保存しました。")
        log_message("通知設定を保存しました。")
        refresh_all_pages()

    def set_test_busy(is_busy: bool) -> None:
        test_connection_button.setText("GMO 接続確認中..." if is_busy else "GMO 接続テスト")
        set_button_enabled(test_connection_button, not is_busy, busy=is_busy)
        if is_busy:
            connection_status.setText("GMO public API の疎通確認を実行しています...")
            conn_chip.set_tone("info")
            conn_chip.set_text("確認中")

    def render_test_result(result: dict[str, object] | None = None, error: str = "") -> None:
        if error:
            page.last_test_result = {"error": error}
        elif result is not None:
            page.last_test_result = result
        record = page.last_test_result
        if not record:
            test_output.setPlainText("接続テストはまだ実行していません。")
            return
        if "error" in record:
            test_output.setPlainText(f"接続テスト失敗\n{record['error']}")
            return
        warning_lines = [f"- {item}" for item in record.get("warnings_ja", [])]
        test_output.setPlainText(
            "\n".join(
                [
                    "GMO public API 接続確認",
                    f"実行時刻: {record.get('tested_at', '-')}",
                    f"ティッカー取得: {record.get('ticker_count', 0)} 件",
                    f"通貨ペアルール取得: {record.get('symbol_count', 0)} 件",
                    f"市場データ確認: {'OK' if record.get('market_data_ok') else '要確認'}",
                    f"確認通貨ペア: {record.get('market_data_symbol', '-')}",
                    f"取得バー数: {record.get('market_data_rows', 0)}",
                    *warning_lines,
                ]
            )
        )

    def on_test_finished(result: dict[str, object]) -> None:
        set_test_busy(False)
        market_ok = "OK" if result.get("market_data_ok") else "要確認"
        connection_status.setText(
            f"GMO public API: 接続成功  •  市場データ: {market_ok}  •  確認通貨ペア: {result.get('market_data_symbol', '-')}"
        )
        conn_chip.set_tone("running")
        conn_chip.set_text("接続OK")
        render_test_result(result=result)
        log_message("GMO 接続テストが完了しました。")
        if result.get("warnings_ja"):
            QMessageBox.information(
                page,
                "接続テスト完了",
                "\n".join(["接続テストは完了しました。", *[str(item) for item in result["warnings_ja"]]]),
            )

    def on_test_error(message: str) -> None:
        set_test_busy(False)
        connection_status.setText(f"GMO 接続確認失敗\n{message}")
        conn_chip.set_tone("neg")
        conn_chip.set_text("失敗")
        render_test_result(error=message)
        QMessageBox.critical(page, "接続テスト失敗", message)
        log_message(f"GMO 接続テスト失敗: {message}")

    def run_connection_test() -> None:
        set_test_busy(True)
        submit_task(
            app_state.test_gmo_connection,
            on_test_finished,
            on_test_error,
        )

    save_notifications_button.clicked.connect(save_notifications)
    save_mode_button.clicked.connect(save_runtime_mode)
    save_sizing_button.clicked.connect(save_order_sizing)
    save_credentials_button.clicked.connect(save_private_credentials)
    clear_credentials_button.clicked.connect(clear_private_credentials)
    test_connection_button.clicked.connect(run_connection_test)
    mode_combo.currentIndexChanged.connect(lambda _: update_mode_status())
    source_combo.currentIndexChanged.connect(lambda _: update_mode_status())
    sizing_combo.currentIndexChanged.connect(lambda _: update_sizing_status())

    def refresh() -> None:
        env = app_state.env
        credential_statuses = app_state.credential_statuses()
        private_status = credential_statuses["private"]
        private_values = app_state.load_credential_values("private")
        private_configured = bool(private_status["configured"])
        warning.setText(
            "GMO 実時間シミュレーションでは実データを取得しますが、売買はまだすべてローカル約定です。"
            if app_state.config.broker.mode.value == "gmo_sim" or app_state.config.data.source == "gmo"
            else "JForex CSV / fixture を使うローカル検証構成です。必要に応じてデータ同期ページから CSV を追加インポートしてください。"
        )
        payload = app_state.config.model_dump(mode="json")
        payload["env_status"] = {
            "gmo_public_api": "利用可",
            "gmo_private_api": "設定済み" if private_configured else "未設定",
            "gmo_private_api_source": private_status["source"],
            "live_trading_enabled": getattr(env, "live_trading_enabled", False),
            "config_path": str(app_state.config_path) if app_state.config_path is not None else "",
        }
        summary_label.setText(
            " • ".join(
                [
                    f"設定ファイル: {app_state.config_path}",
                    f"運用モード: {app_state.config.broker.mode.value}",
                    f"市場データ: {app_state.config.data.source}",
                    f"口座通貨: {app_state.config.risk.account_currency}",
                    f"初期資産: {app_state.config.risk.starting_cash:,.0f}",
                    f"監視: {len(app_state.config.watchlist.symbols)} ペア",
                ]
            )
        )
        notify_enabled.setChecked(app_state.config.automation.notifications_enabled)
        configured_channels = set(app_state.config.automation.notification_channels.channels)
        desktop_box.setChecked("desktop" in configured_channels)
        sound_box.setChecked("sound" in configured_channels)
        log_box.setChecked("log" in configured_channels)
        webhook_box.setChecked("webhook" in configured_channels)
        _set_combo_value(mode_combo, app_state.config.broker.mode.value)
        _set_combo_value(source_combo, app_state.config.data.source)
        stream_box.setChecked(bool(app_state.config.data.stream_enabled))
        update_mode_status()
        _set_combo_value(sizing_combo, app_state.config.risk.order_size_mode.value)
        starting_cash_input.setText(format_number(app_state.config.risk.starting_cash, 2))
        fixed_amount_input.setText(format_number(app_state.config.risk.fixed_order_amount, 2))
        equity_fraction_input.setText(format_number(app_state.config.risk.equity_fraction_per_trade, 4))
        risk_fraction_input.setText(format_number(app_state.config.risk.risk_per_trade, 4))
        update_sizing_status()
        sound_name.setText(app_state.config.automation.notification_channels.sound_name)
        webhook_url.setText(app_state.config.automation.notification_channels.webhook_url)
        log_path_label.setText(str(app_state.config.automation.notification_channels.log_path))
        api_key_input.setText(str(private_values.get("api_key", "")))
        api_secret_input.setText(str(private_values.get("api_secret", "")))
        credential_status.setText(
            " • ".join(
                [
                    f"保存状態: {'設定済み' if private_configured else '未設定'}",
                    f"保存元: {credential_source_text(str(private_status['source']))}",
                    f"API Key: {private_values.get('api_key_masked') or '未設定'}",
                ]
            )
        )
        connection_status.setText(
            " • ".join(
                [
                    "GMO public API: 認証不要",
                    (
                        f"GMO private API: {'設定済み' if private_configured else '未設定'}"
                        f" ({credential_source_text(str(private_status['source']))})"
                    ),
                ]
            )
        )
        if page.last_test_result and "error" not in page.last_test_result:
            conn_chip.set_tone("running")
            conn_chip.set_text("接続OK")
        elif page.last_test_result:
            conn_chip.set_tone("neg")
            conn_chip.set_text("失敗")
        else:
            conn_chip.set_tone("neutral")
            conn_chip.set_text("未テスト")
        config_text.setPlainText(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
        render_test_result()

    page.refresh = refresh
    return page


def build_help_page():  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.widgets.card import Card

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("ヘルプ")
    title.setProperty("role", "h1")
    subtitle = QLabel("進め方・用語・トラブルシュート")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(3):
        grid.setColumnStretch(column, 1)

    start_card = Card(title="はじめる", subtitle="初回の流れ")
    steps_label = QLabel(
        "1. 監視通貨ペアで 3-8 ペアを登録\n"
        "2. データ同期ページで JForex CSV を取り込む\n"
        "3. バックテストで期間別の成績を確認\n"
        "4. 実時間シミュレーションで前向き検証"
    )
    steps_label.setWordWrap(True)
    steps_label.setProperty("role", "muted")
    start_card.addBodyWidget(steps_label)
    grid.addWidget(start_card, 0, 0)

    terms_card = Card(title="用語", subtitle="よく使う言葉")
    terms_label = QLabel(
        "• バックテスト: 過去データで戦略を検証\n"
        "• Walk-Forward: 期間を動かしながら逐次検証\n"
        "• Uplift: ML 適用前後の期待差\n"
        "• ローカル約定: UI からは実売買しません"
    )
    terms_label.setWordWrap(True)
    terms_label.setProperty("role", "muted")
    terms_card.addBodyWidget(terms_label)
    grid.addWidget(terms_card, 0, 1)

    trouble_card = Card(title="トラブル", subtitle="よくある対処")
    trouble_label = QLabel(
        "• 接続失敗: 設定 → GMO 接続テストで状態を確認\n"
        "• チャートが空: バックテストを 1 回実行するか、\n"
        "  実時間シミュレーションで通貨ペアを選び更新\n"
        "• 取込失敗: Bid / Ask の 2 ファイルを同時選択"
    )
    trouble_label.setWordWrap(True)
    trouble_label.setProperty("role", "muted")
    trouble_card.addBodyWidget(trouble_label)
    grid.addWidget(trouble_card, 0, 2)

    layout.addLayout(grid)

    shortcut_card = Card(title="ショートカット")
    shortcut_grid = QGridLayout()
    shortcut_grid.setHorizontalSpacing(18)
    shortcut_grid.setVerticalSpacing(8)
    shortcuts = [
        ("⌘R", "現在のページを再読込"),
        ("⌘⇧D", "デモ実行"),
        ("⌘L", "ログの表示切替"),
        ("⌃Tab", "次のページへ"),
        ("⌃⇧Tab", "前のページへ"),
        ("⌘F", "ページ内の検索"),
    ]
    for index, (key, description) in enumerate(shortcuts):
        key_label = QLabel(key)
        key_label.setProperty("role", "mono")
        desc_label = QLabel(description)
        desc_label.setProperty("role", "muted")
        shortcut_grid.addWidget(key_label, index // 3, (index % 3) * 2)
        shortcut_grid.addWidget(desc_label, index // 3, (index % 3) * 2 + 1)
    shortcut_card.addBodyLayout(shortcut_grid)
    layout.addWidget(shortcut_card)

    disclaimer = QLabel("本アプリは投資助言ではありません。")
    disclaimer.setProperty("role", "muted2")
    disclaimer.setWordWrap(True)
    layout.addWidget(disclaimer)

    layout.addStretch(1)
    return page
