"""Additional desktop pages."""

from __future__ import annotations

import os
import re
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
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled, set_button_role

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
    layout.setContentsMargins(0, 0, 8, 12)
    layout.setSpacing(16)
    page.setWidget(content)
    title = QLabel("チャート")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)
    helper = QLabel(
        "バックテスト結果、または GMO 実時間シミュレーション中の最新チャートを表示します。"
        " 実時間シミュレーション中は最新バーと約定マーカーを表示できます。"
        " 負荷を抑えるため、自動更新は必要な時だけ有効化してください。"
    )
    helper.setWordWrap(True)
    helper.setStyleSheet("background: #f3f7fb; border-radius: 12px; padding: 12px;")
    layout.addWidget(helper)

    controls_card = QFrame()
    controls_card.setStyleSheet("background: white; border: 1px solid #dbe3ee; border-radius: 16px;")
    controls_layout = QVBoxLayout(controls_card)
    controls_layout.setContentsMargins(18, 18, 18, 18)
    controls_layout.setSpacing(12)
    controls_label = QLabel("表示設定")
    controls_label.setStyleSheet("font-size: 16px; font-weight: 700;")
    controls_hint = QLabel("通貨ペアと時間足を選び、必要な時だけ更新してください。チャート本体は下へスクロールして確認できます。")
    controls_hint.setWordWrap(True)
    controls_hint.setStyleSheet("color: #475569;")
    symbol_combo = QComboBox()
    timeframe_combo = QComboBox()
    refresh_button = QPushButton("チャート更新")
    set_button_role(refresh_button, "primary")
    auto_refresh = QCheckBox("自動更新")
    auto_refresh.setChecked(False)
    source_note = QLabel()
    source_note.setWordWrap(True)
    source_note.setStyleSheet("color: #475569;")
    controls = QHBoxLayout()
    controls.addWidget(symbol_combo, 1)
    controls.addWidget(timeframe_combo)
    controls.addWidget(refresh_button)
    controls.addWidget(auto_refresh)
    controls_layout.addWidget(controls_label)
    controls_layout.addWidget(controls_hint)
    controls_layout.addLayout(controls)
    controls_layout.addWidget(source_note)
    layout.addWidget(controls_card)

    chart_card = QFrame()
    chart_card.setStyleSheet("background: white; border: 1px solid #dbe3ee; border-radius: 16px;")
    chart_layout = QVBoxLayout(chart_card)
    chart_layout.setContentsMargins(20, 20, 20, 20)
    chart_layout.setSpacing(16)
    chart_note = QLabel("価格チャート、出来高、RSI を縦に並べて表示します。")
    chart_note.setWordWrap(True)
    chart_note.setStyleSheet("color: #334155; font-weight: 600;")
    chart_layout.addWidget(chart_note)
    web = QWebEngineView() if QWebEngineView is not None else None
    native_chart = NativeSymbolChartWidget() if NativeSymbolChartWidget is not None else None
    fallback = QTextBrowser()
    if native_chart is not None:
        chart_layout.addWidget(native_chart)
    elif web is not None:
        web.setMinimumHeight(1280)
        chart_layout.addWidget(web, 1)
    else:
        fallback.setMinimumHeight(1280)
        chart_layout.addWidget(fallback, 1)
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
            if symbols:
                symbol_combo.addItems(symbols)
            else:
                symbol_combo.addItem("データなし")
            content = "<h3>runtime チャートを読み込み中です。</h3>"
        elif app_state.last_result is None or not app_state.last_result.chart_frames:
            symbol_combo.addItem("データなし")
            source_note.setText("表示ソース: バックテスト結果")
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
    refresh_button.clicked.connect(lambda: request_runtime_render(force_refresh=True) if is_runtime_chart() else refresh())
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
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QTabWidget,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class

    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("取引履歴")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)
    filter_row = QHBoxLayout()
    symbol_filter = QLineEdit()
    symbol_filter.setPlaceholderText("通貨ペアフィルタ")
    side_filter = QComboBox()
    side_filter.addItems(["すべて", "buy", "sell"])
    filter_row.addWidget(symbol_filter, 1)
    filter_row.addWidget(side_filter)
    layout.addLayout(filter_row)
    tabs = QTabWidget()
    views = {}
    models = {}
    raw_frames = {}
    for key, label in [("trades", "取引"), ("orders", "注文"), ("fills", "約定")]:
        table = QTableView()
        model = DataFrameTableModel()
        table.setModel(model)
        tabs.addTab(table, label)
        views[key] = table
        models[key] = model
        raw_frames[key] = pd.DataFrame()
    layout.addWidget(tabs, 1)

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
            apply_filters()
            return
        raw_frames["trades"] = app_state.last_result.trades.copy()
        raw_frames["orders"] = app_state.last_result.orders.copy()
        raw_frames["fills"] = app_state.last_result.fills.copy()
        apply_filters()

    symbol_filter.textChanged.connect(lambda _: apply_filters())
    side_filter.currentTextChanged.connect(lambda _: apply_filters())
    page.refresh = refresh
    return page


def build_reports_page(app_state):  # pragma: no cover - UI helper
    from PySide6.QtCore import QUrl, Qt
    from PySide6.QtWidgets import QLabel, QSplitter, QTableView, QTextBrowser, QTextEdit, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.models import load_dataframe_model_class

    QWebEngineView = _optional_web_view()
    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("レポート")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)
    splitter = QSplitter(Qt.Horizontal)
    table = QTableView()
    detail_splitter = QSplitter(Qt.Vertical)
    detail = QTextEdit()
    detail.setReadOnly(True)
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
    layout.addWidget(splitter, 1)

    def set_preview_content(report_dir: Path | None, run_id: str) -> None:
        if report_dir is None:
            content = "<h3>出力ディレクトリが見つかりません。</h3>"
            if QWebEngineView is not None and isinstance(preview, QWebEngineView):
                preview.setHtml(content)
            else:
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
            if QWebEngineView is not None and isinstance(preview, QWebEngineView):
                preview.setHtml(markdown)
            else:
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
        QDoubleSpinBox,
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

    from fxautotrade_lab.desktop.ui_controls import set_button_enabled, set_button_role

    def card_style(name: str, border: str = "#dbe3ee", background: str = "white") -> str:
        return f"QFrame#{name} {{ background: {background}; border: 1px solid {border}; border-radius: 16px; }}"

    def block_label(text: str) -> QLabel:
        label = QLabel(text)
        label.setStyleSheet("border: none; background: transparent; color: #334155; font-weight: 600;")
        return label

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

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.last_test_result = None
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(16)
    page.setWidget(content)

    title = QLabel("設定")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    helper = QLabel(
        "FX 版では JForex CSV の履歴データと GMO の実時間データを切り替えて運用します。"
        " 現在の自動売買はすべてローカル約定で、バックテストとフォワード検証はローカルで完結します。"
    )
    helper.setWordWrap(True)
    helper.setStyleSheet("background: #eef6ff; color: #0f3c78; border-radius: 14px; padding: 14px;")
    warning = QLabel()
    warning.setWordWrap(True)
    warning.setStyleSheet("background: #fff7ed; color: #9a3412; border-radius: 14px; padding: 14px;")
    layout.addWidget(title)
    layout.addWidget(helper)
    layout.addWidget(warning)

    cards_layout = QVBoxLayout()
    cards_layout.setContentsMargins(0, 0, 0, 0)
    cards_layout.setSpacing(16)
    layout.addLayout(cards_layout, 1)

    def refresh_all_pages() -> None:
        window = page.window()
        if hasattr(window, "refresh_all_pages"):
            window.refresh_all_pages()
        else:
            refresh()

    mode_card = QFrame()
    mode_card.setObjectName("runtimeModeCard")
    mode_card.setStyleSheet(card_style("runtimeModeCard"))
    mode_layout = QVBoxLayout(mode_card)
    mode_title = QLabel("運用モード")
    mode_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    mode_note = QLabel(
        "発注はすべてローカルでシミュレーションします。"
        " ここでは、価格ソースを JForex CSV / GMO / fixture から選びます。"
    )
    mode_note.setWordWrap(True)
    mode_note.setStyleSheet("color: #475569;")
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
    mode_status.setStyleSheet(
        "background: #f8fafc; border: none; border-radius: 10px; padding: 10px; color: #0f172a;"
    )
    mode_form = QGridLayout()
    mode_form.setHorizontalSpacing(16)
    mode_form.setVerticalSpacing(8)
    mode_form.addWidget(block_label("運用モード"), 0, 0)
    mode_form.addWidget(block_label("市場データ"), 0, 1)
    mode_form.addWidget(mode_combo, 1, 0)
    mode_form.addWidget(source_combo, 1, 1)
    mode_form.setColumnStretch(0, 1)
    mode_form.setColumnStretch(1, 1)
    mode_layout.addWidget(mode_title)
    mode_layout.addWidget(mode_note)
    mode_layout.addLayout(mode_form)
    mode_layout.addWidget(stream_box)
    mode_layout.addWidget(mode_status)
    save_mode_button = QPushButton("運用モードを保存")
    set_button_role(save_mode_button, "primary")
    mode_layout.addWidget(save_mode_button, alignment=Qt.AlignLeft)
    cards_layout.addWidget(mode_card)

    sizing_card = QFrame()
    sizing_card.setObjectName("orderSizingCard")
    sizing_card.setStyleSheet(card_style("orderSizingCard"))
    sizing_layout = QVBoxLayout(sizing_card)
    sizing_title = QLabel("資金 / 注文サイズ")
    sizing_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    sizing_note = QLabel(
        "初期資産と注文サイズの両方をここで管理します。"
        " JPY 建ての資金量とリスクから数量を計算します。"
        " 実際の発注数量は最小数量と数量ステップに合わせて丸められます。"
    )
    sizing_note.setWordWrap(True)
    sizing_note.setStyleSheet("color: #475569;")
    sizing_combo = QComboBox()
    sizing_combo.addItem("定額", "fixed_amount")
    sizing_combo.addItem("資産比率", "equity_fraction")
    sizing_combo.addItem("リスク率", "risk_based")
    fixed_amount_input = QLineEdit()
    fixed_amount_input.setPlaceholderText("例: 300000")
    equity_fraction_input = QLineEdit()
    equity_fraction_input.setPlaceholderText("例: 0.10")
    risk_fraction_input = QLineEdit()
    risk_fraction_input.setPlaceholderText("例: 0.01")
    starting_cash_input = QDoubleSpinBox()
    starting_cash_input.setRange(100.0, 1_000_000_000.0)
    starting_cash_input.setDecimals(2)
    starting_cash_input.setSingleStep(1000.0)
    starting_cash_input.setSuffix(" JPY")
    starting_cash_input.setGroupSeparatorShown(True)
    sizing_status = QLabel()
    sizing_status.setWordWrap(True)
    sizing_status.setStyleSheet(
        "background: #f8fafc; border: none; border-radius: 10px; padding: 10px; color: #0f172a;"
    )
    sizing_form = QGridLayout()
    sizing_form.setHorizontalSpacing(16)
    sizing_form.setVerticalSpacing(8)
    sizing_form.addWidget(block_label("初期資産"), 0, 0)
    sizing_form.addWidget(block_label("数量モード"), 0, 1)
    sizing_form.addWidget(block_label("定額 (JPY)"), 0, 2)
    sizing_form.addWidget(block_label("資産比率"), 0, 3)
    sizing_form.addWidget(block_label("リスク率"), 0, 4)
    sizing_form.addWidget(starting_cash_input, 1, 0)
    sizing_form.addWidget(sizing_combo, 1, 1)
    sizing_form.addWidget(fixed_amount_input, 1, 2)
    sizing_form.addWidget(equity_fraction_input, 1, 3)
    sizing_form.addWidget(risk_fraction_input, 1, 4)
    sizing_layout.addWidget(sizing_title)
    sizing_layout.addWidget(sizing_note)
    sizing_layout.addLayout(sizing_form)
    sizing_layout.addWidget(sizing_status)
    save_sizing_button = QPushButton("資金 / 注文サイズを保存")
    set_button_role(save_sizing_button, "primary")
    sizing_layout.addWidget(save_sizing_button, alignment=Qt.AlignLeft)
    cards_layout.addWidget(sizing_card)

    notifications_card = QFrame()
    notifications_card.setObjectName("notificationsCard")
    notifications_card.setStyleSheet(card_style("notificationsCard"))
    notifications_layout = QVBoxLayout(notifications_card)
    notifications_title = QLabel("通知チャネル")
    notifications_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    notifications_layout.addWidget(notifications_title)
    notifications_note = QLabel("注文、エラー、再接続、停止理由の通知先を切り替えます。")
    notifications_note.setWordWrap(True)
    notifications_note.setStyleSheet("color: #475569;")
    notifications_layout.addWidget(notifications_note)
    notify_enabled = QCheckBox("通知を有効化")
    desktop_box = QCheckBox("デスクトップ通知")
    sound_box = QCheckBox("サウンド")
    log_box = QCheckBox("ログ保存")
    webhook_box = QCheckBox("Webhook")
    sound_name = QLineEdit()
    sound_name.setClearButtonEnabled(True)
    webhook_url = QLineEdit()
    webhook_url.setClearButtonEnabled(True)
    log_path_label = QLabel()
    log_path_label.setWordWrap(True)
    log_path_label.setStyleSheet("background: #f8fafc; border: none; border-radius: 10px; padding: 10px;")
    channels_row = QHBoxLayout()
    for widget in (desktop_box, sound_box, log_box, webhook_box):
        channels_row.addWidget(widget)
    channels_row.addStretch(1)
    notifications_layout.addWidget(block_label("通知全体"))
    notifications_layout.addWidget(notify_enabled)
    notifications_layout.addWidget(block_label("チャネル"))
    notifications_layout.addLayout(channels_row)
    notifications_layout.addWidget(block_label("サウンド名"))
    notifications_layout.addWidget(sound_name)
    notifications_layout.addWidget(block_label("Webhook URL"))
    notifications_layout.addWidget(webhook_url)
    notifications_layout.addWidget(block_label("ログ出力先"))
    notifications_layout.addWidget(log_path_label)
    save_notifications_button = QPushButton("通知設定を保存")
    set_button_role(save_notifications_button, "primary")
    notifications_layout.addWidget(save_notifications_button, alignment=Qt.AlignLeft)
    cards_layout.addWidget(notifications_card)

    connection_card = QFrame()
    connection_card.setObjectName("gmoConnectionCard")
    connection_card.setStyleSheet(card_style("gmoConnectionCard", "#cbd5e1"))
    connection_layout = QVBoxLayout(connection_card)
    connection_title = QLabel("GMO 接続確認")
    connection_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    connection_note = QLabel(
        "現在の接続テストは GMO の public API を使う read-only 確認なので API キーは不要です。"
        " private API キーは将来の private API / 実売買拡張に備えて、macOS キーチェーンへ安全に保存できます。"
    )
    connection_note.setWordWrap(True)
    connection_note.setStyleSheet("color: #475569;")
    api_key_input = QLineEdit()
    api_key_input.setPlaceholderText("GMO private API Key")
    api_key_input.setClearButtonEnabled(True)
    api_secret_input = QLineEdit()
    api_secret_input.setPlaceholderText("GMO private API Secret")
    api_secret_input.setClearButtonEnabled(True)
    api_secret_input.setEchoMode(QLineEdit.Password)
    credential_status = QLabel()
    credential_status.setWordWrap(True)
    credential_status.setStyleSheet(
        "background: #f8fafc; border: none; border-radius: 10px; padding: 10px; color: #0f172a;"
    )
    connection_status = QLabel("接続テストは未実行です。")
    connection_status.setWordWrap(True)
    connection_status.setStyleSheet(
        "background: #f8fafc; border: none; border-radius: 10px; padding: 10px; color: #0f172a;"
    )
    credential_form = QGridLayout()
    credential_form.setHorizontalSpacing(16)
    credential_form.setVerticalSpacing(8)
    credential_form.addWidget(block_label("private API Key"), 0, 0)
    credential_form.addWidget(block_label("private API Secret"), 0, 1)
    credential_form.addWidget(api_key_input, 1, 0)
    credential_form.addWidget(api_secret_input, 1, 1)
    credential_form.setColumnStretch(0, 1)
    credential_form.setColumnStretch(1, 1)
    save_credentials_button = QPushButton("private API を保存")
    set_button_role(save_credentials_button, "primary")
    clear_credentials_button = QPushButton("保存済みキーを削除")
    set_button_role(clear_credentials_button, "secondary")
    test_connection_button = QPushButton("GMO 接続テスト")
    set_button_role(test_connection_button, "secondary")
    connection_buttons = QHBoxLayout()
    connection_buttons.addWidget(save_credentials_button)
    connection_buttons.addWidget(clear_credentials_button)
    connection_buttons.addWidget(test_connection_button)
    connection_buttons.addStretch(1)
    connection_layout.addWidget(connection_title)
    connection_layout.addWidget(connection_note)
    connection_layout.addLayout(credential_form)
    connection_layout.addWidget(credential_status)
    connection_layout.addWidget(connection_status)
    connection_layout.addLayout(connection_buttons)
    cards_layout.addWidget(connection_card)

    summary_card = QFrame()
    summary_card.setObjectName("settingsSummaryCard")
    summary_card.setStyleSheet(card_style("settingsSummaryCard"))
    summary_layout = QVBoxLayout(summary_card)
    summary_title = QLabel("状態サマリー")
    summary_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setStyleSheet("background: #f8fafc; border: none; border-radius: 10px; padding: 12px;")
    summary_layout.addWidget(summary_title)
    summary_layout.addWidget(summary_label)
    cards_layout.addWidget(summary_card)

    test_card = QFrame()
    test_card.setObjectName("settingsTestCard")
    test_card.setStyleSheet(card_style("settingsTestCard"))
    test_layout = QVBoxLayout(test_card)
    test_title = QLabel("接続テスト結果")
    test_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    test_output = QTextEdit()
    test_output.setReadOnly(True)
    test_output.setMinimumHeight(220)
    test_layout.addWidget(test_title)
    test_layout.addWidget(test_output)
    cards_layout.addWidget(test_card)

    config_card = QFrame()
    config_card.setObjectName("settingsConfigCard")
    config_card.setStyleSheet(card_style("settingsConfigCard"))
    config_layout = QVBoxLayout(config_card)
    config_title = QLabel("現在の設定スナップショット")
    config_title.setStyleSheet("font-size: 16px; font-weight: 700;")
    config_text = QTextEdit()
    config_text.setReadOnly(True)
    config_text.setMinimumHeight(260)
    config_layout.addWidget(config_title)
    config_layout.addWidget(config_text)
    cards_layout.addWidget(config_card)

    def update_mode_status() -> None:
        selected_mode = str(mode_combo.currentData() or "local_sim")
        selected_source = str(source_combo.currentData() or "csv")
        if selected_mode == "gmo_sim":
            _set_combo_value(source_combo, "gmo")
            source_combo.setEnabled(False)
            stream_box.setEnabled(True)
            stream_box.setToolTip("GMO の価格を定期取得して実時間更新します。")
            mode_status.setStyleSheet(
                "background: #eef6ff; border: none; border-radius: 10px; padding: 10px; color: #0f3c78;"
            )
            mode_status.setText(
                "\n".join(
                    [
                        "現在の選択: GMO 実時間シミュレーション",
                        "市場データ: GMO public API に固定されます。",
                        "発注: すべてローカル約定です。",
                        "用途: フォワード検証で実運用に近い損益推移を確認します。",
                    ]
                )
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
        mode_status.setStyleSheet(
            "background: #f8fafc; border: none; border-radius: 10px; padding: 10px; color: #0f172a;"
        )
        mode_status.setText(
            "\n".join(
                [
                    "現在の選択: ローカルシミュレーション",
                    f"市場データ: {source_text}",
                    f"実時間更新: {'有効化できます' if stream_allowed else 'このソースでは無効です'}",
                ]
            )
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
            " バックテストと同じ考え方でフォワード検証できます。"
        )

    def _parse_positive_float(editor: QLineEdit, default: float) -> float:
        text_value = editor.text().strip()
        if not text_value:
            return default
        return float(text_value)

    def save_order_sizing() -> None:
        try:
            app_state.update_account_settings(starting_cash=float(starting_cash_input.value()))
            app_state.update_order_sizing(
                order_size_mode=str(sizing_combo.currentData() or "fixed_amount"),
                fixed_order_amount=_parse_positive_float(
                    fixed_amount_input,
                    app_state.config.risk.fixed_order_amount,
                ),
                equity_fraction_per_trade=_parse_positive_float(
                    equity_fraction_input,
                    app_state.config.risk.equity_fraction_per_trade,
                ),
                risk_per_trade=_parse_positive_float(
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
            f"GMO public API: 接続成功\n市場データ: {market_ok}\n確認通貨ペア: {result.get('market_data_symbol', '-')}"
        )
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
            "\n".join(
                [
                    f"設定ファイル: {app_state.config_path}",
                    f"運用モード: {app_state.config.broker.mode.value}",
                    f"市場データ: {app_state.config.data.source}",
                    f"実時間更新: {'有効' if app_state.config.data.stream_enabled else '無効'}",
                    f"口座通貨: {app_state.config.risk.account_currency}",
                    f"初期資産: {app_state.config.risk.starting_cash:,.0f} {app_state.config.risk.account_currency}",
                    f"定額エントリー: {app_state.config.risk.fixed_order_amount:,.0f} {app_state.config.risk.account_currency}",
                    f"監視通貨ペア: {len(app_state.config.watchlist.symbols)} ペア",
                    f"インポート先: {app_state.config.data.import_dir}",
                    f"キャッシュ先: {app_state.config.data.cache_dir}",
                    (
                        "GMO private API: 設定済み"
                        f" ({credential_source_text(str(private_status['source']))})"
                        if private_configured
                        else "GMO private API: 未設定"
                    ),
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
        starting_cash_input.setValue(float(app_state.config.risk.starting_cash))
        fixed_amount_input.setText(f"{app_state.config.risk.fixed_order_amount:.2f}")
        equity_fraction_input.setText(f"{app_state.config.risk.equity_fraction_per_trade:.4f}")
        risk_fraction_input.setText(f"{app_state.config.risk.risk_per_trade:.4f}")
        update_sizing_status()
        sound_name.setText(app_state.config.automation.notification_channels.sound_name)
        webhook_url.setText(app_state.config.automation.notification_channels.webhook_url)
        log_path_label.setText(str(app_state.config.automation.notification_channels.log_path))
        api_key_input.setText(str(private_values.get("api_key", "")))
        api_secret_input.setText(str(private_values.get("api_secret", "")))
        credential_status.setText(
            "\n".join(
                [
                    "private API 資格情報",
                    f"保存状態: {'設定済み' if private_configured else '未設定'}",
                    f"保存元: {credential_source_text(str(private_status['source']))}",
                    (
                        "保存先: macOS キーチェーン"
                        if private_status.get("keychain_available")
                        else "保存先: この環境では macOS キーチェーンを利用できません"
                    ),
                    f"API Key: {private_values.get('api_key_masked') or '未設定'}",
                ]
            )
        )
        connection_status.setText(
            "\n".join(
                [
                    "GMO public API: 認証不要",
                    (
                        f"GMO private API: {'設定済み' if private_configured else '未設定'}"
                        f" ({credential_source_text(str(private_status['source']))})"
                    ),
                    (
                        "最終テスト: 実行済み"
                        if page.last_test_result and "error" not in page.last_test_result
                        else "最終テスト: 未実行"
                        if not page.last_test_result
                        else "最終テスト: 失敗"
                    ),
                ]
            )
        )
        config_text.setPlainText(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
        render_test_result()

    page.refresh = refresh
    return page


def build_help_page():  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QLabel, QTextEdit, QVBoxLayout, QWidget

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("ヘルプ")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)
    text = QTextEdit()
    text.setReadOnly(True)
    text.setPlainText(
        "\n".join(
            [
                "FXAutoTrade Lab ガイド",
                "- JForex の CSV をインポートすると、1分足から複数時間足のキャッシュを作成できます。",
                "- GMO の public API は実時間データ取得に使用します。現在の売買はすべてローカルシミュレーションです。",
                "- バックテストとフォワード検証の損益計算はローカルで行います。",
                "",
                "おすすめの進め方",
                "- まずは 3-8 通貨ペアほどをウォッチリストへ追加します。",
                "- データ同期ページから JForex CSV を取り込み、必要な時間足キャッシュを作成します。",
                "- その後にバックテストで期間別の成績を確認し、GMO 実時間シミュレーションで前向き検証します。",
                "",
                "現時点の制約",
                "- UI からの実売買はまだ有効化していません。",
                "- GMO private API キーは将来拡張用で、現段階では必須ではありません。",
                "- 口座通貨は JPY 前提で損益計算しています。",
                "",
                "本アプリは投資助言ではありません。",
            ]
        )
    )
    layout.addWidget(text, 1)
    return page
