"""Data sync page."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fxautotrade_lab.core.enums import TimeFrame


SYNC_TIMEFRAMES = [
    TimeFrame.MONTH_1,
    TimeFrame.WEEK_1,
    TimeFrame.DAY_1,
    TimeFrame.HOUR_12,
    TimeFrame.HOUR_8,
    TimeFrame.HOUR_4,
    TimeFrame.HOUR_1,
    TimeFrame.MIN_30,
    TimeFrame.MIN_15,
    TimeFrame.MIN_10,
    TimeFrame.MIN_5,
    TimeFrame.MIN_1,
]

CATEGORY_LABELS = {
    "watchlist": "運用通貨ペア",
    "benchmark": "比較通貨ペア",
    "sector": "補助通貨ペア",
}

SOURCE_LABELS = {
    "gmo": "GMO 初回取得",
    "gmo_incremental": "GMO 追加取得",
    "gmo_cache": "GMO キャッシュ",
    "gmo_runtime_refresh": "GMO 実時間更新",
    "csv_cache": "CSV キャッシュ",
    "csv_missing": "CSV 未取込",
    "fixture": "fixture 生成",
    "fixture_cache": "fixture キャッシュ",
    "gmo_empty": "GMO データなし",
}


def _detail_frame(details: list[dict[str, object]]) -> pd.DataFrame:
    if not details:
        return pd.DataFrame(
            columns=["区分", "通貨ペア", "時間足", "行数", "開始", "終了", "取得元", "更新", "キャッシュ保存先"]
        )
    frame = pd.DataFrame(details).copy()
    if "category" in frame.columns:
        frame["category"] = frame["category"].map(CATEGORY_LABELS).fillna(frame["category"])
    if "source" in frame.columns:
        frame["source"] = frame["source"].map(SOURCE_LABELS).fillna(frame["source"])
    if "refreshed" in frame.columns:
        frame["refreshed"] = frame["refreshed"].map({True: "再取得", False: "既存利用"}).fillna("")
    for column in ("start", "end"):
        if column in frame.columns:
            timestamps = pd.to_datetime(frame[column], errors="coerce")
            frame[column] = timestamps.dt.strftime("%Y-%m-%d %H:%M").fillna("")
    frame = frame.rename(
        columns={
            "category": "区分",
            "symbol": "通貨ペア",
            "timeframe": "時間足",
            "rows": "行数",
            "start": "開始",
            "end": "終了",
            "source": "取得元",
            "refreshed": "更新",
            "cache_path": "キャッシュ保存先",
        }
    )
    ordered = ["区分", "通貨ペア", "時間足", "行数", "開始", "終了", "取得元", "更新", "キャッシュ保存先"]
    return frame[ordered].sort_values(["区分", "通貨ペア", "時間足"], ignore_index=True)


def build_data_sync_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import QDate, Qt
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QDateEdit,
        QFileDialog,
        QFormLayout,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QMessageBox,
        QProgressBar,
        QPushButton,
        QScrollArea,
        QTableView,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.data.jforex import resolve_bid_ask_csv_selection

    DataFrameTableModel = load_dataframe_model_class()

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("データ同期")
    title.setProperty("role", "h1")
    subtitle = QLabel("CSV インポート / GMO 空白補完 / fixture 生成")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    import_button = QPushButton("Bid / Ask CSV をインポート")
    import_button.setProperty("variant", "ghost")
    sync_button = QPushButton("同期を実行")
    sync_button.setProperty("variant", "primary")
    header_row.addWidget(import_button)
    header_row.addWidget(sync_button)
    layout.addLayout(header_row)

    banner = Card(sunken=True)
    banner_label = QLabel(
        "CSV は長期履歴の母体として使い、GMO は既存キャッシュとの空白期間だけを追加取得する運用を前提にしています。"
        " CSV インポートはファイル内の共通期間で反映します。"
        " GMO は選択した期間内で未取得分だけを補完し、現在時刻まで追いつかせる用途に向いています。"
    )
    banner_label.setWordWrap(True)
    banner_label.setProperty("role", "muted")
    banner.addBodyWidget(banner_label)
    layout.addWidget(banner)

    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    grid.setColumnStretch(0, 1)
    grid.setColumnStretch(1, 1)

    # Sync card
    sync_card = Card(title="GMO / fixture 同期", subtitle="対象範囲と足種を指定")
    form = QFormLayout()
    form.setLabelAlignment(Qt.AlignRight)
    form.setFormAlignment(Qt.AlignTop)
    form.setHorizontalSpacing(12)
    form.setVerticalSpacing(10)
    source_combo = QComboBox()
    source_combo.addItem("GMO 空白補完", "gmo")
    source_combo.addItem("fixture 生成", "fixture")
    default_sync_source = "fixture" if app_state.config.data.source == "fixture" else "gmo"
    source_combo.setCurrentIndex(0 if default_sync_source == "gmo" else 1)
    start_date = QDateEdit()
    start_date.setCalendarPopup(True)
    start_date.setDisplayFormat("yyyy-MM-dd")
    end_date = QDateEdit()
    end_date.setCalendarPopup(True)
    end_date.setDisplayFormat("yyyy-MM-dd")
    start_date.setDate(QDate.fromString(app_state.config.data.start_date, "yyyy-MM-dd"))
    end_date.setDate(
        QDate.currentDate()
        if default_sync_source == "gmo"
        else QDate.fromString(app_state.config.data.end_date, "yyyy-MM-dd")
    )
    timeframe_checks: dict[str, QCheckBox] = {}
    timeframe_widget = QWidget()
    timeframe_layout = QGridLayout(timeframe_widget)
    timeframe_layout.setContentsMargins(0, 0, 0, 0)
    timeframe_layout.setHorizontalSpacing(10)
    timeframe_layout.setVerticalSpacing(6)
    for timeframe in SYNC_TIMEFRAMES:
        box = QCheckBox(timeframe.value)
        box.setChecked(timeframe in set(app_state.config.data.timeframes))
        timeframe_checks[timeframe.value] = box
    timeframe_columns = 4
    for index, timeframe in enumerate(SYNC_TIMEFRAMES):
        timeframe_layout.addWidget(
            timeframe_checks[timeframe.value],
            index // timeframe_columns,
            index % timeframe_columns,
        )
    for column in range(timeframe_columns):
        timeframe_layout.setColumnStretch(column, 1)
    form.addRow("同期ソース", source_combo)
    form.addRow("同期開始日", start_date)
    form.addRow("同期終了日", end_date)
    form.addRow("同期時間足", timeframe_widget)
    sync_card.addBodyLayout(form)
    helper_text = QLabel()
    helper_text.setWordWrap(True)
    helper_text.setProperty("role", "muted")
    sync_card.addBodyWidget(helper_text)
    progress = QProgressBar()
    progress.setAlignment(Qt.AlignCenter)
    progress.setValue(0)
    sync_card.addBodyWidget(progress)
    sync_button_row = QHBoxLayout()
    reload_button = QPushButton("再読込")
    reload_button.setProperty("variant", "ghost")
    sync_button_row.addStretch(1)
    sync_button_row.addWidget(reload_button)
    sync_card.addBodyLayout(sync_button_row)
    grid.addWidget(sync_card, 0, 0)

    # CSV import card
    import_card = Card(title="CSV 履歴インポート", subtitle="JForex Bid / Ask を同時取り込み")
    import_help = QLabel(
        "JForex の Bid / Ask 2ファイルを同時に選択して、履歴キャッシュを作成します。"
        " Bid / Ask の共通期間だけを反映します。"
    )
    import_help.setWordWrap(True)
    import_help.setProperty("role", "muted")
    import_card.addBodyWidget(import_help)
    overwrite_box = QCheckBox("同一期間を上書き（推奨: OFF）")
    overwrite_box.setChecked(False)
    overwrite_box.setEnabled(False)
    import_card.addBodyWidget(overwrite_box)
    import_card.addBodyWidget(QLabel(" "))  # spacer
    import_button_row = QHBoxLayout()
    preview_button = QPushButton("プレビュー")
    preview_button.setProperty("variant", "ghost")
    preview_button.setEnabled(False)
    run_import_button = QPushButton("インポート実行")
    run_import_button.setProperty("variant", "primary")
    import_button_row.addStretch(1)
    import_button_row.addWidget(preview_button)
    import_button_row.addWidget(run_import_button)
    import_card.addBodyLayout(import_button_row)
    grid.addWidget(import_card, 0, 1)
    layout.addLayout(grid)

    # Result table card
    last_run_chip = Chip("直近実行: 未実行", "neutral")
    result_card = Card(
        title="対象一覧 / キャッシュ状態",
        subtitle="各ペアの取得状況",
        header_right=last_run_chip,
    )
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setProperty("role", "muted")
    result_card.addBodyWidget(summary_label)
    result_table = QTableView()
    result_table.setAlternatingRowColors(False)
    result_table.setShowGrid(False)
    result_table.verticalHeader().setVisible(False)
    result_table.horizontalHeader().setStretchLastSection(True)
    result_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
    result_table.setMinimumHeight(320)
    result_model = DataFrameTableModel()
    result_table.setModel(result_model)
    result_card.addBodyWidget(result_table)
    layout.addWidget(result_card)

    # Log card
    log_card = Card(title="同期ログ", subtitle="最新の実行結果")
    output = QTextEdit()
    output.setReadOnly(True)
    output.setMinimumHeight(180)
    output.setProperty("role", "mono")
    log_card.addBodyWidget(output)
    layout.addWidget(log_card)

    layout.addStretch(1)
    page._busy = False

    # Wire up the two header action buttons to the internal buttons
    def invoke_sync() -> None:
        run_sync()

    def invoke_import() -> None:
        import_csv()

    sync_button.clicked.connect(invoke_sync)
    import_button.clicked.connect(invoke_import)

    # Provide a hidden primary sync button for layout (keep existing API)
    primary_sync_button = QPushButton()
    primary_sync_button.hide()

    def set_busy(is_busy: bool) -> None:
        page._busy = is_busy
        sync_button.setText("同期実行中..." if is_busy else "同期を実行")
        set_button_enabled(sync_button, not is_busy, busy=is_busy)
        set_button_enabled(reload_button, not is_busy, busy=is_busy)
        set_button_enabled(import_button, not is_busy, busy=is_busy)
        set_button_enabled(run_import_button, not is_busy, busy=is_busy)
        source_combo.setEnabled(not is_busy)
        start_date.setEnabled(not is_busy)
        end_date.setEnabled(not is_busy)
        for checkbox in timeframe_checks.values():
            checkbox.setEnabled(not is_busy)

    def refresh_summary() -> None:
        cache_dir = Path(app_state.config.data.cache_dir)
        files = list(cache_dir.rglob("*.parquet")) if cache_dir.exists() else []
        total_bytes = sum(path.stat().st_size for path in files) if files else 0
        summary_label.setText(
            "  •  ".join(
                [
                    f"運用: {len(app_state.config.watchlist.symbols)} ペア",
                    f"比較: {len(app_state.config.watchlist.benchmark_symbols)} ペア",
                    f"補助: {len(app_state.config.watchlist.sector_symbols)} ペア",
                    f"キャッシュ {len(files)} ファイル / {total_bytes / (1024 * 1024):.2f} MB",
                    f"アナリシスソース: {app_state.config.data.source}",
                ]
            )
        )

    def update_sync_source_hint() -> None:
        selected_source = str(source_combo.currentData() or source_combo.currentText())
        if selected_source == "fixture":
            helper_text.setText(
                "fixture はテスト用の疑似データを再生成します。"
                " CSV や GMO の既存キャッシュは埋めず、指定期間のテスト用データを作る用途です。"
                " 未選択でも解析に必要な 1Day と "
                f"{app_state.config.strategy.entry_timeframe.value} は自動で含めます。"
            )
            return
        helper_text.setText(
            "GMO は選択期間のうち既存キャッシュにない区間だけを補完します。"
            " 1分〜1時間足の GMO 取得は 2023-10-28 以降のみです。"
            " 未選択でも解析に必要な 1Day と "
            f"{app_state.config.strategy.entry_timeframe.value} は自動で含めます。"
        )

    def selected_timeframes() -> list[TimeFrame]:
        selected = [TimeFrame(value) for value, checkbox in timeframe_checks.items() if checkbox.isChecked()]
        required = [app_state.config.strategy.entry_timeframe, TimeFrame.DAY_1]
        ordered: list[TimeFrame] = []
        for timeframe in [*selected, *required]:
            if timeframe not in ordered:
                ordered.append(timeframe)
        return ordered

    def on_import_finished(result) -> None:  # noqa: ANN001
        set_busy(False)
        progress.setRange(0, 100)
        progress.setValue(100)
        refresh_summary()
        detail_rows = [
            {
                "category": "watchlist",
                "symbol": result["symbol"],
                "timeframe": timeframe,
                "rows": result["imported_rows"] if timeframe == "1Min" else "",
                "start": result["start"],
                "end": result["end"],
                "source": "csv_cache",
                "refreshed": True,
                "cache_path": result["cache_paths"].get(timeframe, ""),
            }
            for timeframe in result.get("timeframes", [])
        ]
        result_model.set_frame(_detail_frame(detail_rows))
        result_table.resizeColumnsToContents()
        last_run_chip.set_text(f"CSV: {result['symbol']}")
        last_run_chip.set_tone("info")
        output_lines = [
            "CSV インポート完了",
            f"通貨ペア: {result['symbol']}",
            f"新規反映行数: {result['imported_rows']:,}",
            f"重複スキップ行数: {result['skipped_rows']:,}",
            f"使用期間: {result['start']} - {result['end']}",
            f"今回反映した期間: {result['applied_start'] or '-'} - {result['applied_end'] or '-'}",
            f"Bid 元期間: {result.get('bid_start', '-')} - {result.get('bid_end', '-')}",
            f"Ask 元期間: {result.get('ask_start', '-')} - {result.get('ask_end', '-')}",
            f"作成時間足: {', '.join(result.get('timeframes', []))}",
            f"Bid ファイル: {result['bid_source_path']}",
            f"Ask ファイル: {result['ask_source_path']}",
        ]
        if result.get("messages"):
            output_lines.extend(["", "補足"] + list(result["messages"]))
        output.setPlainText("\n".join(output_lines))
        QMessageBox.information(page, "完了", f"{result['symbol']} の Bid / Ask CSV を取り込みました。")
        log_message(f"Bid / Ask CSV を取り込みました: {result['symbol']}")

    def on_finished(result) -> None:
        set_busy(False)
        progress.setRange(0, 100)
        progress.setValue(100)
        refresh_summary()
        details = result.get("details", [])
        result_model.set_frame(_detail_frame(details))
        result_table.resizeColumnsToContents()
        last_run_chip.set_text(
            f"同期: {result.get('start_date', '-')} - {result.get('end_date', '-')}"
        )
        last_run_chip.set_tone("running")
        output.setPlainText(
            "\n".join(
                [
                    "完了",
                    f"同期した監視通貨ペア数: {result.get('symbols', 0)}",
                    f"比較通貨ペア数: {result.get('benchmarks', 0)}",
                    f"補助通貨ペア数: {result.get('sectors', 0)}",
                    f"期間: {result.get('start_date', app_state.config.data.start_date)} - {result.get('end_date', app_state.config.data.end_date)}",
                    f"時間足: {', '.join(result.get('timeframes', []))}",
                    f"同期ソース: {result.get('source', app_state.config.data.source)}",
                    (
                        "同期方法: 未取得期間のみ追加取得"
                        if result.get("sync_mode") == "incremental"
                        else "同期方法: fixture を使用"
                    ),
                ]
            )
        )
        log_message("データ同期が完了しました。")

    def on_error(message: str) -> None:
        set_busy(False)
        progress.setRange(0, 100)
        progress.setValue(0)
        result_model.set_frame(None)
        last_run_chip.set_text("エラー")
        last_run_chip.set_tone("neg")
        output.setPlainText(f"エラー\n{message}")
        log_message(f"データ同期エラー: {message}")

    def run_sync() -> None:
        set_busy(True)
        sync_source = str(source_combo.currentData() or source_combo.currentText())
        sync_start = start_date.date().toString("yyyy-MM-dd")
        sync_end = end_date.date().toString("yyyy-MM-dd")
        sync_timeframes = selected_timeframes()
        progress.setRange(0, 0)
        result_model.set_frame(None)
        output.setPlainText(
            "バックグラウンドで同期中...\n"
            "GMO / fixture から選択期間の空白区間だけを取得し、通貨ペア別のキャッシュ結果を更新しています。"
        )
        submit_task(
            lambda: app_state.sync_market_data(
                source=sync_source,
                start_date=sync_start,
                end_date=sync_end,
                timeframes=sync_timeframes,
            ),
            on_finished,
            on_error,
        )

    def import_csv() -> None:
        file_paths, _ = QFileDialog.getOpenFileNames(
            page,
            "JForex CSV を選択",
            str(Path.home()),
            "CSV Files (*.csv)",
        )
        if not file_paths:
            return
        set_busy(True)
        progress.setRange(0, 0)
        result_model.set_frame(None)
        try:
            selection = resolve_bid_ask_csv_selection(file_paths)
        except ValueError as exc:
            on_error(str(exc))
            return
        output.setPlainText(
            "Bid / Ask CSV を取り込み中...\n"
            "ファイル名・通貨ペア・期間を検証し、共通期間だけを 1 分足 quote bar として反映しています。"
        )
        submit_task(
            lambda: app_state.import_jforex_bid_ask_csv(
                str(selection.bid_source_path),
                str(selection.ask_source_path),
                symbol=selection.symbol,
            ),
            on_import_finished,
            on_error,
        )

    run_import_button.clicked.connect(import_csv)
    reload_button.clicked.connect(refresh_summary)
    source_combo.currentIndexChanged.connect(update_sync_source_hint)
    page.refresh = refresh_summary
    update_sync_source_hint()
    refresh_summary()
    return page
