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
        QLabel,
        QMessageBox,
        QProgressBar,
        QPushButton,
        QSplitter,
        QTableView,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled, set_button_role
    from fxautotrade_lab.data.jforex import resolve_bid_ask_csv_selection

    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("データ同期")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)

    banner = QLabel(
        "CSV は長期履歴の母体として使い、GMO は既存キャッシュとの空白期間だけを追加取得する運用を前提にしています。"
        " CSV インポートはファイル内の共通期間で反映し、同期期間 UI は使いません。"
        " GMO は選択した期間内で未取得分だけを補完し、現在時刻まで追いつかせる用途に向いています。"
    )
    banner.setWordWrap(True)
    banner.setStyleSheet("background: #f3f7fb; border-radius: 12px; padding: 12px;")
    layout.addWidget(banner)

    grid = QGridLayout()
    summary_card = QFrame()
    summary_card.setObjectName("dataSyncSummaryCard")
    summary_card.setStyleSheet(
        "QFrame#dataSyncSummaryCard { background: white; border: 1px solid #dbe3ee; border-radius: 14px; }"
    )
    summary_layout = QVBoxLayout(summary_card)
    watchlist_summary = QLabel()
    watchlist_summary.setWordWrap(True)
    cache_summary = QLabel()
    cache_summary.setWordWrap(True)
    summary_layout.addWidget(QLabel("対象一覧"))
    summary_layout.addWidget(watchlist_summary)
    summary_layout.addWidget(QLabel("キャッシュ状態"))
    summary_layout.addWidget(cache_summary)
    grid.addWidget(summary_card, 0, 0)

    sync_card = QFrame()
    sync_card.setObjectName("dataSyncControlCard")
    sync_card.setStyleSheet(
        "QFrame#dataSyncControlCard { background: white; border: 1px solid #dbe3ee; border-radius: 14px; }"
    )
    control_layout = QVBoxLayout(sync_card)
    control_layout.addWidget(QLabel("GMO / fixture 同期"))
    form = QFormLayout()
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
    end_date.setDate(QDate.currentDate() if default_sync_source == "gmo" else QDate.fromString(app_state.config.data.end_date, "yyyy-MM-dd"))
    timeframe_checks: dict[str, QCheckBox] = {}
    timeframe_layout = QHBoxLayout()
    for timeframe in SYNC_TIMEFRAMES:
        box = QCheckBox(timeframe.value)
        box.setChecked(timeframe in set(app_state.config.data.timeframes))
        timeframe_checks[timeframe.value] = box
        timeframe_layout.addWidget(box)
    timeframe_layout.addStretch(1)
    form.addRow("同期ソース", source_combo)
    form.addRow("同期開始日", start_date)
    form.addRow("同期終了日", end_date)
    form.addRow("同期時間足", timeframe_layout)
    control_layout.addLayout(form)
    helper_text = QLabel()
    helper_text.setWordWrap(True)
    helper_text.setStyleSheet("color: #4d647d;")
    control_layout.addWidget(helper_text)
    progress = QProgressBar()
    progress.setAlignment(Qt.AlignCenter)
    progress.setValue(0)
    control_layout.addWidget(progress)
    button_row = QHBoxLayout()
    sync_button = QPushButton("実行")
    reload_button = QPushButton("再読込")
    set_button_role(sync_button, "primary")
    set_button_role(reload_button, "secondary")
    button_row.addWidget(sync_button)
    button_row.addWidget(reload_button)
    button_row.addStretch(1)
    control_layout.addLayout(button_row)
    grid.addWidget(sync_card, 0, 1)
    layout.addLayout(grid)

    import_card = QFrame()
    import_card.setObjectName("dataSyncImportCard")
    import_card.setStyleSheet(
        "QFrame#dataSyncImportCard { background: white; border: 1px solid #dbe3ee; border-radius: 14px; }"
    )
    import_layout = QVBoxLayout(import_card)
    import_layout.addWidget(QLabel("CSV 履歴インポート"))
    import_help = QLabel(
        "JForex の Bid / Ask 2ファイルを同時に選択して、履歴キャッシュを作成します。"
        " CSV インポートでは開始日・終了日は使わず、Bid / Ask の共通期間だけを反映します。"
        " まず CSV で長期履歴を入れ、その後に GMO 同期で空白だけを補完する使い方を推奨します。"
    )
    import_help.setWordWrap(True)
    import_help.setStyleSheet("color: #4d647d;")
    import_layout.addWidget(import_help)
    import_button = QPushButton("Bid / Ask CSV をインポート")
    set_button_role(import_button, "success")
    import_layout.addWidget(import_button, alignment=Qt.AlignLeft)
    layout.addWidget(import_card)

    result_splitter = QSplitter(Qt.Vertical)
    output = QTextEdit()
    output.setReadOnly(True)
    result_table = QTableView()
    result_table.setAlternatingRowColors(True)
    result_model = DataFrameTableModel()
    result_table.setModel(result_model)
    result_splitter.addWidget(output)
    result_splitter.addWidget(result_table)
    result_splitter.setStretchFactor(0, 2)
    result_splitter.setStretchFactor(1, 5)
    layout.addWidget(result_splitter, 1)
    page._busy = False

    def set_busy(is_busy: bool) -> None:
        page._busy = is_busy
        sync_button.setText("同期実行中..." if is_busy else "実行")
        set_button_enabled(sync_button, not is_busy, busy=is_busy)
        set_button_enabled(reload_button, not is_busy, busy=is_busy)
        set_button_enabled(import_button, not is_busy, busy=is_busy)
        source_combo.setEnabled(not is_busy)
        start_date.setEnabled(not is_busy)
        end_date.setEnabled(not is_busy)
        for checkbox in timeframe_checks.values():
            checkbox.setEnabled(not is_busy)

    def refresh_summary() -> None:
        watchlist_summary.setText(
            "\n".join(
                [
                    f"運用通貨ペア: {', '.join(app_state.config.watchlist.symbols)}",
                    f"比較通貨ペア: {', '.join(app_state.config.watchlist.benchmark_symbols) if app_state.config.watchlist.benchmark_symbols else '未設定'}",
                    (
                        "補助通貨ペア: "
                        f"{', '.join(app_state.config.watchlist.sector_symbols) if app_state.config.watchlist.sector_symbols else '未設定'}"
                    ),
                ]
            )
        )
        cache_dir = Path(app_state.config.data.cache_dir)
        files = list(cache_dir.rglob("*.parquet")) if cache_dir.exists() else []
        total_bytes = sum(path.stat().st_size for path in files) if files else 0
        cache_summary.setText(
            "\n".join(
                [
                    f"キャッシュディレクトリ: {cache_dir}",
                    f"ファイル数: {len(files)}",
                    f"合計サイズ: {total_bytes / (1024 * 1024):.2f} MB",
                    f"現在の分析用データソース: {app_state.config.data.source}",
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
            " まず CSV で履歴を入れ、その後に現在時刻までの空白を GMO で埋める運用を推奨します。"
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
        result_model.set_frame(
            _detail_frame(detail_rows)
        )
        result_table.resizeColumnsToContents()
        output_lines = [
            "CSV インポート完了",
            f"通貨ペア: {result['symbol']}",
            f"新規反映行数: {result['imported_rows']:,}",
            f"重複スキップ行数: {result['skipped_rows']:,}",
            f"使用期間: {result['start']} - {result['end']}",
            f"今回反映した期間: {result['applied_start'] or '-'} - {result['applied_end'] or '-'}",
            f"Bid 元期間: {result.get('bid_start', '-') } - {result.get('bid_end', '-')}",
            f"Ask 元期間: {result.get('ask_start', '-') } - {result.get('ask_end', '-')}",
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

    import_button.clicked.connect(import_csv)
    sync_button.clicked.connect(run_sync)
    reload_button.clicked.connect(refresh_summary)
    source_combo.currentIndexChanged.connect(update_sync_source_hint)
    page.refresh = refresh_summary
    update_sync_source_hint()
    refresh_summary()
    return page
