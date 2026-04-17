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

    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("データ同期")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)

    banner = QLabel(
        "JForex の CSV を取り込むと、通貨ペアごとに複数時間足のキャッシュを作成します。"
        " Bid/Ask を分けた 2 ファイル、または bid_* / ask_* を含む 1 ファイル quote CSV を推奨します。"
        " GMO を選ぶと、既存キャッシュは保持したまま、指定期間の未取得分だけを追加取得します。"
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

    control_card = QFrame()
    control_card.setObjectName("dataSyncControlCard")
    control_card.setStyleSheet(
        "QFrame#dataSyncControlCard { background: white; border: 1px solid #dbe3ee; border-radius: 14px; }"
    )
    control_layout = QVBoxLayout(control_card)
    form = QFormLayout()
    source_combo = QComboBox()
    for source in ("csv", "gmo", "fixture"):
        source_combo.addItem(source, source)
    source_combo.setCurrentText(app_state.config.data.source if app_state.config.data.source in {"csv", "gmo", "fixture"} else "csv")
    start_date = QDateEdit()
    start_date.setCalendarPopup(True)
    start_date.setDisplayFormat("yyyy-MM-dd")
    end_date = QDateEdit()
    end_date.setCalendarPopup(True)
    end_date.setDisplayFormat("yyyy-MM-dd")
    start_date.setDate(QDate.fromString(app_state.config.data.start_date, "yyyy-MM-dd"))
    end_date.setDate(QDate.fromString(app_state.config.data.end_date, "yyyy-MM-dd"))
    timeframe_checks: dict[str, QCheckBox] = {}
    timeframe_layout = QHBoxLayout()
    for timeframe in SYNC_TIMEFRAMES:
        box = QCheckBox(timeframe.value)
        box.setChecked(timeframe in set(app_state.config.data.timeframes))
        timeframe_checks[timeframe.value] = box
        timeframe_layout.addWidget(box)
    timeframe_layout.addStretch(1)
    form.addRow("データソース", source_combo)
    form.addRow("開始日", start_date)
    form.addRow("終了日", end_date)
    form.addRow("時間足", timeframe_layout)
    control_layout.addLayout(form)
    helper_text = QLabel(
        "週足・月足は日足から派生します。未選択でも解析に必要な 1Day と "
        f"{app_state.config.strategy.entry_timeframe.value} は自動で含めます。"
    )
    helper_text.setWordWrap(True)
    helper_text.setStyleSheet("color: #4d647d;")
    control_layout.addWidget(helper_text)
    progress = QProgressBar()
    progress.setAlignment(Qt.AlignCenter)
    progress.setValue(0)
    control_layout.addWidget(progress)
    button_row = QHBoxLayout()
    import_button = QPushButton("CSV インポート")
    sync_button = QPushButton("実行")
    reload_button = QPushButton("再読込")
    set_button_role(import_button, "success")
    set_button_role(sync_button, "primary")
    set_button_role(reload_button, "secondary")
    button_row.addWidget(import_button)
    button_row.addWidget(sync_button)
    button_row.addWidget(reload_button)
    button_row.addStretch(1)
    control_layout.addLayout(button_row)
    grid.addWidget(control_card, 0, 1)
    layout.addLayout(grid)

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
                ]
            )
        )
        source_combo.setCurrentText(app_state.config.data.source)
        start_date.setDate(QDate.fromString(app_state.config.data.start_date, "yyyy-MM-dd"))
        end_date.setDate(QDate.fromString(app_state.config.data.end_date, "yyyy-MM-dd"))
        selected = {timeframe.value for timeframe in app_state.config.data.timeframes}
        for timeframe in SYNC_TIMEFRAMES:
            timeframe_checks[timeframe.value].setChecked(timeframe.value in selected)

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
        result_model.set_frame(
            _detail_frame(
                [
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
            )
        )
        result_table.resizeColumnsToContents()
        output.setPlainText(
            "\n".join(
                [
                    "CSV インポート完了",
                    f"通貨ペア: {result['symbol']}",
                    f"行数: {result['imported_rows']:,}",
                    f"期間: {result['start']} - {result['end']}",
                    f"作成時間足: {', '.join(result.get('timeframes', []))}",
                    f"元ファイル: {result['source_path']}",
                ]
            )
        )
        source_combo.setCurrentText("csv")
        QMessageBox.information(page, "完了", f"{result['symbol']} の CSV を取り込みました。")
        log_message(f"CSV を取り込みました: {result['symbol']}")

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
                    f"データソース: {result.get('source', app_state.config.data.source)}",
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
        app_state.config.data.source = str(source_combo.currentData() or source_combo.currentText())
        app_state.config.data.start_date = start_date.date().toString("yyyy-MM-dd")
        app_state.config.data.end_date = end_date.date().toString("yyyy-MM-dd")
        app_state.config.data.timeframes = selected_timeframes()
        app_state.save_config()
        progress.setRange(0, 0)
        result_model.set_frame(None)
        output.setPlainText(
            "バックグラウンドで同期中...\n"
            "市場データを取得し、通貨ペア別のキャッシュ結果を更新しています。"
        )
        submit_task(app_state.sync_data, on_finished, on_error)

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
        if len(file_paths) == 1:
            output.setPlainText(
                "CSV を取り込み中...\n"
                "bid_* / ask_* を含む quote CSV はそのまま使い、単純 OHLC の場合は mid 系列として取り込みます。"
            )
            submit_task(
                lambda: app_state.import_jforex_csv(file_paths[0]),
                on_import_finished,
                on_error,
            )
            return
        if len(file_paths) == 2:
            lowered = {path.lower(): path for path in file_paths}
            bid_path = next((path for key, path in lowered.items() if "bid" in key), "")
            ask_path = next((path for key, path in lowered.items() if "ask" in key), "")
            if not bid_path or not ask_path:
                on_error("2ファイル選択時はファイル名に Bid / Ask を含めてください。")
                return
            output.setPlainText("Bid/Ask CSV を取り込み中...\n1分足の quote bar から複数時間足キャッシュを作成しています。")
            submit_task(
                lambda: app_state.import_jforex_bid_ask_csv(bid_path, ask_path),
                on_import_finished,
                on_error,
            )
            return
        on_error("CSV は 1ファイルまたは Bid/Ask の 2ファイルで選択してください。")

    import_button.clicked.connect(import_csv)
    sync_button.clicked.connect(run_sync)
    reload_button.clicked.connect(refresh_summary)
    page.refresh = refresh_summary
    refresh_summary()
    return page
