"""Data sync page."""

from __future__ import annotations

from datetime import datetime
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


SEG_SOURCE_LABELS = ["GMO 実時間", "fixture", "JForex CSV"]
SEG_SOURCE_KEYS = ["gmo", "fixture", "csv"]

SEG_TF_LABELS = ["1m", "5m", "15m", "1h", "4h"]
SEG_TF_MAP = {
    "1m": TimeFrame.MIN_1,
    "5m": TimeFrame.MIN_5,
    "15m": TimeFrame.MIN_15,
    "1h": TimeFrame.HOUR_1,
    "4h": TimeFrame.HOUR_4,
}

SOURCE_HINTS = {
    "gmo":     "GMO は選択期間のうち既存キャッシュにない区間だけを補完します。1分〜1時間足は 2023-10-28 以降のみ。",
    "fixture": "fixture は疑似データを再生成します。CSV や GMO キャッシュは埋めません。",
    "csv":     "CSV 履歴は右の CSV 履歴インポートから取り込みます。",
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
        QDateEdit,
        QFileDialog,
        QFormLayout,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QMessageBox,
        QProgressBar,
        QPushButton,
        QScrollArea,
        QTableView,
        QTextBrowser,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.banner import Banner
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl
    from fxautotrade_lab.desktop.widgets.suffix_input import LabeledSuffixInput
    from fxautotrade_lab.data.jforex import resolve_bid_ask_csv_selection

    DataFrameTableModel = load_dataframe_model_class()

    # ---- Scroll container ----
    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    # ---- Header ----
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("データ同期")
    title.setProperty("role", "h1")
    subtitle = QLabel(
        "GMO / fixture / JForex CSV から対象ペアの履歴を取得・キャッシュします。"
    )
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    import_button = QPushButton("CSV インポート")
    import_button.setProperty("variant", "ghost")
    sync_button = QPushButton("同期を実行")
    sync_button.setProperty("variant", "primary")
    header.addWidget(import_button)
    header.addWidget(sync_button)
    layout.addLayout(header)

    # ---- Banner ----
    layout.addWidget(
        Banner(
            "CSV は長期履歴の母体、GMO は空白期間の追加取得、fixture はテスト用疑似データです。"
        )
    )

    # ---- grid-2 cards ----
    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    grid.setColumnStretch(0, 1)
    grid.setColumnStretch(1, 1)

    # --- Sync card (LEFT) ---
    sync_card = Card(title="GMO / fixture 同期")
    form = QFormLayout()
    form.setHorizontalSpacing(12)
    form.setVerticalSpacing(10)
    form.setLabelAlignment(Qt.AlignLeft)

    default_source_key = app_state.config.data.source
    if default_source_key not in SEG_SOURCE_KEYS:
        default_source_key = "gmo"
    source_seg = SegmentedControl(
        SEG_SOURCE_LABELS,
        current=SEG_SOURCE_KEYS.index(default_source_key),
        data=SEG_SOURCE_KEYS,
    )

    start_date = QDateEdit()
    start_date.setCalendarPopup(True)
    start_date.setDisplayFormat("yyyy-MM-dd")
    end_date = QDateEdit()
    end_date.setCalendarPopup(True)
    end_date.setDisplayFormat("yyyy-MM-dd")
    start_date.setDate(QDate.fromString(app_state.config.data.start_date, "yyyy-MM-dd"))
    end_date.setDate(QDate.fromString(app_state.config.data.end_date, "yyyy-MM-dd"))

    tf_seg = SegmentedControl(SEG_TF_LABELS, current=2, data=SEG_TF_LABELS)
    try:
        entry_tf = app_state.config.strategy.entry_timeframe.value
        for index, label in enumerate(SEG_TF_LABELS):
            if SEG_TF_MAP[label].value == entry_tf:
                tf_seg.set_current(index)
                break
    except Exception:  # noqa: BLE001
        pass

    parallel = LabeledSuffixInput(value="4", suffix="並列")
    parallel.setFixedWidth(160)

    form.addRow("データソース", source_seg)
    form.addRow("開始日", start_date)
    form.addRow("終了日", end_date)
    form.addRow("足種", tf_seg)
    form.addRow("並列度", parallel)
    sync_card.addBodyLayout(form)

    helper_text = QLabel()
    helper_text.setWordWrap(True)
    helper_text.setProperty("role", "muted")
    sync_card.addBodyWidget(helper_text)

    progress = QProgressBar()
    progress.setAlignment(Qt.AlignCenter)
    progress.setValue(0)
    sync_card.addBodyWidget(progress)

    reload_row = QHBoxLayout()
    reload_button = QPushButton("再読込")
    reload_button.setProperty("variant", "ghost")
    reload_row.addStretch(1)
    reload_row.addWidget(reload_button)
    sync_card.addBodyLayout(reload_row)
    grid.addWidget(sync_card, 0, 0)

    # --- CSV import card (RIGHT) ---
    import_card = Card(title="CSV 履歴インポート")
    import_form = QFormLayout()
    import_form.setHorizontalSpacing(12)
    import_form.setVerticalSpacing(10)
    import_form.setLabelAlignment(Qt.AlignLeft)

    file_row = QHBoxLayout()
    file_row.setSpacing(6)
    file_edit = QLineEdit()
    file_edit.setReadOnly(True)
    file_edit.setPlaceholderText("Bid と Ask の 2 ファイルを選択")
    choose_btn = QPushButton("選択…")
    choose_btn.setProperty("variant", "ghost")
    file_row.addWidget(file_edit, 1)
    file_row.addWidget(choose_btn)

    symbols_edit = QLineEdit()
    symbols_edit.setPlaceholderText("例: USD/JPY, EUR/USD")

    overwrite_box = QCheckBox("同一期間を上書き（推奨: OFF）")
    overwrite_box.setChecked(False)
    overwrite_box.setEnabled(False)

    import_form.addRow("対象ファイル", file_row)
    import_form.addRow("対象ペア", symbols_edit)
    import_form.addRow("既存キャッシュ", overwrite_box)
    import_card.addBodyLayout(import_form)

    import_btns = QHBoxLayout()
    import_btns.addStretch(1)
    preview_button = QPushButton("プレビュー")
    preview_button.setProperty("variant", "ghost")
    preview_button.setEnabled(False)
    run_import_button = QPushButton("インポート実行")
    run_import_button.setProperty("variant", "primary")
    import_btns.addWidget(preview_button)
    import_btns.addWidget(run_import_button)
    import_card.addBodyLayout(import_btns)
    grid.addWidget(import_card, 0, 1)
    layout.addLayout(grid)

    # ---- Result table card ----
    last_run_chip = Chip("直近実行: 未実行", "neutral")
    refresh_icon_btn = QPushButton("↻")
    refresh_icon_btn.setProperty("variant", "ghost")
    refresh_icon_btn.setFixedWidth(32)
    refresh_icon_btn.setToolTip("再読込")
    head_right = QWidget()
    hr = QHBoxLayout(head_right)
    hr.setContentsMargins(0, 0, 0, 0)
    hr.setSpacing(8)
    hr.addWidget(last_run_chip)
    hr.addWidget(refresh_icon_btn)
    result_card = Card(title="対象一覧 / キャッシュ状態", header_right=head_right)
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

    # ---- Log card ----
    log_card = Card(title="同期ログ")
    output = QTextBrowser()
    output.setProperty("role", "logdock")
    output.setOpenExternalLinks(False)
    output.setMinimumHeight(180)
    log_card.addBodyWidget(output)
    layout.addWidget(log_card)

    layout.addStretch(1)
    page._busy = False
    page._selected_files: list[str] = []

    # ---- Helpers ----
    def current_source_key() -> str:
        index = source_seg.current()
        if 0 <= index < len(SEG_SOURCE_KEYS):
            return SEG_SOURCE_KEYS[index]
        return SEG_SOURCE_KEYS[0]

    def selected_timeframes() -> list[TimeFrame]:
        label = SEG_TF_LABELS[tf_seg.current()] if 0 <= tf_seg.current() < len(SEG_TF_LABELS) else "15m"
        chosen = SEG_TF_MAP[label]
        required = [app_state.config.strategy.entry_timeframe, TimeFrame.DAY_1]
        ordered: list[TimeFrame] = [chosen]
        for timeframe in required:
            if timeframe not in ordered:
                ordered.append(timeframe)
        return ordered

    _LOG_COLORS = {
        "INFO": Tokens.INFO,
        "WARN": Tokens.WARN,
        "OK": Tokens.POS,
        "ERR": Tokens.NEG,
    }

    def append_log(level: str, message: str) -> None:
        color = _LOG_COLORS.get(level, Tokens.MUTED)
        timestamp = datetime.now().strftime("%H:%M:%S")
        html = (
            f'<div><span style="color:{Tokens.MUTED_2};">{timestamp}</span> '
            f'<span style="color:{color};font-weight:600;">[{level}]</span> '
            f'{message}</div>'
        )
        output.append(html)

    def set_busy(is_busy: bool) -> None:
        page._busy = is_busy
        sync_button.setText("同期実行中..." if is_busy else "同期を実行")
        for button in (sync_button, reload_button, import_button, run_import_button, choose_btn):
            set_button_enabled(button, not is_busy, busy=is_busy)
        source_seg.setEnabled(not is_busy)
        tf_seg.setEnabled(not is_busy)
        start_date.setEnabled(not is_busy)
        end_date.setEnabled(not is_busy)
        parallel.setEnabled(not is_busy)
        symbols_edit.setEnabled(not is_busy)

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
                    f"キャッシュ {len(files)} / {total_bytes / (1024 * 1024):.2f} MB",
                    f"ソース: {app_state.config.data.source}",
                ]
            )
        )

    def update_hint() -> None:
        helper_text.setText(SOURCE_HINTS.get(current_source_key(), ""))

    # ---- callbacks ----
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
        append_log("OK", f"CSV インポート完了: {result['symbol']}")
        append_log("INFO", f"新規反映行数: {result['imported_rows']:,}")
        append_log("INFO", f"重複スキップ: {result['skipped_rows']:,}")
        append_log("INFO", f"使用期間: {result['start']} - {result['end']}")
        applied_start = result.get("applied_start") or "-"
        applied_end = result.get("applied_end") or "-"
        append_log("INFO", f"今回反映: {applied_start} - {applied_end}")
        append_log(
            "INFO",
            f"Bid: {result.get('bid_start', '-')} - {result.get('bid_end', '-')} / "
            f"Ask: {result.get('ask_start', '-')} - {result.get('ask_end', '-')}",
        )
        append_log("INFO", f"作成時間足: {', '.join(result.get('timeframes', []))}")
        for message in result.get("messages", []) or []:
            append_log("WARN", str(message))
        QMessageBox.information(page, "完了", f"{result['symbol']} の Bid / Ask CSV を取り込みました。")
        log_message(f"Bid / Ask CSV を取り込みました: {result['symbol']}")

    def on_finished(result) -> None:  # noqa: ANN001
        set_busy(False)
        progress.setRange(0, 100)
        progress.setValue(100)
        refresh_summary()
        details = result.get("details", [])
        result_model.set_frame(_detail_frame(details))
        result_table.resizeColumnsToContents()
        start = result.get("start_date", app_state.config.data.start_date)
        end = result.get("end_date", app_state.config.data.end_date)
        last_run_chip.set_text(f"同期: {start} - {end}")
        last_run_chip.set_tone("running")
        append_log("OK", "同期完了")
        append_log(
            "INFO",
            f"監視 {result.get('symbols', 0)} / 比較 {result.get('benchmarks', 0)} / 補助 {result.get('sectors', 0)} ペア",
        )
        append_log("INFO", f"期間: {start} - {end}")
        append_log("INFO", f"時間足: {', '.join(result.get('timeframes', []))}")
        append_log("INFO", f"同期ソース: {result.get('source', app_state.config.data.source)}")
        mode_line = (
            "同期方法: 未取得期間のみ追加取得"
            if result.get("sync_mode") == "incremental"
            else "同期方法: fixture を使用"
        )
        append_log("INFO", mode_line)
        log_message("データ同期が完了しました。")

    def on_error(message: str) -> None:
        set_busy(False)
        progress.setRange(0, 100)
        progress.setValue(0)
        result_model.set_frame(None)
        last_run_chip.set_text("エラー")
        last_run_chip.set_tone("neg")
        append_log("ERR", message)
        log_message(f"データ同期エラー: {message}")

    def run_sync() -> None:
        set_busy(True)
        sync_source = current_source_key()
        sync_start = start_date.date().toString("yyyy-MM-dd")
        sync_end = end_date.date().toString("yyyy-MM-dd")
        sync_timeframes = selected_timeframes()
        progress.setRange(0, 0)
        result_model.set_frame(None)
        append_log("INFO", f"同期開始: {sync_source} / {sync_start} → {sync_end}")
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

    def _open_file_dialog() -> list[str]:
        paths, _ = QFileDialog.getOpenFileNames(
            page,
            "JForex CSV を選択",
            str(Path.home()),
            "CSV Files (*.csv)",
        )
        return list(paths)

    def choose_files() -> None:
        paths = _open_file_dialog()
        if not paths:
            return
        page._selected_files = paths
        file_edit.setText(", ".join(Path(p).name for p in paths))

    def import_csv() -> None:
        paths = list(page._selected_files) if page._selected_files else []
        if not paths:
            paths = _open_file_dialog()
            if not paths:
                return
            page._selected_files = paths
            file_edit.setText(", ".join(Path(p).name for p in paths))
        set_busy(True)
        progress.setRange(0, 0)
        result_model.set_frame(None)
        try:
            selection = resolve_bid_ask_csv_selection(paths)
        except ValueError as exc:
            on_error(str(exc))
            return
        append_log("INFO", "Bid / Ask CSV を取り込み中…")
        submit_task(
            lambda: app_state.import_jforex_bid_ask_csv(
                str(selection.bid_source_path),
                str(selection.ask_source_path),
                symbol=selection.symbol,
            ),
            on_import_finished,
            on_error,
        )

    sync_button.clicked.connect(run_sync)
    import_button.clicked.connect(import_csv)
    run_import_button.clicked.connect(import_csv)
    choose_btn.clicked.connect(choose_files)
    reload_button.clicked.connect(refresh_summary)
    refresh_icon_btn.clicked.connect(refresh_summary)
    source_seg.currentChanged.connect(lambda _: update_hint())

    page.refresh = refresh_summary
    update_hint()
    refresh_summary()
    return page
