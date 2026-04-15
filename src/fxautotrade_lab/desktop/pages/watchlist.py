"""Watchlist editor page."""

from __future__ import annotations

import re

from fxautotrade_lab.core.symbols import normalize_fx_symbol


def build_watchlist_page(app_state, log_message):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import (
        QFormLayout,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QListWidget,
        QListWidgetItem,
        QMessageBox,
        QPushButton,
        QVBoxLayout,
        QWidget,
    )

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("監視通貨ペア")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)

    helper = QLabel(
        "運用通貨ペア、比較通貨ペア、補助通貨ペアを個別に管理します。"
        " `USD_JPY` / `USD/JPY` / `USDJPY` のいずれでも入力できます。"
    )
    helper.setWordWrap(True)
    helper.setStyleSheet("background: #f3f7fb; border-radius: 12px; padding: 12px;")
    layout.addWidget(helper)

    grid = QGridLayout()
    page_lists: dict[str, QListWidget] = {}
    page_inputs: dict[str, QLineEdit] = {}
    titles = {
        "symbols": "運用通貨ペア",
        "benchmarks": "比較通貨ペア",
        "sectors": "補助通貨ペア",
    }

    def build_section(key: str, caption: str) -> QFrame:
        frame = QFrame()
        frame.setObjectName(f"watchlistSection_{key}")
        frame.setStyleSheet(
            f"QFrame#watchlistSection_{key} {{ background: white; border: 1px solid #dbe3ee; border-radius: 14px; }}"
        )
        section_layout = QVBoxLayout(frame)
        section_layout.addWidget(QLabel(caption))
        list_widget = QListWidget()
        list_widget.setSelectionMode(QListWidget.ExtendedSelection)
        section_layout.addWidget(list_widget, 1)
        input_row = QHBoxLayout()
        editor = QLineEdit()
        editor.setPlaceholderText("例: USD_JPY")
        add_button = QPushButton("追加")
        remove_button = QPushButton("削除")
        input_row.addWidget(editor, 1)
        input_row.addWidget(add_button)
        input_row.addWidget(remove_button)
        section_layout.addLayout(input_row)
        page_lists[key] = list_widget
        page_inputs[key] = editor

        def add_item() -> None:
            raw = editor.text().strip()
            if not raw:
                return
            try:
                value = normalize_fx_symbol(raw)
            except ValueError:
                QMessageBox.warning(page, "警告", f"無効な通貨ペアです: {raw}")
                return
            existing = {list_widget.item(index).text() for index in range(list_widget.count())}
            if value in existing:
                QMessageBox.information(page, "情報", f"{value} はすでに登録されています。")
                return
            list_widget.addItem(QListWidgetItem(value))
            editor.clear()
            refresh_summary()

        def remove_selected() -> None:
            for item in list_widget.selectedItems():
                list_widget.takeItem(list_widget.row(item))
            refresh_summary()

        add_button.clicked.connect(add_item)
        remove_button.clicked.connect(remove_selected)
        editor.returnPressed.connect(add_item)
        return frame

    for column, (key, caption) in enumerate(titles.items()):
        grid.addWidget(build_section(key, caption), 0, column)
    layout.addLayout(grid, 1)

    lower = QFormLayout()
    page.summary_label = QLabel()
    page.summary_label.setWordWrap(True)
    lower.addRow("現在の概要", page.summary_label)
    layout.addLayout(lower)

    action_row = QHBoxLayout()
    reload_button = QPushButton("再読込")
    save_button = QPushButton("ウォッチリスト保存")
    action_row.addWidget(reload_button)
    action_row.addStretch(1)
    action_row.addWidget(save_button)
    layout.addLayout(action_row)

    def list_values(key: str) -> list[str]:
        return [page_lists[key].item(index).text() for index in range(page_lists[key].count())]

    def populate() -> None:
        mapping = {
            "symbols": app_state.config.watchlist.symbols,
            "benchmarks": app_state.config.watchlist.benchmark_symbols,
            "sectors": app_state.config.watchlist.sector_symbols,
        }
        for key, values in mapping.items():
            page_lists[key].clear()
            for value in values:
                page_lists[key].addItem(QListWidgetItem(value))
            page_inputs[key].clear()
        refresh_summary()

    def refresh_summary() -> None:
        symbols = list_values("symbols")
        benchmarks = list_values("benchmarks")
        sectors = list_values("sectors")
        page.summary_label.setText(
            "\n".join(
                [
                    f"運用通貨ペア: {len(symbols)} ペア",
                    f"比較通貨ペア: {', '.join(benchmarks) if benchmarks else '未設定'}",
                    f"補助通貨ペア: {', '.join(sectors) if sectors else '未設定'}",
                    "推奨: まずは 3-8 ペア程度から開始",
                ]
            )
        )

    def save_watchlist() -> None:
        parsed_symbols = list_values("symbols")
        parsed_benchmarks = list_values("benchmarks")
        parsed_sectors = list_values("sectors")
        try:
            parsed_symbols = [normalize_fx_symbol(value) for value in parsed_symbols]
            parsed_benchmarks = [normalize_fx_symbol(value) for value in parsed_benchmarks]
            parsed_sectors = [normalize_fx_symbol(value) for value in parsed_sectors]
        except ValueError as exc:
            QMessageBox.warning(page, "警告", str(exc))
            return
        if not parsed_symbols:
            QMessageBox.warning(page, "警告", "運用通貨ペアを 1 つ以上設定してください。")
            return
        app_state.update_watchlist(parsed_symbols, parsed_benchmarks, parsed_sectors)
        QMessageBox.information(page, "完了", "ウォッチリストを保存しました。")
        log_message("監視通貨ペアを更新しました。")
        populate()

    reload_button.clicked.connect(populate)
    save_button.clicked.connect(save_watchlist)
    page.refresh = populate
    populate()
    return page
