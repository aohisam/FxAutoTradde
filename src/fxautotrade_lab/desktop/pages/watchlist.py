"""Watchlist editor page."""

from __future__ import annotations

from fxautotrade_lab.core.symbols import normalize_fx_symbol


def build_watchlist_page(app_state, log_message):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import (
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

    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("監視通貨ペア")
    title.setProperty("role", "h1")
    subtitle = QLabel("例: USD_JPY / USD/JPY / USDJPY のいずれでも入力できます。")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)

    reload_button = QPushButton("再読込")
    reload_button.setProperty("variant", "ghost")
    save_button = QPushButton("ウォッチリスト保存")
    save_button.setProperty("variant", "primary")
    header_row.addWidget(reload_button)
    header_row.addWidget(save_button)
    layout.addLayout(header_row)

    banner = Card(sunken=True)
    banner_label = QLabel(
        "運用通貨ペア、比較通貨ペア、補助通貨ペアを個別に管理します。"
        " まずは 3〜8 ペア程度から運用することを推奨します。"
    )
    banner_label.setWordWrap(True)
    banner_label.setProperty("role", "muted")
    banner.addBodyWidget(banner_label)
    layout.addWidget(banner)

    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(3):
        grid.setColumnStretch(column, 1)

    page_lists: dict[str, QListWidget] = {}
    page_inputs: dict[str, QLineEdit] = {}
    section_chips: dict[str, Chip] = {}

    titles = [
        ("symbols", "運用通貨ペア"),
        ("benchmarks", "比較通貨ペア"),
        ("sectors", "補助通貨ペア"),
    ]

    def build_section(key: str, caption: str, column: int) -> None:
        chip = Chip("0 ペア", "info" if key == "symbols" else "neutral")
        section_chips[key] = chip
        card = Card(title=caption, header_right=chip)

        list_widget = QListWidget()
        list_widget.setSelectionMode(QListWidget.ExtendedSelection)
        list_widget.setMinimumHeight(220)
        card.addBodyWidget(list_widget, 1)

        input_row = QHBoxLayout()
        editor = QLineEdit()
        editor.setPlaceholderText("例: USD_JPY")
        add_button = QPushButton("追加")
        add_button.setProperty("variant", "primary")
        remove_button = QPushButton("削除")
        remove_button.setProperty("variant", "ghost")
        input_row.addWidget(editor, 1)
        input_row.addWidget(add_button)
        input_row.addWidget(remove_button)
        card.addBodyLayout(input_row)

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

        grid.addWidget(card, 0, column)

    for column, (key, caption) in enumerate(titles):
        build_section(key, caption, column)
    layout.addLayout(grid, 1)

    summary_card = Card(title="現在の概要", subtitle="保存前のスナップショット")
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setProperty("role", "muted")
    summary_card.addBodyWidget(summary_label)
    page.summary_label = summary_label
    layout.addWidget(summary_card)

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
        section_chips["symbols"].set_text(f"{len(symbols)} ペア")
        section_chips["benchmarks"].set_text(f"{len(benchmarks)} ペア")
        section_chips["sectors"].set_text(f"{len(sectors)} ペア")
        summary_label.setText(
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
