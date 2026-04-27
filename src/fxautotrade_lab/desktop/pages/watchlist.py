"""Watchlist editor page."""

from __future__ import annotations

from fxautotrade_lab.core.symbols import normalize_fx_symbol


def build_watchlist_page(app_state, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QMessageBox,
        QPushButton,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.widgets.banner import Banner
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.chip_field import ChipField

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    # ---- Header ----
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("監視通貨ペア")
    title.setProperty("role", "h1")

    code_style = (
        f"font-family:{Tokens.FONT_MONO};"
        f"background:{Tokens.BG_SUNKEN};"
        f"color:{Tokens.INK};"
        "padding:1px 6px;border-radius:4px;font-size:11.5px;"
    )
    subtitle = QLabel(
        "運用・比較・補助の3系統で管理。"
        f'<code style="{code_style}">USD_JPY</code> / '
        f'<code style="{code_style}">USD/JPY</code> / '
        f'<code style="{code_style}">USDJPY</code> '
        "のいずれも受付。"
    )
    subtitle.setTextFormat(Qt.RichText)
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)

    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    btn_reload = QPushButton("再読込")
    btn_reload.setProperty("variant", "ghost")
    btn_save = QPushButton("ウォッチリスト保存")
    btn_save.setProperty("variant", "primary")
    header.addWidget(btn_reload)
    header.addWidget(btn_save)
    layout.addLayout(header)

    # ---- Banner ----
    banner = Banner(
        "推奨: まずは 3〜8 ペア程度から。比較通貨ペアは相対強弱の計算、"
        "補助通貨ペアは相関監視に使われます。"
    )
    layout.addWidget(banner)

    # ---- 3 sections ----
    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(3):
        grid.setColumnStretch(column, 1)

    section_chips: dict[str, Chip] = {}
    fields: dict[str, ChipField] = {}

    def show_warning(message: str) -> None:
        QMessageBox.warning(page, "警告", message)

    sections = [
        ("symbols",    "運用通貨ペア", "info",    "primary", "例: USD_JPY"),
        ("benchmarks", "比較通貨ペア", "neutral", "",        "例: USD_CHF"),
        ("sectors",    "補助通貨ペア", "neutral", "",        "例: NZD_JPY"),
    ]

    for column, (key, caption, chip_tone, add_variant, placeholder) in enumerate(sections):
        chip = Chip("0 ペア", chip_tone)
        section_chips[key] = chip
        card = Card(title=caption, header_right=chip)
        field = ChipField(
            placeholder=placeholder,
            add_button_variant=add_variant,
            normalize=normalize_fx_symbol,
            on_error=show_warning,
        )
        field.changed.connect(lambda: refresh_summary())
        card.addBodyWidget(field)
        fields[key] = field
        grid.addWidget(card, 0, column)
    layout.addLayout(grid)

    # ---- Summary card (4-cell detail-grid) ----
    summary_card = Card(title="現在の概要")
    summary_grid = QGridLayout()
    summary_grid.setHorizontalSpacing(16)
    summary_grid.setVerticalSpacing(12)
    summary_values: dict[str, QLabel] = {}
    cells = [
        ("operated", "運用通貨ペア"),
        ("bench",    "比較通貨ペア"),
        ("sector",   "補助通貨ペア"),
        ("saved",    "最終保存"),
    ]
    for index, (key, eyebrow_text) in enumerate(cells):
        summary_grid.setColumnStretch(index, 1)
        cell = QVBoxLayout()
        cell.setContentsMargins(0, 0, 0, 0)
        cell.setSpacing(2)
        eyebrow = QLabel(eyebrow_text.upper())
        eyebrow.setProperty("role", "eyebrow")
        value = QLabel("-")
        value.setProperty("role", "detail-value")
        value.setWordWrap(True)
        cell.addWidget(eyebrow)
        cell.addWidget(value)
        summary_grid.addLayout(cell, 0, index)
        summary_values[key] = value
    summary_card.addBodyLayout(summary_grid)
    layout.addWidget(summary_card)

    layout.addStretch(1)

    # ---- Behavior ----
    def refresh_summary() -> None:
        symbols = fields["symbols"].values()
        benchmarks = fields["benchmarks"].values()
        sectors = fields["sectors"].values()
        section_chips["symbols"].set_text(f"{len(symbols)} ペア")
        section_chips["benchmarks"].set_text(f"{len(benchmarks)} ペア")
        section_chips["sectors"].set_text(f"{len(sectors)} ペア")
        summary_values["operated"].setText(f"{len(symbols)} ペア")
        summary_values["bench"].setText(", ".join(benchmarks) if benchmarks else "未設定")
        summary_values["sector"].setText(", ".join(sectors) if sectors else "未設定")
        summary_values["saved"].setText(getattr(app_state.config.data, "end_date", "-"))

    def populate() -> None:
        fields["symbols"].set_values(app_state.config.watchlist.symbols)
        fields["benchmarks"].set_values(app_state.config.watchlist.benchmark_symbols)
        fields["sectors"].set_values(app_state.config.watchlist.sector_symbols)
        refresh_summary()

    def save_watchlist() -> None:
        try:
            symbols = [normalize_fx_symbol(v) for v in fields["symbols"].values()]
            benchmarks = [normalize_fx_symbol(v) for v in fields["benchmarks"].values()]
            sectors = [normalize_fx_symbol(v) for v in fields["sectors"].values()]
        except ValueError as exc:
            show_warning(str(exc))
            return
        if not symbols:
            show_warning("運用通貨ペアを 1 つ以上設定してください。")
            return
        app_state.update_watchlist(symbols, benchmarks, sectors)
        QMessageBox.information(page, "完了", "ウォッチリストを保存しました。")
        log_message("監視通貨ペアを更新しました。")
        populate()

    btn_reload.clicked.connect(populate)
    btn_save.clicked.connect(save_watchlist)
    page.refresh = lambda: populate() if page.isVisible() else None
    if page.isVisible():
        populate()
    return page
