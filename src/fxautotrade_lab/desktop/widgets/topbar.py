"""Topbar — breadcrumb + search pill + action buttons."""

from __future__ import annotations

import sys

from PySide6.QtCore import QSize, Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QWidget,
)

from ..theme import Tokens, load_icon


class Topbar(QWidget):
    refreshRequested = Signal()
    brokerCheckRequested = Signal()
    demoRunRequested = Signal()
    searchActivated = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("Topbar")
        self.setFixedHeight(48)

        row = QHBoxLayout(self)
        row.setContentsMargins(18, 0, 18, 0)
        row.setSpacing(12)

        # ---- Breadcrumb ----
        self._crumb_section = QLabel("-")
        self._crumb_section.setObjectName("CrumbSection")
        sep = QLabel("›")
        sep.setObjectName("CrumbSep")
        self._crumb_now = QLabel("-")
        self._crumb_now.setObjectName("CrumbNow")
        row.addWidget(self._crumb_section)
        row.addWidget(sep)
        row.addWidget(self._crumb_now)
        row.addStretch(1)

        # ---- Search pill ----
        self._search = QPushButton()
        self._search.setObjectName("TbSearch")
        self._search.setCursor(Qt.IBeamCursor)
        self._search.setFixedSize(280, 30)
        search_row = QHBoxLayout(self._search)
        search_row.setContentsMargins(10, 0, 10, 0)
        search_row.setSpacing(8)
        search_icon = QLabel()
        search_icon.setObjectName("TbSearchIcon")
        search_icon.setFixedSize(14, 14)
        search_icon.setPixmap(load_icon("search", Tokens.MUTED_2, 14).pixmap(QSize(14, 14)))
        placeholder = QLabel("通貨ペア・実行IDを検索")
        placeholder.setObjectName("TbSearchPlaceholder")
        kbd = QLabel("⌘K" if sys.platform == "darwin" else "Ctrl+K")
        kbd.setObjectName("TbKbd")
        search_row.addWidget(search_icon)
        search_row.addWidget(placeholder)
        search_row.addStretch(1)
        search_row.addWidget(kbd)
        self._search.clicked.connect(self.searchActivated)
        row.addWidget(self._search)

        # ---- Action buttons ----
        refresh_btn = self._make_button("ページ更新", "top_refresh", primary=False)
        broker_btn = self._make_button("ブローカー確認", "top_broker", primary=False)
        divider = QFrame()
        divider.setObjectName("TbDivider")
        divider.setFixedSize(1, 20)
        demo_btn = self._make_button("デモ実行", "top_demo", primary=True)

        refresh_btn.clicked.connect(self.refreshRequested)
        broker_btn.clicked.connect(self.brokerCheckRequested)
        demo_btn.clicked.connect(self.demoRunRequested)

        for widget in (refresh_btn, broker_btn, divider, demo_btn):
            row.addWidget(widget)

        self._refresh_btn = refresh_btn
        self._broker_btn = broker_btn
        self._demo_btn = demo_btn

    def _make_button(self, text: str, icon_name: str, *, primary: bool) -> QPushButton:
        button = QPushButton(text)
        button.setObjectName("TbBtn")
        button.setProperty("variant", "primary" if primary else "default")
        color = Tokens.INVERSE if primary else Tokens.MUTED
        button.setIcon(load_icon(icon_name, color, 14))
        button.setIconSize(QSize(14, 14))
        button.setFixedHeight(30)
        return button

    def set_crumbs(self, section: str, page_label: str) -> None:
        self._crumb_section.setText(section or "-")
        self._crumb_now.setText(page_label or "-")

    def set_busy(self, busy: bool) -> None:
        for button in (self._refresh_btn, self._broker_btn, self._demo_btn):
            button.setEnabled(not busy)
