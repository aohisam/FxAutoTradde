"""Grouped navigation list for the main window sidebar."""

from __future__ import annotations

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QBrush, QColor, QFont, QIcon
from PySide6.QtWidgets import QListWidget, QListWidgetItem, QWidget

from fxautotrade_lab.desktop.theme import Tokens, load_icon


class GroupedNavList(QListWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("navSidebar")
        self.setFocusPolicy(Qt.NoFocus)
        self.setUniformItemSizes(False)
        self.setMinimumWidth(220)
        self.setMaximumWidth(260)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setIconSize(QSize(18, 18))

    def add_group(self, caption: str) -> QListWidgetItem:
        item = QListWidgetItem(caption)
        item.setFlags(Qt.NoItemFlags)
        item.setData(Qt.UserRole, "__group__")
        font = QFont()
        font.setPointSize(8)
        font.setBold(True)
        font.setCapitalization(QFont.AllUppercase)
        item.setFont(font)
        item.setForeground(QBrush(QColor(Tokens.MUTED_2)))
        self.addItem(item)
        return item

    def add_page(
        self,
        label: str,
        page_key: str,
        icon_name: str | None = None,
    ) -> QListWidgetItem:
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, page_key)
        if icon_name:
            base = load_icon(icon_name, Tokens.INVERSE_2, 18)
            selected_pm = load_icon(icon_name, Tokens.INVERSE, 18).pixmap(18, 18)
            if not selected_pm.isNull():
                base.addPixmap(selected_pm, QIcon.Selected)
            item.setIcon(base)
        self.addItem(item)
        return item

    def row_for_page(self, page_key: str) -> int:
        for row in range(self.count()):
            item = self.item(row)
            if item.data(Qt.UserRole) == page_key:
                return row
        return -1
