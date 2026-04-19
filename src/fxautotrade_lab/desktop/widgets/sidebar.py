"""Grouped navigation list for the main window sidebar."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QBrush, QColor, QFont
from PySide6.QtWidgets import QListWidget, QListWidgetItem, QWidget


class GroupedNavList(QListWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("navSidebar")
        self.setFocusPolicy(Qt.NoFocus)
        self.setUniformItemSizes(False)
        self.setMinimumWidth(220)
        self.setMaximumWidth(260)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    def add_group(self, caption: str) -> QListWidgetItem:
        item = QListWidgetItem(caption)
        item.setFlags(Qt.NoItemFlags)
        item.setData(Qt.UserRole, "__group__")
        font = QFont()
        font.setPointSize(8)
        font.setBold(True)
        font.setCapitalization(QFont.AllUppercase)
        item.setFont(font)
        item.setForeground(QBrush(QColor("#64748b")))
        self.addItem(item)
        return item

    def add_page(self, label: str, page_key: str) -> QListWidgetItem:
        item = QListWidgetItem(label)
        item.setData(Qt.UserRole, page_key)
        self.addItem(item)
        return item

    def row_for_page(self, page_key: str) -> int:
        for row in range(self.count()):
            item = self.item(row)
            if item.data(Qt.UserRole) == page_key:
                return row
        return -1
