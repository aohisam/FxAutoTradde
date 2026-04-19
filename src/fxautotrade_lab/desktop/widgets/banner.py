"""Banner — thin info banner with leading icon."""

from __future__ import annotations

from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QWidget


class Banner(QFrame):
    def __init__(self, text: str, tone: str = "info", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("banner")
        self.setProperty("tone", tone)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(10)
        self.icon = QLabel("ⓘ")
        self.icon.setObjectName("bannerIcon")
        self.label = QLabel(text)
        self.label.setWordWrap(True)
        self.label.setProperty("role", "muted")
        lay.addWidget(self.icon)
        lay.addWidget(self.label, 1)
