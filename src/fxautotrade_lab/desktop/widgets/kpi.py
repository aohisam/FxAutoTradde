"""KpiTile — label / value / note tile built on Card."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QWidget

from .card import Card


class KpiTile(Card):
    def __init__(
        self,
        label: str,
        value: str = "-",
        note: str = "",
        tone: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent=parent)
        self.setMinimumHeight(108)
        eyebrow = QLabel(label.upper())
        eyebrow.setProperty("role", "eyebrow")
        self.value = QLabel(value)
        self.value.setProperty("role", "kpi-value")
        self.value.setWordWrap(False)
        self.value.setTextInteractionFlags(Qt.TextSelectableByMouse)
        if tone:
            self.value.setProperty("tone", tone)
        self.note = QLabel(note)
        self.note.setProperty("role", "kpi-note")
        self.note.setWordWrap(True)
        self.body_layout.setContentsMargins(16, 14, 16, 14)
        self.body_layout.setSpacing(6)
        self.body_layout.addWidget(eyebrow)
        self.body_layout.addWidget(self.value)
        self.body_layout.addWidget(self.note)

    def set_value(self, value: str, tone: str | None = None) -> None:
        self.value.setText(value)
        self.value.setProperty("tone", tone or "")
        style = self.value.style()
        if style is not None:
            style.unpolish(self.value)
            style.polish(self.value)

    def set_note(self, note: str) -> None:
        self.note.setText(note)
