"""KpiTile — label / value / trend / note tile built on Card."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHBoxLayout, QLabel, QWidget

from ..theme import repolish
from .card import Card


class KpiTile(Card):
    def __init__(
        self,
        label: str,
        value: str = "-",
        note: str = "",
        tone: str | None = None,
        *,
        value_variant: str = "mono",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent=parent)
        self.setMinimumHeight(108)

        eyebrow_row = QHBoxLayout()
        eyebrow_row.setContentsMargins(0, 0, 0, 0)
        eyebrow_row.setSpacing(6)
        self.eyebrow = QLabel(label.upper())
        self.eyebrow.setProperty("role", "eyebrow")
        eyebrow_row.addWidget(self.eyebrow, 1)

        self.value = QLabel(value)
        self.value.setProperty("role", "kpi-value")
        self.value.setProperty("variant", value_variant)
        self.value.setWordWrap(False)
        self.value.setTextInteractionFlags(Qt.TextSelectableByMouse)
        if tone:
            self.value.setProperty("tone", tone)

        self.trend_label = QLabel("")
        self.trend_label.setProperty("role", "kpi-trend")
        self.trend_label.hide()

        self.note_row = QHBoxLayout()
        self.note_row.setContentsMargins(0, 0, 0, 0)
        self.note_row.setSpacing(6)
        self._note_chip: QWidget | None = None
        self.note = QLabel(note)
        self.note.setProperty("role", "kpi-note")
        self.note.setWordWrap(True)
        self.note_row.addWidget(self.note, 1)

        self.body_layout.setContentsMargins(16, 14, 16, 14)
        self.body_layout.setSpacing(4)
        self.body_layout.addLayout(eyebrow_row)
        self.body_layout.addWidget(self.value)
        self.body_layout.addWidget(self.trend_label)
        self.body_layout.addLayout(self.note_row)

    def set_value(self, value: str, tone: str | None = None) -> None:
        self.value.setTextFormat(Qt.AutoText)
        self.value.setText(value)
        self.value.setProperty("tone", tone or "")
        repolish(self.value)

    def set_value_html(self, html: str) -> None:
        self.value.setTextFormat(Qt.RichText)
        self.value.setText(html)
        self.value.setProperty("tone", "")
        repolish(self.value)

    def set_note(self, note: str) -> None:
        self.note.setText(note)

    def set_trend(self, direction: str | None, text: str = "") -> None:
        """direction: 'up' | 'down' | 'flat' | None"""
        if direction is None:
            self.trend_label.hide()
            return
        glyph = {"up": "▲", "down": "▼", "flat": "–"}.get(direction, "–")
        display = f"{glyph} {text}".strip() if text else glyph
        self.trend_label.setText(display)
        tone = {"up": "pos", "down": "neg"}.get(direction, "")
        self.trend_label.setProperty("tone", tone)
        repolish(self.trend_label)
        self.trend_label.show()

    def set_note_chip(self, chip_widget: QWidget | None) -> None:
        """先頭に chip を差す。既存 chip があれば外す。"""
        if self._note_chip is not None:
            self.note_row.removeWidget(self._note_chip)
            self._note_chip.deleteLater()
            self._note_chip = None
        if chip_widget is None:
            return
        self.note_row.insertWidget(0, chip_widget)
        self._note_chip = chip_widget
