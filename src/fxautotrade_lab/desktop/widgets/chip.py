"""Chip — pill-shaped status label."""

from __future__ import annotations

from PySide6.QtWidgets import QLabel, QWidget

_TONES = {"neutral", "running", "paper", "warn", "neg", "info"}


class Chip(QLabel):
    def __init__(self, text: str, tone: str = "neutral", parent: QWidget | None = None) -> None:
        if tone not in _TONES:
            tone = "neutral"
        super().__init__(f"● {text}", parent)
        self.setObjectName(f"chip_{tone}")
        self._tone = tone

    def set_tone(self, tone: str) -> None:
        if tone not in _TONES:
            tone = "neutral"
        if tone == self._tone:
            return
        self._tone = tone
        self.setObjectName(f"chip_{tone}")
        style = self.style()
        if style is not None:
            style.unpolish(self)
            style.polish(self)
        self.update()

    def set_text(self, text: str) -> None:
        self.setText(f"● {text}")
