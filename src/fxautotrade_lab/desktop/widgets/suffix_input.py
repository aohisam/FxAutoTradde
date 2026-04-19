"""LabeledSuffixInput — numeric-style QLineEdit with a trailing suffix label."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QLineEdit, QWidget


class LabeledSuffixInput(QFrame):
    def __init__(
        self,
        value: str = "",
        suffix: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("suffixInput")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 0, 10, 0)
        lay.setSpacing(6)
        self.edit = QLineEdit(value)
        self.edit.setFrame(False)
        self.edit.setProperty("align", "num")
        self.edit.setAlignment(Qt.AlignRight)
        self.suffix = QLabel(suffix)
        self.suffix.setProperty("role", "muted2")
        lay.addWidget(self.edit, 1)
        lay.addWidget(self.suffix)

    def text(self) -> str:
        return self.edit.text().strip()

    def setText(self, value: str) -> None:  # noqa: N802
        self.edit.setText(value)
