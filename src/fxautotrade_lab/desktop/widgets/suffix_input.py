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

    def value_float(self) -> float:
        try:
            return float(self.edit.text().replace(",", "").strip())
        except ValueError:
            return 0.0

    def value_int(self) -> int:
        try:
            return int(float(self.edit.text().replace(",", "").strip()))
        except ValueError:
            return 0

    def set_float(self, value: float, fmt: str = "{:.2f}") -> None:
        self.edit.setText(fmt.format(float(value)))

    def set_int(self, value: int) -> None:
        self.edit.setText(f"{int(value):,}")
