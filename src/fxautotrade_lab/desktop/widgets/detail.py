"""Detail — eyebrow label + value cell used across help / reports / settings."""

from __future__ import annotations

from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget

from ..theme import repolish


class Detail(QWidget):
    def __init__(
        self,
        label: str,
        value: str = "-",
        *,
        variant: str | None = None,
        variant_size: str | None = None,
        tone: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(2)
        self._label = QLabel(label)
        self._label.setProperty("role", "detail-label")
        self._label.setWordWrap(True)
        self._value = QLabel(value)
        self._value.setProperty("role", "detail-value")
        self._value.setWordWrap(True)
        if variant:
            self._value.setProperty("variant", variant)
        if variant_size:
            self._value.setProperty("size", variant_size)
        if tone:
            self._value.setProperty("tone", tone)
        lay.addWidget(self._label)
        lay.addWidget(self._value)

    @property
    def value_label(self) -> QLabel:
        return self._value

    def set_label(self, text: str) -> None:
        self._label.setText(text)

    def set_value(self, text: str, *, tone: str | None = None) -> None:
        self._value.setText(text)
        self._value.setProperty("tone", tone or "")
        repolish(self._value)
