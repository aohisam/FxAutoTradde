"""ChipField — flow-wrapped symbol chips with built-in ✕, plus an input row."""

from __future__ import annotations

from collections.abc import Callable
from typing import Iterable

from PySide6.QtCore import QPoint, QRect, QSize, Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QLayout,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)


class _FlowLayout(QLayout):
    """Packs child items on rows, wrapping onto new rows as needed."""

    def __init__(
        self,
        parent: QWidget | None = None,
        h_spacing: int = 6,
        v_spacing: int = 6,
    ) -> None:
        super().__init__(parent)
        self.setContentsMargins(0, 0, 0, 0)
        self._h_spacing = h_spacing
        self._v_spacing = v_spacing
        self._items: list = []

    def addItem(self, item) -> None:  # noqa: N802
        self._items.append(item)

    def count(self) -> int:
        return len(self._items)

    def itemAt(self, index):  # noqa: N802
        return self._items[index] if 0 <= index < len(self._items) else None

    def takeAt(self, index):  # noqa: N802
        if 0 <= index < len(self._items):
            return self._items.pop(index)
        return None

    def expandingDirections(self) -> Qt.Orientations:  # noqa: N802
        return Qt.Orientations(Qt.Orientation(0))

    def hasHeightForWidth(self) -> bool:  # noqa: N802
        return True

    def heightForWidth(self, width: int) -> int:  # noqa: N802
        return self._do_layout(QRect(0, 0, width, 0), test_only=True)

    def setGeometry(self, rect: QRect) -> None:  # noqa: N802
        super().setGeometry(rect)
        self._do_layout(rect, test_only=False)

    def sizeHint(self) -> QSize:  # noqa: N802
        return self.minimumSize()

    def minimumSize(self) -> QSize:  # noqa: N802
        size = QSize()
        for item in self._items:
            size = size.expandedTo(item.minimumSize())
        margins = self.contentsMargins()
        size += QSize(
            margins.left() + margins.right(),
            margins.top() + margins.bottom(),
        )
        return size

    def _do_layout(self, rect: QRect, *, test_only: bool) -> int:
        x = rect.x()
        y = rect.y()
        line_height = 0
        for item in self._items:
            hint = item.sizeHint()
            next_x = x + hint.width() + self._h_spacing
            if next_x - self._h_spacing > rect.right() and line_height > 0:
                x = rect.x()
                y = y + line_height + self._v_spacing
                next_x = x + hint.width() + self._h_spacing
                line_height = 0
            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), hint))
            x = next_x
            line_height = max(line_height, hint.height())
        return y + line_height - rect.y()


class _FlowContainer(QWidget):
    """QWidget exposing add/remove/clear over the internal _FlowLayout."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._flow = _FlowLayout(self)
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Minimum)
        self._items: list[QWidget] = []

    def add(self, widget: QWidget) -> None:
        self._items.append(widget)
        self._flow.addWidget(widget)

    def remove(self, widget: QWidget) -> None:
        if widget not in self._items:
            return
        self._items.remove(widget)
        self._flow.removeWidget(widget)
        widget.setParent(None)
        widget.deleteLater()

    def clear(self) -> None:
        for widget in list(self._items):
            self._flow.removeWidget(widget)
            widget.setParent(None)
            widget.deleteLater()
        self._items.clear()

    def items(self) -> list[QWidget]:
        return list(self._items)


class SymbolChip(QFrame):
    removed = Signal(str)

    def __init__(self, text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("chip_symbol")
        self._text = text
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 2, 6, 2)
        lay.setSpacing(4)
        self.label = QLabel(text)
        self.label.setProperty("role", "mono")
        self.button = QPushButton("✕")
        self.button.setObjectName("chipRemove")
        self.button.setCursor(Qt.PointingHandCursor)
        self.button.setFixedSize(16, 16)
        self.button.setFlat(True)
        self.button.clicked.connect(lambda: self.removed.emit(self._text))
        lay.addWidget(self.label)
        lay.addWidget(self.button)

    def text(self) -> str:
        return self._text


class ChipField(QWidget):
    changed = Signal()

    def __init__(
        self,
        *,
        placeholder: str = "例: USD_JPY",
        add_button_variant: str = "primary",
        normalize: Callable[[str], str] | None = None,
        on_error: Callable[[str], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._normalize = normalize
        self._on_error = on_error or (lambda _msg: None)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(12)

        self.flow = _FlowContainer()
        self.flow.setMinimumHeight(44)
        outer.addWidget(self.flow)

        row = QHBoxLayout()
        row.setSpacing(6)
        self.edit = QLineEdit()
        self.edit.setPlaceholderText(placeholder)
        self.btn = QPushButton("追加")
        self.btn.setProperty("variant", add_button_variant)
        row.addWidget(self.edit, 1)
        row.addWidget(self.btn)
        outer.addLayout(row)

        self.btn.clicked.connect(self._on_add)
        self.edit.returnPressed.connect(self._on_add)

    def values(self) -> list[str]:
        return [w.text() for w in self.flow.items() if isinstance(w, SymbolChip)]

    def set_values(self, values: Iterable[str]) -> None:
        self.flow.clear()
        for value in values:
            self._append_chip(value)
        self.changed.emit()

    def _append_chip(self, value: str) -> None:
        chip = SymbolChip(value)
        chip.removed.connect(self._on_remove)
        self.flow.add(chip)

    def _on_add(self) -> None:
        raw = self.edit.text().strip()
        if not raw:
            return
        try:
            value = self._normalize(raw) if self._normalize else raw
        except Exception:  # noqa: BLE001
            self._on_error(f"無効な通貨ペアです: {raw}")
            return
        if value in self.values():
            self._on_error(f"{value} はすでに登録されています。")
            return
        self._append_chip(value)
        self.edit.clear()
        self.changed.emit()

    def _on_remove(self, value: str) -> None:
        for widget in self.flow.items():
            if isinstance(widget, SymbolChip) and widget.text() == value:
                self.flow.remove(widget)
                self.changed.emit()
                return
