"""SegmentedControl — pill group of checkable buttons."""

from __future__ import annotations

from typing import Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QButtonGroup, QFrame, QHBoxLayout, QPushButton, QWidget


class SegmentedControl(QFrame):
    def __init__(
        self,
        options: Sequence[str],
        current: int = 0,
        *,
        data: Sequence[object] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setProperty("role", "segmented")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(2)
        self.group = QButtonGroup(self)
        self.group.setExclusive(True)
        self.buttons: list[QPushButton] = []
        self._data: list[object] = list(data) if data is not None else list(options)
        for index, label in enumerate(options):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setProperty("variant", "seg")
            btn.setCursor(Qt.PointingHandCursor)
            if index == current:
                btn.setChecked(True)
            self.group.addButton(btn, index)
            layout.addWidget(btn)
            self.buttons.append(btn)

    # signals ---------------------------------------------------------------
    @property
    def idClicked(self):
        return self.group.idClicked

    @property
    def idToggled(self):
        return self.group.idToggled

    # state access ----------------------------------------------------------
    def currentIndex(self) -> int:
        return max(0, self.group.checkedId())

    def currentData(self) -> object:
        index = self.currentIndex()
        if 0 <= index < len(self._data):
            return self._data[index]
        return None

    def setCurrentIndex(self, index: int) -> None:
        if 0 <= index < len(self.buttons):
            self.buttons[index].setChecked(True)

    def setCurrentData(self, value: object) -> None:
        for index, payload in enumerate(self._data):
            if payload == value:
                self.setCurrentIndex(index)
                return
