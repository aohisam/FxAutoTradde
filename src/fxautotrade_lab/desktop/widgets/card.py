"""Card widget — white surface with 1px hairline and rounded corners."""

from __future__ import annotations

from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QVBoxLayout, QWidget


class Card(QFrame):
    def __init__(
        self,
        title: str | None = None,
        subtitle: str | None = None,
        header_right: QWidget | None = None,
        *,
        sunken: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setProperty("card", "sunken" if sunken else True)
        self.setObjectName("card_sunken" if sunken else "card")
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        if title is not None:
            head = QWidget()
            head.setObjectName("cardHead")
            hl = QHBoxLayout(head)
            hl.setContentsMargins(16, 12, 16, 12)
            hl.setSpacing(10)
            title_label = QLabel(title)
            title_label.setProperty("role", "h2")
            hl.addWidget(title_label)
            if subtitle:
                subtitle_label = QLabel(subtitle)
                subtitle_label.setProperty("role", "muted2")
                hl.addWidget(subtitle_label)
            hl.addStretch(1)
            if header_right is not None:
                hl.addWidget(header_right)
            outer.addWidget(head)
            self.title_label = title_label
        else:
            self.title_label = None

        self.body = QWidget()
        self.body.setObjectName("cardBody")
        self.body_layout = QVBoxLayout(self.body)
        self.body_layout.setContentsMargins(16, 14, 16, 16)
        self.body_layout.setSpacing(10)
        outer.addWidget(self.body, 1)

    def addBodyWidget(self, widget: QWidget, stretch: int = 0) -> None:
        self.body_layout.addWidget(widget, stretch)

    def addBodyLayout(self, layout) -> None:
        self.body_layout.addLayout(layout)

    def setBodyVisible(self, visible: bool) -> None:  # noqa: N802
        self.body.setVisible(visible)
        self.body.setMaximumHeight(16777215 if visible else 0)

    def set_title(self, text: str) -> None:
        if self.title_label is not None:
            self.title_label.setText(text)
