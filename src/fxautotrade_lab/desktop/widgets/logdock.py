"""LogDock — bottom-pinned log dock with custom titlebar."""

from __future__ import annotations

from datetime import datetime

from PySide6.QtCore import QSettings, Qt
from PySide6.QtWidgets import (
    QDockWidget,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QToolButton,
    QWidget,
)

from ..theme import Tokens


_LEVEL_COLOR = {
    "OK": Tokens.POS,
    "INFO": Tokens.INFO,
    "WARN": Tokens.WARN,
    "ERROR": Tokens.NEG,
}


class LogDock(QDockWidget):
    SETTINGS_KEY = "log_dock/visible"

    def __init__(self, parent=None) -> None:
        super().__init__("ログ", parent)
        self.setObjectName("LogDock")
        self.setFeatures(QDockWidget.DockWidgetClosable)
        self.setAllowedAreas(Qt.BottomDockWidgetArea)

        # ---- Custom title bar ----
        title_bar = QWidget()
        title_bar.setObjectName("LogTitleBar")
        title_row = QHBoxLayout(title_bar)
        title_row.setContentsMargins(14, 4, 6, 4)
        title_row.setSpacing(8)
        title_label = QLabel("ログ")
        title_label.setObjectName("LogTitle")
        close_btn = QToolButton()
        close_btn.setObjectName("LogClose")
        close_btn.setText("×")
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.clicked.connect(self.hide)
        title_row.addWidget(title_label)
        title_row.addStretch(1)
        title_row.addWidget(close_btn)
        self.setTitleBarWidget(title_bar)

        # ---- Body ----
        self.view = QPlainTextEdit()
        self.view.setObjectName("LogView")
        self.view.setReadOnly(True)
        self.view.setMaximumBlockCount(5000)
        self.setWidget(self.view)
        self.setMinimumHeight(160)
        self.setMaximumHeight(400)

        self.visibilityChanged.connect(self._on_visibility_changed)

    # ---- Persistence -----------------------------------------------------
    def _on_visibility_changed(self, visible: bool) -> None:
        QSettings("FXAutoTradeLab", "Desktop").setValue(self.SETTINGS_KEY, bool(visible))

    @classmethod
    def is_visible_preference(cls) -> bool:
        value = QSettings("FXAutoTradeLab", "Desktop").value(cls.SETTINGS_KEY, False)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in {"true", "1", "yes"}
        return bool(value)

    # ---- Logging API -----------------------------------------------------
    def append(self, level: str, text: str) -> None:
        level_key = (level or "INFO").upper()
        color = _LEVEL_COLOR.get(level_key, Tokens.MUTED)
        timestamp = datetime.now().strftime("%H:%M:%S")
        safe_text = (
            (text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        html = (
            f'<span style="color:{Tokens.MUTED_2}">{timestamp}</span> '
            f'<span style="color:{color};font-weight:600">[{level_key}]</span> '
            f'<span style="color:{Tokens.INVERSE_2}">{safe_text}</span>'
        )
        self.view.appendHtml(html)

    def clear(self) -> None:
        self.view.clear()
