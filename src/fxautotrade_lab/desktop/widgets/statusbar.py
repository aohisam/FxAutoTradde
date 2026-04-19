"""Status bar with left message cell and right permanent cells."""

from __future__ import annotations

from PySide6.QtWidgets import QHBoxLayout, QLabel, QStatusBar, QWidget


class AppStatusBar(QStatusBar):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setSizeGripEnabled(False)
        self.message = QLabel("FXAutoTrade Lab")
        self.addWidget(self.message)

        right = QWidget()
        layout = QHBoxLayout(right)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(20)
        self.cycle = QLabel("-")
        self.cycle.setProperty("role", "mono")
        self.lag = QLabel("-")
        self.lag.setProperty("role", "mono")
        self.equity = QLabel("-")
        self.equity.setProperty("role", "mono")
        self.daily = QLabel("-")
        for widget in (self.cycle, self.lag, self.equity, self.daily):
            layout.addWidget(widget)
        self.addPermanentWidget(right)

        self.setContentsMargins(12, 0, 12, 0)
        layout.setContentsMargins(0, 0, 0, 0)
        for widget in (self.cycle, self.lag, self.equity, self.daily):
            widget.setContentsMargins(0, 0, 0, 0)

    def show_page(self, name: str) -> None:
        self.message.setText(name)

    def set_runtime(self, cycle: str = "-", lag: str = "-", equity: str = "-", daily: str = "-", daily_tone: str | None = None) -> None:
        self.cycle.setText(f"cycle {cycle}")
        self.lag.setText(f"lag {lag}")
        self.equity.setText(f"equity {equity}")
        self.daily.setText(daily)
        self.daily.setProperty("tone", daily_tone or "")
        style = self.daily.style()
        if style is not None:
            style.unpolish(self.daily)
            style.polish(self.daily)
