"""StatusBar — flat 28px bar placed via grid layout (not QStatusBar)."""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QHBoxLayout, QLabel, QPushButton, QWidget

from ..theme import Tokens, repolish


class _StatusSeg(QWidget):
    def __init__(
        self,
        text: str,
        *,
        dot: bool = False,
        dot_color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("StatusSeg")
        row = QHBoxLayout(self)
        row.setContentsMargins(8, 0, 8, 0)
        row.setSpacing(6)
        self._dot = QLabel("")
        self._dot.setObjectName("StatusDot")
        self._dot.setFixedSize(7, 7)
        self._dot.setVisible(dot)
        self._label = QLabel(text)
        self._label.setObjectName("StatusLabel")
        row.addWidget(self._dot)
        row.addWidget(self._label)
        if dot:
            self.set_dot_color(dot_color or Tokens.POS)

    def set_text(self, text: str) -> None:
        self._label.setText(text)

    def set_dot_color(self, color: str) -> None:
        self._dot.setStyleSheet(f"background: {color}; border-radius: 3px;")

    def set_tone(self, tone: str | None) -> None:
        self.setProperty("tone", tone or "")
        repolish(self)
        repolish(self._label)


class StatusBar(QWidget):
    logToggleRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("StatusBar")
        self.setFixedHeight(28)
        row = QHBoxLayout(self)
        row.setContentsMargins(14, 0, 10, 0)
        row.setSpacing(0)

        self.seg_conn = _StatusSeg("GMO 待機", dot=True, dot_color=Tokens.MUTED_2)
        self.seg_cycle = _StatusSeg("cycle -")
        self.seg_lag = _StatusSeg("lag -")
        row.addWidget(self.seg_conn)
        row.addWidget(self.seg_cycle)
        row.addWidget(self.seg_lag)
        row.addStretch(1)

        self.seg_run = _StatusSeg("-")
        self.seg_equity = _StatusSeg("equity -")
        self.seg_daily = _StatusSeg("日次 -")
        row.addWidget(self.seg_run)
        row.addWidget(self.seg_equity)
        row.addWidget(self.seg_daily)

        self._log_btn = QPushButton("ログ")
        self._log_btn.setObjectName("StatusLogBtn")
        self._log_btn.setFixedHeight(22)
        self._log_btn.setCursor(Qt.PointingHandCursor)
        self._log_btn.clicked.connect(self.logToggleRequested)
        row.addSpacing(8)
        row.addWidget(self._log_btn)

        self._message_label = QLabel("")
        self._message_label.setObjectName("StatusMessage")
        self._message_label.setVisible(False)

    # ---- Setters ---------------------------------------------------------
    def set_connection(self, ok: bool) -> None:
        color = Tokens.POS if ok else Tokens.NEG
        self.seg_conn.set_dot_color(color)
        self.seg_conn.set_text("GMO connected" if ok else "GMO disconnected")

    def set_cycle(self, n: int | None) -> None:
        if n is None:
            self.seg_cycle.set_text("cycle -")
        else:
            self.seg_cycle.set_text(f"cycle {int(n):,}")

    def set_lag(self, ms: int | None) -> None:
        if ms is None:
            self.seg_lag.set_text("lag -")
        else:
            self.seg_lag.set_text(f"lag {int(ms)} ms")

    def set_run_id(self, rid: str | None) -> None:
        self.seg_run.set_text(rid or "-")

    def set_equity(self, value: float | None) -> None:
        if value is None:
            self.seg_equity.set_text("equity -")
        else:
            self.seg_equity.set_text(f"equity {int(value):,} JPY")

    def set_daily_pnl_pct(self, pct: float | None) -> None:
        if pct is None:
            self.seg_daily.set_text("日次 -")
            self.seg_daily.set_tone(None)
            return
        sign = "+" if pct >= 0 else ""
        self.seg_daily.set_text(f"日次 {sign}{pct:.2f}%")
        self.seg_daily.set_tone("pos" if pct >= 0 else "neg")

    # ---- Log toggle ------------------------------------------------------
    def set_log_active(self, active: bool) -> None:
        self._log_btn.setProperty("active", "true" if active else "false")
        repolish(self._log_btn)

    def log_button(self) -> QPushButton:
        return self._log_btn

    # ---- Compatibility helpers -----------------------------------------
    def showMessage(self, text: str, _timeout: int = 0) -> None:  # noqa: N802
        """Best-effort compatibility with QStatusBar.showMessage."""
        self._message_label.setText(text or "")

    def show_page(self, _name: str) -> None:
        """No-op retained for backwards compatibility with previous shell."""
        return None
