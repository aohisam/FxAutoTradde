"""Sidebar — grouped navigation with brand + nav + footer."""

from __future__ import annotations

import platform

from PySide6.QtCore import QSize, Qt, Signal
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from ..theme import Tokens, load_icon, repolish


APP_VERSION = "0.9.3"


# (section_caption, [(page_key, label, icon_name), ...])
NAV_SPEC: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("ダッシュボード", [
        ("overview", "概要", "nav_overview"),
        ("watchlist", "監視通貨ペア", "nav_watchlist"),
    ]),
    ("リサーチ", [
        ("data_sync", "データ同期", "nav_data_sync"),
        ("backtest", "バックテスト", "nav_backtest"),
        ("signals", "シグナル分析", "nav_signals"),
    ]),
    ("実行", [
        ("automation", "実時間シミュレーション", "nav_automation"),
        ("chart", "チャート", "nav_chart"),
        ("history", "取引履歴", "nav_history"),
        ("reports", "レポート", "nav_reports"),
    ]),
    ("システム", [
        ("settings", "設定", "nav_settings"),
        ("help", "ヘルプ", "nav_help"),
    ]),
]


class _NavItem(QWidget):
    clicked = Signal()

    def __init__(
        self,
        page_key: str,
        label: str,
        icon_name: str,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("NavItem")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self._page_key = page_key
        self._icon_name = icon_name

        row = QHBoxLayout(self)
        row.setContentsMargins(10, 8, 10, 8)
        row.setSpacing(10)
        self._icon = QLabel()
        self._icon.setObjectName("NavIcon")
        self._icon.setFixedSize(16, 16)
        self._icon.setAttribute(Qt.WA_TransparentForMouseEvents)
        self._apply_icon(active=False)

        self._text = QLabel(label)
        self._text.setObjectName("NavText")
        self._text.setAttribute(Qt.WA_TransparentForMouseEvents)

        self._badge = QLabel("")
        self._badge.setObjectName("NavBadge")
        self._badge.setVisible(False)
        self._badge.setAttribute(Qt.WA_TransparentForMouseEvents)

        self._dot = QLabel()
        self._dot.setObjectName("NavDot")
        self._dot.setFixedSize(7, 7)
        self._dot.setVisible(False)
        self._dot.setAttribute(Qt.WA_TransparentForMouseEvents)

        row.addWidget(self._icon)
        row.addWidget(self._text, 1)
        row.addWidget(self._badge)
        row.addWidget(self._dot)
        self.setCursor(Qt.PointingHandCursor)
        self.setAutoFillBackground(False)

    @property
    def page_key(self) -> str:
        return self._page_key

    def _apply_icon(self, *, active: bool) -> None:
        color = Tokens.INVERSE if active else Tokens.NAV_MUTED
        icon = load_icon(self._icon_name, color, 16)
        self._icon.setPixmap(icon.pixmap(QSize(16, 16)))

    def set_active(self, active: bool) -> None:
        self.setProperty("active", "true" if active else "false")
        repolish(self)
        self._text.setProperty("active", "true" if active else "false")
        repolish(self._text)
        self._apply_icon(active=active)

    def set_badge(self, text: str | None) -> None:
        if text:
            self._badge.setText(str(text))
            self._badge.setVisible(True)
        else:
            self._badge.clear()
            self._badge.setVisible(False)

    def set_dot(self, visible: bool) -> None:
        self._dot.setVisible(bool(visible))

    def mousePressEvent(self, event) -> None:  # noqa: N802
        if event.button() == Qt.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class Sidebar(QWidget):
    pageRequested = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("Sidebar")
        # QWidget subclasses ignore QSS `background` unless WA_StyledBackground is set.
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setAutoFillBackground(False)
        self.setFixedWidth(232)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # ---- Brand ----
        brand = QWidget()
        brand.setObjectName("Brand")
        brand.setAttribute(Qt.WA_StyledBackground, True)
        brand_row = QHBoxLayout(brand)
        brand_row.setContentsMargins(18, 14, 18, 16)
        brand_row.setSpacing(10)
        mark = QLabel("FX")
        mark.setObjectName("BrandMark")
        mark.setFixedSize(26, 26)
        mark.setAlignment(Qt.AlignCenter)
        title_box = QVBoxLayout()
        title_box.setContentsMargins(0, 0, 0, 0)
        title_box.setSpacing(1)
        t1 = QLabel("FXAutoTrade Lab")
        t1.setObjectName("BrandT1")
        t2 = QLabel("RESEARCH · AUTOMATION")
        t2.setObjectName("BrandT2")
        title_box.addWidget(t1)
        title_box.addWidget(t2)
        brand_row.addWidget(mark)
        brand_row.addLayout(title_box)
        brand_row.addStretch(1)
        outer.addWidget(brand)

        # ---- Nav (scrollable) ----
        self._nav_scroll = QScrollArea()
        self._nav_scroll.setWidgetResizable(True)
        self._nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._nav_scroll.setFrameShape(QFrame.NoFrame)
        self._nav_scroll.setAttribute(Qt.WA_StyledBackground, True)
        # QScrollArea's viewport paints with palette(Base) (white) by default —
        # disable auto-fill so the navy sidebar background shows through.
        self._nav_scroll.viewport().setAutoFillBackground(False)
        self._nav_scroll.setStyleSheet("")  # rely on theme.qss
        nav_body = QWidget()
        nav_body.setObjectName("NavBody")
        nav_body.setAttribute(Qt.WA_StyledBackground, True)
        nav_body.setAutoFillBackground(False)
        nav_layout = QVBoxLayout(nav_body)
        nav_layout.setContentsMargins(10, 10, 10, 12)
        nav_layout.setSpacing(0)

        self._items: dict[str, _NavItem] = {}
        for section, rows in NAV_SPEC:
            section_label = QLabel(section)
            section_label.setObjectName("NavSection")
            nav_layout.addWidget(section_label)
            for page_key, label, icon_name in rows:
                item = _NavItem(page_key, label, icon_name)
                item.clicked.connect(lambda key=page_key: self.pageRequested.emit(key))
                nav_layout.addWidget(item)
                self._items[page_key] = item
        nav_layout.addStretch(1)
        self._nav_scroll.setWidget(nav_body)
        outer.addWidget(self._nav_scroll, 1)

        # ---- Footer ----
        footer = QWidget()
        footer.setObjectName("SidebarFooter")
        footer.setAttribute(Qt.WA_StyledBackground, True)
        footer_row = QHBoxLayout(footer)
        footer_row.setContentsMargins(14, 10, 14, 12)
        footer_row.setSpacing(8)
        self._version_label = QLabel(f"v{APP_VERSION} · {platform.system()}")
        self._version_label.setObjectName("FooterVer")
        self._conn_pill = QLabel("GMO 接続")
        self._conn_pill.setObjectName("FooterPill")
        self._conn_pill.setProperty("state", "ok")
        footer_row.addWidget(self._version_label)
        footer_row.addStretch(1)
        footer_row.addWidget(self._conn_pill)
        outer.addWidget(footer)

    # ---- Public API ------------------------------------------------------
    def set_active(self, page_key: str) -> None:
        for key, item in self._items.items():
            item.set_active(key == page_key)

    def set_badge(self, page_key: str, text: str | None) -> None:
        item = self._items.get(page_key)
        if item is not None:
            item.set_badge(text)

    def set_dot(self, page_key: str, visible: bool) -> None:
        item = self._items.get(page_key)
        if item is not None:
            item.set_dot(visible)

    def set_gmo_connected(self, ok: bool) -> None:
        self._conn_pill.setText("GMO 接続" if ok else "GMO 切断")
        self._conn_pill.setProperty("state", "ok" if ok else "err")
        repolish(self._conn_pill)


# ---------------------------------------------------------------------------
# Backwards-compatible shim (legacy `GroupedNavList` API)
# ---------------------------------------------------------------------------
def _legacy_shim():  # pragma: no cover - import compatibility
    return Sidebar


GroupedNavList = Sidebar  # kept for legacy imports
