"""Design tokens and theme helpers for FXAutoTrade Lab desktop UI."""

from __future__ import annotations

from pathlib import Path
from typing import Final


class Tokens:
    # Surfaces
    BG: Final = "#f7f8fa"
    BG_CARD: Final = "#ffffff"
    BG_SUNKEN: Final = "#f1f3f7"
    BG_HEADER: Final = "#f7f8fa"
    INK_NAVY: Final = "#0f1f36"
    SURFACE_ALT: Final = "#fafbfc"

    # Text
    INK: Final = "#0f172a"
    INK_EMPHASIS: Final = "#334155"
    MUTED: Final = "#475569"
    MUTED_2: Final = "#64748b"
    INVERSE: Final = "#ffffff"
    INVERSE_2: Final = "#cbd6e4"
    NAV_MUTED: Final = "#8591a6"
    NAV_TEXT: Final = "#d9dee8"

    # Lines
    HAIRLINE: Final = "#e6e8ee"
    HAIRLINE_SOFT: Final = "#eff1f5"

    # Semantic
    POS: Final = "#0f766e"
    NEG: Final = "#b91c1c"
    WARN: Final = "#92400e"
    INFO: Final = "#1e40af"

    # Accent (teal)
    ACCENT: Final = "#14b8a6"
    ACCENT_HOVER: Final = "#0ea597"
    ACCENT_PRESS: Final = "#0c8f82"
    ACCENT_SOFT: Final = "rgba(20,184,166,0.12)"
    ACCENT_ROW: Final = "rgba(20,184,166,0.10)"
    ACCENT_NAV: Final = "rgba(20,184,166,0.14)"

    # Radius
    R_SM: Final = 4
    R_MD: Final = 6
    R_LG: Final = 10
    R_PILL: Final = 999

    # Spacing (8pt grid)
    S1: Final = 4
    S2: Final = 8
    S3: Final = 12
    S4: Final = 16
    S5: Final = 20
    S6: Final = 24
    S8: Final = 32

    # Fonts
    FONT_UI: Final = (
        'Inter, "Helvetica Neue", "Hiragino Sans", "Yu Gothic UI", sans-serif'
    )
    FONT_MONO: Final = '"JetBrains Mono", "SF Mono", Menlo, monospace'

    # Font sizes
    FS_XS: Final = 10
    FS_SM: Final = 11
    FS_BASE: Final = 12
    FS_MD: Final = 13
    FS_LG: Final = 14
    FS_XL: Final = 18
    FS_2XL: Final = 22
    FS_3XL: Final = 28


_ASSET_DIR = Path(__file__).parent / "assets"
_ICON_DIR = _ASSET_DIR / "icons"


def load_theme_qss() -> str:
    return (_ASSET_DIR / "theme.qss").read_text(encoding="utf-8")


def repolish(widget) -> None:
    style = widget.style()
    if style is None:
        return
    style.unpolish(widget)
    style.polish(widget)
    widget.update()


def load_icon(name: str, color: str = Tokens.INVERSE_2, size: int = 18):  # pragma: no cover - UI helper
    """Return a QIcon for the named SVG, tinted to ``color``.

    The SVG should use ``currentColor`` for its stroke/fill. Returns an empty
    QIcon when the file is missing so callers do not have to guard.
    """
    from PySide6.QtCore import QByteArray, Qt
    from PySide6.QtGui import QIcon, QPainter, QPixmap
    from PySide6.QtSvg import QSvgRenderer

    path = _ICON_DIR / f"{name}.svg"
    if not path.exists():
        return QIcon()
    svg_bytes = path.read_bytes().replace(b"currentColor", color.encode("ascii"))
    renderer = QSvgRenderer(QByteArray(svg_bytes))
    pm = QPixmap(size, size)
    pm.fill(Qt.transparent)
    painter = QPainter(pm)
    renderer.render(painter)
    painter.end()
    return QIcon(pm)
