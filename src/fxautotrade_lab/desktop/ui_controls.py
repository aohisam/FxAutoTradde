"""Shared button helpers for desktop pages."""

from __future__ import annotations


def _refresh_widget_style(widget) -> None:  # pragma: no cover - UI helper
    style = widget.style()
    if style is not None:
        style.unpolish(widget)
        style.polish(widget)
    widget.update()


def set_button_role(button, role: str) -> None:  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt

    button.setProperty("role", role)
    if button.isEnabled():
        button.setCursor(Qt.PointingHandCursor)
    _refresh_widget_style(button)


def set_button_enabled(
    button, enabled: bool, *, busy: bool = False
) -> None:  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt

    button.setEnabled(enabled)
    button.setProperty("busyDisabled", bool(busy and not enabled))
    if enabled:
        button.setCursor(Qt.PointingHandCursor)
        if button.toolTip() == "処理中のため操作できません。":
            button.setToolTip("")
    elif busy:
        button.setCursor(Qt.ForbiddenCursor)
        button.setToolTip("処理中のため操作できません。")
    else:
        button.setCursor(Qt.ArrowCursor)
        if button.toolTip() == "処理中のため操作できません。":
            button.setToolTip("")
    _refresh_widget_style(button)
