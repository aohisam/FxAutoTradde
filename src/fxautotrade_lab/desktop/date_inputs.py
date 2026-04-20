"""Shared desktop date input helpers."""

from __future__ import annotations


def create_popup_date_edit():  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QCalendarWidget, QDateEdit

    edit = QDateEdit()
    edit.setCalendarPopup(True)
    edit.setDisplayFormat("yyyy-MM-dd")
    edit.setMinimumWidth(148)

    calendar = QCalendarWidget()
    calendar.setGridVisible(False)
    calendar.setNavigationBarVisible(True)
    calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
    calendar.setHorizontalHeaderFormat(QCalendarWidget.ShortDayNames)
    calendar.setFirstDayOfWeek(Qt.Monday)
    calendar.setMinimumSize(320, 240)

    edit.setCalendarWidget(calendar)
    return edit
