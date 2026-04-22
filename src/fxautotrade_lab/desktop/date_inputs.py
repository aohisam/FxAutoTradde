"""Shared desktop date input helpers."""

from __future__ import annotations


def default_popup_qdate(role: str = "end"):  # pragma: no cover - UI helper
    from PySide6.QtCore import QDate

    today = QDate.currentDate()
    if role == "start":
        return today.addMonths(-1)
    return today


def create_popup_date_edit(role: str = "end"):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QCalendarWidget, QDateEdit

    edit = QDateEdit()
    edit.setCalendarPopup(True)
    edit.setDisplayFormat("yyyy-MM-dd")
    edit.setMinimumWidth(164)
    edit.setDate(default_popup_qdate(role))
    edit.setToolTip("右端のカレンダー欄をクリックすると、カレンダーから日付を選べます。")

    calendar = QCalendarWidget()
    calendar.setGridVisible(False)
    calendar.setNavigationBarVisible(True)
    calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
    calendar.setHorizontalHeaderFormat(QCalendarWidget.ShortDayNames)
    calendar.setFirstDayOfWeek(Qt.Monday)
    calendar.setMinimumSize(320, 240)

    edit.setCalendarWidget(calendar)
    return edit
