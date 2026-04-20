"""Reusable desktop UI widgets."""

from __future__ import annotations

__all__ = [
    "AppStatusBar",
    "Banner",
    "Card",
    "Chip",
    "ChipField",
    "Detail",
    "GroupedNavList",
    "KpiTile",
    "LabeledSuffixInput",
    "LogDock",
    "SegmentedControl",
    "Sidebar",
    "StatusBar",
    "SymbolChip",
    "Topbar",
]


def __getattr__(name: str):  # pragma: no cover - thin import proxy
    if name == "Banner":
        from .banner import Banner
        return Banner
    if name == "Card":
        from .card import Card
        return Card
    if name == "Chip":
        from .chip import Chip
        return Chip
    if name in {"ChipField", "SymbolChip"}:
        from . import chip_field
        return getattr(chip_field, name)
    if name == "Detail":
        from .detail import Detail
        return Detail
    if name == "LabeledSuffixInput":
        from .suffix_input import LabeledSuffixInput
        return LabeledSuffixInput
    if name == "SegmentedControl":
        from .segmented import SegmentedControl
        return SegmentedControl
    if name == "KpiTile":
        from .kpi import KpiTile
        return KpiTile
    if name in {"Sidebar", "GroupedNavList"}:
        from .sidebar import Sidebar
        return Sidebar
    if name == "Topbar":
        from .topbar import Topbar
        return Topbar
    if name in {"StatusBar", "AppStatusBar"}:
        from .statusbar import StatusBar
        return StatusBar
    if name == "LogDock":
        from .logdock import LogDock
        return LogDock
    raise AttributeError(name)
