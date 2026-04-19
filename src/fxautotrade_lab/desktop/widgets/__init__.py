"""Reusable desktop UI widgets.

Lazily re-exports the factory-style components so that importing the package
does not require PySide6 to be available at import time (module tests run
headless).
"""

from __future__ import annotations

__all__ = [
    "Banner",
    "Card",
    "Chip",
    "ChipField",
    "LabeledSuffixInput",
    "SegmentedControl",
    "SymbolChip",
    "KpiTile",
    "GroupedNavList",
    "AppStatusBar",
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
    if name == "LabeledSuffixInput":
        from .suffix_input import LabeledSuffixInput
        return LabeledSuffixInput
    if name == "SegmentedControl":
        from .segmented import SegmentedControl
        return SegmentedControl
    if name == "KpiTile":
        from .kpi import KpiTile
        return KpiTile
    if name == "GroupedNavList":
        from .sidebar import GroupedNavList
        return GroupedNavList
    if name == "AppStatusBar":
        from .statusbar import AppStatusBar
        return AppStatusBar
    raise AttributeError(name)
