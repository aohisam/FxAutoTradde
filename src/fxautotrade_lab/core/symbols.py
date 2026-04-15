"""Helpers for normalizing FX symbols."""

from __future__ import annotations

import re
from pathlib import Path


_PAIR_RE = re.compile(r"^[A-Z]{3}[_/]?[A-Z]{3}$")
_FILENAME_PAIR_RE = re.compile(r"([A-Z]{6})(?:_|$)")


def normalize_fx_symbol(value: str) -> str:
    raw = value.strip().upper().replace("-", "_").replace("/", "_")
    if not raw:
        return ""
    if _PAIR_RE.fullmatch(raw.replace("_", "")):
        compact = raw.replace("_", "")
        return f"{compact[:3]}_{compact[3:]}"
    raise ValueError(f"Unsupported FX symbol format: {value}")


def display_fx_symbol(value: str) -> str:
    normalized = normalize_fx_symbol(value)
    return normalized.replace("_", "/")


def split_fx_symbol(value: str) -> tuple[str, str]:
    normalized = normalize_fx_symbol(value)
    base, quote = normalized.split("_", maxsplit=1)
    return base, quote


def infer_fx_symbol_from_filename(path: str | Path) -> str:
    name = Path(path).name.upper()
    match = _FILENAME_PAIR_RE.search(name)
    if match is None:
        raise ValueError(f"Could not infer FX symbol from filename: {path}")
    return normalize_fx_symbol(match.group(1))
