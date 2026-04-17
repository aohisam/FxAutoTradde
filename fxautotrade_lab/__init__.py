"""Repo-root shim for the ``src`` package layout."""

from __future__ import annotations

from pathlib import Path
import pkgutil

__all__ = ["__version__"]
__version__ = "0.1.0"

__path__ = pkgutil.extend_path(__path__, __name__)
SRC_PACKAGE = Path(__file__).resolve().parent.parent / "src" / __name__
if SRC_PACKAGE.exists():
    src_text = str(SRC_PACKAGE)
    if src_text not in __path__:
        __path__.append(src_text)
