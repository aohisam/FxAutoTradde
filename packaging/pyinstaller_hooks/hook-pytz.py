"""Keep pytz lightweight in the desktop bundle.

The application uses the standard-library zoneinfo module for user-facing
timezone handling. Pandas can import pytz as an optional dependency, but the
full zoneinfo data tree is unnecessary for the desktop bundle.
"""

from __future__ import annotations

datas: list[tuple[str, str]] = []
hiddenimports: list[str] = []
