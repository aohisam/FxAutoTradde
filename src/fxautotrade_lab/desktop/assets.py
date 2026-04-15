"""Desktop asset path helpers."""

from __future__ import annotations

from pathlib import Path
import sys


def resolve_app_icon_path() -> Path | None:
    """Return the best available application icon path for source or bundled runs."""

    candidates: list[Path] = []
    executable = Path(sys.executable).resolve()
    if getattr(sys, "frozen", False):
        bundle_resources = executable.parents[1] / "Resources"
        candidates.extend(
            [
                bundle_resources / "app_icon.icns",
                bundle_resources / "resources" / "app_icon.icns",
                bundle_resources / "icon.png",
            ]
        )
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            meipass_root = Path(str(meipass))
            candidates.extend(
                [
                    meipass_root / "resources" / "app_icon.icns",
                    meipass_root / "icon.png",
                ]
            )

    repo_root = Path(__file__).resolve().parents[3]
    candidates.extend(
        [
            repo_root / "icon.png",
            repo_root / "resources" / "app_icon.icns",
            repo_root / "resources" / "app_icon.svg",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None
