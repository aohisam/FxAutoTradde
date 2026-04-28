"""Slim pyarrow hook for the desktop bundle.

The default hook collects pyarrow's C++ source tree as data. On macOS that
forces PyInstaller to inspect thousands of non-runtime files during binary/data
reclassification. The app needs parquet runtime libraries, not bundled sources.
"""

from __future__ import annotations

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

hiddenimports = [
    "pyarrow._compute",
    "pyarrow._fs",
    "pyarrow._parquet",
    "pyarrow.compute",
    "pyarrow.fs",
    "pyarrow.lib",
    "pyarrow.pandas_compat",
    "pyarrow.parquet",
    "pyarrow.types",
]
datas = collect_data_files(
    "pyarrow",
    excludes=[
        "**/benchmark/**",
        "**/benchmarks/**",
        "**/include/**",
        "**/src/**",
        "**/tests/**",
        "**/*.cc",
        "**/*.h",
        "**/*.pxd",
        "**/*.pyx",
    ],
)


def _needed_runtime_library(entry: tuple[str, str]) -> bool:
    source, _target = entry
    name = Path(source).name
    if not name.endswith(".dylib"):
        return False
    blocked = ("dataset", "flight", "gandiva", "orc", "substrait")
    return name.startswith(("libarrow", "libparquet")) and not any(part in name for part in blocked)


binaries = [entry for entry in collect_dynamic_libs("pyarrow") if _needed_runtime_library(entry)]
