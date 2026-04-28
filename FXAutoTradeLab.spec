# -*- mode: python ; coding: utf-8 -*-
# ruff: noqa: F821, UP009

import struct
from pathlib import Path

from PyInstaller.depend import bindepend
from PyInstaller.utils.hooks.qt import QtLibraryInfo

_original_get_imports = bindepend.get_imports
_original_validate_qt_plugin_dependencies = QtLibraryInfo._validate_plugin_dependencies


def _safe_get_imports(filename, search_paths=None):
    try:
        return _original_get_imports(filename, search_paths)
    except struct.error:
        return []


def _safe_validate_qt_plugin_dependencies(self, plugin_file):
    try:
        return _original_validate_qt_plugin_dependencies(self, plugin_file)
    except struct.error as exc:
        return False, f"Mach-O dependency scan failed: {exc}"


QtLibraryInfo._validate_plugin_dependencies = _safe_validate_qt_plugin_dependencies
bindepend.get_imports = _safe_get_imports

# PyInstaller executes the spec in the current working directory context.
ROOT_DIR = Path.cwd().resolve()
SRC_DIR = ROOT_DIR / "src"
SCRIPT_PATH = ROOT_DIR / "scripts" / "desktop_entry.py"
CONFIGS_DIR = ROOT_DIR / "configs"
RESOURCES_DIR = ROOT_DIR / "resources"
DESKTOP_ASSETS_DIR = SRC_DIR / "fxautotrade_lab" / "desktop" / "assets"
ICON_PATH = RESOURCES_DIR / "app_icon.icns"


a = Analysis(
    [str(SCRIPT_PATH)],
    pathex=[str(SRC_DIR)],
    binaries=[],
    datas=[
        (str(CONFIGS_DIR), "configs"),
        (str(RESOURCES_DIR), "resources"),
        (str(DESKTOP_ASSETS_DIR), "fxautotrade_lab/desktop/assets"),
    ],
    hiddenimports=[
        "fxautotrade_lab.desktop.pages.automation",
        "fxautotrade_lab.desktop.pages.backtest",
        "fxautotrade_lab.desktop.pages.chart",
        "fxautotrade_lab.desktop.pages.data_sync",
        "fxautotrade_lab.desktop.pages.help",
        "fxautotrade_lab.desktop.pages.history",
        "fxautotrade_lab.desktop.pages.overview",
        "fxautotrade_lab.desktop.pages.reports",
        "fxautotrade_lab.desktop.pages.settings",
        "fxautotrade_lab.desktop.pages.signals",
        "fxautotrade_lab.desktop.pages.watchlist",
        "fxautotrade_lab.desktop.charts",
        "fxautotrade_lab.desktop.models",
        "fxautotrade_lab.desktop.workers",
    ],
    hookspath=[str(ROOT_DIR / "packaging" / "pyinstaller_hooks")],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Keep the desktop bundle focused on runtime modules; dev/test tools can
        # make PyInstaller traverse large optional dependency trees.
        "_pytest",
        "black",
        "narwhals",
        "pip",
        "plotly",
        "PySide6.QtCharts",
        "pygments",
        "pytest",
        "rich",
        "ruff",
    ],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FXAutoTradeLab",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=[str(ICON_PATH)],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FXAutoTradeLab",
)
app = BUNDLE(
    coll,
    name="FXAutoTradeLab.app",
    icon=str(ICON_PATH),
    bundle_identifier="com.fxautotrade.lab",
)
