# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
SCRIPT_PATH = ROOT_DIR / "scripts" / "desktop_entry.py"
CONFIGS_DIR = ROOT_DIR / "configs"
RESOURCES_DIR = ROOT_DIR / "resources"
ICON_PATH = RESOURCES_DIR / "app_icon.icns"

a = Analysis(
    [str(SCRIPT_PATH)],
    pathex=[str(SRC_DIR)],
    binaries=[],
    datas=[(str(CONFIGS_DIR), 'configs'), (str(RESOURCES_DIR), 'resources')],
    hiddenimports=['pytz', 'fxautotrade_lab.desktop.pages.automation', 'fxautotrade_lab.desktop.pages.backtest', 'fxautotrade_lab.desktop.pages.data_sync', 'fxautotrade_lab.desktop.pages.misc', 'fxautotrade_lab.desktop.pages.overview', 'fxautotrade_lab.desktop.pages.signals', 'fxautotrade_lab.desktop.pages.watchlist', 'fxautotrade_lab.desktop.charts', 'fxautotrade_lab.desktop.models', 'fxautotrade_lab.desktop.workers', 'PySide6.QtCharts'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='FXAutoTradeLab',
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
    name='FXAutoTradeLab',
)
app = BUNDLE(
    coll,
    name='FXAutoTradeLab.app',
    icon=str(ICON_PATH),
    bundle_identifier='com.fxautotrade.lab',
)
