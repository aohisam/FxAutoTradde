from __future__ import annotations

import os


def test_desktop_modules_import():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    from fxautotrade_lab.desktop.app import launch_desktop_app  # noqa: F401
    from fxautotrade_lab.desktop.main_window import load_main_window_class

    assert load_main_window_class is not None


def test_main_window_source_contains_desktop_shell_components():
    text = open("src/fxautotrade_lab/desktop/main_window.py", encoding="utf-8").read()
    for fragment in ["QMainWindow", "QSplitter", "QDockWidget", "QStatusBar", "QToolBar"]:
        assert fragment in text
