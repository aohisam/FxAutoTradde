"""Desktop launcher."""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

from fxautotrade_lab.desktop.runtime import DesktopProcessManager, append_runtime_log, log_runtime_exception
from fxautotrade_lab.desktop.assets import resolve_app_icon_path, should_apply_runtime_window_icon


def _boot_log(message: str) -> None:
    append_runtime_log("desktop_boot.log", message)


def _install_exception_hooks() -> None:
    def handle_exception(exc_type, exc_value, exc_traceback) -> None:  # noqa: ANN001
        log_runtime_exception("main_thread", (exc_type, exc_value, exc_traceback))

    def handle_thread_exception(args) -> None:  # noqa: ANN001
        log_runtime_exception(
            f"thread:{getattr(args, 'thread', None).name if getattr(args, 'thread', None) else 'unknown'}",
            (args.exc_type, args.exc_value, args.exc_traceback),
        )

    def handle_unraisable(unraisable) -> None:  # noqa: ANN001
        exc_type = type(unraisable.exc_value)
        log_runtime_exception(
            f"unraisable:{getattr(unraisable, 'object', None)!r}",
            (exc_type, unraisable.exc_value, unraisable.exc_traceback),
        )

    sys.excepthook = handle_exception
    if hasattr(threading, "excepthook"):
        threading.excepthook = handle_thread_exception
    sys.unraisablehook = handle_unraisable


def _prepare_qt_runtime() -> dict[str, str]:
    try:
        import PySide6
    except ImportError:
        return {}
    package_dir = Path(PySide6.__file__).resolve().parent
    plugins_dir = package_dir / "Qt" / "plugins"
    platforms_dir = plugins_dir / "platforms"
    libraries_dir = package_dir / "Qt" / "lib"
    if plugins_dir.exists():
        os.environ.setdefault("QT_PLUGIN_PATH", str(plugins_dir))
    if platforms_dir.exists():
        os.environ.setdefault("QT_QPA_PLATFORM_PLUGIN_PATH", str(platforms_dir))
    if libraries_dir.exists():
        for env_name in ("DYLD_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH", "DYLD_FALLBACK_FRAMEWORK_PATH"):
            existing = os.environ.get(env_name, "")
            paths = [str(libraries_dir), *([existing] if existing else [])]
            os.environ[env_name] = ":".join(path for path in paths if path)
    helper_candidates = [
        package_dir / "Qt" / "lib" / "QtWebEngineCore.framework" / "Helpers" / "QtWebEngineProcess.app" / "Contents" / "MacOS" / "QtWebEngineProcess",
        package_dir / "Qt" / "lib" / "QtWebEngineCore.framework" / "Versions" / "A" / "Helpers" / "QtWebEngineProcess.app" / "Contents" / "MacOS" / "QtWebEngineProcess",
        package_dir / "Qt" / "lib" / "QtWebEngineCore.framework" / "Versions" / "Resources" / "Helpers" / "QtWebEngineProcess.app" / "Contents" / "MacOS" / "QtWebEngineProcess",
        Path(sys.executable).resolve().parents[1]
        / "Frameworks"
        / "PySide6"
        / "Qt"
        / "lib"
        / "QtWebEngineCore.framework"
        / "Helpers"
        / "QtWebEngineProcess.app"
        / "Contents"
        / "MacOS"
        / "QtWebEngineProcess",
        Path(sys.executable).resolve().parents[1]
        / "Frameworks"
        / "PySide6"
        / "Qt"
        / "lib"
        / "QtWebEngineCore.framework"
        / "Versions"
        / "A"
        / "Helpers"
        / "QtWebEngineProcess.app"
        / "Contents"
        / "MacOS"
        / "QtWebEngineProcess",
        Path(sys.executable).resolve().parents[1]
        / "Frameworks"
        / "PySide6"
        / "Qt"
        / "lib"
        / "QtWebEngineCore.framework"
        / "Versions"
        / "Resources"
        / "Helpers"
        / "QtWebEngineProcess.app"
        / "Contents"
        / "MacOS"
        / "QtWebEngineProcess",
    ]
    for helper_path in helper_candidates:
        if helper_path.exists():
            os.environ.setdefault("QTWEBENGINEPROCESS_PATH", str(helper_path))
            break
    os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")
    os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")
    return {
        "plugins_dir": str(plugins_dir),
        "platforms_dir": str(platforms_dir),
        "libraries_dir": str(libraries_dir),
        "qtwebengineprocess_path": os.environ.get("QTWEBENGINEPROCESS_PATH", ""),
    }


def _resolve_config_path(config_path: Path | None) -> Path | None:
    if config_path is not None:
        return config_path
    bundle_resources = Path(sys.executable).resolve().parents[1] / "Resources"
    pyinstaller_meipass = Path(getattr(sys, "_MEIPASS", "")) if getattr(sys, "_MEIPASS", None) else None
    candidates: list[Path] = []
    if getattr(sys, "frozen", False):
        candidates.append(bundle_resources / "configs" / "mac_desktop_default.yaml")
        if pyinstaller_meipass is not None:
            candidates.append(pyinstaller_meipass / "configs" / "mac_desktop_default.yaml")
    candidates.extend(
        [
            Path.cwd() / "configs" / "mac_desktop_default.yaml",
            Path(__file__).resolve().parents[3] / "configs" / "mac_desktop_default.yaml",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _desktop_storage_overrides() -> dict[str, object] | None:
    if not getattr(sys, "frozen", False):
        return None
    app_support = Path.home() / "Library" / "Application Support" / "FXAutoTradeLab"
    return {
        "data": {"cache_dir": str(app_support / "data_cache")},
        "persistence": {"sqlite_path": str(app_support / "runtime" / "trading_lab.sqlite")},
        "reporting": {"output_dir": str(app_support / "reports")},
        "automation": {
            "notification_channels": {
                "log_path": str(app_support / "runtime" / "notifications.log"),
            }
        },
    }


def launch_desktop_app(config_path: Path | None = None) -> None:  # pragma: no cover - UI helper
    resolved_config = _resolve_config_path(config_path)
    config_overrides = _desktop_storage_overrides()
    runtime_paths = _prepare_qt_runtime()
    process_manager = DesktopProcessManager()
    process_manager.prepare()
    _install_exception_hooks()
    try:
        from PySide6.QtCore import QCoreApplication, QTimer
        from PySide6.QtGui import QIcon
        from PySide6.QtWidgets import QApplication
    except ImportError as exc:
        raise RuntimeError(
            "PySide6 がインストールされていません。`pip install PySide6` の後に再実行してください。"
        ) from exc

    if runtime_paths.get("plugins_dir"):
        QCoreApplication.setLibraryPaths([runtime_paths["plugins_dir"]])

    app = QApplication.instance() or QApplication([])
    app.setApplicationName("FXAutoTrade Lab")
    app.setApplicationDisplayName("FXAutoTrade Lab")
    app.setQuitOnLastWindowClosed(True)
    icon_path = resolve_app_icon_path()
    if icon_path is not None and should_apply_runtime_window_icon():
        app.setWindowIcon(QIcon(str(icon_path)))
    app.aboutToQuit.connect(process_manager.cleanup)

    from fxautotrade_lab.desktop.main_window import load_main_window_class

    _boot_log(
        f"launch:start frozen={getattr(sys, 'frozen', False)} config={resolved_config} "
        f"overrides={'yes' if config_overrides else 'no'}"
    )
    window_class = load_main_window_class()
    window = window_class(resolved_config, config_overrides=config_overrides)
    _boot_log("launch:window_created")

    def present_window() -> None:
        window.showNormal()
        window.raise_()
        window.activateWindow()
        app.processEvents()
        _boot_log(
            "launch:window_presented "
            f"visible={window.isVisible()} minimized={window.isMinimized()} size={window.size().width()}x{window.size().height()}"
        )

    present_window()
    QTimer.singleShot(0, present_window)
    QTimer.singleShot(250, present_window)
    _boot_log("launch:exec")
    app.exec()
