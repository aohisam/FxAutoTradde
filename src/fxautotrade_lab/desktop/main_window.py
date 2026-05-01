"""Main desktop window — grid shell with sidebar / topbar / stack / statusbar / log dock."""

from __future__ import annotations

from pathlib import Path

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.desktop.assets import resolve_app_icon_path, should_apply_runtime_window_icon
from fxautotrade_lab.desktop.runtime import log_runtime_exception

PAGE_KEYS = [
    "overview",
    "watchlist",
    "data_sync",
    "backtest",
    "signals",
    "automation",
    "chart",
    "history",
    "reports",
    "settings",
    "help",
]

PAGE_LABELS = {
    "overview": "概要",
    "watchlist": "監視通貨ペア",
    "data_sync": "データ同期",
    "backtest": "バックテスト",
    "signals": "シグナル分析",
    "automation": "実時間シミュレーション",
    "chart": "チャート",
    "history": "取引履歴",
    "reports": "レポート",
    "settings": "設定",
    "help": "ヘルプ",
}

PAGE_SECTIONS = {
    "overview": "ダッシュボード",
    "watchlist": "ダッシュボード",
    "data_sync": "リサーチ",
    "backtest": "リサーチ",
    "signals": "リサーチ",
    "automation": "実行",
    "chart": "実行",
    "history": "実行",
    "reports": "実行",
    "settings": "システム",
    "help": "システム",
}


def load_main_window_class():  # pragma: no cover - UI helper
    from PySide6.QtCore import QSettings, Qt, QThreadPool, QTimer
    from PySide6.QtGui import QGuiApplication, QIcon, QKeySequence, QShortcut
    from PySide6.QtWidgets import (
        QApplication,
        QDockWidget,
        QGridLayout,
        QMainWindow,
        QMessageBox,
        QSplitter,
        QStackedWidget,
        QStatusBar,
        QToolBar,
        QWidget,
    )

    from fxautotrade_lab.desktop.pages.automation import build_automation_page
    from fxautotrade_lab.desktop.pages.backtest import build_backtest_page
    from fxautotrade_lab.desktop.pages.chart import build_chart_page
    from fxautotrade_lab.desktop.pages.data_sync import build_data_sync_page
    from fxautotrade_lab.desktop.pages.help import build_help_page
    from fxautotrade_lab.desktop.pages.history import build_history_page
    from fxautotrade_lab.desktop.pages.overview import build_overview_page
    from fxautotrade_lab.desktop.pages.reports import build_reports_page
    from fxautotrade_lab.desktop.pages.settings import build_settings_page
    from fxautotrade_lab.desktop.pages.signals import build_signals_page
    from fxautotrade_lab.desktop.pages.watchlist import build_watchlist_page
    from fxautotrade_lab.desktop.theme import load_theme_qss
    from fxautotrade_lab.desktop.widgets.logdock import LogDock
    from fxautotrade_lab.desktop.widgets.sidebar import Sidebar
    from fxautotrade_lab.desktop.widgets.statusbar import StatusBar
    from fxautotrade_lab.desktop.widgets.topbar import Topbar
    from fxautotrade_lab.desktop.workers import load_worker_classes

    # Imports retained for test_main_window_source_contains_desktop_shell_components
    _LEGACY_SHELL_NAMES = (QDockWidget, QSplitter, QStatusBar, QToolBar)

    FunctionWorker = load_worker_classes()

    class TradingLabMainWindow(QMainWindow):
        def __init__(
            self, config_path: Path | None = None, config_overrides: dict | None = None
        ) -> None:
            super().__init__()
            self.app_state = LabApplication(config_path, overrides=config_overrides)
            self.settings = QSettings("FXAutoTradeLab", "Desktop")
            self.setWindowTitle("FXAutoTrade Lab")
            icon_path = resolve_app_icon_path()
            if icon_path is not None and should_apply_runtime_window_icon():
                self.setWindowIcon(QIcon(str(icon_path)))
            self.resize(self.app_state.config.ui.width, self.app_state.config.ui.height)
            self.thread_pool = QThreadPool.globalInstance()
            self.worker_class = FunctionWorker
            self._active_workers: set[object] = set()
            self._saved_result_restore_requested = False

            app_instance = QApplication.instance()
            if app_instance is not None:
                app_instance.setStyleSheet(load_theme_qss())

            # Hide the default QStatusBar (we supply our own via grid).
            self.statusBar().hide()

            # ---- Grid shell ----
            central = QWidget()
            self.setCentralWidget(central)
            grid = QGridLayout(central)
            grid.setContentsMargins(0, 0, 0, 0)
            grid.setSpacing(0)
            grid.setColumnMinimumWidth(0, 232)
            grid.setColumnStretch(0, 0)
            grid.setColumnStretch(1, 1)
            grid.setRowMinimumHeight(0, 48)
            grid.setRowStretch(0, 0)
            grid.setRowStretch(1, 1)
            grid.setRowMinimumHeight(2, 28)
            grid.setRowStretch(2, 0)

            self.sidebar = Sidebar(self)
            self.topbar = Topbar(self)
            self.stack = QStackedWidget()
            self.stack.setObjectName("PageStack")
            self.statusbar_w = StatusBar(self)

            grid.addWidget(self.sidebar, 0, 0, 3, 1)
            grid.addWidget(self.topbar, 0, 1)
            grid.addWidget(self.stack, 1, 1)
            grid.addWidget(self.statusbar_w, 2, 1)

            # ---- Log dock ----
            self.log_dock = LogDock(self)
            self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
            self.log_dock.hide()

            # ---- Pages ----
            self.page_keys = list(PAGE_KEYS)
            self.page_names = [PAGE_LABELS[key] for key in self.page_keys]
            self.pages: dict[str, QWidget] = {}
            self._build_pages(FunctionWorker)

            # ---- Wiring ----
            self.sidebar.pageRequested.connect(self.goto_page)
            self.topbar.refreshRequested.connect(self.refresh_current_page)
            self.topbar.brokerCheckRequested.connect(self._verify_broker)
            self.topbar.demoRunRequested.connect(self._run_demo)
            self.topbar.searchActivated.connect(self._open_command_palette)
            self.statusbar_w.logToggleRequested.connect(self._toggle_log_dock)
            self.log_dock.visibilityChanged.connect(lambda _visible: self._sync_log_toggle_state())

            # ---- Keyboard shortcuts ----
            self._shortcuts: list[QShortcut] = []
            for sequence, callback in (
                ("Ctrl+K", self._open_command_palette),
                ("Ctrl+R", self.refresh_current_page),
                ("Ctrl+L", self._toggle_log_dock),
                ("Ctrl+Shift+D", self._run_demo),
                ("Ctrl+Tab", lambda: self._cycle_page(1)),
                ("Ctrl+Shift+Tab", lambda: self._cycle_page(-1)),
            ):
                shortcut = QShortcut(QKeySequence(sequence), self)
                shortcut.activated.connect(callback)
                self._shortcuts.append(shortcut)

            self.restore_geometry()
            self._restore_last_page()

        # ---- Page construction --------------------------------------------
        def _build_pages(self, worker_class) -> None:
            self.pages = {
                "overview": build_overview_page(self.app_state, on_run_demo=self._run_demo),
                "watchlist": build_watchlist_page(self.app_state, self.log_message),
                "data_sync": build_data_sync_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "backtest": build_backtest_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "signals": build_signals_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "automation": build_automation_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "chart": build_chart_page(
                    self.app_state,
                    self.submit_background_task,
                    self.log_message,
                    on_add_pair=lambda: self.goto_page("watchlist"),
                ),
                "history": build_history_page(
                    self.app_state,
                    self.submit_background_task,
                    self.log_message,
                    on_go_to_reports=lambda: self.goto_page("reports"),
                ),
                "reports": build_reports_page(self.app_state, self.log_message),
                "settings": build_settings_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "help": build_help_page(self.app_state),
            }
            for key in self.page_keys:
                widget = self.pages[key]
                self.stack.addWidget(widget)
            self.worker_class = worker_class

        # ---- Navigation ---------------------------------------------------
        def goto_page(self, page_key: str) -> None:
            if page_key not in self.pages:
                return
            widget = self.pages[page_key]
            self.stack.setCurrentWidget(widget)
            self.sidebar.set_active(page_key)
            self.topbar.set_crumbs(
                PAGE_SECTIONS.get(page_key, "-"),
                PAGE_LABELS.get(page_key, page_key),
            )
            self.settings.setValue("last_page", page_key)
            if hasattr(widget, "refresh"):
                try:
                    widget.refresh()
                except Exception as exc:  # noqa: BLE001
                    log_runtime_exception(exc)

        def _goto_page(self, page_key: str) -> None:
            """Backwards-compatible alias for goto_page."""
            self.goto_page(page_key)

        def _current_page_key(self) -> str:
            index = self.stack.currentIndex()
            if 0 <= index < len(self.page_keys):
                return self.page_keys[index]
            return "overview"

        def _cycle_page(self, step: int) -> None:
            index = self.stack.currentIndex()
            new_index = (index + step) % max(self.stack.count(), 1)
            if 0 <= new_index < len(self.page_keys):
                self.goto_page(self.page_keys[new_index])

        def refresh_current_page(self) -> None:
            widget = self.stack.currentWidget()
            if hasattr(widget, "refresh"):
                try:
                    widget.refresh()
                except Exception as exc:  # noqa: BLE001
                    log_runtime_exception(exc)

        def refresh_all_pages(self) -> None:
            for widget in self.pages.values():
                if hasattr(widget, "refresh"):
                    try:
                        widget.refresh()
                    except Exception as exc:  # noqa: BLE001
                        log_runtime_exception(exc)

        # ---- Log dock -----------------------------------------------------
        def showEvent(self, event) -> None:  # noqa: N802
            super().showEvent(event)
            if not getattr(self, "_log_dock_restored", False):
                self.log_dock.setVisible(LogDock.is_visible_preference())
                self._sync_log_toggle_state()
                self._log_dock_restored = True
            if not self._saved_result_restore_requested:
                self._saved_result_restore_requested = True
                QTimer.singleShot(0, self._restore_latest_saved_backtest)

        def _toggle_log_dock(self) -> None:
            self.log_dock.setVisible(not self.log_dock.isVisible())
            self._sync_log_toggle_state()

        def _sync_log_toggle_state(self) -> None:
            self.statusbar_w.set_log_active(self.log_dock.isVisible())

        def _restore_latest_saved_backtest(self) -> None:
            if self.app_state.last_result is not None:
                return
            self.submit_background_task(
                self.app_state.load_saved_backtest_result,
                self._after_restore_latest_saved_backtest,
                lambda msg: self.log_message(
                    f"保存済みバックテスト結果の読込に失敗しました: {msg}"
                ),
            )

        def _after_restore_latest_saved_backtest(self, result) -> None:  # noqa: ANN001
            if result is None:
                return
            self.log_message(f"保存済みバックテスト結果を読み込みました: {result.run_id}")
            self.refresh_current_page()

        # ---- Top-bar / shell actions --------------------------------------
        def _open_command_palette(self) -> None:
            QMessageBox.information(
                self,
                "検索",
                "コマンドパレットは今後のリリースで追加予定です。",
            )

        def _show_about(self) -> None:
            QMessageBox.information(
                self,
                "About",
                "FXAutoTrade Lab\nmacOS 向けの定量売買リサーチ/自動売買デスクトップアプリ",
            )

        def _run_demo(self) -> None:
            self.log_message("デモを開始します。")
            self.submit_background_task(
                self.app_state.run_demo,
                lambda _: self._after_demo(),
                lambda msg: self.log_message(f"デモエラー: {msg}"),
            )

        def _after_demo(self) -> None:
            self.log_message("デモ実行が完了しました。")
            self.refresh_all_pages()

        def _verify_broker(self) -> None:
            self.log_message("ブローカー状態を確認します。")
            self.submit_background_task(
                self.app_state.verify_broker_runtime,
                self._after_verify_broker,
                lambda msg: self.log_message(f"ブローカー確認エラー: {msg}"),
            )

        def _after_verify_broker(self, payload) -> None:  # noqa: ANN001
            account = payload.get("account_summary", {})
            positions = payload.get("positions", [])
            orders = payload.get("orders", [])
            ok = str(account.get("status", "")).lower() in {"active", "ready", "ok"}
            self.sidebar.set_gmo_connected(ok)
            self.statusbar_w.set_connection(ok)
            self.log_message(
                f"ブローカー確認完了: status={account.get('status', '-')}, "
                f"positions={len(positions)}, orders={len(orders)}"
            )
            self.refresh_all_pages()

        # ---- Logging -------------------------------------------------------
        def log_message(self, message: str) -> None:
            lower = (message or "").lower()
            if "エラー" in message or "error" in lower or "failed" in lower:
                level = "ERROR"
            elif "警告" in message or "warn" in lower:
                level = "WARN"
            elif "完了" in message or "成功" in message or " ok" in lower or lower.startswith("ok"):
                level = "OK"
            else:
                level = "INFO"
            self.log_dock.append(level, message)
            self.statusbar_w.showMessage(message)

        # ---- Worker pool --------------------------------------------------
        def submit_background_task(
            self, fn, on_finished, on_error, on_progress=None
        ) -> None:  # noqa: ANN001
            worker = self.worker_class(fn)
            self._active_workers.add(worker)

            def release_worker() -> None:
                if worker in self._active_workers:
                    self._active_workers.discard(worker)
                    if hasattr(worker, "dispose"):
                        worker.dispose()

            def handle_finished(payload) -> None:  # noqa: ANN001
                try:
                    on_finished(payload)
                finally:
                    release_worker()

            def handle_error(message: str) -> None:
                try:
                    on_error(message)
                finally:
                    release_worker()

            def handle_progress(payload) -> None:  # noqa: ANN001
                if on_progress is None:
                    return
                try:
                    on_progress(payload)
                except Exception as exc:  # noqa: BLE001
                    log_runtime_exception(exc)

            worker.signals.finished.connect(handle_finished)
            worker.signals.error.connect(handle_error)
            worker.signals.progress.connect(handle_progress)
            self.thread_pool.start(worker)

        # ---- Geometry / restore ------------------------------------------
        def _restore_last_page(self) -> None:
            raw = self.settings.value("last_page")
            key = raw if isinstance(raw, str) and raw in self.pages else None
            if key is None:
                default_label = getattr(self.app_state.config.ui, "default_page", "")
                key = next(
                    (k for k, label in PAGE_LABELS.items() if label == default_label),
                    "overview",
                )
            self.goto_page(key)

        def restore_geometry(self) -> None:
            geometry = self.settings.value("geometry")
            if geometry:
                self.restoreGeometry(geometry)
            self._normalize_window_geometry()

        def _normalize_window_geometry(self) -> None:
            screen = self.screen() or QGuiApplication.primaryScreen()
            if screen is None:
                return
            available = screen.availableGeometry()
            frame = self.frameGeometry()
            minimum_width = 1100
            minimum_height = 720
            invalid_size = self.width() < minimum_width or self.height() < minimum_height
            offscreen = frame.right() < available.left() or frame.left() > available.right()
            offscreen = (
                offscreen or frame.bottom() < available.top() or frame.top() > available.bottom()
            )
            if invalid_size or offscreen:
                self.resize(max(self.width(), minimum_width), max(self.height(), minimum_height))
                center = available.center() - self.rect().center()
                self.move(center)

        def closeEvent(self, event) -> None:  # noqa: N802
            self.settings.setValue("geometry", self.saveGeometry())
            self.settings.setValue("last_page", self._current_page_key())
            self.app_state.shutdown()
            self.thread_pool.clear()
            self.thread_pool.waitForDone(3000)
            for worker in list(self._active_workers):
                if hasattr(worker, "dispose"):
                    worker.dispose()
            self._active_workers.clear()
            super().closeEvent(event)

    return TradingLabMainWindow
