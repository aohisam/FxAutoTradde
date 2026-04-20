"""Main desktop window."""

from __future__ import annotations

from pathlib import Path

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.desktop.assets import resolve_app_icon_path, should_apply_runtime_window_icon
from fxautotrade_lab.desktop.runtime import log_runtime_exception


NAV_GROUPS = [
    ("ダッシュボード", [
        ("概要", "overview", "nav_overview"),
    ]),
    ("リサーチ", [
        ("監視通貨ペア", "watchlist", "nav_watchlist"),
        ("データ同期", "data_sync", "nav_data_sync"),
        ("バックテスト", "backtest", "nav_backtest"),
        ("シグナル分析", "signals", "nav_signals"),
    ]),
    ("実行", [
        ("実時間シミュレーション", "automation", "nav_automation"),
        ("チャート", "chart", "nav_chart"),
        ("取引履歴", "history", "nav_history"),
        ("レポート", "reports", "nav_reports"),
    ]),
    ("システム", [
        ("設定", "settings", "nav_settings"),
        ("ヘルプ", "help", "nav_help"),
    ]),
]

PAGE_KEY_TO_LABEL = {
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

PAGE_LABEL_TO_KEY = {label: key for key, label in PAGE_KEY_TO_LABEL.items()}


def load_main_window_class():  # pragma: no cover - UI helper
    from PySide6.QtCore import QSettings, QSize, QThreadPool, QTimer, Qt
    from PySide6.QtGui import QGuiApplication, QIcon
    from PySide6.QtWidgets import (
        QApplication,
        QDockWidget,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QSizePolicy,
        QSplitter,
        QStackedWidget,
        QStatusBar,
        QTextEdit,
        QToolBar,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.pages.automation import build_automation_page
    from fxautotrade_lab.desktop.pages.backtest import build_backtest_page
    from fxautotrade_lab.desktop.pages.chart import build_chart_page
    from fxautotrade_lab.desktop.pages.data_sync import build_data_sync_page
    from fxautotrade_lab.desktop.pages.history import build_history_page
    from fxautotrade_lab.desktop.pages.misc import (
        build_help_page,
        build_settings_page,
    )
    from fxautotrade_lab.desktop.pages.reports import build_reports_page
    from fxautotrade_lab.desktop.pages.overview import build_overview_page
    from fxautotrade_lab.desktop.pages.signals import build_signals_page
    from fxautotrade_lab.desktop.pages.watchlist import build_watchlist_page
    from fxautotrade_lab.desktop.theme import Tokens, load_icon, load_theme_qss
    from fxautotrade_lab.desktop.widgets.sidebar import GroupedNavList
    from fxautotrade_lab.desktop.widgets.statusbar import AppStatusBar
    from fxautotrade_lab.desktop.workers import load_worker_classes

    FunctionWorker = load_worker_classes()

    class TradingLabMainWindow(QMainWindow):
        def __init__(self, config_path: Path | None = None, config_overrides: dict | None = None) -> None:
            super().__init__()
            self.app_state = LabApplication(config_path, overrides=config_overrides)
            self.settings = QSettings("FXAutoTradeLab", "Desktop")
            self.setWindowTitle("FXAutoTrade Lab")
            icon_path = resolve_app_icon_path()
            if icon_path is not None and should_apply_runtime_window_icon():
                self.setWindowIcon(QIcon(str(icon_path)))
            self.resize(self.app_state.config.ui.width, self.app_state.config.ui.height)
            self.thread_pool = QThreadPool.globalInstance()
            self._active_workers: set[object] = set()

            app_instance = QApplication.instance()
            if app_instance is not None:
                app_instance.setStyleSheet(load_theme_qss())

            self.log_output = QTextEdit()
            self.log_output.setObjectName("logOutput")
            self.log_output.setReadOnly(True)

            self.page_names = [PAGE_KEY_TO_LABEL[entry[1]] for _, entries in NAV_GROUPS for entry in entries]
            self.page_keys = [entry[1] for _, entries in NAV_GROUPS for entry in entries]

            self.sidebar = GroupedNavList()
            self._page_row_by_key: dict[str, int] = {}
            self._build_sidebar()

            self.stack = QStackedWidget()
            splitter = QSplitter()
            splitter.setHandleWidth(1)
            splitter.addWidget(self.sidebar)
            splitter.addWidget(self.stack)
            splitter.setStretchFactor(0, 0)
            splitter.setStretchFactor(1, 1)
            splitter.setSizes([230, max(self.width() - 230, 980)])

            self.app_statusbar = AppStatusBar(self)
            self.setStatusBar(self.app_statusbar)

            central = QWidget()
            central_layout = QVBoxLayout(central)
            central_layout.setContentsMargins(0, 0, 0, 0)
            central_layout.setSpacing(0)
            central_layout.addWidget(splitter, 1)
            self.setCentralWidget(central)

            self._build_pages(FunctionWorker)
            self._build_toolbar()
            self._build_log_dock()

            self.sidebar.currentRowChanged.connect(self._on_sidebar_row_changed)
            self.restore_geometry()
            if self.settings.value("page_index") is None:
                default_label = self.app_state.config.ui.default_page
                default_key = PAGE_LABEL_TO_KEY.get(default_label, "overview")
                row = self._page_row_by_key.get(default_key, -1)
                if row >= 0:
                    self.sidebar.setCurrentRow(row)
            self.refresh_current_page()

        def _build_sidebar(self) -> None:
            for group_caption, entries in NAV_GROUPS:
                self.sidebar.add_group(group_caption)
                for entry in entries:
                    label, key = entry[0], entry[1]
                    icon_name = entry[2] if len(entry) > 2 else None
                    item = self.sidebar.add_page(label, key, icon_name)
                    self._page_row_by_key[key] = self.sidebar.row(item)

        def _build_pages(self, worker_class) -> None:
            self.pages = {
                "概要": build_overview_page(self.app_state),
                "監視通貨ペア": build_watchlist_page(self.app_state, self.log_message),
                "データ同期": build_data_sync_page(self.app_state, self.submit_background_task, self.log_message),
                "バックテスト": build_backtest_page(self.app_state, self.submit_background_task, self.log_message),
                "シグナル分析": build_signals_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "実時間シミュレーション": build_automation_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "チャート": build_chart_page(
                    self.app_state,
                    self.submit_background_task,
                    self.log_message,
                    on_add_pair=lambda: self._goto_page("watchlist"),
                ),
                "取引履歴": build_history_page(
                    self.app_state,
                    self.log_message,
                    on_go_to_reports=lambda: self._goto_page("reports"),
                ),
                "レポート": build_reports_page(self.app_state, self.log_message),
                "設定": build_settings_page(self.app_state, self.submit_background_task, self.log_message),
                "ヘルプ": build_help_page(),
            }
            for name in self.page_names:
                self.stack.addWidget(self.pages[name])
            self.worker_class = worker_class

        def _build_toolbar(self) -> None:
            toolbar = QToolBar("メイン")
            toolbar.setObjectName("topBar")
            toolbar.setMovable(False)
            toolbar.setFloatable(False)
            self.addToolBar(toolbar)

            self.crumb_root = QLabel("FXAutoTrade Lab")
            self.crumb_root.setProperty("role", "breadcrumb")
            crumb_sep = QLabel("/")
            crumb_sep.setProperty("role", "breadcrumb")
            self.crumb_page = QLabel("概要")
            self.crumb_page.setProperty("role", "breadcrumb-current")
            toolbar.addWidget(self.crumb_root)
            toolbar.addWidget(crumb_sep)
            toolbar.addWidget(self.crumb_page)

            spacer = QWidget()
            spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            toolbar.addWidget(spacer)

            buttons = [
                ("再読込", "top_refresh", "ghost", self.refresh_current_page),
                ("デモ実行", "top_demo", "ghost", self._run_demo),
                ("ブローカー確認", "top_broker", "ghost", self._verify_broker),
                ("バックテスト", "top_backtest", "primary", lambda: self._goto_page("backtest")),
                ("About", "top_about", "ghost", self._show_about),
            ]
            for label, icon_name, variant, callback in buttons:
                btn = QPushButton(label)
                btn.setProperty("variant", variant)
                icon_color = Tokens.INVERSE if variant == "primary" else Tokens.MUTED
                btn.setIcon(load_icon(icon_name, icon_color, 16))
                btn.setIconSize(QSize(16, 16))
                btn.clicked.connect(callback)
                toolbar.addWidget(btn)

        def _build_log_dock(self) -> None:
            from PySide6.QtCore import Qt
            dock = QDockWidget("ログ", self)
            dock.setObjectName("logDock")
            dock.setWidget(self.log_output)
            dock.setAllowedAreas(Qt.BottomDockWidgetArea)
            self.addDockWidget(Qt.BottomDockWidgetArea, dock)
            self.log_output.setMinimumHeight(96)
            QTimer.singleShot(0, lambda: self.resizeDocks([dock], [120], Qt.Vertical))

        # navigation ---------------------------------------------------------
        def _on_sidebar_row_changed(self, row: int) -> None:
            item = self.sidebar.item(row) if row >= 0 else None
            if item is None:
                return
            key = item.data(0x0100)  # Qt.UserRole
            if key in (None, "__group__"):
                return
            try:
                page_index = self.page_keys.index(key)
            except ValueError:
                return
            self.stack.setCurrentIndex(page_index)
            self.crumb_page.setText(self.page_names[page_index])
            self.refresh_current_page()

        def _goto_page(self, key: str) -> None:
            row = self._page_row_by_key.get(key, -1)
            if row >= 0:
                self.sidebar.setCurrentRow(row)

        # toolbar handlers ---------------------------------------------------
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
            self.log_message(
                f"ブローカー確認完了: status={account.get('status', '-')}, "
                f"positions={len(positions)}, orders={len(orders)}"
            )
            self.refresh_all_pages()

        # page helpers -------------------------------------------------------
        def refresh_current_page(self) -> None:
            page = self.stack.currentWidget()
            if hasattr(page, "refresh"):
                try:
                    page.refresh()
                except Exception:  # noqa: BLE001
                    log_runtime_exception("refresh_current_page")
                    self.log_message("画面更新中にエラーが発生しました。詳細は runtime/desktop_error.log を確認してください。")
            index = self.stack.currentIndex()
            if 0 <= index < len(self.page_names):
                name = self.page_names[index]
                self.app_statusbar.show_page(name)
                self.crumb_page.setText(name)

        def refresh_all_pages(self) -> None:
            for page in self.pages.values():
                if hasattr(page, "refresh"):
                    try:
                        page.refresh()
                    except Exception:  # noqa: BLE001
                        log_runtime_exception("refresh_all_pages")
                        self.log_message("一部の画面更新でエラーが発生しました。詳細は runtime/desktop_error.log を確認してください。")

        def log_message(self, message: str) -> None:
            self.log_output.append(message)
            self.app_statusbar.showMessage(message, 5000)

        def submit_background_task(self, fn, on_finished, on_error, on_progress=None) -> None:  # noqa: ANN001
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
                except Exception:  # noqa: BLE001
                    log_runtime_exception("background_task:on_finished")
                    QMessageBox.critical(
                        self,
                        "処理結果の反映エラー",
                        "処理完了後の画面反映でエラーが発生しました。詳細は runtime/desktop_error.log を確認してください。",
                    )
                finally:
                    release_worker()

            def handle_error(message: str) -> None:
                try:
                    on_error(message)
                except Exception:  # noqa: BLE001
                    log_runtime_exception("background_task:on_error")
                    QMessageBox.critical(
                        self,
                        "エラー表示の反映失敗",
                        "エラー内容の表示中に別のエラーが発生しました。詳細は runtime/desktop_error.log を確認してください。",
                    )
                finally:
                    release_worker()

            def handle_progress(payload) -> None:  # noqa: ANN001
                if on_progress is None:
                    return
                try:
                    on_progress(payload)
                except Exception:  # noqa: BLE001
                    log_runtime_exception("background_task:on_progress")
                    self.log_message("進捗表示の更新でエラーが発生しました。詳細は runtime/desktop_error.log を確認してください。")

            worker.signals.finished.connect(handle_finished)
            worker.signals.error.connect(handle_error)
            if on_progress is not None:
                worker.signals.progress.connect(handle_progress)
            self.thread_pool.start(worker)

        def restore_geometry(self) -> None:
            geometry = self.settings.value("geometry")
            if geometry:
                self.restoreGeometry(geometry)
            page_index = self.settings.value("page_index")
            if page_index is not None:
                try:
                    idx = int(page_index)
                except (TypeError, ValueError):
                    idx = 0
                if 0 <= idx < len(self.page_keys):
                    row = self._page_row_by_key.get(self.page_keys[idx], -1)
                    if row >= 0:
                        self.sidebar.setCurrentRow(row)
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
            offscreen = offscreen or frame.bottom() < available.top() or frame.top() > available.bottom()
            if invalid_size or offscreen:
                self.resize(max(self.width(), minimum_width), max(self.height(), minimum_height))
                center = available.center() - self.rect().center()
                self.move(center)

        def closeEvent(self, event) -> None:  # noqa: N802
            self.settings.setValue("geometry", self.saveGeometry())
            self.settings.setValue("page_index", self.stack.currentIndex())
            self.app_state.shutdown()
            self.thread_pool.clear()
            self.thread_pool.waitForDone(3000)
            for worker in list(self._active_workers):
                if hasattr(worker, "dispose"):
                    worker.dispose()
            self._active_workers.clear()
            super().closeEvent(event)

    return TradingLabMainWindow
