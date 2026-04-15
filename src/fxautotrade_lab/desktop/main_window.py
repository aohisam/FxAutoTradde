"""Main desktop window."""

from __future__ import annotations

from pathlib import Path

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.desktop.assets import resolve_app_icon_path


def load_main_window_class():  # pragma: no cover - UI helper
    from PySide6.QtCore import QSettings, QThreadPool, QTimer, Qt
    from PySide6.QtGui import QGuiApplication, QIcon
    from PySide6.QtWidgets import (
        QDockWidget,
        QListWidget,
        QListWidgetItem,
        QMainWindow,
        QMessageBox,
        QSplitter,
        QStackedWidget,
        QStatusBar,
        QTextEdit,
        QToolBar,
    )

    from fxautotrade_lab.desktop.pages.automation import build_automation_page
    from fxautotrade_lab.desktop.pages.backtest import build_backtest_page
    from fxautotrade_lab.desktop.pages.data_sync import build_data_sync_page
    from fxautotrade_lab.desktop.pages.misc import (
        build_chart_page,
        build_help_page,
        build_history_page,
        build_reports_page,
        build_settings_page,
    )
    from fxautotrade_lab.desktop.pages.overview import build_overview_page
    from fxautotrade_lab.desktop.pages.signals import build_signals_page
    from fxautotrade_lab.desktop.pages.watchlist import build_watchlist_page
    from fxautotrade_lab.desktop.workers import load_worker_classes

    FunctionWorker = load_worker_classes()

    class TradingLabMainWindow(QMainWindow):
        def __init__(self, config_path: Path | None = None, config_overrides: dict | None = None) -> None:
            super().__init__()
            self.app_state = LabApplication(config_path, overrides=config_overrides)
            self.settings = QSettings("FXAutoTradeLab", "Desktop")
            self.setWindowTitle("FXAutoTrade Lab")
            icon_path = resolve_app_icon_path()
            if icon_path is not None:
                self.setWindowIcon(QIcon(str(icon_path)))
            self.resize(self.app_state.config.ui.width, self.app_state.config.ui.height)
            self.thread_pool = QThreadPool.globalInstance()
            self._active_workers: set[object] = set()
            self.log_output = QTextEdit()
            self.log_output.setReadOnly(True)
            self.page_names = [
                "概要",
                "監視通貨ペア",
                "データ同期",
                "バックテスト",
                "シグナル分析",
                "フォワード自動売買",
                "チャート",
                "取引履歴",
                "レポート",
                "設定",
                "ヘルプ",
            ]
            self.sidebar = QListWidget()
            self.sidebar.setMinimumWidth(210)
            self.sidebar.setMaximumWidth(300)
            self.sidebar.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            for name in self.page_names:
                QListWidgetItem(name, self.sidebar)
            self.stack = QStackedWidget()
            splitter = QSplitter()
            splitter.addWidget(self.sidebar)
            splitter.addWidget(self.stack)
            splitter.setStretchFactor(0, 0)
            splitter.setStretchFactor(1, 1)
            splitter.setSizes([220, max(self.width() - 220, 980)])
            self.setCentralWidget(splitter)
            self._build_pages(FunctionWorker)
            self._build_toolbar()
            self._build_log_dock()
            self._apply_theme()
            self.setStatusBar(QStatusBar())
            self.sidebar.currentRowChanged.connect(self.stack.setCurrentIndex)
            self.sidebar.currentRowChanged.connect(lambda _: self.refresh_current_page())
            self.restore_geometry()
            if self.settings.value("page_index") is None:
                self.sidebar.setCurrentRow(max(0, self.page_names.index(self.app_state.config.ui.default_page)))
            self.refresh_current_page()

        def _build_pages(self, worker_class) -> None:
            self.pages = {
                "概要": build_overview_page(self.app_state),
                "監視通貨ペア": build_watchlist_page(self.app_state, self.log_message),
                "データ同期": build_data_sync_page(self.app_state, self.submit_background_task, self.log_message),
                "バックテスト": build_backtest_page(self.app_state, self.submit_background_task, self.log_message),
                "シグナル分析": build_signals_page(self.app_state),
                "フォワード自動売買": build_automation_page(
                    self.app_state, self.submit_background_task, self.log_message
                ),
                "チャート": build_chart_page(self.app_state, self.submit_background_task, self.log_message),
                "取引履歴": build_history_page(self.app_state),
                "レポート": build_reports_page(self.app_state),
                "設定": build_settings_page(self.app_state, self.submit_background_task, self.log_message),
                "ヘルプ": build_help_page(),
            }
            for name in self.page_names:
                self.stack.addWidget(self.pages[name])
            self.worker_class = worker_class

        def _build_toolbar(self) -> None:
            toolbar = QToolBar("メイン")
            toolbar.setMovable(False)
            self.addToolBar(toolbar)
            sync_action = toolbar.addAction("再読込")
            backtest_action = toolbar.addAction("バックテスト")
            demo_action = toolbar.addAction("デモ実行")
            verify_action = toolbar.addAction("ブローカー確認")
            refresh_action = toolbar.addAction("ページ更新")
            about_action = toolbar.addAction("About")
            sync_action.triggered.connect(lambda: self.sidebar.setCurrentRow(self.page_names.index("データ同期")))
            backtest_action.triggered.connect(lambda: self.sidebar.setCurrentRow(self.page_names.index("バックテスト")))
            demo_action.triggered.connect(self._run_demo)
            verify_action.triggered.connect(self._verify_broker)
            refresh_action.triggered.connect(self.refresh_current_page)
            about_action.triggered.connect(self._show_about)

        def _build_log_dock(self) -> None:
            dock = QDockWidget("ログ", self)
            dock.setObjectName("logDock")
            dock.setWidget(self.log_output)
            dock.setAllowedAreas(Qt.BottomDockWidgetArea)
            self.addDockWidget(Qt.BottomDockWidgetArea, dock)
            self.log_output.setMinimumHeight(96)
            QTimer.singleShot(0, lambda: self.resizeDocks([dock], [120], Qt.Vertical))

        def _apply_theme(self) -> None:
            self.setStyleSheet(
                """
                QMainWindow, QWidget {
                    background: #f5f7fb;
                    color: #10243f;
                    font-size: 13px;
                }
                QLabel {
                    border: none;
                }
                QListWidget {
                    background: white;
                    border: 1px solid #dbe3ee;
                    border-radius: 16px;
                    padding: 8px;
                }
                QListWidget::item {
                    padding: 10px 12px;
                    border-radius: 10px;
                    margin: 2px 0;
                }
                QListWidget::item:selected {
                    background: #dcecff;
                    color: #0f172a;
                }
                QToolBar {
                    background: white;
                    spacing: 8px;
                    border: 1px solid #dbe3ee;
                    border-radius: 14px;
                    padding: 8px;
                }
                QPushButton {
                    background: #1d4ed8;
                    color: white;
                    border: none;
                    border-radius: 10px;
                    padding: 8px 14px;
                    min-height: 34px;
                    font-weight: 700;
                }
                QPushButton:hover:enabled {
                    background: #1e40af;
                }
                QPushButton[role="secondary"] {
                    background: #475569;
                }
                QPushButton[role="secondary"]:hover:enabled {
                    background: #334155;
                }
                QPushButton[role="success"] {
                    background: #0f766e;
                }
                QPushButton[role="success"]:hover:enabled {
                    background: #115e59;
                }
                QPushButton[role="danger"] {
                    background: #b91c1c;
                }
                QPushButton[role="danger"]:hover:enabled {
                    background: #991b1b;
                }
                QPushButton[role="neutral"] {
                    background: #64748b;
                }
                QPushButton[role="neutral"]:hover:enabled {
                    background: #475569;
                }
                QPushButton:disabled {
                    background: #d7e0eb;
                    color: #7b8797;
                    border: 1px solid #c2ccd8;
                }
                QPushButton[busyDisabled="true"]:disabled {
                    background: #e5ebf3;
                    color: #728094;
                    border: 1px dashed #b6c2d2;
                }
                QTextEdit, QTextBrowser, QTableView, QComboBox, QLineEdit, QDateEdit {
                    background: white;
                    border: 1px solid #dbe3ee;
                    border-radius: 10px;
                    padding: 8px 10px;
                }
                QComboBox, QLineEdit, QDateEdit {
                    min-width: 260px;
                    min-height: 24px;
                }
                QHeaderView::section {
                    background: #edf2f7;
                    padding: 8px;
                    border: none;
                }
                """
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
            self.log_message(
                f"ブローカー確認完了: status={account.get('status', '-')}, "
                f"positions={len(positions)}, orders={len(orders)}"
            )
            self.refresh_all_pages()

        def refresh_current_page(self) -> None:
            page = self.stack.currentWidget()
            if hasattr(page, "refresh"):
                page.refresh()
            self.statusBar().showMessage(self.page_names[self.stack.currentIndex()])

        def refresh_all_pages(self) -> None:
            for page in self.pages.values():
                if hasattr(page, "refresh"):
                    page.refresh()

        def log_message(self, message: str) -> None:
            self.log_output.append(message)
            self.statusBar().showMessage(message, 5000)

        def submit_background_task(self, fn, on_finished, on_error) -> None:  # noqa: ANN001
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

            worker.signals.finished.connect(handle_finished)
            worker.signals.error.connect(handle_error)
            self.thread_pool.start(worker)

        def restore_geometry(self) -> None:
            geometry = self.settings.value("geometry")
            if geometry:
                self.restoreGeometry(geometry)
            page_index = self.settings.value("page_index")
            if page_index is not None:
                self.sidebar.setCurrentRow(int(page_index))
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
            self.settings.setValue("page_index", self.sidebar.currentRow())
            self.app_state.shutdown()
            self.thread_pool.clear()
            self.thread_pool.waitForDone(3000)
            for worker in list(self._active_workers):
                if hasattr(worker, "dispose"):
                    worker.dispose()
            self._active_workers.clear()
            super().closeEvent(event)

    return TradingLabMainWindow
