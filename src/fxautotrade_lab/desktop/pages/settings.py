"""Settings page."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import yaml

from fxautotrade_lab.core.enums import TimeFrame


MODE_LABELS = ["GMO 実時間シミュレーション", "ローカルシミュレーション"]
MODE_KEYS = ["gmo_sim", "local_sim"]
MODE_LABEL_BY_KEY = dict(zip(MODE_KEYS, MODE_LABELS))

SOURCE_LABELS = ["GMO", "JForex CSV", "fixture"]
SOURCE_KEYS = ["gmo", "csv", "fixture"]
SOURCE_LABEL_BY_KEY = dict(zip(SOURCE_KEYS, SOURCE_LABELS))

ENTRY_TF_LABELS = ["5m", "15m", "1h", "4h"]
ENTRY_TF_ENUMS = [
    TimeFrame.MIN_5,
    TimeFrame.MIN_15,
    TimeFrame.HOUR_1,
    TimeFrame.HOUR_4,
]
ENTRY_TF_LABEL_BY_VALUE = dict(zip([tf.value for tf in ENTRY_TF_ENUMS], ENTRY_TF_LABELS))

PAGE_LABELS = ["概要", "バックテスト", "実時間シミュレーション", "チャート"]

SIZE_MODE_LABELS = ["定額", "資産比率", "リスク率"]
SIZE_MODE_KEYS = ["fixed_amount", "equity_fraction", "risk_based"]
SIZE_MODE_LABEL_BY_KEY = dict(zip(SIZE_MODE_KEYS, SIZE_MODE_LABELS))

LEVEL_LABELS = ["error のみ", "warn 以上", "info 以上"]
LEVEL_KEYS = ["error", "warning", "info"]
LEVEL_LABEL_BY_KEY = dict(zip(LEVEL_KEYS, LEVEL_LABELS))


def _config_to_dict(cfg) -> dict:
    if hasattr(cfg, "model_dump"):
        try:
            return cfg.model_dump(mode="json")
        except Exception:  # noqa: BLE001
            pass
    if hasattr(cfg, "dict"):
        try:
            return cfg.dict()
        except Exception:  # noqa: BLE001
            pass
    return {"repr": repr(cfg)}


def build_settings_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import QSettings, Qt, QUrl
    from PySide6.QtGui import QDesktopServices
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QScrollArea,
        QSizePolicy,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl
    from fxautotrade_lab.desktop.widgets.suffix_input import LabeledSuffixInput

    # ---- Helpers ----------------------------------------------------------

    def make_form_grid() -> tuple[QWidget, QGridLayout]:
        widget = QWidget()
        grid = QGridLayout(widget)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(12)
        grid.setColumnMinimumWidth(0, 160)
        grid.setColumnStretch(1, 1)
        return widget, grid

    def add_row(grid: QGridLayout, label_text: str, field: QWidget) -> None:
        row = grid.rowCount()
        label = QLabel(label_text)
        label.setProperty("role", "form-label")
        grid.addWidget(label, row, 0, Qt.AlignRight | Qt.AlignTop)
        grid.addWidget(field, row, 1)

    def make_hint(text: str) -> QLabel:
        hint = QLabel(text)
        hint.setProperty("role", "muted2")
        return hint

    def make_detail_cell(label_text: str, value_text: str = "-", *, mono: bool = True) -> tuple[QWidget, QLabel]:
        wrap = QWidget()
        vbox = QVBoxLayout(wrap)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.setSpacing(2)
        eyebrow = QLabel(label_text)
        eyebrow.setProperty("role", "detail-label")
        value = QLabel(value_text)
        value.setProperty("role", "detail-value" if mono else "mono")
        value.setWordWrap(True)
        vbox.addWidget(eyebrow)
        vbox.addWidget(value)
        return wrap, value

    # ---- Page scaffold ----------------------------------------------------

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    page.last_test_result = None  # preserved from legacy API

    # ---- Header -----------------------------------------------------------
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("設定")
    title.setProperty("role", "h1")
    subtitle = QLabel("運用モード、リスク、通知、GMO 接続を一括管理します。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    reset_btn = QPushButton("既定値に戻す")
    reset_btn.setProperty("variant", "ghost")
    save_btn = QPushButton("設定を保存")
    save_btn.setProperty("variant", "primary")
    header.addWidget(reset_btn)
    header.addWidget(save_btn)
    layout.addLayout(header)

    # ---- Card 1: 運用モード ----------------------------------------------
    mode_seg = SegmentedControl(MODE_LABELS, current=0, data=MODE_KEYS)
    source_seg = SegmentedControl(SOURCE_LABELS, current=0, data=SOURCE_KEYS)
    entry_tf_seg = SegmentedControl(ENTRY_TF_LABELS, current=1, data=ENTRY_TF_ENUMS)
    default_page_combo = QComboBox()
    default_page_combo.addItems(PAGE_LABELS)
    default_page_combo.setMaximumWidth(280)

    mode_body, mode_grid = make_form_grid()
    add_row(mode_grid, "モード", mode_seg)
    add_row(mode_grid, "市場データソース", source_seg)
    add_row(mode_grid, "エントリー足", entry_tf_seg)
    add_row(mode_grid, "デフォルトのページ", default_page_combo)

    mode_card = Card(
        title="運用モード",
        header_right=make_hint("GMO 実時間データ / ローカル fixture"),
    )
    mode_card.addBodyWidget(mode_body)
    layout.addWidget(mode_card)

    # ---- Card 2: 資金 / 注文サイズ ---------------------------------------
    starting_cash = LabeledSuffixInput(suffix="JPY")
    starting_cash.setMaximumWidth(240)
    size_mode_seg = SegmentedControl(SIZE_MODE_LABELS, current=1, data=SIZE_MODE_KEYS)
    eq_frac = LabeledSuffixInput(suffix="%")
    eq_frac.setMaximumWidth(200)

    daily_loss_jpy = LabeledSuffixInput(suffix="JPY")
    daily_loss_jpy.setMaximumWidth(240)
    daily_loss_pct = LabeledSuffixInput(suffix="%")
    daily_loss_pct.setMaximumWidth(200)
    max_positions = LabeledSuffixInput(suffix="件")
    max_positions.setMaximumWidth(160)

    left_body, left_grid = make_form_grid()
    add_row(left_grid, "初期資産", starting_cash)
    add_row(left_grid, "注文サイズモード", size_mode_seg)
    add_row(left_grid, "資産比率 / 取引", eq_frac)

    right_body, right_grid = make_form_grid()
    add_row(right_grid, "日次損失上限 (JPY)", daily_loss_jpy)
    add_row(right_grid, "日次損失上限 (%)", daily_loss_pct)
    add_row(right_grid, "同時保有上限", max_positions)

    cash_row = QHBoxLayout()
    cash_row.setSpacing(16)
    cash_row.addWidget(left_body, 1)
    cash_row.addWidget(right_body, 1)
    cash_wrap = QWidget()
    cash_wrap.setLayout(cash_row)

    cash_card = Card(title="資金 / 注文サイズ")
    cash_card.addBodyWidget(cash_wrap)
    layout.addWidget(cash_card)

    # ---- Card 3: 通知チャネル --------------------------------------------
    log_path_edit = QLineEdit()
    log_path_edit.setReadOnly(True)
    log_open_btn = QPushButton("開く")
    log_open_btn.setProperty("variant", "ghost")
    log_row_widget = QWidget()
    log_row = QHBoxLayout(log_row_widget)
    log_row.setContentsMargins(0, 0, 0, 0)
    log_row.setSpacing(6)
    log_row.addWidget(log_path_edit, 1)
    log_row.addWidget(log_open_btn)

    slack_edit = QLineEdit()
    slack_edit.setEchoMode(QLineEdit.Password)
    slack_edit.setPlaceholderText("https://hooks.slack.com/...")

    level_seg = SegmentedControl(LEVEL_LABELS, current=2, data=LEVEL_KEYS)

    events_widget = QWidget()
    events_row = QHBoxLayout(events_widget)
    events_row.setContentsMargins(0, 0, 0, 0)
    events_row.setSpacing(14)
    notify_new_order = QCheckBox("新規注文")
    notify_new_order.setChecked(True)
    notify_fill = QCheckBox("約定")
    notify_fill.setChecked(True)
    notify_kill = QCheckBox("キルスイッチ")
    notify_kill.setChecked(True)
    notify_reconnect = QCheckBox("再接続")
    notify_daily_pnl = QCheckBox("日次損益")
    for box in (notify_new_order, notify_fill, notify_kill, notify_reconnect, notify_daily_pnl):
        events_row.addWidget(box)
    events_row.addStretch(1)

    notif_body, notif_grid = make_form_grid()
    add_row(notif_grid, "ログファイル", log_row_widget)
    add_row(notif_grid, "Slack Webhook", slack_edit)
    add_row(notif_grid, "通知レベル", level_seg)
    add_row(notif_grid, "通知対象イベント", events_widget)

    notif_card = Card(
        title="通知チャネル",
        header_right=make_hint("注文、エラー、再接続、停止理由の通知先"),
    )
    notif_card.addBodyWidget(notif_body)
    layout.addWidget(notif_card)

    # ---- Card 4: GMO 接続確認 --------------------------------------------
    gmo_chip = Chip("未テスト", "neutral")
    test_btn = QPushButton("接続テスト")
    test_btn.setProperty("variant", "ghost")
    gmo_head_right = QWidget()
    head_row = QHBoxLayout(gmo_head_right)
    head_row.setContentsMargins(0, 0, 0, 0)
    head_row.setSpacing(8)
    head_row.addWidget(gmo_chip)
    head_row.addWidget(test_btn)

    api_public = QLineEdit()
    api_public.setEchoMode(QLineEdit.Password)
    api_public.setReadOnly(True)
    api_private = QLineEdit()
    api_private.setEchoMode(QLineEdit.Password)
    api_private.setReadOnly(True)
    api_private.setPlaceholderText("未設定")
    storage_edit = QLineEdit()
    storage_edit.setReadOnly(True)

    gmo_left_body, gmo_left_grid = make_form_grid()
    add_row(gmo_left_grid, "API キー (public)", api_public)
    add_row(gmo_left_grid, "API キー (private)", api_private)
    add_row(gmo_left_grid, "保存先", storage_edit)

    gmo_right_body = QWidget()
    gmo_right_grid = QGridLayout(gmo_right_body)
    gmo_right_grid.setContentsMargins(0, 0, 0, 0)
    gmo_right_grid.setHorizontalSpacing(14)
    gmo_right_grid.setVerticalSpacing(12)
    for column in range(2):
        gmo_right_grid.setColumnStretch(column, 1)
    last_test_cell, last_test_value = make_detail_cell("最終テスト", "-")
    latency_cell, latency_value = make_detail_cell("レイテンシ", "-")
    rate_cell, rate_value = make_detail_cell("レート残量", "-")
    account_cell, account_value = make_detail_cell("アカウント", "-", mono=False)
    gmo_right_grid.addWidget(last_test_cell, 0, 0)
    gmo_right_grid.addWidget(latency_cell, 0, 1)
    gmo_right_grid.addWidget(rate_cell, 1, 0)
    gmo_right_grid.addWidget(account_cell, 1, 1)

    gmo_row = QHBoxLayout()
    gmo_row.setSpacing(16)
    gmo_row.addWidget(gmo_left_body, 1)
    gmo_row.addWidget(gmo_right_body, 1)
    gmo_wrap = QWidget()
    gmo_wrap.setLayout(gmo_row)

    gmo_card = Card(title="GMO 接続確認", header_right=gmo_head_right)
    gmo_card.addBodyWidget(gmo_wrap)
    layout.addWidget(gmo_card)

    # ---- Card 5: 現在の設定スナップショット ------------------------------
    snapshot_view = QPlainTextEdit()
    snapshot_view.setReadOnly(True)
    snapshot_view.setProperty("role", "yaml-pre")
    snapshot_view.setFrameShape(QFrame.NoFrame)
    snapshot_view.setMinimumHeight(240)
    snapshot_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.MinimumExpanding)
    snap_card = Card(
        title="現在の設定スナップショット",
        header_right=make_hint("書き出し用"),
    )
    snap_card.addBodyWidget(snapshot_view)
    layout.addWidget(snap_card)

    layout.addStretch(1)

    # ---- Shared helpers ---------------------------------------------------

    qsettings = QSettings("FXAutoTradeLab", "Desktop")

    def _render_snapshot() -> None:
        payload = _config_to_dict(app_state.config)
        try:
            text = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
        except Exception:  # noqa: BLE001
            text = repr(payload)
        snapshot_view.setPlainText(text)

    def _credential_storage_label() -> str:
        if sys.platform == "darwin":
            return "macOS キーチェーン"
        if sys.platform.startswith("win"):
            return "Windows 資格情報マネージャ"
        return "Secret Service (os credential store)"

    def _bind_credentials() -> None:
        try:
            statuses = app_state.credential_statuses()
        except Exception:  # noqa: BLE001
            statuses = {}
        private_status = statuses.get("private", {}) if statuses else {}
        try:
            private_values = app_state.load_credential_values("private")
        except Exception:  # noqa: BLE001
            private_values = {}
        api_public.setText("public API は認証不要")
        masked = str(private_values.get("api_key_masked") or "")
        if private_status.get("configured") and masked:
            api_private.setText(masked)
        else:
            api_private.clear()
        storage_edit.setText(_credential_storage_label())

    def _rebind_from_config() -> None:
        cfg = app_state.config
        mode_seg.setCurrentData(cfg.broker.mode.value)
        source_seg.setCurrentData(cfg.data.source)
        try:
            entry_tf_seg.setCurrentData(cfg.strategy.entry_timeframe)
        except Exception:  # noqa: BLE001
            pass
        default_page_value = getattr(getattr(cfg, "ui", None), "default_page", None)
        if default_page_value in PAGE_LABELS:
            default_page_combo.setCurrentText(default_page_value)

        risk = cfg.risk
        starting_cash.set_int(int(getattr(risk, "starting_cash", 0) or 0))
        size_mode_seg.setCurrentData(risk.order_size_mode.value)
        eq_frac.set_float(float(getattr(risk, "equity_fraction_per_trade", 0.0) or 0.0) * 100, "{:.1f}")
        daily_loss_jpy.set_int(int(getattr(risk, "max_daily_loss_amount", 0) or 0))
        daily_loss_pct.set_float(float(getattr(risk, "max_daily_loss_pct", 0.0) or 0.0) * 100, "{:.1f}")
        mp = getattr(risk, "max_positions", None)
        if mp is None:
            mp = qsettings.value("settings/max_positions")
        try:
            max_positions.set_int(int(mp)) if mp is not None else max_positions.set_int(0)
        except (TypeError, ValueError):
            max_positions.set_int(0)

        channels = set(cfg.automation.notification_channels.channels or [])
        log_path_edit.setText(str(cfg.automation.notification_channels.log_path or ""))
        slack_edit.setText(str(cfg.automation.notification_channels.webhook_url or ""))

        stored_level = qsettings.value("settings/notification_level")
        if stored_level in LEVEL_KEYS:
            level_seg.setCurrentData(stored_level)
        else:
            level_seg.setCurrentData("info")

        def _load_flag(key: str, default: bool) -> bool:
            value = qsettings.value(f"settings/notifications/{key}")
            if value is None:
                return default
            if isinstance(value, str):
                return value.lower() in {"true", "1", "yes"}
            return bool(value)

        notify_new_order.setChecked(_load_flag("new_order", True))
        notify_fill.setChecked(_load_flag("fill", True))
        notify_kill.setChecked(_load_flag("kill_switch", True))
        notify_reconnect.setChecked(_load_flag("reconnect", "webhook" in channels))
        notify_daily_pnl.setChecked(_load_flag("daily_pnl", False))

        _bind_credentials()
        _render_snapshot()

    def _apply_ui_to_config() -> None:
        cfg = app_state.config
        # Runtime mode
        app_state.update_runtime_mode(
            broker_mode=str(mode_seg.currentData() or "local_sim"),
            data_source=str(source_seg.currentData() or "csv"),
            stream_enabled=bool(getattr(cfg.data, "stream_enabled", False)),
        )
        # Entry timeframe
        tf_value = entry_tf_seg.currentData()
        if tf_value is not None and hasattr(cfg.strategy, "entry_timeframe"):
            try:
                cfg.strategy.entry_timeframe = tf_value
            except Exception:  # noqa: BLE001
                pass
        # Default page
        selected_page = default_page_combo.currentText()
        ui_cfg = getattr(cfg, "ui", None)
        if ui_cfg is not None and hasattr(ui_cfg, "default_page"):
            try:
                ui_cfg.default_page = selected_page
            except Exception:  # noqa: BLE001
                pass
        # Cash / sizing
        app_state.update_account_settings(starting_cash=starting_cash.value_float())
        app_state.update_order_sizing(
            order_size_mode=str(size_mode_seg.currentData() or "fixed_amount"),
            fixed_order_amount=float(getattr(cfg.risk, "fixed_order_amount", 0.0) or 0.0),
            equity_fraction_per_trade=eq_frac.value_float() / 100.0,
            risk_per_trade=float(getattr(cfg.risk, "risk_per_trade", 0.0) or 0.0),
        )
        # Daily loss limits
        if hasattr(cfg.risk, "max_daily_loss_amount"):
            try:
                cfg.risk.max_daily_loss_amount = daily_loss_jpy.value_float()
            except Exception:  # noqa: BLE001
                pass
        if hasattr(cfg.risk, "max_daily_loss_pct"):
            try:
                cfg.risk.max_daily_loss_pct = daily_loss_pct.value_float() / 100.0
            except Exception:  # noqa: BLE001
                pass
        # Max positions (schema may not have it; store in QSettings as fallback)
        if hasattr(cfg.risk, "max_positions"):
            try:
                cfg.risk.max_positions = max_positions.value_int()
            except Exception:  # noqa: BLE001
                qsettings.setValue("settings/max_positions", max_positions.value_int())
        else:
            qsettings.setValue("settings/max_positions", max_positions.value_int())
        # Notifications — channels derived from UI state
        channels: list[str] = []
        if any(
            box.isChecked()
            for box in (notify_new_order, notify_fill, notify_kill, notify_reconnect, notify_daily_pnl)
        ):
            channels.append("log")
        webhook = slack_edit.text().strip()
        if webhook:
            channels.append("webhook")
        app_state.update_notification_settings(
            enabled=bool(channels),
            channels=channels,
            sound_name=str(getattr(cfg.automation.notification_channels, "sound_name", "") or ""),
            webhook_url=webhook or str(getattr(cfg.automation.notification_channels, "webhook_url", "") or ""),
        )
        # Persist UI-only notification preferences
        qsettings.setValue("settings/notification_level", str(level_seg.currentData() or "info"))
        qsettings.setValue("settings/notifications/new_order", notify_new_order.isChecked())
        qsettings.setValue("settings/notifications/fill", notify_fill.isChecked())
        qsettings.setValue("settings/notifications/kill_switch", notify_kill.isChecked())
        qsettings.setValue("settings/notifications/reconnect", notify_reconnect.isChecked())
        qsettings.setValue("settings/notifications/daily_pnl", notify_daily_pnl.isChecked())

    # ---- Buttons ----------------------------------------------------------

    def _open_log_file() -> None:
        raw = log_path_edit.text().strip()
        if not raw:
            log_message("ログファイルパスが未設定です。")
            return
        path = Path(raw).expanduser()
        if path.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))
        else:
            QMessageBox.information(page, "ログファイル", f"ファイルが見つかりません: {path}")

    def _on_test_finished(result) -> None:  # noqa: ANN001
        page.last_test_result = result
        set_button_enabled(test_btn, True)
        test_btn.setText("接続テスト")
        last_test_value.setText(datetime.now().strftime("%m/%d %H:%M"))
        market_ok = bool(result.get("market_data_ok"))
        gmo_chip.set_tone("running" if market_ok else "warn")
        gmo_chip.set_text("接続済み" if market_ok else "要確認")
        latency_ms = result.get("latency_ms")
        if latency_ms is not None:
            latency_value.setText(f"{float(latency_ms):.0f} ms")
        else:
            latency_value.setText("-")
        rate_remaining = result.get("rate_remaining")
        rate_limit = result.get("rate_limit")
        if rate_remaining is not None and rate_limit is not None:
            rate_value.setText(f"{rate_remaining} / {rate_limit}")
        else:
            tickers = result.get("ticker_count")
            if tickers is not None:
                rate_value.setText(f"ティッカー {tickers}")
            else:
                rate_value.setText("-")
        account_value.setText(
            "利用可能" if market_ok else str(result.get("error") or "要確認")
        )
        log_message("GMO 接続テストが完了しました。")

    def _on_test_error(message: str) -> None:
        page.last_test_result = {"error": message}
        set_button_enabled(test_btn, True)
        test_btn.setText("接続テスト")
        gmo_chip.set_tone("neg")
        gmo_chip.set_text("失敗")
        last_test_value.setText(datetime.now().strftime("%m/%d %H:%M"))
        latency_value.setText("-")
        rate_value.setText("-")
        account_value.setText(message)
        log_message(f"GMO 接続テスト失敗: {message}")

    def _run_connection_test() -> None:
        set_button_enabled(test_btn, False, busy=True)
        test_btn.setText("テスト中...")
        gmo_chip.set_tone("info")
        gmo_chip.set_text("確認中")
        submit_task(app_state.test_gmo_connection, _on_test_finished, _on_test_error)

    def _save() -> None:
        try:
            _apply_ui_to_config()
            if hasattr(app_state, "save_config"):
                app_state.save_config()
            _render_snapshot()
            log_message("設定を保存しました。")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(page, "設定", f"保存に失敗しました: {exc}")

    def _reset() -> None:
        answer = QMessageBox.question(
            page,
            "設定",
            "UI の編集を破棄して、保存済み設定を読み直しますか？",
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        for attr in ("reload_config", "reset_config", "reload"):
            fn = getattr(app_state, attr, None)
            if callable(fn):
                try:
                    fn()
                except Exception:  # noqa: BLE001
                    pass
                break
        _rebind_from_config()
        log_message("設定を読み直しました。")

    log_open_btn.clicked.connect(_open_log_file)
    test_btn.clicked.connect(_run_connection_test)
    save_btn.clicked.connect(_save)
    reset_btn.clicked.connect(_reset)

    page.refresh = _rebind_from_config
    _rebind_from_config()
    return page
