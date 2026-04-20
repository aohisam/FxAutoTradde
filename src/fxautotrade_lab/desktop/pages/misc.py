"""Additional desktop pages."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import yaml


def _optional_web_view():  # pragma: no cover - UI helper
    try:
        if os.environ.get("QT_QPA_PLATFORM") == "offscreen":
            raise ImportError("offscreen mode")
        if getattr(sys, "frozen", False):
            raise ImportError("qtwebengine disabled in packaged app")
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except ImportError:
        return None
    return QWebEngineView





def build_settings_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMessageBox,
        QPushButton,
        QScrollArea,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.ml_labels import ML_MODE_CHOICES, ml_mode_description, ml_mode_label
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip

    def _set_combo_value(combo: QComboBox, value: str) -> None:
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def credential_source_text(source: str) -> str:
        return {
            "public_api": "public API",
            "keychain": "macOS キーチェーン",
            "env": ".env",
            "unset": "未設定",
        }.get(source, source)

    def number_input(placeholder: str) -> QLineEdit:
        editor = QLineEdit()
        editor.setPlaceholderText(placeholder)
        editor.setClearButtonEnabled(True)
        editor.setAlignment(Qt.AlignRight)
        editor.setProperty("align", "num")
        return editor

    def format_number(value: float, decimals: int = 2) -> str:
        text = f"{float(value):,.{decimals}f}"
        if decimals and "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    def parse_number_input(editor: QLineEdit, default: float) -> float:
        text_value = editor.text().strip()
        if not text_value:
            return default
        return float(text_value.replace(",", "").replace("JPY", "").strip())

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    page.last_test_result = None
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    # Header
    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("設定")
    title.setProperty("role", "h1")
    subtitle = QLabel("運用モード / 資金管理 / ML / 通知 / GMO 接続")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    # Banner
    banner = Card(sunken=True)
    helper = QLabel(
        "FX 版では JForex CSV の履歴データと GMO の実時間データを切り替えて運用します。"
        " 現在の自動売買はすべてローカル約定で、バックテストとフォワード検証はローカルで完結します。"
    )
    helper.setWordWrap(True)
    helper.setProperty("role", "muted")
    banner.addBodyWidget(helper)
    warning = QLabel()
    warning.setWordWrap(True)
    warning.setProperty("tone", "warn")
    banner.addBodyWidget(warning)
    layout.addWidget(banner)

    def refresh_all_pages() -> None:
        window = page.window()
        if hasattr(window, "refresh_all_pages"):
            window.refresh_all_pages()
        else:
            refresh()

    # Runtime mode card
    mode_card = Card(
        title="運用モード",
        subtitle="発注はすべてローカルでシミュレーションします",
    )
    mode_combo = QComboBox()
    mode_combo.addItem("ローカルシミュレーション", "local_sim")
    mode_combo.addItem("GMO 実時間シミュレーション", "gmo_sim")
    source_combo = QComboBox()
    source_combo.addItem("JForex CSV キャッシュ", "csv")
    source_combo.addItem("GMO 実時間データ", "gmo")
    source_combo.addItem("fixture / デモ履歴", "fixture")
    stream_box = QCheckBox("実時間更新を有効化")
    mode_status = QLabel()
    mode_status.setWordWrap(True)
    mode_status.setProperty("role", "muted")
    mode_form = QGridLayout()
    mode_form.setHorizontalSpacing(14)
    mode_form.setVerticalSpacing(8)
    label_mode = QLabel("運用モード")
    label_mode.setProperty("role", "muted2")
    label_source = QLabel("市場データ")
    label_source.setProperty("role", "muted2")
    mode_form.addWidget(label_mode, 0, 0)
    mode_form.addWidget(label_source, 0, 1)
    mode_form.addWidget(mode_combo, 1, 0)
    mode_form.addWidget(source_combo, 1, 1)
    mode_form.setColumnStretch(0, 1)
    mode_form.setColumnStretch(1, 1)
    mode_card.addBodyLayout(mode_form)
    mode_card.addBodyWidget(stream_box)
    mode_card.addBodyWidget(mode_status)
    mode_button_row = QHBoxLayout()
    mode_button_row.addStretch(1)
    save_mode_button = QPushButton("運用モードを保存")
    save_mode_button.setProperty("variant", "primary")
    mode_button_row.addWidget(save_mode_button)
    mode_card.addBodyLayout(mode_button_row)
    layout.addWidget(mode_card)

    # Sizing card
    sizing_card = Card(title="資金 / 注文サイズ", subtitle="JPY 建ての資金量とリスク")
    sizing_combo = QComboBox()
    sizing_combo.addItem("定額", "fixed_amount")
    sizing_combo.addItem("資産比率", "equity_fraction")
    sizing_combo.addItem("リスク率", "risk_based")
    fixed_amount_input = number_input("例: 300000")
    equity_fraction_input = number_input("例: 0.10")
    risk_fraction_input = number_input("例: 0.01")
    starting_cash_input = number_input("例: 5000000")
    sizing_status = QLabel()
    sizing_status.setWordWrap(True)
    sizing_status.setProperty("role", "muted")
    sizing_form = QGridLayout()
    sizing_form.setHorizontalSpacing(14)
    sizing_form.setVerticalSpacing(8)
    labels = [
        ("初期資産 (JPY)", 0, 0),
        ("数量モード", 0, 1),
        ("定額 (JPY)", 2, 0),
        ("資産比率", 2, 1),
        ("リスク率", 4, 0),
    ]
    for text, row, column in labels:
        lbl = QLabel(text)
        lbl.setProperty("role", "muted2")
        sizing_form.addWidget(lbl, row, column)
    sizing_form.addWidget(starting_cash_input, 1, 0)
    sizing_form.addWidget(sizing_combo, 1, 1)
    sizing_form.addWidget(fixed_amount_input, 3, 0)
    sizing_form.addWidget(equity_fraction_input, 3, 1)
    sizing_form.addWidget(risk_fraction_input, 5, 0, 1, 2)
    sizing_form.setColumnStretch(0, 1)
    sizing_form.setColumnStretch(1, 1)
    sizing_card.addBodyLayout(sizing_form)
    sizing_card.addBodyWidget(sizing_status)
    sizing_button_row = QHBoxLayout()
    sizing_button_row.addStretch(1)
    save_sizing_button = QPushButton("資金 / 注文サイズを保存")
    save_sizing_button.setProperty("variant", "primary")
    sizing_button_row.addWidget(save_sizing_button)
    sizing_card.addBodyLayout(sizing_button_row)
    layout.addWidget(sizing_card)

    # ML card
    ml_card = Card(title="ML 参加フィルタ", subtitle="FX breakout 戦略で使う ML 設定")
    ml_enabled_box = QCheckBox("ML 参加フィルタを有効化")
    ml_mode_combo = QComboBox()
    for key, label in ML_MODE_CHOICES:
        ml_mode_combo.addItem(label, key)
    ml_mode_status = QLabel()
    ml_mode_status.setWordWrap(True)
    ml_mode_status.setProperty("role", "muted")
    ml_model_status = QLabel()
    ml_model_status.setWordWrap(True)
    ml_model_status.setProperty("role", "muted")
    ml_model_status.setTextInteractionFlags(Qt.TextSelectableByMouse)
    ml_form = QGridLayout()
    ml_form.setHorizontalSpacing(14)
    ml_form.setVerticalSpacing(8)
    ml_enabled_label = QLabel("ML 有効化")
    ml_enabled_label.setProperty("role", "muted2")
    ml_mode_label_widget = QLabel("バックテストでの使い方")
    ml_mode_label_widget.setProperty("role", "muted2")
    ml_form.addWidget(ml_enabled_label, 0, 0)
    ml_form.addWidget(ml_mode_label_widget, 0, 1)
    ml_form.addWidget(ml_enabled_box, 1, 0)
    ml_form.addWidget(ml_mode_combo, 1, 1)
    ml_form.setColumnStretch(0, 1)
    ml_form.setColumnStretch(1, 1)
    ml_card.addBodyLayout(ml_form)
    ml_card.addBodyWidget(ml_mode_status)
    ml_card.addBodyWidget(ml_model_status)
    ml_button_row = QHBoxLayout()
    ml_button_row.addStretch(1)
    save_ml_button = QPushButton("ML 設定を保存")
    save_ml_button.setProperty("variant", "primary")
    ml_button_row.addWidget(save_ml_button)
    ml_card.addBodyLayout(ml_button_row)
    layout.addWidget(ml_card)

    # Notifications card
    notifications_card = Card(
        title="通知チャネル",
        subtitle="注文・エラー・再接続・停止理由の通知先",
    )
    notify_enabled = QCheckBox("通知を有効化")
    desktop_box = QCheckBox("デスクトップ通知")
    sound_box = QCheckBox("サウンド")
    log_box = QCheckBox("ログ保存")
    webhook_box = QCheckBox("Webhook")
    sound_name = QLineEdit()
    sound_name.setClearButtonEnabled(True)
    webhook_url = QLineEdit()
    webhook_url.setClearButtonEnabled(True)
    webhook_url.setEchoMode(QLineEdit.Password)
    log_path_label = QLabel()
    log_path_label.setWordWrap(True)
    log_path_label.setProperty("role", "muted")
    channels_row = QHBoxLayout()
    for widget in (desktop_box, sound_box, log_box, webhook_box):
        channels_row.addWidget(widget)
    channels_row.addStretch(1)
    notifications_card.addBodyWidget(notify_enabled)
    label_channels = QLabel("チャネル")
    label_channels.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_channels)
    notifications_card.addBodyLayout(channels_row)
    label_sound = QLabel("サウンド名")
    label_sound.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_sound)
    notifications_card.addBodyWidget(sound_name)
    label_webhook = QLabel("Webhook URL")
    label_webhook.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_webhook)
    notifications_card.addBodyWidget(webhook_url)
    label_logpath = QLabel("ログ出力先")
    label_logpath.setProperty("role", "muted2")
    notifications_card.addBodyWidget(label_logpath)
    notifications_card.addBodyWidget(log_path_label)
    notif_button_row = QHBoxLayout()
    notif_button_row.addStretch(1)
    save_notifications_button = QPushButton("通知設定を保存")
    save_notifications_button.setProperty("variant", "primary")
    notif_button_row.addWidget(save_notifications_button)
    notifications_card.addBodyLayout(notif_button_row)
    layout.addWidget(notifications_card)

    # Connection card
    conn_chip = Chip("接続テスト未実行", "neutral")
    connection_card = Card(
        title="GMO 接続確認",
        subtitle="public API の疎通確認 / private API キー管理",
        header_right=conn_chip,
    )
    api_key_input = QLineEdit()
    api_key_input.setPlaceholderText("GMO private API Key")
    api_key_input.setClearButtonEnabled(True)
    api_secret_input = QLineEdit()
    api_secret_input.setPlaceholderText("GMO private API Secret")
    api_secret_input.setClearButtonEnabled(True)
    api_secret_input.setEchoMode(QLineEdit.Password)
    credential_status = QLabel()
    credential_status.setWordWrap(True)
    credential_status.setProperty("role", "muted")
    connection_status = QLabel("接続テストは未実行です。")
    connection_status.setWordWrap(True)
    connection_status.setProperty("role", "muted")
    credential_form = QGridLayout()
    credential_form.setHorizontalSpacing(14)
    credential_form.setVerticalSpacing(8)
    label_key = QLabel("private API Key")
    label_key.setProperty("role", "muted2")
    label_secret = QLabel("private API Secret")
    label_secret.setProperty("role", "muted2")
    credential_form.addWidget(label_key, 0, 0)
    credential_form.addWidget(label_secret, 0, 1)
    credential_form.addWidget(api_key_input, 1, 0)
    credential_form.addWidget(api_secret_input, 1, 1)
    credential_form.setColumnStretch(0, 1)
    credential_form.setColumnStretch(1, 1)
    connection_card.addBodyLayout(credential_form)
    connection_card.addBodyWidget(credential_status)
    connection_card.addBodyWidget(connection_status)
    connection_buttons = QHBoxLayout()
    save_credentials_button = QPushButton("private API を保存")
    save_credentials_button.setProperty("variant", "primary")
    clear_credentials_button = QPushButton("保存済みキーを削除")
    clear_credentials_button.setProperty("variant", "ghost")
    test_connection_button = QPushButton("GMO 接続テスト")
    test_connection_button.setProperty("variant", "ghost")
    connection_buttons.addStretch(1)
    connection_buttons.addWidget(clear_credentials_button)
    connection_buttons.addWidget(test_connection_button)
    connection_buttons.addWidget(save_credentials_button)
    connection_card.addBodyLayout(connection_buttons)

    # Test result area inline under connection card
    test_output = QTextEdit()
    test_output.setReadOnly(True)
    test_output.setMinimumHeight(180)
    test_output.setProperty("role", "mono")
    connection_card.addBodyWidget(test_output)
    layout.addWidget(connection_card)

    # Summary + config snapshot card
    summary_card = Card(title="現在の設定スナップショット", subtitle="YAML 形式の全量表示")
    summary_label = QLabel()
    summary_label.setWordWrap(True)
    summary_label.setProperty("role", "muted")
    summary_card.addBodyWidget(summary_label)
    config_text = QTextEdit()
    config_text.setReadOnly(True)
    config_text.setMinimumHeight(260)
    config_text.setProperty("role", "mono")
    summary_card.addBodyWidget(config_text)
    layout.addWidget(summary_card)

    layout.addStretch(1)

    def update_mode_status() -> None:
        selected_mode = str(mode_combo.currentData() or "local_sim")
        selected_source = str(source_combo.currentData() or "csv")
        if selected_mode == "gmo_sim":
            _set_combo_value(source_combo, "gmo")
            source_combo.setEnabled(False)
            stream_box.setEnabled(True)
            mode_status.setText(
                "GMO 実時間シミュレーション: 市場データは GMO public API に固定されます。"
                " 発注はすべてローカル約定です。"
            )
            return
        source_combo.setEnabled(True)
        stream_allowed = selected_source == "gmo"
        stream_box.setEnabled(stream_allowed)
        if not stream_allowed:
            stream_box.setChecked(False)
        source_text = {
            "csv": "JForex CSV から作成した複数時間足キャッシュを使うオフライン検証です。",
            "gmo": "GMO の価格を取得しつつ、注文はローカルで約定させます。",
            "fixture": "fixture の生成データを使う軽量な検証モードです。",
        }.get(selected_source, selected_source)
        mode_status.setText(
            f"ローカルシミュレーション: {source_text}"
            f"  実時間更新: {'有効化できます' if stream_allowed else 'このソースでは無効です'}"
        )

    def save_runtime_mode() -> None:
        selected_mode = str(mode_combo.currentData() or "local_sim")
        selected_source = str(source_combo.currentData() or "csv")
        try:
            app_state.update_runtime_mode(
                broker_mode=selected_mode,
                data_source=selected_source,
                stream_enabled=stream_box.isChecked(),
            )
        except Exception as exc:  # pragma: no cover - config/runtime guard
            QMessageBox.critical(page, "エラー", f"運用モードの保存に失敗しました。\n{exc}")
            return
        mode_label = {
            "local_sim": "ローカルシミュレーション",
            "gmo_sim": "GMO 実時間シミュレーション",
        }.get(selected_mode, selected_mode)
        QMessageBox.information(page, "完了", f"運用モードを保存しました。\n{mode_label}")
        log_message(f"運用モードを保存しました: {selected_mode} / {app_state.config.data.source}")
        refresh_all_pages()

    def update_sizing_status() -> None:
        selected_mode = str(sizing_combo.currentData() or "fixed_amount")
        fixed_amount_input.setEnabled(selected_mode == "fixed_amount")
        equity_fraction_input.setEnabled(selected_mode == "equity_fraction")
        risk_fraction_input.setEnabled(selected_mode == "risk_based")
        if selected_mode == "fixed_amount":
            sizing_status.setText(
                "定額モード: 指定した JPY 金額に近い数量を計算します。"
                f" 最小数量 {app_state.config.risk.minimum_order_quantity:,} / "
                f"数量ステップ {app_state.config.risk.quantity_step:,} に合わせて丸めます。"
            )
            return
        if selected_mode == "equity_fraction":
            sizing_status.setText(
                "資産比率モード: 現在資産の一定割合を 1 回の新規エントリーに使います。"
                " 例: 0.10 なら約 10% です。"
            )
            return
        sizing_status.setText(
            "リスク率モード: ATR ベースのストップ距離と許容損失率から数量を計算します。"
        )

    def update_ml_status() -> None:
        selected_mode = str(ml_mode_combo.currentData() or "rule_only")
        ml_mode_status.setText(ml_mode_description(selected_mode))
        model_status = app_state.model_status()
        ml_model_status.setText(
            "\n".join(
                [
                    f"現在の設定: {'有効' if ml_enabled_box.isChecked() else '無効'}",
                    f"モード: {ml_mode_label(selected_mode)}",
                    f"モデル保存先: {model_status.get('model_path', '-')}",
                    f"保存済みモデル: {'あり' if model_status.get('exists') else 'なし'}",
                    "学習実行はバックテスト画面の「FX ML 学習」から行えます。",
                ]
            )
        )

    def save_order_sizing() -> None:
        try:
            app_state.update_account_settings(
                starting_cash=parse_number_input(starting_cash_input, app_state.config.risk.starting_cash)
            )
            app_state.update_order_sizing(
                order_size_mode=str(sizing_combo.currentData() or "fixed_amount"),
                fixed_order_amount=parse_number_input(
                    fixed_amount_input,
                    app_state.config.risk.fixed_order_amount,
                ),
                equity_fraction_per_trade=parse_number_input(
                    equity_fraction_input,
                    app_state.config.risk.equity_fraction_per_trade,
                ),
                risk_per_trade=parse_number_input(
                    risk_fraction_input,
                    app_state.config.risk.risk_per_trade,
                ),
            )
        except Exception as exc:  # pragma: no cover - UI validation
            QMessageBox.critical(page, "エラー", f"資金 / 注文サイズ設定の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "資金 / 注文サイズ設定を保存しました。")
        log_message(
            "資金 / 注文サイズ設定を保存しました: "
            f"{app_state.config.risk.starting_cash:,.0f} {app_state.config.risk.account_currency} / "
            f"{app_state.config.risk.order_size_mode.value}"
        )
        refresh_all_pages()

    def save_ml_settings() -> None:
        try:
            app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled = ml_enabled_box.isChecked()
            app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode = str(
                ml_mode_combo.currentData() or "rule_only"
            )
            app_state.save_config()
        except Exception as exc:  # pragma: no cover - config persistence
            QMessageBox.critical(page, "エラー", f"ML 設定の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "ML 設定を保存しました。")
        log_message(
            "ML 設定を保存しました: "
            f"{'有効' if app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled else '無効'} / "
            f"{ml_mode_label(app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)}"
        )
        refresh_all_pages()

    def save_private_credentials() -> None:
        try:
            values = app_state.save_gmo_credentials(
                "private",
                api_key_input.text(),
                api_secret_input.text(),
            )
        except Exception as exc:  # pragma: no cover - OS credential store
            QMessageBox.critical(page, "エラー", f"GMO private API の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "GMO private API を macOS キーチェーンへ保存しました。")
        log_message(f"GMO private API を保存しました: {credential_source_text(str(values.get('source', 'keychain')))}")
        refresh_all_pages()

    def clear_private_credentials() -> None:
        answer = QMessageBox.question(
            page,
            "確認",
            "macOS キーチェーンに保存済みの GMO private API 資格情報を削除します。よろしいですか？",
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        try:
            deleted = app_state.delete_gmo_credentials("private")
        except Exception as exc:  # pragma: no cover - OS credential store
            QMessageBox.critical(page, "エラー", f"GMO private API の削除に失敗しました。\n{exc}")
            return
        if deleted:
            QMessageBox.information(page, "完了", "GMO private API を macOS キーチェーンから削除しました。")
            log_message("GMO private API を削除しました。")
        else:
            QMessageBox.information(page, "情報", "削除対象の GMO private API は見つかりませんでした。")
        refresh_all_pages()

    def save_notifications() -> None:
        channels = []
        if desktop_box.isChecked():
            channels.append("desktop")
        if sound_box.isChecked():
            channels.append("sound")
        if log_box.isChecked():
            channels.append("log")
        if webhook_box.isChecked():
            channels.append("webhook")
        try:
            app_state.update_notification_settings(
                enabled=notify_enabled.isChecked(),
                channels=channels,
                sound_name=sound_name.text(),
                webhook_url=webhook_url.text(),
            )
        except Exception as exc:  # pragma: no cover - file system path
            QMessageBox.critical(page, "エラー", f"通知設定の保存に失敗しました。\n{exc}")
            return
        QMessageBox.information(page, "完了", "通知設定を保存しました。")
        log_message("通知設定を保存しました。")
        refresh_all_pages()

    def set_test_busy(is_busy: bool) -> None:
        test_connection_button.setText("GMO 接続確認中..." if is_busy else "GMO 接続テスト")
        set_button_enabled(test_connection_button, not is_busy, busy=is_busy)
        if is_busy:
            connection_status.setText("GMO public API の疎通確認を実行しています...")
            conn_chip.set_tone("info")
            conn_chip.set_text("確認中")

    def render_test_result(result: dict[str, object] | None = None, error: str = "") -> None:
        if error:
            page.last_test_result = {"error": error}
        elif result is not None:
            page.last_test_result = result
        record = page.last_test_result
        if not record:
            test_output.setPlainText("接続テストはまだ実行していません。")
            return
        if "error" in record:
            test_output.setPlainText(f"接続テスト失敗\n{record['error']}")
            return
        warning_lines = [f"- {item}" for item in record.get("warnings_ja", [])]
        test_output.setPlainText(
            "\n".join(
                [
                    "GMO public API 接続確認",
                    f"実行時刻: {record.get('tested_at', '-')}",
                    f"ティッカー取得: {record.get('ticker_count', 0)} 件",
                    f"通貨ペアルール取得: {record.get('symbol_count', 0)} 件",
                    f"市場データ確認: {'OK' if record.get('market_data_ok') else '要確認'}",
                    f"確認通貨ペア: {record.get('market_data_symbol', '-')}",
                    f"取得バー数: {record.get('market_data_rows', 0)}",
                    *warning_lines,
                ]
            )
        )

    def on_test_finished(result: dict[str, object]) -> None:
        set_test_busy(False)
        market_ok = "OK" if result.get("market_data_ok") else "要確認"
        connection_status.setText(
            f"GMO public API: 接続成功  •  市場データ: {market_ok}  •  確認通貨ペア: {result.get('market_data_symbol', '-')}"
        )
        conn_chip.set_tone("running")
        conn_chip.set_text("接続OK")
        render_test_result(result=result)
        log_message("GMO 接続テストが完了しました。")
        if result.get("warnings_ja"):
            QMessageBox.information(
                page,
                "接続テスト完了",
                "\n".join(["接続テストは完了しました。", *[str(item) for item in result["warnings_ja"]]]),
            )

    def on_test_error(message: str) -> None:
        set_test_busy(False)
        connection_status.setText(f"GMO 接続確認失敗\n{message}")
        conn_chip.set_tone("neg")
        conn_chip.set_text("失敗")
        render_test_result(error=message)
        QMessageBox.critical(page, "接続テスト失敗", message)
        log_message(f"GMO 接続テスト失敗: {message}")

    def run_connection_test() -> None:
        set_test_busy(True)
        submit_task(
            app_state.test_gmo_connection,
            on_test_finished,
            on_test_error,
        )

    save_notifications_button.clicked.connect(save_notifications)
    save_mode_button.clicked.connect(save_runtime_mode)
    save_sizing_button.clicked.connect(save_order_sizing)
    save_ml_button.clicked.connect(save_ml_settings)
    save_credentials_button.clicked.connect(save_private_credentials)
    clear_credentials_button.clicked.connect(clear_private_credentials)
    test_connection_button.clicked.connect(run_connection_test)
    mode_combo.currentIndexChanged.connect(lambda _: update_mode_status())
    source_combo.currentIndexChanged.connect(lambda _: update_mode_status())
    sizing_combo.currentIndexChanged.connect(lambda _: update_sizing_status())
    ml_enabled_box.toggled.connect(lambda _: update_ml_status())
    ml_mode_combo.currentIndexChanged.connect(lambda _: update_ml_status())

    def refresh() -> None:
        env = app_state.env
        credential_statuses = app_state.credential_statuses()
        private_status = credential_statuses["private"]
        private_values = app_state.load_credential_values("private")
        private_configured = bool(private_status["configured"])
        warning.setText(
            "GMO 実時間シミュレーションでは実データを取得しますが、売買はまだすべてローカル約定です。"
            if app_state.config.broker.mode.value == "gmo_sim" or app_state.config.data.source == "gmo"
            else "JForex CSV / fixture を使うローカル検証構成です。必要に応じてデータ同期ページから CSV を追加インポートしてください。"
        )
        payload = app_state.config.model_dump(mode="json")
        payload["env_status"] = {
            "gmo_public_api": "利用可",
            "gmo_private_api": "設定済み" if private_configured else "未設定",
            "gmo_private_api_source": private_status["source"],
            "live_trading_enabled": getattr(env, "live_trading_enabled", False),
            "config_path": str(app_state.config_path) if app_state.config_path is not None else "",
        }
        summary_label.setText(
            " • ".join(
                [
                    f"設定ファイル: {app_state.config_path}",
                    f"運用モード: {app_state.config.broker.mode.value}",
                    f"市場データ: {app_state.config.data.source}",
                    f"口座通貨: {app_state.config.risk.account_currency}",
                    f"初期資産: {app_state.config.risk.starting_cash:,.0f}",
                    f"監視: {len(app_state.config.watchlist.symbols)} ペア",
                ]
            )
        )
        notify_enabled.setChecked(app_state.config.automation.notifications_enabled)
        configured_channels = set(app_state.config.automation.notification_channels.channels)
        desktop_box.setChecked("desktop" in configured_channels)
        sound_box.setChecked("sound" in configured_channels)
        log_box.setChecked("log" in configured_channels)
        webhook_box.setChecked("webhook" in configured_channels)
        _set_combo_value(mode_combo, app_state.config.broker.mode.value)
        _set_combo_value(source_combo, app_state.config.data.source)
        stream_box.setChecked(bool(app_state.config.data.stream_enabled))
        update_mode_status()
        _set_combo_value(sizing_combo, app_state.config.risk.order_size_mode.value)
        starting_cash_input.setText(format_number(app_state.config.risk.starting_cash, 2))
        fixed_amount_input.setText(format_number(app_state.config.risk.fixed_order_amount, 2))
        equity_fraction_input.setText(format_number(app_state.config.risk.equity_fraction_per_trade, 4))
        risk_fraction_input.setText(format_number(app_state.config.risk.risk_per_trade, 4))
        update_sizing_status()
        ml_enabled_box.setChecked(app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled)
        _set_combo_value(ml_mode_combo, app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)
        update_ml_status()
        sound_name.setText(app_state.config.automation.notification_channels.sound_name)
        webhook_url.setText(app_state.config.automation.notification_channels.webhook_url)
        log_path_label.setText(str(app_state.config.automation.notification_channels.log_path))
        api_key_input.setText(str(private_values.get("api_key", "")))
        api_secret_input.setText(str(private_values.get("api_secret", "")))
        credential_status.setText(
            " • ".join(
                [
                    f"保存状態: {'設定済み' if private_configured else '未設定'}",
                    f"保存元: {credential_source_text(str(private_status['source']))}",
                    f"API Key: {private_values.get('api_key_masked') or '未設定'}",
                ]
            )
        )
        connection_status.setText(
            " • ".join(
                [
                    "GMO public API: 認証不要",
                    (
                        f"GMO private API: {'設定済み' if private_configured else '未設定'}"
                        f" ({credential_source_text(str(private_status['source']))})"
                    ),
                ]
            )
        )
        if page.last_test_result and "error" not in page.last_test_result:
            conn_chip.set_tone("running")
            conn_chip.set_text("接続OK")
        elif page.last_test_result:
            conn_chip.set_tone("neg")
            conn_chip.set_text("失敗")
        else:
            conn_chip.set_tone("neutral")
            conn_chip.set_text("未テスト")
        config_text.setPlainText(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False))
        render_test_result()

    page.refresh = refresh
    return page


def build_help_page():  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.widgets.card import Card

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("ヘルプ")
    title.setProperty("role", "h1")
    subtitle = QLabel("進め方・用語・トラブルシュート")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(3):
        grid.setColumnStretch(column, 1)

    start_card = Card(title="はじめる", subtitle="初回の流れ")
    steps_label = QLabel(
        "1. 監視通貨ペアで 3-8 ペアを登録\n"
        "2. データ同期ページで JForex CSV を取り込む\n"
        "3. バックテストで期間別の成績を確認\n"
        "4. 実時間シミュレーションで前向き検証"
    )
    steps_label.setWordWrap(True)
    steps_label.setProperty("role", "muted")
    start_card.addBodyWidget(steps_label)
    grid.addWidget(start_card, 0, 0)

    terms_card = Card(title="用語", subtitle="よく使う言葉")
    terms_label = QLabel(
        "• バックテスト: 過去データで戦略を検証\n"
        "• Walk-Forward: 期間を動かしながら逐次検証\n"
        "• Uplift: ML 適用前後の期待差\n"
        "• ローカル約定: UI からは実売買しません"
    )
    terms_label.setWordWrap(True)
    terms_label.setProperty("role", "muted")
    terms_card.addBodyWidget(terms_label)
    grid.addWidget(terms_card, 0, 1)

    trouble_card = Card(title="トラブル", subtitle="よくある対処")
    trouble_label = QLabel(
        "• 接続失敗: 設定 → GMO 接続テストで状態を確認\n"
        "• チャートが空: バックテストを 1 回実行するか、\n"
        "  実時間シミュレーションで通貨ペアを選び更新\n"
        "• 取込失敗: Bid / Ask の 2 ファイルを同時選択"
    )
    trouble_label.setWordWrap(True)
    trouble_label.setProperty("role", "muted")
    trouble_card.addBodyWidget(trouble_label)
    grid.addWidget(trouble_card, 0, 2)

    layout.addLayout(grid)

    shortcut_card = Card(title="ショートカット")
    shortcut_grid = QGridLayout()
    shortcut_grid.setHorizontalSpacing(18)
    shortcut_grid.setVerticalSpacing(8)
    shortcuts = [
        ("⌘R", "現在のページを再読込"),
        ("⌘⇧D", "デモ実行"),
        ("⌘L", "ログの表示切替"),
        ("⌃Tab", "次のページへ"),
        ("⌃⇧Tab", "前のページへ"),
        ("⌘F", "ページ内の検索"),
    ]
    for index, (key, description) in enumerate(shortcuts):
        key_label = QLabel(key)
        key_label.setProperty("role", "mono")
        desc_label = QLabel(description)
        desc_label.setProperty("role", "muted")
        shortcut_grid.addWidget(key_label, index // 3, (index % 3) * 2)
        shortcut_grid.addWidget(desc_label, index // 3, (index % 3) * 2 + 1)
    shortcut_card.addBodyLayout(shortcut_grid)
    layout.addWidget(shortcut_card)

    disclaimer = QLabel("本アプリは投資助言ではありません。")
    disclaimer.setProperty("role", "muted2")
    disclaimer.setWordWrap(True)
    layout.addWidget(disclaimer)

    layout.addStretch(1)
    return page
