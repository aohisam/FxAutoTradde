"""Overview page."""

from __future__ import annotations


def build_overview_page(app_state):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QFrame, QGridLayout, QLabel, QVBoxLayout, QWidget

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("概要")
    title.setStyleSheet("font-size: 24px; font-weight: 700;")
    layout.addWidget(title)

    cards = QGridLayout()
    labels = {
        "mode": "現在のモード",
        "automation": "自動売買状態",
        "market": "市場ステータス",
        "sync": "最終同期時刻",
        "strategy": "選択中の戦略",
        "equity": "総損益",
        "connection": "接続状態",
        "runs": "保存済み実行数",
    }
    page.kpi_labels = {}
    row = 0
    col = 0
    for key, label in labels.items():
        card = QFrame()
        card.setObjectName("kpiCard")
        card.setStyleSheet(
            "QFrame#kpiCard { background: white; border: 1px solid #dbe3ee; border-radius: 14px; padding: 12px; }"
        )
        card_layout = QVBoxLayout(card)
        label_widget = QLabel(label)
        label_widget.setStyleSheet("border: none; background: transparent; color: #475569;")
        card_layout.addWidget(label_widget)
        value = QLabel("-")
        value.setStyleSheet("font-size: 18px; font-weight: 600; border: none; background: transparent;")
        card_layout.addWidget(value)
        page.kpi_labels[key] = value
        cards.addWidget(card, row, col)
        col += 1
        if col == 3:
            row += 1
            col = 0
    layout.addLayout(cards)
    summary = QLabel("最新サマリーはここに表示されます。")
    summary.setWordWrap(True)
    summary.setStyleSheet("padding: 12px; background: #f3f6fa; border-radius: 12px;")
    page.summary_label = summary
    layout.addWidget(summary)
    layout.addStretch(1)

    def refresh() -> None:
        private_configured = bool(getattr(app_state.env, "has_credentials", lambda _profile: False)("private"))
        connection_text = "GMO public API / 認証不要"
        if private_configured:
            connection_text = "GMO public API / private API キー設定済み"
        page.kpi_labels["mode"].setText(
            {
                "local_sim": "ローカルシミュレーション",
                "gmo_sim": "GMO 実時間シミュレーション",
            }.get(app_state.config.broker.mode.value, app_state.config.broker.mode.value)
        )
        page.kpi_labels["automation"].setText("自動売買稼働中" if app_state.automation_controller else "停止")
        page.kpi_labels["market"].setText(
            {
                "gmo": "GMO 実時間データ",
                "csv": "JForex CSV キャッシュ",
                "fixture": "fixture 検証データ",
            }.get(app_state.config.data.source, app_state.config.data.source)
        )
        page.kpi_labels["sync"].setText(app_state.config.data.end_date)
        page.kpi_labels["strategy"].setText(app_state.config.strategy.name)
        page.kpi_labels["connection"].setText(connection_text)
        page.kpi_labels["runs"].setText(str(len(app_state.list_runs())))
        if app_state.last_result is None:
            page.kpi_labels["equity"].setText("-")
            page.summary_label.setText("まだバックテストまたはデモ実行はありません。")
        else:
            page.kpi_labels["equity"].setText(f"{app_state.last_result.metrics.get('total_return', 0):.2%}")
            page.summary_label.setText(
                f"最新実行ID: {app_state.last_result.run_id}\n"
                f"最大ドローダウン: {app_state.last_result.metrics.get('max_drawdown', 0):.2%}\n"
                f"取引回数: {app_state.last_result.metrics.get('number_of_trades', 0)}"
            )

    page.refresh = refresh
    return page
