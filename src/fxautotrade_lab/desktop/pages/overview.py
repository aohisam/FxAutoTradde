"""Overview page."""

from __future__ import annotations


def build_overview_page(app_state):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header = QHBoxLayout()
    header.setSpacing(12)
    title = QLabel("概要")
    title.setProperty("role", "h1")
    subtitle = QLabel("稼働状況、直近取引、モデル状態の一目確認")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    layout.addLayout(header)

    kpi_specs = [
        ("mode", "現在のモード", ""),
        ("automation", "自動売買状態", ""),
        ("equity", "総損益", ""),
        ("runs", "保存済み実行数", ""),
        ("market", "市場ステータス", ""),
        ("sync", "最終同期時刻", ""),
        ("strategy", "選択中の戦略", ""),
        ("connection", "接続状態", ""),
    ]
    page.kpi_labels = {}
    page.kpi_tiles: dict = {}
    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(4):
        grid.setColumnStretch(column, 1)
    for index, (key, label_text, note_text) in enumerate(kpi_specs):
        tile = KpiTile(label=label_text, value="-", note=note_text)
        grid.addWidget(tile, index // 4, index % 4)
        page.kpi_labels[key] = tile.value
        page.kpi_tiles[key] = tile
    layout.addLayout(grid)

    # Summary card + health card side by side
    split = QGridLayout()
    split.setHorizontalSpacing(14)
    split.setColumnStretch(0, 3)
    split.setColumnStretch(1, 2)

    summary_card = Card(title="最新サマリー", subtitle="バックテスト / デモ実行の結果")
    summary_label = QLabel("まだバックテストまたはデモ実行はありません。")
    summary_label.setWordWrap(True)
    summary_label.setProperty("role", "muted")
    summary_card.addBodyWidget(summary_label)
    page.summary_label = summary_label

    health_card = Card(title="システムヘルス", subtitle="主要サービスの接続状態")
    page.health_chips = {
        "gmo": Chip("GMO", "neutral"),
        "broker": Chip("ブローカー", "neutral"),
        "ml": Chip("ML", "neutral"),
        "sync": Chip("データ同期", "neutral"),
    }
    for key, caption in [("gmo", "GMO public API"), ("broker", "ブローカー接続"), ("ml", "ML モデル"), ("sync", "データ同期")]:
        row = QHBoxLayout()
        row.setSpacing(10)
        label = QLabel(caption)
        label.setProperty("role", "muted")
        row.addWidget(label)
        row.addStretch(1)
        row.addWidget(page.health_chips[key])
        health_card.addBodyLayout(row)

    split.addWidget(summary_card, 0, 0)
    split.addWidget(health_card, 0, 1)
    layout.addLayout(split)

    highlights_card = Card(title="今日の注目", subtitle="最新のトピック")
    page.highlight_label = QLabel("監視通貨ペアを設定してバックテストを回すと、ここに要約が表示されます。")
    page.highlight_label.setWordWrap(True)
    page.highlight_label.setProperty("role", "muted")
    highlights_card.addBodyWidget(page.highlight_label)
    layout.addWidget(highlights_card)

    layout.addStretch(1)

    def refresh() -> None:
        private_configured = bool(getattr(app_state.env, "has_credentials", lambda _profile: False)("private"))
        connection_text = "GMO public API / 認証不要"
        if private_configured:
            connection_text = "GMO public API / private API キー設定済み"
        page.kpi_tiles["mode"].set_value(
            {
                "local_sim": "ローカル",
                "gmo_sim": "GMO 実時間",
            }.get(app_state.config.broker.mode.value, app_state.config.broker.mode.value)
        )
        running = app_state.automation_controller is not None
        page.kpi_tiles["automation"].set_value(
            "稼働中" if running else "停止", tone="pos" if running else None
        )
        page.kpi_tiles["market"].set_value(
            {
                "gmo": "GMO 実時間",
                "csv": "JForex CSV",
                "fixture": "fixture",
            }.get(app_state.config.data.source, app_state.config.data.source)
        )
        page.kpi_tiles["sync"].set_value(app_state.config.data.end_date)
        page.kpi_tiles["strategy"].set_value(app_state.config.strategy.name)
        page.kpi_tiles["connection"].set_note(connection_text)
        page.kpi_tiles["connection"].set_value("認証不要" if not private_configured else "キー設定済")
        page.kpi_tiles["runs"].set_value(str(len(app_state.list_runs())))

        page.health_chips["gmo"].set_tone("running")
        page.health_chips["gmo"].set_text("GMO: public API")
        broker_tone = "running" if private_configured else "warn"
        page.health_chips["broker"].set_tone(broker_tone)
        page.health_chips["broker"].set_text(
            "Broker: 認証済" if private_configured else "Broker: public のみ"
        )
        model_status = app_state.model_status()
        page.health_chips["ml"].set_tone("running" if model_status.get("exists") else "neutral")
        page.health_chips["ml"].set_text(
            "ML: モデルあり" if model_status.get("exists") else "ML: 未学習"
        )
        page.health_chips["sync"].set_tone("running")
        page.health_chips["sync"].set_text(f"同期: {app_state.config.data.end_date}")

        if app_state.last_result is None:
            page.kpi_tiles["equity"].set_value("-")
            summary_label.setText("まだバックテストまたはデモ実行はありません。")
            page.highlight_label.setText("監視通貨ペアを設定してバックテストを回すと、ここに要約が表示されます。")
        else:
            result = app_state.last_result
            total_return = result.metrics.get("total_return", 0.0)
            tone = "pos" if total_return >= 0 else "neg"
            page.kpi_tiles["equity"].set_value(f"{total_return:.2%}", tone=tone)
            summary_label.setText(
                f"実行ID: {result.run_id}\n"
                f"最大ドローダウン: {result.metrics.get('max_drawdown', 0):.2%}\n"
                f"取引回数: {result.metrics.get('number_of_trades', 0)}"
            )
            page.highlight_label.setText(
                f"最新バックテストは {result.metrics.get('number_of_trades', 0)} 件の取引を記録し、"
                f"総損益 {total_return:.2%} / シャープ {(result.metrics.get('sharpe') or 0):.2f} でした。"
            )

    page.refresh = refresh
    return page
