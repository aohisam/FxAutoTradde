"""Overview page."""

from __future__ import annotations


def build_overview_page(app_state, on_run_demo=None):  # pragma: no cover - UI helper
    from PySide6.QtWidgets import (
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QPushButton,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.ml_labels import ml_mode_label
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl

    def make_card_header_tools(hint_text: str, segmented=None) -> QWidget:
        box = QWidget()
        lay = QHBoxLayout(box)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(10)
        if hint_text:
            hint = QLabel(hint_text)
            hint.setProperty("role", "muted2")
            lay.addWidget(hint)
        if segmented is not None:
            lay.addWidget(segmented)
        return box

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    # ---- Header ----
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("概要")
    title.setProperty("role", "h1")
    subtitle = QLabel("現在の運用状態と最新のバックテスト結果をまとめて確認できます。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    btn_refresh = QPushButton("更新")
    btn_refresh.setProperty("variant", "ghost")
    btn_demo = QPushButton("デモ実行")
    btn_demo.setProperty("variant", "primary")
    header.addWidget(btn_refresh)
    header.addWidget(btn_demo)
    layout.addLayout(header)

    # ---- KPI grid ----
    page.kpi_tiles: dict = {}
    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(4):
        grid.setColumnStretch(column, 1)
    kpi_specs = [
        ("mode",       "現在のモード",           "mono"),
        ("automation", "自動売買状態",           "mono"),
        ("equity",     "総損益 (直近実行)",     "mono"),
        ("drawdown",   "最大ドローダウン",       "mono"),
        ("market",     "市場ステータス",         "mono"),
        ("strategy",   "選択中の戦略",           "code-md"),
        ("runs",       "保存済み実行数",         "mono"),
        ("connection", "接続状態",               "mono"),
    ]
    for index, (key, label_text, variant) in enumerate(kpi_specs):
        tile = KpiTile(label=label_text, value="-", note="", value_variant=variant)
        grid.addWidget(tile, index // 4, index % 4)
        page.kpi_tiles[key] = tile
    layout.addLayout(grid)

    # ---- split-2 ----
    split = QGridLayout()
    split.setHorizontalSpacing(14)
    split.setColumnStretch(0, 3)
    split.setColumnStretch(1, 2)

    # left: 資産推移 (直近 30 日)
    segmented = SegmentedControl(["1W", "1M", "3M", "YTD"], current=1)
    chart_card = Card(
        title="資産推移 (直近 30 日)",
        header_right=make_card_header_tools("最新バックテスト結果", segmented=segmented),
    )
    equity_chart_ph = QFrame()
    equity_chart_ph.setObjectName("equityChartPlaceholder")
    equity_chart_ph.setMinimumHeight(240)
    equity_chart_ph.setFrameShape(QFrame.NoFrame)
    chart_card.addBodyWidget(equity_chart_ph, 1)
    page.equity_chart_placeholder = equity_chart_ph
    page.equity_range_segmented = segmented
    split.addWidget(chart_card, 0, 0)

    # right: 最新サマリー (2x3 detail grid)
    summary_card = Card(title="最新サマリー")
    detail = QGridLayout()
    detail.setHorizontalSpacing(16)
    detail.setVerticalSpacing(12)
    detail.setColumnStretch(0, 1)
    detail.setColumnStretch(1, 1)
    latest_summary_labels: dict[str, QLabel] = {}
    cells = [
        ("run_id",  "実行ID"),
        ("period",  "検証期間"),
        ("initial", "初期資産"),
        ("final",   "最終評価"),
        ("is",      "IS 総損益"),
        ("oos",     "OOS 総損益"),
    ]
    for index, (key, eyebrow_text) in enumerate(cells):
        cell = QVBoxLayout()
        cell.setContentsMargins(0, 0, 0, 0)
        cell.setSpacing(2)
        eyebrow = QLabel(eyebrow_text.upper())
        eyebrow.setProperty("role", "eyebrow")
        value = QLabel("-")
        value.setProperty("role", "detail-value")
        value.setWordWrap(True)
        cell.addWidget(eyebrow)
        cell.addWidget(value)
        detail.addLayout(cell, index // 2, index % 2)
        latest_summary_labels[key] = value
    summary_card.addBodyLayout(detail)
    split.addWidget(summary_card, 0, 1)
    layout.addLayout(split, 1)

    # ---- wiring ----
    btn_refresh.clicked.connect(lambda: page.refresh())
    if on_run_demo is not None:
        btn_demo.clicked.connect(on_run_demo)
    else:
        btn_demo.setEnabled(False)
        btn_demo.setToolTip("デモ実行コールバックが未接続です")

    def refresh() -> None:
        cfg = app_state.config

        # mode
        mode_value = cfg.broker.mode.value
        page.kpi_tiles["mode"].set_value(
            {
                "local_sim": "ローカルシミュレーション",
                "gmo_sim": "GMO 実時間シミュレーション",
            }.get(mode_value, mode_value)
        )
        paper_chip = Chip(
            "paper" if mode_value == "gmo_sim" else "ローカル",
            tone="info",
        )
        page.kpi_tiles["mode"].set_note_chip(paper_chip)
        page.kpi_tiles["mode"].set_note("実売買は行いません")

        # automation
        running = app_state.automation_controller is not None
        page.kpi_tiles["automation"].set_value(
            "稼働中" if running else "停止",
            tone="pos" if running else None,
        )
        page.kpi_tiles["automation"].set_note_chip(
            Chip("running" if running else "stopped", tone="running" if running else "neutral")
        )
        page.kpi_tiles["automation"].set_note("")

        # equity / drawdown
        if app_state.last_result is None:
            page.kpi_tiles["equity"].set_value("-")
            page.kpi_tiles["equity"].set_trend(None)
            page.kpi_tiles["equity"].set_note("")
            page.kpi_tiles["drawdown"].set_value("-")
            page.kpi_tiles["drawdown"].set_trend("flat", "-")
            page.kpi_tiles["drawdown"].set_note("")
        else:
            result = app_state.last_result
            metrics = result.metrics
            total = metrics.get("total_return", 0.0)
            drawdown = metrics.get("max_drawdown", 0.0)
            sharpe = metrics.get("sharpe") or 0.0
            trades = metrics.get("number_of_trades", 0)
            win_rate = metrics.get("win_rate", 0.0)
            annualized = metrics.get("annualized_return", 0.0)
            page.kpi_tiles["equity"].set_value(
                f"{total:+.2%}",
                tone="pos" if total >= 0 else "neg",
            )
            page.kpi_tiles["equity"].set_trend(
                "up" if total >= 0 else "down",
                f"{abs(total):.2%}",
            )
            page.kpi_tiles["equity"].set_note(
                f"年率換算 {annualized:+.1%} · シャープ {sharpe:.2f}"
            )
            page.kpi_tiles["drawdown"].set_value(
                f"{drawdown:.2%}",
                tone="neg" if drawdown < 0 else None,
            )
            page.kpi_tiles["drawdown"].set_trend("flat", "-")
            page.kpi_tiles["drawdown"].set_note(
                f"取引回数 {trades} · 勝率 {win_rate:.1%}"
            )

        # market
        page.kpi_tiles["market"].set_value(
            {
                "gmo": "GMO 実時間データ",
                "csv": "JForex CSV キャッシュ",
                "fixture": "fixture 検証データ",
            }.get(cfg.data.source, cfg.data.source)
        )
        page.kpi_tiles["market"].set_note(
            f"エントリー足: {cfg.strategy.entry_timeframe.value}"
        )

        # strategy
        page.kpi_tiles["strategy"].set_value(cfg.strategy.name)
        ml_cfg = getattr(cfg.strategy, "fx_breakout_pullback", None)
        ml_filter = getattr(ml_cfg, "ml_filter", None) if ml_cfg is not None else None
        ml_enabled = bool(getattr(ml_filter, "enabled", False)) if ml_filter is not None else False
        ml_mode = getattr(ml_filter, "backtest_mode", "-") if ml_filter is not None else "-"
        page.kpi_tiles["strategy"].set_note(
            f"ML 参加フィルタ {'有効' if ml_enabled else '無効'} · {ml_mode_label(ml_mode)}"
        )

        # runs
        page.kpi_tiles["runs"].set_value(str(len(app_state.list_runs())))
        page.kpi_tiles["runs"].set_note(f"最終同期: {cfg.data.end_date}")

        # connection
        has_credentials = getattr(app_state.env, "has_credentials", None)
        private_cfg = bool(has_credentials("private")) if callable(has_credentials) else False
        page.kpi_tiles["connection"].set_value(
            "接続済み" if private_cfg else "認証不要"
        )
        page.kpi_tiles["connection"].set_note_chip(Chip("stream healthy", tone="running"))
        page.kpi_tiles["connection"].set_note("")

        # latest summary card
        if app_state.last_result is None:
            for value_label in latest_summary_labels.values():
                value_label.setText("-")
            return

        result = app_state.last_result
        metrics = result.metrics
        initial_cash = float(
            getattr(cfg.risk, "starting_cash", None)
            or getattr(cfg.risk, "initial_capital", 0.0)
            or 0.0
        )
        final_value = initial_cash * (1.0 + metrics.get("total_return", 0.0))
        is_return = getattr(result, "in_sample_metrics", {}).get(
            "total_return", metrics.get("total_return", 0.0)
        )
        oos_return = getattr(result, "out_of_sample_metrics", {}).get(
            "total_return", 0.0
        )
        backtest_start = getattr(result, "backtest_start", cfg.data.start_date)
        backtest_end = getattr(result, "backtest_end", cfg.data.end_date)
        latest_summary_labels["run_id"].setText(result.run_id)
        latest_summary_labels["period"].setText(f"{backtest_start} → {backtest_end}")
        latest_summary_labels["initial"].setText(f"{initial_cash:,.0f} JPY")
        latest_summary_labels["final"].setText(f"{final_value:+,.0f}")
        latest_summary_labels["is"].setText(f"{is_return:+.2%}")
        latest_summary_labels["oos"].setText(f"{oos_return:+.2%}")

    page.refresh = lambda: refresh() if page.isVisible() else None
    if page.isVisible():
        refresh()
    return page
