"""Backtest page."""

from __future__ import annotations

import os
import sys


def build_backtest_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    import pandas as pd

    from PySide6.QtCore import QDate, Qt, QTimer
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QCheckBox,
        QComboBox,
        QDateEdit,
        QDoubleSpinBox,
        QFormLayout,
        QFrame,
        QGridLayout,
        QHeaderView,
        QLabel,
        QPushButton,
        QScrollArea,
        QSizePolicy,
        QTableView,
        QTextBrowser,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.charts import render_backtest_dashboard_fallback_html, render_backtest_dashboard_html
    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled, set_button_role

    try:
        if os.environ.get("QT_QPA_PLATFORM") == "offscreen" or getattr(sys, "frozen", False):
            raise ImportError("offscreen mode")
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except ImportError:  # pragma: no cover - fallback path
        QWebEngineView = None

    DataFrameTableModel = load_dataframe_model_class()

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(12, 12, 12, 12)
    layout.setSpacing(12)
    page.setWidget(content)

    title = QLabel("バックテスト")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)

    def card_style(name: str) -> str:
        return f"QFrame#{name} {{ background: white; border: 1px solid #dbe3ee; border-radius: 16px; }}"

    def build_metric_card(name: str, label_text: str) -> tuple[QFrame, QLabel]:
        card = QFrame()
        card.setObjectName(name)
        card.setStyleSheet(card_style(name))
        card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(14, 12, 14, 12)
        card_layout.setSpacing(6)
        label = QLabel(label_text)
        label.setStyleSheet("color: #64748b; font-size: 12px; font-weight: 600;")
        value = QLabel("-")
        value.setWordWrap(True)
        value.setStyleSheet("color: #0f172a; font-size: 22px; font-weight: 700;")
        card_layout.addWidget(label)
        card_layout.addWidget(value)
        return card, value

    def build_section(title_text: str, helper_text: str | None = None) -> tuple[QFrame, QVBoxLayout]:
        section = QFrame()
        object_name = f"backtestSection_{title_text}"
        section.setObjectName(object_name)
        section.setStyleSheet(card_style(object_name))
        section_layout = QVBoxLayout(section)
        section_layout.setContentsMargins(16, 14, 16, 14)
        section_layout.setSpacing(10)
        title_label = QLabel(title_text)
        title_label.setStyleSheet("font-size: 17px; font-weight: 700; color: #0f172a;")
        section_layout.addWidget(title_label)
        if helper_text:
            helper_label = QLabel(helper_text)
            helper_label.setWordWrap(True)
            helper_label.setStyleSheet("color: #475569;")
            section_layout.addWidget(helper_label)
        return section, section_layout

    top_card, top_layout = build_section(
        "実行設定",
        "未チェックのときはデータ同期と同じ期間でバックテストします。"
        " チェックすると、同期済みキャッシュから指定期間だけを切り出して検証します。"
        " 初期資産は JPY 基準です。",
    )
    form = QFormLayout()
    strategy_combo = QComboBox()
    strategy_combo.addItems(["baseline_trend_pullback", "multi_timeframe_pattern_scoring", "fx_breakout_pullback"])
    strategy_combo.setCurrentText(app_state.config.strategy.name)
    custom_window_box = QCheckBox("同期期間とは別にバックテスト期間を指定")
    custom_window_box.setChecked(app_state.config.backtest.use_custom_window)
    starting_cash_input = QDoubleSpinBox()
    starting_cash_input.setRange(100.0, 1_000_000_000.0)
    starting_cash_input.setDecimals(2)
    starting_cash_input.setSingleStep(1000.0)
    starting_cash_input.setSuffix(" JPY")
    starting_cash_input.setGroupSeparatorShown(True)
    starting_cash_input.setValue(float(app_state.config.risk.starting_cash))
    start_date = QDateEdit()
    start_date.setCalendarPopup(True)
    start_date.setDisplayFormat("yyyy-MM-dd")
    end_date = QDateEdit()
    end_date.setCalendarPopup(True)
    end_date.setDisplayFormat("yyyy-MM-dd")
    form.addRow("戦略", strategy_combo)
    form.addRow("初期資産", starting_cash_input)
    form.addRow("期間指定", custom_window_box)
    form.addRow("開始日", start_date)
    form.addRow("終了日", end_date)
    top_layout.addLayout(form)
    run_button = QPushButton("バックテスト実行")
    set_button_role(run_button, "primary")
    top_layout.addWidget(run_button)
    layout.addWidget(top_card)

    ml_card, ml_layout = build_section(
        "ML / Research",
        "FX breakout 戦略では、rule-only・学習済みモデル読込・学習後バックテスト・walk-forward 研究をここから操作できます。",
    )
    ml_form = QFormLayout()
    ml_enabled_box = QCheckBox("ML 参加フィルタを有効化")
    ml_mode_combo = QComboBox()
    ml_mode_combo.addItems(["rule_only", "load_pretrained", "train_from_scratch", "walk_forward_train"])
    research_mode_combo = QComboBox()
    research_mode_combo.addItems(["quick", "standard", "exhaustive"])
    model_status_label = QLabel()
    model_status_label.setWordWrap(True)
    model_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    research_status_label = QLabel("まだ research_run は実行していません。")
    research_status_label.setWordWrap(True)
    research_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    ml_form.addRow("ML 有効化", ml_enabled_box)
    ml_form.addRow("ML Backtest モード", ml_mode_combo)
    ml_form.addRow("Research モード", research_mode_combo)
    ml_form.addRow("モデル状態", model_status_label)
    ml_layout.addLayout(ml_form)
    ml_buttons = QGridLayout()
    train_button = QPushButton("FX ML 学習")
    research_button = QPushButton("Research 実行")
    set_button_role(train_button, "success")
    set_button_role(research_button, "secondary")
    ml_buttons.addWidget(train_button, 0, 0)
    ml_buttons.addWidget(research_button, 0, 1)
    ml_layout.addLayout(ml_buttons)
    ml_layout.addWidget(research_status_label)
    layout.addWidget(ml_card)

    summary_card, summary_layout = build_section("結果サマリー")
    summary_meta = QLabel("まだバックテスト結果はありません。")
    summary_meta.setWordWrap(True)
    summary_meta.setTextInteractionFlags(Qt.TextSelectableByMouse)
    summary_meta.setStyleSheet("color: #334155; background: #f8fafc; border-radius: 12px; padding: 12px;")
    summary_layout.addWidget(summary_meta)
    summary_grid = QGridLayout()
    summary_grid.setHorizontalSpacing(10)
    summary_grid.setVerticalSpacing(10)
    for column in range(4):
        summary_grid.setColumnStretch(column, 1)
    metric_specs = [
        ("total_return", "総損益"),
        ("annualized_return", "年率換算"),
        ("max_drawdown", "最大ドローダウン"),
        ("win_rate", "勝率"),
        ("sharpe", "シャープレシオ"),
        ("trades", "取引回数"),
        ("avg_hold", "平均保有期間"),
        ("sample_split", "IS / OOS"),
    ]
    metric_labels: dict[str, QLabel] = {}
    for index, (key, label_text) in enumerate(metric_specs):
        metric_card, metric_value = build_metric_card(f"backtestMetric_{key}", label_text)
        summary_grid.addWidget(metric_card, index // 4, index % 4)
        metric_labels[key] = metric_value
    summary_layout.addLayout(summary_grid)
    layout.addWidget(summary_card)

    dashboard_card, dashboard_layout = build_section(
        "分析ダッシュボード",
        "資産推移、通貨ペア別寄与、月次リターン、Walk-Forward をまとめて確認できます。",
    )
    dashboard_view = QWebEngineView() if QWebEngineView is not None else QTextBrowser()
    dashboard_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
    if isinstance(dashboard_view, QTextBrowser):
        dashboard_view.setFrameShape(QFrame.NoFrame)
        dashboard_view.setOpenExternalLinks(True)
        dashboard_view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        dashboard_view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    dashboard_layout.addWidget(dashboard_view)
    layout.addWidget(dashboard_card)

    def configure_table(view: QTableView) -> None:
        view.setAlternatingRowColors(True)
        view.setSelectionBehavior(QAbstractItemView.SelectRows)
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        view.setShowGrid(False)
        view.verticalHeader().setVisible(False)
        view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        header = view.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setDefaultSectionSize(150)
        header.setMinimumSectionSize(90)

    def fit_table_height(view: QTableView) -> None:
        model = view.model()
        rows = model.rowCount() if model is not None else 0
        if model is None or model.columnCount() == 0:
            view.setMinimumHeight(96)
            view.setMaximumHeight(96)
            return
        view.resizeColumnsToContents()
        view.resizeRowsToContents()
        height = view.horizontalHeader().height() + (view.frameWidth() * 2)
        if rows == 0:
            height += 44
        else:
            height += sum(view.rowHeight(row) for row in range(rows))
        if view.horizontalScrollBar().isVisible():
            height += view.horizontalScrollBar().height()
        view.setMinimumHeight(height)
        view.setMaximumHeight(height)

    def build_table_section(title_text: str, helper_text: str) -> tuple[QTableView, DataFrameTableModel]:
        card, card_layout = build_section(title_text, helper_text)
        table = QTableView()
        configure_table(table)
        model = DataFrameTableModel()
        table.setModel(model)
        card_layout.addWidget(table)
        layout.addWidget(card)
        return table, model

    trades_view, trades_model = build_table_section(
        "取引一覧",
        "直近 300 件の取引を上から順に確認できます。ページ全体を縦スクロールして閲覧します。",
    )
    signals_view, signals_model = build_table_section(
        "シグナル",
        "バックテスト中に記録された直近 300 件のシグナルです。",
    )
    attribution_view, attribution_model = build_table_section(
        "通貨ペア別寄与",
        "通貨ペアごとの純損益寄与をまとめています。",
    )
    walk_forward_view, walk_forward_model = build_table_section(
        "Walk-Forward",
        "時間窓ごとの成績変化です。直近区間だけ良いかどうかも確認できます。",
    )

    layout.addStretch(1)
    page._busy = False

    def parse_qdate(value: str, fallback: str) -> QDate:
        parsed = QDate.fromString(value, "yyyy-MM-dd")
        if parsed.isValid():
            return parsed
        return QDate.fromString(fallback, "yyyy-MM-dd")

    def refresh_controls() -> None:
        strategy_combo.setCurrentText(app_state.config.strategy.name)
        custom_window_box.setChecked(app_state.config.backtest.use_custom_window)
        starting_cash_input.setValue(float(app_state.config.risk.starting_cash))
        ml_enabled_box.setChecked(app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled)
        ml_mode_combo.setCurrentText(app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)
        research_mode_combo.setCurrentText(app_state.config.research.mode)
        start_date.setDate(
            parse_qdate(
                app_state.config.backtest.start_date or app_state.config.data.start_date,
                app_state.config.data.start_date,
            )
        )
        end_date.setDate(
            parse_qdate(
                app_state.config.backtest.end_date or app_state.config.data.end_date,
                app_state.config.data.end_date,
            )
        )
        model_status = app_state.model_status()
        model_status_label.setText(
            "\n".join(
                [
                    f"ML 有効: {'はい' if model_status['enabled'] else 'いいえ'}",
                    f"Backtest モード: {model_status['backtest_mode']}",
                    f"モデル保存先: {model_status['model_path']}",
                    f"存在: {'あり' if model_status['exists'] else 'なし'}",
                ]
            )
        )
        if app_state.last_research_result is not None:
            research_status_label.setText(
                "\n".join(
                    [
                        f"最新 research_run: {app_state.last_research_result.get('run_id', '-')}",
                        f"出力先: {app_state.last_research_result.get('output_dir', '-')}",
                        f"Mode: {app_state.last_research_result.get('mode', '-')}",
                    ]
                )
            )
        update_window_enabled()

    def update_window_enabled() -> None:
        enabled = custom_window_box.isChecked() and not page._busy
        start_date.setEnabled(enabled)
        end_date.setEnabled(enabled)

    def set_busy(is_busy: bool) -> None:
        page._busy = is_busy
        run_button.setText("バックテスト実行中..." if is_busy else "バックテスト実行")
        set_button_enabled(run_button, not is_busy, busy=is_busy)
        strategy_combo.setEnabled(not is_busy)
        starting_cash_input.setEnabled(not is_busy)
        custom_window_box.setEnabled(not is_busy)
        ml_enabled_box.setEnabled(not is_busy)
        ml_mode_combo.setEnabled(not is_busy)
        research_mode_combo.setEnabled(not is_busy)
        set_button_enabled(train_button, not is_busy, busy=is_busy)
        set_button_enabled(research_button, not is_busy, busy=is_busy)
        update_window_enabled()

    def fit_dashboard_height() -> None:
        if QWebEngineView is not None and isinstance(dashboard_view, QWebEngineView):
            dashboard_view.setMinimumHeight(1240)
            dashboard_view.setMaximumHeight(1240)
            return
        document = dashboard_view.document()
        document.setTextWidth(max(dashboard_view.viewport().width() - 12, 900))
        height = int(document.size().height()) + 24
        dashboard_view.setMinimumHeight(max(height, 440))
        dashboard_view.setMaximumHeight(max(height, 440))

    def set_dashboard_html(html: str) -> None:
        dashboard_view.setHtml(html)
        QTimer.singleShot(0, fit_dashboard_height)

    def render_dashboard(result) -> None:  # noqa: ANN001
        if QWebEngineView is not None and isinstance(dashboard_view, QWebEngineView):
            set_dashboard_html(render_backtest_dashboard_html(result))
            return
        set_dashboard_html(render_backtest_dashboard_fallback_html(result))

    def fit_all_tables() -> None:
        for view in (trades_view, signals_view, attribution_view, walk_forward_view):
            fit_table_height(view)

    def refresh_views() -> None:
        if app_state.last_result is None:
            summary_meta.setText("まだバックテスト結果はありません。")
            metric_labels["total_return"].setText("-")
            metric_labels["annualized_return"].setText("-")
            metric_labels["max_drawdown"].setText("-")
            metric_labels["win_rate"].setText("-")
            metric_labels["sharpe"].setText("-")
            metric_labels["trades"].setText("-")
            metric_labels["avg_hold"].setText("-")
            metric_labels["sample_split"].setText("-")
            trades_model.set_frame(None)
            signals_model.set_frame(None)
            attribution_model.set_frame(None)
            walk_forward_model.set_frame(None)
            set_dashboard_html("<h3>まだ分析ダッシュボードはありません。</h3>")
            fit_all_tables()
            return
        result = app_state.last_result
        summary_meta.setText(
            "\n".join(
                [
                    f"実行ID: {result.run_id}",
                    f"出力先: {result.output_dir}",
                    f"検証期間: {result.backtest_start} - {result.backtest_end}",
                    f"初期資産: {result.starting_cash:,.2f} JPY",
                    f"In-Sample 総損益: {result.in_sample_metrics.get('total_return', 0):.2%}",
                    f"Out-of-Sample 総損益: {result.out_of_sample_metrics.get('total_return', 0):.2%}",
                ]
            )
        )
        metric_labels["total_return"].setText(f"{result.metrics.get('total_return', 0):.2%}")
        metric_labels["annualized_return"].setText(f"{result.metrics.get('annualized_return', 0):.2%}")
        metric_labels["max_drawdown"].setText(f"{result.metrics.get('max_drawdown', 0):.2%}")
        metric_labels["win_rate"].setText(f"{result.metrics.get('win_rate', 0):.2%}")
        metric_labels["sharpe"].setText(f"{(result.metrics.get('sharpe') or 0):.2f}")
        metric_labels["trades"].setText(str(result.metrics.get("number_of_trades", 0)))
        metric_labels["avg_hold"].setText(f"{result.metrics.get('average_hold_bars', 0):.2f}")
        metric_labels["sample_split"].setText(
            f"{result.in_sample_metrics.get('total_return', 0):.2%} / {result.out_of_sample_metrics.get('total_return', 0):.2%}"
        )
        trades_model.set_frame(result.trades.tail(300))
        signal_columns = [
            "timestamp",
            "symbol",
            "signal_action",
            "signal_score",
            "sub_score_trend_regime",
            "sub_score_pullback_continuation",
            "sub_score_breakout_compression",
            "explanation_ja",
        ]
        available_signal_columns = [column for column in signal_columns if column in result.signals.columns]
        signals_model.set_frame(result.signals[available_signal_columns].tail(300) if available_signal_columns else None)
        attribution = result.metrics.get("per_symbol_contribution", {})
        attribution_model.set_frame(
            None
            if not attribution
            else pd.DataFrame(
                [{"通貨ペア": key, "純損益": value} for key, value in attribution.items()]
            )
        )
        walk_forward_model.set_frame(
            None
            if not result.walk_forward
            else pd.DataFrame(
                [
                    {
                        "窓": row.get("window"),
                        "開始": row.get("start"),
                        "終了": row.get("end"),
                        "総損益": f"{row.get('metrics', {}).get('total_return', 0):.2%}",
                        "シャープ": f"{(row.get('metrics', {}).get('sharpe') or 0):.2f}",
                        "最大DD": f"{row.get('metrics', {}).get('max_drawdown', 0):.2%}",
                    }
                    for row in result.walk_forward
                ]
            )
        )
        render_dashboard(result)
        fit_all_tables()

    def on_finished(result) -> None:
        _ = result
        set_busy(False)
        refresh_views()
        log_message("バックテストが完了しました。")

    def on_error(message: str) -> None:
        set_busy(False)
        summary_meta.setText(f"エラー\n{message}")
        metric_labels["total_return"].setText("-")
        set_dashboard_html(f"<h3>エラー</h3><p>{message}</p>")
        fit_all_tables()
        log_message(f"バックテストエラー: {message}")

    def persist_fx_controls() -> None:
        app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled = ml_enabled_box.isChecked()
        app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode = ml_mode_combo.currentText()
        app_state.config.research.mode = research_mode_combo.currentText()

    def run_backtest() -> None:
        selected_start = start_date.date().toString("yyyy-MM-dd")
        selected_end = end_date.date().toString("yyyy-MM-dd")
        if selected_start > selected_end:
            on_error("開始日は終了日以前にしてください。")
            return
        app_state.config.risk.starting_cash = float(starting_cash_input.value())
        set_busy(True)
        app_state.config.strategy.name = strategy_combo.currentText()
        app_state.config.backtest.use_custom_window = custom_window_box.isChecked()
        app_state.config.backtest.start_date = selected_start
        app_state.config.backtest.end_date = selected_end
        persist_fx_controls()
        app_state.save_config()
        summary_meta.setText("バックテスト実行中...")
        set_dashboard_html("<h3>バックテスト実行中...</h3>")
        fit_all_tables()
        submit_task(app_state.run_backtest, on_finished, on_error)

    def on_train_finished(summary) -> None:  # noqa: ANN001
        set_busy(False)
        refresh_controls()
        dataset_path = summary.get("dataset_path", "")
        model_path = summary.get("latest_model_path") or summary.get("model_path") or ""
        research_status_label.setText(
            "\n".join(
                [
                    f"学習完了: {summary.get('trained_rows', 0)} 行",
                    f"学習期間: {summary.get('train_start', '-') } - {summary.get('train_end', '-')}",
                    f"モデル: {model_path}",
                    f"ラベルデータ: {dataset_path}",
                ]
            )
        )
        log_message("FX ML 学習が完了しました。")

    def run_train() -> None:
        persist_fx_controls()
        app_state.save_config()
        set_busy(True)
        research_status_label.setText("FX ML 学習を実行中...")
        submit_task(app_state.train_fx_model, on_train_finished, on_error)

    def on_research_finished(summary) -> None:  # noqa: ANN001
        set_busy(False)
        refresh_controls()
        research_status_label.setText(
            "\n".join(
                [
                    f"research_run 完了: {summary.get('run_id', '-')}",
                    f"出力先: {summary.get('output_dir', '-')}",
                    f"Uplift: {summary.get('uplift', {}).get('total_return_delta', 0.0):.2%}",
                ]
            )
        )
        log_message("research_run が完了しました。")

    def run_research() -> None:
        persist_fx_controls()
        app_state.save_config()
        set_busy(True)
        research_status_label.setText("research_run を実行中...")
        submit_task(
            lambda: app_state.run_research(mode=research_mode_combo.currentText()),
            on_research_finished,
            on_error,
        )

    def refresh_page() -> None:
        refresh_controls()
        refresh_views()

    run_button.clicked.connect(run_backtest)
    train_button.clicked.connect(run_train)
    research_button.clicked.connect(run_research)
    custom_window_box.toggled.connect(update_window_enabled)
    page.refresh = refresh_page
    refresh_controls()
    refresh_views()
    return page
