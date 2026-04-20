"""Backtest page."""

from __future__ import annotations


RESEARCH_MODES = ["quick", "standard", "exhaustive"]
STRATEGY_LABELS = [
    "fx_breakout_pullback",
    "baseline_trend_pullback",
    "multi_timeframe_pattern_scoring",
]
TF_SEG_LABELS = ["5m", "15m", "1h", "4h"]


def build_backtest_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    import pandas as pd

    from PySide6.QtCore import QDate, Qt
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QCheckBox,
        QComboBox,
        QDateEdit,
        QFormLayout,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QPushButton,
        QScrollArea,
        QSizePolicy,
        QStackedWidget,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.date_inputs import create_popup_date_edit
    from fxautotrade_lab.desktop.ml_labels import ML_MODE_CHOICES, ml_mode_description, ml_mode_label
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl
    from fxautotrade_lab.desktop.widgets.suffix_input import LabeledSuffixInput

    DataFrameTableModel = load_dataframe_model_class()

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    # ---- Header ----
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("バックテスト")
    title.setProperty("role", "h1")
    subtitle = QLabel("戦略・期間・ML 設定を選んでバックテストを実行します。")
    subtitle.setProperty("role", "muted")
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    reload_cfg_btn = QPushButton("前回設定を読込")
    reload_cfg_btn.setProperty("variant", "ghost")
    run_button = QPushButton("バックテスト実行")
    run_button.setProperty("variant", "primary")
    header.addWidget(reload_cfg_btn)
    header.addWidget(run_button)
    layout.addLayout(header)

    def labeled(label_text: str, hint_text: str = "") -> QWidget:
        if not hint_text:
            label = QLabel(label_text)
            label.setProperty("role", "form-label")
            return label
        wrap = QWidget()
        lay = QHBoxLayout(wrap)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(6)
        label = QLabel(label_text)
        label.setProperty("role", "form-label")
        hint = QLabel(hint_text)
        hint.setProperty("role", "muted2")
        lay.addWidget(label)
        lay.addWidget(hint)
        lay.addStretch(1)
        return wrap

    def _set_combo_by_data(combo: QComboBox, value: str) -> None:
        index = combo.findData(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    # ---- Config card (grid-2) ----
    config_hint = QLabel("未チェック時はデータ同期と同じ期間で検証")
    config_hint.setProperty("role", "muted2")
    config_card = Card(title="実行設定", header_right=config_hint)

    config_body = QWidget()
    cols = QHBoxLayout(config_body)
    cols.setContentsMargins(0, 0, 0, 0)
    cols.setSpacing(16)

    left_form = QFormLayout()
    left_form.setHorizontalSpacing(12)
    left_form.setVerticalSpacing(10)
    left_form.setLabelAlignment(Qt.AlignLeft)

    strategy_combo = QComboBox()
    strategy_combo.addItems(STRATEGY_LABELS)

    starting_cash_input = LabeledSuffixInput(value="", suffix="JPY")
    starting_cash_input.edit.setAlignment(Qt.AlignRight)
    starting_cash_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    custom_window_box = QCheckBox("同期期間とは別にバックテスト期間を指定する")

    left_form.addRow(labeled("戦略", "run に使う戦略を選択"), strategy_combo)
    left_form.addRow(labeled("初期資産", "JPY 基準"), starting_cash_input)
    left_form.addRow(labeled("期間指定"), custom_window_box)

    right_form = QFormLayout()
    right_form.setHorizontalSpacing(12)
    right_form.setVerticalSpacing(10)
    right_form.setLabelAlignment(Qt.AlignLeft)

    start_date = create_popup_date_edit()
    end_date = create_popup_date_edit()
    tf_seg = SegmentedControl(TF_SEG_LABELS, current=1, data=TF_SEG_LABELS)

    right_form.addRow(labeled("開始日"), start_date)
    right_form.addRow(labeled("終了日"), end_date)
    right_form.addRow(labeled("タイムフレーム"), tf_seg)

    left_wrap = QWidget()
    left_wrap.setLayout(left_form)
    right_wrap = QWidget()
    right_wrap.setLayout(right_form)
    cols.addWidget(left_wrap, 1)
    cols.addWidget(right_wrap, 1)
    config_card.addBodyWidget(config_body)
    layout.addWidget(config_card)

    # ---- ML / Research card ----
    train_btn = QPushButton("FX ML 学習")
    train_btn.setProperty("variant", "success")
    research_btn = QPushButton("Research 実行")
    research_btn.setProperty("variant", "ghost")
    ml_tools = QWidget()
    mt = QHBoxLayout(ml_tools)
    mt.setContentsMargins(0, 0, 0, 0)
    mt.setSpacing(8)
    ml_hint = QLabel("FX breakout 戦略の ML フィルタと walk-forward を操作")
    ml_hint.setProperty("role", "muted2")
    mt.addWidget(ml_hint)
    mt.addSpacing(8)
    mt.addWidget(train_btn)
    mt.addWidget(research_btn)
    ml_card = Card(title="ML / Research", header_right=ml_tools)

    ml_body = QWidget()
    ml_cols = QHBoxLayout(ml_body)
    ml_cols.setContentsMargins(0, 0, 0, 0)
    ml_cols.setSpacing(16)
    ml_left = QFormLayout()
    ml_left.setHorizontalSpacing(12)
    ml_left.setVerticalSpacing(10)
    ml_left.setLabelAlignment(Qt.AlignLeft)

    ml_enabled_box = QCheckBox("ML 参加フィルタを有効化する")
    ml_mode_combo = QComboBox()
    for key, label in ML_MODE_CHOICES:
        ml_mode_combo.addItem(label, key)
    research_seg = SegmentedControl(RESEARCH_MODES, current=1, data=RESEARCH_MODES)
    ml_mode_hint = QLabel()
    ml_mode_hint.setProperty("role", "muted")
    ml_mode_hint.setWordWrap(True)

    ml_left.addRow(labeled("ML 有効化"), ml_enabled_box)
    ml_left.addRow(labeled("ML の使い方"), ml_mode_combo)
    ml_left.addRow(labeled("Research モード"), research_seg)
    ml_left.addRow(labeled("モード説明"), ml_mode_hint)

    ml_right_cell = QWidget()
    mrc = QVBoxLayout(ml_right_cell)
    mrc.setContentsMargins(0, 0, 0, 0)
    mrc.setSpacing(2)
    mrc_label = QLabel("モデル状態")
    mrc_label.setProperty("role", "eyebrow")
    model_status_label = QLabel("-")
    model_status_label.setProperty("role", "detail-value")
    model_status_label.setWordWrap(True)
    model_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    mrc.addWidget(mrc_label)
    mrc.addWidget(model_status_label)

    research_status_label = QLabel("まだ research_run は実行していません。")
    research_status_label.setProperty("role", "muted")
    research_status_label.setWordWrap(True)
    research_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    mrc.addWidget(research_status_label)
    mrc.addStretch(1)

    ml_left_wrap = QWidget()
    ml_left_wrap.setLayout(ml_left)
    ml_cols.addWidget(ml_left_wrap, 1)
    ml_cols.addWidget(ml_right_cell, 1)
    ml_card.addBodyWidget(ml_body)
    layout.addWidget(ml_card)

    # ---- Result summary card with KPI 4x2 inside ----
    run_id_hint = QLabel("-")
    run_id_hint.setProperty("role", "mono")
    summary_card = Card(title="結果サマリー", header_right=run_id_hint)

    kpi_specs = [
        ("total_return",      "総損益"),
        ("annualized_return", "年率換算"),
        ("max_drawdown",      "最大ドローダウン"),
        ("win_rate",          "勝率"),
        ("sharpe",            "シャープレシオ"),
        ("trades",            "取引回数"),
        ("avg_hold",          "平均保有期間"),
        ("sample_split",      "IS / OOS"),
    ]
    kpi_grid = QGridLayout()
    kpi_grid.setHorizontalSpacing(12)
    kpi_grid.setVerticalSpacing(12)
    for column in range(4):
        kpi_grid.setColumnStretch(column, 1)
    metric_tiles: dict[str, KpiTile] = {}
    for index, (key, label_text) in enumerate(kpi_specs):
        variant = "code-md" if key == "sample_split" else "mono"
        tile = KpiTile(label=label_text, value="-", value_variant=variant)
        kpi_grid.addWidget(tile, index // 4, index % 4)
        metric_tiles[key] = tile
    summary_card.addBodyLayout(kpi_grid)
    layout.addWidget(summary_card)

    # ---- Single table card w/ segmented switcher ----
    tabs_seg = SegmentedControl(
        ["取引", "シグナル", "通貨ペア別寄与", "Walk-Forward"],
        current=0,
    )
    tab_hint = QLabel("直近 300 件")
    tab_hint.setProperty("role", "muted2")
    tab_tools = QWidget()
    tt = QHBoxLayout(tab_tools)
    tt.setContentsMargins(0, 0, 0, 0)
    tt.setSpacing(10)
    tt.addWidget(tab_hint)
    tt.addWidget(tabs_seg)
    trades_card = Card(title="取引一覧", header_right=tab_tools)

    def _configure_table(view: QTableView) -> None:
        view.setAlternatingRowColors(False)
        view.setShowGrid(False)
        view.setSelectionBehavior(QAbstractItemView.SelectRows)
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        view.verticalHeader().setVisible(False)
        header = view.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        view.setMinimumHeight(320)

    def _make_table() -> tuple[QTableView, DataFrameTableModel]:
        table = QTableView()
        _configure_table(table)
        model = DataFrameTableModel()
        table.setModel(model)
        return table, model

    stack = QStackedWidget()
    trades_view, trades_model = _make_table()
    signals_view, signals_model = _make_table()
    attribution_view, attribution_model = _make_table()
    walk_forward_view, walk_forward_model = _make_table()
    stack.addWidget(trades_view)
    stack.addWidget(signals_view)
    stack.addWidget(attribution_view)
    stack.addWidget(walk_forward_view)
    trades_card.addBodyWidget(stack)
    layout.addWidget(trades_card)

    tabs_seg.currentChanged.connect(lambda i: stack.setCurrentIndex(i))

    layout.addStretch(1)
    page._busy = False

    # ---- helpers ----

    def parse_number_input(editor, *, label: str) -> float:
        if isinstance(editor, LabeledSuffixInput):
            text = editor.edit.text().strip()
        else:
            text = editor.text().strip()
        if not text:
            raise ValueError(f"{label}を入力してください。")
        try:
            return float(text.replace(",", "").replace("JPY", "").strip())
        except ValueError as exc:
            raise ValueError(f"{label}は数値で入力してください。") from exc

    def format_number(value: float, decimals: int = 2) -> str:
        text = f"{float(value):,.{decimals}f}"
        if decimals and "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    def parse_qdate(value: str, fallback: str) -> QDate:
        parsed = QDate.fromString(value, "yyyy-MM-dd")
        if parsed.isValid():
            return parsed
        return QDate.fromString(fallback, "yyyy-MM-dd")

    def tone_for(value: float) -> str | None:
        if value > 0:
            return "pos"
        if value < 0:
            return "neg"
        return None

    _SIDE_LABELS = {"buy": "買い", "sell": "売り", "long": "買い", "short": "売り"}

    def _trades_frame(df):
        if df is None or df.empty:
            return None
        out = pd.DataFrame(index=df.index)
        if "timestamp" in df.columns:
            stamps = pd.to_datetime(df["timestamp"], errors="coerce")
            out["時刻"] = stamps.dt.strftime("%m/%d %H:%M").fillna("")
        elif "entry_time" in df.columns:
            stamps = pd.to_datetime(df["entry_time"], errors="coerce")
            out["時刻"] = stamps.dt.strftime("%m/%d %H:%M").fillna("")
        else:
            out["時刻"] = ""
        out["通貨ペア"] = df["symbol"].astype(str) if "symbol" in df.columns else ""
        if "side" in df.columns:
            out["売買"] = df["side"].astype(str).str.lower().map(_SIDE_LABELS).fillna(df["side"].astype(str))
        else:
            out["売買"] = ""
        if "quantity" in df.columns:
            out["数量"] = df["quantity"]
        elif "qty" in df.columns:
            out["数量"] = df["qty"]
        else:
            out["数量"] = 0
        out["価格"] = df["price"] if "price" in df.columns else (df.get("entry_price", 0) if hasattr(df, "get") else 0)
        if "pnl" in df.columns:
            out["損益"] = df["pnl"]
        elif "realized_pnl" in df.columns:
            out["損益"] = df["realized_pnl"]
        else:
            out["損益"] = 0
        out["状態"] = df["status"].astype(str) if "status" in df.columns else "約定済み"
        if "explanation_ja" in df.columns:
            out["説明"] = df["explanation_ja"].astype(str)
        elif "explanation" in df.columns:
            out["説明"] = df["explanation"].astype(str)
        else:
            out["説明"] = ""
        return out.tail(300)

    def refresh_controls() -> None:
        strategy_combo.setCurrentText(app_state.config.strategy.name)
        custom_window_box.setChecked(app_state.config.backtest.use_custom_window)
        starting_cash_input.edit.setText(format_number(app_state.config.risk.starting_cash, 2))
        ml_enabled_box.setChecked(app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled)
        _set_combo_by_data(ml_mode_combo, app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)
        current_mode = app_state.config.research.mode
        if current_mode in RESEARCH_MODES:
            research_seg.set_current(RESEARCH_MODES.index(current_mode))
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
        try:
            entry_tf = app_state.config.strategy.entry_timeframe.value
            mapping = {"5Min": 0, "15Min": 1, "1Hour": 2, "4Hour": 3}
            if entry_tf in mapping:
                tf_seg.set_current(mapping[entry_tf])
        except Exception:  # noqa: BLE001
            pass
        update_model_status()
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

    def update_model_status() -> None:
        status = app_state.model_status()
        selected_enabled = ml_enabled_box.isChecked()
        selected_mode = str(ml_mode_combo.currentData() or status.get("backtest_mode", "-"))
        lines = [
            f"ML 有効: {'はい' if selected_enabled else 'いいえ'}",
            f"ML モード: {ml_mode_label(selected_mode)}",
            ml_mode_description(selected_mode),
            f"モデル保存先: {status.get('model_path', '-')}",
            f"存在: {'あり' if status.get('exists') else 'なし'}",
        ]
        model_status_label.setText("\n".join(line for line in lines if line))
        ml_mode_hint.setText(ml_mode_description(selected_mode))

    def update_window_enabled() -> None:
        enabled = not page._busy
        start_date.setEnabled(enabled)
        end_date.setEnabled(enabled)
        tooltip = (
            "期間指定を ON にすると、この日付範囲でバックテストします。"
            if custom_window_box.isChecked()
            else "いまは未使用ですが、ON にするとこの日付範囲を使います。"
        )
        start_date.setToolTip(tooltip)
        end_date.setToolTip(tooltip)

    def set_busy(is_busy: bool) -> None:
        page._busy = is_busy
        run_button.setText("バックテスト実行中..." if is_busy else "バックテスト実行")
        set_button_enabled(run_button, not is_busy, busy=is_busy)
        set_button_enabled(reload_cfg_btn, not is_busy, busy=is_busy)
        strategy_combo.setEnabled(not is_busy)
        starting_cash_input.setEnabled(not is_busy)
        custom_window_box.setEnabled(not is_busy)
        ml_enabled_box.setEnabled(not is_busy)
        ml_mode_combo.setEnabled(not is_busy)
        research_seg.setEnabled(not is_busy)
        tf_seg.setEnabled(not is_busy)
        set_button_enabled(train_btn, not is_busy, busy=is_busy)
        set_button_enabled(research_btn, not is_busy, busy=is_busy)
        update_window_enabled()

    def refresh_views() -> None:
        if app_state.last_result is None:
            run_id_hint.setText("-")
            for tile in metric_tiles.values():
                tile.set_value("-")
            trades_model.set_frame(None)
            signals_model.set_frame(None)
            attribution_model.set_frame(None)
            walk_forward_model.set_frame(None)
            return
        result = app_state.last_result
        run_id_hint.setText(result.run_id)
        total_return = result.metrics.get("total_return", 0)
        metric_tiles["total_return"].set_value(f"{total_return:+.2%}", tone=tone_for(total_return))
        annualized = result.metrics.get("annualized_return", 0)
        metric_tiles["annualized_return"].set_value(f"{annualized:+.2%}", tone=tone_for(annualized))
        drawdown = result.metrics.get("max_drawdown", 0)
        metric_tiles["max_drawdown"].set_value(f"{drawdown:.2%}", tone="neg" if drawdown < 0 else None)
        metric_tiles["win_rate"].set_value(f"{result.metrics.get('win_rate', 0):.2%}")
        metric_tiles["sharpe"].set_value(f"{(result.metrics.get('sharpe') or 0):.2f}")
        metric_tiles["trades"].set_value(str(result.metrics.get("number_of_trades", 0)))
        metric_tiles["avg_hold"].set_value(f"{result.metrics.get('average_hold_bars', 0):.2f}")
        metric_tiles["sample_split"].set_value(
            f"{result.in_sample_metrics.get('total_return', 0):+.2%} / "
            f"{result.out_of_sample_metrics.get('total_return', 0):+.2%}"
        )
        trades_model.set_frame(_trades_frame(result.trades))
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
        available_signal_columns = [c for c in signal_columns if c in result.signals.columns]
        signals_model.set_frame(
            result.signals[available_signal_columns].tail(300)
            if available_signal_columns
            else None
        )
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
                        "総損益": f"{row.get('metrics', {}).get('total_return', 0):+.2%}",
                        "シャープ": f"{(row.get('metrics', {}).get('sharpe') or 0):.2f}",
                        "最大DD": f"{row.get('metrics', {}).get('max_drawdown', 0):.2%}",
                    }
                    for row in result.walk_forward
                ]
            )
        )

    # ---- callbacks ----

    def on_finished(result) -> None:  # noqa: ANN001
        _ = result
        set_busy(False)
        tabs_seg.set_current(0)
        stack.setCurrentIndex(0)
        refresh_views()
        log_message("バックテストが完了しました。")

    def on_error(message: str) -> None:
        set_busy(False)
        run_id_hint.setText("エラー")
        for tile in metric_tiles.values():
            tile.set_value("-")
        log_message(f"バックテストエラー: {message}")

    def persist_fx_controls() -> None:
        app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled = ml_enabled_box.isChecked()
        app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode = str(
            ml_mode_combo.currentData() or "rule_only"
        )
        app_state.config.research.mode = RESEARCH_MODES[research_seg.current()]

    def run_backtest() -> None:
        selected_start = start_date.date().toString("yyyy-MM-dd")
        selected_end = end_date.date().toString("yyyy-MM-dd")
        if selected_start > selected_end:
            on_error("開始日は終了日以前にしてください。")
            return
        try:
            app_state.config.risk.starting_cash = parse_number_input(starting_cash_input, label="初期資産")
        except ValueError as exc:
            on_error(str(exc))
            return
        set_busy(True)
        app_state.config.strategy.name = strategy_combo.currentText()
        app_state.config.backtest.use_custom_window = custom_window_box.isChecked()
        app_state.config.backtest.start_date = selected_start
        app_state.config.backtest.end_date = selected_end
        persist_fx_controls()
        app_state.save_config()
        run_id_hint.setText("実行中…")
        submit_task(app_state.run_backtest, on_finished, on_error)

    def on_train_finished(summary) -> None:  # noqa: ANN001
        set_busy(False)
        refresh_controls()
        model_path = summary.get("latest_model_path") or summary.get("model_path") or ""
        research_status_label.setText(
            "\n".join(
                [
                    f"学習完了: {summary.get('trained_rows', 0)} 行",
                    f"学習期間: {summary.get('train_start', '-')} - {summary.get('train_end', '-')}",
                    f"モデル: {model_path}",
                    f"ラベルデータ: {summary.get('dataset_path', '')}",
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
        uplift = summary.get("uplift", {}).get("total_return_delta", 0.0)
        research_status_label.setText(
            "\n".join(
                [
                    f"research_run 完了: {summary.get('run_id', '-')}",
                    f"出力先: {summary.get('output_dir', '-')}",
                    f"Uplift: {uplift:+.2%}",
                ]
            )
        )
        log_message("research_run が完了しました。")

    def run_research() -> None:
        persist_fx_controls()
        app_state.save_config()
        set_busy(True)
        research_status_label.setText("research_run を実行中...")
        selected_mode = RESEARCH_MODES[research_seg.current()]
        submit_task(
            lambda: app_state.run_research(mode=selected_mode),
            on_research_finished,
            on_error,
        )

    def refresh_page() -> None:
        refresh_controls()
        refresh_views()

    run_button.clicked.connect(run_backtest)
    reload_cfg_btn.clicked.connect(refresh_controls)
    train_btn.clicked.connect(run_train)
    research_btn.clicked.connect(run_research)
    custom_window_box.toggled.connect(lambda _checked=None: update_window_enabled())
    ml_enabled_box.toggled.connect(lambda _checked=None: update_model_status())
    ml_mode_combo.currentIndexChanged.connect(lambda _index=None: update_model_status())

    page.refresh = refresh_page
    refresh_controls()
    refresh_views()
    return page
