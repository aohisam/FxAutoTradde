"""Backtest page."""

from __future__ import annotations

from pathlib import Path


TF_SEG_LABELS = ["5m", "15m", "1h", "4h"]
_LATEST_MODEL_TOKEN = "__LATEST__"


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
    from fxautotrade_lab.desktop.date_inputs import create_popup_date_edit, default_popup_qdate
    from fxautotrade_lab.desktop.ml_labels import (
        ML_MODE_CHOICES,
        RESEARCH_MODE_CHOICES,
        STRATEGY_CHOICES,
        ml_mode_description,
        ml_mode_label,
        research_mode_description,
        research_mode_label,
        strategy_description,
        strategy_label,
    )
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
    subtitle = QLabel("バックテスト設定で単発検証を行い、別カードから ML モデル学習と研究パイプラインを実行できます。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
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

    # ---- Backtest settings card (grid-2) ----
    reload_cfg_btn = QPushButton("前回設定を読込")
    reload_cfg_btn.setProperty("variant", "ghost")
    run_button = QPushButton("バックテスト実行")
    run_button.setProperty("variant", "primary")
    config_tools = QWidget()
    config_tools_layout = QHBoxLayout(config_tools)
    config_tools_layout.setContentsMargins(0, 0, 0, 0)
    config_tools_layout.setSpacing(8)
    config_tools_layout.addWidget(reload_cfg_btn)
    config_tools_layout.addWidget(run_button)
    config_card = Card(title="バックテスト設定", header_right=config_tools)

    config_body = QWidget()
    config_body_layout = QVBoxLayout(config_body)
    config_body_layout.setContentsMargins(0, 0, 0, 0)
    config_body_layout.setSpacing(12)
    config_hint = QLabel(
        "ここで設定した戦略・期間・ML 参加フィルタは、"
        "「バックテスト実行」を押したときだけ使います。未チェック時の期間はデータ同期と同じです。"
    )
    config_hint.setProperty("role", "muted2")
    config_hint.setWordWrap(True)
    config_body_layout.addWidget(config_hint)
    cols = QHBoxLayout()
    cols.setContentsMargins(0, 0, 0, 0)
    cols.setSpacing(16)

    left_form = QFormLayout()
    left_form.setHorizontalSpacing(12)
    left_form.setVerticalSpacing(10)
    left_form.setLabelAlignment(Qt.AlignLeft)

    strategy_combo = QComboBox()
    for key, label in STRATEGY_CHOICES:
        strategy_combo.addItem(label, key)
    strategy_hint = QLabel()
    strategy_hint.setProperty("role", "muted")
    strategy_hint.setWordWrap(True)
    strategy_hint.setMinimumHeight(64)
    strategy_hint.setAlignment(Qt.AlignTop | Qt.AlignLeft)

    starting_cash_input = LabeledSuffixInput(value="", suffix="JPY")
    starting_cash_input.edit.setAlignment(Qt.AlignRight)
    starting_cash_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    custom_window_box = QCheckBox("同期期間とは別にバックテスト期間を指定する")
    ml_enabled_box = QCheckBox("ML 参加フィルタを有効化する")
    ml_mode_combo = QComboBox()
    for key, label in ML_MODE_CHOICES:
        ml_mode_combo.addItem(label, key)
    model_select_combo = QComboBox()
    ml_mode_hint = QLabel()
    ml_mode_hint.setProperty("role", "muted")
    ml_mode_hint.setWordWrap(True)
    ml_mode_hint.setMinimumHeight(72)
    ml_mode_hint.setAlignment(Qt.AlignTop | Qt.AlignLeft)
    model_status_label = QLabel("-")
    model_status_label.setProperty("role", "detail-value")
    model_status_label.setWordWrap(True)
    model_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)

    left_form.addRow(labeled("戦略", "実行に使う売買ロジック"), strategy_combo)
    left_form.addRow(labeled("戦略説明"), strategy_hint)
    left_form.addRow(labeled("初期資産", "JPY 基準"), starting_cash_input)
    left_form.addRow(labeled("ML 参加フィルタ"), ml_enabled_box)
    left_form.addRow(labeled("ML の使い方"), ml_mode_combo)
    left_form.addRow(labeled("使用モデル"), model_select_combo)

    right_form = QFormLayout()
    right_form.setHorizontalSpacing(12)
    right_form.setVerticalSpacing(10)
    right_form.setLabelAlignment(Qt.AlignLeft)

    start_date = create_popup_date_edit("start")
    end_date = create_popup_date_edit("end")
    tf_seg = SegmentedControl(TF_SEG_LABELS, current=1, data=TF_SEG_LABELS)

    right_form.addRow(labeled("期間指定"), custom_window_box)
    right_form.addRow(labeled("開始日"), start_date)
    right_form.addRow(labeled("終了日"), end_date)
    right_form.addRow(labeled("タイムフレーム"), tf_seg)
    right_form.addRow(labeled("バックテスト用 ML 説明"), ml_mode_hint)
    right_form.addRow(labeled("バックテストでの ML 状態"), model_status_label)

    left_wrap = QWidget()
    left_wrap.setLayout(left_form)
    right_wrap = QWidget()
    right_wrap.setLayout(right_form)
    cols.addWidget(left_wrap, 1)
    cols.addWidget(right_wrap, 1)
    config_body_layout.addLayout(cols)
    config_card.addBodyWidget(config_body)
    layout.addWidget(config_card)

    # ---- ML / Research card ----
    train_btn = QPushButton("ML モデル学習")
    train_btn.setProperty("variant", "success")
    research_btn = QPushButton("研究パイプライン実行")
    research_btn.setProperty("variant", "ghost")
    ml_tools = QWidget()
    mt = QHBoxLayout(ml_tools)
    mt.setContentsMargins(0, 0, 0, 0)
    mt.setSpacing(8)
    ml_hint = QLabel("ここでは保存モデルの更新と研究レポート生成を行います。バックテスト用の ML 設定とは独立しています。")
    ml_hint.setProperty("role", "muted2")
    ml_hint.setWordWrap(True)
    mt.addWidget(ml_hint)
    mt.addSpacing(8)
    mt.addWidget(train_btn)
    mt.addWidget(research_btn)
    ml_card = Card(title="ML / Research", header_right=ml_tools)

    ml_body = QWidget()
    ml_cols = QHBoxLayout(ml_body)
    ml_cols.setContentsMargins(0, 0, 0, 0)
    ml_cols.setSpacing(16)
    research_seg = SegmentedControl(
        [label for _, label in RESEARCH_MODE_CHOICES],
        current=1,
        data=[key for key, _ in RESEARCH_MODE_CHOICES],
    )
    research_mode_hint = QLabel()
    research_mode_hint.setProperty("role", "muted")
    research_mode_hint.setWordWrap(True)
    research_mode_hint.setMinimumHeight(72)
    research_mode_hint.setAlignment(Qt.AlignTop | Qt.AlignLeft)
    action_help_label = QLabel()
    action_help_label.setProperty("role", "muted")
    action_help_label.setWordWrap(True)
    action_help_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)

    ml_left = QFormLayout()
    ml_left.setHorizontalSpacing(12)
    ml_left.setVerticalSpacing(10)
    ml_left.setLabelAlignment(Qt.AlignLeft)
    ml_left.addRow(labeled("Research モード"), research_seg)
    ml_left.addRow(labeled("Research の説明"), research_mode_hint)
    ml_left.addRow(labeled("各ボタンの用途"), action_help_label)

    ml_right_cell = QWidget()
    mrc = QVBoxLayout(ml_right_cell)
    mrc.setContentsMargins(0, 0, 0, 0)
    mrc.setSpacing(6)
    task_title = QLabel("処理状態")
    task_title.setProperty("role", "eyebrow")
    task_status_label = QLabel("まだバックテスト / ML 学習 / 研究パイプラインは実行していません。")
    task_status_label.setProperty("role", "muted")
    task_status_label.setWordWrap(True)
    task_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    mrc.addWidget(task_title)
    mrc.addWidget(task_status_label)
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
    page._active_task = None

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

    def parse_qdate(value: str, fallback: QDate) -> QDate:
        parsed = QDate.fromString(value, "yyyy-MM-dd")
        if parsed.isValid():
            return parsed
        return fallback

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

    def selected_strategy_name() -> str:
        return str(strategy_combo.currentData() or app_state.config.strategy.name)

    def supports_fx_ml_research() -> bool:
        return selected_strategy_name() == "fx_breakout_pullback"

    def selected_ml_mode() -> str:
        return str(ml_mode_combo.currentData() or app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)

    def uses_saved_model_selection() -> bool:
        return supports_fx_ml_research() and ml_enabled_box.isChecked() and selected_ml_mode() == "load_pretrained"

    def refresh_model_choices() -> None:
        status = app_state.model_status()
        selected_key = str(status.get("selected_model_key") or _LATEST_MODEL_TOKEN)
        model_select_combo.blockSignals(True)
        model_select_combo.clear()
        if not supports_fx_ml_research():
            model_select_combo.addItem("この戦略では使用しません", "")
        elif not ml_enabled_box.isChecked():
            model_select_combo.addItem("ML 無効のため使用しません", "")
        elif selected_ml_mode() == "rule_only":
            model_select_combo.addItem("ルールのみのため使用しません", "")
        elif selected_ml_mode() == "train_from_scratch":
            model_select_combo.addItem("今回その場で学習したモデルを使います", "")
        elif selected_ml_mode() == "walk_forward_train":
            model_select_combo.addItem("各期間でその都度学習したモデルを使います", "")
        else:
            available_models = list(status.get("available_models") or [])
            if not available_models:
                model_select_combo.addItem("保存済みモデルがありません", _LATEST_MODEL_TOKEN)
            else:
                for entry in available_models:
                    label = str(entry.get("label") or entry.get("path") or "-")
                    if not bool(entry.get("exists")):
                        label = f"{label} / 未作成"
                    model_select_combo.addItem(label, str(entry.get("key") or ""))
                _set_combo_by_data(model_select_combo, selected_key)
                if model_select_combo.currentIndex() < 0:
                    model_select_combo.setCurrentIndex(0)
        model_select_combo.blockSignals(False)

    def refresh_controls() -> None:
        _set_combo_by_data(strategy_combo, app_state.config.strategy.name)
        custom_window_box.setChecked(app_state.config.backtest.use_custom_window)
        starting_cash_input.edit.setText(format_number(app_state.config.risk.starting_cash, 2))
        ml_enabled_box.setChecked(app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled)
        _set_combo_by_data(ml_mode_combo, app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode)
        current_mode = app_state.config.research.mode
        research_seg.setCurrentData(current_mode)
        start_date.setDate(
            parse_qdate(
                app_state.config.backtest.start_date or app_state.config.data.start_date,
                default_popup_qdate("start"),
            )
        )
        end_date.setDate(
            parse_qdate(
                app_state.config.backtest.end_date or app_state.config.data.end_date,
                default_popup_qdate("end"),
            )
        )
        try:
            entry_tf = app_state.config.strategy.entry_timeframe.value
            mapping = {"5Min": 0, "15Min": 1, "1Hour": 2, "4Hour": 3}
            if entry_tf in mapping:
                tf_seg.set_current(mapping[entry_tf])
        except Exception:  # noqa: BLE001
            pass
        update_strategy_status()
        refresh_model_choices()
        update_model_status()
        update_research_mode_status()
        update_action_help()
        update_action_availability()
        if app_state.last_research_result is not None:
            task_status_label.setText(
                "\n".join(
                    [
                        f"最新 research_run: {app_state.last_research_result.get('run_id', '-')}",
                        f"出力先: {app_state.last_research_result.get('output_dir', '-')}",
                        f"モード: {research_mode_label(app_state.last_research_result.get('mode', '-'))}",
                    ]
                )
            )
        update_window_enabled()

    def update_strategy_status() -> None:
        selected_strategy = selected_strategy_name()
        lines = [strategy_description(selected_strategy)]
        if not supports_fx_ml_research():
            lines.append("この戦略では ML モデル学習と研究パイプラインは使いません。利用するには「FX ブレイクアウト押し目」を選んでください。")
        strategy_hint.setText("\n".join(line for line in lines if line))

    def update_model_status() -> None:
        status = app_state.model_status()
        selected_enabled = ml_enabled_box.isChecked()
        selected_mode = selected_ml_mode()
        if not supports_fx_ml_research():
            model_status_label.setText(
                "現在の戦略ではバックテスト用 ML 参加フィルタを使いません。\n"
                "ML モデル学習 / 研究パイプラインを使う場合は、戦略を「FX ブレイクアウト押し目」に切り替えてください。"
            )
            ml_mode_hint.setText("この戦略では ML モード設定は未使用です。")
            return
        applies_ml = selected_enabled and selected_mode != "rule_only"
        selected_model_line = ""
        if selected_mode == "load_pretrained":
            selected_model_line = f"使用する保存済みモデル: {status.get('selected_model_label', '-')}"
        elif selected_mode == "rule_only":
            selected_model_line = "使用モデル: ルールのみのため保存済みモデルは使いません。"
        elif selected_mode == "train_from_scratch":
            selected_model_line = "使用モデル: 今回のバックテスト専用に、その場で学習した一時モデルを使います。"
        elif selected_mode == "walk_forward_train":
            selected_model_line = "使用モデル: 各検証窓ごとに学習した一時モデルを順番に使います。"
        lines = [
            "この欄はバックテスト実行時の ML 参加フィルタだけを説明します。",
            f"ML スイッチ: {'有効' if selected_enabled else '無効'}",
            f"このバックテストで ML を使う: {'はい' if applies_ml else 'いいえ'}",
            f"ML モード: {ml_mode_label(selected_mode)}",
            ml_mode_description(selected_mode),
            selected_model_line,
            f"モデル保存先: {status.get('model_path', '-')}",
            f"存在: {'あり' if status.get('exists') else 'なし'}",
        ]
        model_status_label.setText("\n".join(line for line in lines if line))
        ml_mode_hint.setText(ml_mode_description(selected_mode))

    def update_research_mode_status() -> None:
        selected_mode = str(research_seg.currentData() or app_state.config.research.mode)
        research_mode_hint.setText(research_mode_description(selected_mode))

    def update_action_help() -> None:
        lines = [
            "バックテスト設定カードの「ML 参加フィルタ / ML の使い方 / 使用モデル」は、バックテスト実行時だけ使います。",
            "バックテスト実行: 現在の設定で 1 回だけ検証し、取引結果を確認します。",
            "  バックテスト実行では保存済み latest モデルは更新しません。学習が必要なモードでも、その回の検証専用モデルとして扱います。",
            "ML モデル学習: FX breakout 戦略用の ML フィルタだけを学習して保存します。保存済み latest モデルを更新します。バックテスト用 ML 設定には影響されません。",
            "研究パイプライン実行: データ検証、学習、ベースライン比較、頑健性チェック、感度分析、レポート出力をまとめて実行します。Research モードだけを見て処理を分け、研究内の学習ステップで latest モデルを更新します。",
        ]
        if not supports_fx_ml_research():
            lines.append("現在の戦略では下2つは使えません。FX ブレイクアウト押し目戦略に切り替えると有効になります。")
        action_help_label.setText("\n".join(lines))

    def update_action_availability() -> None:
        can_edit_backtest_ml = supports_fx_ml_research() and not page._busy
        can_run_ml = supports_fx_ml_research() and not page._busy
        can_select_saved_model = can_edit_backtest_ml and uses_saved_model_selection()
        unsupported_tooltip = (
            ""
            if supports_fx_ml_research()
            else "ML モデル学習と研究パイプラインは FX ブレイクアウト押し目戦略でのみ利用できます。"
        )
        ml_enabled_box.setEnabled(can_edit_backtest_ml)
        ml_mode_combo.setEnabled(can_edit_backtest_ml)
        model_select_combo.setEnabled(can_select_saved_model)
        research_seg.setEnabled(can_run_ml)
        ml_enabled_box.setToolTip(unsupported_tooltip)
        ml_mode_combo.setToolTip(unsupported_tooltip)
        if not supports_fx_ml_research():
            model_select_combo.setToolTip(unsupported_tooltip)
        elif not ml_enabled_box.isChecked():
            model_select_combo.setToolTip("ML 無効のため、保存済みモデルは使いません。")
        elif selected_ml_mode() == "rule_only":
            model_select_combo.setToolTip("ルールのみのため、保存済みモデルは使いません。")
        elif selected_ml_mode() != "load_pretrained":
            model_select_combo.setToolTip("この ML モードでは保存済みモデルではなく、その場で学習したモデルを使います。")
        else:
            model_select_combo.setToolTip("load_pretrained で使う保存済みモデルを選びます。")
        research_seg.setToolTip(unsupported_tooltip)
        if page._busy:
            set_button_enabled(train_btn, False, busy=True)
            set_button_enabled(research_btn, False, busy=True)
        else:
            set_button_enabled(train_btn, can_run_ml, busy=False)
            set_button_enabled(research_btn, can_run_ml, busy=False)
            train_btn.setToolTip(unsupported_tooltip)
            research_btn.setToolTip(unsupported_tooltip)

    def update_window_enabled() -> None:
        enabled = not page._busy and custom_window_box.isChecked()
        start_date.setEnabled(enabled)
        end_date.setEnabled(enabled)
        tooltip = (
            "期間指定を ON にすると、この日付範囲でバックテストします。"
            if custom_window_box.isChecked()
            else "いまは未使用です。期間指定を ON にすると、この日付を選べます。"
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
        tf_seg.setEnabled(not is_busy)
        update_window_enabled()
        update_action_availability()

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

    def _task_title(task_key: str | None) -> str:
        return {
            "backtest": "バックテスト",
            "train": "ML モデル学習",
            "research": "研究パイプライン",
        }.get(task_key or "", "処理")

    def on_progress(payload) -> None:  # noqa: ANN001
        if not isinstance(payload, dict):
            return
        active_task = page._active_task
        title_text = _task_title(active_task)
        current = int(payload.get("current", 0) or 0)
        total = int(payload.get("total", 0) or 0)
        message = str(payload.get("message", "")).strip() or f"{title_text}を実行中です。"
        phase = str(payload.get("phase", "running") or "running")
        if total > 0:
            run_id_hint.setText(f"{min(current, total)}/{total}")
        else:
            run_id_hint.setText("実行中…")
        lines = [f"{title_text}を実行中..."]
        if total > 0:
            lines.append(f"進行: {min(current, total)}/{total}")
        lines.append(message)
        if phase == "done":
            lines[0] = f"{title_text}が完了しました。"
        task_status_label.setText("\n".join(lines))

    def on_finished(result) -> None:  # noqa: ANN001
        _ = result
        page._active_task = None
        set_busy(False)
        tabs_seg.set_current(0)
        stack.setCurrentIndex(0)
        refresh_views()
        task_status_label.setText("バックテストが完了しました。")
        log_message("バックテストが完了しました。")

    def on_error(message: str) -> None:
        active_task = page._active_task
        page._active_task = None
        set_busy(False)
        run_id_hint.setText("エラー")
        for tile in metric_tiles.values():
            tile.set_value("-")
        if active_task == "research":
            task_status_label.setText(f"研究パイプラインでエラーが発生しました。\n{message}")
        elif active_task == "train":
            task_status_label.setText(f"ML モデル学習でエラーが発生しました。\n{message}")
        elif active_task == "backtest":
            task_status_label.setText(f"バックテストでエラーが発生しました。\n{message}")
        log_message(f"{_task_title(active_task)}エラー: {message}")

    def persist_shared_controls() -> None:
        app_state.config.strategy.name = selected_strategy_name()

    def persist_backtest_controls() -> None:
        persist_shared_controls()
        app_state.config.strategy.fx_breakout_pullback.ml_filter.enabled = ml_enabled_box.isChecked()
        app_state.config.strategy.fx_breakout_pullback.ml_filter.backtest_mode = str(
            ml_mode_combo.currentData() or "rule_only"
        )
        selected_model_key = str(model_select_combo.currentData() or "").strip()
        app_state.config.strategy.fx_breakout_pullback.ml_filter.pretrained_model_path = (
            None
            if not selected_model_key or selected_model_key == _LATEST_MODEL_TOKEN
            else Path(selected_model_key)
        )

    def persist_research_controls() -> None:
        persist_shared_controls()
        app_state.config.research.mode = str(research_seg.currentData() or "standard")

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
        page._active_task = "backtest"
        set_busy(True)
        app_state.config.backtest.use_custom_window = custom_window_box.isChecked()
        app_state.config.backtest.start_date = selected_start
        app_state.config.backtest.end_date = selected_end
        persist_backtest_controls()
        app_state.save_config()
        run_id_hint.setText("実行中…")
        task_status_label.setText("バックテストを実行中...\n準備しています。")
        submit_task(app_state.run_backtest, on_finished, on_error, on_progress)

    def on_train_finished(summary) -> None:  # noqa: ANN001
        page._active_task = None
        set_busy(False)
        refresh_controls()
        model_path = summary.get("latest_model_path") or summary.get("model_path") or ""
        task_status_label.setText(
            "\n".join(
                [
                    f"学習完了: {summary.get('trained_rows', 0)} 行",
                    f"学習期間: {summary.get('train_start', '-')} - {summary.get('train_end', '-')}",
                    f"モデル: {model_path}",
                    f"ラベルデータ: {summary.get('dataset_path', '')}",
                ]
            )
        )
        log_message("ML モデル学習が完了しました。")

    def run_train() -> None:
        if not supports_fx_ml_research():
            on_error("ML モデル学習は FX ブレイクアウト押し目戦略でのみ利用できます。")
            return
        persist_shared_controls()
        app_state.save_config()
        page._active_task = "train"
        set_busy(True)
        task_status_label.setText(
            "ML モデル学習を実行中...\n"
            "保存済みモデルを更新し、次回の load_pretrained などで使えるようにします。\n"
            "バックテスト設定カードの ML 設定には影響されません。"
        )
        submit_task(app_state.train_fx_model, on_train_finished, on_error, on_progress)

    def on_research_finished(summary) -> None:  # noqa: ANN001
        page._active_task = None
        set_busy(False)
        refresh_controls()
        uplift = summary.get("uplift", {}).get("total_return_delta", 0.0)
        task_status_label.setText(
            "\n".join(
                [
                    f"research_run 完了: {summary.get('run_id', '-')}",
                    f"出力先: {summary.get('output_dir', '-')}",
                    f"Uplift: {uplift:+.2%}",
                ]
            )
        )
        log_message("研究パイプラインが完了しました。")

    def run_research() -> None:
        if not supports_fx_ml_research():
            on_error("研究パイプラインは FX ブレイクアウト押し目戦略でのみ利用できます。")
            return
        persist_research_controls()
        app_state.save_config()
        page._active_task = "research"
        set_busy(True)
        selected_mode = str(research_seg.currentData() or "standard")
        task_status_label.setText(
            "研究パイプラインを実行中...\n"
            f"モード: {research_mode_label(selected_mode)}\n"
            "バックテスト設定カードの ML 設定ではなく、Research モードに従って進みます。"
        )
        submit_task(
            lambda: app_state.run_research(mode=selected_mode),
            on_research_finished,
            on_error,
            on_progress,
        )

    def refresh_page() -> None:
        refresh_controls()
        refresh_views()

    run_button.clicked.connect(run_backtest)
    reload_cfg_btn.clicked.connect(refresh_controls)
    train_btn.clicked.connect(run_train)
    research_btn.clicked.connect(run_research)
    custom_window_box.toggled.connect(lambda _checked=None: update_window_enabled())
    ml_enabled_box.toggled.connect(lambda _checked=None: refresh_model_choices())
    ml_enabled_box.toggled.connect(lambda _checked=None: update_model_status())
    ml_enabled_box.toggled.connect(lambda _checked=None: update_action_availability())
    ml_mode_combo.currentIndexChanged.connect(lambda _index=None: refresh_model_choices())
    ml_mode_combo.currentIndexChanged.connect(lambda _index=None: update_model_status())
    ml_mode_combo.currentIndexChanged.connect(lambda _index=None: update_action_availability())
    strategy_combo.currentIndexChanged.connect(lambda _index=None: update_strategy_status())
    strategy_combo.currentIndexChanged.connect(lambda _index=None: update_model_status())
    strategy_combo.currentIndexChanged.connect(lambda _index=None: update_action_help())
    strategy_combo.currentIndexChanged.connect(lambda _index=None: refresh_model_choices())
    strategy_combo.currentIndexChanged.connect(lambda _index=None: update_action_availability())
    model_select_combo.currentIndexChanged.connect(lambda _index=None: update_model_status())
    research_seg.currentChanged.connect(lambda _index=None: update_research_mode_status())

    page.refresh = refresh_page
    refresh_controls()
    refresh_views()
    return page
