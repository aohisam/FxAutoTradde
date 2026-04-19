"""Automation page."""

from __future__ import annotations

import pandas as pd


MODE_LABELS = {
    "local_sim": "ローカルシミュレーション",
    "gmo_sim": "GMO 実時間シミュレーション",
}

STATUS_LABELS = {
    "starting": "開始中",
    "running": "稼働中",
    "stopping": "停止中",
    "stopped": "停止",
    "error": "エラー",
}

CONNECTION_LABELS = {
    "idle": "待機中",
    "connecting": "接続中",
    "connected": "接続済み",
    "polling_only": "ポーリング継続",
    "reconnecting": "再接続中",
    "degraded": "劣化",
}

ACCOUNT_STATUS_LABELS = {
    "active": "利用可能",
    "ready": "準備完了",
    "local_sim_ready": "ローカル準備完了",
    "stopped": "停止",
    "unavailable": "取得不可",
}

ORDER_SIZE_LABELS = {
    "fixed_amount": "定額",
    "equity_fraction": "資産比率",
    "risk_based": "リスク率",
}

SIDE_LABELS = {
    "buy": "買い",
    "sell": "売り",
    "long": "ロング",
    "short": "ショート",
}

ORDER_STATUS_LABELS = {
    "new": "新規",
    "accepted": "受付済み",
    "pending_new": "受付待ち",
    "filled": "約定済み",
    "partially_filled": "一部約定",
    "filled_local_sim": "ローカル約定済み",
    "cancelled": "取消済み",
    "canceled": "取消済み",
    "rejected": "拒否",
    "expired": "期限切れ",
}

SIGNAL_ACTION_LABELS = {
    "buy": "買い",
    "sell": "売り",
    "hold": "様子見",
}

EVENT_LEVEL_LABELS = {
    "info": "情報",
    "warning": "警告",
    "error": "エラー",
    "debug": "デバッグ",
}

COLUMN_LABELS = {
    "fill_id": "約定ID",
    "order_id": "注文ID",
    "symbol": "通貨ペア",
    "qty": "数量",
    "filled_qty": "約定数量",
    "side": "売買",
    "status": "状態",
    "price": "価格",
    "filled_avg_price": "平均約定価格",
    "filled_at": "約定時刻",
    "submitted_at": "注文時刻",
    "timestamp": "時刻",
    "signal_action": "シグナル",
    "signal_score": "スコア",
    "accepted": "採用",
    "explanation_ja": "説明",
    "session_label_ja": "市場セッション",
    "market_value": "時価評価額",
    "unrealized_pl": "含み損益",
    "unrealized_plpc": "含み損益率",
    "current_price": "現在値",
    "avg_entry_price": "平均取得価格",
    "cost_basis": "取得総額",
    "managed_initial_stop_price": "現在の初期ストップ価格",
    "managed_stop_price": "防御ストップ",
    "managed_trailing_stop_price": "トレーリングストップ",
    "managed_active_stop_price": "現在の有効ストップ",
    "managed_partial_target_price": "一部利確目標",
    "managed_partial_reference_price": "一部利確の比較対象高値",
    "managed_reference_bar_at": "比較バー時刻",
    "managed_next_trailing_price": "次のトレーリング価格",
    "managed_trailing_multiple": "トレーリング倍率",
    "managed_bars_held": "保有バー数",
    "managed_partial_taken": "一部利確済み",
    "managed_break_even_armed": "建値防衛",
    "level": "レベル",
    "message_ja": "メッセージ",
    "event": "イベント",
    "reason": "理由",
}

POSITION_COLUMNS = [
    "symbol",
    "qty",
    "side",
    "avg_entry_price",
    "current_price",
    "market_value",
    "unrealized_pl",
    "unrealized_plpc",
    "managed_initial_stop_price",
    "managed_active_stop_price",
    "managed_partial_target_price",
    "managed_partial_reference_price",
    "managed_next_trailing_price",
    "managed_break_even_armed",
    "managed_partial_taken",
    "managed_bars_held",
]

SIGNAL_COLUMNS = [
    "timestamp",
    "symbol",
    "signal_action",
    "signal_score",
    "accepted",
    "session_label_ja",
    "explanation_ja",
]

ORDER_COLUMNS = [
    "submitted_at",
    "symbol",
    "side",
    "qty",
    "filled_qty",
    "filled_avg_price",
    "status",
    "reason",
    "order_id",
]

FILL_COLUMNS = [
    "filled_at",
    "symbol",
    "side",
    "qty",
    "price",
    "order_id",
    "fill_id",
]

EVENT_COLUMNS = [
    "timestamp",
    "level",
    "message_ja",
]


def _label(mapping: dict[str, str], value: object, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value)
    normalized = text.strip().lower()
    return mapping.get(normalized, mapping.get(text, text or default))


def _bool_label(value: object) -> str:
    return "はい" if bool(value) else "いいえ"


def _coerce_float(value: object) -> float | None:
    try:
        return float(value) if value not in {None, ""} else None
    except (TypeError, ValueError):
        return None


def _format_money(value: object, digits: int = 2, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return f"{numeric:,.{digits}f}"


def _format_percent(value: object, digits: int = 2, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return f"{numeric:.{digits}%}"


def _format_count(value: object, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return str(int(round(numeric)))


def _format_timestamp(value: object, default: str = "-") -> str:
    if value in {None, ""}:
        return default
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return str(value)
    return stamp.strftime("%m/%d %H:%M:%S")


def _format_latest_bar_map(latest_bar_summary: dict[str, str], *, multiline: bool = False) -> str:
    if not latest_bar_summary:
        return "-"
    items = [f"{symbol}: {_format_timestamp(timestamp)}" for symbol, timestamp in latest_bar_summary.items()]
    return "\n".join(items) if multiline else " / ".join(items)


def _select_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    ordered = [name for name in columns if name in frame.columns]
    rest = [name for name in frame.columns if name not in ordered]
    return frame.loc[:, ordered + rest]


def _localize_automation_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    localized = frame.copy()
    for column in localized.columns:
        if column == "mode":
            localized[column] = localized[column].map(lambda value: _label(MODE_LABELS, value))
        elif column == "status":
            localized[column] = localized[column].map(
                lambda value: _label(ORDER_STATUS_LABELS | STATUS_LABELS | ACCOUNT_STATUS_LABELS, value)
            )
        elif column == "side":
            localized[column] = localized[column].map(lambda value: _label(SIDE_LABELS, value))
        elif column == "signal_action":
            localized[column] = localized[column].map(lambda value: _label(SIGNAL_ACTION_LABELS, value))
        elif column == "accepted":
            localized[column] = localized[column].map(_bool_label)
        elif column in {"managed_partial_taken", "managed_break_even_armed"}:
            localized[column] = localized[column].map(_bool_label)
        elif column == "level":
            localized[column] = localized[column].map(lambda value: _label(EVENT_LEVEL_LABELS, value))
        elif column == "event":
            localized[column] = localized[column].map(lambda value: _label(ORDER_STATUS_LABELS, value))
    localized = localized.rename(columns={name: COLUMN_LABELS.get(name, name) for name in localized.columns})
    return localized


def build_automation_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt, QTimer
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QMessageBox,
        QPushButton,
        QScrollArea,
        QTabWidget,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile

    DataFrameTableModel = load_dataframe_model_class()

    def configure_table(view: QTableView) -> None:
        view.setAlternatingRowColors(False)
        view.setSelectionBehavior(QAbstractItemView.SelectRows)
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        view.verticalHeader().setVisible(False)
        view.setShowGrid(False)
        header = view.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setDefaultSectionSize(140)
        header.setMinimumSectionSize(84)

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    # Header -----------------------------------------------------------------
    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("実時間シミュレーション")
    title.setProperty("role", "h1")
    subtitle = QLabel("GMO 実時間 / ローカルシミュレーションの運用状況")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)

    status_chip = Chip("停止", "neutral")
    mode_chip = Chip("ローカル", "paper")
    header_right = QHBoxLayout()
    header_right.setSpacing(8)
    header_right.addWidget(mode_chip)
    header_right.addWidget(status_chip)
    header_row.addLayout(header_right)
    layout.addLayout(header_row)

    # Banner / guide ---------------------------------------------------------
    banner_card = Card(sunken=True)
    banner = QLabel()
    banner.setWordWrap(True)
    banner.setProperty("role", "muted")
    banner_card.addBodyWidget(banner)
    guide = QLabel()
    guide.setWordWrap(True)
    guide.setProperty("role", "muted2")
    banner_card.addBodyWidget(guide)
    layout.addWidget(banner_card)

    # Action row -------------------------------------------------------------
    button_row = QHBoxLayout()
    button_row.setSpacing(10)
    start_button = QPushButton("自動売買を開始")
    stop_button = QPushButton("停止")
    kill_button = QPushButton("キルスイッチで停止")
    close_selected_button = QPushButton("選択ポジションを手動決済")
    close_all_button = QPushButton("全ポジションを決済")
    start_button.setProperty("variant", "primary")
    stop_button.setProperty("variant", "ghost")
    kill_button.setProperty("variant", "kill")
    close_selected_button.setProperty("variant", "success")
    close_all_button.setProperty("variant", "success")
    for widget in (start_button, stop_button, kill_button, close_selected_button, close_all_button):
        button_row.addWidget(widget)
    button_row.addStretch(1)
    layout.addLayout(button_row)

    # Metrics grid -----------------------------------------------------------
    metrics_grid = QGridLayout()
    metrics_grid.setHorizontalSpacing(12)
    metrics_grid.setVerticalSpacing(12)
    for column in range(4):
        metrics_grid.setColumnStretch(column, 1)
    metric_specs = [
        ("mode", "運用モード"),
        ("connection", "接続状態"),
        ("market", "市場データ"),
        ("order_size", "注文サイズ"),
        ("equity", "評価資産"),
        ("daily_pl", "日次損益"),
        ("positions", "保有ポジション"),
        ("heartbeat", "更新状況"),
    ]
    page.metric_labels = {}
    page.metric_tiles = {}
    for index, (key, label_text) in enumerate(metric_specs):
        tile = KpiTile(label=label_text, value="-", note="-")
        metrics_grid.addWidget(tile, index // 4, index % 4)
        page.metric_tiles[key] = tile
        page.metric_labels[key] = {"value": tile.value, "note": tile.note}
    layout.addLayout(metrics_grid)

    # Position detail + runtime summary --------------------------------------
    detail_grid = QGridLayout()
    detail_grid.setHorizontalSpacing(14)
    detail_grid.setColumnStretch(0, 3)
    detail_grid.setColumnStretch(1, 2)

    position_card = Card(title="選択ポジションの出口管理", subtitle="初期ストップ・トレーリング・一部利確")
    position_summary = QLabel("保有ポジションを選ぶと、初期ストップや一部利確目標をここで確認できます。")
    position_summary.setWordWrap(True)
    position_summary.setProperty("role", "muted")
    position_card.addBodyWidget(position_summary)

    detail_specs = [
        ("symbol", "選択通貨ペア"),
        ("qty", "保有数量"),
        ("avg_entry_price", "平均取得価格"),
        ("current_price", "現在値"),
        ("market_value", "時価評価額"),
        ("unrealized_pl", "含み損益"),
        ("managed_initial_stop_price", "初期ストップ"),
        ("managed_active_stop_price", "有効ストップ"),
        ("managed_partial_target_price", "一部利確目標"),
        ("managed_partial_reference_price", "比較対象高値"),
        ("managed_next_trailing_price", "次のトレーリング"),
        ("managed_reference_bar_at", "比較バー時刻"),
        ("managed_break_even_armed", "建値防衛"),
        ("managed_partial_taken", "一部利確済み"),
        ("managed_bars_held", "保有バー数"),
    ]
    detail_cells = QGridLayout()
    detail_cells.setHorizontalSpacing(10)
    detail_cells.setVerticalSpacing(10)
    page.position_detail_labels: dict[str, QLabel] = {}
    for index, (key, label_text) in enumerate(detail_specs):
        eyebrow = QLabel(label_text.upper())
        eyebrow.setProperty("role", "eyebrow")
        value_label = QLabel("-")
        value_label.setProperty("role", "detail-value")
        value_label.setWordWrap(True)
        cell = QVBoxLayout()
        cell.setContentsMargins(0, 0, 0, 0)
        cell.setSpacing(2)
        cell.addWidget(eyebrow)
        cell.addWidget(value_label)
        detail_cells.addLayout(cell, index // 3, index % 3)
        page.position_detail_labels[key] = value_label
    position_card.addBodyLayout(detail_cells)
    detail_grid.addWidget(position_card, 0, 0)

    runtime_card = Card(title="実行サマリー", subtitle="ストリーム / 接続 / キルスイッチ")
    runtime_summary = QLabel("停止中")
    runtime_summary.setWordWrap(True)
    runtime_summary.setProperty("role", "muted")
    runtime_card.addBodyWidget(runtime_summary)
    detail_grid.addWidget(runtime_card, 0, 1)
    layout.addLayout(detail_grid)

    # Tabs card --------------------------------------------------------------
    tabs_card = Card(title="一覧", subtitle="ポジション / シグナル / 注文 / 約定 / ログ")
    tabs = QTabWidget()
    tabs.setMinimumHeight(360)
    tabs_card.addBodyWidget(tabs)
    orders_view = QTableView()
    signals_view = QTableView()
    positions_view = QTableView()
    fills_view = QTableView()
    events_view = QTableView()
    orders_model = DataFrameTableModel()
    signals_model = DataFrameTableModel()
    positions_model = DataFrameTableModel()
    fills_model = DataFrameTableModel()
    events_model = DataFrameTableModel()
    for view, model in (
        (orders_view, orders_model),
        (signals_view, signals_model),
        (positions_view, positions_model),
        (fills_view, fills_model),
        (events_view, events_model),
    ):
        view.setModel(model)
        configure_table(view)
    tabs.addTab(positions_view, "現在のポジション")
    tabs.addTab(signals_view, "直近シグナル")
    tabs.addTab(orders_view, "最近の注文")
    tabs.addTab(fills_view, "最近の約定")
    tabs.addTab(events_view, "実行ログ")
    layout.addWidget(tabs_card)
    layout.addStretch(1)

    page._raw_positions_frame = pd.DataFrame()
    page._manual_supported = False
    page._latest_status = "stopped"
    page._busy = False
    page._refresh_timer = QTimer(page)
    page._refresh_timer.setInterval(min(5000, max(2000, app_state.config.automation.poll_interval_seconds * 1000)))

    def set_automation_busy(is_busy: bool, *, pending_start: bool = False) -> None:
        page._busy = is_busy
        if pending_start:
            start_button.setText("開始中...")
            set_button_enabled(start_button, False, busy=True)
            set_button_enabled(stop_button, True)
            set_button_enabled(kill_button, True)
        elif not is_busy and page._latest_status in {"stopped", "error"}:
            set_button_enabled(stop_button, False)
            set_button_enabled(kill_button, False)
        set_button_enabled(
            close_selected_button,
            (selected_position() is not None) and page._manual_supported and not is_busy,
            busy=is_busy,
        )
        set_button_enabled(
            close_all_button,
            bool(page._raw_positions_frame.to_dict("records")) and page._manual_supported and not is_busy,
            busy=is_busy,
        )

    def handle_periodic_refresh() -> None:
        if not page.isVisible():
            return
        if page._latest_status in {"starting", "running", "stopping"} or page._busy:
            refresh_snapshot()
        elif page._refresh_timer.isActive():
            page._refresh_timer.stop()

    def set_metric(key: str, value: str, note: str) -> None:
        page.metric_tiles[key].set_value(value)
        page.metric_tiles[key].set_note(note)

    def selected_position() -> dict[str, object] | None:
        frame = page._raw_positions_frame
        index = positions_view.currentIndex()
        if frame.empty:
            return None
        if not index.isValid():
            return frame.iloc[0].to_dict()
        return frame.iloc[index.row()].to_dict()

    def refresh_position_detail() -> None:
        record = selected_position()
        set_button_enabled(
            close_selected_button,
            record is not None and page._manual_supported and not page._busy,
            busy=page._busy,
        )
        if record is None:
            position_summary.setText(
                "保有ポジションがないため、出口管理値はまだありません。自動売買中にポジションを持つとここに表示されます。"
            )
            for label in page.position_detail_labels.values():
                label.setText("-")
            return
        symbol = str(record.get("symbol", "")).upper() or "-"
        has_managed = any(
            record.get(name) not in {None, "", "-"}
            for name in (
                "managed_initial_stop_price",
                "managed_active_stop_price",
                "managed_partial_target_price",
                "managed_partial_reference_price",
                "managed_next_trailing_price",
            )
        )
        if has_managed:
            position_summary.setText(
                f"{symbol} の出口管理を表示中です。"
                " 一部利確は現在値ではなく、比較バー時刻の最新エントリー足高値と目標価格を比べて判定します。"
            )
        else:
            position_summary.setText(
                f"{symbol} の保有情報を表示中です。停止中の保有または未管理ポジションのため、"
                " 出口管理値は稼働中のみ更新されます。"
            )
        page.position_detail_labels["symbol"].setText(symbol)
        page.position_detail_labels["qty"].setText(_format_count(record.get("qty")))
        page.position_detail_labels["avg_entry_price"].setText(_format_money(record.get("avg_entry_price")))
        page.position_detail_labels["current_price"].setText(_format_money(record.get("current_price")))
        page.position_detail_labels["market_value"].setText(_format_money(record.get("market_value")))
        page.position_detail_labels["unrealized_pl"].setText(
            f"{_format_money(record.get('unrealized_pl'))} / {_format_percent(record.get('unrealized_plpc'))}"
        )
        page.position_detail_labels["managed_initial_stop_price"].setText(
            _format_money(record.get("managed_initial_stop_price"), digits=4)
        )
        page.position_detail_labels["managed_active_stop_price"].setText(
            _format_money(record.get("managed_active_stop_price"), digits=4)
        )
        page.position_detail_labels["managed_partial_target_price"].setText(
            _format_money(record.get("managed_partial_target_price"), digits=4)
        )
        page.position_detail_labels["managed_partial_reference_price"].setText(
            _format_money(record.get("managed_partial_reference_price"), digits=4)
        )
        page.position_detail_labels["managed_next_trailing_price"].setText(
            _format_money(record.get("managed_next_trailing_price"), digits=4)
        )
        page.position_detail_labels["managed_reference_bar_at"].setText(
            _format_timestamp(record.get("managed_reference_bar_at"))
        )
        page.position_detail_labels["managed_break_even_armed"].setText(
            _bool_label(record.get("managed_break_even_armed"))
        )
        page.position_detail_labels["managed_partial_taken"].setText(
            _bool_label(record.get("managed_partial_taken"))
        )
        page.position_detail_labels["managed_bars_held"].setText(_format_count(record.get("managed_bars_held")))

    def refresh_snapshot() -> None:
        gmo_notice = "GMO 実時間データ連動 / 実売買は行いません / 約定はローカルシミュレーションです"
        snapshot = None
        snapshot_error = ""
        try:
            snapshot = app_state.runtime_status_snapshot()
        except Exception as exc:  # pragma: no cover - UI feedback
            snapshot_error = str(exc)
        current_mode = snapshot["mode"] if snapshot is not None else app_state.config.broker.mode.value
        current_status = snapshot["status"] if snapshot is not None else "stopped"
        page._latest_status = current_status
        ready_for_start = current_status in {"stopped", "error"}
        guidance_lines: list[str] = []
        if current_mode == "gmo_sim":
            start_button.setText("GMO 実時間シミュレーションを開始")
            if app_state.config.data.source != "gmo":
                ready_for_start = False
                guidance_lines = [
                    "市場データ設定が不足しています。",
                    "設定画面で運用モードを GMO 実時間シミュレーションに保存し直してください。",
                ]
            else:
                guidance_lines = [
                    "GMO 実時間シミュレーションの準備は完了しています。",
                    "GMO の価格を監視しながら、ローカルで約定・損益計算を行います。",
                    "停止後はポジションを保持しないため、手動決済は稼働中のみ利用できます。",
                ]
        else:
            start_button.setText("自動売買を開始")
            if app_state.config.data.source == "gmo":
                guidance_lines = [
                    "現在はローカルシミュレーションです。",
                    "GMO の実時間データは使いますが、注文はローカル約定です。",
                ]
            elif app_state.config.data.source == "csv":
                guidance_lines = [
                    "現在はローカルシミュレーションです。",
                    "JForex CSV から作成したキャッシュを使うオフライン検証モードです。",
                ]
            else:
                guidance_lines = [
                    "現在はローカルシミュレーションです。",
                    "fixture 履歴を使う検証モードです。",
                ]
        is_running = current_status in {"starting", "running", "stopping"}
        set_button_enabled(
            start_button,
            ready_for_start and not is_running and not page._busy,
            busy=is_running or page._busy,
        )
        set_button_enabled(stop_button, is_running and not page._busy, busy=page._busy)
        set_button_enabled(kill_button, is_running and not page._busy, busy=page._busy)
        guide.setText(" ".join(guidance_lines))

        positions = snapshot.get("positions", []) if snapshot is not None else []
        page._manual_supported = app_state.automation_controller is not None
        set_button_enabled(
            close_all_button,
            bool(positions) and page._manual_supported and not page._busy,
            busy=page._busy,
        )
        if is_running:
            if not page._refresh_timer.isActive():
                page._refresh_timer.start()
        elif page._refresh_timer.isActive() and not page._busy:
            page._refresh_timer.stop()

        # Chip updates
        status_tone_map = {
            "running": "running",
            "starting": "info",
            "stopping": "warn",
            "stopped": "neutral",
            "error": "neg",
        }
        status_chip.set_tone(status_tone_map.get(current_status, "neutral"))
        status_chip.set_text(_label(STATUS_LABELS, current_status))
        mode_chip.set_tone("paper" if current_mode == "local_sim" else "info")
        mode_chip.set_text(_label(MODE_LABELS, current_mode))

        if snapshot is None:
            idle_mode_text = (
                gmo_notice if current_mode == "gmo_sim" else "ローカルシミュレーション"
            )
            banner.setText(f"実時間シミュレーション  •  {idle_mode_text}")
            runtime_summary.setText(f"停止中\n{snapshot_error}" if snapshot_error else "停止中")
            set_metric("mode", _label(MODE_LABELS, current_mode), "開始準備待ち")
            set_metric("connection", "待機中", snapshot_error or "まだ接続確認を行っていません。")
            set_metric(
                "market",
                {
                    "gmo": "GMO 実時間データ",
                    "csv": "JForex CSV キャッシュ",
                    "fixture": "fixture 検証データ",
                }.get(app_state.config.data.source, app_state.config.data.source),
                f"エントリー足: {app_state.config.strategy.entry_timeframe.value}",
            )
            set_metric(
                "order_size",
                _label(ORDER_SIZE_LABELS, app_state.config.risk.order_size_mode.value),
                "開始後に数量計算へ反映されます。",
            )
            set_metric("equity", "-", "口座情報は停止中です。")
            set_metric(
                "daily_pl",
                "-",
                f"上限 {app_state.config.risk.max_daily_loss_amount:.0f} JPY / {app_state.config.risk.max_daily_loss_pct:.1%}",
            )
            set_metric("positions", "0 件", "保有ポジションはありません。")
            set_metric("heartbeat", "停止中", "自動売買はまだ動いていません。")
            page._raw_positions_frame = pd.DataFrame()
            orders_model.set_frame(None)
            signals_model.set_frame(None)
            positions_model.set_frame(None)
            fills_model.set_frame(None)
            events_model.set_frame(None)
            refresh_position_detail()
            return

        banner_lines = [
            "実時間シミュレーション",
            f"モード: {_label(MODE_LABELS, snapshot['mode'])}",
            f"状態: {_label(STATUS_LABELS, snapshot['status'])}",
            f"接続: {_label(CONNECTION_LABELS, snapshot.get('connection_state', '-'))}",
        ]
        if snapshot["mode"] == "gmo_sim":
            banner_lines.append(gmo_notice)
        banner.setText("  •  ".join(banner_lines))

        stream_state = snapshot.get("stream_state", {})
        latest_bar_summary = snapshot.get("latest_market_bar_at", {})
        account_summary = snapshot.get("account_summary", {})
        open_symbols = snapshot.get("open_symbols", [])
        message = account_summary.get("message") or "-"
        equity_value = account_summary.get("equity") or account_summary.get("portfolio_value")
        set_metric(
            "mode",
            _label(MODE_LABELS, snapshot["mode"]),
            f"状態: {_label(STATUS_LABELS, snapshot['status'])}",
        )
        set_metric(
            "connection",
            _label(CONNECTION_LABELS, snapshot.get("connection_state", "-")),
            f"ストリーム: {_bool_label(stream_state.get('connected', False))} / 健全性: {_bool_label(stream_state.get('healthy', False))}",
        )
        set_metric(
            "market",
            {
                "gmo": "GMO 実時間データ",
                "csv": "JForex CSV キャッシュ",
                "fixture": "fixture 検証データ",
            }.get(snapshot.get("data_source"), str(snapshot.get("data_source", "-"))),
            f"エントリー足: {snapshot.get('entry_timeframe', '-')}",
        )
        order_size_mode = snapshot.get("order_size_mode", app_state.config.risk.order_size_mode.value)
        order_size_value = _label(ORDER_SIZE_LABELS, order_size_mode)
        if order_size_mode == "fixed_amount":
            order_note = f"{app_state.config.risk.fixed_order_amount:,.0f} JPY 相当"
        elif order_size_mode == "equity_fraction":
            order_note = f"総資産の {app_state.config.risk.equity_fraction_per_trade:.1%}"
        else:
            order_note = f"想定損失 {app_state.config.risk.risk_per_trade:.1%}"
        set_metric("order_size", order_size_value, order_note)
        set_metric(
            "equity",
            _format_money(equity_value),
            f"口座状態: {_label(ACCOUNT_STATUS_LABELS, account_summary.get('status', 'unknown'))}",
        )
        daily_pl_numeric = _coerce_float(account_summary.get("daily_pl"))
        daily_pl_tone = None
        if daily_pl_numeric is not None:
            daily_pl_tone = "pos" if daily_pl_numeric >= 0 else "neg"
        page.metric_tiles["daily_pl"].set_value(_format_money(account_summary.get("daily_pl")), tone=daily_pl_tone)
        page.metric_tiles["daily_pl"].set_note(
            f"上限 {app_state.config.risk.max_daily_loss_amount:,.0f} JPY / {app_state.config.risk.max_daily_loss_pct:.1%}"
        )
        set_metric(
            "positions",
            f"{len(positions)} 件",
            ", ".join(open_symbols) if open_symbols else "保有なし",
        )
        set_metric(
            "heartbeat",
            _format_timestamp(snapshot.get("heartbeat")),
            f"サイクル {snapshot.get('cycle_count', 0)} / 再接続 {snapshot.get('reconnect_attempts', 0)} 回",
        )

        runtime_summary.setText(
            "\n".join(
                [
                    f"実行ID: {snapshot['run_id'] or '-'}",
                    f"口座メッセージ: {message}",
                    f"最新市場バー:\n{_format_latest_bar_map(latest_bar_summary, multiline=True)}",
                    f"ストリーム最終受信: {_format_timestamp(stream_state.get('last_message_at'))}",
                    f"ストリーム最終エラー: {stream_state.get('last_error', '-') or '-'}",
                    f"最終再接続: {_format_timestamp(snapshot.get('last_reconnect_at'))}",
                    f"キルスイッチ: {snapshot.get('kill_switch_reason', '未発動') or '未発動'}",
                    f"直近アクション: {snapshot['last_actions'] if snapshot.get('last_actions') else 'なし'}",
                ]
            )
        )

        page._raw_positions_frame = pd.DataFrame(snapshot["positions"])
        positions_frame = _select_columns(page._raw_positions_frame, POSITION_COLUMNS)
        signals_frame = _select_columns(pd.DataFrame(snapshot["recent_signals"]), SIGNAL_COLUMNS)
        orders_frame = _select_columns(pd.DataFrame(snapshot["recent_orders"]), ORDER_COLUMNS)
        fills_frame = _select_columns(pd.DataFrame(snapshot["recent_fills"]), FILL_COLUMNS)
        events_frame = _select_columns(pd.DataFrame(snapshot["recent_events"]), EVENT_COLUMNS)
        orders_model.set_frame(_localize_automation_frame(orders_frame))
        signals_model.set_frame(_localize_automation_frame(signals_frame))
        positions_model.set_frame(_localize_automation_frame(positions_frame))
        fills_model.set_frame(_localize_automation_frame(fills_frame))
        events_model.set_frame(_localize_automation_frame(events_frame))
        if not page._raw_positions_frame.empty and not positions_view.currentIndex().isValid():
            positions_view.selectRow(0)
        refresh_position_detail()

    def on_finished(events) -> None:
        app_state.persist_automation_events(events)
        set_automation_busy(False)
        refresh_snapshot()
        log_message("自動売買ループが終了しました。")

    def on_error(message: str) -> None:
        set_automation_busy(False)
        runtime_summary.setText(f"エラー\n{message}")
        log_message(f"自動売買エラー: {message}")

    def start_loop() -> None:
        controller = app_state.start_automation()
        page._latest_status = "starting"
        runtime_summary.setText("開始要求を送信しました。\n自動売買ループを初期化しています。")
        set_automation_busy(True, pending_start=True)
        if not page._refresh_timer.isActive():
            page._refresh_timer.start()
        refresh_snapshot()
        submit_task(controller.run, on_finished, on_error)
        log_message("自動売買を開始しました。")

    def stop_loop() -> None:
        set_automation_busy(True)
        app_state.stop_automation()
        refresh_snapshot()
        log_message("停止要求を送信しました。")

    def close_selected_position() -> None:
        record = selected_position()
        if record is None:
            QMessageBox.information(page, "情報", "決済するポジションを一覧から選択してください。")
            return
        symbol = str(record.get("symbol", "")).upper()
        set_automation_busy(True)
        try:
            result = app_state.manual_close_position(symbol)
        except Exception as exc:  # pragma: no cover - runtime/broker path
            set_automation_busy(False)
            QMessageBox.critical(page, "エラー", f"{symbol} の手動決済に失敗しました。\n{exc}")
            return
        set_automation_busy(False)
        log_message(f"{symbol} を手動決済しました。")
        QMessageBox.information(page, "完了", f"{symbol} の手動決済を送信しました。\n注文ID: {result.get('order_id', '-')}")
        refresh_snapshot()

    def close_all_positions() -> None:
        set_automation_busy(True)
        try:
            result = app_state.manual_close_all_positions()
        except Exception as exc:  # pragma: no cover - runtime/broker path
            set_automation_busy(False)
            QMessageBox.critical(page, "エラー", f"全ポジションの決済に失敗しました。\n{exc}")
            return
        set_automation_busy(False)
        log_message("全ポジションの手動決済を送信しました。")
        QMessageBox.information(
            page,
            "完了",
            f"全ポジションの決済を送信しました。\n件数: {result.get('closed_positions', '-')}",
        )
        refresh_snapshot()

    start_button.clicked.connect(start_loop)
    stop_button.clicked.connect(stop_loop)
    kill_button.clicked.connect(stop_loop)
    close_selected_button.clicked.connect(close_selected_position)
    close_all_button.clicked.connect(close_all_positions)
    positions_view.selectionModel().selectionChanged.connect(lambda *_args: refresh_position_detail())
    page._refresh_timer.timeout.connect(handle_periodic_refresh)
    page.refresh = refresh_snapshot
    refresh_snapshot()
    return page
