"""Automation page."""

from __future__ import annotations

import html as html_mod

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
    "long": "買い",
    "short": "売り",
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

EVENT_LEVEL_KEYS = {
    "情報": "INFO",
    "警告": "WARN",
    "エラー": "ERROR",
    "デバッグ": "INFO",
    "info": "INFO",
    "warning": "WARN",
    "error": "ERROR",
    "debug": "INFO",
}


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


def _format_signed_money(value: object, digits: int = 0, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return f"{numeric:+,.{digits}f}"


def _format_percent(value: object, digits: int = 2, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return f"{numeric:.{digits}%}"


def _format_signed_percent(value: object, digits: int = 2, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return f"{numeric:+.{digits}%}"


def _format_count(value: object, default: str = "-") -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return default
    return str(int(round(numeric)))


def _format_timestamp(value: object, default: str = "-", *, fmt: str = "%m/%d %H:%M:%S") -> str:
    if value in {None, ""}:
        return default
    try:
        stamp = pd.Timestamp(value)
    except Exception:  # noqa: BLE001
        return str(value)
    return stamp.strftime(fmt)


def _format_latest_bars(latest_bar_summary: dict[str, str]) -> str:
    if not latest_bar_summary:
        return "-"
    lines = [
        f"{symbol}: {_format_timestamp(timestamp, fmt='%m/%d %H:%M')}"
        for symbol, timestamp in latest_bar_summary.items()
    ]
    return "\n".join(lines[:3])


def _positions_frame(rows):
    if not rows:
        return None
    out = []
    for row in rows:
        pnl_val = _coerce_float(row.get("unrealized_pl"))
        pnl_pct = _coerce_float(row.get("unrealized_plpc"))
        parts = []
        if pnl_val is not None:
            parts.append(f"{pnl_val:+,.0f}")
        if pnl_pct is not None:
            parts.append(f"{pnl_pct:+.2%}")
        pnl_combined = " / ".join(parts) if parts else "-"
        out.append(
            {
                "通貨ペア": str(row.get("symbol", "")).upper() or "-",
                "売買": _label(SIDE_LABELS, row.get("side")),
                "数量": _format_count(row.get("qty")),
                "平均取得": _format_money(row.get("avg_entry_price")),
                "現在値": _format_money(row.get("current_price")),
                "時価評価": _format_money(row.get("market_value")),
                "含み損益": pnl_combined,
                "有効ストップ": _format_money(row.get("managed_active_stop_price"), digits=4),
                "次トレール": _format_money(row.get("managed_next_trailing_price"), digits=4),
                "保有バー": _format_count(row.get("managed_bars_held")),
            }
        )
    return pd.DataFrame(out)


def _signals_frame(rows):
    if not rows:
        return None
    out = []
    for row in rows:
        out.append(
            {
                "時刻": _format_timestamp(row.get("timestamp"), fmt="%m/%d %H:%M:%S"),
                "通貨ペア": str(row.get("symbol", "")),
                "シグナル": _label(SIGNAL_ACTION_LABELS, row.get("signal_action")),
                "スコア": f"{_coerce_float(row.get('signal_score')) or 0:.2f}",
                "採用": _bool_label(row.get("accepted")),
                "市場セッション": str(row.get("session_label_ja") or "-"),
                "説明": str(row.get("explanation_ja") or ""),
            }
        )
    return pd.DataFrame(out)


def _orders_frame(rows):
    if not rows:
        return None
    out = []
    for row in rows:
        out.append(
            {
                "注文時刻": _format_timestamp(row.get("submitted_at")),
                "通貨ペア": str(row.get("symbol", "")),
                "売買": _label(SIDE_LABELS, row.get("side")),
                "数量": _format_count(row.get("qty")),
                "約定数量": _format_count(row.get("filled_qty")),
                "平均価格": _format_money(row.get("filled_avg_price")),
                "状態": _label(ORDER_STATUS_LABELS, row.get("status")),
                "理由": str(row.get("reason") or "-"),
            }
        )
    return pd.DataFrame(out)


def _fills_frame(rows):
    if not rows:
        return None
    out = []
    for row in rows:
        out.append(
            {
                "約定時刻": _format_timestamp(row.get("filled_at")),
                "通貨ペア": str(row.get("symbol", "")),
                "売買": _label(SIDE_LABELS, row.get("side")),
                "数量": _format_count(row.get("qty")),
                "価格": _format_money(row.get("price")),
                "注文ID": str(row.get("order_id") or "-"),
            }
        )
    return pd.DataFrame(out)


def _events_frame(rows):
    if not rows:
        return None
    out = []
    for row in rows:
        out.append(
            {
                "時刻": _format_timestamp(row.get("timestamp")),
                "レベル": _label(EVENT_LEVEL_LABELS, row.get("level")),
                "メッセージ": str(row.get("message_ja") or ""),
            }
        )
    return pd.DataFrame(out)


def build_automation_page(app_state, submit_task, log_message):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt, QTimer, Signal
    from PySide6.QtGui import QColor, QPainter, QPen
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
        QStackedWidget,
        QStyledItemDelegate,
        QTableView,
        QTextBrowser,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.banner import Banner
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile

    DataFrameTableModel = load_dataframe_model_class()

    # ---- Helper widgets -------------------------------------------------------

    class DetailGrid(QWidget):
        def __init__(self, columns: int = 2, parent: QWidget | None = None) -> None:
            super().__init__(parent)
            self.columns = max(1, columns)
            self._grid = QGridLayout(self)
            self._grid.setHorizontalSpacing(18)
            self._grid.setVerticalSpacing(10)
            self._grid.setContentsMargins(0, 0, 0, 0)
            for column in range(self.columns):
                self._grid.setColumnStretch(column, 1)
            self._rows: dict[str, QLabel] = {}

        def add(self, key: str, label_text: str) -> None:
            cell = QWidget()
            cell_lay = QVBoxLayout(cell)
            cell_lay.setContentsMargins(0, 0, 0, 0)
            cell_lay.setSpacing(2)
            eyebrow = QLabel(label_text.upper())
            eyebrow.setProperty("role", "eyebrow")
            value = QLabel("-")
            value.setProperty("role", "detail-value")
            value.setWordWrap(True)
            cell_lay.addWidget(eyebrow)
            cell_lay.addWidget(value)
            idx = len(self._rows)
            self._grid.addWidget(cell, idx // self.columns, idx % self.columns)
            self._rows[key] = value

        def set(self, key: str, text: str, *, tone: str | None = None) -> None:
            value = self._rows.get(key)
            if value is None:
                return
            value.setText(text)
            value.setProperty("tone", tone or "")
            style = value.style()
            if style is not None:
                style.unpolish(value)
                style.polish(value)

    class CountedTabs(QWidget):
        currentChanged = Signal(int)

        def __init__(self, labels: list[str], parent: QWidget | None = None) -> None:
            super().__init__(parent)
            row = QHBoxLayout(self)
            row.setContentsMargins(12, 8, 12, 0)
            row.setSpacing(4)
            self._labels = list(labels)
            self._counts = [0] * len(labels)
            self._buttons: list[QPushButton] = []
            for index, name in enumerate(labels):
                button = QPushButton(self._format_label(name, 0))
                button.setCheckable(True)
                button.setProperty("role", "tab")
                button.setCursor(Qt.PointingHandCursor)
                button.clicked.connect(lambda _=False, i=index: self.set_current(i))
                row.addWidget(button)
                self._buttons.append(button)
            row.addStretch(1)
            self._current = 0
            if self._buttons:
                self._buttons[0].setChecked(True)

        def _format_label(self, name: str, count: int) -> str:
            return f"{name}  {count:,}" if count else f"{name}"

        def set_count(self, index: int, count: int) -> None:
            if 0 <= index < len(self._counts):
                self._counts[index] = count
                self._buttons[index].setText(self._format_label(self._labels[index], count))

        def set_current(self, index: int) -> None:
            if not (0 <= index < len(self._buttons)):
                return
            for position, button in enumerate(self._buttons):
                button.setChecked(position == index)
            if index != self._current:
                self._current = index
            self.currentChanged.emit(index)

        def current(self) -> int:
            return self._current

    _TONE_COLOR = {
        "running": Tokens.POS,
        "info": Tokens.INFO,
        "warn": Tokens.WARN,
        "neg": Tokens.NEG,
        "neutral": Tokens.MUTED_2,
        "accent": Tokens.ACCENT,
    }

    class ChipDelegate(QStyledItemDelegate):
        def __init__(self, parent=None, tone_for=None):
            super().__init__(parent)
            self._tone_for = tone_for or (lambda _text: None)

        def paint(self, painter, option, index):
            text = str(index.data() or "")
            tone = self._tone_for(text) if text else None
            if not tone:
                super().paint(painter, option, index)
                return
            tone_color = _TONE_COLOR.get(tone, Tokens.MUTED_2)
            painter.save()
            painter.setRenderHint(QPainter.Antialiasing)
            rect = option.rect.adjusted(6, 4, -6, -4)
            bg = QColor(tone_color)
            bg.setAlphaF(0.10)
            border = QColor(tone_color)
            border.setAlphaF(0.30)
            painter.setBrush(bg)
            painter.setPen(QPen(border, 1))
            radius = rect.height() / 2
            painter.drawRoundedRect(rect, radius, radius)
            painter.setPen(QColor(tone_color))
            painter.drawText(rect.adjusted(10, 0, -10, 0), Qt.AlignVCenter | Qt.AlignLeft, text)
            painter.restore()

    class MonoRightDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")

    class MonoSignedDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")
            text = str(index.data(Qt.DisplayRole) or "").lstrip()
            if text.startswith("+"):
                option.palette.setColor(option.palette.Text, QColor(Tokens.POS))
            elif text.startswith(("-", "−")):
                option.palette.setColor(option.palette.Text, QColor(Tokens.NEG))

    def signal_tone(text: str) -> str | None:
        return "accent" if text in ("買い", "売り") else None

    def order_status_tone(text: str) -> str | None:
        mapping = {
            "約定済み": "neutral",
            "ローカル約定済み": "running",
            "一部約定": "warn",
            "取消済み": "neutral",
            "拒否": "neg",
            "新規": "info",
            "受付済み": "info",
            "受付待ち": "info",
            "期限切れ": "warn",
        }
        return mapping.get(text)

    def level_tone(text: str) -> str | None:
        mapping = {
            "情報": "info",
            "警告": "warn",
            "エラー": "neg",
            "デバッグ": "neutral",
        }
        return mapping.get(text)

    # ---- Page scaffolding -----------------------------------------------------

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)
    page._delegates = []  # type: ignore[attr-defined]

    # ---- Header ----
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("実時間シミュレーション")
    title.setProperty("role", "h1")
    subtitle = QLabel(
        "GMO 実時間データ連動 · 実売買は行いません · 約定はローカルシミュレーションです"
    )
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    status_running = Chip("停止中", "neutral")
    status_gmo = Chip("GMO 未接続", "warn")
    header.addWidget(status_running)
    header.addWidget(status_gmo)
    layout.addLayout(header)

    # ---- Banner ----
    banner = Banner(
        "GMO 実時間シミュレーションの準備は完了しています。"
        "GMO の価格を監視しながら、ローカルで約定・損益計算を行います。"
        " 停止後はポジションを保持しないため、手動決済は稼働中のみ利用できます。"
    )
    layout.addWidget(banner)

    # ---- Action row ----
    action_row = QHBoxLayout()
    action_row.setSpacing(8)
    action_row.setContentsMargins(0, 0, 0, 0)
    start_button = QPushButton("自動売買を開始")
    start_button.setProperty("variant", "primary")
    stop_button = QPushButton("停止")
    stop_button.setProperty("variant", "ghost")
    kill_button = QPushButton("キルスイッチで停止")
    kill_button.setProperty("variant", "kill")
    close_selected_button = QPushButton("選択ポジションを手動決済")
    close_selected_button.setProperty("variant", "success")
    close_all_button = QPushButton("全ポジションを決済")
    close_all_button.setProperty("variant", "success")
    action_row.addWidget(start_button)
    action_row.addWidget(stop_button)
    action_row.addWidget(kill_button)
    action_row.addStretch(1)
    action_row.addWidget(close_selected_button)
    action_row.addWidget(close_all_button)
    layout.addLayout(action_row)

    # ---- KPI 4x2 ----
    kpi_grid = QGridLayout()
    kpi_grid.setHorizontalSpacing(12)
    kpi_grid.setVerticalSpacing(12)
    for column in range(4):
        kpi_grid.setColumnStretch(column, 1)
    kpi_mode = KpiTile(label="運用モード", value="-", value_variant="plain")
    kpi_conn = KpiTile(label="接続状態", value="-", value_variant="plain")
    kpi_data = KpiTile(label="市場データ", value="-", value_variant="plain")
    kpi_size = KpiTile(label="注文サイズ", value="-", value_variant="plain")
    kpi_equity = KpiTile(label="評価資産", value="-", value_variant="mono")
    kpi_daily = KpiTile(label="日次損益", value="-", value_variant="mono")
    kpi_positions = KpiTile(label="保有ポジション", value="-", value_variant="mono")
    kpi_heartbeat = KpiTile(label="更新状況", value="-", value_variant="mono-md")
    tiles = [kpi_mode, kpi_conn, kpi_data, kpi_size, kpi_equity, kpi_daily, kpi_positions, kpi_heartbeat]
    for index, tile in enumerate(tiles):
        kpi_grid.addWidget(tile, index // 4, index % 4)
    layout.addLayout(kpi_grid)
    page.kpi_tiles = {
        "mode": kpi_mode,
        "connection": kpi_conn,
        "market": kpi_data,
        "order_size": kpi_size,
        "equity": kpi_equity,
        "daily_pl": kpi_daily,
        "positions": kpi_positions,
        "heartbeat": kpi_heartbeat,
    }

    # ---- split-2: exit management + runtime summary ----
    exit_hint = QLabel("-")
    exit_hint.setProperty("role", "muted2")
    exit_card = Card(title="選択ポジションの出口管理", header_right=exit_hint)
    exit_grid = DetailGrid(columns=2)
    exit_fields = [
        ("symbol", "選択通貨ペア"),
        ("qty", "保有数量"),
        ("avg_entry_price", "平均取得価格"),
        ("current_price", "現在値"),
        ("market_value", "時価評価額"),
        ("unrealized_pl", "含み損益"),
        ("managed_initial_stop_price", "現在の初期ストップ"),
        ("managed_active_stop_price", "現在の有効ストップ"),
        ("managed_partial_target_price", "一部利確目標"),
        ("managed_partial_reference_price", "比較対象高値"),
        ("managed_next_trailing_price", "次のトレーリング価格"),
        ("managed_reference_bar_at", "比較バー時刻"),
        ("managed_break_even_armed", "建値防衛"),
        ("managed_partial_taken", "一部利確済み"),
        ("managed_bars_held", "保有バー数"),
    ]
    for key, caption in exit_fields:
        exit_grid.add(key, caption)
    exit_card.addBodyWidget(exit_grid)

    run_card = Card(title="実行サマリー")
    run_grid = DetailGrid(columns=1)
    run_fields = [
        ("run_id", "実行ID"),
        ("account_msg", "口座メッセージ"),
        ("last_bars", "最新市場バー"),
        ("stream_last", "ストリーム最終受信"),
        ("stream_err", "ストリーム最終エラー"),
        ("kill", "キルスイッチ"),
        ("last_action", "直近アクション"),
    ]
    for key, caption in run_fields:
        run_grid.add(key, caption)
    run_card.addBodyWidget(run_grid)

    split = QHBoxLayout()
    split.setSpacing(16)
    split_left = QWidget()
    split_left_lay = QVBoxLayout(split_left)
    split_left_lay.setContentsMargins(0, 0, 0, 0)
    split_left_lay.addWidget(exit_card)
    split_right = QWidget()
    split_right_lay = QVBoxLayout(split_right)
    split_right_lay.setContentsMargins(0, 0, 0, 0)
    split_right_lay.addWidget(run_card)
    split.addWidget(split_left, 3)
    split.addWidget(split_right, 2)
    layout.addLayout(split)

    # ---- Tabs card ----
    tabs_card = Card()
    tabs = CountedTabs(
        [
            "現在のポジション",
            "直近シグナル",
            "最近の注文",
            "最近の約定",
            "実行ログ",
        ]
    )
    tabs_card.addBodyWidget(tabs)
    tab_stack = QStackedWidget()

    def _mk_table() -> tuple[QTableView, DataFrameTableModel]:
        view = QTableView()
        view.setAlternatingRowColors(False)
        view.setShowGrid(False)
        view.setSelectionBehavior(QAbstractItemView.SelectRows)
        view.setSelectionMode(QAbstractItemView.SingleSelection)
        view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        view.verticalHeader().setVisible(False)
        view.setWordWrap(False)
        view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        hdr = view.horizontalHeader()
        hdr.setStretchLastSection(True)
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        view.setMinimumHeight(320)
        model = DataFrameTableModel()
        view.setModel(model)
        return view, model

    positions_view, positions_model = _mk_table()
    signals_view, signals_model = _mk_table()
    orders_view, orders_model = _mk_table()
    fills_view, fills_model = _mk_table()
    events_view, events_model = _mk_table()
    tab_stack.addWidget(positions_view)
    tab_stack.addWidget(signals_view)
    tab_stack.addWidget(orders_view)
    tab_stack.addWidget(fills_view)
    tab_stack.addWidget(events_view)
    tabs.currentChanged.connect(tab_stack.setCurrentIndex)
    tabs_card.addBodyWidget(tab_stack)
    layout.addWidget(tabs_card)

    # Column delegates
    # positions: 通貨ペア(0) 売買(1) 数量(2) 平均取得(3) 現在値(4) 時価評価(5) 含み損益(6) 有効ストップ(7) 次トレール(8) 保有バー(9)
    positions_side_delegate = ChipDelegate(positions_view, lambda t: "accent" if t in ("買い", "売り") else None)
    page._delegates.append(positions_side_delegate)
    positions_view.setItemDelegateForColumn(1, positions_side_delegate)

    # signals: 時刻(0) 通貨ペア(1) シグナル(2) スコア(3) 採用(4) 市場セッション(5) 説明(6)
    signals_chip_delegate = ChipDelegate(signals_view, signal_tone)
    page._delegates.append(signals_chip_delegate)
    signals_view.setItemDelegateForColumn(2, signals_chip_delegate)

    # orders: 注文時刻(0) 通貨ペア(1) 売買(2) 数量(3) 約定数量(4) 平均価格(5) 状態(6) 理由(7)
    orders_side_delegate = ChipDelegate(orders_view, lambda t: "accent" if t in ("買い", "売り") else None)
    page._delegates.append(orders_side_delegate)
    orders_view.setItemDelegateForColumn(2, orders_side_delegate)
    orders_status_delegate = ChipDelegate(orders_view, order_status_tone)
    page._delegates.append(orders_status_delegate)
    orders_view.setItemDelegateForColumn(6, orders_status_delegate)

    # fills: 約定時刻 通貨ペア 売買 数量 価格 注文ID
    fills_side_delegate = ChipDelegate(fills_view, lambda t: "accent" if t in ("買い", "売り") else None)
    page._delegates.append(fills_side_delegate)
    fills_view.setItemDelegateForColumn(2, fills_side_delegate)

    # events: 時刻 レベル メッセージ
    events_level_delegate = ChipDelegate(events_view, level_tone)
    page._delegates.append(events_level_delegate)
    events_view.setItemDelegateForColumn(1, events_level_delegate)

    # ---- Log dock (navy) ----
    log_dock = QFrame()
    log_dock.setProperty("role", "logdock-dark")
    dock_lay = QVBoxLayout(log_dock)
    dock_lay.setContentsMargins(14, 10, 14, 10)
    dock_lay.setSpacing(6)
    dock_head = QHBoxLayout()
    dock_title = QLabel("ログ")
    dock_title.setProperty("role", "h2-sm")
    dock_chip = Chip("最新 20 件", "neutral")
    dock_head.addWidget(dock_title)
    dock_head.addSpacing(8)
    dock_head.addWidget(dock_chip)
    dock_head.addStretch(1)
    dock_lay.addLayout(dock_head)
    log_body = QTextBrowser()
    log_body.setProperty("role", "logdock-body")
    log_body.setOpenLinks(False)
    log_body.setFrameShape(QFrame.NoFrame)
    log_body.setFixedHeight(130)
    dock_lay.addWidget(log_body)
    layout.addWidget(log_dock)

    layout.addStretch(1)

    # ---- State ----
    page._raw_positions_frame = pd.DataFrame()
    page._manual_supported = False
    page._latest_status = "stopped"
    page._busy = False
    page._refresh_timer = QTimer(page)
    page._refresh_timer.setInterval(
        min(5000, max(2000, app_state.config.automation.poll_interval_seconds * 1000))
    )

    # ---- Log rendering ----

    _LOG_CSS = (
        f".time{{color:{Tokens.MUTED_2};margin-right:6px;"
        f"font-family:{Tokens.FONT_MONO}}}"
        f".lv-ok{{color:{Tokens.POS};margin-right:6px;font-weight:600}}"
        f".lv-info{{color:{Tokens.INFO};margin-right:6px;font-weight:600}}"
        f".lv-warn{{color:{Tokens.WARN};margin-right:6px;font-weight:600}}"
        f".lv-err{{color:{Tokens.NEG};margin-right:6px;font-weight:600}}"
        f"div{{font-family:{Tokens.FONT_MONO};font-size:12px;"
        f"line-height:1.55;color:{Tokens.INVERSE_2}}}"
    )

    def _log_class(level_text: str) -> str:
        key = EVENT_LEVEL_KEYS.get(level_text, "INFO")
        return {"OK": "lv-ok", "INFO": "lv-info", "WARN": "lv-warn", "ERROR": "lv-err"}[key]

    def _render_log(events: list[dict[str, object]]) -> None:
        if not events:
            log_body.setHtml(
                f"<style>{_LOG_CSS}</style>"
                f'<div><span class="lv-info">[INFO]</span> ログはまだありません。</div>'
            )
            return
        last = events[-20:]
        lines = []
        for event in last:
            timestamp = _format_timestamp(event.get("timestamp"), fmt="%H:%M:%S")
            level_raw = str(event.get("level") or "info")
            level_key = EVENT_LEVEL_KEYS.get(level_raw, "INFO")
            klass = _log_class(level_raw)
            message = html_mod.escape(str(event.get("message_ja") or ""))
            lines.append(
                f'<div><span class="time">{timestamp}</span>'
                f'<span class="{klass}">[{level_key}]</span> {message}</div>'
            )
        log_body.setHtml(f"<style>{_LOG_CSS}</style>" + "".join(lines))

    # ---- Busy / button plumbing ----

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

    def selected_position() -> dict[str, object] | None:
        frame = page._raw_positions_frame
        index = positions_view.currentIndex()
        if frame.empty:
            return None
        if not index.isValid():
            return frame.iloc[0].to_dict()
        return frame.iloc[index.row()].to_dict()

    # ---- Snapshot apply ----

    def _apply_status_chips(status_key: str, mode_key: str, connection_key: str, stream_health: bool) -> None:
        # running chip
        tone_map = {
            "running": "running",
            "starting": "info",
            "stopping": "warn",
            "stopped": "neutral",
            "error": "neg",
        }
        status_running.set_tone(tone_map.get(status_key, "neutral"))
        status_running.set_text(_label(STATUS_LABELS, status_key))
        # GMO chip
        connected = connection_key == "connected"
        if connected and stream_health:
            status_gmo.set_tone("running")
            status_gmo.set_text("GMO 接続済み")
        elif connected:
            status_gmo.set_tone("warn")
            status_gmo.set_text("GMO 接続 (劣化)")
        elif mode_key == "gmo_sim":
            status_gmo.set_tone("warn")
            status_gmo.set_text("GMO 未接続")
        else:
            status_gmo.set_tone("info")
            status_gmo.set_text("ローカルシミュレーション")

    def refresh_position_detail() -> None:
        record = selected_position()
        set_button_enabled(
            close_selected_button,
            record is not None and page._manual_supported and not page._busy,
            busy=page._busy,
        )
        if record is None:
            exit_hint.setText("保有ポジション未選択")
            for key, _ in exit_fields:
                exit_grid.set(key, "-")
            return
        symbol = str(record.get("symbol", "")).upper() or "-"
        exit_hint.setText(f"{symbol} の出口管理を表示中")
        exit_grid.set("symbol", symbol)
        exit_grid.set("qty", _format_count(record.get("qty")))
        exit_grid.set("avg_entry_price", _format_money(record.get("avg_entry_price")))
        exit_grid.set("current_price", _format_money(record.get("current_price")))
        exit_grid.set("market_value", _format_money(record.get("market_value")))
        pnl = _coerce_float(record.get("unrealized_pl"))
        pnl_pct = _coerce_float(record.get("unrealized_plpc"))
        if pnl is None and pnl_pct is None:
            exit_grid.set("unrealized_pl", "-")
        else:
            parts = []
            if pnl is not None:
                parts.append(f"{pnl:+,.0f}")
            if pnl_pct is not None:
                parts.append(f"{pnl_pct:+.2%}")
            tone = "pos" if (pnl or 0) >= 0 else "neg"
            exit_grid.set("unrealized_pl", " / ".join(parts), tone=tone)
        exit_grid.set(
            "managed_initial_stop_price",
            _format_money(record.get("managed_initial_stop_price"), digits=4),
        )
        exit_grid.set(
            "managed_active_stop_price",
            _format_money(record.get("managed_active_stop_price"), digits=4),
        )
        exit_grid.set(
            "managed_partial_target_price",
            _format_money(record.get("managed_partial_target_price"), digits=4),
        )
        exit_grid.set(
            "managed_partial_reference_price",
            _format_money(record.get("managed_partial_reference_price"), digits=4),
        )
        exit_grid.set(
            "managed_next_trailing_price",
            _format_money(record.get("managed_next_trailing_price"), digits=4),
        )
        exit_grid.set(
            "managed_reference_bar_at",
            _format_timestamp(record.get("managed_reference_bar_at")),
        )
        exit_grid.set(
            "managed_break_even_armed",
            _bool_label(record.get("managed_break_even_armed")),
        )
        exit_grid.set(
            "managed_partial_taken",
            _bool_label(record.get("managed_partial_taken")),
        )
        exit_grid.set("managed_bars_held", _format_count(record.get("managed_bars_held")))

    def refresh_snapshot() -> None:
        if not page.isVisible():
            return
        snapshot = None
        snapshot_error = ""
        try:
            snapshot = app_state.runtime_status_snapshot()
        except Exception as exc:  # noqa: BLE001 - UI feedback
            snapshot_error = str(exc)
        current_mode = snapshot["mode"] if snapshot is not None else app_state.config.broker.mode.value
        current_status = snapshot["status"] if snapshot is not None else "stopped"
        page._latest_status = current_status
        connection_state = snapshot.get("connection_state", "idle") if snapshot is not None else "idle"
        stream_state = snapshot.get("stream_state", {}) if snapshot is not None else {}
        stream_healthy = bool(stream_state.get("healthy", False))
        _apply_status_chips(current_status, current_mode, connection_state, stream_healthy)

        # button enablement
        ready_for_start = current_status in {"stopped", "error"}
        if current_mode == "gmo_sim" and app_state.config.data.source != "gmo":
            ready_for_start = False
        is_running = current_status in {"starting", "running", "stopping"}
        start_button.setText(
            "GMO 実時間シミュレーションを開始" if current_mode == "gmo_sim" else "自動売買を開始"
        )
        set_button_enabled(
            start_button,
            ready_for_start and not is_running and not page._busy,
            busy=is_running or page._busy,
        )
        set_button_enabled(stop_button, is_running and not page._busy, busy=page._busy)
        set_button_enabled(kill_button, is_running and not page._busy, busy=page._busy)

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

        if snapshot is None:
            kpi_mode.set_value(_label(MODE_LABELS, current_mode))
            kpi_mode.set_note(snapshot_error or "開始準備待ち")
            kpi_conn.set_value("待機中")
            kpi_conn.set_note(snapshot_error or "まだ接続確認を行っていません。")
            kpi_data.set_value(
                {
                    "gmo": "GMO 実時間データ",
                    "csv": "JForex CSV キャッシュ",
                    "fixture": "fixture 検証データ",
                }.get(app_state.config.data.source, app_state.config.data.source)
            )
            kpi_data.set_note(f"エントリー足: {app_state.config.strategy.entry_timeframe.value}")
            kpi_size.set_value(_label(ORDER_SIZE_LABELS, app_state.config.risk.order_size_mode.value))
            kpi_size.set_note("開始後に数量計算へ反映")
            kpi_equity.set_value("-")
            kpi_equity.set_note("口座情報は停止中")
            kpi_daily.set_value("-")
            kpi_daily.set_trend(None)
            kpi_daily.set_note(
                f"上限 {app_state.config.risk.max_daily_loss_amount:,.0f} JPY / "
                f"{app_state.config.risk.max_daily_loss_pct:.1%}"
            )
            kpi_positions.set_value("0 件")
            kpi_positions.set_note("保有なし")
            kpi_heartbeat.set_value("停止中")
            kpi_heartbeat.set_note("自動売買はまだ動いていません")

            run_grid.set("run_id", "-")
            run_grid.set("account_msg", snapshot_error or "-")
            run_grid.set("last_bars", "-")
            run_grid.set("stream_last", "-")
            run_grid.set("stream_err", "-")
            run_grid.set("kill", "未発動")
            run_grid.set("last_action", "なし")

            page._raw_positions_frame = pd.DataFrame()
            positions_model.set_frame(None)
            signals_model.set_frame(None)
            orders_model.set_frame(None)
            fills_model.set_frame(None)
            events_model.set_frame(None)
            for index in range(5):
                tabs.set_count(index, 0)
            _render_log([])
            refresh_position_detail()
            return

        # KPI bind
        kpi_mode.set_value(_label(MODE_LABELS, snapshot["mode"]))
        kpi_mode.set_note(f"状態: {_label(STATUS_LABELS, snapshot['status'])}")
        kpi_conn.set_value(_label(CONNECTION_LABELS, connection_state))
        kpi_conn.set_note(
            f"ストリーム: {_bool_label(stream_state.get('connected', False))} / "
            f"健全性 {_bool_label(stream_healthy)}"
        )
        kpi_data.set_value(
            {
                "gmo": "GMO 実時間データ",
                "csv": "JForex CSV キャッシュ",
                "fixture": "fixture 検証データ",
            }.get(snapshot.get("data_source"), str(snapshot.get("data_source", "-")))
        )
        kpi_data.set_note(f"エントリー足: {snapshot.get('entry_timeframe', '-')}")

        order_size_mode = snapshot.get("order_size_mode", app_state.config.risk.order_size_mode.value)
        kpi_size.set_value(_label(ORDER_SIZE_LABELS, order_size_mode))
        if order_size_mode == "fixed_amount":
            kpi_size.set_note(f"{app_state.config.risk.fixed_order_amount:,.0f} JPY 相当")
        elif order_size_mode == "equity_fraction":
            kpi_size.set_note(f"総資産の {app_state.config.risk.equity_fraction_per_trade:.1%}")
        else:
            kpi_size.set_note(f"想定損失 {app_state.config.risk.risk_per_trade:.1%}")

        account_summary = snapshot.get("account_summary", {})
        equity_value = account_summary.get("equity") or account_summary.get("portfolio_value")
        kpi_equity.set_value(_format_money(equity_value))
        kpi_equity.set_note(
            f"口座状態: {_label(ACCOUNT_STATUS_LABELS, account_summary.get('status', 'unknown'))}"
        )

        daily_pl = _coerce_float(account_summary.get("daily_pl"))
        starting_cash = _coerce_float(app_state.config.risk.starting_cash) or 0.0
        if daily_pl is None:
            kpi_daily.set_value("-")
            kpi_daily.set_trend(None)
        else:
            tone = "pos" if daily_pl >= 0 else "neg"
            kpi_daily.set_value(f"{daily_pl:+,.0f}", tone=tone)
            if starting_cash:
                pct = daily_pl / starting_cash
                kpi_daily.set_trend(
                    "up" if pct >= 0 else "down",
                    f"{abs(pct):.2%}",
                )
            else:
                kpi_daily.set_trend("flat", "-")
        kpi_daily.set_note(
            f"上限 {app_state.config.risk.max_daily_loss_amount:,.0f} JPY / "
            f"{app_state.config.risk.max_daily_loss_pct:.1%}"
        )

        open_symbols = snapshot.get("open_symbols", [])
        kpi_positions.set_value(f"{len(positions)} 件")
        kpi_positions.set_note(", ".join(open_symbols) if open_symbols else "保有なし")

        kpi_heartbeat.set_value(_format_timestamp(snapshot.get("heartbeat"), fmt="%H:%M:%S"))
        kpi_heartbeat.set_note(
            f"サイクル {snapshot.get('cycle_count', 0):,} / 再接続 {snapshot.get('reconnect_attempts', 0)} 回"
        )

        # Runtime summary detail-grid
        run_grid.set("run_id", str(snapshot.get("run_id") or "-"))
        run_grid.set("account_msg", str(account_summary.get("message") or "-"))
        run_grid.set("last_bars", _format_latest_bars(snapshot.get("latest_market_bar_at", {})))
        run_grid.set(
            "stream_last",
            _format_timestamp(stream_state.get("last_message_at")),
        )
        run_grid.set("stream_err", str(stream_state.get("last_error") or "-") or "-")
        run_grid.set("kill", str(snapshot.get("kill_switch_reason") or "未発動"))
        last_actions = snapshot.get("last_actions")
        run_grid.set("last_action", str(last_actions) if last_actions else "なし")

        # Tables
        page._raw_positions_frame = pd.DataFrame(positions)
        positions_model.set_frame(_positions_frame(positions))
        signals_model.set_frame(_signals_frame(snapshot.get("recent_signals", [])))
        orders_model.set_frame(_orders_frame(snapshot.get("recent_orders", [])))
        fills_model.set_frame(_fills_frame(snapshot.get("recent_fills", [])))
        events_model.set_frame(_events_frame(snapshot.get("recent_events", [])))
        _apply_table_widths()

        tabs.set_count(0, len(positions))
        tabs.set_count(1, len(snapshot.get("recent_signals", [])))
        tabs.set_count(2, len(snapshot.get("recent_orders", [])))
        tabs.set_count(3, len(snapshot.get("recent_fills", [])))
        tabs.set_count(4, len(snapshot.get("recent_events", [])))

        _render_log(snapshot.get("recent_events", []))

        if not page._raw_positions_frame.empty and not positions_view.currentIndex().isValid():
            positions_view.selectRow(0)
        refresh_position_detail()

    # ---- Callbacks ----

    def on_finished(events) -> None:
        app_state.persist_automation_events(events)
        set_automation_busy(False)
        refresh_snapshot()
        log_message("自動売買ループが終了しました。")

    def on_error(message: str) -> None:
        set_automation_busy(False)
        run_grid.set("account_msg", f"エラー: {message}", tone="neg")
        log_message(f"自動売買エラー: {message}")

    def start_loop() -> None:
        controller = app_state.start_automation()
        page._latest_status = "starting"
        run_grid.set("account_msg", "開始要求を送信しました。")
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
        except Exception as exc:  # noqa: BLE001 - runtime/broker path
            set_automation_busy(False)
            QMessageBox.critical(page, "エラー", f"{symbol} の手動決済に失敗しました。\n{exc}")
            return
        set_automation_busy(False)
        log_message(f"{symbol} を手動決済しました。")
        QMessageBox.information(
            page,
            "完了",
            f"{symbol} の手動決済を送信しました。\n注文ID: {result.get('order_id', '-')}",
        )
        refresh_snapshot()

    def close_all_positions() -> None:
        set_automation_busy(True)
        try:
            result = app_state.manual_close_all_positions()
        except Exception as exc:  # noqa: BLE001 - runtime/broker path
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

    def _apply_width(view: QTableView, model: DataFrameTableModel, width_map: dict[int, int]) -> None:
        header = view.horizontalHeader()
        for column, width in width_map.items():
            if column < model.columnCount():
                header.resizeSection(column, width)

    def _apply_table_widths() -> None:
        _apply_width(positions_view, positions_model, {0: 90, 1: 82, 2: 70, 3: 92, 4: 92, 5: 100, 6: 110, 7: 100, 8: 100, 9: 72})
        _apply_width(signals_view, signals_model, {0: 115, 1: 90, 2: 82, 3: 70, 4: 64, 5: 110, 6: 300})
        _apply_width(orders_view, orders_model, {0: 115, 1: 90, 2: 82, 3: 70, 4: 78, 5: 92, 6: 100, 7: 220})
        _apply_width(fills_view, fills_model, {0: 115, 1: 90, 2: 82, 3: 70, 4: 92, 5: 180})
        _apply_width(events_view, events_model, {0: 115, 1: 82, 2: 520})

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
