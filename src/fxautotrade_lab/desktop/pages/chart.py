"""Chart page — QPainter-based candlestick / volume / RSI canvases."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Candle:
    ts: str
    o: float
    h: float
    l: float
    c: float
    volume: float = 0.0


TF_LABELS = ["1m", "5m", "15m", "1h", "4h", "1D"]
TF_VALUE_ALIASES = {
    "1m": {"1Min", "1m"},
    "5m": {"5Min", "5m"},
    "15m": {"15Min", "15m"},
    "1h": {"1Hour", "1h"},
    "4h": {"4Hour", "4h"},
    "1D": {"1Day", "1D"},
}
RANGE_LABELS = ["1W", "1M", "3M", "YTD"]
RANGE_TAILS = {0: 60, 1: 80, 2: 120, 3: 240}


def _ema(values: list[float], period: int) -> list[float]:
    out: list[float] = []
    if not values:
        return out
    k = 2.0 / (period + 1)
    prev: float | None = None
    for value in values:
        prev = value if prev is None else value * k + prev * (1 - k)
        out.append(prev)
    return out


def _rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    out = [50.0] * period
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(closes) - 1):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l else float("inf")
        out.append(100 - 100 / (1 + rs)) if avg_l else out.append(100.0)
    if out:
        out.append(out[-1])
    while len(out) < len(closes):
        out.append(50.0)
    return out[: len(closes)]


def _latest_entry_stop(app_state, symbol: str) -> tuple[float | None, float | None]:
    result = getattr(app_state, "last_result", None)
    if result is None:
        return None, None
    trades = getattr(result, "trades", None)
    if trades is None or trades.empty or "symbol" not in trades.columns:
        return None, None
    sub = trades[trades["symbol"].astype(str) == symbol]
    if sub.empty:
        return None, None
    row = sub.iloc[-1]

    def _pick(*names):
        for name in names:
            if name in row and row[name] is not None:
                try:
                    return float(row[name])
                except (TypeError, ValueError):
                    continue
        return None

    entry = _pick("entry_price", "avg_entry_price", "price")
    stop = _pick("stop_price", "managed_active_stop_price", "managed_initial_stop_price")
    return entry, stop


def build_chart_page(app_state, submit_task, log_message, on_add_pair=None):  # pragma: no cover - UI helper
    import pandas as pd

    from PySide6.QtCore import Qt, QRectF
    from PySide6.QtGui import QBrush, QColor, QFont, QPainter, QPen
    from PySide6.QtWidgets import (
        QCheckBox,
        QComboBox,
        QFrame,
        QHBoxLayout,
        QLabel,
        QMessageBox,
        QPushButton,
        QScrollArea,
        QSizePolicy,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl

    # ---- Canvases ---------------------------------------------------------
    class PriceCanvas(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setMinimumHeight(300)
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.candles: list[Candle] = []
            self.ema: list[float] = []
            self.entry_price: float | None = None
            self.stop_price: float | None = None

        def set_data(self, candles, ema, entry=None, stop=None) -> None:
            self.candles = list(candles)
            self.ema = list(ema)
            self.entry_price = entry
            self.stop_price = stop
            self.update()

        def paintEvent(self, _event) -> None:  # noqa: N802
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.fillRect(self.rect(), QColor(Tokens.BG_CARD))
            rect = self.rect()
            if not self.candles:
                painter.setPen(QColor(Tokens.MUTED_2))
                painter.drawText(rect, Qt.AlignCenter, "データがありません")
                painter.end()
                return
            pad_l, pad_r, pad_t, pad_b = 52, 12, 12, 20
            plot = QRectF(
                rect.left() + pad_l,
                rect.top() + pad_t,
                rect.width() - pad_l - pad_r,
                rect.height() - pad_t - pad_b,
            )
            prices = [value for candle in self.candles for value in (candle.h, candle.l)]
            if self.ema:
                prices.extend(self.ema)
            for extra in (self.entry_price, self.stop_price):
                if extra is not None:
                    prices.append(extra)
            mn = min(prices)
            mx = max(prices)
            if mn == mx:
                mx = mn + 1.0

            def y(value: float) -> float:
                return plot.bottom() - (value - mn) / (mx - mn) * plot.height()

            n = len(self.candles)
            slot = plot.width() / max(n, 1)

            grid_pen = QPen(QColor(Tokens.HAIRLINE), 1)
            mono_font = QFont("JetBrains Mono")
            mono_font.setPointSizeF(8.5)
            painter.setFont(mono_font)
            for i in range(5):
                gy = plot.top() + plot.height() * i / 4
                painter.setPen(grid_pen)
                painter.drawLine(int(plot.left()), int(gy), int(plot.right()), int(gy))
                value = mx - (mx - mn) * i / 4
                painter.setPen(QColor(Tokens.MUTED_2))
                painter.drawText(int(rect.left()) + 6, int(gy) + 4, f"{value:.3f}")

            pos_color = QColor(Tokens.POS)
            neg_color = QColor(Tokens.NEG)
            body_w = max(4.0, slot * 0.55)
            for i, candle in enumerate(self.candles):
                cx = plot.left() + slot * i + slot / 2
                up = candle.c >= candle.o
                color = pos_color if up else neg_color
                painter.setPen(QPen(color, 1.5))
                painter.drawLine(int(cx), int(y(candle.h)), int(cx), int(y(candle.l)))
                top_y = y(max(candle.o, candle.c))
                bot_y = y(min(candle.o, candle.c))
                painter.setBrush(QBrush(color))
                painter.setPen(Qt.NoPen)
                painter.drawRect(
                    QRectF(cx - body_w / 2, top_y, body_w, max(1.0, bot_y - top_y))
                )

            if self.ema:
                painter.setPen(QPen(QColor(Tokens.ACCENT), 1.5))
                previous: tuple[float, float] | None = None
                for i, value in enumerate(self.ema):
                    cx = plot.left() + slot * i + slot / 2
                    if previous is not None:
                        painter.drawLine(
                            int(previous[0]),
                            int(previous[1]),
                            int(cx),
                            int(y(value)),
                        )
                    previous = (cx, y(value))

            marker_x = plot.left() + slot * int(max(0, n * 0.55))
            if self.entry_price is not None:
                ey = y(self.entry_price)
                painter.setBrush(pos_color)
                painter.setPen(Qt.NoPen)
                painter.drawEllipse(int(marker_x) - 5, int(ey) - 5, 10, 10)
                painter.setPen(QColor(Tokens.POS))
                painter.setFont(mono_font)
                painter.drawText(
                    int(marker_x) + 10, int(ey) + 4, f"ENTRY {self.entry_price:.3f}"
                )
            if self.stop_price is not None:
                sy = y(self.stop_price)
                painter.setPen(QPen(QColor(Tokens.NEG), 1, Qt.DashLine))
                painter.drawLine(int(marker_x), int(sy), int(plot.right()), int(sy))
                painter.setFont(mono_font)
                painter.setPen(QColor(Tokens.NEG))
                painter.drawText(
                    int(plot.right()) - 92, int(sy) - 4, f"STOP {self.stop_price:.3f}"
                )
            painter.end()

    class VolumeCanvas(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setFixedHeight(120)
            self.volumes: list[float] = []

        def set_data(self, volumes) -> None:
            self.volumes = list(volumes)
            self.update()

        def paintEvent(self, _event) -> None:  # noqa: N802
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.fillRect(self.rect(), QColor(Tokens.BG_CARD))
            if not self.volumes:
                painter.setPen(QColor(Tokens.MUTED_2))
                painter.drawText(self.rect(), Qt.AlignCenter, "データがありません")
                painter.end()
                return
            pad = 8
            area = self.rect().adjusted(pad, pad, -pad, -pad)
            mx = max(self.volumes) or 1.0
            n = len(self.volumes)
            slot = area.width() / n
            body_w = max(3.0, slot * 0.55)
            accent = QColor(Tokens.ACCENT)
            accent.setAlphaF(0.60)
            for i, value in enumerate(self.volumes):
                height = area.height() * (value / mx)
                x = area.left() + slot * i + (slot - body_w) / 2
                painter.fillRect(
                    QRectF(x, area.bottom() - height, body_w, height),
                    accent,
                )
            painter.end()

    class RsiCanvas(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setFixedHeight(120)
            self.values: list[float] = []

        def set_values(self, values) -> None:
            self.values = list(values)
            self.update()

        def paintEvent(self, _event) -> None:  # noqa: N802
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.fillRect(self.rect(), QColor(Tokens.BG_CARD))
            pad_l, pad_r, pad_t, pad_b = 26, 8, 8, 8
            plot = self.rect().adjusted(pad_l, pad_t, -pad_r, -pad_b)

            def y(value: float) -> float:
                return plot.bottom() - (value / 100.0) * plot.height()

            mono_font = QFont("JetBrains Mono")
            mono_font.setPointSizeF(8.5)
            painter.setFont(mono_font)
            painter.setPen(QPen(QColor(Tokens.NEG), 1, Qt.DashLine))
            painter.drawLine(int(plot.left()), int(y(70)), int(plot.right()), int(y(70)))
            painter.setPen(QColor(Tokens.NEG))
            painter.drawText(4, int(y(70)) - 2, "70")
            painter.setPen(QPen(QColor(Tokens.POS), 1, Qt.DashLine))
            painter.drawLine(int(plot.left()), int(y(30)), int(plot.right()), int(y(30)))
            painter.setPen(QColor(Tokens.POS))
            painter.drawText(4, int(y(30)) + 10, "30")
            if not self.values:
                painter.end()
                return
            painter.setPen(QPen(QColor(Tokens.ACCENT), 1.5))
            n = len(self.values)
            slot = plot.width() / max(n - 1, 1)
            previous: tuple[float, float] | None = None
            for i, value in enumerate(self.values):
                x = plot.left() + slot * i
                if previous is not None:
                    painter.drawLine(
                        int(previous[0]),
                        int(previous[1]),
                        int(x),
                        int(y(value)),
                    )
                previous = (x, y(value))
            painter.end()

    # ---- Scaffolding ------------------------------------------------------
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
    title = QLabel("チャート")
    title.setProperty("role", "h1")
    subtitle = QLabel("価格チャート、出来高、RSI を縦に並べて表示します。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    reload_btn = QPushButton("再読込")
    reload_btn.setProperty("variant", "ghost")
    add_pair_btn = QPushButton("ペアを追加")
    add_pair_btn.setProperty("variant", "primary")
    header.addWidget(reload_btn)
    header.addWidget(add_pair_btn)
    layout.addLayout(header)

    # ---- Settings card (head only) ----
    symbol_combo = QComboBox()
    symbol_combo.setFixedWidth(160)
    tf_seg = SegmentedControl(TF_LABELS, current=2, data=TF_LABELS)
    range_seg = SegmentedControl(RANGE_LABELS, current=1, data=RANGE_LABELS)
    signals_cb = QCheckBox("シグナル表示")
    signals_cb.setChecked(True)
    exit_cb = QCheckBox("出口管理")
    exit_cb.setChecked(True)

    settings_tools = QWidget()
    st = QHBoxLayout(settings_tools)
    st.setContentsMargins(0, 0, 0, 0)
    st.setSpacing(8)
    for widget in (symbol_combo, tf_seg, range_seg, signals_cb, exit_cb):
        st.addWidget(widget)
    settings_card = Card(title="表示設定", header_right=settings_tools)
    settings_card.setBodyVisible(False)
    layout.addWidget(settings_card)

    # ---- Main chart card ----
    live_chip = Chip("未実行", "neutral")
    ohlc_hint = QLabel("O - · H - · L - · C - · Δ -")
    ohlc_hint.setProperty("role", "mono-hint")
    head_right = QWidget()
    hr = QHBoxLayout(head_right)
    hr.setContentsMargins(0, 0, 0, 0)
    hr.setSpacing(10)
    hr.addWidget(live_chip)
    hr.addWidget(ohlc_hint)
    main_card = Card(title="-", header_right=head_right)
    price_canvas = PriceCanvas()
    main_card.addBodyWidget(price_canvas)
    layout.addWidget(main_card)

    # ---- grid-2: volume + RSI ----
    volume_card = Card(title="出来高")
    volume_canvas = VolumeCanvas()
    volume_card.addBodyWidget(volume_canvas)

    rsi_chip = Chip("-", "info")
    rsi_card = Card(title="RSI (14)", header_right=rsi_chip)
    rsi_canvas = RsiCanvas()
    rsi_card.addBodyWidget(rsi_canvas)

    grid2 = QHBoxLayout()
    grid2.setSpacing(12)
    grid2.addWidget(volume_card, 1)
    grid2.addWidget(rsi_card, 1)
    layout.addLayout(grid2)

    layout.addStretch(1)
    page._chart_request_id = 0
    page._chart_loading = False
    page._chart_loaded_once = False

    # ---- Data helpers -----------------------------------------------------
    def _is_runtime_chart() -> bool:
        return (
            app_state.automation_controller is not None
            or app_state.config.data.source == "gmo"
            or app_state.config.broker.mode.value == "gmo_sim"
        )

    def _available_symbols() -> list[str]:
        result = getattr(app_state, "last_result", None)
        if _is_runtime_chart():
            return list(dict.fromkeys(app_state.config.watchlist.symbols))
        if result is not None and getattr(result, "chart_frames", None):
            return list(result.chart_frames.keys())
        return list(dict.fromkeys(app_state.config.watchlist.symbols))

    def _populate_symbol_combo() -> None:
        symbols = _available_symbols()
        current = symbol_combo.currentText()
        symbol_combo.blockSignals(True)
        symbol_combo.clear()
        if symbols:
            symbol_combo.addItems(symbols)
            if current in symbols:
                symbol_combo.setCurrentText(current)
        else:
            symbol_combo.addItem("データなし")
        symbol_combo.blockSignals(False)

    def _display_symbol() -> str:
        raw = symbol_combo.currentText()
        if raw == "データなし":
            return ""
        return raw.replace("_", "/")

    def _current_tf_label() -> str:
        index = tf_seg.current()
        return TF_LABELS[index] if 0 <= index < len(TF_LABELS) else "15m"

    def _frame_to_candles(frame, tail: int) -> list[Candle]:
        if frame is None or frame.empty:
            return []
        rows = frame.tail(tail)
        candles: list[Candle] = []
        for ts, row in rows.iterrows():
            candles.append(
                Candle(
                    ts=pd.to_datetime(ts).strftime("%m/%d %H:%M"),
                    o=float(row.get("open", 0.0)),
                    h=float(row.get("high", 0.0)),
                    l=float(row.get("low", 0.0)),
                    c=float(row.get("close", 0.0)),
                    volume=float(row.get("volume", 0.0) or 0.0),
                )
            )
        return candles

    def _ohlc_summary(candles: list[Candle]) -> str:
        if not candles:
            return "O - · H - · L - · C - · Δ -"
        last = candles[-1]
        delta = last.c - last.o
        pct = (delta / last.o * 100.0) if last.o else 0.0
        sign = "+" if delta >= 0 else ""
        return (
            f"O {last.o:.3f} · H {last.h:.3f} · L {last.l:.3f} · C {last.c:.3f} · "
            f"Δ {sign}{delta:.3f} ({sign}{pct:.2f}%)"
        )

    def _rsi_tone(value: float) -> str:
        if value >= 70.0:
            return "warn"
        if value <= 30.0:
            return "running"
        return "info"

    def _clear_canvases(message: str, chip_tone: str = "neutral", chip_text: str = "no data") -> None:
        price_canvas.set_data([], [])
        volume_canvas.set_data([])
        rsi_canvas.set_values([])
        rsi_chip.set_text("-")
        rsi_chip.set_tone("neutral")
        live_chip.set_text(chip_text)
        live_chip.set_tone(chip_tone)
        ohlc_hint.setText(message)

    def _update_title(symbol_text: str, tf_label: str) -> None:
        symbol_display = symbol_text or "-"
        main_card.set_title(f"{symbol_display} · {tf_label}")

    def _apply_candles(frame, *, live: bool, title_suffix: str = "") -> None:
        tf_label = _current_tf_label()
        tail = RANGE_TAILS.get(range_seg.current(), 80)
        candles = _frame_to_candles(frame, tail=tail)
        if not candles:
            _clear_canvases(f"O - · H - · L - · C - · Δ - {title_suffix}".strip())
            _update_title(_display_symbol(), tf_label)
            return
        closes = [candle.c for candle in candles]
        ema = _ema(closes, 21)
        entry = stop = None
        symbol_value = symbol_combo.currentText()
        if signals_cb.isChecked() or exit_cb.isChecked():
            entry_raw, stop_raw = _latest_entry_stop(app_state, symbol_value)
            if signals_cb.isChecked():
                entry = entry_raw
            if exit_cb.isChecked():
                stop = stop_raw
        price_canvas.set_data(candles, ema, entry=entry, stop=stop)
        volume_canvas.set_data([candle.volume for candle in candles])
        rsi_values = _rsi(closes, 14)
        rsi_canvas.set_values(rsi_values)
        latest_rsi = rsi_values[-1] if rsi_values else 50.0
        rsi_chip.set_text(f"{latest_rsi:.1f}")
        rsi_chip.set_tone(_rsi_tone(latest_rsi))
        ohlc_hint.setText(_ohlc_summary(candles))
        live_chip.set_text("live" if live else "backtest")
        live_chip.set_tone("running" if live else "info")
        _update_title(_display_symbol(), tf_label)

    # ---- Runtime async path ----------------------------------------------
    def _on_runtime_loaded(request_id: int, frame) -> None:
        if request_id != page._chart_request_id:
            return
        page._chart_loading = False
        page._chart_loaded_once = True
        set_button_enabled(reload_btn, True)
        _apply_candles(frame, live=True)

    def _on_runtime_error(request_id: int, message: str) -> None:
        if request_id != page._chart_request_id:
            return
        page._chart_loading = False
        set_button_enabled(reload_btn, True)
        _clear_canvases(f"取得失敗: {message}", chip_tone="neg", chip_text="error")
        log_message(f"チャート取得エラー: {message}")

    def _request_runtime_render(*, force_refresh: bool = False) -> None:
        symbol = symbol_combo.currentText()
        timeframe = _current_tf_label()
        if not symbol or symbol == "データなし":
            _clear_canvases("O - · H - · L - · C - · Δ -")
            return
        if page._chart_loading:
            return
        page._chart_request_id += 1
        request_id = page._chart_request_id
        page._chart_loading = True
        set_button_enabled(reload_btn, False, busy=True)
        live_chip.set_text("loading")
        live_chip.set_tone("info")

        def _load():
            return app_state.load_chart_dataset(symbol, timeframe, force_refresh=force_refresh)

        def _handle(dataset, rid=request_id):
            frame = None
            if isinstance(dataset, dict):
                frame = dataset.get("frame")
            _on_runtime_loaded(rid, frame)

        def _handle_err(message, rid=request_id):
            _on_runtime_error(rid, message)

        submit_task(_load, _handle, _handle_err)

    # ---- Backtest sync path ---------------------------------------------
    def _apply_backtest() -> None:
        symbol = symbol_combo.currentText()
        timeframe = _current_tf_label()
        result = getattr(app_state, "last_result", None)
        frames = getattr(result, "chart_frames", None) if result is not None else None
        frame = None
        if frames:
            candidates = frames.get(symbol, {})
            frame = candidates.get(timeframe)
            if frame is None:
                for alias in TF_VALUE_ALIASES.get(timeframe, set()):
                    frame = candidates.get(alias)
                    if frame is not None:
                        break
        if frame is None:
            _clear_canvases("バックテスト後にチャートを表示できます。")
            live_chip.set_text("no data")
            live_chip.set_tone("neutral")
            _update_title(_display_symbol(), timeframe)
            return
        page._chart_loaded_once = True
        _apply_candles(frame, live=False)

    # ---- Main refresh ----------------------------------------------------
    def refresh_chart(force_refresh: bool = False) -> None:
        _populate_symbol_combo()
        if symbol_combo.currentText() in {"", "データなし"}:
            _clear_canvases("O - · H - · L - · C - · Δ -")
            _update_title("", _current_tf_label())
            return
        if _is_runtime_chart() and not force_refresh and not page.isVisible():
            if not page._chart_loaded_once:
                _clear_canvases(
                    "チャートページを開くと最新データを読み込みます。",
                    chip_tone="neutral",
                    chip_text="standby",
                )
                _update_title(_display_symbol(), _current_tf_label())
            return
        if _is_runtime_chart():
            _request_runtime_render(force_refresh=force_refresh)
        else:
            _apply_backtest()

    # ---- Wiring ----------------------------------------------------------
    def _reload():
        refresh_chart(force_refresh=True)

    def _on_add_pair():
        if on_add_pair is not None:
            try:
                on_add_pair()
                return
            except Exception:  # noqa: BLE001
                pass
        QMessageBox.information(
            page,
            "ペアを追加",
            "監視通貨ペアページでペアを追加してください。",
        )

    symbol_combo.currentTextChanged.connect(lambda _=None: refresh_chart())
    tf_seg.currentChanged.connect(lambda _=None: refresh_chart())
    range_seg.currentChanged.connect(lambda _=None: refresh_chart())
    signals_cb.toggled.connect(lambda _=None: refresh_chart())
    exit_cb.toggled.connect(lambda _=None: refresh_chart())
    reload_btn.clicked.connect(_reload)
    add_pair_btn.clicked.connect(_on_add_pair)

    page.refresh = refresh_chart
    _populate_symbol_combo()
    _clear_canvases(
        "チャートページを開くと最新データを読み込みます。",
        chip_tone="neutral",
        chip_text="standby",
    )
    _update_title(_display_symbol(), _current_tf_label())
    return page
