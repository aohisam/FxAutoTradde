"""Signals page."""

from __future__ import annotations

_ACTION_LABELS = {
    "buy": "買い",
    "sell": "売り",
    "hold": "様子見",
    "flat": "様子見",
    "watch": "様子見",
}
_SCORE_THRESHOLD = 0.55
_SIDE_SEG_LABELS = ["全て", "買い", "売り", "様子見"]
_HIST_SEG_LABELS = ["全体", "採用", "非採用"]


def build_signals_page(
    app_state, submit_task=None, log_message=None
):  # pragma: no cover - UI helper
    import pandas as pd
    from PySide6.QtCore import QSignalBlocker, QSortFilterProxyModel, Qt
    from PySide6.QtGui import QColor, QPainter, QPen
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QComboBox,
        QFileDialog,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QPushButton,
        QScrollArea,
        QStyledItemDelegate,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.ui_controls import set_button_enabled
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl

    DataFrameTableModel = load_dataframe_model_class()

    class ScoreHistogram(QWidget):
        def __init__(self, parent=None):
            super().__init__(parent)
            self.setMinimumHeight(220)
            self._scores: list[float] = []
            self._threshold = _SCORE_THRESHOLD
            self._bins = 11
            self._counts: list[int] = [0] * self._bins

        def set_scores(self, scores, threshold: float = _SCORE_THRESHOLD) -> None:
            self._scores = [float(v) for v in scores if v is not None and v == v]
            self._threshold = threshold
            self._counts = [0] * self._bins
            for score in self._scores:
                bounded = max(0.0, min(1.0, score))
                idx = min(self._bins - 1, int(bounded * self._bins))
                self._counts[idx] += 1
            self.update()

        def set_bars(self, counts, threshold: float = _SCORE_THRESHOLD) -> None:
            parsed = []
            for count in list(counts or []):
                try:
                    parsed.append(max(0, int(count)))
                except (TypeError, ValueError):
                    parsed.append(0)
            if len(parsed) < self._bins:
                parsed.extend([0] * (self._bins - len(parsed)))
            self._scores = []
            self._threshold = threshold
            self._counts = parsed[: self._bins]
            self.update()

        def paintEvent(self, _event) -> None:  # noqa: N802
            painter = QPainter(self)
            painter.setRenderHint(QPainter.Antialiasing)
            rect = self.rect().adjusted(12, 12, -12, -12)
            painter.fillRect(self.rect(), QColor(Tokens.BG_CARD))
            if not any(self._counts):
                painter.setPen(QColor(Tokens.MUTED_2))
                painter.drawText(rect, Qt.AlignCenter, "データがありません")
                painter.end()
                return
            counts = self._counts
            max_count = max(counts) or 1
            bar_width = rect.width() / self._bins
            accent = QColor(Tokens.ACCENT)
            for index, count in enumerate(counts):
                height = rect.height() * (count / max_count)
                left = rect.left() + index * bar_width + 4
                top = rect.bottom() - height
                painter.fillRect(
                    int(left),
                    int(top),
                    int(bar_width - 8),
                    int(height),
                    accent,
                )
            threshold_x = rect.left() + rect.width() * self._threshold
            pen = QPen(QColor(Tokens.NEG), 1.5, Qt.DashLine)
            painter.setPen(pen)
            painter.drawLine(int(threshold_x), rect.top(), int(threshold_x), rect.bottom())
            painter.setPen(QColor(Tokens.NEG))
            painter.drawText(int(threshold_x) + 4, rect.top() + 12, f"閾値 {self._threshold:.2f}")
            painter.end()

    class SignalChipDelegate(QStyledItemDelegate):
        def paint(self, painter, option, index):  # noqa: D401
            text = str(index.data() or "")
            if text not in ("買い", "売り"):
                super().paint(painter, option, index)
                return
            painter.save()
            painter.setRenderHint(QPainter.Antialiasing)
            rect = option.rect.adjusted(6, 4, -6, -4)
            bg = QColor(Tokens.ACCENT)
            bg.setAlphaF(0.10)
            border = QColor(Tokens.ACCENT)
            border.setAlphaF(0.35)
            painter.setBrush(bg)
            painter.setPen(QPen(border, 1))
            radius = rect.height() / 2
            painter.drawRoundedRect(rect, radius, radius)
            painter.setBrush(QColor(Tokens.ACCENT))
            painter.setPen(Qt.NoPen)
            dot_y = rect.center().y() - 3
            painter.drawEllipse(rect.left() + 8, dot_y, 6, 6)
            painter.setPen(QColor(Tokens.ACCENT))
            painter.drawText(rect.adjusted(22, 0, -8, 0), Qt.AlignVCenter | Qt.AlignLeft, text)
            painter.restore()

    class _SignalFilterProxy(QSortFilterProxyModel):
        def __init__(self, parent=None):
            super().__init__(parent)
            self._symbol_filter = ""
            self._side_filter: str | None = None

        def set_symbol_filter(self, text: str) -> None:
            self._symbol_filter = text.strip().upper()
            self.invalidateFilter()

        def set_side_filter(self, side: str | None) -> None:
            self._side_filter = side
            self.invalidateFilter()

        def filterAcceptsRow(self, row, parent):  # noqa: N802
            model = self.sourceModel()
            if model is None:
                return True
            sym_idx = model.index(row, 1, parent)
            side_idx = model.index(row, 2, parent)
            symbol = str(model.data(sym_idx) or "").upper()
            side_value = str(model.data(side_idx) or "")
            if self._symbol_filter and self._symbol_filter not in symbol:
                return False
            return not (self._side_filter and self._side_filter != side_value)

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
    title = QLabel("シグナル分析")
    title.setProperty("role", "h1")
    subtitle = QLabel(
        "直近バックテストの生成シグナルを、スコア・通貨ペア・市場セッションで掘り下げます。"
    )
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    export_btn = QPushButton("CSV 書き出し")
    export_btn.setProperty("variant", "ghost")
    recalc_btn = QPushButton("現在の戦略で再計算")
    recalc_btn.setProperty("variant", "primary")
    header.addWidget(export_btn)
    header.addWidget(recalc_btn)
    layout.addLayout(header)

    selection_card = Card(
        title="表示する結果",
        subtitle="起動時は最新の保存済みバックテスト結果を自動で読み込みます。過去の run もここから切り替えられます。",
    )
    selection_grid = QGridLayout()
    selection_grid.setHorizontalSpacing(12)
    selection_grid.setVerticalSpacing(8)
    selection_grid.setColumnStretch(1, 1)

    result_combo = QComboBox()
    result_combo.setMinimumWidth(340)
    result_combo.setMinimumContentsLength(28)
    result_combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
    strategy_value = QLabel("-")
    strategy_value.setProperty("role", "detail-value")
    strategy_value.setWordWrap(True)
    period_value = QLabel("-")
    period_value.setProperty("role", "detail-value")
    run_id_value = QLabel("-")
    run_id_value.setProperty("role", "detail-value")
    run_id_value.setTextInteractionFlags(Qt.TextSelectableByMouse)
    ml_value = QLabel("-")
    ml_value.setProperty("role", "detail-value")
    ml_value.setWordWrap(True)

    def _meta_label(text: str) -> QLabel:
        label = QLabel(text)
        label.setProperty("role", "muted2")
        return label

    selection_grid.addWidget(_meta_label("保存済み結果"), 0, 0)
    selection_grid.addWidget(result_combo, 0, 1)
    selection_grid.addWidget(_meta_label("戦略"), 1, 0)
    selection_grid.addWidget(strategy_value, 1, 1)
    selection_grid.addWidget(_meta_label("検証期間"), 2, 0)
    selection_grid.addWidget(period_value, 2, 1)
    selection_grid.addWidget(_meta_label("使用 ML"), 3, 0)
    selection_grid.addWidget(ml_value, 3, 1)
    selection_grid.addWidget(_meta_label("実行ID"), 4, 0)
    selection_grid.addWidget(run_id_value, 4, 1)
    selection_card.addBodyLayout(selection_grid)
    layout.addWidget(selection_card)

    # ---- KPI grid ----
    kpi_grid = QGridLayout()
    kpi_grid.setHorizontalSpacing(12)
    kpi_grid.setVerticalSpacing(12)
    for column in range(4):
        kpi_grid.setColumnStretch(column, 1)
    kpi_total = KpiTile(
        label="総シグナル数", value="-", value_variant="mono", note="直近バックテスト"
    )
    kpi_accepted = KpiTile(label="採用率", value="-", value_variant="mono")
    kpi_side = KpiTile(label="買い / 売り", value="-", value_variant="mono-md")
    kpi_score = KpiTile(
        label="平均スコア", value="-", value_variant="mono", note=f"閾値 {_SCORE_THRESHOLD:.2f}"
    )
    for index, tile in enumerate([kpi_total, kpi_accepted, kpi_side, kpi_score]):
        kpi_grid.addWidget(tile, 0, index)
    layout.addLayout(kpi_grid)

    # ---- split-2 ----
    split = QHBoxLayout()
    split.setSpacing(16)

    hist_seg = SegmentedControl(_HIST_SEG_LABELS, current=1, data=_HIST_SEG_LABELS)
    hist_card = Card(title="スコア分布", header_right=hist_seg)
    histogram = ScoreHistogram()
    hist_card.addBodyWidget(histogram)
    split.addWidget(hist_card, 1)

    symbol_card = Card(title="通貨ペア別採用", subtitle="採用順の上位 5 ペア")
    symbol_table = QTableView()
    symbol_table.setAlternatingRowColors(False)
    symbol_table.setShowGrid(False)
    symbol_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
    symbol_table.setSelectionMode(QAbstractItemView.NoSelection)
    symbol_table.verticalHeader().setVisible(False)
    symbol_table.verticalHeader().setDefaultSectionSize(28)
    symbol_table.setWordWrap(False)
    symbol_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
    symbol_model = DataFrameTableModel()
    symbol_table.setModel(symbol_model)
    symbol_card.addBodyWidget(symbol_table)
    split.addWidget(symbol_card, 1)
    layout.addLayout(split)

    # ---- Signals list card ----
    list_hint = QLabel("直近 300 件")
    list_hint.setProperty("role", "muted2")
    filter_input = QLineEdit()
    filter_input.setPlaceholderText("通貨ペアで絞込")
    filter_input.setMinimumWidth(160)
    filter_input.setMaximumWidth(200)
    side_seg = SegmentedControl(_SIDE_SEG_LABELS, current=0, data=_SIDE_SEG_LABELS)
    list_tools = QWidget()
    tl = QHBoxLayout(list_tools)
    tl.setContentsMargins(0, 0, 0, 0)
    tl.setSpacing(10)
    tl.addWidget(list_hint)
    tl.addWidget(filter_input)
    tl.addWidget(side_seg)
    list_card = Card(title="シグナル一覧", header_right=list_tools)

    list_model = DataFrameTableModel()
    list_proxy = _SignalFilterProxy()
    list_proxy.setSourceModel(list_model)
    list_view = QTableView()
    list_view.setAlternatingRowColors(False)
    list_view.setShowGrid(False)
    list_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
    list_view.setSelectionBehavior(QAbstractItemView.SelectRows)
    list_view.setSelectionMode(QAbstractItemView.SingleSelection)
    list_view.verticalHeader().setVisible(False)
    list_view.verticalHeader().setDefaultSectionSize(30)
    list_view.setWordWrap(False)
    list_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
    list_view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
    list_view.horizontalHeader().setStretchLastSection(True)
    list_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
    list_view.setMinimumHeight(320)
    list_view.setModel(list_proxy)
    signal_delegate = SignalChipDelegate(list_view)
    page._delegates.append(signal_delegate)
    list_view.setItemDelegateForColumn(2, signal_delegate)
    list_card.addBodyWidget(list_view)
    layout.addWidget(list_card)

    layout.addStretch(1)
    page._full_frame = None
    page._signal_snapshot = None
    page._saved_runs = []  # type: ignore[attr-defined]
    page._selected_run_id = None  # type: ignore[attr-defined]
    page._loading_run_id = None  # type: ignore[attr-defined]

    # ---- Helpers ----
    def _action_series(df):
        if "signal_action" in df.columns:
            raw = df["signal_action"].astype(str).str.lower()
        elif "side" in df.columns:
            raw = df["side"].astype(str).str.lower()
        else:
            return pd.Series([""] * len(df), index=df.index)
        return raw.map(_ACTION_LABELS).fillna("様子見")

    def _score_series(df):
        if "signal_score" in df.columns:
            return pd.to_numeric(df["signal_score"], errors="coerce")
        if "score" in df.columns:
            return pd.to_numeric(df["score"], errors="coerce")
        return pd.Series([0.0] * len(df), index=df.index)

    def _accepted_series(df, scores):
        if "accepted" in df.columns:
            return df["accepted"].astype(bool)
        return scores.fillna(0.0) >= _SCORE_THRESHOLD

    def _sub_score_series(df, names):
        for name in names:
            if name in df.columns:
                return pd.to_numeric(df[name], errors="coerce")
        return pd.Series([float("nan")] * len(df), index=df.index)

    def _money_series(df, column: str):
        values = pd.to_numeric(df[column], errors="coerce")
        return values.map(lambda v: "-" if pd.isna(v) else f"{v:,.0f}円")

    def _signed_money_series(df, column: str):
        values = pd.to_numeric(df[column], errors="coerce")
        return values.map(lambda v: "-" if pd.isna(v) else f"{v:+,.0f}円")

    def _quantity_series(df, column: str):
        values = pd.to_numeric(df[column], errors="coerce")
        return values.map(lambda v: "-" if pd.isna(v) else f"{v:,.0f}")

    def _price_series(df, column: str):
        values = pd.to_numeric(df[column], errors="coerce")
        return values.map(lambda v: "-" if pd.isna(v) else f"{v:,.3f}")

    def _signals_frame(df):
        if df is None or df.empty:
            return None
        out = pd.DataFrame(index=df.index)
        if "timestamp" in df.columns:
            stamps = pd.to_datetime(df["timestamp"], errors="coerce")
            out["時刻"] = stamps.dt.strftime("%m/%d %H:%M").fillna("")
        else:
            out["時刻"] = ""
        out["通貨ペア"] = df["symbol"].astype(str) if "symbol" in df.columns else ""
        out["シグナル"] = _action_series(df)
        scores = _score_series(df)
        out["スコア"] = scores.map(lambda v: "-" if pd.isna(v) else f"{v:.2f}")
        out["トレンド"] = _sub_score_series(df, ["sub_score_trend_regime", "trend"]).map(
            lambda v: "-" if pd.isna(v) else f"{v:.2f}"
        )
        out["Pullback"] = _sub_score_series(
            df, ["sub_score_pullback_continuation", "pullback"]
        ).map(lambda v: "-" if pd.isna(v) else f"{v:.2f}")
        out["Compression"] = _sub_score_series(
            df, ["sub_score_breakout_compression", "compression"]
        ).map(lambda v: "-" if pd.isna(v) else f"{v:.2f}")
        accepted = _accepted_series(df, scores)
        out["採用"] = accepted.map({True: "はい", False: "いいえ"})
        if "trade_quantity" in df.columns:
            out["数量"] = _quantity_series(df, "trade_quantity")
        if "trade_entry_price" in df.columns:
            out["約定価格"] = _price_series(df, "trade_entry_price")
        if "trade_entry_notional_jpy" in df.columns:
            out["建玉金額"] = _money_series(df, "trade_entry_notional_jpy")
        if "trade_initial_risk_jpy" in df.columns:
            out["リスク額"] = _money_series(df, "trade_initial_risk_jpy")
        if "trade_net_pnl_jpy" in df.columns:
            out["取引損益"] = _signed_money_series(df, "trade_net_pnl_jpy")
        if "session_label_ja" in df.columns:
            out["市場セッション"] = df["session_label_ja"].astype(str)
        elif "session" in df.columns:
            out["市場セッション"] = df["session"].astype(str)
        else:
            out["市場セッション"] = ""
        if "explanation_ja" in df.columns:
            explanations = df["explanation_ja"].astype(str)
        elif "explanation" in df.columns:
            explanations = df["explanation"].astype(str)
        else:
            explanations = pd.Series([""] * len(df), index=df.index)
        out["説明"] = explanations.map(
            lambda value: value if len(value) <= 54 else f"{value[:54]}…"
        )
        return out.tail(300).reset_index(drop=True)

    def _symbol_frame(df):
        if df is None or df.empty or "symbol" not in df.columns:
            return None
        scores = _score_series(df)
        accepted = _accepted_series(df, scores)
        work = pd.DataFrame(
            {
                "symbol": df["symbol"].astype(str),
                "score": scores,
                "accepted": accepted,
            }
        )
        grouped = (
            work.groupby("symbol")
            .agg(
                total=("symbol", "size"),
                accepted=("accepted", "sum"),
                mean_score=("score", "mean"),
            )
            .reset_index()
        )
        if grouped.empty:
            return None
        grouped["採用率"] = (grouped["accepted"] / grouped["total"] * 100).map(
            lambda v: f"{v:.1f}%"
        )
        grouped["平均スコア"] = grouped["mean_score"].map(
            lambda v: "-" if pd.isna(v) else f"{v:.2f}"
        )
        grouped = grouped.rename(
            columns={"symbol": "通貨ペア", "total": "総数", "accepted": "採用"}
        )
        grouped = grouped[["通貨ペア", "総数", "採用", "採用率", "平均スコア"]]
        grouped["採用"] = grouped["採用"].astype(int)
        return grouped.sort_values("採用", ascending=False).head(5).reset_index(drop=True)

    def _snapshot_from_frame(df):
        if df is None or df.empty:
            return {
                "recent_signals": pd.DataFrame(),
                "summary": {
                    "total": 0,
                    "accepted": 0,
                    "buy_accepted": 0,
                    "sell_accepted": 0,
                    "mean_score": float("nan"),
                },
                "histogram": {"all": [0] * 11, "accepted": [0] * 11, "rejected": [0] * 11},
                "symbol_frame": pd.DataFrame(
                    columns=["通貨ペア", "総数", "採用", "採用率", "平均スコア"]
                ),
            }
        scores = _score_series(df)
        accepted = _accepted_series(df, scores)
        actions = _action_series(df)
        histogram = {"all": [0] * 11, "accepted": [0] * 11, "rejected": [0] * 11}
        for value, is_accepted in zip(scores.tolist(), accepted.tolist(), strict=False):
            if value != value:
                continue
            bounded = max(0.0, min(1.0, float(value)))
            bucket = min(10, int(bounded * 11))
            histogram["all"][bucket] += 1
            histogram["accepted" if is_accepted else "rejected"][bucket] += 1
        return {
            "recent_signals": df.tail(300).reset_index(drop=True),
            "summary": {
                "total": int(len(df)),
                "accepted": int(accepted.sum()),
                "buy_accepted": int(((actions == "買い") & accepted).sum()),
                "sell_accepted": int(((actions == "売り") & accepted).sum()),
                "mean_score": float(scores.mean()) if scores.notna().any() else float("nan"),
            },
            "histogram": histogram,
            "symbol_frame": _symbol_frame(df),
        }

    def _update_kpis(snapshot):
        summary = dict(snapshot.get("summary") or {}) if snapshot else {}
        total = int(summary.get("total") or 0)
        accepted_count = int(summary.get("accepted") or 0)
        buy = int(summary.get("buy_accepted") or 0)
        sell = int(summary.get("sell_accepted") or 0)
        mean_score = (
            float(summary.get("mean_score"))
            if summary.get("mean_score") is not None
            else float("nan")
        )
        if total <= 0:
            for tile in (kpi_total, kpi_accepted, kpi_side, kpi_score):
                tile.set_value("-")
                tile.set_note("")
            kpi_score.set_note(f"閾値 {_SCORE_THRESHOLD:.2f}")
            return
        kpi_total.set_value(f"{total:,}")
        kpi_total.set_note("直近バックテスト")
        kpi_accepted.set_value(f"{(accepted_count / total * 100):.1f}%")
        kpi_accepted.set_note(f"{accepted_count:,} / {total:,}")
        kpi_side.set_value_html(
            f'<span style="color:{Tokens.POS}">{buy}</span>'
            f'<span style="color:{Tokens.MUTED_2}"> / </span>'
            f'<span style="color:{Tokens.NEG}">{sell}</span>'
        )
        kpi_side.set_note("採用分のみ")
        if mean_score == mean_score:  # not NaN
            kpi_score.set_value(f"{mean_score:.2f}")
        else:
            kpi_score.set_value("-")
        kpi_score.set_note(f"閾値 {_SCORE_THRESHOLD:.2f}")

    def _refresh_histogram():
        snapshot = page._signal_snapshot or {}
        histogram_data = dict(snapshot.get("histogram") or {})
        if not histogram_data:
            histogram.set_scores([])
            return
        mode = hist_seg.current()
        mode_key = "all" if mode == 0 else ("accepted" if mode == 1 else "rejected")
        histogram.set_bars(histogram_data.get(mode_key, []), threshold=_SCORE_THRESHOLD)

    def _apply_list_filters():
        list_proxy.set_symbol_filter(filter_input.text())
        selected = side_seg.current()
        if selected == 0:
            list_proxy.set_side_filter(None)
        else:
            list_proxy.set_side_filter(_SIDE_SEG_LABELS[selected])

    def _render_all():
        snapshot = page._signal_snapshot or {}
        recent_frame = snapshot.get("recent_signals")
        _update_kpis(snapshot)
        _refresh_histogram()
        symbol_model.set_frame(snapshot.get("symbol_frame"))
        list_model.set_frame(_signals_frame(recent_frame))
        header = list_view.horizontalHeader()
        width_by_label = {
            "時刻": 90,
            "通貨ペア": 100,
            "シグナル": 90,
            "スコア": 70,
            "トレンド": 70,
            "Pullback": 78,
            "Compression": 94,
            "採用": 72,
            "数量": 78,
            "約定価格": 92,
            "建玉金額": 100,
            "リスク額": 88,
            "取引損益": 88,
            "市場セッション": 110,
            "説明": 360,
        }
        for column, label in enumerate(getattr(list_model, "_headers", [])):
            header.resizeSection(column, width_by_label.get(label, 100))
        _apply_list_filters()

    def _selected_run() -> dict[str, object] | None:
        selected_run_id = getattr(page, "_selected_run_id", None)
        for row in getattr(page, "_saved_runs", []):
            if str(row.get("run_id")) == str(selected_run_id):
                return row
        return None

    def _update_run_meta(run: dict[str, object] | None) -> None:
        if run is None:
            strategy_value.setText("保存済みバックテスト結果がありません。")
            period_value.setText("-")
            run_id_value.setText("-")
            ml_value.setText("-")
            return
        strategy_value.setText(str(run.get("strategy_label") or run.get("strategy_name") or "-"))
        period_value.setText(f"{run.get('start_date') or '-'} → {run.get('end_date') or '-'}")
        run_id_value.setText(str(run.get("run_id") or "-"))
        ml_mode = str(run.get("ml_mode_label") or "-")
        ml_label = str(run.get("ml_model_label") or "-")
        ml_value.setText(ml_label if ml_label == ml_mode else f"{ml_mode}\n{ml_label}")

    def _sync_saved_run_choices(preferred_run_id: str | None = None) -> None:
        runs = list(app_state.list_backtest_runs() or [])
        page._saved_runs = runs
        if preferred_run_id is None:
            preferred_run_id = getattr(page, "_selected_run_id", None)
        if preferred_run_id is None:
            last_result = getattr(app_state, "last_result", None)
            preferred_run_id = getattr(last_result, "run_id", None)
        if preferred_run_id is None and runs:
            preferred_run_id = str(runs[0].get("run_id") or "")
        blocker = QSignalBlocker(result_combo)
        result_combo.clear()
        for row in runs:
            result_combo.addItem(
                str(row.get("display_label") or row.get("run_id") or "-"),
                str(row.get("run_id") or ""),
            )
        if not runs:
            result_combo.setEnabled(False)
            page._selected_run_id = None
            del blocker
            _update_run_meta(None)
            return
        result_combo.setEnabled(True)
        target_index = next(
            (
                index
                for index, row in enumerate(runs)
                if str(row.get("run_id") or "") == str(preferred_run_id or "")
            ),
            0,
        )
        result_combo.setCurrentIndex(target_index)
        page._selected_run_id = str(runs[target_index].get("run_id") or "")
        del blocker
        _update_run_meta(_selected_run())

    def _set_busy(is_busy: bool) -> None:
        set_button_enabled(recalc_btn, not is_busy, busy=is_busy)
        set_button_enabled(export_btn, not is_busy, busy=is_busy)
        recalc_btn.setText("再計算中..." if is_busy else "現在の戦略で再計算")

    def _set_loading(is_loading: bool, *, run_id: str | None = None) -> None:
        page._loading_run_id = run_id if is_loading else None
        result_combo.setEnabled(not is_loading and bool(page._saved_runs))
        filter_input.setEnabled(not is_loading)
        side_seg.setEnabled(not is_loading)
        hist_seg.setEnabled(not is_loading)
        if is_loading:
            strategy_value.setText("保存済み結果を読み込んでいます...")
            period_value.setText("-")
            run_id_value.setText(str(run_id or "-"))
            ml_value.setText("-")

    def _log(message: str) -> None:
        if log_message is not None:
            log_message(message)

    def _export_csv() -> None:
        df = page._full_frame
        if df is None or df.empty:
            _log("シグナルが存在しません。")
            return
        path, _ = QFileDialog.getSaveFileName(page, "CSV 書き出し", "signals.csv", "CSV (*.csv)")
        if not path:
            return
        frame = _signals_frame(df)
        if frame is None:
            _log("シグナルが存在しません。")
            return
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        _log(f"CSV を書き出しました: {path}")

    def _on_recalc_finished(_result) -> None:
        _set_busy(False)
        page._selected_run_id = getattr(_result, "run_id", None)
        refresh()
        _log("シグナル再計算が完了しました。")

    def _on_recalc_error(message: str) -> None:
        _set_busy(False)
        _log(f"シグナル再計算に失敗しました: {message}")

    def _recalc() -> None:
        if submit_task is None:
            return
        _set_busy(True)
        submit_task(app_state.run_backtest, _on_recalc_finished, _on_recalc_error)

    def _load_selected_saved_run(run_id: str | None) -> None:
        if not run_id:
            page._full_frame = None
            page._signal_snapshot = None
            _update_run_meta(None)
            _render_all()
            return
        if submit_task is not None:
            _set_loading(True, run_id=str(run_id))

            def _on_loaded(snapshot) -> None:  # noqa: ANN001
                page._signal_snapshot = snapshot
                recent_signals = snapshot.get("recent_signals")
                page._full_frame = (
                    recent_signals.copy()
                    if recent_signals is not None and not recent_signals.empty
                    else None
                )
                _set_loading(False)
                _update_run_meta(_selected_run())
                _render_all()

            def _on_load_error(message: str) -> None:
                _set_loading(False)
                last_result = getattr(app_state, "last_result", None)
                if last_result is not None and getattr(last_result, "run_id", None) == str(run_id):
                    fallback_frame = getattr(last_result, "signals", None)
                    page._full_frame = (
                        fallback_frame.copy()
                        if fallback_frame is not None and not fallback_frame.empty
                        else None
                    )
                    page._signal_snapshot = _snapshot_from_frame(page._full_frame)
                    _update_run_meta(_selected_run())
                    _render_all()
                else:
                    page._full_frame = None
                    page._signal_snapshot = None
                    _update_run_meta(_selected_run())
                    _render_all()
                _log(f"保存済みバックテスト結果の読込に失敗しました: {message}")

            submit_task(
                lambda run_id=str(run_id): app_state.load_saved_run_signal_snapshot(run_id),
                _on_loaded,
                _on_load_error,
            )
            return
        try:
            snapshot = app_state.load_saved_run_signal_snapshot(str(run_id))
        except Exception as exc:  # noqa: BLE001
            last_result = getattr(app_state, "last_result", None)
            if last_result is not None and getattr(last_result, "run_id", None) == str(run_id):
                fallback_frame = getattr(last_result, "signals", None)
                page._full_frame = (
                    fallback_frame.copy()
                    if fallback_frame is not None and not fallback_frame.empty
                    else None
                )
                page._signal_snapshot = _snapshot_from_frame(page._full_frame)
            else:
                page._full_frame = None
                page._signal_snapshot = None
            _update_run_meta(_selected_run())
            _render_all()
            _log(f"保存済みバックテスト結果の読込に失敗しました: {exc}")
            return
        page._signal_snapshot = snapshot
        recent_signals = snapshot.get("recent_signals")
        page._full_frame = (
            recent_signals.copy()
            if recent_signals is not None and not recent_signals.empty
            else None
        )
        _update_run_meta(_selected_run())
        _render_all()

    def refresh(*, allow_io: bool = True) -> None:
        current_run_id = getattr(page, "_selected_run_id", None)
        if current_run_id is None:
            current_run_id = getattr(getattr(app_state, "last_result", None), "run_id", None)
        _sync_saved_run_choices(current_run_id)
        selected_run_id = getattr(page, "_selected_run_id", None)
        getattr(app_state, "last_result", None)
        if not allow_io or (not page.isVisible()):
            _update_run_meta(_selected_run())
            return
        if getattr(page, "_loading_run_id", None) == selected_run_id:
            return
        _load_selected_saved_run(selected_run_id)

    export_btn.clicked.connect(_export_csv)
    recalc_btn.clicked.connect(_recalc)
    result_combo.currentIndexChanged.connect(
        lambda _=None: (
            setattr(page, "_selected_run_id", result_combo.currentData()),
            _load_selected_saved_run(result_combo.currentData()),
        )
    )
    filter_input.textChanged.connect(lambda _=None: _apply_list_filters())
    side_seg.currentChanged.connect(lambda _=None: _apply_list_filters())
    hist_seg.currentChanged.connect(lambda _=None: _refresh_histogram())

    if submit_task is None:
        recalc_btn.setEnabled(False)
        recalc_btn.setToolTip("再計算コールバックが未接続です")

    page.refresh = refresh
    refresh(allow_io=False)
    return page
