"""Trade history page."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


_SIDE_LABELS = {"buy": "買い", "sell": "売り", "long": "買い", "short": "売り"}
_WL_LABELS = ["全て", "勝ち", "負け"]


def _pick_numeric(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for column in candidates:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce")
    return pd.Series([float("nan")] * len(df), index=df.index)


def _history_frame(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    out = pd.DataFrame(index=df.index)

    def _ts_series(columns: list[str]) -> pd.Series:
        for column in columns:
            if column in df.columns:
                stamps = pd.to_datetime(df[column], errors="coerce")
                return stamps.dt.strftime("%m/%d %H:%M").fillna("")
        return pd.Series([""] * len(df), index=df.index)

    out["エントリー時刻"] = _ts_series(["entry_ts", "entry_time", "timestamp"])
    out["決済時刻"] = _ts_series(["exit_ts", "exit_time", "closed_at"])
    out["通貨ペア"] = df["symbol"].astype(str) if "symbol" in df.columns else ""
    if "side" in df.columns:
        out["売買"] = (
            df["side"].astype(str).str.lower().map(_SIDE_LABELS).fillna(df["side"].astype(str))
        )
    else:
        out["売買"] = ""
    qty = _pick_numeric(df, ["quantity", "qty", "size"])
    out["数量"] = qty.map(lambda v: "-" if pd.isna(v) else f"{v:,.0f}")
    entry = _pick_numeric(df, ["entry_price", "avg_entry_price", "price"])
    out["エントリー"] = entry.map(lambda v: "-" if pd.isna(v) else f"{v:,.3f}")
    exit_px = _pick_numeric(df, ["exit_price", "close_price"])
    out["決済"] = exit_px.map(lambda v: "-" if pd.isna(v) else f"{v:,.3f}")
    pnl = _pick_numeric(df, ["pnl_jpy", "pnl", "realized_pnl", "profit"])
    out["損益 (JPY)"] = pnl.map(lambda v: "-" if pd.isna(v) else f"{v:+,.0f}")
    pnl_pct = _pick_numeric(df, ["pnl_pct", "return", "return_pct"])
    if pnl_pct.notna().any():
        out["損益率"] = pnl_pct.map(
            lambda v: "-" if pd.isna(v) else (f"{v:+.2%}" if abs(v) <= 1.5 else f"{v:+.2f}%")
        )
    else:
        out["損益率"] = "-"
    holding = _pick_numeric(df, ["holding_bars", "managed_bars_held", "bars_held"])
    out["保有"] = holding.map(lambda v: "-" if pd.isna(v) else f"{int(v)} bar")
    if "exit_reason" in df.columns:
        out["終了理由"] = df["exit_reason"].astype(str)
    elif "close_reason" in df.columns:
        out["終了理由"] = df["close_reason"].astype(str)
    else:
        out["終了理由"] = ""
    return out.reset_index(drop=True)


def _pnl_value(text: str) -> float:
    if not text:
        return 0.0
    cleaned = str(text).replace(",", "").replace("+", "").replace("−", "-").rstrip("%").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def build_history_page(app_state, submit_task=None, log_message=None, on_go_to_reports=None):  # pragma: no cover - UI helper
    from PySide6.QtCore import QSortFilterProxyModel, Qt, QUrl
    from PySide6.QtGui import QColor, QDesktopServices
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
        QMessageBox,
        QPushButton,
        QStyledItemDelegate,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.theme import Tokens
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.kpi import KpiTile
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl

    DataFrameTableModel = load_dataframe_model_class()

    # ---- Delegates ---------------------------------------------------------

    class MonoRightDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")

    class SymbolDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.font.setFamily("JetBrains Mono")

    class PnLDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")
            text = str(index.data(Qt.DisplayRole) or "").strip()
            if not text or text == "-":
                return
            if text.startswith("+"):
                option.palette.setColor(option.palette.Text, QColor(Tokens.POS))
            elif text.startswith(("-", "−")):
                option.palette.setColor(option.palette.Text, QColor(Tokens.NEG))

    # ---- Proxy filter ------------------------------------------------------

    class HistoryProxy(QSortFilterProxyModel):
        PAIR_COL = 2
        PNL_COL = 7

        def __init__(self, parent=None):
            super().__init__(parent)
            self._pair = ""
            self._wl = 0

        def set_filters(self, pair: str, wl: int) -> None:
            self._pair = pair.strip().upper()
            self._wl = wl
            self.invalidateFilter()

        def filterAcceptsRow(self, row, parent):  # noqa: N802
            model = self.sourceModel()
            if model is None:
                return True
            sym_idx = model.index(row, self.PAIR_COL, parent)
            symbol = str(model.data(sym_idx) or "").upper()
            if self._pair and self._pair not in symbol:
                return False
            pnl_idx = model.index(row, self.PNL_COL, parent)
            pnl = _pnl_value(model.data(pnl_idx) or "0")
            if self._wl == 1 and pnl <= 0:
                return False
            if self._wl == 2 and pnl >= 0:
                return False
            return True

    # ---- Page scaffold -----------------------------------------------------

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page._delegates = []  # type: ignore[attr-defined]

    # Header
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("取引履歴")
    title.setProperty("role", "h1")
    subtitle = QLabel("保存済み実行から、確定した取引と損益を確認できます。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    export_btn = QPushButton("CSV 書き出し")
    export_btn.setProperty("variant", "ghost")
    report_btn = QPushButton("詳細レポート")
    report_btn.setProperty("variant", "primary")
    header.addWidget(export_btn)
    header.addWidget(report_btn)
    layout.addLayout(header)

    # KPI grid
    kpi_grid = QGridLayout()
    kpi_grid.setHorizontalSpacing(12)
    kpi_grid.setVerticalSpacing(12)
    for column in range(4):
        kpi_grid.setColumnStretch(column, 1)
    kpi_total = KpiTile(label="累計取引", value="-", value_variant="mono", note="全実行合計")
    kpi_pnl = KpiTile(label="累計損益", value="-", value_variant="mono", note="初期資産比")
    kpi_winrate = KpiTile(label="勝率", value="-", value_variant="mono")
    kpi_pf = KpiTile(label="Profit Factor", value="-", value_variant="mono", note="平均利益 / 平均損失")
    tiles = [kpi_total, kpi_pnl, kpi_winrate, kpi_pf]
    for index, tile in enumerate(tiles):
        kpi_grid.addWidget(tile, 0, index)
    layout.addLayout(kpi_grid)

    # Trade log card
    run_combo = QComboBox()
    run_combo.setFixedWidth(200)
    filter_pair = QLineEdit()
    filter_pair.setPlaceholderText("通貨ペア")
    filter_pair.setFixedWidth(140)
    wl_seg = SegmentedControl(_WL_LABELS, current=0, data=_WL_LABELS)

    tools = QWidget()
    tools_lay = QHBoxLayout(tools)
    tools_lay.setContentsMargins(0, 0, 0, 0)
    tools_lay.setSpacing(8)
    tools_lay.addWidget(run_combo)
    tools_lay.addWidget(filter_pair)
    tools_lay.addWidget(wl_seg)

    log_card = Card(title="取引ログ", header_right=tools)

    log_model = DataFrameTableModel()
    proxy = HistoryProxy()
    proxy.setSourceModel(log_model)
    log_view = QTableView()
    log_view.setAlternatingRowColors(False)
    log_view.setShowGrid(False)
    log_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
    log_view.setSelectionBehavior(QAbstractItemView.SelectRows)
    log_view.setSelectionMode(QAbstractItemView.SingleSelection)
    log_view.verticalHeader().setVisible(False)
    hdr = log_view.horizontalHeader()
    hdr.setStretchLastSection(True)
    hdr.setSectionResizeMode(QHeaderView.Interactive)
    log_view.setWordWrap(False)
    log_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
    log_view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
    log_view.setMinimumHeight(360)
    log_view.setModel(proxy)

    log_card.addBodyWidget(log_view)
    layout.addWidget(log_card, 1)

    # ---- Helpers -----------------------------------------------------------

    page._current_df = None  # type: ignore[attr-defined]
    page._loading = False  # type: ignore[attr-defined]

    def _log(message: str) -> None:
        if log_message is not None:
            log_message(message)

    def _load_trades_for_run(row: dict) -> pd.DataFrame | None:
        run_id = str(row.get("run_id", ""))
        last_result = getattr(app_state, "last_result", None)
        if last_result is not None and getattr(last_result, "run_id", None) == run_id:
            trades = getattr(last_result, "trades", None)
            if trades is not None:
                return trades.copy()
        output_dir_value = row.get("output_dir")
        if not output_dir_value:
            return None
        output_dir = Path(str(output_dir_value))
        if not output_dir.exists():
            return None
        for name in ("trades.parquet", "trades.csv"):
            candidate = output_dir / name
            if candidate.exists():
                try:
                    if name.endswith(".parquet"):
                        return pd.read_parquet(candidate)
                    return pd.read_csv(candidate)
                except Exception:  # noqa: BLE001
                    continue
        return None

    def _populate_run_combo() -> None:
        try:
            runs = app_state.list_runs() or []
        except Exception:  # noqa: BLE001
            runs = []
        run_combo.blockSignals(True)
        current_data = run_combo.currentData()
        run_combo.clear()
        run_combo.addItem("全実行", userData=None)
        for row in runs:
            run_id = str(row.get("run_id", ""))
            finished_at = str(row.get("finished_at", "")).split(".")[0]
            label = f"{run_id}"
            if finished_at:
                label = f"{run_id}  {finished_at}"
            run_combo.addItem(label, userData=run_id)
        if current_data is not None:
            for index in range(run_combo.count()):
                if run_combo.itemData(index) == current_data:
                    run_combo.setCurrentIndex(index)
                    break
        elif run_combo.count() > 1:
            run_combo.setCurrentIndex(1)
        run_combo.blockSignals(False)

    def _set_loading(is_loading: bool) -> None:
        page._loading = is_loading
        run_combo.setEnabled(not is_loading)
        filter_pair.setEnabled(not is_loading)
        wl_seg.setEnabled(not is_loading)
        export_btn.setEnabled(not is_loading)
        report_btn.setEnabled(not is_loading)
        subtitle.setText("保存済み実行から、確定した取引と損益を確認できます。" if not is_loading else "取引履歴を読み込んでいます...")

    def _apply_log_table_widths() -> None:
        width_map = {
            0: 110,
            1: 110,
            2: 90,
            3: 70,
            4: 84,
            5: 92,
            6: 88,
            7: 92,
            8: 72,
            9: 280,
        }
        header = log_view.horizontalHeader()
        for column, width in width_map.items():
            if column < log_model.columnCount():
                header.resizeSection(column, width)

    def _apply_loaded_df(df: pd.DataFrame | None, initial_cash: float) -> None:
        page._current_df = df
        log_model.set_frame(_history_frame(df))
        _apply_log_table_widths()
        _update_kpis(df, initial_cash)
        _apply_filters()

    def _load_selected_run() -> None:
        try:
            runs = app_state.list_runs() or []
        except Exception:  # noqa: BLE001
            runs = []
        run_id = run_combo.currentData()
        initial_cash = float(getattr(app_state.config.risk, "starting_cash", 0.0) or 0.0)
        if run_id is None:
            frames = []
            for row in runs:
                trades = _load_trades_for_run(row)
                if trades is not None and not trades.empty:
                    frames.append(trades)
            combined = pd.concat(frames, ignore_index=True) if frames else None
            _apply_loaded_df(combined, initial_cash)
        else:
            row = next((r for r in runs if str(r.get("run_id", "")) == run_id), None)
            _apply_loaded_df(_load_trades_for_run(row) if row else None, initial_cash)

    def _load_selected_run_async() -> None:
        if submit_task is None or not page.isVisible():
            _load_selected_run()
            return
        _set_loading(True)

        def _worker():
            try:
                runs = app_state.list_runs() or []
            except Exception:  # noqa: BLE001
                runs = []
            run_id = run_combo.currentData()
            initial_cash = float(getattr(app_state.config.risk, "starting_cash", 0.0) or 0.0)
            if run_id is None:
                frames = []
                for row in runs:
                    trades = _load_trades_for_run(row)
                    if trades is not None and not trades.empty:
                        frames.append(trades)
                combined = pd.concat(frames, ignore_index=True) if frames else None
                return combined, initial_cash
            row = next((r for r in runs if str(r.get("run_id", "")) == run_id), None)
            return (_load_trades_for_run(row) if row else None, initial_cash)

        def _on_loaded(payload) -> None:  # noqa: ANN001
            df, initial_cash = payload
            _set_loading(False)
            _apply_loaded_df(df, float(initial_cash))

        def _on_error(message: str) -> None:
            _set_loading(False)
            _log(f"取引履歴の読込に失敗しました: {message}")

        submit_task(_worker, _on_loaded, _on_error)

    def _update_kpis(df: pd.DataFrame | None, initial_cash: float) -> None:
        if df is None or df.empty:
            for tile in tiles:
                tile.set_value("-")
                tile.set_trend(None)
            kpi_total.set_note("全実行合計")
            kpi_pnl.set_note("初期資産比")
            kpi_winrate.set_note("")
            kpi_pf.set_note("平均利益 / 平均損失")
            return
        pnl = _pick_numeric(df, ["pnl_jpy", "pnl", "realized_pnl", "profit"]).fillna(0.0)
        total = len(df)
        pnl_sum = float(pnl.sum())
        wins_count = int((pnl > 0).sum())
        losses_count = int((pnl < 0).sum())
        win_rate = wins_count / total if total else 0.0
        avg_win = float(pnl[pnl > 0].mean()) if wins_count else 0.0
        avg_loss = float(abs(pnl[pnl < 0].mean())) if losses_count else 0.0
        pf: float
        if avg_loss == 0 and avg_win > 0:
            pf = float("inf")
        elif avg_loss == 0:
            pf = 0.0
        else:
            pf = avg_win / avg_loss

        kpi_total.set_value(f"{total:,}")
        kpi_total.set_note("全実行合計" if run_combo.currentData() is None else "選択実行")
        tone = "pos" if pnl_sum >= 0 else "neg"
        kpi_pnl.set_value(f"{pnl_sum:+,.0f}", tone=tone)
        if initial_cash:
            pnl_pct = pnl_sum / initial_cash
            kpi_pnl.set_trend(
                "up" if pnl_pct >= 0 else "down",
                f"{abs(pnl_pct):.1%}",
            )
            kpi_pnl.set_note(f"初期資産 {initial_cash:,.0f} JPY")
        else:
            kpi_pnl.set_trend(None)
            kpi_pnl.set_note("初期資産比")
        kpi_winrate.set_value(f"{win_rate * 100:.1f}%")
        kpi_winrate.set_note(f"{wins_count:,} 勝 / {losses_count:,} 負")
        if pf == float("inf"):
            kpi_pf.set_value("∞")
        elif pf == 0.0 and avg_win == 0 and avg_loss == 0:
            kpi_pf.set_value("-")
        else:
            kpi_pf.set_value(f"{pf:.2f}")
        kpi_pf.set_note("平均利益 / 平均損失")

    def _apply_filters() -> None:
        proxy.set_filters(filter_pair.text(), wl_seg.current())

    def _export_csv() -> None:
        df = page._current_df
        if df is None or df.empty:
            _log("書き出す取引がありません。")
            return
        path, _ = QFileDialog.getSaveFileName(page, "CSV 書き出し", "trades.csv", "CSV (*.csv)")
        if not path:
            return
        frame = _history_frame(df)
        if frame is None or frame.empty:
            _log("書き出す取引がありません。")
            return
        pair_query = filter_pair.text().strip().upper()
        if pair_query:
            frame = frame[frame["通貨ペア"].str.upper().str.contains(pair_query, na=False)]
        wl = wl_seg.current()
        if wl != 0:
            mask = frame["損益 (JPY)"].apply(lambda v: _pnl_value(v) > 0)
            frame = frame[mask if wl == 1 else ~mask]
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        _log(f"CSV を書き出しました: {path}")

    def _open_report() -> None:
        run_id = run_combo.currentData()
        report_path: Path | None = None
        if run_id is not None and hasattr(app_state, "locate_report"):
            try:
                report_dir = app_state.locate_report(run_id)
            except Exception:  # noqa: BLE001
                report_dir = None
            if report_dir is not None:
                candidate = Path(report_dir) / "report.html"
                if candidate.exists():
                    report_path = candidate
        if report_path is not None:
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(report_path)))
            _log(f"レポートを開きました: {report_path}")
            return
        if on_go_to_reports is not None:
            try:
                on_go_to_reports()
                return
            except Exception:  # noqa: BLE001
                pass
        QMessageBox.information(
            page,
            "詳細レポート",
            "選択した実行にはレポート HTML が存在しません。",
        )

    def refresh() -> None:
        if not page.isVisible():
            return
        _populate_run_combo()
        _load_selected_run_async()

    # ---- Wiring ------------------------------------------------------------

    run_combo.currentIndexChanged.connect(lambda _=None: _load_selected_run_async())
    filter_pair.textChanged.connect(lambda _=None: _apply_filters())
    wl_seg.currentChanged.connect(lambda _=None: _apply_filters())
    export_btn.clicked.connect(_export_csv)
    report_btn.clicked.connect(_open_report)

    page.refresh = refresh
    refresh()
    return page
