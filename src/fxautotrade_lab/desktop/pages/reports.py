"""Reports hub page — browse saved run reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


@dataclass
class RunView:
    run_id: str
    strategy_name: str
    finished_at: str
    output_dir: Path | None
    total_return_pct: float
    sharpe: float
    max_drawdown_pct: float
    trade_count: int
    starting_cash: float
    period_start: str
    period_end: str
    is_latest: bool = field(default=False)

    @property
    def report_html_path(self) -> Path | None:
        if self.output_dir is None:
            return None
        candidate = self.output_dir / "report.html"
        return candidate if candidate.exists() else None

    @property
    def report_csv_path(self) -> Path | None:
        if self.output_dir is None:
            return None
        for name in ("summary.csv", "trades.csv", "report.csv"):
            candidate = self.output_dir / name
            if candidate.exists():
                return candidate
        return None


def _to_run_view(row: dict, cfg) -> RunView:
    output_dir_value = row.get("output_dir")
    output_dir = Path(str(output_dir_value)) if output_dir_value else None
    metrics = row.get("metrics", {}) or {}
    total = float(metrics.get("total_return", 0.0) or 0.0) * 100
    sharpe = metrics.get("sharpe")
    sharpe_val = float(sharpe) if sharpe is not None else 0.0
    dd = float(metrics.get("max_drawdown", 0.0) or 0.0) * 100
    trades = int(metrics.get("number_of_trades", 0) or 0)
    starting_cash = float(getattr(cfg.risk, "starting_cash", 0.0) or 0.0)
    period_start = str(row.get("start_date") or getattr(cfg.data, "start_date", ""))
    period_end = str(row.get("end_date") or getattr(cfg.data, "end_date", ""))
    return RunView(
        run_id=str(row.get("run_id", "")),
        strategy_name=str(row.get("strategy_name", "")),
        finished_at=str(row.get("finished_at", "")),
        output_dir=output_dir,
        total_return_pct=total,
        sharpe=sharpe_val,
        max_drawdown_pct=dd,
        trade_count=trades,
        starting_cash=starting_cash,
        period_start=period_start,
        period_end=period_end,
    )


def _format_period(start: str, end: str) -> str:
    def _trim(value: str) -> str:
        if not value:
            return "-"
        try:
            return pd.Timestamp(value).strftime("%Y-%m")
        except Exception:  # noqa: BLE001
            return value[:7] if len(value) >= 7 else value

    return f"{_trim(start)} → {_trim(end)}"


def _format_created_at(finished_at: str) -> str:
    if not finished_at:
        return "-"
    try:
        return pd.Timestamp(finished_at).strftime("%m/%d %H:%M")
    except Exception:  # noqa: BLE001
        return finished_at[:16] if len(finished_at) >= 16 else finished_at


def build_reports_page(app_state, log_message=None):  # pragma: no cover - UI helper
    from PySide6.QtCore import QEvent, Qt, QUrl
    from PySide6.QtGui import QColor, QDesktopServices
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QApplication,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QMessageBox,
        QPushButton,
        QStyle,
        QStyleOptionButton,
        QStyledItemDelegate,
        QTableView,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.theme import Tokens, repolish
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.chip import Chip

    DataFrameTableModel = load_dataframe_model_class()

    # ---- Delegates --------------------------------------------------------

    class SymbolDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.font.setFamily("JetBrains Mono")

    class MonoRightDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")

    class PnLPctDelegate(QStyledItemDelegate):
        def initStyleOption(self, option, index):  # noqa: N802
            super().initStyleOption(option, index)
            option.displayAlignment = Qt.AlignRight | Qt.AlignVCenter
            option.font.setFamily("JetBrains Mono")
            text = str(option.text or "").strip()
            if not text or text in ("-", "-%"):
                return
            if text.startswith("+"):
                option.palette.setColor(option.palette.Text, QColor(Tokens.POS))
            elif text.startswith(("-", "−")):
                option.palette.setColor(option.palette.Text, QColor(Tokens.NEG))

    class OpenButtonDelegate(QStyledItemDelegate):
        def __init__(self, parent, on_click):
            super().__init__(parent)
            self._on_click = on_click

        def paint(self, painter, option, index):
            painter.save()
            btn = QStyleOptionButton()
            margin_x = 6
            btn.rect = option.rect.adjusted(margin_x, 4, -margin_x, -4)
            btn.text = "開く"
            btn.state = QStyle.State_Enabled | QStyle.State_Raised
            QApplication.style().drawControl(QStyle.CE_PushButton, btn, painter)
            painter.restore()

        def editorEvent(self, event, model, option, index):  # noqa: N802
            if event.type() == QEvent.MouseButtonRelease and option.rect.contains(event.position().toPoint()):
                self._on_click(index.row())
                return True
            return False

    # ---- Page scaffold ----------------------------------------------------

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    # Header
    header = QHBoxLayout()
    header.setSpacing(12)
    header_text = QVBoxLayout()
    header_text.setSpacing(2)
    title = QLabel("レポート")
    title.setProperty("role", "h1")
    subtitle = QLabel("実行ごとに生成された HTML / CSV レポートへアクセスできます。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    folder_btn = QPushButton("フォルダを開く")
    folder_btn.setProperty("variant", "ghost")
    generate_btn = QPushButton("レポートを生成")
    generate_btn.setProperty("variant", "primary")
    header.addWidget(folder_btn)
    header.addWidget(generate_btn)
    layout.addLayout(header)

    # Pinned row (max 3 cards)
    pinned_row = QHBoxLayout()
    pinned_row.setSpacing(12)
    layout.addLayout(pinned_row)

    # All runs card
    count_label = QLabel("0 件")
    count_label.setProperty("role", "muted2")
    all_card = Card(title="全ての実行", header_right=count_label)
    all_view = QTableView()
    all_view.setAlternatingRowColors(False)
    all_view.setShowGrid(False)
    all_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
    all_view.setSelectionBehavior(QAbstractItemView.SelectRows)
    all_view.setSelectionMode(QAbstractItemView.SingleSelection)
    all_view.verticalHeader().setVisible(False)
    hdr = all_view.horizontalHeader()
    hdr.setStretchLastSection(False)
    hdr.setSectionResizeMode(QHeaderView.ResizeToContents)
    all_view.setMinimumHeight(320)
    all_model = DataFrameTableModel()
    all_view.setModel(all_model)
    all_card.addBodyWidget(all_view)
    layout.addWidget(all_card, 1)

    # ---- State ------------------------------------------------------------
    page._runs_cache: list[RunView] = []  # type: ignore[attr-defined]

    def _log(message: str) -> None:
        if log_message is not None:
            log_message(message)

    def _reports_dir() -> Path:
        reporting = getattr(app_state.config, "reporting", None)
        if reporting is not None:
            output_dir = getattr(reporting, "output_dir", None)
            if output_dir:
                return Path(str(output_dir))
        return Path.cwd() / "reports"

    def _open_path(path: Path | None, missing_message: str) -> None:
        if path is not None and path.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))
            return
        QMessageBox.information(page, "レポート", missing_message)

    def _open_report_html(run: RunView) -> None:
        _open_path(run.report_html_path, f"HTML レポートが見つかりません: {run.run_id}")

    def _open_report_csv(run: RunView) -> None:
        _open_path(run.report_csv_path, f"CSV が見つかりません: {run.run_id}")

    def _open_reports_folder() -> None:
        folder = _reports_dir()
        folder.mkdir(parents=True, exist_ok=True)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(folder)))

    def _generate_latest_report() -> None:
        last_result = getattr(app_state, "last_result", None)
        if last_result is None:
            QMessageBox.information(
                page,
                "レポート",
                "先にバックテストを実行してください。",
            )
            return
        for attr in ("regenerate_report", "regenerate_reports", "generate_report", "save_reports"):
            fn = getattr(app_state, attr, None)
            if callable(fn):
                try:
                    fn()
                except Exception as exc:  # noqa: BLE001
                    QMessageBox.warning(page, "レポート", f"レポート生成に失敗しました: {exc}")
                    return
                _log("レポートを生成しました。")
                _reload()
                return
        QMessageBox.information(
            page,
            "レポート",
            "レポートは最新のバックテスト実行時に自動生成されます。再生成するにはバックテストを再実行してください。",
        )
        _reload()

    # ---- Pinned card builder ---------------------------------------------

    def _make_detail_cell(label_text: str, value_text: str, tone: str | None = None) -> QWidget:
        wrap = QWidget()
        vbox = QVBoxLayout(wrap)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.setSpacing(2)
        label = QLabel(label_text)
        label.setProperty("role", "detail-label")
        value = QLabel(value_text)
        value.setProperty("role", "detail-value")
        if tone:
            value.setProperty("tone", tone)
        vbox.addWidget(label)
        vbox.addWidget(value)
        return wrap

    def _make_pinned_card(run: RunView) -> Card:
        chip = Chip("最新", "running") if run.is_latest else None
        card = Card(title=run.run_id, header_right=chip)
        if card.title_label is not None:
            card.title_label.setProperty("variant", "mono-sm")
            repolish(card.title_label)

        detail_grid = QGridLayout()
        detail_grid.setHorizontalSpacing(12)
        detail_grid.setVerticalSpacing(10)
        detail_grid.setContentsMargins(0, 0, 0, 0)
        for column in range(2):
            detail_grid.setColumnStretch(column, 1)

        pnl = run.total_return_pct
        pnl_str = f"{'+' if pnl >= 0 else ''}{pnl:.2f}%"
        pnl_tone = "pos" if pnl >= 0 else "neg"
        detail_grid.addWidget(_make_detail_cell("総損益", pnl_str, tone=pnl_tone), 0, 0)
        detail_grid.addWidget(_make_detail_cell("シャープ", f"{run.sharpe:.2f}"), 0, 1)
        dd_value = run.max_drawdown_pct
        if dd_value == 0:
            dd_str = "0.00%"
            dd_tone = None
        else:
            dd_str = f"{'−' if dd_value >= 0 else ''}{abs(dd_value):.2f}%"
            dd_tone = "neg"
        detail_grid.addWidget(_make_detail_cell("DD", dd_str, tone=dd_tone), 1, 0)
        detail_grid.addWidget(_make_detail_cell("取引数", f"{run.trade_count:,}"), 1, 1)
        card.addBodyLayout(detail_grid)

        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(6)
        html_btn = QPushButton("HTML を開く")
        html_btn.setProperty("variant", "ghost")
        csv_btn = QPushButton("CSV")
        csv_btn.setProperty("variant", "ghost")
        button_row.addWidget(html_btn, 1)
        button_row.addWidget(csv_btn, 1)
        card.addBodyLayout(button_row)

        html_btn.clicked.connect(lambda _=False, r=run: _open_report_html(r))
        csv_btn.clicked.connect(lambda _=False, r=run: _open_report_csv(r))
        return card

    # ---- Table builder ---------------------------------------------------

    def _all_runs_frame(runs: list[RunView]) -> pd.DataFrame | None:
        if not runs:
            return None
        rows = []
        for run in runs:
            pnl = run.total_return_pct
            dd = run.max_drawdown_pct
            rows.append(
                {
                    "実行ID": run.run_id,
                    "戦略": run.strategy_name or "-",
                    "期間": _format_period(run.period_start, run.period_end),
                    "初期資産": f"{int(run.starting_cash):,}" if run.starting_cash else "-",
                    "総損益": f"{'+' if pnl >= 0 else ''}{pnl:.2f}%",
                    "シャープ": f"{run.sharpe:.2f}",
                    "DD": f"{'−' if dd != 0 else ''}{abs(dd):.2f}%" if dd else "0.00%",
                    "取引数": f"{run.trade_count:,}",
                    "作成日時": _format_created_at(run.finished_at),
                    "": "",
                }
            )
        return pd.DataFrame(rows)

    def _install_table_delegates() -> None:
        all_view.setItemDelegateForColumn(0, SymbolDelegate(all_view))
        for column in (3, 5, 7):
            all_view.setItemDelegateForColumn(column, MonoRightDelegate(all_view))
        all_view.setItemDelegateForColumn(4, PnLPctDelegate(all_view))
        all_view.setItemDelegateForColumn(6, PnLPctDelegate(all_view))
        all_view.setItemDelegateForColumn(
            9,
            OpenButtonDelegate(all_view, on_click=_handle_table_open),
        )

    def _handle_table_open(row_index: int) -> None:
        if 0 <= row_index < len(page._runs_cache):
            _open_report_html(page._runs_cache[row_index])

    # ---- Reload ----------------------------------------------------------

    def _clear_pinned() -> None:
        while pinned_row.count():
            item = pinned_row.takeAt(0)
            widget = item.widget() if item is not None else None
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

    def _reload() -> None:
        cfg = app_state.config
        try:
            raw_rows = list(app_state.list_runs() or [])
        except Exception:  # noqa: BLE001
            raw_rows = []
        raw_rows.sort(key=lambda row: row.get("finished_at", ""), reverse=True)
        runs = [_to_run_view(row, cfg) for row in raw_rows]
        for index, run in enumerate(runs):
            run.is_latest = index == 0
        page._runs_cache = runs

        _clear_pinned()
        top3 = runs[:3]
        for run in top3:
            pinned_row.addWidget(_make_pinned_card(run), 1)
        if len(top3) < 3:
            pinned_row.addStretch(3 - len(top3))
        if not top3:
            empty = QLabel("保存済み実行はまだありません。バックテストを実行するとここに表示されます。")
            empty.setProperty("role", "muted")
            empty.setWordWrap(True)
            pinned_row.insertWidget(0, empty, 3)

        frame = _all_runs_frame(runs)
        all_model.set_frame(frame)
        _install_table_delegates()
        count_label.setText(f"{len(runs):,} 件")

    # ---- Wiring ----------------------------------------------------------

    folder_btn.clicked.connect(_open_reports_folder)
    generate_btn.clicked.connect(_generate_latest_report)

    page.refresh = _reload
    _reload()
    return page
