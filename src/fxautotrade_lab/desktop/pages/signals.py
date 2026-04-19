"""Signals page."""

from __future__ import annotations


def build_signals_page(app_state):  # pragma: no cover - UI helper
    import pandas as pd

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QFrame,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QScrollArea,
        QSplitter,
        QTableView,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.models import load_dataframe_model_class
    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.segmented import SegmentedControl

    DataFrameTableModel = load_dataframe_model_class()

    page = QScrollArea()
    page.setWidgetResizable(True)
    page.setFrameShape(QFrame.NoFrame)
    content = QWidget()
    layout = QVBoxLayout(content)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)
    page.setWidget(content)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("シグナル分析")
    title.setProperty("role", "h1")
    subtitle = QLabel("バックテスト / 実時間シミュレーションから採取したシグナル")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    filter_row = QHBoxLayout()
    filter_row.setSpacing(10)
    symbol_filter = QLineEdit()
    symbol_filter.setPlaceholderText("通貨ペアで絞り込み")
    symbol_filter.setFixedWidth(220)
    action_segment = SegmentedControl(
        options=["全て", "買い", "売り", "様子見"],
        current=0,
        data=["all", "買い", "売り", "様子見"],
    )
    filter_row.addWidget(symbol_filter)
    filter_row.addWidget(action_segment)
    filter_row.addStretch(1)

    splitter = QSplitter(Qt.Vertical)
    table_card = Card(title="シグナル一覧", subtitle="直近 300 件")
    table_card.addBodyLayout(filter_row)
    table = QTableView()
    model = DataFrameTableModel()
    table.setModel(model)
    table.setAlternatingRowColors(False)
    table.setShowGrid(False)
    table.setSortingEnabled(False)
    table.verticalHeader().setVisible(False)
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.Interactive)
    header.setDefaultSectionSize(170)
    header.setMinimumSectionSize(120)
    table_card.addBodyWidget(table)
    splitter.addWidget(table_card)

    detail_card = Card(title="選択シグナル詳細")
    detail = QTextEdit()
    detail.setReadOnly(True)
    detail.setProperty("role", "mono")
    detail_card.addBodyWidget(detail)
    splitter.addWidget(detail_card)
    splitter.setStretchFactor(0, 3)
    splitter.setStretchFactor(1, 2)
    layout.addWidget(splitter, 1)

    column_labels = {
        "timestamp": "時刻",
        "symbol": "通貨ペア",
        "signal_action": "シグナル",
        "signal_score": "総合スコア",
        "sub_score_trend_regime": "トレンド評価",
        "sub_score_pullback_continuation": "押し目/継続評価",
        "sub_score_market_context": "市場文脈評価",
        "explanation_ja": "説明",
    }
    action_labels = {"buy": "買い", "sell": "売り", "hold": "様子見"}

    page._full_frame = pd.DataFrame()

    def table_frame(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        localized = frame.copy()
        if "timestamp" in localized.columns:
            stamps = pd.to_datetime(localized["timestamp"], errors="coerce")
            localized["timestamp"] = stamps.dt.strftime("%Y-%m-%d %H:%M").fillna(localized["timestamp"].astype(str))
        if "signal_action" in localized.columns:
            localized["signal_action"] = localized["signal_action"].map(
                lambda value: action_labels.get(str(value), str(value))
            )
        return localized.rename(columns=column_labels)

    def resize_columns() -> None:
        for index in range(model.columnCount()):
            table.resizeColumnToContents(index)
            current = table.columnWidth(index)
            table.setColumnWidth(index, max(current + 24, 150))
        explanation_index = model.columnCount() - 1
        if explanation_index >= 0:
            table.setColumnWidth(explanation_index, max(table.columnWidth(explanation_index), 520))

    def apply_filters() -> None:
        if page._full_frame is None or page._full_frame.empty:
            model.set_frame(None)
            detail.setPlainText("まだシグナル分析結果はありません。")
            return
        frame = page._full_frame.copy()
        if "signal_action" in frame.columns:
            frame = frame.copy()
            frame["signal_action"] = frame["signal_action"].map(
                lambda value: action_labels.get(str(value), str(value))
            )
        needle = symbol_filter.text().strip().upper()
        if needle and "symbol" in frame.columns:
            frame = frame[frame["symbol"].astype(str).str.upper().str.contains(needle, na=False)]
        selected_action = str(action_segment.currentData() or "all")
        if selected_action != "all" and "signal_action" in frame.columns:
            frame = frame[frame["signal_action"].astype(str) == selected_action]
        if "timestamp" in frame.columns:
            stamps = pd.to_datetime(frame["timestamp"], errors="coerce")
            frame["timestamp"] = stamps.dt.strftime("%Y-%m-%d %H:%M").fillna(frame["timestamp"].astype(str))
        frame = frame.rename(columns=column_labels)
        model.set_frame(frame.tail(300))
        resize_columns()
        detail.setPlainText("行を選択すると詳細理由を表示します。")

    def on_clicked(index) -> None:  # noqa: ANN001
        if app_state.last_result is None or app_state.last_result.signals.empty:
            detail.setPlainText("データがありません。")
            return
        row = app_state.last_result.signals.iloc[index.row()]
        detail.setPlainText(
            "\n".join(
                [
                    f"通貨ペア: {row.get('symbol', '')}",
                    f"時刻: {row.get('timestamp', '')}",
                    f"アクション: {action_labels.get(str(row.get('signal_action', '')), row.get('signal_action', ''))}",
                    f"スコア: {row.get('signal_score', 0):.4f}",
                    f"トレンド評価: {row.get('sub_score_trend_regime', '-')}",
                    f"押し目/継続評価: {row.get('sub_score_pullback_continuation', '-')}",
                    f"ブレイクアウト評価: {row.get('sub_score_breakout_compression', '-')}",
                    f"ローソク足評価: {row.get('sub_score_candle_price_action', '-')}",
                    f"上位足整合: {row.get('sub_score_multi_timeframe_alignment', '-')}",
                    f"市場文脈評価: {row.get('sub_score_market_context', '-')}",
                    f"説明: {row.get('explanation_ja', '')}",
                ]
            )
        )

    table.clicked.connect(on_clicked)
    symbol_filter.textChanged.connect(lambda _: apply_filters())
    action_segment.idClicked.connect(lambda _: apply_filters())

    def refresh() -> None:
        if app_state.last_result is None or app_state.last_result.signals.empty:
            page._full_frame = pd.DataFrame()
            model.set_frame(None)
            detail.setPlainText("まだシグナル分析結果はありません。")
            return
        columns = [
            "timestamp",
            "symbol",
            "signal_action",
            "signal_score",
            "sub_score_trend_regime",
            "sub_score_pullback_continuation",
            "sub_score_market_context",
            "explanation_ja",
        ]
        available = [column for column in columns if column in app_state.last_result.signals.columns]
        page._full_frame = (
            app_state.last_result.signals[available].tail(300).copy()
            if available
            else pd.DataFrame()
        )
        apply_filters()

    page.refresh = refresh
    return page
