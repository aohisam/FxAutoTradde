"""Signals page."""

from __future__ import annotations


def build_signals_page(app_state):  # pragma: no cover - UI helper
    import pandas as pd

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QLabel, QHeaderView, QSplitter, QTableView, QTextEdit, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.models import load_dataframe_model_class

    DataFrameTableModel = load_dataframe_model_class()

    page = QWidget()
    layout = QVBoxLayout(page)
    title = QLabel("シグナル分析")
    title.setStyleSheet("font-size: 22px; font-weight: 700;")
    layout.addWidget(title)
    splitter = QSplitter(Qt.Vertical)
    table = QTableView()
    detail = QTextEdit()
    detail.setReadOnly(True)
    model = DataFrameTableModel()
    table.setModel(model)
    table.setAlternatingRowColors(True)
    table.setSortingEnabled(False)
    table.verticalHeader().setVisible(False)
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.Interactive)
    header.setDefaultSectionSize(170)
    header.setMinimumSectionSize(120)
    splitter.addWidget(table)
    splitter.addWidget(detail)
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

    def table_frame(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()
        localized = frame.copy()
        if "timestamp" in localized.columns:
            stamps = pd.to_datetime(localized["timestamp"], errors="coerce")
            localized["timestamp"] = stamps.dt.strftime("%Y-%m-%d %H:%M").fillna(localized["timestamp"].astype(str))
        if "signal_action" in localized.columns:
            localized["signal_action"] = localized["signal_action"].map(lambda value: action_labels.get(str(value), str(value)))
        return localized.rename(columns=column_labels)

    def resize_columns() -> None:
        for index in range(model.columnCount()):
            table.resizeColumnToContents(index)
            current = table.columnWidth(index)
            table.setColumnWidth(index, max(current + 24, 150))
        explanation_index = model.columnCount() - 1
        if explanation_index >= 0:
            table.setColumnWidth(explanation_index, max(table.columnWidth(explanation_index), 520))

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

    def refresh() -> None:
        if app_state.last_result is None or app_state.last_result.signals.empty:
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
        model.set_frame(table_frame(app_state.last_result.signals[available].tail(300)))
        resize_columns()
        detail.setPlainText("行を選択すると詳細理由を表示します。")

    page.refresh = refresh
    return page
