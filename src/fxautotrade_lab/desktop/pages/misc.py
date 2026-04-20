"""Additional desktop pages."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import yaml


def _optional_web_view():  # pragma: no cover - UI helper
    try:
        if os.environ.get("QT_QPA_PLATFORM") == "offscreen":
            raise ImportError("offscreen mode")
        if getattr(sys, "frozen", False):
            raise ImportError("qtwebengine disabled in packaged app")
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except ImportError:
        return None
    return QWebEngineView






def build_help_page():  # pragma: no cover - UI helper
    from PySide6.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QVBoxLayout, QWidget

    from fxautotrade_lab.desktop.widgets.card import Card

    page = QWidget()
    layout = QVBoxLayout(page)
    layout.setContentsMargins(20, 20, 20, 20)
    layout.setSpacing(16)

    header_row = QHBoxLayout()
    header_left = QVBoxLayout()
    header_left.setSpacing(2)
    title = QLabel("ヘルプ")
    title.setProperty("role", "h1")
    subtitle = QLabel("進め方・用語・トラブルシュート")
    subtitle.setProperty("role", "muted")
    header_left.addWidget(title)
    header_left.addWidget(subtitle)
    header_row.addLayout(header_left, 1)
    layout.addLayout(header_row)

    grid = QGridLayout()
    grid.setHorizontalSpacing(14)
    grid.setVerticalSpacing(14)
    for column in range(3):
        grid.setColumnStretch(column, 1)

    start_card = Card(title="はじめる", subtitle="初回の流れ")
    steps_label = QLabel(
        "1. 監視通貨ペアで 3-8 ペアを登録\n"
        "2. データ同期ページで JForex CSV を取り込む\n"
        "3. バックテストで期間別の成績を確認\n"
        "4. 実時間シミュレーションで前向き検証"
    )
    steps_label.setWordWrap(True)
    steps_label.setProperty("role", "muted")
    start_card.addBodyWidget(steps_label)
    grid.addWidget(start_card, 0, 0)

    terms_card = Card(title="用語", subtitle="よく使う言葉")
    terms_label = QLabel(
        "• バックテスト: 過去データで戦略を検証\n"
        "• Walk-Forward: 期間を動かしながら逐次検証\n"
        "• Uplift: ML 適用前後の期待差\n"
        "• ローカル約定: UI からは実売買しません"
    )
    terms_label.setWordWrap(True)
    terms_label.setProperty("role", "muted")
    terms_card.addBodyWidget(terms_label)
    grid.addWidget(terms_card, 0, 1)

    trouble_card = Card(title="トラブル", subtitle="よくある対処")
    trouble_label = QLabel(
        "• 接続失敗: 設定 → GMO 接続テストで状態を確認\n"
        "• チャートが空: バックテストを 1 回実行するか、\n"
        "  実時間シミュレーションで通貨ペアを選び更新\n"
        "• 取込失敗: Bid / Ask の 2 ファイルを同時選択"
    )
    trouble_label.setWordWrap(True)
    trouble_label.setProperty("role", "muted")
    trouble_card.addBodyWidget(trouble_label)
    grid.addWidget(trouble_card, 0, 2)

    layout.addLayout(grid)

    shortcut_card = Card(title="ショートカット")
    shortcut_grid = QGridLayout()
    shortcut_grid.setHorizontalSpacing(18)
    shortcut_grid.setVerticalSpacing(8)
    shortcuts = [
        ("⌘R", "現在のページを再読込"),
        ("⌘⇧D", "デモ実行"),
        ("⌘L", "ログの表示切替"),
        ("⌃Tab", "次のページへ"),
        ("⌃⇧Tab", "前のページへ"),
        ("⌘F", "ページ内の検索"),
    ]
    for index, (key, description) in enumerate(shortcuts):
        key_label = QLabel(key)
        key_label.setProperty("role", "mono")
        desc_label = QLabel(description)
        desc_label.setProperty("role", "muted")
        shortcut_grid.addWidget(key_label, index // 3, (index % 3) * 2)
        shortcut_grid.addWidget(desc_label, index // 3, (index % 3) * 2 + 1)
    shortcut_card.addBodyLayout(shortcut_grid)
    layout.addWidget(shortcut_card)

    disclaimer = QLabel("本アプリは投資助言ではありません。")
    disclaimer.setProperty("role", "muted2")
    disclaimer.setWordWrap(True)
    layout.addWidget(disclaimer)

    layout.addStretch(1)
    return page
