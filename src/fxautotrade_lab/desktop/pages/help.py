"""Help page — static quick-start, glossary, troubleshooting, shortcuts."""

from __future__ import annotations

import sys

_START_HTML = """
<ol style="margin:0 0 0 18px;padding:0;line-height:1.9;">
  <li><b>監視通貨ペア</b> で運用ペアを 3〜8 追加</li>
  <li><b>データ同期</b> で 15m を 1 年以上取得</li>
  <li><b>バックテスト</b> で戦略を評価</li>
  <li><b>実時間シミュレーション</b> で GMO 価格を流しつつローカル約定</li>
</ol>
"""

_TROUBLE_HTML = """
<ul style="margin:0 0 0 18px;padding:0;line-height:1.9;">
  <li>ストリームが切れる → <b>設定 → GMO 接続確認</b> でレート残量を確認</li>
  <li>同期が遅い → <b>データ同期</b> で並列度を下げる</li>
  <li>モデルがない → <b>バックテスト → FX ML 学習</b></li>
  <li>完全停止したい → 右上の <b>キルスイッチ</b></li>
</ul>
"""

_TERMS = [
    ("paper", "実売買なし。GMO の価格のみ使用"),
    ("Walk-Forward", "時間窓を前進させつつ学習→検証"),
    ("Uplift", "ML フィルタ有無の成績差"),
    ("キルスイッチ", "全停止+手動解除のみ"),
]

_SHORTCUTS_MAC = [
    ("コマンドパレット", "⌘ K"),
    ("ページ更新", "⌘ R"),
    ("デモ実行", "⌘ ⇧ D"),
    ("キルスイッチ", "⌘ ⇧ ."),
    ("ログ表示", "⌘ L"),
    ("次のページ", "⌃ Tab"),
    ("前のページ", "⌃ ⇧ Tab"),
    ("検索", "⌘ F"),
]


def _shortcuts_for_platform() -> list[tuple[str, str]]:
    if sys.platform == "darwin":
        return list(_SHORTCUTS_MAC)

    def _to_ctrl(value: str) -> str:
        return value.replace("⌘", "Ctrl").replace("⌃", "Ctrl").replace("⇧", "Shift")

    return [(label, _to_ctrl(key)) for label, key in _SHORTCUTS_MAC]


_DEFAULT_HELP_URL = (
    "https://github.com/anthropics/fxautotrade_lab#readme"  # TODO: 本番 URL に差し替え
)
_DEFAULT_SUPPORT_EMAIL = "support@example.com"  # TODO: 本番メールに差し替え


def build_help_page(app_state=None):  # pragma: no cover - UI helper
    from PySide6.QtCore import Qt, QUrl
    from PySide6.QtGui import QDesktopServices
    from PySide6.QtWidgets import (
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QLabel,
        QPushButton,
        QScrollArea,
        QVBoxLayout,
        QWidget,
    )

    from fxautotrade_lab.desktop.widgets.card import Card
    from fxautotrade_lab.desktop.widgets.detail import Detail

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
    title = QLabel("ヘルプ")
    title.setProperty("role", "h1")
    subtitle = QLabel("最初にやること、よくある質問、トラブルシュートへのショートカット。")
    subtitle.setProperty("role", "muted")
    subtitle.setWordWrap(True)
    header_text.addWidget(title)
    header_text.addWidget(subtitle)
    header.addLayout(header_text, 1)
    docs_btn = QPushButton("ドキュメントを開く")
    docs_btn.setProperty("variant", "ghost")
    contact_btn = QPushButton("問い合わせ")
    contact_btn.setProperty("variant", "primary")
    header.addWidget(docs_btn)
    header.addWidget(contact_btn)
    layout.addLayout(header)

    # Resolve URLs from config when available
    def _resolve_urls() -> tuple[str, str]:
        help_url = _DEFAULT_HELP_URL
        support_email = _DEFAULT_SUPPORT_EMAIL
        if app_state is not None:
            cfg = getattr(app_state, "config", None)
            if cfg is not None:
                help_url = str(getattr(cfg, "help_url", None) or help_url)
                support_email = str(getattr(cfg, "support_email", None) or support_email)
        return help_url, support_email

    def _open_docs() -> None:
        help_url, _ = _resolve_urls()
        QDesktopServices.openUrl(QUrl(help_url))

    def _open_contact() -> None:
        _, email = _resolve_urls()
        QDesktopServices.openUrl(QUrl(f"mailto:{email}?subject=FXAutoTrade%20Lab"))

    docs_btn.clicked.connect(_open_docs)
    contact_btn.clicked.connect(_open_contact)

    # ---- grid-3 ----
    grid3 = QHBoxLayout()
    grid3.setSpacing(12)

    # ① はじめる
    start_body = QLabel()
    start_body.setTextFormat(Qt.RichText)
    start_body.setWordWrap(True)
    start_body.setOpenExternalLinks(False)
    start_body.setProperty("role", "help-body")
    start_body.setText(_START_HTML)
    start_card = Card(title="はじめる")
    start_card.addBodyWidget(start_body)
    grid3.addWidget(start_card, 1)

    # ② 用語
    terms_body = QWidget()
    terms_lay = QVBoxLayout(terms_body)
    terms_lay.setContentsMargins(0, 0, 0, 0)
    terms_lay.setSpacing(10)
    for label, desc in _TERMS:
        terms_lay.addWidget(Detail(label, desc, variant="text", variant_size="sm"))
    terms_card = Card(title="用語")
    terms_card.addBodyWidget(terms_body)
    grid3.addWidget(terms_card, 1)

    # ③ トラブル
    trouble_body = QLabel()
    trouble_body.setTextFormat(Qt.RichText)
    trouble_body.setWordWrap(True)
    trouble_body.setOpenExternalLinks(False)
    trouble_body.setProperty("role", "help-body")
    trouble_body.setText(_TROUBLE_HTML)
    trouble_card = Card(title="トラブル")
    trouble_card.addBodyWidget(trouble_body)
    grid3.addWidget(trouble_card, 1)

    layout.addLayout(grid3)

    # ---- Shortcuts card ----
    shortcuts = _shortcuts_for_platform()
    shortcut_grid = QGridLayout()
    shortcut_grid.setHorizontalSpacing(16)
    shortcut_grid.setVerticalSpacing(12)
    # Fixed-width cells + trailing spacer column so the shortcut group
    # clusters on the left instead of stretching across the whole card.
    for column in range(4):
        shortcut_grid.setColumnStretch(column, 0)
        shortcut_grid.setColumnMinimumWidth(column, 180)
    shortcut_grid.setColumnStretch(4, 1)
    for index, (label, key) in enumerate(shortcuts):
        shortcut_grid.addWidget(
            Detail(label, key, variant="mono", variant_size="sm"),
            index // 4,
            index % 4,
        )
    shortcut_wrap = QWidget()
    shortcut_wrap.setLayout(shortcut_grid)
    shortcut_card = Card(title="ショートカット")
    shortcut_card.addBodyWidget(shortcut_wrap)
    layout.addWidget(shortcut_card)

    layout.addStretch(1)

    page.refresh = lambda: None  # static page
    return page
