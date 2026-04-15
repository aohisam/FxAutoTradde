from __future__ import annotations

import os
from pathlib import Path

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.config.loader import load_app_config

from tests.conftest import write_config


def test_required_docs_and_configs_exist():
    for path in [
        "README.md",
        "AGENTS.md",
        "docs/architecture.md",
        "docs/strategy_engine.md",
        "docs/safety.md",
        "docs/free_plan_notes.md",
        "docs/paper_vs_live.md",
        "docs/mac_packaging.md",
        "docs/future_live_trading.md",
        "configs/demo_local.yaml",
        "configs/paper_alpaca_free.yaml",
        "configs/paper_alpaca_plus.yaml",
        "configs/live_alpaca_disabled.yaml",
        "configs/backtest_baseline.yaml",
        "configs/backtest_multitimeframe_scoring.yaml",
        "configs/mac_desktop_default.yaml",
    ]:
        assert Path(path).exists(), path


def test_app_config_contains_required_execution_modes(tmp_path):
    config = load_app_config(write_config(tmp_path))
    assert config.broker.mode.value in {"local_sim", "alpaca_paper", "alpaca_live"}
    assert config.ui.default_page == "概要"


def test_readme_mentions_critical_japanese_sections():
    text = Path("README.md").read_text(encoding="utf-8")
    for phrase in [
        "無料でできる範囲",
        "無料プランの制限",
        "ペーパー取引とライブ取引の違い",
        "本番移行時に必要な変更",
        "ライブ取引は既定で無効化されています",
    ]:
        assert phrase in text


def test_main_window_contains_required_pages(tmp_path):
    text = Path("src/fxautotrade_lab/desktop/main_window.py").read_text(encoding="utf-8")
    required_pages = {
        "概要",
        "監視銘柄",
        "データ同期",
        "バックテスト",
        "シグナル分析",
        "フォワード自動売買",
        "チャート",
        "取引履歴",
        "レポート",
        "設定",
        "ヘルプ",
    }
    for page in required_pages:
        assert page in text


def test_application_lists_persisted_runs(tmp_path):
    app = LabApplication(write_config(tmp_path))
    app.run_backtest()
    rows = app.list_runs()
    assert rows
