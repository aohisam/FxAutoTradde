from __future__ import annotations

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
        "configs/realtime_gmo_public.yaml",
        "configs/backtest_baseline.yaml",
        "configs/backtest_fx_breakout.yaml",
        "configs/backtest_multitimeframe_scoring.yaml",
        "configs/mac_desktop_default.yaml",
    ]:
        assert Path(path).exists(), path


def test_app_config_contains_required_execution_modes(tmp_path):
    config = load_app_config(write_config(tmp_path))
    assert config.broker.mode.value in {"local_sim", "gmo_sim"}
    assert config.ui.default_page == "概要"


def test_readme_mentions_critical_japanese_sections():
    text = Path("README.md").read_text(encoding="utf-8")
    for phrase in [
        "GMO public API の制約",
        "実時間シミュレーションと実売買の違い",
        "将来の実売買移行",
        "実売買は既定で無効化されています",
    ]:
        assert phrase in text


def test_main_window_contains_required_pages(tmp_path):
    _ = tmp_path
    text = Path("src/fxautotrade_lab/desktop/main_window.py").read_text(encoding="utf-8")
    required_pages = {
        "概要",
        "監視通貨ペア",
        "データ同期",
        "バックテスト",
        "シグナル分析",
        "実時間シミュレーション",
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
