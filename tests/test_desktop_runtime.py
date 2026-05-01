from __future__ import annotations

import json
import sys
from pathlib import Path

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.desktop.app import _desktop_storage_overrides, _resolve_config_path
from fxautotrade_lab.desktop.assets import resolve_app_icon_path, should_apply_runtime_window_icon
from fxautotrade_lab.desktop.runtime import DesktopProcessManager


def test_desktop_process_manager_removes_state_file(tmp_path):
    state_path = tmp_path / "desktop_state.json"
    manager = DesktopProcessManager(state_path=state_path)
    state_path.write_text(json.dumps({"pid": 999999}), encoding="utf-8")
    manager.cleanup()
    assert not state_path.exists()


def test_desktop_process_manager_matches_only_desktop_commands(tmp_path):
    manager = DesktopProcessManager(state_path=tmp_path / "desktop_state.json")
    assert manager._matches_signature(
        "/usr/bin/python -m fxautotrade_lab.cli launch-desktop --config x"
    )
    assert manager._matches_signature(
        "/Applications/FXAutoTradeLab.app/Contents/MacOS/FXAutoTradeLab"
    )
    assert not manager._matches_signature("/usr/bin/python my_other_script.py")


def test_desktop_storage_overrides_for_frozen(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    overrides = _desktop_storage_overrides()
    assert overrides is not None
    assert overrides["persistence"]["sqlite_path"].endswith(
        "Application Support/FXAutoTradeLab/runtime/trading_lab.sqlite"
    )
    assert overrides["data"]["cache_dir"].endswith("Application Support/FXAutoTradeLab/data_cache")
    assert overrides["research"]["output_dir"].endswith(
        "Application Support/FXAutoTradeLab/research_runs"
    )
    assert overrides["research"]["cache_dir"].endswith(
        "Application Support/FXAutoTradeLab/research_cache"
    )
    assert overrides["strategy"]["fx_breakout_pullback"]["ml_filter"]["model_dir"].endswith(
        "Application Support/FXAutoTradeLab/models/fx_ml"
    )


def test_resolve_config_path_from_bundle_resources(monkeypatch, tmp_path):
    config_path = (
        tmp_path
        / "FXAutoTradeLab.app"
        / "Contents"
        / "Resources"
        / "configs"
        / "mac_desktop_default.yaml"
    )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("app_name: FXAutoTrade Lab\n", encoding="utf-8")
    executable = tmp_path / "FXAutoTradeLab.app" / "Contents" / "MacOS" / "FXAutoTradeLab"
    executable.parent.mkdir(parents=True, exist_ok=True)
    executable.write_text("", encoding="utf-8")
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    monkeypatch.chdir(empty_dir)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(executable), raising=False)
    resolved = _resolve_config_path(None)
    assert resolved == config_path


def test_desktop_process_manager_requests_system_events_quit_on_macos(monkeypatch, tmp_path):
    manager = DesktopProcessManager(state_path=tmp_path / "desktop_state.json")
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):  # noqa: ANN001
        calls.append(command)

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr("fxautotrade_lab.desktop.runtime.sys.platform", "darwin")
    monkeypatch.setattr("fxautotrade_lab.desktop.runtime.subprocess.run", fake_run)
    manager._request_quit(12345)
    assert calls
    assert calls[0][0] == "osascript"
    assert "12345" in calls[0][-1]


def test_lab_application_copies_bundle_config_to_app_support(monkeypatch, tmp_path):
    source_config = (
        tmp_path
        / "FXAutoTradeLab.app"
        / "Contents"
        / "Resources"
        / "configs"
        / "mac_desktop_default.yaml"
    )
    source_config.parent.mkdir(parents=True, exist_ok=True)
    source_config.write_text(
        "app_name: FXAutoTrade Lab\nwatchlist:\n  symbols: ['USD_JPY']\n", encoding="utf-8"
    )
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "Home")
    app = LabApplication(source_config)
    assert "Application Support/FXAutoTradeLab/configs" in str(app.config_path)
    assert app.config_path is not None and app.config_path.exists()


def test_resolve_app_icon_path_prefers_repo_icon():
    icon_path = resolve_app_icon_path()
    assert icon_path is not None
    assert icon_path.name in {"icon.png", "app_icon.icns", "app_icon.svg"}


def test_should_apply_runtime_window_icon_depends_on_platform(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin", raising=False)
    assert should_apply_runtime_window_icon() is False
    monkeypatch.setattr(sys, "platform", "linux", raising=False)
    assert should_apply_runtime_window_icon() is True


def test_spec_uses_relative_paths():
    spec_text = Path("FXAutoTradeLab.spec").read_text(encoding="utf-8")
    assert "/Users/" not in spec_text
    assert "ROOT_DIR = Path.cwd().resolve()" in spec_text


def test_chart_page_defers_runtime_load_until_visible():
    source = Path("src/fxautotrade_lab/desktop/pages/chart.py").read_text(encoding="utf-8")
    assert "not page.isVisible()" in source
    tail = source.split("page.refresh = refresh_chart", 1)[1]
    assert "refresh_chart()" not in tail


def test_data_sync_page_uses_launch_date_window_instead_of_saved_config():
    source = Path("src/fxautotrade_lab/desktop/pages/data_sync.py").read_text(encoding="utf-8")
    assert 'start_date.setDate(default_popup_qdate("start"))' in source
    assert 'end_date.setDate(default_popup_qdate("end"))' in source
    assert "configured_start" not in source
    assert "configured_end" not in source


def test_backtest_page_uses_launch_date_window_instead_of_saved_config():
    source = Path("src/fxautotrade_lab/desktop/pages/backtest.py").read_text(encoding="utf-8")
    assert 'start_date.setDate(default_popup_qdate("start"))' in source
    assert 'end_date.setDate(default_popup_qdate("end"))' in source
    assert "app_state.config.backtest.start_date or app_state.config.data.start_date" not in source
    assert "app_state.config.backtest.end_date or app_state.config.data.end_date" not in source
