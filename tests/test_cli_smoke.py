from __future__ import annotations

import subprocess
import sys

from tests.conftest import write_config


def test_cli_backtest_smoke(tmp_path):
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "fxautotrade_lab.cli", "backtest", "--config", str(config_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "reports" in result.stdout


def test_cli_demo_smoke(tmp_path):
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "fxautotrade_lab.cli", "demo-run", "--config", str(config_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_realtime_sim_smoke(tmp_path):
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "fxautotrade_lab.cli", "realtime-sim", "--config", str(config_path), "--max-cycles", "1"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_verify_broker_smoke(tmp_path):
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "fxautotrade_lab.cli", "verify-broker", "--config", str(config_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "ブローカー確認完了" in result.stdout
