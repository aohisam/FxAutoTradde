from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tests.conftest import write_config


def test_lab_application_exposes_scalping_cli_methods(tmp_path):
    from fxautotrade_lab.application import LabApplication

    config_path = write_config(tmp_path)
    app = LabApplication(config_path)

    for method_name in (
        "import_jforex_tick_csv",
        "run_scalping_backtest",
        "run_scalping_realtime_sim",
        "record_gmo_scalping_ticks",
        "import_jforex_bid_ask_csv",
    ):
        assert hasattr(app, method_name)
        assert callable(getattr(app, method_name))


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
        [
            sys.executable,
            "-m",
            "fxautotrade_lab.cli",
            "realtime-sim",
            "--config",
            str(config_path),
            "--max-cycles",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_verify_broker_smoke(tmp_path):
    config_path = write_config(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "fxautotrade_lab.cli",
            "verify-broker",
            "--config",
            str(config_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "ブローカー確認完了" in result.stdout


def test_cli_scalping_help_commands_smoke():
    commands = [
        [],
        ["import-tick-csv"],
        ["scalping-backtest"],
        ["scalping-realtime-sim"],
        ["record-gmo-ticks"],
        ["scalping-outcomes-summary"],
    ]
    for command in commands:
        result = subprocess.run(
            [sys.executable, "-m", "fxautotrade_lab.cli", *command, "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "Usage" in result.stdout or "usage" in result.stdout


def test_cli_scalping_argparse_dispatches_application_methods(monkeypatch, capsys, tmp_path):
    from fxautotrade_lab.cli import __main__ as cli_main

    calls: list[tuple[str, dict[str, object]]] = []
    config_path = tmp_path / "config.yaml"
    tick_path = tmp_path / "ticks.csv"

    class FakeLabApplication:
        def __init__(self, config: Path) -> None:
            self.config = config

        def import_jforex_tick_csv(self, **kwargs: object) -> dict[str, object]:
            calls.append(("import_jforex_tick_csv", {"config": self.config, **kwargs}))
            return {"ok": True}

        def run_scalping_backtest(self, **kwargs: object) -> dict[str, object]:
            calls.append(("run_scalping_backtest", {"config": self.config, **kwargs}))
            return {"ok": True}

        def run_scalping_realtime_sim(self, **kwargs: object) -> dict[str, object]:
            calls.append(("run_scalping_realtime_sim", {"config": self.config, **kwargs}))
            return {"ok": True}

        def record_gmo_scalping_ticks(self, **kwargs: object) -> dict[str, object]:
            calls.append(("record_gmo_scalping_ticks", {"config": self.config, **kwargs}))
            return {"ok": True}

        def load_scalping_outcome_summary(self) -> dict[str, object]:
            calls.append(("load_scalping_outcome_summary", {"config": self.config}))
            return {"ok": True}

    monkeypatch.setattr(cli_main, "LabApplication", FakeLabApplication)
    cases = [
        (
            [
                "fxautotrade",
                "import-tick-csv",
                "--config",
                str(config_path),
                "--file",
                str(tick_path),
                "--symbol",
                "USD_JPY",
            ],
            "import_jforex_tick_csv",
            {
                "config": config_path,
                "file_path": str(tick_path),
                "symbol": "USD_JPY",
            },
        ),
        (
            [
                "fxautotrade",
                "scalping-backtest",
                "--config",
                str(config_path),
                "--tick-file",
                str(tick_path),
                "--symbol",
                "EUR_JPY",
                "--start",
                "2026-02-02T09:00:00+09:00",
                "--end",
                "2026-02-02T10:00:00+09:00",
            ],
            "run_scalping_backtest",
            {
                "config": config_path,
                "tick_file_path": str(tick_path),
                "symbol": "EUR_JPY",
                "start": "2026-02-02T09:00:00+09:00",
                "end": "2026-02-02T10:00:00+09:00",
            },
        ),
        (
            [
                "fxautotrade",
                "scalping-realtime-sim",
                "--config",
                str(config_path),
                "--symbol",
                "GBP_JPY",
                "--max-ticks",
                "3",
                "--poll-seconds",
                "0",
            ],
            "run_scalping_realtime_sim",
            {
                "config": config_path,
                "symbol": "GBP_JPY",
                "max_ticks": 3,
                "poll_seconds": 0.0,
            },
        ),
        (
            [
                "fxautotrade",
                "record-gmo-ticks",
                "--config",
                str(config_path),
                "--symbol",
                "AUD_JPY",
                "--max-ticks",
                "2",
            ],
            "record_gmo_scalping_ticks",
            {
                "config": config_path,
                "symbol": "AUD_JPY",
                "max_ticks": 2,
                "output_path": None,
            },
        ),
        (
            [
                "fxautotrade",
                "scalping-outcomes-summary",
                "--config",
                str(config_path),
            ],
            "load_scalping_outcome_summary",
            {
                "config": config_path,
            },
        ),
    ]

    for argv, expected_method, expected_kwargs in cases:
        calls.clear()
        monkeypatch.setattr(sys, "argv", argv)

        cli_main._argparse_main()

        assert calls == [(expected_method, expected_kwargs)]
        assert "{'ok': True}" in capsys.readouterr().out
