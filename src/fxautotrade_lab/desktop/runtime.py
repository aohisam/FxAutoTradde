"""Desktop process lifecycle helpers."""

from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path


PROCESS_SIGNATURES = (
    "fxautotrade_lab.cli launch-desktop",
    "scripts/desktop_entry.py",
    "launch_desktop_macos.sh",
    "FXAutoTradeLab.app/Contents/MacOS",
    "FXAutoTrade Lab.app/Contents/MacOS",
)


def _default_state_path() -> Path:
    return Path.home() / "Library" / "Application Support" / "FXAutoTradeLab" / "runtime" / "desktop_app_state.json"


@dataclass(slots=True)
class DesktopProcessManager:
    state_path: Path = field(default_factory=_default_state_path)
    signatures: tuple[str, ...] = PROCESS_SIGNATURES
    _registered: bool = field(default=False, init=False)

    def prepare(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.cleanup_stale_processes(exclude_pids={os.getpid(), os.getppid()})
        self.state_path.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "started_at": time.time(),
                    "command": "FXAutoTrade Lab Desktop",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        if not self._registered:
            atexit.register(self.cleanup)
            for signum in (signal.SIGINT, signal.SIGTERM):
                signal.signal(signum, self._handle_signal)
            self._registered = True

    def cleanup(self) -> None:
        self._remove_state_file()
        self.cleanup_stale_processes(exclude_pids={os.getpid(), os.getppid()})

    def cleanup_stale_processes(self, exclude_pids: set[int] | None = None) -> None:
        excluded = exclude_pids or set()
        stale_pids = self._pids_from_state_file()
        for pid, command in self._list_processes():
            if pid in excluded:
                continue
            if pid in stale_pids or self._matches_signature(command):
                self._terminate_pid(pid)

    def _pids_from_state_file(self) -> set[int]:
        if not self.state_path.exists():
            return set()
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._remove_state_file()
            return set()
        pid = payload.get("pid")
        return {int(pid)} if isinstance(pid, int) else set()

    def _list_processes(self) -> list[tuple[int, str]]:
        try:
            result = subprocess.run(
                ["ps", "-Ao", "pid=,command="],
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, PermissionError):
            return []
        rows: list[tuple[int, str]] = []
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            pid_text, _, command = stripped.partition(" ")
            if not pid_text.isdigit():
                continue
            rows.append((int(pid_text), command.strip()))
        return rows

    def _matches_signature(self, command: str) -> bool:
        return any(signature in command for signature in self.signatures)

    def _terminate_pid(self, pid: int) -> None:
        self._request_quit(pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except PermissionError:
            return
        time.sleep(0.1)
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            return
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except PermissionError:
            return

    def _request_quit(self, pid: int) -> None:
        if sys.platform != "darwin":
            return
        subprocess.run(
            [
                "osascript",
                "-e",
                f'tell application "System Events" to quit (first process whose unix id is {pid})',
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    def _remove_state_file(self) -> None:
        if self.state_path.exists():
            try:
                self.state_path.unlink()
            except OSError:
                pass

    def _handle_signal(self, signum, _frame) -> None:  # noqa: ANN001
        self.cleanup()
        raise SystemExit(128 + int(signum))
