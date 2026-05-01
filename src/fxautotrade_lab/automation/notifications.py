"""Notification channels for automation events."""

from __future__ import annotations

import json
import os
import platform
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from urllib import request

from fxautotrade_lab.config.models import NotificationChannelConfig


def _escape_applescript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


@dataclass(slots=True)
class MultiChannelNotifier:
    enabled: bool = True
    config: NotificationChannelConfig = field(default_factory=NotificationChannelConfig)

    def notify(self, title: str, message: str, subtitle: str = "") -> dict[str, bool]:
        if not self.enabled:
            return {"desktop": False, "sound": False, "log": False, "webhook": False}
        results = {
            "desktop": self._notify_desktop(title=title, message=message, subtitle=subtitle),
            "sound": self._notify_sound(),
            "log": self._notify_log(title=title, message=message, subtitle=subtitle),
            "webhook": self._notify_webhook(title=title, message=message, subtitle=subtitle),
        }
        return results

    def _notify_desktop(self, title: str, message: str, subtitle: str = "") -> bool:
        if (
            "desktop" not in self.config.channels
            or platform.system() != "Darwin"
            or os.getenv("PYTEST_CURRENT_TEST")
        ):
            return False
        script = (
            'display notification "'
            + _escape_applescript(message)
            + '" with title "'
            + _escape_applescript(title)
            + '"'
        )
        if subtitle:
            script += ' subtitle "' + _escape_applescript(subtitle) + '"'
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=False
        )
        return result.returncode == 0

    def _notify_sound(self) -> bool:
        if (
            "sound" not in self.config.channels
            or platform.system() != "Darwin"
            or os.getenv("PYTEST_CURRENT_TEST")
        ):
            return False
        sound_path = Path("/System/Library/Sounds") / f"{self.config.sound_name}.aiff"
        if sound_path.exists():
            result = subprocess.run(
                ["afplay", str(sound_path)], capture_output=True, text=True, check=False
            )
            return result.returncode == 0
        result = subprocess.run(
            ["osascript", "-e", "beep 1"], capture_output=True, text=True, check=False
        )
        return result.returncode == 0

    def _notify_log(self, title: str, message: str, subtitle: str = "") -> bool:
        if "log" not in self.config.channels:
            return False
        self.config.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.log_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "title": title,
                        "subtitle": subtitle,
                        "message": message,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        return True

    def _notify_webhook(self, title: str, message: str, subtitle: str = "") -> bool:
        if "webhook" not in self.config.channels or not self.config.webhook_url.strip():
            return False
        payload = json.dumps(
            {
                "title": title,
                "subtitle": subtitle,
                "message": message,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        try:
            req = request.Request(
                self.config.webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=self.config.webhook_timeout_seconds):
                return True
        except Exception:
            return False
