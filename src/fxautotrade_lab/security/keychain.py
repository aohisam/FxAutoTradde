"""macOS Keychain-backed credential storage."""

from __future__ import annotations

import json
import platform
import shutil
import subprocess
from dataclasses import dataclass


SERVICE_PREFIX = "com.autotrade.lab.alpaca"
ACCOUNT_NAME = "default"


@dataclass(slots=True)
class StoredCredentialSet:
    profile: str
    api_key: str = ""
    api_secret: str = ""
    source: str = "none"

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def key_hint(self) -> str:
        if not self.api_key:
            return "未設定"
        suffix = self.api_key[-4:] if len(self.api_key) >= 4 else self.api_key
        return f"***{suffix}"


class KeychainCredentialStore:
    """Store paper/live Alpaca credentials in the user's macOS Keychain."""

    def __init__(self, service_prefix: str = SERVICE_PREFIX) -> None:
        self.service_prefix = service_prefix

    def available(self) -> bool:
        return platform.system() == "Darwin" and shutil.which("security") is not None

    def load_credentials(self, profile: str) -> StoredCredentialSet:
        normalized = self._normalize_profile(profile)
        if not self.available():
            return StoredCredentialSet(profile=normalized, source="unsupported")
        payload = self._run_security(
            ["find-generic-password", "-w", "-s", self._service_name(normalized), "-a", ACCOUNT_NAME]
        )
        if payload is None:
            return StoredCredentialSet(profile=normalized, source="none")
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return StoredCredentialSet(profile=normalized, source="invalid")
        return StoredCredentialSet(
            profile=normalized,
            api_key=str(decoded.get("api_key", "")),
            api_secret=str(decoded.get("api_secret", "")),
            source="keychain",
        )

    def save_credentials(self, profile: str, api_key: str, api_secret: str) -> StoredCredentialSet:
        normalized = self._normalize_profile(profile)
        if not self.available():
            raise RuntimeError("macOS キーチェーンにアクセスできません。")
        payload = json.dumps(
            {
                "profile": normalized,
                "api_key": api_key.strip(),
                "api_secret": api_secret.strip(),
            },
            ensure_ascii=False,
        )
        result = subprocess.run(
            [
                "security",
                "add-generic-password",
                "-U",
                "-s",
                self._service_name(normalized),
                "-a",
                ACCOUNT_NAME,
                "-w",
                payload,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "キーチェーン保存に失敗しました。")
        return self.load_credentials(normalized)

    def delete_credentials(self, profile: str) -> bool:
        normalized = self._normalize_profile(profile)
        if not self.available():
            return False
        result = subprocess.run(
            [
                "security",
                "delete-generic-password",
                "-s",
                self._service_name(normalized),
                "-a",
                ACCOUNT_NAME,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0

    def status_map(self) -> dict[str, StoredCredentialSet]:
        return {
            "paper": self.load_credentials("paper"),
            "live": self.load_credentials("live"),
        }

    def _run_security(self, command: list[str]) -> str | None:
        result = subprocess.run(
            ["security", *command],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        return result.stdout.strip()

    def _service_name(self, profile: str) -> str:
        return f"{self.service_prefix}.{profile}"

    @staticmethod
    def _normalize_profile(profile: str) -> str:
        normalized = profile.lower().strip()
        if normalized not in {"paper", "live"}:
            raise ValueError(f"Unsupported Alpaca credential profile: {profile}")
        return normalized

