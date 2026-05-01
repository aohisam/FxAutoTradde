"""macOS Keychain helpers for sensitive local credentials."""

from __future__ import annotations

import shutil
import subprocess
import sys
from dataclasses import dataclass

KEYCHAIN_SERVICE = "jp.secon.fxautotradelab"
GMO_PRIVATE_KEY_ACCOUNT = "gmo_private_api_key"
GMO_PRIVATE_SECRET_ACCOUNT = "gmo_private_api_secret"


@dataclass(slots=True)
class GmoPrivateCredentialRecord:
    api_key: str = ""
    api_secret: str = ""
    source: str = "unset"
    keychain_available: bool = False
    configured: bool = False


def keychain_available() -> bool:
    return sys.platform == "darwin" and shutil.which("security") is not None


def resolve_private_gmo_credentials(
    *, env_api_key: str = "", env_api_secret: str = ""
) -> GmoPrivateCredentialRecord:
    keychain_ready = keychain_available()
    if keychain_ready:
        api_key = _find_generic_password(GMO_PRIVATE_KEY_ACCOUNT)
        api_secret = _find_generic_password(GMO_PRIVATE_SECRET_ACCOUNT)
        if api_key and api_secret:
            return GmoPrivateCredentialRecord(
                api_key=api_key,
                api_secret=api_secret,
                source="keychain",
                keychain_available=True,
                configured=True,
            )
    fallback_key = env_api_key.strip()
    fallback_secret = env_api_secret.strip()
    if fallback_key and fallback_secret:
        return GmoPrivateCredentialRecord(
            api_key=fallback_key,
            api_secret=fallback_secret,
            source="env",
            keychain_available=keychain_ready,
            configured=True,
        )
    return GmoPrivateCredentialRecord(
        api_key=fallback_key,
        api_secret=fallback_secret,
        source="unset",
        keychain_available=keychain_ready,
        configured=False,
    )


def save_private_gmo_credentials(*, api_key: str, api_secret: str) -> GmoPrivateCredentialRecord:
    normalized_key = api_key.strip()
    normalized_secret = api_secret.strip()
    if not normalized_key or not normalized_secret:
        raise ValueError("GMO private API キーとシークレットは両方入力してください。")
    _ensure_keychain_available()
    _add_generic_password(
        GMO_PRIVATE_KEY_ACCOUNT, normalized_key, "FXAutoTradeLab GMO Private API Key"
    )
    _add_generic_password(
        GMO_PRIVATE_SECRET_ACCOUNT, normalized_secret, "FXAutoTradeLab GMO Private API Secret"
    )
    return GmoPrivateCredentialRecord(
        api_key=normalized_key,
        api_secret=normalized_secret,
        source="keychain",
        keychain_available=True,
        configured=True,
    )


def delete_private_gmo_credentials() -> bool:
    if not keychain_available():
        return False
    removed_key = _delete_generic_password(GMO_PRIVATE_KEY_ACCOUNT)
    removed_secret = _delete_generic_password(GMO_PRIVATE_SECRET_ACCOUNT)
    return removed_key or removed_secret


def _ensure_keychain_available() -> None:
    if not keychain_available():
        raise RuntimeError("この環境では macOS キーチェーンを利用できません。")


def _run_security(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["security", *command],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def _find_generic_password(account: str) -> str:
    result = _run_security(["find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account, "-w"])
    if result.returncode == 0:
        return result.stdout.strip()
    return ""


def _add_generic_password(account: str, secret: str, label: str) -> None:
    result = _run_security(
        [
            "add-generic-password",
            "-U",
            "-s",
            KEYCHAIN_SERVICE,
            "-a",
            account,
            "-l",
            label,
            "-w",
            secret,
        ]
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "security コマンドに失敗しました。").strip()
        raise RuntimeError(f"macOS キーチェーンへの保存に失敗しました: {detail}")


def _delete_generic_password(account: str) -> bool:
    result = _run_security(["delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account])
    if result.returncode == 0:
        return True
    detail = f"{result.stdout}\n{result.stderr}".lower()
    if "could not be found" in detail or "指定された項目が見つかりません" in detail:
        return False
    raise RuntimeError(
        f"macOS キーチェーンからの削除に失敗しました: {(result.stderr or result.stdout).strip()}"
    )
