from __future__ import annotations

import json

from fxautotrade_lab.security.keychain import KeychainCredentialStore


class _Result:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_keychain_store_save_load_delete(monkeypatch):
    stored: dict[str, str] = {}

    def fake_run(command, capture_output, text, check):  # noqa: ANN001
        action = command[1]
        service = command[command.index("-s") + 1]
        if action == "add-generic-password":
            stored[service] = command[command.index("-w") + 1]
            return _Result(0)
        if action == "find-generic-password":
            if service in stored:
                return _Result(0, stdout=stored[service])
            return _Result(44, stderr="not found")
        if action == "delete-generic-password":
            if service in stored:
                stored.pop(service, None)
                return _Result(0)
            return _Result(44, stderr="not found")
        raise AssertionError(f"unexpected security command: {command}")

    monkeypatch.setattr("fxautotrade_lab.security.keychain.platform.system", lambda: "Darwin")
    monkeypatch.setattr("fxautotrade_lab.security.keychain.shutil.which", lambda _: "/usr/bin/security")
    monkeypatch.setattr("fxautotrade_lab.security.keychain.subprocess.run", fake_run)

    store = KeychainCredentialStore()
    record = store.save_credentials("paper", "PK-1234", "PS-5678")
    assert record.configured is True
    assert record.key_hint == "***1234"
    loaded = store.load_credentials("paper")
    assert loaded.api_key == "PK-1234"
    assert loaded.api_secret == "PS-5678"
    assert json.loads(stored["com.autotrade.lab.alpaca.paper"])["profile"] == "paper"
    assert store.delete_credentials("paper") is True
    assert store.load_credentials("paper").configured is False
