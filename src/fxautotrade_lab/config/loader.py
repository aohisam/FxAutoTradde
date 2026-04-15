"""Config loading and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from fxautotrade_lab.config.models import AppConfig, EnvironmentConfig


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_yaml_config(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_app_config(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> AppConfig:
    raw = load_yaml_config(path)
    if overrides:
        raw = _deep_merge(raw, overrides)
    config = AppConfig.model_validate(raw)
    return config


def load_environment() -> EnvironmentConfig:
    return EnvironmentConfig()


def save_app_config(config: AppConfig, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            config.model_dump(mode="json"),
            handle,
            sort_keys=False,
            allow_unicode=True,
        )
