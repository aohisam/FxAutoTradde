"""Japanese labels for FX ML UI."""

from __future__ import annotations

ML_MODE_CHOICES: list[tuple[str, str]] = [
    ("rule_only", "ルールのみ"),
    ("load_pretrained", "学習済みモデルを使う"),
    ("train_from_scratch", "その場で再学習して使う"),
    ("walk_forward_train", "期間をずらしながら逐次学習する"),
]

ML_MODE_DESCRIPTIONS: dict[str, str] = {
    "rule_only": "ML を使わず、ルールベース戦略だけでバックテストします。",
    "load_pretrained": "保存済みモデルを読み込み、参加してよいシグナルだけに絞り込みます。",
    "train_from_scratch": "指定期間より前の履歴で学習してから、そのモデルをバックテストに使います。",
    "walk_forward_train": "学習と検証を時系列でずらしながら繰り返し、未来漏れを避けて評価します。",
}


def ml_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip()
    return dict(ML_MODE_CHOICES).get(normalized, normalized or "-")


def ml_mode_description(mode: str) -> str:
    normalized = str(mode or "").strip()
    return ML_MODE_DESCRIPTIONS.get(normalized, "")
