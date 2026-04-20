"""Japanese labels for backtest / ML UI."""

from __future__ import annotations

ML_MODE_CHOICES: list[tuple[str, str]] = [
    ("rule_only", "ルールのみ"),
    ("load_pretrained", "学習済みモデルを使う"),
    ("train_from_scratch", "その場で再学習して使う"),
    ("walk_forward_train", "期間をずらしながら逐次学習する"),
]

RESEARCH_MODE_CHOICES: list[tuple[str, str]] = [
    ("quick", "クイック"),
    ("standard", "標準"),
    ("exhaustive", "網羅"),
]

RESEARCH_MODE_DESCRIPTIONS: dict[str, str] = {
    "quick": "最短で傾向を見る軽量モードです。頑健性チェックは一部だけ行い、パラメータ感度分析は省略します。",
    "standard": "通常の検証モードです。頑健性チェックを実行し、主要パラメータの感度分析は代表ケースだけ試します。",
    "exhaustive": "時間はかかりますが、頑健性チェックと感度分析をできるだけ広く回します。",
}

STRATEGY_CHOICES: list[tuple[str, str]] = [
    ("fx_breakout_pullback", "FX ブレイクアウト押し目"),
    ("baseline_trend_pullback", "ベースライン順張り押し目"),
    ("multi_timeframe_pattern_scoring", "マルチタイムフレーム総合スコア"),
]

STRATEGY_DESCRIPTIONS: dict[str, str] = {
    "fx_breakout_pullback": "USD/JPY 向けの主力戦略です。上位足トレンドを確認し、15分足ブレイク後の浅い押しを待って参加します。",
    "baseline_trend_pullback": "比較用の単純な順張り押し目戦略です。日足 EMA と RSI 回復を中心に判定します。",
    "multi_timeframe_pattern_scoring": "複数時間足の特徴量を点数化して判断する戦略です。従来の総合スコア型ロジックに近い比較用です。",
}

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


def research_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip()
    return dict(RESEARCH_MODE_CHOICES).get(normalized, normalized or "-")


def research_mode_description(mode: str) -> str:
    normalized = str(mode or "").strip()
    return RESEARCH_MODE_DESCRIPTIONS.get(normalized, "")


def strategy_label(name: str) -> str:
    normalized = str(name or "").strip()
    return dict(STRATEGY_CHOICES).get(normalized, normalized or "-")


def strategy_description(name: str) -> str:
    normalized = str(name or "").strip()
    return STRATEGY_DESCRIPTIONS.get(normalized, "")
