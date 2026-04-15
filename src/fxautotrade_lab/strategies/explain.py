"""Japanese explanation helpers."""

from __future__ import annotations


def build_explanation(reasons: list[str], accepted: bool, action_ja: str) -> str:
    if not reasons:
        reasons = ["明確な優位性が不足しています"]
    prefix = "採用" if accepted else "見送り"
    return f"{action_ja}判断: {prefix}。理由: {' / '.join(reasons)}"
