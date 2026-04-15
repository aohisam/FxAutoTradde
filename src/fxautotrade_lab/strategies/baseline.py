"""Baseline trend pullback strategy."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.enums import SignalAction
from fxautotrade_lab.strategies.base import BaseStrategy
from fxautotrade_lab.strategies.explain import build_explanation


class BaselineTrendPullbackStrategy(BaseStrategy):
    """Reference long-only pullback strategy."""

    name = "baseline_trend_pullback"

    def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config.strategy.baseline
        working = frame.copy()
        bullish_regime = (
            (working["daily_ema_50"] > working["daily_ema_200"])
            & (working["close"] > working["daily_ema_200"])
            & (working["daily_slope_20"] > cfg.min_daily_slope)
        )
        pullback_zone = (working["close"] <= working["entry_ema_20"] * 1.01) & (
            working["pullback_depth_atr"].between(-0.5, 1.6)
        )
        rsi_recovery = (working["entry_rsi_14"] > cfg.rsi_recovery_level) & (
            working["entry_rsi_14"].shift(1) <= cfg.rsi_recovery_level
        )
        volume_confirm = (working["volume_zscore"] > -0.2) | (not cfg.require_volume_confirmation)
        breakout_resume = working["continuation_ready"] > 0
        entry = bullish_regime & pullback_zone & rsi_recovery & volume_confirm & breakout_resume
        working["entry_signal"] = entry.astype(bool)
        working["exit_signal"] = (
            (working["entry_rsi_14"] < 42)
            | (working["close"] < working["entry_ema_50"])
            | (working["gap_exhaustion"] > 0)
        ).astype(bool)
        working["signal_score"] = (
            bullish_regime.astype(float) * 0.4
            + pullback_zone.astype(float) * 0.25
            + rsi_recovery.astype(float) * 0.2
            + breakout_resume.astype(float) * 0.15
        ).clip(0.0, 1.0)
        working["signal_action"] = pd.Series(SignalAction.HOLD.value, index=working.index)
        working.loc[working["entry_signal"], "signal_action"] = SignalAction.BUY.value
        working.loc[working["exit_signal"], "signal_action"] = SignalAction.SELL.value
        working["reasons_ja"] = working.apply(self._build_reason_list, axis=1)
        working["explanation_ja"] = working.apply(
            lambda row: build_explanation(
                row["reasons_ja"],
                bool(row["entry_signal"]),
                "買い" if row["signal_action"] == SignalAction.BUY.value else "保持/手仕舞い",
            ),
            axis=1,
        )
        working["sub_score_trend"] = bullish_regime.astype(float)
        working["sub_score_pullback"] = pullback_zone.astype(float)
        working["sub_score_momentum"] = rsi_recovery.astype(float)
        return working

    def _build_reason_list(self, row: pd.Series) -> list[str]:
        reasons: list[str] = []
        if row["daily_ema_50"] > row["daily_ema_200"]:
            reasons.append("上位足トレンドが上向き")
        if row["pullback_depth_atr"] > 0:
            reasons.append("押し目ゾーンへの接近")
        if row["entry_rsi_14"] > self.config.strategy.baseline.rsi_recovery_level:
            reasons.append("押し目からの回復を検出")
        if row["volume_zscore"] > -0.2:
            reasons.append("出来高確認あり")
        if row["gap_exhaustion"] > 0:
            reasons.append("ギャップ過熱のため注意")
        return reasons
