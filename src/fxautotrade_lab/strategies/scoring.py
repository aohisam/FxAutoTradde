"""Multi-timeframe explainable scoring strategy."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.enums import SignalAction
from fxautotrade_lab.strategies.base import BaseStrategy
from fxautotrade_lab.strategies.explain import build_explanation


class MultiTimeframePatternScoringStrategy(BaseStrategy):
    """Default strategy engine for v1 automation."""

    name = "multi_timeframe_pattern_scoring"

    def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        weights = self.config.strategy.scoring.weights
        working = frame.copy()

        trend_regime = (
            (working["daily_ema_50"] > working["daily_ema_200"]).astype(float) * 0.35
            + (working["daily_slope_20"] > 0).astype(float) * 0.2
            + working["daily_hhhl_score"].clip(0.0, 1.0) * 0.2
            + (working["close"] > working["daily_ema_200"]).astype(float) * 0.15
            + (working["benchmark_trend"] > 0).astype(float) * 0.1
        )
        pullback_continuation = (
            working["pullback_depth_atr"]
            .clip(lower=-0.5, upper=2.0)
            .map(lambda x: max(0.0, min(1.0, 1 - abs(x - 0.7) / 1.5)))
            * 0.35
            + (working["entry_rsi_14"] > 48).astype(float) * 0.2
            + working["continuation_ready"].astype(float) * 0.25
            + (working["volume_zscore"] > -0.1).astype(float) * 0.2
        )
        breakout_compression = (
            working["entry_compression"].clip(0.0, 1.0) * 0.35
            + working["breakout_20"].astype(float) * 0.35
            + (working["volume_zscore"] > 0.2).astype(float) * 0.15
            + (working["gap_exhaustion"] == 0).astype(float) * 0.15
        )
        candle_price_action = (
            working["bullish_engulfing"] * 0.3
            + working["hammer"] * 0.25
            + working["inside_bar_breakout"] * 0.25
            + (1 - working["doji"]).clip(0.0, 1.0) * 0.2
        )
        multi_timeframe_alignment = (
            (working["weekly_slope_20"] > 0).astype(float) * 0.3
            + (working["monthly_slope_20"] > 0).astype(float) * 0.25
            + (working["close"] > working["entry_ema_20"]).astype(float) * 0.2
            + (working["daily_return_20"] > 0).astype(float) * 0.15
            + (working["weekly_return_20"] > 0).astype(float) * 0.1
        )
        market_context = (
            (working["relative_strength"] > 0).astype(float) * 0.3
            + (working["sector_relative_strength"] > -0.01).astype(float) * 0.15
            + (working["benchmark_volatility"] < 0.04).astype(float) * 0.2
            + working["liquidity_score"].clip(0.0, 1.0) * 0.2
            + (working["gap_risk"] < 0.025).astype(float) * 0.15
        )

        final_score = (
            trend_regime * weights.trend_regime
            + pullback_continuation * weights.pullback_continuation
            + breakout_compression * weights.breakout_compression
            + candle_price_action * weights.candle_price_action
            + multi_timeframe_alignment * weights.multi_timeframe_alignment
            + market_context * weights.market_context
        )
        final_score = final_score.clip(0.0, 1.0)
        accepted = (
            (final_score >= self.config.strategy.scoring.entry_score_threshold)
            & (working["gap_exhaustion"] == 0)
            & (working["doji"] < 1)
            & (working["daily_ema_50"] > working["daily_ema_200"])
        )
        exit_signal = (
            (final_score <= self.config.strategy.scoring.exit_score_threshold)
            | (working["gap_exhaustion"] > 0)
            | ((working["entry_rsi_14"] > 72) & (working["volume_zscore"] < 0))
            | ((working["close"] < working["entry_ema_50"]) & (working["daily_slope_20"] < 0))
        )
        working["entry_signal"] = accepted.astype(bool)
        working["exit_signal"] = exit_signal.astype(bool)
        working["signal_score"] = final_score
        working["signal_action"] = pd.Series(SignalAction.HOLD.value, index=working.index)
        working.loc[working["entry_signal"], "signal_action"] = SignalAction.BUY.value
        working.loc[working["exit_signal"], "signal_action"] = SignalAction.SELL.value
        working["sub_score_trend_regime"] = trend_regime
        working["sub_score_pullback_continuation"] = pullback_continuation
        working["sub_score_breakout_compression"] = breakout_compression
        working["sub_score_candle_price_action"] = candle_price_action
        working["sub_score_multi_timeframe_alignment"] = multi_timeframe_alignment
        working["sub_score_market_context"] = market_context
        working["reasons_ja"] = working.apply(self._build_reason_list, axis=1)
        working["explanation_ja"] = working.apply(
            lambda row: build_explanation(
                row["reasons_ja"],
                bool(row["entry_signal"]),
                "買い" if row["signal_action"] == SignalAction.BUY.value else "保持/手仕舞い",
            ),
            axis=1,
        )
        return working

    def _build_reason_list(self, row: pd.Series) -> list[str]:
        reasons: list[str] = []
        if row["daily_ema_50"] > row["daily_ema_200"]:
            reasons.append("上位足トレンドが上向き")
        if row["pullback_depth_atr"] > 0:
            reasons.append("押し目からの回復を検出")
        if row["breakout_20"] > 0:
            reasons.append("保ち合い上抜けを検出")
        if row["volume_zscore"] > 0:
            reasons.append("出来高確認あり")
        if row["relative_strength"] > 0:
            reasons.append("ベンチマーク比で相対強度が良好")
        if row["gap_exhaustion"] > 0:
            reasons.append("過熱シグナルのため新規買い見送り")
        if row["doji"] > 0:
            reasons.append("迷い足のため慎重判断")
        return reasons
