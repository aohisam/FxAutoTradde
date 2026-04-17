"""Rule-only FX breakout + pullback strategy."""

from __future__ import annotations

import pandas as pd

from fxautotrade_lab.core.enums import SignalAction
from fxautotrade_lab.strategies.base import BaseStrategy


class FxBreakoutPullbackStrategy(BaseStrategy):
    """FX breakout strategy with explicit pullback state transitions and side-aware signals."""

    name = "fx_breakout_pullback"

    @staticmethod
    def _as_bool(value: object, default: bool = False) -> bool:
        if value is None or value is pd.NA:
            return default
        if isinstance(value, float) and pd.isna(value):
            return default
        return bool(value)

    @staticmethod
    def _as_float(value: object, default: float = 0.0) -> float:
        try:
            if value is None or value is pd.NA:
                return default
            result = float(value)
        except (TypeError, ValueError):
            return default
        return default if pd.isna(result) else result

    def generate_signal_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        fx_cfg = self.config.strategy.fx_breakout_pullback
        working = frame.copy().sort_index()
        working["entry_signal"] = False
        working["exit_signal"] = False
        working["partial_exit_signal"] = False
        working["signal_score"] = 0.0
        working["signal_action"] = SignalAction.HOLD.value
        working["strategy_state"] = "FLAT"
        working["position_side"] = pd.NA
        working["entry_order_side"] = pd.NA
        working["exit_order_side"] = pd.NA
        working["reverse_exit_signal"] = False
        working["breakout_episode_id"] = 0
        working["pullback_depth_atr"] = 0.0
        working["entry_trigger_price"] = pd.NA
        working["initial_stop_price"] = pd.NA
        working["initial_risk_price"] = pd.NA
        working["entry_trigger_distance_atr"] = 0.0
        working["reasons_ja"] = [[] for _ in range(len(working.index))]
        working["explanation_ja"] = ""

        breakout_episode_id = 0
        pending_breakout_idx: int | None = None
        breakout_level = 0.0
        breakout_atr = 0.0
        breakout_peak = 0.0
        active_episode_id = 0
        last_breakout_bar: pd.Timestamp | None = None
        last_trend_bar: pd.Timestamp | None = None

        states: list[str] = []
        reason_lists: list[list[str]] = []
        explanations: list[str] = []
        scores: list[float] = []

        for position, (timestamp, row) in enumerate(working.iterrows()):
            reasons: list[str] = []
            state = "FLAT"
            pullback_depth = 0.0
            if self._as_bool(row.get("trend_long_allowed_1h", False)):
                reasons.append("1時間足トレンド許可")
            if self._as_bool(row.get("spread_context_ok", True), default=True):
                reasons.append("時間帯別スプレッド条件を通過")
            if self._as_bool(row.get("spread_ratio_ok", True), default=True):
                reasons.append("spread/ATR 条件を通過")
            if self._as_bool(row.get("event_blackout", False)):
                reasons.append("重要指標ブラックアウト")
            if self._as_bool(row.get("rollover_blackout", False)):
                reasons.append("ロールオーバー回避時間")
            if self._as_bool(row.get("tokyo_early_blackout", False)):
                reasons.append("東京早朝回避時間")

            breakout_bar = row.get("signal_bar_timestamp")
            if pd.notna(breakout_bar) and breakout_bar != last_breakout_bar and self._as_bool(row.get("breakout_signal_15m", False)):
                last_breakout_bar = pd.Timestamp(breakout_bar)
                if self._as_bool(row.get("trend_long_allowed_1h", False)):
                    breakout_episode_id += 1
                    active_episode_id = breakout_episode_id
                    pending_breakout_idx = position
                    breakout_level = self._as_float(row.get("breakout_level_15m"), self._as_float(row["close"], 0.0))
                    breakout_atr = max(
                        self._as_float(row.get("breakout_atr_15m"), self._as_float(row.get("atr_15m"), 0.01)),
                        0.01,
                    )
                    breakout_peak = max(self._as_float(row.get("mid_high", row["high"])), breakout_level)
                    reasons.append("15分足終値ブレイクを検出")
                    state = "BREAKOUT_DETECTED"

            if pending_breakout_idx is not None and active_episode_id > 0:
                state = "WAIT_PULLBACK"
                bars_since_breakout = position - pending_breakout_idx
                if bars_since_breakout > fx_cfg.pullback_window_bars:
                    pending_breakout_idx = None
                    active_episode_id = 0
                    breakout_level = 0.0
                    breakout_atr = 0.0
                    breakout_peak = 0.0
                    state = "FLAT"
                elif position > pending_breakout_idx:
                    breakout_peak = max(breakout_peak, self._as_float(row.get("mid_high", row["high"])))
                    pullback_depth = (breakout_peak - self._as_float(row.get("mid_low", row["low"]))) / max(breakout_atr, 0.01)
                    working.at[timestamp, "pullback_depth_atr"] = pullback_depth
                    break_floor = breakout_level - fx_cfg.pullback_break_below_buffer_atr * breakout_atr
                    valid_pullback = (
                        fx_cfg.min_pullback_atr_ratio <= pullback_depth <= fx_cfg.shallow_pullback_max_ratio
                        and self._as_float(row.get("mid_low", row["low"])) >= break_floor
                    )
                    invalid_pullback = (
                        self._as_float(row.get("mid_low", row["low"])) < break_floor
                        or pullback_depth > fx_cfg.shallow_pullback_max_ratio
                    )
                    if invalid_pullback:
                        reasons.append("押しが深すぎるかブレイク水準を下抜き")
                        pending_breakout_idx = None
                        active_episode_id = 0
                        breakout_level = 0.0
                        breakout_atr = 0.0
                        breakout_peak = 0.0
                        state = "FLAT"
                    elif valid_pullback:
                        trigger_price = max(
                            self._as_float(row.get("ask_high", row.get("mid_high", row["high"]))),
                            self._as_float(working.iloc[position - 1].get("ask_high", row.get("ask_high", row["high"]))) if position > 0 else self._as_float(row.get("ask_high", row["high"])),
                        )
                        swing_start = max(0, position - fx_cfg.swing_lookback_bars + 1)
                        swing_low = self._as_float(
                            row.get("swing_low_reference"),
                            self._as_float(
                                working.iloc[swing_start : position + 1]["mid_low"].min()
                                if "mid_low" in working.columns
                                else working.iloc[swing_start : position + 1]["low"].min(),
                            ),
                        )
                        initial_stop = min(
                            trigger_price - fx_cfg.atr_stop_mult * breakout_atr,
                            swing_low - fx_cfg.swing_buffer_atr * breakout_atr,
                        )
                        initial_risk = max(trigger_price - initial_stop, 0.01)
                        trigger_distance_atr = (trigger_price - breakout_level) / max(breakout_atr, 0.01)
                        if self._as_bool(row.get("entry_context_ok", False)):
                            working.at[timestamp, "entry_signal"] = True
                            working.at[timestamp, "position_side"] = "long"
                            working.at[timestamp, "entry_order_side"] = SignalAction.BUY.value
                            working.at[timestamp, "exit_order_side"] = SignalAction.SELL.value
                            working.at[timestamp, "entry_trigger_price"] = trigger_price
                            working.at[timestamp, "initial_stop_price"] = initial_stop
                            working.at[timestamp, "initial_risk_price"] = initial_risk
                            working.at[timestamp, "entry_trigger_distance_atr"] = trigger_distance_atr
                            reasons.append("軽い押しを確認し再上昇トリガーを設定")
                            state = "ENTRY_ARMED"
                        else:
                            working.at[timestamp, "entry_trigger_distance_atr"] = trigger_distance_atr
                            reasons.append("押しは有効だがエントリー禁止条件で見送り")
                            state = "ENTRY_BLOCKED"
                        pending_breakout_idx = None
                        active_episode_id = 0
                        breakout_level = 0.0
                        breakout_atr = 0.0
                        breakout_peak = 0.0

            trend_bar = row.get("trend_bar_timestamp")
            if pd.notna(trend_bar) and trend_bar != last_trend_bar:
                last_trend_bar = pd.Timestamp(trend_bar)
                if self._as_bool(row.get("full_exit_trend_break_1h", False)):
                    working.at[timestamp, "exit_signal"] = True
                    working.at[timestamp, "reverse_exit_signal"] = True
                    working.at[timestamp, "exit_order_side"] = SignalAction.SELL.value
                    reasons.append("1時間足EMAクロスで全決済シグナル")
                    state = "FLAT_EXITED"
                elif self._as_bool(row.get("partial_exit_trend_break_1h", False)):
                    working.at[timestamp, "partial_exit_signal"] = True
                    working.at[timestamp, "exit_order_side"] = SignalAction.SELL.value
                    reasons.append("1時間足トレンド崩れで一部手仕舞いシグナル")
                    state = "PARTIAL_EXIT_DONE"

            action = SignalAction.HOLD.value
            if self._as_bool(working.at[timestamp, "entry_signal"]):
                action = SignalAction.BUY.value
            elif self._as_bool(working.at[timestamp, "exit_signal"]) or self._as_bool(working.at[timestamp, "partial_exit_signal"]):
                action = SignalAction.SELL.value
            working.at[timestamp, "signal_action"] = action
            working.at[timestamp, "breakout_episode_id"] = active_episode_id or breakout_episode_id

            score = 0.0
            score += 0.35 if self._as_bool(row.get("trend_long_allowed_1h", False)) else 0.0
            score += 0.20 if self._as_bool(row.get("spread_context_ok", True), default=True) else 0.0
            score += 0.20 if self._as_bool(row.get("spread_ratio_ok", True), default=True) else 0.0
            score += 0.25 if self._as_bool(working.at[timestamp, "entry_signal"]) else 0.0
            score = min(score, 1.0)
            working.at[timestamp, "signal_score"] = score
            states.append(state)
            reason_lists.append(reasons)
            explanations.append(self._build_explanation(state, reasons))
            scores.append(score)

        working["strategy_state"] = states
        working["reasons_ja"] = reason_lists
        working["explanation_ja"] = explanations
        working["signal_score"] = scores
        return working

    def _build_explanation(self, state: str, reasons: list[str]) -> str:
        if not reasons:
            return f"{state}: 条件未達"
        return f"{state}: {' / '.join(reasons)}"
