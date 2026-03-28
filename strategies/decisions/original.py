"""
OriginalDecision — 原版 DS_DMTR 决策逻辑 1:1 搬迁。

与 DsDmtrStrategy._decide() 行为完全一致，作为回归验证的基准。
"""

from __future__ import annotations

from strategies.decisions.base import DecisionBase, DecisionContext, Signal


class OriginalDecision(DecisionBase):
    """原版 DS_DMTR 决策逻辑。"""

    # ── 参数（默认值与 ds_dmtr_params.json 对齐） ──
    thresh_sigma_min: float = 0.8
    thresh_sigma_min_high: float = 2.55
    thresh_sigma_day: float = 0.4
    thresh_delta_min: float = 0.0025
    cooldown_seconds: int = 1800
    base_pct: float = 0.32
    high_pct: float = 0.3

    parameters = [
        "thresh_sigma_min", "thresh_sigma_min_high",
        "thresh_sigma_day", "thresh_delta_min",
        "cooldown_seconds",
        "base_pct", "high_pct",
    ]

    def decide(self, ctx: DecisionContext) -> Signal | None:
        if not ctx.min_stats_valid:
            return None

        abs_dsm = abs(ctx.delta_sigma_minutes)
        abs_dm = abs(ctx.delta_minutes)

        if abs_dsm <= self.thresh_sigma_min or abs_dm <= self.thresh_delta_min:
            return None

        # 方向
        if ctx.delta_sigma_minutes > 0:
            direction = "SELL_A_BUY_B"
        else:
            direction = "SELL_B_BUY_A"

        # 冷却
        if ctx.last_trade_direction == direction and ctx.last_trade_time:
            elapsed = (ctx.current_time - ctx.last_trade_time).total_seconds()
            if elapsed < self.cooldown_seconds:
                return None

        # 交易比例：判断是否达到加码条件
        abs_dsd = abs(ctx.delta_sigma_days) if ctx.day_stats_valid else 0.0
        day_minute_same_dir = (
            ctx.day_stats_valid
            and ctx.delta_sigma_minutes * ctx.delta_sigma_days > 0
        )

        if (
            abs_dsm > self.thresh_sigma_min_high
            or (abs_dsd > self.thresh_sigma_day and day_minute_same_dir)
        ):
            trade_pct = self.high_pct
        else:
            trade_pct = self.base_pct

        return Signal(
            direction=direction,
            trade_pct=trade_pct,
            reason=(
                f"dsm={ctx.delta_sigma_minutes:+.3f} "
                f"dm={ctx.delta_minutes:+.5f} "
                f"dsd={ctx.delta_sigma_days:+.3f}"
            ),
        )

    def is_still_valid(self, ctx: DecisionContext, direction: str) -> bool:
        """与原版 _check_signal_reversion 一致：
        重新调用 decide()，如果仍产生同方向信号则有效。
        """
        signal = self.decide(ctx)
        return signal is not None and signal.direction == direction
