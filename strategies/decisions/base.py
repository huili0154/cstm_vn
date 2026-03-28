"""
可插拔决策器 — 基类与数据类型定义。

DecisionContext: 决策器的只读输入快照
Signal:          决策器的输出
DecisionBase:    决策器抽象基类
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar


@dataclass(frozen=True)
class DecisionContext:
    """决策器的只读输入快照。

    由策略壳在每个 tick 构建，传给决策器。
    frozen=True 保证决策器不会修改上下文。
    """

    # ── 指标 ──
    ab_ratio: float
    mu_min: float
    sigma_min: float
    mu_day: float
    sigma_day: float
    delta_sigma_minutes: float   # 分钟级 z-score
    delta_sigma_days: float      # 日线级 z-score
    delta_minutes: float         # 分钟级百分比偏离
    min_stats_valid: bool
    day_stats_valid: bool

    # ── 持仓 ──
    a_percentage: float          # A 占 NAV 比例

    # ── 交易历史（用于冷却判断） ──
    last_trade_direction: str    # "SELL_A_BUY_B" / "SELL_B_BUY_A" / ""
    last_trade_time: datetime | None

    # ── 时间 ──
    current_time: datetime


@dataclass
class Signal:
    """决策器产生的交易信号。"""

    direction: str      # "SELL_A_BUY_B" 或 "SELL_B_BUY_A"
    trade_pct: float    # 交易比例（占 NAV），由决策器直接给出
    reason: str         # 人类可读的触发原因（写入 block 日志）


class DecisionBase(ABC):
    """信号决策器基类。

    子类只需实现 decide() 和 is_still_valid() 两个方法。
    参数通过 parameters 列表声明，从 setting dict 自动加载。
    """

    # 子类声明自己需要的参数名，用于 GUI 动态渲染和 JSON 序列化
    parameters: ClassVar[list[str]] = []

    def __init__(self, setting: dict | None = None):
        """从 setting dict 加载参数值。"""
        if setting:
            for key in self.parameters:
                if key in setting:
                    setattr(self, key, setting[key])

    @abstractmethod
    def decide(self, ctx: DecisionContext) -> Signal | None:
        """核心决策：看指标，决定是否交易。

        返回 Signal 表示应交易，返回 None 表示无信号。
        """
        ...

    @abstractmethod
    def is_still_valid(self, ctx: DecisionContext, direction: str) -> bool:
        """信号存续检查：当前市场状态下，已发出的信号是否仍然有效？

        在 block 执行期间（PENDING/MATCHING 阶段）每个 tick 调用。
        返回 False 会触发信号失效处理（撤单/再平衡）。

        Parameters
        ----------
        ctx : DecisionContext
            当前时刻的指标快照
        direction : str
            当前 block 的方向（"SELL_A_BUY_B" / "SELL_B_BUY_A"）
        """
        ...

    def get_param_schema(self) -> dict[str, tuple[type, object]]:
        """返回参数的类型和默认值，供 GUI 渲染。

        Returns {param_name: (type, default_value)}
        """
        schema = {}
        for key in self.parameters:
            val = getattr(self, key, None)
            if val is not None:
                schema[key] = (type(val), val)
        return schema
