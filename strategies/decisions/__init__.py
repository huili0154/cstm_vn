"""
可插拔决策器注册表。

新增决策器只需：
1. 在本目录下新建文件实现 DecisionBase 子类
2. 在下方 DECISION_REGISTRY 中注册一行
"""

from __future__ import annotations

from strategies.decisions.base import DecisionBase, DecisionContext, Signal
from strategies.decisions.original import OriginalDecision

DECISION_REGISTRY: dict[str, type[DecisionBase]] = {
    "original": OriginalDecision,
}

__all__ = [
    "DecisionBase",
    "DecisionContext",
    "Signal",
    "DECISION_REGISTRY",
    "create_decision",
]


def create_decision(decision_type: str, setting: dict | None = None) -> DecisionBase:
    """根据类型名创建决策器实例。"""
    cls = DECISION_REGISTRY.get(decision_type)
    if cls is None:
        raise ValueError(
            f"Unknown decision_type={decision_type!r}, "
            f"available: {list(DECISION_REGISTRY.keys())}"
        )
    return cls(setting)
