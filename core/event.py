"""
轻量事件总线。

回测时同步调用所有 handler；实盘时可扩展为异步。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

# 事件类型常量
EVENT_TICK = "event_tick"
EVENT_BAR = "event_bar"
EVENT_ORDER = "event_order"
EVENT_TRADE = "event_trade"
EVENT_POSITION = "event_position"
EVENT_ACCOUNT = "event_account"
EVENT_LOG = "event_log"

HandlerType = Callable[[Any], None]


class EventBus:
    """
    同步事件总线。

    用法::

        bus = EventBus()
        bus.on(EVENT_TRADE, my_handler)
        bus.emit(EVENT_TRADE, trade_data)
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[HandlerType]] = defaultdict(list)

    def on(self, event_type: str, handler: HandlerType) -> None:
        """注册事件处理函数。同一事件可注册多个 handler，按注册顺序调用。"""
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)

    def off(self, event_type: str, handler: HandlerType) -> None:
        """取消注册。"""
        handlers = self._handlers.get(event_type)
        if handlers and handler in handlers:
            handlers.remove(handler)

    def emit(self, event_type: str, data: Any = None) -> None:
        """触发事件，同步调用所有已注册的 handler。"""
        for handler in self._handlers.get(event_type, []):
            handler(data)

    def clear(self) -> None:
        """清除所有注册。"""
        self._handlers.clear()
