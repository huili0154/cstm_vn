"""
ETF 网格交易策略 — 回测/实盘通用。

原理:
  以基准价为中心，每隔 grid_step 设置一个网格。
  价格每下穿一格买入，每上穿一格卖出，赚取震荡收益。

参数:
  grid_step   — 网格间距 (如 0.02 = 2%)
  grid_lots   — 每格交易量 (份)
  max_grids   — 最大网格层数 (单边)
  base_price  — 基准价 (0=自动取首个tick/bar的价格)
"""

from __future__ import annotations

from core.datatypes import BarData, Order, TickData, Trade
from core.strategy import StrategyBase


class GridStrategy(StrategyBase):
    """ETF 网格策略。"""

    author = "cstm_vn"

    # ── 可配置参数 ──
    grid_step: float = 0.02        # 网格间距 (百分比, 0.02 = 2%)
    grid_lots: int = 10000         # 每格交易量 (份)
    max_grids: int = 5             # 单边最大网格层数
    base_price: float = 0.0        # 基准价 (0=自动)

    parameters = ["grid_step", "grid_lots", "max_grids", "base_price"]
    variables = ["current_grid", "actual_base"]

    def on_init(self) -> None:
        self.current_grid: int = 0      # 当前网格位置 (0=基准, 负=低于基准)
        self.actual_base: float = 0.0   # 实际使用的基准价
        self.write_log(
            f"GridStrategy init: step={self.grid_step}, "
            f"lots={self.grid_lots}, max_grids={self.max_grids}"
        )

    def on_start(self) -> None:
        self.write_log("GridStrategy started")

    def on_stop(self) -> None:
        self.write_log(
            f"GridStrategy stopped. final_grid={self.current_grid}, "
            f"base={self.actual_base:.3f}"
        )

    def on_day_begin(self, bar: BarData) -> None:
        """CLOSE_FILL 模式 / 实盘每日开始前回调。"""
        # 自动设定基准价
        if self.actual_base == 0.0:
            self.actual_base = self.base_price or bar.close
            self.write_log(f"Base price set: {self.actual_base:.3f}")

    def on_tick(self, tick: TickData) -> None:
        """Tick 驱动的核心逻辑。"""
        price = tick.last_price
        if price <= 0:
            return

        # 自动设定基准价
        if self.actual_base == 0.0:
            self.actual_base = self.base_price or price
            self.write_log(f"Base price set: {self.actual_base:.3f}")
            return

        # 计算当前价格对应的网格层
        target_grid = self._price_to_grid(price)

        # 网格变化 → 交易
        while self.current_grid < target_grid:
            # 价格上穿 → 卖出
            if self.current_grid < 0:
                # 只有持仓时才卖
                pos = self.get_position(tick.symbol)
                if pos.available >= self.grid_lots:
                    sell_price = self._grid_to_price(self.current_grid + 1)
                    self.sell(tick.symbol, sell_price, self.grid_lots)
                    self.write_log(
                        f"SELL grid {self.current_grid}→{self.current_grid+1} "
                        f"@ {sell_price:.3f}"
                    )
            self.current_grid += 1

        while self.current_grid > target_grid:
            # 价格下穿 → 买入
            if self.current_grid > -self.max_grids:
                buy_price = self._grid_to_price(self.current_grid - 1)
                # 检查资金
                acct = self.get_account()
                cost = buy_price * self.grid_lots
                if acct.available >= cost:
                    self.buy(tick.symbol, buy_price, self.grid_lots)
                    self.write_log(
                        f"BUY grid {self.current_grid}→{self.current_grid-1} "
                        f"@ {buy_price:.3f}"
                    )
            self.current_grid -= 1

    def on_order(self, order: Order) -> None:
        self.write_log(
            f"Order: {order.symbol} {order.direction.value} "
            f"{order.status.value} vol={order.volume} traded={order.traded}"
        )

    def on_trade(self, trade: Trade) -> None:
        self.write_log(
            f"Trade: {trade.symbol} {trade.direction.value} "
            f"price={trade.price:.3f} vol={trade.volume}"
        )
        pos = self.get_position(trade.symbol)
        self.write_log(
            f"  Position: vol={pos.volume} available={pos.available} "
            f"cost={pos.cost_price:.3f}"
        )

    # ── 辅助方法 ──

    def _price_to_grid(self, price: float) -> int:
        """价格 → 网格层 (相对于基准价)。"""
        ratio = (price - self.actual_base) / self.actual_base
        return round(ratio / self.grid_step)

    def _grid_to_price(self, grid: int) -> float:
        """网格层 → 对应价格。"""
        return round(self.actual_base * (1 + grid * self.grid_step), 3)
