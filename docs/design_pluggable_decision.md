# 可插拔决策器设计文档

> **Pluggable Decision** — 将信号决策从 DS_DMTR 策略中解耦，支持多种决策器变体独立开发、互不干扰。

***

## 修订记录

| 版本 | 日期       | 内容     |
| ---- | ---------- | -------- |
| v0.2 | 2026-03-28 | 去掉 strength，决策器直接输出 trade_pct |
| v0.1 | 2026-03-28 | 初稿     |

***

## 1. 动机

当前 `DsDmtrStrategy` 的信号判断（`_decide()`）与指标计算、订单执行、block 生命周期管理耦合在同一个 1500+ 行的类中。想要尝试新的信号变体（如趋势过滤、自相关过滤等）时，只能在原有 `_decide()` 里加 `if` 分支，存在以下问题：

1. **污染风险**：每次改动都可能触碰原有逻辑
2. **参数膨胀**：所有变体的参数混在一个 JSON 中
3. **不可并行实验**：无法同时保有多个独立的信号逻辑进行对比回测

**目标**：将 `_decide()` 抽为可插拔的 `Decision` 组件。新增信号变体 = 新建文件写新类，原有策略代码和执行逻辑零修改。

***

## 2. 整体架构

```
DsDmtrStrategy (策略壳)
│
├── 指标计算 (on_tick 中现有逻辑, 不动)
│     ↓ 构建
├── DecisionContext (只读快照)
│     ↓ 传入
├── Decision (可插拔决策器)
│     ↓ 输出
├── Signal | None
│     ↓ 传入
└── 执行层 (_execute + block 管理, 不动)
```

改动范围集中在**策略壳内部的组装方式**，不涉及 `core/`、`backtest/` 等底层模块。

***

## 3. 核心接口

### 3.1 DecisionContext — 决策器的输入

决策器不直接访问策略实例，只通过 `DecisionContext` 获取信息。这保证决策器是**纯函数式**的——相同输入必产生相同输出，便于测试和调试。

```python
@dataclass(frozen=True)
class DecisionContext:
    """决策器的只读输入快照。"""

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
```

**设计要点**：

- 使用 `frozen=True` 禁止决策器修改上下文
- 如果未来需要新指标（如自相关系数），在此添加字段，所有决策器自动可用
- 不含 account/position 信息——仓位管理是执行层的职责

### 3.2 Signal — 决策器的输出

```python
@dataclass
class Signal:
    """决策器产生的交易信号。"""

    direction: str      # "SELL_A_BUY_B" 或 "SELL_B_BUY_A"
    trade_pct: float    # 交易比例（占 NAV），由决策器直接给出
    reason: str         # 人类可读的触发原因（写入 block 日志）
```

**设计要点**：

- `trade_pct` 由决策器直接决定（如 base_pct 或 high_pct），执行层拿到即用，无需映射
- 决策器返回 `Signal | None`，`None` 表示无信号
- `reason` 用于日志和回测分析，例如 `"zscore=2.3 > thresh=0.8, day_align OK"`
- 交易量的计算（trade_pct → 实际股数）仍由执行层负责，因为涉及持仓和现金约束

### 3.3 DecisionBase — 决策器基类

```python
class DecisionBase(ABC):
    """信号决策器基类。"""

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

    def get_param_schema(self) -> dict[str, tuple[type, any]]:
        """返回参数的类型和默认值，供 GUI 渲染。

        Returns {param_name: (type, default_value)}
        """
        schema = {}
        for key in self.parameters:
            val = getattr(self, key, None)
            if val is not None:
                schema[key] = (type(val), val)
        return schema
```

***

## 4. 决策器实现

### 4.1 OriginalDecision — 原版逻辑 1:1 搬迁

```python
class OriginalDecision(DecisionBase):
    """原版 DS_DMTR 决策逻辑，与当前 _decide() 行为完全一致。"""

    # ── 参数 ──
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
        direction = "SELL_A_BUY_B" if ctx.delta_sigma_minutes > 0 else "SELL_B_BUY_A"

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
```

### 4.2 DayAlignDecision — 日线方向一致性过滤

```python
class DayAlignDecision(DecisionBase):
    """在 OriginalDecision 基础上增加趋势过滤：
    当日线偏离显著时，只允许与日线偏离方向一致的信号。
    """

    # 原版参数
    thresh_sigma_min: float = 0.8
    thresh_sigma_min_high: float = 2.55
    thresh_sigma_day: float = 0.4
    thresh_delta_min: float = 0.0025
    cooldown_seconds: int = 1800
    base_pct: float = 0.32
    high_pct: float = 0.3

    # 新增参数
    day_align_min_sigma: float = 0.3   # 日线 z-score 超过此值时启用方向一致性约束

    parameters = [
        "thresh_sigma_min", "thresh_sigma_min_high",
        "thresh_sigma_day", "thresh_delta_min",
        "cooldown_seconds",
        "base_pct", "high_pct",
        "day_align_min_sigma",
    ]

    def decide(self, ctx: DecisionContext) -> Signal | None:
        if not ctx.min_stats_valid:
            return None

        abs_dsm = abs(ctx.delta_sigma_minutes)
        abs_dm = abs(ctx.delta_minutes)

        if abs_dsm <= self.thresh_sigma_min or abs_dm <= self.thresh_delta_min:
            return None

        direction = "SELL_A_BUY_B" if ctx.delta_sigma_minutes > 0 else "SELL_B_BUY_A"

        # 冷却
        if ctx.last_trade_direction == direction and ctx.last_trade_time:
            elapsed = (ctx.current_time - ctx.last_trade_time).total_seconds()
            if elapsed < self.cooldown_seconds:
                return None

        # ★ 趋势过滤：日线偏离显著时，要求方向一致
        if ctx.day_stats_valid and abs(ctx.delta_sigma_days) >= self.day_align_min_sigma:
            if ctx.delta_sigma_minutes * ctx.delta_sigma_days < 0:
                return None  # 分钟信号与日线趋势方向相反，过滤

        # 交易比例
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
                f"dsd={ctx.delta_sigma_days:+.3f} "
                f"[day_align]"
            ),
        )

    def is_still_valid(self, ctx: DecisionContext, direction: str) -> bool:
        signal = self.decide(ctx)
        return signal is not None and signal.direction == direction
```

### 4.3 未来可扩展的方向（举例，暂不实现）

- `AutocorrDecision` — 加自相关趋势检测
- `AdaptiveThreshDecision` — 根据近期胜率动态调整阈值
- `CompositeDecision` — 组合多个 Decision，投票决策

***

## 5. 策略壳改动

### 5.1 DsDmtrStrategy 的变化

改动集中在三处，其余代码（指标计算、block 管理、订单执行）完全不动。

#### 5.1.1 新增属性和初始化

```python
class DsDmtrStrategy(StrategyBase):

    # ── 新增：决策器类型 ──
    decision_type: str = "original"

    parameters = [
        ...,  # 原有参数保留
        "decision_type",
    ]

    def __init__(self, engine, strategy_name, symbols, setting=None):
        super().__init__(engine, strategy_name, symbols, setting)
        ...  # 原有初始化不动

        # 创建决策器实例
        self._decision = create_decision(self.decision_type, setting)
```

#### 5.1.2 on_tick 中替换 _decide 调用

```python
# 原来 (第 685-687 行)：
should_act, direction, trade_pct = self._decide(tick.datetime)
if should_act:
    self._execute(direction, trade_pct, tick.datetime, nav)

# 改为：
ctx = self._build_context(tick.datetime)
signal = self._decision.decide(ctx)
if signal:
    self._execute(signal.direction, signal.trade_pct, tick.datetime, nav)
```

#### 5.1.3 信号失效检测中替换 _decide 调用

```python
# 原来 (_check_signal_reversion 第 1353 行)：
should_act, direction, _ = self._decide(now)
if should_act and direction == block.direction:
    return False

# 改为：
ctx = self._build_context(now)
if self._decision.is_still_valid(ctx, block.direction):
    return False
```

### 5.2 辅助方法

```python
def _build_context(self, current_time: datetime) -> DecisionContext:
    """从策略当前状态构建 DecisionContext 快照。"""
    return DecisionContext(
        ab_ratio=self.ab_ratio,
        mu_min=self.mu_min,
        sigma_min=self.sigma_min,
        mu_day=self.mu_day,
        sigma_day=self.sigma_day,
        delta_sigma_minutes=self.delta_sigma_minutes,
        delta_sigma_days=self.delta_sigma_days,
        delta_minutes=self.delta_minutes,
        min_stats_valid=self._min_stats_valid,
        day_stats_valid=self._day_stats_valid,
        a_percentage=self.a_percentage,
        last_trade_direction=self._last_trade_direction,
        last_trade_time=self._last_trade_time,
        current_time=current_time,
    )
```

### 5.3 决策器注册表

```python
# strategies/decisions/__init__.py

from strategies.decisions.original import OriginalDecision
from strategies.decisions.day_align import DayAlignDecision

DECISION_REGISTRY: dict[str, type[DecisionBase]] = {
    "original": OriginalDecision,
    "day_align": DayAlignDecision,
}

def create_decision(decision_type: str, setting: dict | None = None) -> DecisionBase:
    cls = DECISION_REGISTRY.get(decision_type)
    if cls is None:
        raise ValueError(
            f"Unknown decision_type={decision_type!r}, "
            f"available: {list(DECISION_REGISTRY.keys())}"
        )
    return cls(setting)
```

新增决策器只需：
1. 在 `strategies/decisions/` 下新建文件
2. 在 `__init__.py` 的 `DECISION_REGISTRY` 中注册一行

***

## 6. 文件结构

```
strategies/
├── ds_dmtr_strategy.py           # 策略壳（改动 ~30 行）
├── mstr_strategy.py              # 不动
└── decisions/                    # 新增目录
    ├── __init__.py               # 注册表 + create_decision()
    ├── base.py                   # DecisionBase, DecisionContext, Signal
    ├── original.py               # OriginalDecision（原版 1:1）
    └── day_align.py              # DayAlignDecision（趋势过滤）
```

***

## 7. JSON 参数格式

### 7.1 原有行为（完全兼容）

```json
{
  "strategy": {
    "symbol_a": "159655.SZ",
    "symbol_b": "513500.SH",
    "decision_type": "original",
    "thresh_sigma_min": 0.8,
    "thresh_sigma_min_high": 2.55,
    ...
  }
}
```

`decision_type` 缺省为 `"original"`，行为与改动前完全一致。

### 7.2 使用 DayAlign 变体

```json
{
  "strategy": {
    "symbol_a": "159655.SZ",
    "symbol_b": "513500.SH",
    "decision_type": "day_align",
    "thresh_sigma_min": 0.8,
    "thresh_sigma_min_high": 2.55,
    "day_align_min_sigma": 0.3,
    ...
  }
}
```

决策器从同一个 `strategy` dict 中取自己需要的参数，互不干扰。

***

## 8. GUI 改动

### 8.1 DS_DMTR GUI

在现有参数面板顶部增加一个下拉框：

```
信号决策器: [Original ▼]
            ├── Original
            ├── DayAlign
            └── ...
```

切换决策器时：
1. 从 `DECISION_REGISTRY` 获取对应类
2. 调用 `get_param_schema()` 获取参数列表和默认值
3. 动态渲染该决策器的专属参数控件

**执行层参数**（`passive_slice_count`、`block_timeout_minutes` 等）始终显示，不随决策器切换而变化。

### 8.2 MSTR GUI

本次不改动。

***

## 9. RotationBlock 快照

现有 `RotationBlock` 中的指标快照字段（`delta_sigma_minutes`、`mu_min` 等）保留不变。
新增一个字段记录信号来源：

```python
@dataclass
class RotationBlock:
    ...
    # ── 新增 ──
    signal_reason: str = ""      # Signal.reason
```

在 `_execute()` 创建 block 时从 `Signal.reason` 写入。

***

## 11. 验证计划

### 11.1 回归验证

用**完全相同的参数**，分别运行：
- 改动前的 `DsDmtrStrategy`（原版 `_decide()`）
- 改动后的 `DsDmtrStrategy`（`decision_type="original"`）

对比两次回测的逐 block 结果，要求 **完全一致**（block 数量、方向、时间、成交量、NAV 曲线）。

### 11.2 新决策器验证

运行 `decision_type="day_align"` 回测，检查：
- 被日线过滤的 block 数量是否合理
- 在趋势月份（如 9-10 月）的胜率是否提升

***

## 10. 参数归属

重构后，原 `DsDmtrStrategy` 的参数分为两类：

**决策器参数**（随决策器类型变化，由决策器自己持有）：
- `thresh_sigma_min`, `thresh_sigma_min_high`, `thresh_sigma_day`, `thresh_delta_min`
- `cooldown_seconds`
- `base_pct`, `high_pct`
- 以及各变体自己的参数（如 `day_align_min_sigma`）

**策略壳 / 执行层参数**（始终存在，不随决策器变化）：
- `symbol_a`, `symbol_b`, `dataset_dir`
- `bar_interval_minutes`, `window_minutes`, `window_days`
- `k_sigma_minutes`, `k_sigma_days`（用于可视化布林带，属指标层）
- `trading_cutoff_str`, `open_wait_minutes`
- `block_timeout_minutes`, `timeout_recover_minutes`, `timeout_recover_policy`
- `chase_wait_ticks`, `max_chase_rounds`, `passive_slice_count`, `cancel_priority`
- `min_order_ratio`, `enable_signal_check`, `enable_t0`

原 `DsDmtrStrategy.parameters` 列表中的决策器参数保留（保持 JSON 向后兼容），
但策略壳不再直接使用它们——统一通过 `self._decision` 访问。

***

## 11. 实施步骤

| 步骤 | 内容 | 风险 |
|------|------|------|
| 1 | 创建 `strategies/decisions/` 目录，编写 `base.py`（接口定义） | 无 |
| 2 | 编写 `original.py`（从 `_decide()` 1:1 搬迁） | 低 |
| 3 | 修改 `DsDmtrStrategy`：添加 `_build_context()`，替换 `_decide()` 和 `_check_signal_reversion` 的调用 | 中 — 需回归验证 |
| 4 | 运行回归验证，确认 block 结果完全一致 | — |
| 5 | 编写 `day_align.py` | 低 |
| 6 | GUI 添加决策器下拉框和动态参数面板 | 低 |

步骤 1-4 是最关键的，完成后即可验证架构可行性。步骤 5-6 可在验证通过后进行。
