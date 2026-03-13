"""
MSTR 策略烟雾测试。

验证策略能在 BacktestEngine 中正常运行:
  - on_init / on_day_begin / on_tick / on_order / on_trade / on_stop 全流程
  - 信号模型计算不崩溃
  - Block 交易能正常启动和完成
"""

import pytest
from pathlib import Path

from backtest.engine import BacktestEngine
from core.datatypes import MatchingMode
from strategies.mstr_strategy import MstrStrategy, BlockState


DATASET_DIR = Path(__file__).parent.parent / "dataset"


class TestMstrSmoke:
    """MSTR 策略基础烟雾测试。"""

    def _make_engine(self, mode=MatchingMode.SMART_TICK_DELAY_FILL):
        return BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=mode,
            initial_capital=500_000.0,
            rate=0.00005,
            pricetick=0.001,
        )

    def test_import(self):
        """策略类可以正常导入。"""
        assert MstrStrategy is not None
        assert hasattr(MstrStrategy, "parameters")

    def test_short_backtest_3_symbols(self):
        """
        3 只 ETF + 2 个交易日：策略能正常运行不崩溃。
        """
        engine = self._make_engine()
        symbols = ["510300.SH", "510330.SH", "510350.SH"]
        strategy = MstrStrategy(
            engine,
            "mstr_test",
            symbols,
            setting={
                "dataset_dir": str(DATASET_DIR),
                "window": 20,
                "k_threshold": 0.8,
                "least_bias": 0.01,
                "num_blocks": 5,
                "sub_lots": 3,
            },
        )

        result = engine.run(
            strategy,
            start_date="20240201",
            end_date="20240204",
        )

        # 基本检查: 引擎正常返回结果
        assert result is not None
        assert result.start_balance == 500_000.0
        assert result.end_balance > 0

        # 策略状态检查
        assert strategy._stats_initialized
        assert strategy.cash_blocks + sum(strategy.block_count.values()) == strategy.num_blocks

    def test_warmup_period(self):
        """
        窗口不足时策略应该保持静默（不交易）。
        使用极早的起始日期，确保历史数据不足。
        """
        engine = self._make_engine()
        symbols = ["510300.SH", "510330.SH"]
        strategy = MstrStrategy(
            engine,
            "mstr_warmup",
            symbols,
            setting={
                "dataset_dir": str(DATASET_DIR),
                "window": 20,
                "num_blocks": 3,
            },
        )

        # 极早的日期，只有0~1天历史，window=20 需要19天历史
        result = engine.run(
            strategy,
            start_date="20230104",
            end_date="20230106",
        )

        # 窗口不够时应该全为空
        assert result.end_balance == result.start_balance
        assert len(strategy._block_logs) == 0

    def test_longer_run_5_symbols(self):
        """
        5 只 ETF + ~20 个交易日：测信号计算和 Block 交易。
        """
        engine = self._make_engine()
        symbols = [
            "510300.SH", "510330.SH", "510350.SH",
            "159300.SZ", "159919.SZ",
        ]
        strategy = MstrStrategy(
            engine,
            "mstr_longer",
            symbols,
            setting={
                "dataset_dir": str(DATASET_DIR),
                "window": 20,
                "k_threshold": 0.6,    # 降低阈值增大触发概率
                "least_bias": 0.001,   # 降低噪音门槛
                "num_blocks": 4,
                "sub_lots": 2,
                "cooldown_1": 5,
                "cooldown_2": 8,
                "block_timeout": 10,
            },
        )

        result = engine.run(
            strategy,
            start_date="20240301",
            end_date="20240325",
        )

        assert result is not None
        assert result.end_balance > 0

        # 一致性检查
        total_blocks = sum(strategy.block_count.values()) + strategy.cash_blocks
        assert total_blocks == strategy.num_blocks, (
            f"Block invariant violated: {strategy.block_count} + "
            f"cash={strategy.cash_blocks} != {strategy.num_blocks}"
        )

        # 检查所有完成的 Block 日志状态合理
        for log in strategy._block_logs:
            assert log.state in (
                BlockState.DONE,
                BlockState.TIMEOUT,
                BlockState.CRITICAL,
            ), f"Block {log.block_id} in unexpected state {log.state}"

    def test_block_count_invariant(self):
        """block_count + cash_blocks = num_blocks（始终成立）。"""
        engine = self._make_engine()
        symbols = ["510300.SH", "510330.SH", "510350.SH"]
        strategy = MstrStrategy(
            engine,
            "mstr_invariant",
            symbols,
            setting={
                "dataset_dir": str(DATASET_DIR),
                "window": 20,
                "k_threshold": 0.5,
                "least_bias": 0.001,
                "num_blocks": 3,
                "sub_lots": 2,
                "cooldown_1": 3,
                "cooldown_2": 5,
            },
        )

        result = engine.run(
            strategy,
            start_date="20240301",
            end_date="20240320",
        )

        total = sum(strategy.block_count.values()) + strategy.cash_blocks
        assert total == strategy.num_blocks


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
