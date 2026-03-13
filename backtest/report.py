"""
绩效报告模块 — 从 BacktestResult 计算指标并绘制图表。

用法::

    from backtest.engine import BacktestEngine
    from backtest.report import BacktestReport

    result = engine.run(strategy, "20250101", "20251231")
    report = BacktestReport(result)
    report.print_summary()
    report.show_charts()            # 交互显示
    report.show_charts("out.png")   # 保存到文件

指标说明
--------
- 总收益率 / 年化收益率
- 年化波动率
- 最大回撤
- Sharpe Ratio  (日收益超额 / 日收益标准差 × √252)
- Sortino Ratio (日收益超额 / 下行标准差 × √252)
- Calmar Ratio  (年化收益 / 最大回撤绝对值)
- 胜率          (日收益 > 0 的天数占比)
- 盈亏比        (盈利日均收益 / 亏损日均收益绝对值)
- 最长连续亏损天数
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec

if TYPE_CHECKING:
    from backtest.engine import BacktestResult

# 每年交易日
_TRADING_DAYS = 252


def _setup_chinese_font() -> None:
    """尝试设置 matplotlib 中文字体（Windows 优先 Microsoft YaHei）。"""
    import matplotlib
    candidates = ["Microsoft YaHei", "SimHei", "STSong", "SimSun"]
    import matplotlib.font_manager as fm
    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidates:
        if font in available:
            matplotlib.rcParams["font.family"] = font
            break
    matplotlib.rcParams["axes.unicode_minus"] = False


class BacktestReport:
    """
    回测绩效报告。

    Parameters
    ----------
    result : BacktestResult
        BacktestEngine.run() 的返回值。
    risk_free_rate : float
        年化无风险利率，默认 0.02（2%）。
    """

    def __init__(
        self,
        result: "BacktestResult",
        risk_free_rate: float = 0.02,
    ) -> None:
        self.result = result
        self.risk_free_rate = risk_free_rate
        self._nav_df: pd.DataFrame = self._build_nav_df()
        self.stats: dict = self._compute_stats()

    # ════════════════════════════════════════════════
    #  数据准备
    # ════════════════════════════════════════════════

    def _build_nav_df(self) -> pd.DataFrame:
        """构建含日期索引的净值 DataFrame，计算日收益率和回撤。"""
        if not self.result.daily_nav:
            return pd.DataFrame(columns=["nav", "return", "drawdown"])

        rows = [(pd.Timestamp(d), nav) for d, nav in self.result.daily_nav]
        df = pd.DataFrame(rows, columns=["date", "nav"]).set_index("date")

        # 日收益率第一天相对初始资金
        initial = self.result.start_balance
        if initial > 0:
            prev_series = pd.Series(
                [initial],
                index=[df.index[0] - pd.Timedelta(days=1)],
                name="nav",
            )
            nav_full = pd.concat([prev_series, df["nav"]])
            df["return"] = nav_full.pct_change().iloc[1:].values
        else:
            # 无初始资金（全持仓模式）：第一天收益设为 0
            df["return"] = df["nav"].pct_change().fillna(0.0)

        # 回撤
        peak = df["nav"].cummax()
        df["drawdown"] = (df["nav"] - peak) / peak

        return df

    # ════════════════════════════════════════════════
    #  指标计算
    # ════════════════════════════════════════════════

    def _compute_stats(self) -> dict:
        df = self._nav_df
        s: dict = {}
        n = len(df)

        # ── 基础信息 ──
        s["start_balance"] = self.result.start_balance
        s["end_balance"] = self.result.end_balance
        s["total_commission"] = self.result.total_commission
        s["trade_count"] = len(self.result.trades)
        s["trading_days"] = n

        if n == 0:
            return s

        # ── 收益 ──
        initial = (
            self.result.start_balance
            if self.result.start_balance > 0
            else df["nav"].iloc[0]
        )
        s["total_return"] = (
            (self.result.end_balance - initial) / initial if initial > 0 else 0.0
        )
        s["annual_return"] = (
            (1 + s["total_return"]) ** (_TRADING_DAYS / max(n, 1)) - 1
        )

        # ── 风险 ──
        s["annual_volatility"] = float(
            df["return"].std() * math.sqrt(_TRADING_DAYS)
        )
        s["max_drawdown"] = float(df["drawdown"].min())  # 负数

        # ── Sharpe ──
        rf_daily = (1 + self.risk_free_rate) ** (1 / _TRADING_DAYS) - 1
        excess = df["return"] - rf_daily
        excess_std = float(excess.std())
        s["sharpe"] = (
            float(excess.mean() / excess_std * math.sqrt(_TRADING_DAYS))
            if excess_std > 1e-10 else 0.0
        )

        # ── Sortino ──
        downside = excess[excess < 0]
        downside_std = (
            math.sqrt(float((downside ** 2).mean())) if len(downside) > 0 else 0.0
        )
        s["sortino"] = (
            float(excess.mean() * math.sqrt(_TRADING_DAYS) / downside_std)
            if downside_std > 1e-10 else 0.0
        )

        # ── Calmar ──
        s["calmar"] = (
            s["annual_return"] / abs(s["max_drawdown"])
            if s["max_drawdown"] < -1e-10 else 0.0
        )

        # ── 胜率 & 盈亏比（基于日收益）──
        wins = df["return"][df["return"] > 0]
        losses = df["return"][df["return"] < 0]
        s["win_rate"] = len(wins) / n
        s["profit_loss_ratio"] = (
            float(wins.mean() / abs(losses.mean()))
            if len(losses) > 0 and abs(float(losses.mean())) > 1e-10 else 0.0
        )

        # ── 最长连续亏损天数 ──
        streak = max_streak = 0
        for r in df["return"]:
            if r < 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        s["max_loss_streak"] = max_streak

        return s

    # ════════════════════════════════════════════════
    #  文字摘要
    # ════════════════════════════════════════════════

    def print_summary(self) -> None:
        """打印文字绩效摘要到 stdout。"""
        s = self.stats
        n = s.get("trading_days", 0)

        print()
        print("═" * 56)
        print("  回测绩效报告")
        print("═" * 56)
        print(f"  {'初始资金':<14}  {s.get('start_balance', 0):>16,.2f}")
        print(f"  {'期末净值':<14}  {s.get('end_balance', 0):>16,.2f}")
        print(f"  {'总手续费':<14}  {s.get('total_commission', 0):>16,.2f}")
        print(f"  {'成交笔数':<14}  {s.get('trade_count', 0):>16,}")
        print(f"  {'回测交易日':<14}  {n:>16}")
        print("─" * 56)
        print(f"  {'总收益率':<14}  {s.get('total_return', 0):>15.2%}")
        print(f"  {'年化收益率':<14}  {s.get('annual_return', 0):>15.2%}")
        print(f"  {'年化波动率':<14}  {s.get('annual_volatility', 0):>15.2%}")
        print(f"  {'最大回撤':<14}  {s.get('max_drawdown', 0):>15.2%}")
        print("─" * 56)
        print(f"  {'Sharpe Ratio':<14}  {s.get('sharpe', 0):>16.3f}")
        print(f"  {'Sortino Ratio':<14}  {s.get('sortino', 0):>16.3f}")
        print(f"  {'Calmar Ratio':<14}  {s.get('calmar', 0):>16.3f}")
        print("─" * 56)
        print(f"  {'胜率 (日)':<14}  {s.get('win_rate', 0):>15.2%}")
        plr = s.get('profit_loss_ratio', 0)
        plr_str = f"{plr:>16.3f}" if not math.isnan(plr) else f"{'—':>16}"
        print(f"  {'盈亏比':<14}  {plr_str}")
        print(f"  {'最长连亏天数':<14}  {s.get('max_loss_streak', 0):>16}")
        print("═" * 56)

    # ════════════════════════════════════════════════
    #  图表
    # ════════════════════════════════════════════════

    def show_charts(self, save_path: str | None = None) -> None:
        """
        绘制权益曲线、回撤曲线、月度收益热力图。

        Parameters
        ----------
        save_path : str | None
            指定路径则保存文件（PNG/PDF），否则交互显示。
        """
        if save_path:
            plt.switch_backend("Agg")  # 非交互后端，避免 Qt/display 冲突，必须在 plt 导入后调用
        _setup_chinese_font()

        df = self._nav_df
        if df.empty:
            print("[Report] 无净值数据，跳过绘图")
            return

        has_heatmap = self._has_monthly_data(df)
        n_rows = 3 if has_heatmap else 2

        fig = plt.figure(figsize=(14, 4 * n_rows))
        gs = GridSpec(n_rows, 1, figure=fig, hspace=0.45)

        ax1 = fig.add_subplot(gs[0])
        self._plot_equity(ax1, df)

        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        self._plot_drawdown(ax2, df)

        if has_heatmap:
            ax3 = fig.add_subplot(gs[2])
            self._plot_monthly_heatmap(ax3, df)

        fig.suptitle("回测绩效报告", fontsize=14, fontweight="bold", y=0.995)

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"[Report] 图表已保存至 {save_path}")
        else:
            plt.show()

        plt.close(fig)

    # ── 子图 ─────────────────────────────────────────

    def _plot_equity(self, ax: plt.Axes, df: pd.DataFrame) -> None:
        ax.plot(df.index, df["nav"], color="#2196F3", linewidth=1.5, label="净值")
        ax.fill_between(df.index, df["nav"], df["nav"].iloc[0], alpha=0.08, color="#2196F3")
        ax.set_title("权益曲线")
        ax.set_ylabel("净值 (元)")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
        )
        ax.legend(loc="upper left", fontsize=9)
        ax.grid(True, alpha=0.3)

    def _plot_drawdown(self, ax: plt.Axes, df: pd.DataFrame) -> None:
        dd_pct = df["drawdown"] * 100
        ax.fill_between(df.index, dd_pct, 0, color="#F44336", alpha=0.55, label="回撤")
        ax.plot(df.index, dd_pct, color="#F44336", linewidth=0.8)
        max_dd = self.stats.get("max_drawdown", 0)
        ax.set_title(f"回撤曲线  (最大回撤 {max_dd:.2%})")
        ax.set_ylabel("回撤 (%)")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:.1f}%")
        )
        ax.legend(loc="lower left", fontsize=9)
        ax.grid(True, alpha=0.3)

    @staticmethod
    def _has_monthly_data(df: pd.DataFrame) -> bool:
        if df.empty:
            return False
        monthly = df["nav"].resample("ME").last().dropna()
        return len(monthly) >= 2

    def _plot_monthly_heatmap(self, ax: plt.Axes, df: pd.DataFrame) -> None:
        monthly_nav = df["nav"].resample("ME").last().dropna()
        monthly_ret = monthly_nav.pct_change().dropna()

        pivoted = pd.DataFrame({
            "year": monthly_ret.index.year,
            "month": monthly_ret.index.month,
            "ret": monthly_ret.values,
        })
        table = pivoted.pivot_table(
            values="ret", index="year", columns="month", aggfunc="first"
        )

        month_labels = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
        existing = sorted(table.columns.tolist())
        table = table[existing]
        col_labels = [month_labels[m - 1] for m in existing]

        data = table.values.astype(float)
        valid = data[~np.isnan(data)]
        vmax = max(abs(valid).max(), 0.001) if len(valid) > 0 else 0.01

        im = ax.imshow(
            data, cmap="RdYlGn", vmin=-vmax, vmax=vmax, aspect="auto"
        )
        plt.colorbar(
            im, ax=ax,
            format=mticker.FuncFormatter(lambda x, _: f"{x:.1%}"),
            shrink=0.8,
        )

        ax.set_xticks(range(len(col_labels)))
        ax.set_xticklabels(col_labels)
        ax.set_yticks(range(len(table.index)))
        ax.set_yticklabels(table.index)

        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                val = data[i, j]
                if not np.isnan(val):
                    text_color = "black" if abs(val) < vmax * 0.65 else "white"
                    ax.text(
                        j, i, f"{val:.1%}",
                        ha="center", va="center",
                        fontsize=8, color=text_color,
                    )

        ax.set_title("月度收益热力图")
