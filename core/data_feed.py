"""
数据源适配器 — 从 Parquet 文件加载行情数据。

ParquetBarFeed:  读取 dataset/daily/{symbol}/*.parquet → BarData 列表
ParquetTickFeed: 读取 dataset/ticks/{symbol}/{YYYY-MM}/{YYYYMMDD}.parquet → TickData 列表

设计要点:
  - 按需加载、延迟读取，不一次性加载所有品种的全部 tick
  - 返回标准 TickData / BarData 对象，与 core/datatypes.py 对齐
  - 日期范围过滤 [start_date, end_date]（闭区间）
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from core.datatypes import BarData, TickData


# ════════════════════════════════════════════════
#  日线数据源
# ════════════════════════════════════════════════


class ParquetBarFeed:
    """
    从 dataset/daily/{symbol}/ 读取日线 Parquet。

    文件命名: {数字}.parquet — 通常按年分文件 (如 2026.parquet)。
    Parquet schema: ts_code, trade_date(str YYYYMMDD), open, high, low, close,
        pre_close, change, pct_chg, adj_factor, open_bwd, high_bwd, low_bwd,
        close_bwd, pre_close_bwd, volume, turnover, name
    """

    def __init__(self, dataset_dir: str | Path) -> None:
        """
        Parameters
        ----------
        dataset_dir : str | Path
            dataset 根目录，包含 daily/ 和 ticks/ 子目录。
        """
        self._root = Path(dataset_dir) / "daily"

    def load(
        self,
        symbol: str,
        start_date: str = "",
        end_date: str = "",
    ) -> list[BarData]:
        """
        加载日线数据。

        Parameters
        ----------
        symbol : str
            品种代码，如 "510300.SH"。
        start_date : str
            起始日期 YYYYMMDD（含），空字符串表示不限。
        end_date : str
            截止日期 YYYYMMDD（含），空字符串表示不限。

        Returns
        -------
        list[BarData]
            按日期升序排列。
        """
        symbol_dir = self._root / symbol
        if not symbol_dir.is_dir():
            return []

        dfs = []
        for fp in sorted(symbol_dir.glob("*.parquet")):
            df = pq.read_table(fp).to_pandas()
            dfs.append(df)

        if not dfs:
            return []

        df = pd.concat(dfs, ignore_index=True)
        df.sort_values("trade_date", inplace=True)

        if start_date:
            df = df[df["trade_date"] >= start_date]
        if end_date:
            df = df[df["trade_date"] <= end_date]

        bars: list[BarData] = []
        for _, row in df.iterrows():
            dt = datetime.strptime(row["trade_date"], "%Y%m%d")
            bar = BarData(
                symbol=symbol,
                datetime=dt,
                open_price=float(row.get("open_bwd", row["open"])),
                high_price=float(row.get("high_bwd", row["high"])),
                low_price=float(row.get("low_bwd", row["low"])),
                close_price=float(row.get("close_bwd", row["close"])),
                pre_close=float(row.get("pre_close_bwd", row["pre_close"])),
                volume=float(row["volume"]),
                turnover=float(row["turnover"]),
                raw_open=float(row["open"]),
                raw_high=float(row["high"]),
                raw_low=float(row["low"]),
                raw_close=float(row["close"]),
                adj_factor=float(row.get("adj_factor", 1.0)),
                pct_chg=float(row.get("pct_chg", 0.0)),
                name=str(row.get("name", "")),
            )
            bars.append(bar)

        return bars

    def get_trading_dates(
        self,
        symbol: str,
        start_date: str = "",
        end_date: str = "",
    ) -> list[str]:
        """返回交易日列表 (YYYYMMDD 字符串)。"""
        symbol_dir = self._root / symbol
        if not symbol_dir.is_dir():
            return []

        dfs = []
        for fp in sorted(symbol_dir.glob("*.parquet")):
            df = pq.read_table(fp, columns=["trade_date"]).to_pandas()
            dfs.append(df)

        if not dfs:
            return []

        df = pd.concat(dfs, ignore_index=True)
        dates = sorted(df["trade_date"].unique())

        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]

        return dates


# ════════════════════════════════════════════════
#  Tick 数据源
# ════════════════════════════════════════════════


class ParquetTickFeed:
    """
    从 dataset/ticks/{symbol}/{YYYY-MM}/{YYYYMMDD}.parquet 读取 Tick 数据。

    Parquet schema: 58 列，bid/ask 交错排列 (bid_price_N, ask_price_N,
    bid_volume_N, ask_volume_N)。
    """

    def __init__(self, dataset_dir: str | Path) -> None:
        self._root = Path(dataset_dir) / "ticks"

    def load_day(self, symbol: str, date: str) -> list[TickData]:
        """
        加载单日 Tick 数据。

        Parameters
        ----------
        symbol : str
            品种代码 "510300.SH"。
        date : str
            日期 YYYYMMDD，如 "20250102"。

        Returns
        -------
        list[TickData]
            按时间升序排列。
        """
        # date YYYYMMDD → YYYY-MM 目录
        month_dir = f"{date[:4]}-{date[4:6]}"
        fp = self._root / symbol / month_dir / f"{date}.parquet"
        if not fp.exists():
            return []

        df = pq.read_table(fp).to_pandas()
        df.sort_values("datetime", inplace=True)

        return self._df_to_ticks(df, symbol)

    def load_range(
        self,
        symbol: str,
        start_date: str = "",
        end_date: str = "",
    ) -> list[TickData]:
        """
        加载日期范围内所有 Tick 数据（可能很大，谨慎使用）。

        Returns
        -------
        list[TickData]
            按时间升序排列。
        """
        symbol_dir = self._root / symbol
        if not symbol_dir.is_dir():
            return []

        ticks: list[TickData] = []
        for month_dir in sorted(symbol_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.glob("*.parquet")):
                date_str = fp.stem  # YYYYMMDD
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue

                df = pq.read_table(fp).to_pandas()
                df.sort_values("datetime", inplace=True)
                ticks.extend(self._df_to_ticks(df, symbol))

        return ticks

    def get_available_dates(self, symbol: str) -> list[str]:
        """返回该品种可用的所有 Tick 日期 (YYYYMMDD)。"""
        symbol_dir = self._root / symbol
        if not symbol_dir.is_dir():
            return []

        dates = []
        for month_dir in sorted(symbol_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.glob("*.parquet")):
                dates.append(fp.stem)

        return dates

    @staticmethod
    def _df_to_ticks(df: pd.DataFrame, symbol: str) -> list[TickData]:
        """DataFrame → TickData 列表。使用 to_dict('records') 代替 iterrows 提速。"""
        ticks: list[TickData] = []

        for row in df.to_dict("records"):
            dt = row["datetime"]
            if isinstance(dt, pd.Timestamp):
                dt = dt.to_pydatetime()

            tick = TickData(
                symbol=symbol,
                datetime=dt,
                last_price=float(row["last_price"]),
                cum_volume=int(row["cum_volume"]),
                cum_turnover=float(row["cum_turnover"]),
                volume=float(row["volume"]),
                turnover=float(row["turnover"]),
                open_price=float(row["open_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                pre_close=float(row["pre_close"]),
                trades_count=int(row["trades_count"]),
                bs_flag=str(row.get("bs_flag", "")),
                trade_flag=str(row.get("trade_flag", "")),
                iopv=int(row.get("iopv", 0)),
                weighted_avg_ask_price=float(row.get("weighted_avg_ask_price", 0)),
                weighted_avg_bid_price=float(row.get("weighted_avg_bid_price", 0)),
                total_ask_volume=int(row.get("total_ask_volume", 0)),
                total_bid_volume=int(row.get("total_bid_volume", 0)),
                bid_price_1=float(row["bid_price_1"]),
                ask_price_1=float(row["ask_price_1"]),
                bid_volume_1=int(row["bid_volume_1"]),
                ask_volume_1=int(row["ask_volume_1"]),
                bid_price_2=float(row["bid_price_2"]),
                ask_price_2=float(row["ask_price_2"]),
                bid_volume_2=int(row["bid_volume_2"]),
                ask_volume_2=int(row["ask_volume_2"]),
                bid_price_3=float(row["bid_price_3"]),
                ask_price_3=float(row["ask_price_3"]),
                bid_volume_3=int(row["bid_volume_3"]),
                ask_volume_3=int(row["ask_volume_3"]),
                bid_price_4=float(row["bid_price_4"]),
                ask_price_4=float(row["ask_price_4"]),
                bid_volume_4=int(row["bid_volume_4"]),
                ask_volume_4=int(row["ask_volume_4"]),
                bid_price_5=float(row["bid_price_5"]),
                ask_price_5=float(row["ask_price_5"]),
                bid_volume_5=int(row["bid_volume_5"]),
                ask_volume_5=int(row["ask_volume_5"]),
                bid_price_6=float(row["bid_price_6"]),
                ask_price_6=float(row["ask_price_6"]),
                bid_volume_6=int(row["bid_volume_6"]),
                ask_volume_6=int(row["ask_volume_6"]),
                bid_price_7=float(row["bid_price_7"]),
                ask_price_7=float(row["ask_price_7"]),
                bid_volume_7=int(row["bid_volume_7"]),
                ask_volume_7=int(row["ask_volume_7"]),
                bid_price_8=float(row["bid_price_8"]),
                ask_price_8=float(row["ask_price_8"]),
                bid_volume_8=int(row["bid_volume_8"]),
                ask_volume_8=int(row["ask_volume_8"]),
                bid_price_9=float(row["bid_price_9"]),
                ask_price_9=float(row["ask_price_9"]),
                bid_volume_9=int(row["bid_volume_9"]),
                ask_volume_9=int(row["ask_volume_9"]),
                bid_price_10=float(row["bid_price_10"]),
                ask_price_10=float(row["ask_price_10"]),
                bid_volume_10=int(row["bid_volume_10"]),
                ask_volume_10=int(row["ask_volume_10"]),
            )
            ticks.append(tick)

        return ticks
