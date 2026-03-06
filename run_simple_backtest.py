from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.object import Interval, Exchange
from datetime import datetime
from vnpy_ctastrategy.strategies.double_ma_strategy import DoubleMaStrategy

def run_backtest():
    engine = BacktestingEngine()
    
    engine.set_parameters(
        vt_symbol="002253.SZSE",
        interval=Interval.MINUTE,
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31),
        rate=0.3/10000,
        slippage=0.2,
        size=100, # Stock size is usually 100 or 1
        pricetick=0.01, # Stock price tick
        capital=1_000_000,
    )
    
    engine.add_strategy(DoubleMaStrategy, {})
    
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()
    # engine.show_chart() # Requires GUI

if __name__ == "__main__":
    run_backtest()
