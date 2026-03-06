from vnpy.trader.database import get_database
from vnpy.trader.object import BarData, Exchange, Interval
from datetime import datetime
import csv
import pytz

def load_data_002253(file_path):
    database = get_database()
    
    bars = []
    symbol = "002253"
    exchange = Exchange.SZSE
    interval = Interval.MINUTE # Using 5-min data, but mapped to Interval.MINUTE type for compatibility if needed, or better, vnpy handles it.
    # Note: vnpy's Interval enum has MINUTE, HOUR, DAILY, WEEKLY, TICK. 
    # It doesn't have explicit "5m". Usually we store it as MINUTE and strategy handles window size, or we just store it.
    
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip header
        
        for row in reader:
            # Baostock format: date,time,code,open,high,low,close,volume,amount,adjustflag
            # time example: 20250102093500000 -> YYYYMMDDHHMMSSmmm
            time_str = row[1] 
            code = row[2] # sz.002253
            
            open_p = float(row[3])
            high_p = float(row[4])
            low_p = float(row[5])
            close_p = float(row[6])
            volume = float(row[7])
            
            # Parse datetime: 20250102093500000
            # We take first 14 chars: 20250102093500
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            # Set timezone to local/China
            dt = dt.replace(tzinfo=pytz.timezone("Asia/Shanghai"))

            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=volume,
                open_price=open_p,
                high_price=high_p,
                low_price=low_p,
                close_price=close_p,
                gateway_name="DB"
            )
            bars.append(bar)
    
    if bars:
        database.save_bar_data(bars)
        print(f"Loaded {len(bars)} bars into database for {symbol}.")
    else:
        print("No bars loaded.")

if __name__ == "__main__":
    load_data_002253("sz.002253_2025-01-01_2025-12-31_5min.csv")
