from vnpy.trader.database import get_database
from vnpy.trader.object import BarData, Exchange, Interval
from datetime import datetime
import csv
import pytz

def load_data(file_path):
    database = get_database()
    
    bars = []
    symbol = "TEST"
    exchange = Exchange.SSE
    interval = Interval.MINUTE
    
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip header
        
        for row in reader:
            dt_str = row[0]
            open_p = float(row[1])
            high_p = float(row[2])
            low_p = float(row[3])
            close_p = float(row[4])
            volume = float(row[5])
            
            # Simple datetime parsing
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
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
        print(f"Loaded {len(bars)} bars into database.")
    else:
        print("No bars loaded.")

if __name__ == "__main__":
    load_data("mock_data_2025.csv")
