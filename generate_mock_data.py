import csv
import random
from datetime import datetime, timedelta

def generate_mock_data(file_path, symbol="TEST", exchange="SSE", start_date="20250101", days=10):
    """
    Generate mock 1-minute bar data for testing.
    Generating second-level data for a whole year is too large (millions of rows).
    We will generate 1-minute data which is standard for backtesting.
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = start + timedelta(days=days)
    
    current = start
    price = 100.0
    
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["datetime", "open", "high", "low", "close", "volume"])
        
        while current < end:
            # Trading hours: 9:30-11:30, 13:00-15:00
            if (current.hour == 9 and current.minute >= 30) or \
               (current.hour == 10) or \
               (current.hour == 11 and current.minute <= 30) or \
               (current.hour >= 13 and current.hour < 15):
                
                # Random walk
                change = random.uniform(-0.5, 0.5)
                open_p = price
                close_p = price + change
                high_p = max(open_p, close_p) + random.uniform(0, 0.2)
                low_p = min(open_p, close_p) - random.uniform(0, 0.2)
                volume = random.randint(100, 1000)
                
                writer.writerow([
                    current.strftime("%Y-%m-%d %H:%M:%S"),
                    f"{open_p:.2f}",
                    f"{high_p:.2f}",
                    f"{low_p:.2f}",
                    f"{close_p:.2f}",
                    volume
                ])
                
                price = close_p
            
            current += timedelta(minutes=1)

if __name__ == "__main__":
    generate_mock_data("mock_data_2025.csv", days=30) # Generate 1 month of data
    print("Generated mock_data_2025.csv")
