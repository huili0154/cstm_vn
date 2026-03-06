from dataclasses import dataclass

@dataclass
class Tick:
    datetime: str
    last_price: float
    ask_price_1: float
    ask_volume_1: int
    bid_price_1: float
    bid_volume_1: int
    volume: int  # Cumulative volume

@dataclass
class Order:
    price: float
    volume: int
    direction: str  # "LONG" or "SHORT"
    status: str = "PENDING"
    queue_position: int = 0  # Volume ahead of us in the queue

class QueueSimulationEngine:
    def __init__(self):
        self.orders = []
        self.last_tick_volume = 0

    def place_limit_order(self, tick: Tick, price: float, volume: int, direction: str):
        order = Order(price=price, volume=volume, direction=direction)
        
        # Determine initial queue position
        if direction == "LONG":
            # Buying: If price >= Ask1, we might cross spread (taker), immediate match logic usually.
            # But if we place at Bid1 (maker), we join the queue.
            if price == tick.bid_price_1:
                order.queue_position = tick.bid_volume_1
            else:
                # Placing inside spread or better, assume front of queue (simplified)
                order.queue_position = 0
        elif direction == "SHORT":
            # Selling: Join Ask queue
            if price == tick.ask_price_1:
                order.queue_position = tick.ask_volume_1
            else:
                order.queue_position = 0
                
        self.orders.append(order)
        print(f"[{tick.datetime}] Placed {direction} Order @ {price}, Volume: {volume}. Queue Position: {order.queue_position}")

    def on_tick(self, tick: Tick):
        # Calculate volume traded since last tick
        traded_volume = tick.volume - self.last_tick_volume if self.last_tick_volume > 0 else 0
        self.last_tick_volume = tick.volume
        
        # Simplified: Assume traded volume is distributed equally to Buy/Sell sides or based on price move
        # For this demo, we assume all traded volume consumes the queue at the current best price.
        
        for order in self.orders:
            if order.status != "PENDING":
                continue
                
            # Update Queue Position
            if order.direction == "SHORT" and tick.ask_price_1 == order.price:
                # If market is still trading at our price
                order.queue_position -= traded_volume
                print(f"  [{tick.datetime}] Tick Update: Traded {traded_volume}. Order Queue remaining: {max(0, order.queue_position)}")
                
                if order.queue_position <= 0:
                    order.status = "FILLED"
                    print(f"  >>> ORDER FILLED @ {order.price} <<<")

def run_demo():
    engine = QueueSimulationEngine()
    
    # Simulated Tick Stream for a Stock (e.g. 10.00 Ask)
    ticks = [
        Tick("09:30:00", 10.00, 10.00, 5000, 9.99, 2000, 10000), # Initial State
        Tick("09:30:03", 10.00, 10.00, 4800, 9.99, 2200, 10200), # Traded 200
        Tick("09:30:06", 10.00, 10.00, 4000, 9.99, 2500, 11000), # Traded 800
        Tick("09:30:09", 10.00, 10.00, 1000, 9.99, 3000, 14000), # Traded 3000
        Tick("09:30:12", 10.01, 10.01, 2000, 10.00, 500, 16000), # Price moved up! Ask is now 10.01
    ]
    
    # 1. Initial State
    t0 = ticks[0]
    engine.last_tick_volume = t0.volume
    # Place a Sell Order at 10.00 (Current Ask1)
    # There are already 5000 volume ahead of us.
    engine.place_limit_order(t0, 10.00, 1000, "SHORT")
    
    # 2. Process subsequent ticks
    for t in ticks[1:]:
        engine.on_tick(t)

if __name__ == "__main__":
    run_demo()
