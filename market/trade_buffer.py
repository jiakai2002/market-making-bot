from collections import deque


class TradeBuffer:
    def __init__(self, max_trades=100):
        self.trades = deque(maxlen=max_trades)

    def add_trade(self, data):
        trade = {
            "price": float(data["p"]),
            "qty": float(data["q"]),
            "side": "SELL" if data["m"] else "BUY",
            "ts": data["T"],
        }

        self.trades.appendleft(trade)

    def recent(self, n=10):
        return list(self.trades)[:n]