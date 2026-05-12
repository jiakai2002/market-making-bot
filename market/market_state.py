class MarketState:
    def __init__(self, orderbook, trades):
        self.orderbook = orderbook
        self.trades = trades

    def snapshot(self):
        best_bid = self.orderbook.best_bid
        best_ask = self.orderbook.best_ask

        return {
            "mid": self.orderbook.mid_price,
            "spread": self.orderbook.spread,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "top_bids": list(self.orderbook.bids.items())[:5],
            "top_asks": list(self.orderbook.asks.items())[:5],
            "recent_trades": self.trades.recent(10),
        }