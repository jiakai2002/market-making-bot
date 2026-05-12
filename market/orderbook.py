import aiohttp
from collections import deque


class OrderBook:
    def __init__(self, symbol: str, max_levels=20):
        self.symbol = symbol.upper()
        self.max_levels = max_levels

        self.bids = {}
        self.asks = {}

        self.last_update_id = None
        self.synced = False
        self.buffer = deque(maxlen=5000)

    async def initialize(self):
        snapshot = await self.fetch_snapshot()

        self.last_update_id = snapshot["lastUpdateId"]

        self.load_snapshot(snapshot)

        self.synced = True

        buffered = list(self.buffer)
        self.buffer = deque(maxlen=5000)

        for msg in buffered:
            self.process_message(msg)

        print("Orderbook synchronized.")

    async def fetch_snapshot(self):
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit=1000"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.json()

    def load_snapshot(self, snapshot):
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}

        self.sort_books()

    def sort_books(self):
        self.bids = dict(sorted(self.bids.items(), reverse=True)[:self.max_levels])
        self.asks = dict(sorted(self.asks.items())[:self.max_levels])

    def update_side(self, side, updates):
        for price_str, qty_str in updates:
            price = float(price_str)
            qty = float(qty_str)

            if qty == 0:
                side.pop(price, None)
            else:
                side[price] = qty

    def process_message(self, data):
        if not self.synced:
            self.buffer.append(data)
            return

        U = data["U"]
        u = data["u"]

        if u <= self.last_update_id:
            return

        if U > self.last_update_id + 1:
            raise RuntimeError("Orderbook gap detected. Resync required.")

        self.update_side(self.bids, data["b"])
        self.update_side(self.asks, data["a"])

        self.last_update_id = u

        self.sort_books()

    @property
    def best_bid(self):
        if not self.bids:
            return None

        return next(iter(self.bids.items()))

    @property
    def best_ask(self):
        if not self.asks:
            return None

        return next(iter(self.asks.items()))

    @property
    def mid_price(self):
        if not self.best_bid or not self.best_ask:
            return None

        return (self.best_bid[0] + self.best_ask[0]) / 2

    @property
    def spread(self):
        if not self.best_bid or not self.best_ask:
            return None

        return self.best_ask[0] - self.best_bid[0]