import math
import random
from dataclasses import dataclass
from config import FEE_BPS, FILL_K


@dataclass
class Order:
    id:          int
    side:        str
    price:       float
    size:        float
    queue_depth: float = 0.0


@dataclass
class Fill:
    order_id:  int
    side:      str
    price:     float
    size:      float
    fee:       float
    timestamp: int


class Exchange:
    def __init__(self):
        self.fee        = FEE_BPS / 10_000
        self.fill_k     = FILL_K
        self.orders:    dict[int, Order] = {}
        self.fills:     list[Fill]       = []
        self.next_id    = 0
        self.cash_flow  = 0.0
        self.order_count = 0

    def place_limit_order(self, side: str, price: float, size: float, timestamp: int, queue_depth: float = 0.0) -> int:
        oid = self.next_id
        self.next_id    += 1
        self.order_count += 1
        self.orders[oid] = Order(id=oid, side=side, price=price, size=size, queue_depth=queue_depth)
        return oid

    def cancel_all(self):
        self.orders.clear()

    def _fill_prob(self, order: Order, best_bid: float, best_ask: float) -> float:
        # Queue position model with 50% queue assumption
        queue_ahead = max(order.queue_depth * 0.5, 1e-8)
        mid = (best_bid + best_ask) / 2
        expected_vol = self.fill_k * mid

        return 1.0 - math.exp(-expected_vol / queue_ahead)

    def check_fills(self, row: dict) -> list[Fill]:
        best_bid = row["bid_0_price"]
        best_ask = row["ask_0_price"]
        ts       = row["timestamp"]
        new_fills = []

        for oid in list(self.orders):
            o = self.orders.get(oid)
            if not o:
                continue

            crossed = (
                (o.side == "buy"  and best_ask <= o.price) or
                (o.side == "sell" and best_bid >= o.price)
            )
            if not crossed:
                continue

            if random.random() > self._fill_prob(o, best_bid, best_ask):
                continue

            fee = o.size * o.price * self.fee

            if o.side == "buy":
                self.cash_flow -= o.size * o.price + fee
            else:
                self.cash_flow += o.size * o.price - fee

            fill = Fill(order_id=o.id, side=o.side, price=o.price,
                        size=o.size, fee=fee, timestamp=ts)
            self.fills.append(fill)
            new_fills.append(fill)
            del self.orders[oid]

        return new_fills

    @property
    def inventory(self) -> float:
        return sum(f.size if f.side == "buy" else -f.size for f in self.fills)

    def pnl(self, mid: float) -> float:
        return self.cash_flow + self.inventory * mid

    def summary(self) -> dict:
        return {
            "fills":       len(self.fills),
            "orders":      self.order_count,
            "open_orders": len(self.orders),
            "fees":        sum(f.fee for f in self.fills),
            "inventory":   self.inventory,
        }