import random
from dataclasses import dataclass


@dataclass
class Order:
    id: int
    side: str
    price: float
    size: float
    filled: float = 0.0
    status: str = "open"


@dataclass
class Fill:
    order_id: int
    side: str
    price: float
    size: float
    fee: float
    timestamp: int


class Exchange:
    def __init__(self, fee_bps: float = 1.5, fill_prob: float = 0.7):
        self.fee = fee_bps / 10_000
        self.fill_prob = fill_prob

        self.orders: dict[int, Order] = {}
        self.fills: list[Fill] = []
        self.next_id = 0

        self.cash_flow = 0.0

    def place_limit_order(self, side: str, price: float, size: float, timestamp: int) -> int:
        oid = self.next_id
        self.next_id += 1

        self.orders[oid] = Order(
            id=oid,
            side=side,
            price=price,
            size=size,
        )
        return oid

    def cancel_all(self):
        self.orders.clear()

    def check_fills(self, row: dict) -> list[Fill]:
        best_bid = row["bid_0_price"]
        best_ask = row["ask_0_price"]
        ts = row["timestamp"]

        new_fills = []

        for oid in list(self.orders.keys()):
            o = self.orders.get(oid)
            if not o:
                continue

            # ---- CROSSING LOGIC ----
            crossed = (
                (o.side == "buy" and best_ask <= o.price) or
                (o.side == "sell" and best_bid >= o.price)
            )

            if not crossed:
                continue

            # ---- PROBABILISTIC FILL ----
            if random.random() > self.fill_prob:
                continue

            fill_size = o.size - o.filled

            fee = fill_size * o.price * self.fee

            # ---- CASHFLOW UPDATE ----
            if o.side == "buy":
                self.cash_flow -= fill_size * o.price + fee
            else:
                self.cash_flow += fill_size * o.price - fee

            fill = Fill(
                order_id=o.id,
                side=o.side,
                price=o.price,
                size=fill_size,
                fee=fee,
                timestamp=ts,
            )

            self.fills.append(fill)
            new_fills.append(fill)

            o.filled += fill_size
            o.status = "filled"

            del self.orders[oid]

        return new_fills

    @property
    def inventory(self) -> float:
        return sum(
            f.size if f.side == "buy" else -f.size
            for f in self.fills
        )

    def pnl(self, mid: float) -> float:
        return self.cash_flow + self.inventory * mid

    def summary(self) -> dict:
        return {
            "fills": len(self.fills),
            "open_orders": len(self.orders),
            "fees": sum(f.fee for f in self.fills),
            "inventory": self.inventory,
        }