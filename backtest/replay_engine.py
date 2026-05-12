import pandas as pd

from strategy.strategy import Strategy
from execution.exchange import Exchange


class ReplayEngine:
    def __init__(self):
        self.strategy = Strategy()
        self.exchange = Exchange(fee_bps=1.5, fill_prob=0.7)
        self.orders   = 0
        self.fills    = 0

    def run(self, parquet_path: str) -> pd.DataFrame:
        df = pd.read_parquet(parquet_path)

        timestamps = []
        pnl_curve  = []
        inv_curve  = []

        for row in df.itertuples():
            fair        = self.strategy.fair_value(row.mid, row.alpha, self.exchange.inventory)
            half_spread = self.strategy.half_spread(row.vol_10s, row.mid, abs(row.tfi))
            bid, ask    = self.strategy.quotes(fair, half_spread)

            self.exchange.cancel_all()
            self.exchange.place_limit_order("buy",  bid, 0.01, row.timestamp)
            self.exchange.place_limit_order("sell", ask, 0.01, row.timestamp)
            self.orders += 2

            fills = self.exchange.check_fills({
                "bid_0_price": row.bid_0_price,
                "ask_0_price": row.ask_0_price,
                "timestamp":   row.timestamp,
            })
            self.fills += len(fills)

            timestamps.append(row.timestamp)
            pnl_curve.append(self.exchange.pnl(row.mid))
            inv_curve.append(self.exchange.inventory)

        return pd.DataFrame({
            "timestamp": timestamps,
            "pnl":       pnl_curve,
            "inventory": inv_curve,
        })