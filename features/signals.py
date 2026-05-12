import math
from utils.math import EMA, clamp
from config import W_BOOK_IMBALANCE, W_TRADE_IMBALANCE, W_MICROPRICE_EDGE


class EWMAVolatility:
    def __init__(self, alpha=0.05):
        self.var_ema = EMA(alpha)

    def update(self, ret: float):
        var = self.var_ema.update(ret * ret)
        return math.sqrt(var) if var is not None else 0.0


class MultiHorizonVol:
    def __init__(self):
        self.vol_10s = EWMAVolatility(alpha=0.20)
        self.vol_60s = EWMAVolatility(alpha=0.05)
        self.vol_5m  = EWMAVolatility(alpha=0.01)

    def update(self, ret: float):
        return {
            "vol_10s": self.vol_10s.update(ret),
            "vol_60s": self.vol_60s.update(ret),
            "vol_5m":  self.vol_5m.update(ret),
        }


class AlphaSignal:
    def __init__(self):
        self.value = 0.0

    def compute(self, book_imbalance: float, trade_imbalance: float, microprice_edge: float):
        self.value = clamp(
            W_BOOK_IMBALANCE  * book_imbalance +
            W_TRADE_IMBALANCE * trade_imbalance +
            W_MICROPRICE_EDGE * microprice_edge
        )
        return self.value