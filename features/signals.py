import math
from utils.math import clamp
from config import W_BOOK_IMBALANCE, W_TRADE_IMBALANCE, W_MICROPRICE_EDGE


class EWMAVolatility:
    def __init__(self, tau_seconds: float):
        self.var = None
        self.tau = tau_seconds

    def update(self, log_ret: float, dt: float) -> float:
        alpha  = 1.0 - math.exp(-dt / self.tau)
        x      = (log_ret * log_ret) / dt        # variance rate
        self.var = x if self.var is None else alpha * x + (1 - alpha) * self.var
        return math.sqrt(self.var)


class MultiHorizonVol:
    def __init__(self):
        self.fast = EWMAVolatility(tau_seconds=10)
        self.mid  = EWMAVolatility(tau_seconds=60)
        self.slow = EWMAVolatility(tau_seconds=300)

    def update(self, log_ret: float, dt: float, best_bid: float, best_ask: float) -> dict:
        spread_vol = math.log(best_ask / best_bid) / math.sqrt(4 * math.log(2))
        floor = max(spread_vol * 0.5, 1e-6)   # market-implied floor, not a magic constant

        v_fast = max(self.fast.update(log_ret, dt), floor)
        v_mid  = max(self.mid.update(log_ret, dt),  floor)
        v_slow = max(self.slow.update(log_ret, dt), floor)

        cap = 2e-4   # ~3× normal BTC per-second vol
        return {
            "vol_10s": min(v_fast, cap),
            "vol_60s": min(v_mid,  cap),
            "vol_5m":  min(v_slow, cap),
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