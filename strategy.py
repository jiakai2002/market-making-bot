import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class ASConfig:
    gamma: float = 0.1
    kappa: float = 1.5

    session_minutes: float = 60.0

    vol_horizon_sec: float = 1.0
    vol_cap: float = 0.5
    vol_floor: float = 1e-6

    max_inventory: float = 0.05
    target_inventory: float = 0.0

    order_size: float = 0.001
    min_spread: float = 0.50

    tick_size: float = 0.10
    quote_refresh_ticks: int = 1

    kappa_sampling_length: int = 30
    kappa_min_samples: int = 10
    kappa_recalib_ticks: int = 100


class ASQuoter:
    def __init__(self, cfg: ASConfig):
        self.cfg = cfg

    def time_factor(self, t: float) -> float:
        return max(0.0, min(1.0, 1.0 - t))

    def reservation_price(self, mid: float, q: float, sigma: float, t: float) -> float:
        sigma_log = sigma / mid
        tau = self.time_factor(t)
        return mid - q * self.cfg.gamma * (sigma_log ** 2) * tau

    def optimal_spread(self, mid: float, sigma: float, t: float) -> float:
        sigma_log = sigma / mid
        tau = self.time_factor(t)
        gamma = self.cfg.gamma
        kappa = self.cfg.kappa
        term1 = gamma * (sigma_log ** 2) * tau
        term2 = (2.0 / gamma) * math.log(1.0 + gamma / kappa)
        return max((term1 + term2) / 2.0, self.cfg.min_spread)

    def quotes(self, mid: float, q: float, sigma: float, t: float) -> tuple[Optional[float], Optional[float]]:
        r = self.reservation_price(mid, q, sigma, t)
        half = self.optimal_spread(mid, sigma, t)
        bid_price = r - half
        ask_price = r + half
        if q >= self.cfg.max_inventory:
            bid_price = None
        if q <= -self.cfg.max_inventory:
            ask_price = None
        return bid_price, ask_price
