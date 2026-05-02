import math
import warnings
from typing import Optional

import numpy as np
from scipy.optimize import curve_fit, OptimizeWarning

warnings.simplefilter("ignore", OptimizeWarning)


class VolatilityEstimator:
    def __init__(self, lambda_=0.97, floor=1e-6, cap=0.5, horizon_sec=1.0, warmup=100):
        self.lambda_ = lambda_
        self.floor = floor
        self.cap = cap
        self.horizon_sec = horizon_sec
        self.warmup = warmup
        self.prev_mid = None
        self.prev_ts = None
        self.var_per_sec = floor * floor
        self.samples = 0
        self.last_sigma = floor

    def update(self, mid: float, ts_ms: int) -> float:
        if mid <= 0:
            return self.last_sigma
        if self.prev_mid is None:
            self.prev_mid = mid
            self.prev_ts = ts_ms
            return self.last_sigma

        dt = (ts_ms - self.prev_ts) / 1000.0
        if dt <= 0:
            return self.last_sigma

        r = math.log(mid / self.prev_mid)
        r = max(min(r, 0.05), -0.05)
        self.prev_mid = mid
        self.prev_ts = ts_ms

        inst_var = (r * r) / max(dt, 1e-3)
        self.var_per_sec = self.lambda_ * self.var_per_sec + (1.0 - self.lambda_) * inst_var

        sigma_log = math.sqrt(self.var_per_sec * self.horizon_sec)
        sigma_log = max(self.floor, min(self.cap, sigma_log))
        self.last_sigma = sigma_log * mid
        self.samples += 1
        return self.last_sigma

    @property
    def ready(self) -> bool:
        return self.samples >= self.warmup


class TradingIntensityIndicator:
    def __init__(self, sampling_length: int = 30, min_samples: int = 10,
                 smooth_alpha: float = 0.2, kappa_min: float = 0.05, kappa_max: float = 50.0):
        self.sampling_length = sampling_length
        self.min_samples = min_samples
        self.smooth_alpha = smooth_alpha
        self.kappa_min = kappa_min
        self.kappa_max = kappa_max

        self.mid: Optional[float] = None
        self._current_sample: list[dict] = []
        self._samples: dict[int, list[dict]] = {}
        self.alpha: float = 1.0
        self.kappa: float = 1.5

    def update_mid(self, best_bid: float, best_ask: float):
        self.mid = 0.5 * (best_bid + best_ask)

    def on_trade(self, price: float, qty: float, timestamp_ms: int):
        if self.mid is None:
            return
        price_level = abs(price - self.mid)
        self._current_sample.append({"price_level": price_level, "amount": qty})

    def flush_sample(self, timestamp_ms: int):
        if not self._current_sample:
            return
        self._samples[timestamp_ms] = self._current_sample
        self._current_sample = []

        if len(self._samples) > self.sampling_length:
            oldest = min(self._samples.keys())
            del self._samples[oldest]

        if self.ready:
            self._fit()

    def _fit(self):
        trades_consolidated: dict[float, float] = {}
        for tick in self._samples.values():
            for t in tick:
                pl = t["price_level"]
                trades_consolidated[pl] = trades_consolidated.get(pl, 0) + t["amount"]

        price_levels = sorted(trades_consolidated.keys(), reverse=True)
        lambdas = [max(trades_consolidated[pl], 1e-10) for pl in price_levels]

        try:
            params, _ = curve_fit(
                lambda t, a, b: a * np.exp(-b * t),
                price_levels,
                lambdas,
                p0=(self.alpha, self.kappa),
                method='dogbox',
                bounds=([0, 0], [np.inf, np.inf])
            )
            k_new = float(np.clip(params[1], self.kappa_min, self.kappa_max))
            self.kappa = (1 - self.smooth_alpha) * self.kappa + self.smooth_alpha * k_new
            self.alpha = float(params[0])
        except (RuntimeError, ValueError):
            pass

    @property
    def ready(self) -> bool:
        return len(self._samples) >= self.min_samples
