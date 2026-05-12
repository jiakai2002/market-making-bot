
class EMA:
    def __init__(self, alpha: float):
        self.alpha = alpha
        self.value = None

    def update(self, x: float):
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value

        return self.value


def round_to_tick(price: float, tick_size: float):
    return round(price / tick_size) * tick_size


def clamp(x: float, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))