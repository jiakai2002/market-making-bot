
def round_to_tick(price: float, tick_size: float):
    return round(price / tick_size) * tick_size


def clamp(x: float, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))