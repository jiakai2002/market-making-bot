from utils.math import round_to_tick


class Strategy:
    def __init__(self, base_spread=0.5, vol_mult=5.0, tox_mult=2.0,
                 inv_k=2.0, tick_size=0.5):
        self.base_spread = base_spread
        self.vol_mult    = vol_mult
        self.tox_mult    = tox_mult
        self.inv_k       = inv_k
        self.tick_size   = tick_size

    def fair_value(self, mid, alpha, inventory):
        alpha_shift    = alpha * mid * 0.001
        inventory_skew = self.inv_k * inventory
        return mid + alpha_shift - inventory_skew

    def half_spread(self, vol, mid, toxicity):
        return max(
            self.base_spread + self.vol_mult * vol * mid + self.tox_mult * toxicity,
            0.01
        )

    def quotes(self, fair, half_spread):
        bid = round_to_tick(fair - half_spread, self.tick_size)
        ask = round_to_tick(fair + half_spread, self.tick_size)
        if bid >= ask:
            bid = ask - self.tick_size
        return bid, ask