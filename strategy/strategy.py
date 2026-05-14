from utils.math import round_to_tick
from config import BASE_SPREAD, VOL_MULT, TOX_MULT, INV_K, TICK_SIZE, MAX_INVENTORY


class Strategy:
    def __init__(self):
        self.base_spread = BASE_SPREAD
        self.vol_mult    = VOL_MULT
        self.tox_mult    = TOX_MULT
        self.inv_k       = INV_K
        self.tick_size   = TICK_SIZE
        self.max_inventory = MAX_INVENTORY

    def fair_value(self, mid, alpha, inventory):
        inv_norm = inventory / self.max_inventory
        return mid + alpha * mid * 0.001 - self.inv_k * inv_norm

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
