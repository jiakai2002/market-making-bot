from config import MAX_INVENTORY, MAX_VOL, MAX_TOXICITY, MAX_STALE_MS, ALPHA_THRESHOLD


class KillSwitch:
    def __init__(self):
        self.triggered     = False
        self.reason: str   = ""
        self._resync_count = 0

    def trigger(self, reason: str):
        self.triggered = True
        self.reason    = reason

    def on_resync(self):
        self._resync_count += 1
        if self._resync_count >= 3:
            self.trigger("too many resyncs")

    def reset(self):
        self.triggered     = False
        self.reason        = ""
        self._resync_count = 0

    def allow_trading(self) -> bool:
        return not self.triggered


class RiskLimits:
    def __init__(self):
        self.max_inventory  = MAX_INVENTORY
        self.max_vol        = MAX_VOL
        self.max_toxicity   = MAX_TOXICITY
        self.max_stale_ms   = MAX_STALE_MS
        self.alpha_threshold = ALPHA_THRESHOLD
        self.reasons: list[str] = []

    def allow_quotes(self, inventory, vol, tfi, last_update_ms, now_ms, mid, alpha=0.0):
        self.reasons = []

        # ── Hard stops — block both sides ──────────────────────────────
        if vol * mid > self.max_vol:
            self.reasons.append("vol")
            return False, False

        if (now_ms - last_update_ms) > self.max_stale_ms:
            self.reasons.append("stale")
            return False, False

        if abs(tfi) > self.max_toxicity:
            self.reasons.append("toxicity")
            return False, False

        # ── Per-side checks ────────────────────────────────────────────
        bid_ok = inventory < self.max_inventory
        ask_ok = inventory > -self.max_inventory

        if not bid_ok:
            self.reasons.append("inventory_long")
        if not ask_ok:
            self.reasons.append("inventory_short")

        if bid_ok and alpha < -self.alpha_threshold:
            bid_ok = False
            self.reasons.append("alpha_sell")

        if ask_ok and alpha > self.alpha_threshold:
            ask_ok = False
            self.reasons.append("alpha_buy")

        return bid_ok, ask_ok