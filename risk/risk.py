from config import MAX_INVENTORY, MAX_VOL, MAX_TOXICITY, MAX_STALE_MS


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
        self.max_inventory = MAX_INVENTORY
        self.max_vol       = MAX_VOL
        self.max_toxicity  = MAX_TOXICITY
        self.max_stale_ms  = MAX_STALE_MS
        self.reasons: list[str] = []

    def allow_quotes(self, inventory, vol, tfi, last_update_ms, now_ms, mid):
        self.reasons = []
        hard_block = []
        if vol * mid > self.max_vol:
            hard_block.append("vol")
        if (now_ms - last_update_ms) > self.max_stale_ms:
            hard_block.append("stale")
        if hard_block:
            self.reasons = hard_block
            return False, False

        bid_ok = inventory <  self.max_inventory
        ask_ok = inventory > -self.max_inventory

        if abs(tfi) > self.max_toxicity:
            self.reasons.append("toxicity")
            return False, False

        if not bid_ok: self.reasons.append("inventory")
        if not ask_ok: self.reasons.append("inventory")
        return bid_ok, ask_ok