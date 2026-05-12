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
    def __init__(self, max_inventory=0.1, max_vol=20.0,
                 max_toxicity=0.7, max_stale_ms=5000):
        self.max_inventory = max_inventory
        self.max_vol       = max_vol
        self.max_toxicity  = max_toxicity
        self.max_stale_ms  = max_stale_ms
        self.reasons: list[str] = []

    def allow_quotes(self, inventory, vol, tfi, last_update_ms, now_ms, mid) -> bool:
        self.reasons = []
        if abs(inventory) > self.max_inventory:
            self.reasons.append(f"inventory")
        if vol * mid > self.max_vol:
            self.reasons.append(f"vol")
        if abs(tfi) > self.max_toxicity:
            self.reasons.append(f"toxicity")
        if (now_ms - last_update_ms) > self.max_stale_ms:
            self.reasons.append(f"stale")
        return len(self.reasons) == 0