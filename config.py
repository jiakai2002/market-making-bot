SYMBOL = "btcusdt"

DEPTH_STREAM = f"{SYMBOL}@depth@100ms"
TRADE_STREAM = f"{SYMBOL}@trade"

WS_URL = (
    f"wss://stream.binance.com:9443/stream"
    f"?streams={DEPTH_STREAM}/{TRADE_STREAM}"
)

SNAPSHOT_INTERVAL = 1.0

MAX_LEVELS = 20
MAX_RECENT_TRADES = 20

# Alpha signal weights (must sum to 1.0)
W_BOOK_IMBALANCE  = 0.45
W_TRADE_IMBALANCE = 0.45
W_MICROPRICE_EDGE = 0.10