SYMBOL = "btcusdt"

DEPTH_STREAM = f"{SYMBOL}@depth@100ms"
TRADE_STREAM = f"{SYMBOL}@aggTrade"
WS_URL       = f"wss://stream.binance.com:9443/stream?streams={DEPTH_STREAM}/{TRADE_STREAM}"

# Data
MAX_LEVELS        = 20
MAX_RECENT_TRADES = 20

# Alpha weights (must sum to 1.0)
W_BOOK_IMBALANCE  = 0.45
W_TRADE_IMBALANCE = 0.45
W_MICROPRICE_EDGE = 0.10

# Strategy
BASE_SPREAD    = 0.5
VOL_MULT       = 5.0
TOX_MULT       = 2.0
INV_K          = 2.0
TICK_SIZE      = 0.5
QUOTE_SIZE     = 0.01

# Risk
MAX_INVENTORY  = 0.1
MAX_VOL        = 20.0
MAX_TOXICITY   = 0.7
MAX_STALE_MS   = 5000

# Exchange
FEE_BPS        = 1.5
FILL_PROB      = 0.7

# Session
SNAPSHOT_INTERVAL = 1.0
SAVE_INTERVAL     = 300   # seconds between parquet flushes
