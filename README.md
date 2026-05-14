# BTC/USDT Market Making Bot

A live paper-trading market maker for BTC/USDT on Binance. Computes dynamic bid/ask quotes using orderbook signals, manages inventory risk, and logs all activity for backtesting.

<img width="653" height="454" alt="mm" src="https://github.com/user-attachments/assets/c04c7b8e-378b-464d-8f62-496ad1266c89" />

---

## How It Works

```
Binance WS → OrderBook → Features → Strategy → Paper Exchange
                                        ↓
                                    Risk Gates
                                        ↓
                                  Parquet Logger → Backtest → Metrics
```

Each cycle:
1. **Features** — microprice, orderbook imbalance, trade flow imbalance, EWMA vol (3 horizons)
2. **Alpha** — weighted combination of features → directional signal in [-1, 1]  
   Weights: OBI × 0.45 + TFI × 0.45 + microprice edge × 0.10
3. **Fair value** — mid + alpha shift − inventory skew (Avellaneda-Stoikov inspired)
4. **Spread** — base + vol_mult × vol × mid + tox_mult × |TFI|
5. **Risk** — gates quoting on inventory, vol, flow toxicity, and data staleness
6. **Exchange** — probabilistic fill simulation against live Binance order book

---

## Structure

```
market_maker/
├── main.py               # entry point, event loop
├── config.py             # tunable parameters
├── market/               # live data feed
│   ├── binance_ws.py     # websocket connection + reconnect
│   ├── orderbook.py      # L2 book with Binance sync protocol
│   ├── trade_buffer.py   # rolling trade window
│   └── market_state.py   # unified snapshot
├── features/
│   ├── features.py       # microprice, OBI, TFI, mid return
│   └── signals.py        # EWMA vol (10s/60s/5m), alpha signal
├── strategy/
│   └── strategy.py       # fair value, spread, quoting
├── execution/
│   └── exchange.py       # paper exchange, fill simulation, PnL
├── risk/
│   └── risk.py           # kill switch, position/vol/toxicity limits
├── backtest/
│   ├── replay_engine.py  # replays saved parquet sessions
│   ├── metrics.py        # Sharpe, drawdown, fill rate, inventory turnover
│   └── run_backtest.py   # CLI entry point
├── data/                 # parquet session files (auto-generated)
├── logs/                 # event log (auto-generated)
└── utils/
    └── math.py           # clamp, round_to_tick
```

---

## Quickstart

```bash
pip install -r requirements.txt
python main.py                                          # run live
python run_backtest.py data/btcusdt_<session>.parquet  # backtest a session
```

Live session saves a parquet file to `data/` every 5 minutes and on shutdown. Feed it directly to the backtest.

---

## Key Design Decisions

**Reservation price** — fair value is skewed away from inventory accumulation. Long 0.05 BTC → ask moves tighter, bid moves wider, mean-reverting the position passively. Skew magnitude controlled by `inv_k = 15.0`.

**Dynamic spread** — widens with realised volatility (dollar-scaled) and trade flow toxicity. Protects against adverse selection during momentum regimes. Vol is floored at half the implied spread-vol to avoid collapse in quiet markets.

**Toxicity gate** — quoting halts when `abs(TFI) > 0.95`. One-sided flow means informed traders are active; providing liquidity into a trend is negative EV.

**Alpha threshold** — quotes are skipped when `|alpha| < 0.3`, avoiding unnecessary exposure when the directional signal is too weak to justify a position.

**Binance sync protocol** — orderbook opens WS first, buffers diffs, fetches REST snapshot, replays buffer. Skipping this causes silent book corruption.

**Multi-horizon vol** — three EWMA volatility estimates (10s, 60s, 5m) expose short-term spikes separately from structural regime shifts. Spread uses the 10s estimate; risk limits can consult longer horizons.

**Parquet logging** — every feature snapshot saved at 1s cadence during live trading. Backtest replays the exact same strategy logic against recorded market conditions.

---

## Backtest Results

Sessions run on live-collected BTC/USDT data at 1s feature cadence.

| Session       | Sharpe | Final PnL | Max Drawdown | Fill Rate |
|---------------|--------|-----------|--------------|-----------|
| baseline      | —      | —         | —            | —         |
| tight spread  | —      | —         | —            | —         |
| wide spread   | —      | —         | —            | —         |

*Run sessions and update with real numbers.*

---

## Parameters

| Parameter         | Default  | Description                                        |
|-------------------|----------|----------------------------------------------------|
| `base_spread`     | 0.05     | Minimum half-spread ($)                            |
| `vol_mult`        | 3.0      | Vol contribution to spread                         |
| `tox_mult`        | 2.0      | Toxicity contribution to spread                    |
| `inv_k`           | 15.0     | Inventory skew magnitude ($/BTC normalised)        |
| `quote_size`      | 0.005    | Order size in BTC                                  |
| `tick_size`       | 0.01     | Price rounding granularity ($)                     |
| `max_inventory`   | 0.1      | Max BTC position before halt                       |
| `max_vol`         | 5.0      | Max per-second vol before halt                     |
| `max_toxicity`    | 0.95     | Max \|TFI\| before halt                            |
| `max_stale_ms`    | 5000     | Max age of market data before halt (ms)            |
| `alpha_threshold` | 0.3      | Min \|alpha\| required to place quotes             |
| `fee_bps`         | 10       | Exchange fee in basis points                       |
| `fill_k`          | 0.0001   | Fill rate coefficient (BTC/$)                      |
