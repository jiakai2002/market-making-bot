## Market maker bot

Inspired by the Avellaneda-Stoikov (2006) model, supports live trading and backtesting on BTC/USDT from Binance.

---

## Strategy

We quote bid and ask around a **reservation price** that skews with inventory, with a **spread** that widens with volatility and narrows with liquidity:

```
r = mid − q · γ · σ² · τ
δ = ½ [ γ · σ² · τ  +  (2/γ) · ln(1 + γ/κ) ]
```

## Features

- Infinite horizon τ=1
- Simple price-cross fill simulation
- Dynamic γ recomputed each cycle from spread bounds and inventory skew
- Live-calibrated via exponential fit to aggTrade depth data
- Exponential decay with |q| — reduces size as inventory skews


## Architecture

```
stream.py       — Binance USDT-M Futures depth WebSocket collector (n_rows based)
orderbook.py    — Incremental order book: snapshot + delta reconciliation
indicators.py   — EWMA volatility + TradingIntensityIndicator (κ, α)
strategy.py     — ASConfig, ASQuoter: reservation price, spread, quotes
exchange.py     — Simulated exchange: price-cross fill model
market_maker.py — Tick loop, order lifecycle, κ recalibration, logging
logger.py       — Structured console logger
```

### Live data flow

```
Binance depth WS @100ms  →  OrderBookManager  →  MarketMaker.on_tick()
Binance aggTrade WS       →  TradingIntensityIndicator.on_trade()
```

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `gamma` | `0.1` | Risk aversion — fallback when `dynamic_gamma=False` |
| `kappa` | `1.5` | Order book depth — updated live by `TradingIntensityIndicator` |
| `infinite_horizon` | `True` | τ=1 always; set `False` for vol-driven τ decay |
| `tau_decay` | `0.5` | τ decay rate when `infinite_horizon=False` |
| `dynamic_gamma` | `True` | Recompute γ each cycle from spread bounds and |q| |
| `inventory_risk_aversion` | `0.5` | IRA ∈ (0,1] — scales γ relative to theoretical maximum |
| `gamma_cap` | `2.0` | Hard upper bound on dynamic γ |
| `min_spread` | `0.50` | Minimum half-spread (USD) |
| `max_spread` | `20.0` | Maximum half-spread — bounds dynamic γ |
| `vol_spike_threshold` | `2.0` | vol_ratio above this cancels quotes and forces κ recalibration |
| `max_inventory` | `0.05` | One-sided quoting beyond this threshold |
| `order_size` | `0.001` | Base order size in BTC |
| `eta_decay` | `0.0` | Size decay: `size = base · exp(−η·|q|)`. 0 = constant |
| `kappa_recalib_ticks` | `100` | Ticks between κ calibration windows |
| `kappa_sampling_length` | `30` | Rolling window size for κ estimation |
| `kappa_min_samples` | `10` | Minimum samples before κ estimation begins |

## κ Estimation

`TradingIntensityIndicator` fits `λ(δ) = α · exp(−κ · δ)` to live aggTrade data, where δ is the distance of each trade from mid-price at the time of the trade.

- **Timestamped mid buffer** — δ resolved against mid snapshot just before each trade, not current mid
- **Arrival rate normalisation** — volume divided by window duration (sec) so κ is window-size invariant
- **EMA smoothing** — `κ ← (1−α)·κ_prev + α·κ_new` with hard clamp `[kappa_min, kappa_max]`
- **Warm start + fallback** — previous (α, κ) used as initial guess; last valid estimate kept on fit failure

## Volatility

EWMA on log returns scaled to `vol_horizon_sec`, output in absolute price units:

```
σ²_t = λ · σ²_{t-1} + (1−λ) · r²_t / dt
σ    = sqrt(σ² · horizon_sec) · mid
```

Returns `(sigma, vol_ratio)`. When `vol_ratio > vol_spike_threshold`, active quotes are cancelled and κ recalibration is forced immediately.

## Run

```bash
pip install -r requirements.txt

# backtest (streams fresh data if parquet not found)
python market_maker.py backtest

# live (logs quotes)
python market_maker.py live
```
