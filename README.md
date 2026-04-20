# Order Book Microstructure Analysis (BTC/USDT Futures)

Goal is to investigate whether order book imbalance can predict 
short-term price movements in BTC/USDT futures.

## Overview
Real-time L2 order book data is streamed from Binance Futures WebSocket and used to compute 
microstructure metrics. The core hypothesis is that order book imbalance — a measure of buying 
vs selling pressure — is a leading indicator of short-term price direction.

<img width="1500" height="2400" alt="plots" src="https://github.com/user-attachments/assets/1b16abba-dd31-4bde-b77a-388a2ed149c1" />


## Methodology

**Data Collection**
- Stream L2 order book data via Binance Futures WebSocket (`@depth@100ms`)
- Maintain a local order book using the snapshot + delta update protocol
- Buffer incoming updates, fetch REST snapshot, discard stale updates, apply valid ones
- On each update, flatten top 5 bid/ask levels into a row — 5000 snapshots total
- Time series is event-driven (irregular intervals), not fixed frequency

**Metrics**
| Metric | Description |
|---|---|
| `mid_price` | Average of best bid and ask |
| `spread` | Best ask minus best bid, proxy for liquidity |
| `imbalance` | Normalised bid/ask volume ratio across top 5 levels, measures buying/selling pressure |
| `microprice` | Mid price weighted by imbalance, implied fair value |
| `mid_return` | Percentage change in mid price between consecutive updates |
| `next_mid_return` | Target variable — mid return of the following update |

**Analysis**
- Scatter plot of imbalance vs next mid return
- Quantile analysis — average next mid return per imbalance bucket
- Rolling correlation (window=50) to assess signal stability over time

**Model**
- Logistic regression classifier predicting price direction (next mid return > 0)
- Features: `imbalance`, `imbalance_lag1`, `imbalance_lag2`
- Validated with TimeSeriesSplit (5 folds) — never random split on time series
- Mean accuracy: 95.2% across folds
- Feature coefficients: imbalance (4.17), lag1 (-0.75), lag2 (-0.37)

## Key Findings
- OBI has a positive correlation with next mid return, strongest at extremes (OBI near ±1)
- Rolling correlation predominantly positive but unstable — signal is regime-dependent
- High accuracy driven largely by predicting zero returns (price unchanged between most updates)
- When one side of the book is thin, incoming orders consume more price levels,
  resulting in greater price impact: less depth = less resistance = larger price move

## Project Structure
```
quant/
  data/
    raw/
      book.parquet          ← raw order book snapshots
    processed/
      metrics.parquet       ← computed metrics
      plots.png             ← analysis plots
  data.py                   ← data collection (WebSocket streaming)
  analysis.ipynb            ← metrics, analysis and model
```

## Requirements
```bash
pip install pandas numpy matplotlib statsmodels scikit-learn websockets sortedcontainers
```

## Usage
```bash
# collect data
python data.py

# run analysis
jupyter notebook analysis.ipynb
```