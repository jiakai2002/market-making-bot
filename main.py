import asyncio
import logging
import os
import time
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field

from config import SYMBOL, WS_URL, MAX_LEVELS, MAX_RECENT_TRADES, QUOTE_SIZE
from market.binance_ws import BinanceWSClient
from market.orderbook import OrderBook
from market.trade_buffer import TradeBuffer
from market.market_state import MarketState
from features.features import microprice, orderbook_imbalance, trade_flow_imbalance, mid_return
from features.signals import AlphaSignal, MultiHorizonVol
from strategy.strategy import Strategy
from execution.exchange import Exchange
from risk.risk import KillSwitch, RiskLimits


os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logger = logging.getLogger("mm")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("logs/events.log")
handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(handler)
logger.propagate = False

orderbook    = OrderBook(symbol=SYMBOL, max_levels=MAX_LEVELS)
trades       = TradeBuffer(max_trades=MAX_RECENT_TRADES)
market_state = MarketState(orderbook, trades)
volatility   = MultiHorizonVol()
alpha_model  = AlphaSignal()
strategy     = Strategy()
exchange     = Exchange()
kill_switch  = KillSwitch()
risk         = RiskLimits()
rows         = []
SESSION_FILE = f"data/{SYMBOL}_{int(time.time())}.parquet"


@dataclass
class FeatureState:
    mid: float             = 0.0
    mp: float              = 0.0
    spread: float          = 0.0
    obi: float             = 0.0
    tfi: float             = 0.0
    alpha: float           = 0.0
    vols: dict             = field(default_factory=dict)
    prev_mid: float | None = None
    last_update_ms: int    = 0
    ready: bool            = False
    fair: float            = 0.0
    half_spread: float     = 0.0
    quote_bid: float       = 0.0
    quote_ask: float       = 0.0
    bid_ok: bool           = False
    ask_ok: bool           = False


fs = FeatureState()


def update_features():
    snap = market_state.snapshot()
    mid  = snap["mid"]
    if mid is None:
        return
    now_ms = int(time.time() * 1000)
    dt     = max((now_ms - fs.last_update_ms) / 1000.0, 0.01) if fs.last_update_ms else 1.0

    spread    = snap["spread"] or 1e-8
    mp        = microprice(orderbook)
    obi       = orderbook_imbalance(orderbook)
    tfi       = trade_flow_imbalance(trades)
    fs.mid    = mid
    fs.mp     = mp
    fs.spread = spread
    fs.obi    = obi
    fs.tfi    = tfi
    fs.vols   = volatility.update(
        mid_return(mid, fs.prev_mid), dt,
        orderbook.best_bid[0], orderbook.best_ask[0]
    )
    fs.alpha  = alpha_model.compute(
        book_imbalance=obi,
        trade_imbalance=tfi,
        microprice_edge=(mp - mid) / spread,
    )
    fs.prev_mid       = mid
    fs.last_update_ms = int(time.time() * 1000)
    fs.ready = volatility.warmed_up


def save_parquet():
    if not rows:
        return
    new = pd.DataFrame(rows)
    if Path(SESSION_FILE).exists():
        existing = pd.read_parquet(SESSION_FILE)
        pd.concat([existing, new], ignore_index=True).to_parquet(SESSION_FILE, index=False)
    else:
        new.to_parquet(SESSION_FILE, index=False)
    logger.info(f"PARQUET_SAVE rows={len(rows)} file={SESSION_FILE}")
    rows.clear()


async def refresh_quotes():
    """Called on every depth update (~100ms). Cancels stale quotes and places fresh ones."""
    if not fs.ready:
        counts = [volatility.fast.n, volatility.mid.n, volatility.slow.n]
        warmup = volatility.fast.warmup
        logger.info(f"Calculating volatility {min(counts)}/{warmup} ticks")
        return

    vol         = fs.vols.get("vol_10s", 0.0)
    fair        = strategy.fair_value(fs.mid, fs.alpha, exchange.inventory)
    half_spread = strategy.half_spread(vol, fs.mid, abs(fs.tfi))
    bid, ask    = strategy.quotes(fair, half_spread)

    bid_ok, ask_ok = risk.allow_quotes(
        inventory=exchange.inventory, vol=vol, tfi=fs.tfi,
        last_update_ms=fs.last_update_ms, now_ms=int(time.time() * 1000),
        mid=fs.mid, alpha=fs.alpha,
    ) if kill_switch.allow_trading() else (False, False)

    exchange.cancel_all()
    ts = int(time.time() * 1000)
    if bid_ok:
        bid_queue = orderbook.best_bid[1] if orderbook.best_bid else 0.0
        buy_id = exchange.place_limit_order("buy", bid, QUOTE_SIZE, ts, queue_depth=bid_queue)
        logger.info(f"ORDER BUY id={buy_id} px={bid:.2f} queue={bid_queue:.4f}")
    if ask_ok:
        ask_queue = orderbook.best_ask[1] if orderbook.best_ask else 0.0
        sell_id = exchange.place_limit_order("sell", ask, QUOTE_SIZE, ts, queue_depth=ask_queue)
        logger.info(f"ORDER SELL id={sell_id} px={ask:.2f} queue={ask_queue:.4f}")
    if not bid_ok or not ask_ok:
        active_reasons = [r for r in (risk.reasons or [kill_switch.reason]) if r]
        if active_reasons:
            logger.info(f"QUOTES BLOCKED — {', '.join(active_reasons)}")

    fs.fair        = fair
    fs.half_spread = half_spread
    fs.quote_bid   = bid
    fs.quote_ask   = ask
    fs.bid_ok      = bid_ok
    fs.ask_ok      = ask_ok


async def on_message(msg):
    stream = msg.get("stream")
    data   = msg.get("data", {})
    try:
        if "depth" in stream:
            orderbook.process_message(data)
            update_features()
            await refresh_quotes()
            for f in exchange.check_fills({
                "bid_0_price": orderbook.best_bid[0] if orderbook.best_bid else 0.0,
                "ask_0_price": orderbook.best_ask[0] if orderbook.best_ask else 0.0,
                "timestamp":   int(time.time() * 1000),
            }):
                logger.info(f"FILL {f.side.upper()} px={f.price:.2f} sz={f.size:.4f} fee={f.fee:.6f}")
        elif "aggTrade" in stream:
            trades.add_trade(data)
            fs.tfi = trade_flow_imbalance(trades)
    except RuntimeError as e:
        logger.warning(f"RESYNC reason={e}")
        kill_switch.on_resync()
        orderbook.synced = False
        orderbook.buffer = []
        await orderbook.initialize()


async def printer():
    """Display-only loop at 1s. Also records rows for parquet."""
    while True:
        if fs.ready:
            rows.append({
                "timestamp":   fs.last_update_ms,
                "bid_0_price": orderbook.best_bid[0] if orderbook.best_bid else 0.0,
                "ask_0_price": orderbook.best_ask[0] if orderbook.best_ask else 0.0,
                "mid":         fs.mid,
                "spread":      fs.spread,
                "microprice":  fs.mp,
                "obi":         fs.obi,
                "tfi":         fs.tfi,
                "alpha":       fs.alpha,
                "fair":        fs.fair,
                "half_spread": fs.half_spread,
                "quote_bid":   fs.quote_bid,
                "quote_ask":   fs.quote_ask,
                "inventory":   exchange.inventory,
                "pnl":         exchange.pnl(fs.mid),
                "vol_10s":     fs.vols.get("vol_10s", 0.0),
                "vol_60s":     fs.vols.get("vol_60s", 0.0),
                "vol_5m":      fs.vols.get("vol_5m",  0.0),
            })

            if len(rows) % 100 == 0:
                save_parquet()

            sides = "".join(["BID " if fs.bid_ok else "", "ASK" if fs.ask_ok else ""]).strip()
            blocked_reasons = [r for r in (risk.reasons or [kill_switch.reason]) if r]

            print("\033[H\033[J", end="")
            print("=" * 50)
            print(SYMBOL.upper())
            print("=" * 50)
            print(f"Mid        : {fs.mid:.2f}")
            print(f"Fair       : {fs.fair:.2f}")
            print(f"Alpha      : {fs.alpha:+.4f}")
            print(f"OB Imb     : {fs.obi:+.3f}")
            print(f"Trade Imb  : {fs.tfi:+.3f}")
            print(f"Inventory  : {exchange.inventory:+.4f} BTC")
            print(f"PnL        : {exchange.pnl(fs.mid):+.2f}")
            print(f"HalfSpread : {fs.half_spread:.2f}")
            print(f"QUOTE      : BID {fs.quote_bid:.2f} | ASK {fs.quote_ask:.2f}")
            print(f"Quoting    : {sides if sides else 'NO — ' + ', '.join(blocked_reasons)}")
            if fs.vols:
                print(f"Vols       : {fs.vols['vol_10s']:.6f} | {fs.vols['vol_60s']:.6f} | {fs.vols['vol_5m']:.6f}")

        await asyncio.sleep(1.0)


async def main():
    await orderbook.initialize()
    ws = BinanceWSClient(WS_URL, on_message)
    try:
        await asyncio.gather(ws.connect(), printer())
    finally:
        save_parquet()


if __name__ == "__main__":
    asyncio.run(main())