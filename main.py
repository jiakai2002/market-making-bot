import asyncio
import logging
import os
import time
import pandas as pd
from dataclasses import dataclass, field

from config import SYMBOL, WS_URL, SNAPSHOT_INTERVAL, MAX_LEVELS, MAX_RECENT_TRADES
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
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)
logger.propagate = False

orderbook    = OrderBook(symbol=SYMBOL, max_levels=MAX_LEVELS)
trades       = TradeBuffer(max_trades=MAX_RECENT_TRADES)
market_state = MarketState(orderbook, trades)
volatility   = MultiHorizonVol()
alpha_model  = AlphaSignal()
strategy     = Strategy()
exchange     = Exchange(fee_bps=1.5, fill_prob=0.7)
kill_switch  = KillSwitch()
risk         = RiskLimits()
rows         = []


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


fs = FeatureState()


def update_features():
    snap = market_state.snapshot()
    mid  = snap["mid"]
    if mid is None:
        return
    spread     = snap["spread"] or 1e-8
    mp         = microprice(orderbook)
    obi        = orderbook_imbalance(orderbook)
    tfi        = trade_flow_imbalance(trades)
    fs.mid     = mid
    fs.mp      = mp
    fs.spread  = spread
    fs.obi     = obi
    fs.tfi     = tfi
    fs.vols    = volatility.update(mid_return(mid, fs.prev_mid))
    fs.alpha   = alpha_model.compute(
        book_imbalance=obi,
        trade_imbalance=tfi,
        microprice_edge=(mp - mid) / spread,
    )
    fs.prev_mid       = mid
    fs.last_update_ms = int(time.time() * 1000)
    fs.ready          = True


def save_parquet():
    if not rows:
        return
    path = f"data/{SYMBOL.lower()}_{int(time.time())}.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    logger.info(f"PARQUET_SAVE rows={len(rows)} file={path}")
    rows.clear()


async def on_message(msg):
    stream = msg.get("stream")
    data   = msg.get("data", {})
    try:
        if "depth" in stream:
            orderbook.process_message(data)
            update_features()
            for f in exchange.check_fills({
                "bid_0_price": orderbook.best_bid[0] if orderbook.best_bid else 0.0,
                "ask_0_price": orderbook.best_ask[0] if orderbook.best_ask else 0.0,
                "timestamp":   int(time.time() * 1000),
            }):
                logger.info(f"FILL {f.side.upper()} px={f.price:.2f} sz={f.size:.4f} fee={f.fee:.6f}")
        elif "trade" in stream:
            trades.add_trade(data)
            fs.tfi = trade_flow_imbalance(trades)

    except RuntimeError as e:
        logger.warning(f"RESYNC reason={e}")
        kill_switch.on_resync()
        orderbook.synced = False
        orderbook.buffer = []
        await orderbook.initialize()


async def printer():
    while True:
        if fs.ready:
            vol         = fs.vols.get("vol_10s", 0.0)
            fair        = strategy.fair_value(fs.mid, fs.alpha, exchange.inventory)
            half_spread = strategy.half_spread(vol, fs.mid, abs(fs.tfi))
            bid, ask    = strategy.quotes(fair, half_spread)
            quoting     = kill_switch.allow_trading() and risk.allow_quotes(
                inventory=exchange.inventory, vol=vol, tfi=fs.tfi,
                last_update_ms=fs.last_update_ms, now_ms=int(time.time() * 1000),
                mid=fs.mid,
            )
            exchange.cancel_all()
            if quoting:
                ts = int(time.time() * 1000)
                buy_id  = exchange.place_limit_order("buy",  bid, 0.01, ts)
                sell_id = exchange.place_limit_order("sell", ask, 0.01, ts)
                logger.info(f"ORDER BUY id={buy_id} px={bid:.2f}")
                logger.info(f"ORDER SELL id={sell_id} px={ask:.2f}")
            else:
                logger.info(f"QUOTES BLOCKED — {', '.join(risk.reasons)}")

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
                "fair":        fair,
                "half_spread": half_spread,
                "quote_bid":   bid,
                "quote_ask":   ask,
                "inventory":   exchange.inventory,
                "pnl":         exchange.pnl(fs.mid),
                "vol_10s":     fs.vols.get("vol_10s", 0.0),
                "vol_60s":     fs.vols.get("vol_60s", 0.0),
                "vol_5m":      fs.vols.get("vol_5m",  0.0),
            })
            if len(rows) % 100 == 0:
                save_parquet()

            print("\033[H\033[J", end="")
            print("=" * 50)
            print(SYMBOL.upper())
            print("=" * 50)
            print(f"Mid        : {fs.mid:.2f}")
            print(f"Fair       : {fair:.2f}")
            print(f"Alpha      : {fs.alpha:+.4f}")
            print(f"OB Imb     : {fs.obi:+.3f}")
            print(f"Trade Imb  : {fs.tfi:+.3f}")
            print(f"Inventory  : {exchange.inventory:+.4f} BTC")
            print(f"PnL        : {exchange.pnl(fs.mid):+.2f}")
            print(f"HalfSpread : {half_spread:.2f}")
            print(f"QUOTE      : BID {bid:.2f} | ASK {ask:.2f}")
            print(f"Quoting    : {'YES' if quoting else 'NO — ' + (kill_switch.reason or 'risk limit')}")
            if fs.vols:
                print(f"Vols       : {fs.vols['vol_10s']:.6f} | {fs.vols['vol_60s']:.6f} | {fs.vols['vol_5m']:.6f}")

        await asyncio.sleep(SNAPSHOT_INTERVAL)


async def main():
    await orderbook.initialize()
    ws = BinanceWSClient(WS_URL, on_message)
    try:
        await asyncio.gather(ws.connect(), printer())
    finally:
        save_parquet()


if __name__ == "__main__":
    asyncio.run(main())