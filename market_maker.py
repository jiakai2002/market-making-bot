import asyncio
import math
import os
import time
from typing import Optional

import pandas as pd

from exchange import SimulatedExchange
from indicators import TradingIntensityIndicator, VolatilityEstimator
from strategy import ASConfig, ASQuoter
from logger import get_logger
logger = get_logger("market_maker")

class MarketMaker:
    def __init__(self, cfg: ASConfig = ASConfig()):
        self.cfg = cfg
        self.exchange = SimulatedExchange()
        self.quoter = ASQuoter(cfg)
        self.vol_est = VolatilityEstimator(
            horizon_sec=cfg.vol_horizon_sec,
            cap=cfg.vol_cap,
            floor=cfg.vol_floor,
        )
        self.kappa_calib = TradingIntensityIndicator(
            sampling_length=cfg.kappa_sampling_length,
            min_samples=cfg.kappa_min_samples,
        )

        self.inventory: float = 0.0
        self.session_start: int = int(time.time() * 1000)
        self._tick_count: int = 0
        self._calib_tick: int = 0
        self._active_bid_id: Optional[int] = None
        self._active_ask_id: Optional[int] = None
        self._active_bid_px: Optional[float] = None
        self._active_ask_px: Optional[float] = None

        self._log: list[dict] = []
        self._fill_log: list[dict] = []
        self._quote_log: list[dict] = []
        self._quotes_placed: int = 0
        self._fills_bid: int = 0
        self._fills_ask: int = 0

    def _current_t(self, now_ms: int) -> float:
        elapsed_ms = now_ms - self.session_start
        session_ms = self.cfg.session_minutes * 60_000
        return (elapsed_ms % session_ms) / session_ms

    def _process_fills(self, fills) -> None:
        for fill in fills:
            if fill.side == "buy":
                self.inventory += fill.size
                self._fills_bid += 1
            else:
                self.inventory -= fill.size
                self._fills_ask += 1
            self._fill_log.append({
                "timestamp": fill.timestamp,
                "side":      fill.side,
                "price":     fill.price,
                "size":      fill.size,
                "fee":       fill.fee,
                "inventory": self.inventory,
            })
            logger.info(f"FILL {fill.side} {fill.size} BTC @ {fill.price:.2f}  fee=${fill.fee:.4f}")

    def _quotes_stale(self, new_bid, new_ask) -> bool:
        tick = self.cfg.tick_size
        bid_moved = (new_bid is None) != (self._active_bid_px is None) or (
            new_bid is not None and self._active_bid_px is not None and
            abs(new_bid - self._active_bid_px) >= tick
        )
        ask_moved = (new_ask is None) != (self._active_ask_px is None) or (
            new_ask is not None and self._active_ask_px is not None and
            abs(new_ask - self._active_ask_px) >= tick
        )
        return bid_moved or ask_moved

    def _cancel_stale_quotes(self) -> None:
        if self._active_bid_id is not None:
            self.exchange.cancel_order(self._active_bid_id)
            self._active_bid_id = None
            self._active_bid_px = None
        if self._active_ask_id is not None:
            self.exchange.cancel_order(self._active_ask_id)
            self._active_ask_id = None
            self._active_ask_px = None

    def _place_quotes(self, bid_price, ask_price, timestamp: int) -> None:
        size = self.cfg.order_size
        tick = self.cfg.tick_size
        if bid_price is not None:
            bid_price = math.floor(bid_price / tick) * tick
            self._active_bid_id = self.exchange.place_limit_order("buy", bid_price, size, timestamp)
            self._active_bid_px = bid_price
            self._quotes_placed += 1
            self._quote_log.append({"timestamp": timestamp, "side": "bid", "price": bid_price, "size": size})
            logger.info(f"ORDER  buy  {size} BTC @ {bid_price:.2f}")
        if ask_price is not None:
            ask_price = math.ceil(ask_price / tick) * tick
            self._active_ask_id = self.exchange.place_limit_order("sell", ask_price, size, timestamp)
            self._active_ask_px = ask_price
            self._quotes_placed += 1
            self._quote_log.append({"timestamp": timestamp, "side": "ask", "price": ask_price, "size": size})
            logger.info(f"ORDER  sell {size} BTC @ {ask_price:.2f}")

    def on_tick(self, row: dict) -> None:
        ts = row["timestamp"]
        mid = (row["bid_0_price"] + row["ask_0_price"]) / 2.0

        self.kappa_calib.update_mid(row["bid_0_price"], row["ask_0_price"])

        sigma = self.vol_est.update(mid, ts)
        if not self.vol_est.ready:
            if self._tick_count % 10 == 0:
                logger.info(f"calculating volatility... {self.vol_est.samples}/{self.vol_est.warmup}")
            self._tick_count += 1
            return

        self._calib_tick += 1
        if self._calib_tick % self.cfg.kappa_recalib_ticks == 0:
            self.kappa_calib.flush_sample(ts)
            if self.kappa_calib.ready:
                self.cfg.kappa = self.kappa_calib.kappa
                logger.info(f"[calib] κ={self.cfg.kappa:.4f}  α={self.kappa_calib.alpha:.4f}")

        t = self._current_t(ts)

        fills = self.exchange.check_fills(row)
        self._process_fills(fills)
        if fills:
            self._cancel_stale_quotes()

        self._tick_count += 1
        if self._tick_count % self.cfg.quote_refresh_ticks == 0:
            q = self.inventory - self.cfg.target_inventory
            bid_px, ask_px = self.quoter.quotes(mid, q, sigma, t)

            if self._quotes_stale(bid_px, ask_px):
                self._cancel_stale_quotes()
                self._place_quotes(bid_px, ask_px, ts)

            if self._tick_count % 500 == 0:
                tick = self.cfg.tick_size
                r = self.quoter.reservation_price(mid, q, sigma, t)
                half = self.quoter.optimal_spread(mid, sigma, t)
                pnl = self.exchange.realized_pnl(self.inventory, mid)
                bid_display = round(math.floor(bid_px / tick) * tick, 2) if bid_px else 'N/A'
                ask_display = round(math.ceil(ask_px / tick) * tick, 2) if ask_px else 'N/A'
                fill_rate = (self._fills_bid + self._fills_ask) / max(self._quotes_placed, 1) * 100
                logger.info(
                    f"\n"
                    f"  Parameter        Value\n"
                    f"  {'─'*30}\n"
                    f"  mid price        {mid:.2f}\n"
                    f"  bid / ask        {bid_display} / {ask_display}\n"
                    f"  half spread      ${half:.2f}\n"
                    f"  volatility       ${sigma:.4f}\n"
                    f"  kappa            {self.cfg.kappa:.3f}\n"
                    f"  inventory        {self.inventory:+.4f} BTC\n"
                    f"  unrealized pnl   ${pnl:+.4f}\n"
                    f"  fills            {self._fills_bid}b / {self._fills_ask}a\n"
                    f"  fill rate        {fill_rate:.1f}%"
                )

        self._log.append({
            "timestamp": ts,
            "mid":       mid,
            "inventory": self.inventory,
            "sigma":     sigma,
            "kappa":     self.cfg.kappa,
            "t":         t,
        })

    def run(self, df: pd.DataFrame) -> dict:
        logger.info(f"Running backtest on {len(df)} ticks…")
        for row in df.to_dict("records"):
            self.on_tick(row)
        self.exchange.cancel_all()
        final_mid = (df.iloc[-1]["bid_0_price"] + df.iloc[-1]["ask_0_price"]) / 2.0
        summary = self.exchange.summary()
        summary["realized_pnl"] = round(self.exchange.realized_pnl(self.inventory, final_mid), 6)
        summary["final_inventory"] = round(self.inventory, 6)
        return summary

    def log_df(self) -> pd.DataFrame:
        return pd.DataFrame(self._log)

    def fill_log_df(self) -> pd.DataFrame:
        return pd.DataFrame(self._fill_log)

    def quote_log_df(self) -> pd.DataFrame:
        return pd.DataFrame(self._quote_log)


async def run_live(symbol: str = "btcusdt", cfg: ASConfig = None):
    if cfg is None:
        cfg = ASConfig()

    from orderbook import OrderBookManager
    import websockets
    import json

    mm = MarketMaker(cfg)
    manager = OrderBookManager(symbol)

    logger.info(f"Running A-S market maker bot with γ={cfg.gamma}  κ={cfg.kappa}")

    async def depth_loop():
        ws_url = f"wss://fstream.binance.com/ws/{symbol}@depth@100ms"
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
            for _ in range(10):
                manager.buffer.append(json.loads(await ws.recv()))
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, manager.fetch_snapshot)
            manager.apply_buffered_updates(manager.buffer)
            logger.info(f"Order book initialized for {symbol.upper()}")
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("u", 0) <= manager.last_update_id:
                    continue
                manager.apply_update(msg)
                row = manager.flatten()
                if row:
                    mm.on_tick(row)

    async def trade_loop():
        ws_url = f"wss://fstream.binance.com/market/ws/{symbol}@aggTrade"
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
            async for msg_raw in ws:
                msg = json.loads(msg_raw)
                mm.kappa_calib.on_trade(float(msg["p"]), float(msg["q"]), int(msg["T"]))

    await asyncio.gather(depth_loop(), trade_loop())


def run_backtest(parquet_path: str, cfg: ASConfig = None) -> dict:
    if cfg is None:
        cfg = ASConfig()

    if not os.path.exists(parquet_path):
        from stream import stream
        logger.info(f"No data at {parquet_path}, collecting...")
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df = asyncio.run(stream("btcusdt", n_rows=10_000))
        df.to_parquet(parquet_path)
        logger.info(f"Saved {len(df)} rows → {parquet_path}")

    df = pd.read_parquet(parquet_path)
    mm = MarketMaker(cfg)
    summary = mm.run(df)
    logger.info("── Backtest Summary ──────────────────────")
    for k, v in summary.items():
        logger.info(f"  {k}: {v}")
    return summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["live", "backtest"])
    args = parser.parse_args()
    cfg = ASConfig()
    if args.mode == "live":
        asyncio.run(run_live("btcusdt", cfg))
    else:
        run_backtest("data/raw/book_1777480982.parquet", cfg)
