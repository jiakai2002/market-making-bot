import math


def microprice(orderbook):
    bid_p, bid_q = orderbook.best_bid
    ask_p, ask_q = orderbook.best_ask

    return (bid_p * ask_q + ask_p * bid_q) / (bid_q + ask_q)


def orderbook_imbalance(orderbook, depth=5):
    bids = list(orderbook.bids.items())[:depth]
    asks = list(orderbook.asks.items())[:depth]

    bid_vol = sum(q for _, q in bids)
    ask_vol = sum(q for _, q in asks)

    total = bid_vol + ask_vol

    if total == 0:
        return 0.0

    return (bid_vol - ask_vol) / total


def trade_flow_imbalance(trades, n=50):
    recent = trades.recent(n)

    buy_vol = sum(t["qty"] for t in recent if t["side"] == "BUY")
    sell_vol = sum(t["qty"] for t in recent if t["side"] == "SELL")

    total = buy_vol + sell_vol

    if total == 0:
        return 0.0

    return (buy_vol - sell_vol) / total


def mid_return(curr_mid, prev_mid):
    if prev_mid is None or prev_mid <= 0:
        return 0.0

    return math.log(curr_mid / prev_mid)