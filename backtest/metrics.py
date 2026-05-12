import numpy as np
import matplotlib.pyplot as plt


def sharpe(pnl_curve):
    r = np.diff(np.asarray(pnl_curve))
    return 0.0 if len(r) < 2 or r.std() == 0 else r.mean() / r.std()


def max_drawdown(pnl_curve):
    pnl  = np.asarray(pnl_curve)
    peak = np.maximum.accumulate(pnl)
    return (pnl - peak).min()


def fill_rate(fills, orders):
    return 0.0 if orders == 0 else fills / orders


def inventory_turnover(inv_curve):
    return np.abs(np.diff(np.asarray(inv_curve))).sum()


def plot(result_df):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax1.plot(result_df["pnl"])
    ax1.set_title("PnL")
    ax1.grid(True)
    ax2.plot(result_df["inventory"], color="orange")
    ax2.set_title("Inventory")
    ax2.grid(True)
    plt.tight_layout()
    plt.show()


def summarize(engine, result_df):
    pnl = result_df["pnl"].values
    inv = result_df["inventory"].values

    print("\nBACKTEST METRICS")
    print("=" * 40)
    print(f"Final PnL          : {pnl[-1]:.2f}")
    print(f"Sharpe             : {sharpe(pnl):.4f}")
    print(f"Max Drawdown       : {max_drawdown(pnl):.2f}")
    print(f"Fill Rate          : {fill_rate(engine.fills, engine.orders):.4f}")
    print(f"Inventory Turnover : {inventory_turnover(inv):.4f}")
    plot(result_df)