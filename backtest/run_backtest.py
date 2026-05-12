import sys
from backtest.replay_engine import ReplayEngine
from backtest.metrics import summarize

path = sys.argv[1] if len(sys.argv) > 1 else "data/latest.parquet"

engine  = ReplayEngine()
results = engine.run(path)
summarize(engine, results)