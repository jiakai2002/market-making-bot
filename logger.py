import logging
import os
os.makedirs("logs", exist_ok=True)
file = logging.FileHandler("logs/market_maker.log")

def get_logger(name: str = __name__) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        # console handler — suppress INFO, only show WARNING+
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        console.setFormatter(logging.Formatter(fmt="%(asctime)s - %(message)s", datefmt="%H:%M:%S"))

        # file handler — everything
        file = logging.FileHandler("market_maker.log")
        file.setLevel(logging.INFO)
        file.setFormatter(logging.Formatter(fmt="%(asctime)s - %(message)s", datefmt="%H:%M:%S"))

        logger.addHandler(console)
        logger.addHandler(file)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger