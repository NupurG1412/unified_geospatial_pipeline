import logging
import os
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")

def setup_logger():

    logger = logging.getLogger()

    if not logger.handlers:

        print("Initializing logging system...")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        logging.info("Logger initialized")


def get_logger(name):
    setup_logger()
    return logging.getLogger(name)