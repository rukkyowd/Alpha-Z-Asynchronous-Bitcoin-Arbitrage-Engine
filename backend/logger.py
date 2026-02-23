import logging
import json
from datetime import datetime

# Setup professional logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("data/system_events.log"),
        logging.StreamHandler()
    ]
)

class QuantLogger:
    @staticmethod
    def log_trade(trade_data):
        # Professional trade logging includes fees and slippage
        with open("data/detailed_trades.jsonl", "a") as f:
            f.write(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                **trade_data
            }) + "\n")

    @staticmethod
    def log_event(event_type, details, level="info"):
        msg = f"[{event_type.upper()}] {details}"
        if level == "error": logging.error(msg)
        elif level == "warning": logging.warning(msg)
        else: logging.info(msg)