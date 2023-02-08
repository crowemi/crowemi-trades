import os
from logging import *

LOG_LEVEL = {
    "critical": CRITICAL,
    "fatal": FATAL,
    "error": ERROR,
    "warning": WARNING,
    "warn": WARN,
    "info": INFO,
    "debug": DEBUG,
}


def create_logger(name: str) -> Logger:
    log_level = os.getenv("LOG_LEVEL") if os.getenv("LOG_LEVEL", None) else "info"
    if log_level.lower() in LOG_LEVEL:
        log_level = LOG_LEVEL[log_level]
    else:
        raise Exception(f"Invalid log level. {log_level}")

    logger = getLogger(name)
    handler = FileHandler(f"./logs/{name}.log", mode="w")
    handler.setLevel(log_level)

    format = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(format)
    logger.addHandler(handler)
    return logger
