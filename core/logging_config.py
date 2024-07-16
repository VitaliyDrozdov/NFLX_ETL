import logging
import sys


def setup_logging():
    """ "Настройки логирования."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - func(%(funcName)s)- [%(levelname)s] - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler("log.log", mode="w")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


logger = setup_logging()
