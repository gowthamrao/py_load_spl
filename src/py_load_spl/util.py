import logging
import sys

from pythonjsonlogger import jsonlogger


def setup_logging(log_level: str, log_format: str):
    """
    Configures the root logger for the application.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level.upper())

    # Remove any existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)

    if log_format.lower() == "json":
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s"
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logging.getLogger(__name__).info(
        f"Logging configured. Level: {log_level}, Format: {log_format}"
    )
