import logging
import sys


def setup_logging() -> None:
    """
    Configure application-wide logging for pipeline execution.

    Logs are written to standard output with timestamp, level,
    logger name, and message.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger instance by name.

    Args:
        name: Logger name, typically __name__ from the calling module.

    Returns:
        logging.Logger: Logger instance for the requested name.
    """
    return logging.getLogger(name)