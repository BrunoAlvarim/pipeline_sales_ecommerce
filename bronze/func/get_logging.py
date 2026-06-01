import logging
import sys


def logger(name: str) -> logging.Logger:

    _logger = logging.getLogger(name)

    _logger.setLevel(logging.INFO)

    _logger.propagate = False

    if not _logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
    return _logger

def get_logger(name: str) -> logging.Logger:
    """Alias de logger() para compatibilidade com projetos que usam get_logger."""
    return logger(name)


def configure_logging() -> None:
    """No-op: configuração já aplicada individualmente em cada get_logger/logger."""
    pass