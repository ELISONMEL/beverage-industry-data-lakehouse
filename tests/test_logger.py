import logging

from src.utils.logger import get_logger


def test_get_logger_configura_logger_corretamente():
    logger_name = "test_logger_configuracao"

    logger = logging.getLogger(logger_name)
    logger.handlers.clear()

    result = get_logger(logger_name)

    assert result.name == logger_name
    assert result.level == logging.INFO
    assert len(result.handlers) == 1

    handler = result.handlers[0]

    assert isinstance(handler, logging.StreamHandler)
    assert handler.formatter is not None

    assert handler.formatter._fmt == (
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    logger.handlers.clear()


def test_get_logger_nao_duplica_handlers():
    logger_name = "test_logger_sem_duplicacao"

    logger = logging.getLogger(logger_name)
    logger.handlers.clear()

    first_logger = get_logger(logger_name)
    second_logger = get_logger(logger_name)

    assert first_logger is second_logger
    assert len(second_logger.handlers) == 1

    logger.handlers.clear()