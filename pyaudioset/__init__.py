import csv
import logging
import pathlib
import sys

import structlog


def stringify_paths(logger, name, event_dict):
    for key, value in event_dict.items():
        if isinstance(value, pathlib.PurePath):
            event_dict[key] = str(value)

    return event_dict


def setup_logging():
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
    logging.getLogger("nmslib").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            stringify_paths,
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer()  # <===
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


setup_logging()
