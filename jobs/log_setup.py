import logging
import os

import structlog


def setup(name: str) -> structlog.stdlib.BoundLogger:
    fmt = os.getenv("LOG_FORMAT", "console").lower()
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level, logging.INFO),
    )

    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(name)
