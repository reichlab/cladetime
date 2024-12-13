import os
import sys

import structlog

# must import Tree before CladeTime
from cladetime.tree import Tree
from cladetime.clade import Clade
from cladetime.cladetime import CladeTime
from cladetime.util.reference import _docker_installed

__all__ = ["Clade", "CladeTime", "Tree"]

# tells us package to consider DC a state
os.environ["DC_STATEHOOD"] = "1"
os.environ["DOCKER_ENABLED"] = str(_docker_installed())


def setup_logging():
    shared_processors = [
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.add_log_level,
    ]

    if sys.stderr.isatty():
        # If we're in a terminal, pretty print the logs.
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]
    else:
        # Otherwise, output logs in JSON format
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        cache_logger_on_first_use=True,
    )


setup_logging()
