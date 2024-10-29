import os
import subprocess
import sys

import structlog

from cladetime.cladetime import CladeTime
from cladetime.tree import Tree

__all__ = ["CladeTime", "Tree"]

# tells us package to consider DC a state
os.environ["DC_STATEHOOD"] = "1"

# whether or not Docker-dependent features are enabled
DOCKER_FEATURES_ENABLED = False


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


def check_docker_installed():
    """Check if Docker is installed and running."""
    try:
        subprocess.run(["docker", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        docker_enabled = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        msg = (
            "WARNING: Docker is not installed on this machine, or it is not currently running.\n"
            "Cladetime features that require Docker (for example, custom clade assignment) will "
            "not be available."
        )
        docker_enabled = False
        print(msg)

    return docker_enabled


setup_logging()
DOCKER_FEATURES_ENABLED = check_docker_installed()
