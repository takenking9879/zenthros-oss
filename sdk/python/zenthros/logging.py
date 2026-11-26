# SPDX-License-Identifier: Apache-2.0

"""Single logger factory. Always logger, never print()."""

import logging
import os
import sys

_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# What a person at a notebook reads: one short line, and only what asks
# something of them (R9). The job's own log keeps _FORMAT.
_USER_FORMAT = "zenthros: %(message)s"
# ZENTHROS_LOG_LEVEL=INFO (or DEBUG) brings back every line, full format.
LEVEL_ENV = "ZENTHROS_LOG_LEVEL"


def create_logger(name, log_file=None, user=False):
    """Return a named logger with a single StreamHandler (and optional file).

    Console is INFO+ (DEBUG with ZENTHROS_LOG_LEVEL=DEBUG). File, if given,
    is ERROR+ only. user=True is a logger of the notebook side: its console
    shows WARNING+ as "zenthros: <message>", unless ZENTHROS_LOG_LEVEL names
    another level. It does not propagate: an application's own root handler
    would print each line twice.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    console = logging.StreamHandler(sys.stderr)
    named = logging.getLevelName(os.environ.get(LEVEL_ENV, "").strip().upper() or "-")
    level = named if isinstance(named, int) else (logging.WARNING if user else logging.INFO)
    console.setLevel(level)
    # Below WARNING a person asked for the detail: they get it whole.
    console.setFormatter(logging.Formatter(_USER_FORMAT if user and level >= logging.WARNING else _FORMAT))
    logger.addHandler(console)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(file_handler)
    return logger
