"""Colour-aware stdlib logging helpers (stdlib-only, no external colour libs).

Keeps the richer terminal detection (TTY, editors, docker, Windows terminals)
while honouring the spec's ``FORCE_COLOUR`` / ``NO_COLOUR`` overrides, plain
formatter fallback, and ``get_logger`` convenience wrapper.
"""

import logging
import os
import sys
from typing import Any

# Plain, colour-free format used when colour is disabled.
PLAIN_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def is_docker() -> bool:
    path = "/proc/self/cgroup"
    if os.path.exists("/.dockerenv"):
        return True
    if not os.path.isfile(path):
        return False
    with open(path) as f:
        return any("docker" in line for line in f)


def _env_flag(name: str) -> bool:
    return os.environ.get(name) == "1"


def stream_supports_colour(stream: Any) -> bool:
    # NO_COLOUR wins over everything (spec precedence); accept the common
    # NO_COLOR spelling too.
    if _env_flag("NO_COLOUR") or _env_flag("NO_COLOR"):
        return False
    if _env_flag("FORCE_COLOUR") or _env_flag("FORCE_COLOR"):
        return True

    is_a_tty = hasattr(stream, "isatty") and stream.isatty()

    # special case editors
    if "PYCHARM_HOSTED" in os.environ or os.environ.get("TERM_PROGRAM") == "vscode":
        return is_a_tty

    if sys.platform != "win32":
        return is_a_tty or is_docker()

    return is_a_tty and ("ANSICON" in os.environ or "WT_SESSION" in os.environ)


class ColourFormatter(logging.Formatter):
    LEVEL_COLOURS = (
        (logging.DEBUG, "\x1b[40;1m"),
        (logging.INFO, "\x1b[34;1m"),
        (logging.WARNING, "\x1b[33;1m"),
        (logging.ERROR, "\x1b[31m"),
        (logging.CRITICAL, "\x1b[41m"),
    )

    FORMATS = {
        level: logging.Formatter(
            f"\x1b[30;1m%(asctime)s\x1b[0m {colour}%(levelname)-8s\x1b[0m \x1b[35m%(name)s\x1b[0m %(message)s",
            _DATEFMT,
        )
        for level, colour in LEVEL_COLOURS
    }

    def format(self, record: logging.LogRecord) -> str:
        formatter = self.FORMATS.get(record.levelno)
        if formatter is None:
            formatter = self.FORMATS[logging.DEBUG]

        # Preserve any exc_text a prior handler set; restore it afterwards
        # instead of clobbering the shared record with None.
        prior_exc_text = record.exc_text
        if record.exc_info:
            text = formatter.formatException(record.exc_info)
            record.exc_text = f"\x1b[31m{text}\x1b[0m"

        output = formatter.format(record)

        record.exc_text = prior_exc_text
        return output


# Spec spelling alias.
ColoredFormatter = ColourFormatter


def make_formatter(stream: Any = None) -> logging.Formatter:
    """Colour formatter when ``stream`` supports colour, else a plain one."""
    target = stream if stream is not None else sys.stderr
    if stream_supports_colour(target):
        return ColourFormatter()
    return logging.Formatter(PLAIN_FORMAT, _DATEFMT)


def get_logger(name: str) -> logging.Logger:
    """Convenience wrapper for the package logger hierarchy."""
    return logging.getLogger(name)


def configure_logging(
    *,
    level: int = logging.INFO,
    stream: Any = None,
    durable: logging.Handler | None = None,
) -> None:
    """Install the package's logging handlers on the ``pubsub`` root logger.

    Attaches a colour-aware ``StreamHandler`` (colour auto-detected via
    ``make_formatter``) and, when given, a ``durable`` handler for persistence.
    Idempotent: existing ``pubsub`` handlers are cleared first. Propagation to
    the root logger is disabled so records are not double-emitted by a
    ``basicConfig`` handler an application may also have installed.
    """
    root = logging.getLogger("pubsub")
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(level)

    target = stream if stream is not None else sys.stderr
    console = logging.StreamHandler(target)
    console.setFormatter(make_formatter(target))
    root.addHandler(console)

    if durable is not None:
        if durable.formatter is None:
            durable.setFormatter(logging.Formatter(PLAIN_FORMAT, _DATEFMT))
        root.addHandler(durable)

    root.propagate = False
