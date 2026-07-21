"""Injectable clock.

``Clock`` is a Protocol so tests and deterministic replay can supply a fixed
time source. The external contract is Unix epoch seconds (``float``); an
implementation may use ``datetime`` internally.
"""

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    """Wall-clock implementation backed by ``time.time()``."""

    def now(self) -> float:
        return time.time()
