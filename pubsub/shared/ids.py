"""Broker-internal identifiers.

Message, delivery, and subscription ids are internal correlation handles, not
security tokens, so they do not need ``uuid4``'s CSPRNG. A 128-bit draw from the
(non-cryptographic) ``random`` module is ~10x cheaper — no ``os.urandom`` syscall
per id — and per-delivery id minting sits on the fanout hot path. Collision
probability at 128 random bits is negligible for this use. Not time-ordered:
delivery ordering comes from ``created_at``, and these ids are only ever dict keys.
"""

import random


def new_id() -> str:
    """A 128-bit random id as 32 lowercase hex chars (same shape as ``uuid4().hex``)."""
    return "%032x" % random.getrandbits(128)
