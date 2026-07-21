"""Retry backoff/budget guards.

The backoff is intentionally a pure function so its bounds and exponent cap can
be checked deterministically (injected ``rng``) without any async machinery.
These target the two ways backoff goes wrong: unbounded growth and off-by-one
budget accounting.
"""

import unittest

from pubsub.server.retry import RetryPolicy, full_jitter_backoff


class Backoff(unittest.TestCase):
    def test_lower_bound_is_zero(self) -> None:
        # Full jitter must be able to fire immediately (draw == 0).
        self.assertEqual(full_jitter_backoff(5, base=1.0, rng=lambda: 0.0), 0.0)

    def test_upper_bound_is_base_times_2_pow_attempt(self) -> None:
        # Within the cap, the ceiling is base * 2**attempt (draw == 1).
        self.assertEqual(full_jitter_backoff(3, base=1.0, rng=lambda: 1.0), 8.0)

    def test_exponent_is_capped(self) -> None:
        # Without a cap this delay would be astronomically large / overflow-prone.
        capped = full_jitter_backoff(1000, base=1.0, cap_exponent=10, rng=lambda: 1.0)
        self.assertEqual(capped, float(2**10))

    def test_never_exceeds_capped_ceiling(self) -> None:
        # Any attempt past the cap shares the same ceiling; none may exceed it.
        ceiling = 2.0 * (2**10)
        for attempt in (10, 11, 50, 500):
            with self.subTest(attempt=attempt):
                self.assertEqual(
                    full_jitter_backoff(attempt, base=2.0, rng=lambda: 1.0), ceiling
                )


class Budget(unittest.TestCase):
    def test_boundary_is_inclusive_at_max_attempts(self) -> None:
        # Off-by-one here means one too few or too many redeliveries before DLQ.
        policy = RetryPolicy(max_attempts=10)
        self.assertFalse(policy.exhausted(9))
        self.assertTrue(policy.exhausted(10))


if __name__ == "__main__":
    unittest.main()
