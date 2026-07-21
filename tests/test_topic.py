"""Topic validation/matching failure-case guards.

Each test pins a rule whose violation would silently route messages wrong or
accept malformed subjects — not construction smoke tests.
"""

import unittest

from pubsub.shared import topic


class ValidateSubject(unittest.TestCase):
    def test_empty_subject_rejected(self) -> None:
        # An empty subject has no tokens to route on.
        with self.assertRaises(topic.TopicError):
            topic.validate_subject("")

    def test_empty_token_rejected(self) -> None:
        # Empty tokens make dot-boundaries ambiguous; must be rejected.
        for subject in ("a..b", ".a", "a."):
            with self.subTest(subject=subject):
                with self.assertRaises(topic.TopicError):
                    topic.validate_subject(subject)

    def test_wildcard_in_published_subject_rejected(self) -> None:
        # Publishers address one concrete subject; a wildcard would fan it out.
        for subject in ("a.*", "a.>", "a.*.b"):
            with self.subTest(subject=subject):
                with self.assertRaises(topic.TopicError):
                    topic.validate_subject(subject)


class ValidatePattern(unittest.TestCase):
    def test_tail_wildcard_must_be_final(self) -> None:
        # `>` past the end would let following tokens be silently ignored.
        with self.assertRaises(topic.TopicError):
            topic.validate_pattern("a.>.b")

    def test_partial_token_wildcard_rejected(self) -> None:
        # Wildcards must be whole tokens; a partial one is a config error.
        for pattern in ("a.f*o", "a.b>", "*x"):
            with self.subTest(pattern=pattern):
                with self.assertRaises(topic.TopicError):
                    topic.validate_pattern(pattern)


class Matches(unittest.TestCase):
    def test_single_token_wildcard_spans_exactly_one_token(self) -> None:
        # `*` is one token: it must not swallow a deeper subject.
        self.assertTrue(topic.matches("a.*", "a.b"))
        self.assertFalse(topic.matches("a.*", "a.b.c"))

    def test_tail_wildcard_requires_at_least_one_token(self) -> None:
        # `a.>` matches deeper subjects but not the bare prefix `a`.
        self.assertTrue(topic.matches("a.>", "a.b"))
        self.assertTrue(topic.matches("a.>", "a.b.c.d"))
        self.assertFalse(topic.matches("a.>", "a"))

    def test_length_mismatch_without_wildcard_does_not_match(self) -> None:
        # A literal pattern must match token count exactly.
        self.assertFalse(topic.matches("a.b", "a.b.c"))
        self.assertFalse(topic.matches("a.b.c", "a.b"))


if __name__ == "__main__":
    unittest.main()
