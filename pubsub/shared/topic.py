"""NATS-style subject validation and pattern matching.

Subjects are dot-separated tokens. Subscription patterns may use ``*`` (exactly
one token) and ``>`` (tail wildcard, final token only). Wildcards must be whole
tokens. Empty tokens are invalid. The core enforces no length/count limits.
"""

TOKEN_WILDCARD = "*"
TAIL_WILDCARD = ">"


class TopicError(ValueError):
    """Raised for an invalid subject or subscription pattern."""


def _split(subject: str) -> list[str]:
    if subject == "":
        raise TopicError("subject is empty")
    tokens = subject.split(".")
    for token in tokens:
        if token == "":
            raise TopicError(f"empty token in subject {subject!r}")
    return tokens


def validate_subject(subject: str) -> list[str]:
    """Validate a concrete published subject. Wildcards are not allowed."""
    tokens = _split(subject)
    for token in tokens:
        if TOKEN_WILDCARD in token or TAIL_WILDCARD in token:
            raise TopicError(
                f"published subject may not contain wildcards: {subject!r}"
            )
    return tokens


def validate_pattern(pattern: str) -> list[str]:
    """Validate a subscription pattern; ``>`` only as final whole token."""
    tokens = _split(pattern)
    last = len(tokens) - 1
    for i, token in enumerate(tokens):
        if token == TAIL_WILDCARD:
            if i != last:
                raise TopicError(f"'>' must be the final token: {pattern!r}")
        elif token == TOKEN_WILDCARD:
            continue
        elif TAIL_WILDCARD in token or TOKEN_WILDCARD in token:
            raise TopicError(f"wildcard must be a whole token: {token!r}")
    return tokens


def matches_tokens(pattern_tokens: list[str], subject: str) -> bool:
    """Match a subject against an already-validated, pre-split pattern.

    The hot fanout path uses this so a pattern validated once at subscribe time
    is not re-validated per published message. ``>`` matches one or more
    remaining tokens (never zero), consistent with NATS semantics.
    """
    sub = _split(subject)
    for i, token in enumerate(pattern_tokens):
        if token == TAIL_WILDCARD:
            return len(sub) > i
        if i >= len(sub):
            return False
        if token == TOKEN_WILDCARD:
            continue
        if token != sub[i]:
            return False
    return len(pattern_tokens) == len(sub)


def matches(pattern: str, subject: str) -> bool:
    """True if ``subject`` matches subscription ``pattern`` (validates ``pattern``)."""
    return matches_tokens(validate_pattern(pattern), subject)
