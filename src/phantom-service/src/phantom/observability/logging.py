"""Structured logging with bearer + sensitive-capture redaction.

Configures stdlib logging with two filters:

* :class:`BearerRedactionFilter` scrubs ``Bearer <token>`` substrings
  from every formatted log record so admin output never leaks tokens
  (ADR-004).
* :class:`SensitiveCaptureRedactor` redacts captured values whose
  declaring ``ChainCapture.sensitive`` flag is ``True`` (e.g., an
  upstream's presigned-PUT URL). Components that log captured-value dicts emit
  records with structured ``extra`` fields; this filter mutates the
  record in-place before the formatter sees it.

String-level filters cannot reach values that dependencies interpolate
through non-string args at format time, so :func:`configure_logging` also
caps the known secret-bearing dependency loggers (``_DEPENDENCY_LOG_CAPS``).
"""

from __future__ import annotations

import logging
import re
from typing import Final

_BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._\-]+")
_REDACTED = "Bearer <redacted>"
_SENSITIVE_REDACTED = "<redacted>"

# Dependency loggers whose records interpolate secret-bearing values by
# design and therefore bypass string-level redaction. aiosqlite's DEBUG
# statement log formats the bound-parameter tuple through a non-string
# functools.partial arg (bearer tokens on the token-cache INSERT, credential
# JSON on the store write), so BearerRedactionFilter never sees the token as
# a string. httpx's INFO request line and httpcore's DEBUG wire chatter
# carry full request URLs, and a presigned upload URL is a sensitive
# capture. Capping these loggers is the leak boundary for dependency-
# authored records; Phantom's own records stay at the operator-configured
# level and pass through the redaction filters. The production no-leak
# guard (tests/e2e/test_production_log_no_leak.py) enforces this boundary.
_DEPENDENCY_LOG_CAPS: Final[dict[str, int]] = {
    "aiosqlite": logging.INFO,
    "httpx": logging.WARNING,
    "httpcore": logging.WARNING,
}


class BearerRedactionFilter(logging.Filter):
    """Strip Bearer tokens from every formatted log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact in-place and pass the record through."""
        if isinstance(record.msg, str):
            record.msg = _BEARER_RE.sub(_REDACTED, record.msg)
        if record.args:
            redacted_args: list[object] = []
            for arg in record.args if isinstance(record.args, tuple) else (record.args,):
                if isinstance(arg, str):
                    redacted_args.append(_BEARER_RE.sub(_REDACTED, arg))
                else:
                    redacted_args.append(arg)
            record.args = tuple(redacted_args)
        return True


class SensitiveCaptureRedactor(logging.Filter):
    """Filter that redacts sensitive captured-value strings in log output.

    Contract: components that log captured-value dicts (the chain
    executor's capture-extraction path is the canonical site) emit log
    records with two structured extras::

        logger.debug(
            "captured value",
            extra={
                "captures": {<step_name>: {<capture_name>: <value>, ...}, ...},
                "sensitive_captures": {<step_name>: {<capture_name>, ...}, ...},
            },
        )

    For each ``(step, capture)`` pair listed in
    ``record.sensitive_captures``, this filter replaces the value in
    ``record.captures`` with the literal string ``"<redacted>"`` BEFORE
    the formatter sees the record. Other args/extras pass through
    unchanged.

    Records without both extras are passed through with no change —
    non-capture log records are unaffected; only the executor's
    capture-time log lines get redacted.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact sensitive captures in-place and pass the record through."""
        captures = getattr(record, "captures", None)
        sensitive = getattr(record, "sensitive_captures", None)
        if captures is None or sensitive is None:
            return True
        if not isinstance(captures, dict) or not isinstance(sensitive, dict):
            return True
        for step_name, sensitive_keys in sensitive.items():
            step_captures = captures.get(step_name)
            if not isinstance(step_captures, dict):
                continue
            for key in sensitive_keys:
                if key in step_captures:
                    step_captures[key] = _SENSITIVE_REDACTED
        return True


def configure_logging(level: str) -> None:
    """Apply redaction filters to the root logger.

    Both :class:`BearerRedactionFilter` and
    :class:`SensitiveCaptureRedactor` are installed; the bearer filter
    runs first so a ``Bearer <token>`` substring embedded inside a
    captured value still gets scrubbed before the capture-redactor
    inspects the record.

    Args:
        level: ``DEBUG`` | ``INFO`` | ``WARNING`` | ``ERROR``.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    handler.addFilter(BearerRedactionFilter())
    handler.addFilter(SensitiveCaptureRedactor())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # Bound the dependency loggers that embed secrets or sensitive URLs in
    # their own records (see _DEPENDENCY_LOG_CAPS). Applied after the root
    # level so an operator DEBUG never re-opens the dependency leak surface.
    for name, cap in _DEPENDENCY_LOG_CAPS.items():
        logging.getLogger(name).setLevel(cap)
