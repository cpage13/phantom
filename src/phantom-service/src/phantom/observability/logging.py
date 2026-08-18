"""Structured logging with bearer + sensitive-capture redaction.

:func:`configure_logging` owns the ROOT logger's sink set and its
filters. The sinks come from the whole ``observability`` block:
``log_to_stdout`` streams records to ``sys.stdout`` and ``log_to_file``
adds a secondary file sink. Neither configured is a legal operator
choice and means silence, installed as a single
:class:`logging.NullHandler`: without a handler, Python's
``logging.lastResort`` would emit WARNING and above to stderr with no
formatter and no filters, so the one configuration that looks like "no
logs" would be the only one able to print an unredacted bearer.

BOTH filters are attached to EVERY handler rather than to the logger,
which is the shape a multi-sink set has to preserve per sink: a handler
added without them is a silent leak, and the file sink is where that
matters most, since a console leak scrolls away while a file leak
persists for the retention of the volume (ADR-004).

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
import sys
from typing import Final

from phantom.config.settings import ObservabilityCfg

logger = logging.getLogger(__name__)

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

    Records without both extras are passed through with no change -
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


def configure_logging(observability: ObservabilityCfg) -> None:
    """Install the root logger's sinks and its redaction filters.

    Consumes the whole ``observability`` block so the two documented sink
    knobs are real: ``log_to_stdout`` streams records to ``sys.stdout`` and
    ``log_to_file`` adds a secondary file sink. BOTH filters are attached to
    EVERY handler; a file sink without the redaction pair would be an ADR-004
    leak with longer retention than the console. The bearer filter runs
    first so a ``Bearer <token>`` substring embedded inside a captured
    value still gets scrubbed before the capture-redactor inspects the
    record.

    The knobs are restart-required (ADR-013): this is called once in
    ``create_app`` and the reload path does not re-run it, because
    reloadable sinks would mean tearing down and rebuilding handlers
    under live workers for a knob an operator changes once per
    deployment.

    Args:
        observability: The validated ``observability`` settings block.
    """
    handlers: list[logging.Handler] = []
    file_error: OSError | None = None
    if observability.log_to_stdout:
        # sys.stdout is resolved at CALL time, not import time: a bare
        # StreamHandler() is stderr, which is what made the documented
        # default false, and taking the stream now also keeps the sink
        # observable under capsys, which replaces it per test.
        handlers.append(logging.StreamHandler(sys.stdout))
    if observability.log_to_file is not None:
        try:
            # delay=False (the default) on purpose: a bad path then fails
            # here at boot, where the operator is watching, rather than at
            # the first ERROR record at 3am.
            handlers.append(logging.FileHandler(observability.log_to_file, encoding="utf-8"))
        except OSError as exc:
            # A bad log path is recoverable config: keep the process running
            # on whatever sink remains and say so once. Phantom does NOT
            # create the parent directory; choosing where an operator's logs
            # land, and with what permissions, is not Phantom's call.
            file_error = exc
    if not handlers:
        # "No sinks" is a legal operator choice, but an EMPTY root handler
        # list is not: logging.lastResort would then emit WARNING and above
        # to stderr with no formatter and, decisively, NO redaction filters.
        # A NullHandler makes the choice mean silence instead of an
        # unredacted fallback channel (ADR-004).
        handlers.append(logging.NullHandler())
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    for handler in handlers:
        handler.setFormatter(formatter)
        # Bearer first: a Bearer substring inside a captured value must be
        # scrubbed before the capture redactor inspects the record.
        handler.addFilter(BearerRedactionFilter())
        handler.addFilter(SensitiveCaptureRedactor())
    root = logging.getLogger()
    root.handlers.clear()
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(observability.log_level)
    # Bound the dependency loggers that embed secrets or sensitive URLs in
    # their own records (see _DEPENDENCY_LOG_CAPS). Applied after the root
    # level so an operator DEBUG never re-opens the dependency leak surface.
    for name, cap in _DEPENDENCY_LOG_CAPS.items():
        logging.getLogger(name).setLevel(cap)
    if file_error is not None:
        # Emitted AFTER installation so it lands in whichever sink did
        # install. With no sink it goes nowhere, which is self-consistent
        # with an operator who asked for no sinks and gave a bad path.
        logger.error(
            "log_to_file %r could not be opened (%s); continuing without the file sink",
            observability.log_to_file,
            file_error,
        )
