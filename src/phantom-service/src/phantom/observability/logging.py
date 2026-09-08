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
holds every logger OUTSIDE Phantom's own tree at
:data:`_DEPENDENCY_LOG_FLOOR` (see that constant for why the boundary is a
default rather than a list of named dependencies).
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping
from typing import Final

from phantom.config.settings import ObservabilityCfg

logger = logging.getLogger(__name__)

_BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._\-]+")
_REDACTED = "Bearer <redacted>"
_SENSITIVE_REDACTED = "<redacted>"

# The level every logger that is NOT Phantom's own is held at, whatever the
# operator sets ``observability.log_level`` to.
#
# This used to be an ALLOWLIST of three named dependency loggers (aiosqlite,
# httpx, httpcore) chosen because each was known to interpolate a secret or a
# sensitive URL through a non-string arg that string-level redaction cannot
# reach. An allowlist is the wrong shape for a leak boundary: it is only as
# complete as the last audit. botocore was the proof - its signer writes the
# live STS session token, the derived signature and the full canonical
# request into a DEBUG record, and because ``botocore.auth`` is NOTSET it
# inherited the root level and reached EVERY sink, the persistent file sink
# included, on any deployment running at DEBUG.
#
# So the boundary is inverted: third-party loggers are capped by DEFAULT and
# Phantom's own tree (:data:`_PHANTOM_LOGGER_NAMES`) is the explicit
# exception that runs at the operator's level. A dependency added tomorrow is
# inside the boundary without anyone remembering to add it. WARNING is the
# floor because it is what the previous allowlist already held its two
# noisiest members at, and because a dependency's WARNING and ERROR records
# are the ones an operator actually needs. The production no-leak guard
# (tests/e2e/test_production_log_no_leak.py) enforces the boundary end to end.
_DEPENDENCY_LOG_FLOOR: Final[int] = logging.WARNING

# The loggers carrying PHANTOM'S OWN records, which run at the
# operator-configured level. ``phantom`` is the package tree (every module
# uses ``logging.getLogger(__name__)``). ``__main__`` is the launcher:
# ``python -m phantom`` executes ``phantom/__main__.py`` with ``__name__``
# set to ``"__main__"``, so its logger sits outside the package tree.
_PHANTOM_LOGGER_NAMES: Final[tuple[str, ...]] = ("phantom", "__main__")


class BearerRedactionFilter(logging.Filter):
    """Strip Bearer tokens from every formatted log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact in-place and pass the record through."""
        if isinstance(record.msg, str):
            record.msg = _BEARER_RE.sub(_REDACTED, record.msg)
        args = record.args
        if isinstance(args, Mapping):
            # ``%(name)s``-style formatting. ``LogRecord.__init__`` stores a
            # single non-empty Mapping arg AS THE MAPPING, not wrapped in a
            # tuple, and ``getMessage`` then evaluates ``msg % mapping``.
            # Coercing it to a tuple made that ``msg % (mapping,)``, which
            # raises "format requires a mapping": the handler emitted nothing,
            # printed "--- Logging error ---" to stderr, and THE RECORD WAS
            # LOST. This filter is attached to every root handler, so that was
            # every mapping-style record from every dependency.
            record.args = {key: _redact_arg(value) for key, value in args.items()}
        elif args:
            record.args = tuple(_redact_arg(arg) for arg in args)
        return True


def _redact_arg(arg: object) -> object:
    """Scrub ``Bearer <token>`` from one interpolation argument.

    Args:
        arg: One positional or mapping value from ``LogRecord.args``.

    Returns:
        The argument with bearer substrings replaced when it is a string,
        otherwise the argument unchanged (its ``__str__`` runs at format time,
        past every filter, which is what the dependency-logger floor covers).
    """
    return _BEARER_RE.sub(_REDACTED, arg) if isinstance(arg, str) else arg


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

    ``log_level`` applies to Phantom's own loggers. Every other logger is
    held at :data:`_DEPENDENCY_LOG_FLOOR`, so an operator DEBUG turns up
    Phantom's own detail without opening a dependency's secret-bearing
    records; see that constant for why the boundary is a default rather than
    a list of names.

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
    # Default-deny for third-party records, explicit-allow for Phantom's own.
    # The ROOT level is the effective level of every logger that sets none of
    # its own (which is every dependency), so holding root at the floor caps
    # them all; naming Phantom's own loggers then lifts exactly those back to
    # what the operator asked for. ``max`` rather than a plain assignment so
    # an operator who asks for ERROR gets ERROR everywhere rather than having
    # the floor loosen their choice.
    operator_level = logging.getLevelNamesMapping()[observability.log_level]
    root.setLevel(max(operator_level, _DEPENDENCY_LOG_FLOOR))
    for name in _PHANTOM_LOGGER_NAMES:
        logging.getLogger(name).setLevel(operator_level)
    if file_error is not None:
        # Emitted AFTER installation so it lands in whichever sink did
        # install. With no sink it goes nowhere, which is self-consistent
        # with an operator who asked for no sinks and gave a bad path.
        logger.error(
            "log_to_file %r could not be opened (%s); continuing without the file sink",
            observability.log_to_file,
            file_error,
        )
