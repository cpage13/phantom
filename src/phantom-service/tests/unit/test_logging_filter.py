"""Unit tests for the SensitiveCaptureRedactor filter (plan §12).

The chain executor emits structured DEBUG log records with two extras:
``captures`` (the captured-value dict) and ``sensitive_captures`` (the
set of capture names declared ``sensitive=True``). This filter scrubs
the values in-place before the formatter sees them. Records without
both extras pass through unchanged so non-capture log lines stay
intact.
"""

from __future__ import annotations

import io
import logging

from phantom.observability import (
    BearerRedactionFilter,
    SensitiveCaptureRedactor,
    configure_logging,
)


def _format_record(filt: logging.Filter, record: logging.LogRecord) -> str:
    """Run ``filt`` against ``record`` and return the formatted message."""
    filt.filter(record)
    formatter = logging.Formatter("%(message)s :: captures=%(captures)s")
    return formatter.format(record)


def test_sensitive_capture_redacted() -> None:
    """A capture marked sensitive becomes ``<redacted>`` in formatted output."""
    captures: dict[str, dict[str, object]] = {
        "create_file": {
            "upload_url": "https://files.example.com/presigned?sig=secret",
        }
    }
    sensitive: dict[str, set[str]] = {"create_file": {"upload_url"}}
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.DEBUG,
        pathname="x.py",
        lineno=1,
        msg="chain step captured values",
        args=(),
        exc_info=None,
    )
    record.captures = captures
    record.sensitive_captures = sensitive

    formatted = _format_record(SensitiveCaptureRedactor(), record)
    assert "<redacted>" in formatted
    assert "https://files.example.com/presigned?sig=secret" not in formatted
    # And the in-place mutation is reflected on the captures dict for any
    # downstream filter/handler reading from the same reference.
    assert captures["create_file"]["upload_url"] == "<redacted>"


def test_non_sensitive_capture_not_redacted() -> None:
    """A non-sensitive capture passes through to the formatter unchanged."""
    captures: dict[str, dict[str, object]] = {
        "create_file": {"file_information": {"id": "abc-123"}}
    }
    sensitive: dict[str, set[str]] = {"create_file": set()}
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.DEBUG,
        pathname="x.py",
        lineno=1,
        msg="chain step captured values",
        args=(),
        exc_info=None,
    )
    record.captures = captures
    record.sensitive_captures = sensitive

    formatted = _format_record(SensitiveCaptureRedactor(), record)
    assert "abc-123" in formatted
    assert "<redacted>" not in formatted


def test_record_without_capture_extras_passes_through() -> None:
    """Records without ``captures`` / ``sensitive_captures`` are untouched."""
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="ordinary log line %s",
        args=("payload",),
        exc_info=None,
    )
    filt = SensitiveCaptureRedactor()
    assert filt.filter(record) is True
    assert record.getMessage() == "ordinary log line payload"


def test_bearer_still_redacted() -> None:
    """Bearer-redaction continues to work alongside capture redaction."""
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="upstream sent Bearer eyJ.value.here back",
        args=(),
        exc_info=None,
    )
    BearerRedactionFilter().filter(record)
    formatted = record.getMessage()
    assert "Bearer <redacted>" in formatted
    assert "eyJ.value.here" not in formatted


def test_configure_logging_installs_both_filters() -> None:
    """``configure_logging`` attaches both filters to the root handler."""
    # Snapshot existing root state so the test is restorable.
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        configure_logging("INFO")
        # The newest handler is the one configure_logging installed.
        assert root.handlers, "configure_logging must install at least one handler"
        handler = root.handlers[0]
        filter_classes = {type(f).__name__ for f in handler.filters}
        assert "BearerRedactionFilter" in filter_classes
        assert "SensitiveCaptureRedactor" in filter_classes
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_redactor_drives_end_to_end_through_log_handler() -> None:
    """Full path: logger.debug → filter → formatter → captured stream.

    Captures end-to-end behavior the executor relies on — even though the
    filter mutates the record in-place, the emitted output also carries
    ``<redacted>``.
    """
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s captures=%(captures)s"))
    handler.addFilter(SensitiveCaptureRedactor())
    test_logger = logging.getLogger("phantom.test_logging_filter.e2e")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    test_logger.debug(
        "chain step captured values",
        extra={
            "captures": {"step": {"url": "https://example.com/secret"}},
            "sensitive_captures": {"step": {"url"}},
        },
    )
    output = stream.getvalue()
    assert "<redacted>" in output
    assert "https://example.com/secret" not in output
