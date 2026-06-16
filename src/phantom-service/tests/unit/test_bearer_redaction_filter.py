"""Unit tests for the bearer redaction filter (ADR-004).

Replaces the previous ``test_observability.py``, which exercised the
since-removed metrics and tracing surface. The remaining contract is
narrower: every Bearer token in a log record (whether in the message
body or in a string ``args`` value) must be scrubbed before the record
reaches a formatter.
"""

from __future__ import annotations

import logging

from phantom.observability import BearerRedactionFilter


def test_bearer_in_message_redacted() -> None:
    """A Bearer token in the formatted message is scrubbed."""
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="Authorization: Bearer abc.def.ghi-xyz",
        args=(),
        exc_info=None,
    )
    BearerRedactionFilter().filter(record)
    assert "Bearer <redacted>" in record.getMessage()
    assert "abc.def.ghi-xyz" not in record.getMessage()


def test_bearer_in_dict_arg_redacted() -> None:
    """A Bearer header passed via a string ``args`` slot is also scrubbed.

    Phantom's structured-logging path interpolates header dicts via
    ``%s`` style formatting; this test exercises the args-side codepath
    on the filter by routing the header through a ``%s`` placeholder.
    """
    headers = {"Authorization": "Bearer secret.value"}
    record = logging.LogRecord(
        name="phantom.test",
        level=logging.INFO,
        pathname="x.py",
        lineno=1,
        msg="headers=%s",
        args=(str(headers),),
        exc_info=None,
    )
    BearerRedactionFilter().filter(record)
    formatted = record.getMessage()
    assert "secret.value" not in formatted
    assert "Bearer <redacted>" in formatted
