"""Chain module — JSONPath wrapper, parser, executor.

Modules:

* :mod:`phantom.chain.jsonpath` — JSONPath compile/extract/scan helpers.
* :mod:`phantom.chain.parser` — envelope+body_refs parser.
* :mod:`phantom.chain.executor` — one-step execution primitive.
* :mod:`phantom.chain.query`: the byte-preserving query-string fold.
"""

from __future__ import annotations

from phantom.chain.executor import (
    CaptureExpiredRewind,
    CaptureExpiredStored,
    CaptureIncomplete,
    CaptureNotRenderable,
    ChainExecutor,
    ExecuteStepResult,
    Failed4xx,
    Failed5xx,
    FailedAuth,
    FailedNetwork,
    InlineBodyInvalid,
    RouteUnresolved,
    SendDeadlineExpired,
    Succeeded,
    TemplateUnresolved,
    default_clock,
)
from phantom.chain.jsonpath import (
    extract,
    find_placeholders,
    substitute,
    validate_path,
    whole_placeholder,
)
from phantom.chain.parser import (
    ENVELOPE_MAX_BYTES,
    InlineBodyDecodeError,
    ParserError,
    decode_inline_body_b64,
    envelope_from_persistence_json,
    parse_json_request,
    parse_multipart_request,
)

__all__ = [
    "ENVELOPE_MAX_BYTES",
    "CaptureExpiredRewind",
    "CaptureExpiredStored",
    "CaptureIncomplete",
    "CaptureNotRenderable",
    "ChainExecutor",
    "ExecuteStepResult",
    "Failed4xx",
    "Failed5xx",
    "FailedAuth",
    "FailedNetwork",
    "InlineBodyDecodeError",
    "InlineBodyInvalid",
    "ParserError",
    "RouteUnresolved",
    "SendDeadlineExpired",
    "Succeeded",
    "TemplateUnresolved",
    "decode_inline_body_b64",
    "default_clock",
    "envelope_from_persistence_json",
    "extract",
    "find_placeholders",
    "parse_json_request",
    "parse_multipart_request",
    "substitute",
    "validate_path",
    "whole_placeholder",
]
