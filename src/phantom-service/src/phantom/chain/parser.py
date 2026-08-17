"""Chain-envelope parser.

Accepts raw bytes (JSON path) or Starlette multipart parts
(multipart path) and produces ``(ChainEnvelope, body_refs)`` plus
typed errors with ADR-010 error codes.

Both entry points run the same static validation passes through
:func:`_post_validate`: no duplicate step names, compilable capture
JSONPaths, statically resolvable ``{{step.var}}`` placeholders, and
decodable inline base64 bodies. This module also owns the ONE definition
of how Phantom decodes an inline base64 body,
:func:`decode_inline_body_b64`, plus its exception, so admission and the
executor cannot drift apart on either the decode rule or its failure
taxonomy.
"""

from __future__ import annotations

import base64
import logging
from collections.abc import AsyncIterator
from typing import Any, Protocol

from pydantic import ValidationError

from phantom.chain.jsonpath import find_placeholders, validate_path
from phantom.models.chain import (
    ChainBodyBytes,
    ChainBodyJson,
    ChainBodyRef,
    ChainBodyText,
    ChainEnvelope,
)
from phantom.models.errors import ErrorCode

logger = logging.getLogger(__name__)

# Max bytes for the envelope JSON part of a multipart request. A
# multi-MiB envelope is malformed; 1 MiB is plenty for any reasonable
# chain definition (a few KiB in practice).
ENVELOPE_MAX_BYTES = 1_048_576


class ParserError(Exception):
    """Typed envelope/body-ref validation error."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Construct a typed error.

        Args:
            code: The ADR-010 stable error code.
            message: Human-readable description.
            details: Code-specific context (defaults to empty).
        """
        super().__init__(message)
        self.code: ErrorCode = code
        self.message = message
        self.details: dict[str, Any] = details or {}


class InlineBodyDecodeError(Exception):
    """A ``ChainBodyBytes.value_b64`` payload is not decodable base64.

    The ONE exception either layer catches. ``base64.b64decode`` raises
    ``binascii.Error`` for malformed base64 and a bare ``ValueError`` for
    non-ASCII input, and both must be treated identically: unhandled, either
    one escapes the sender's worker loop and permanently crash-loops the
    service. Wrapping them here means neither caller has to know which
    library exception the decoder emits, and a future decoder change cannot
    silently widen the escape surface.

    Attributes:
        step_name: The chain step whose inline body failed to decode.
        reason: The underlying decoder message. Safe to log: neither
            exception type echoes the offending input.
    """

    def __init__(self, step_name: str, reason: str) -> None:
        """Construct the wrapper.

        Args:
            step_name: The chain step whose inline body failed to decode.
            reason: The underlying decoder message.
        """
        super().__init__(f"step {step_name!r}: {reason}")
        self.step_name = step_name
        self.reason = reason


def decode_inline_body_b64(value_b64: str, *, step_name: str) -> bytes:
    """Decode a ``ChainBodyBytes.value_b64`` payload.

    The ONE definition of how Phantom decodes an inline base64 body. Admission
    calls it to validate at ingress and the executor calls it to render the
    outbound body, so a payload that admission accepts is exactly a payload the
    executor can render.

    Standard (non-urlsafe) base64 with default validation: non-alphabet
    characters are discarded rather than rejected, so MIME-style
    newline-wrapped input from ``base64.encodebytes`` decodes normally, while a
    genuinely malformed length or padding raises. Do NOT add
    ``validate=True``: it would reject newline-wrapped base64, which is
    legitimate producer output.

    The ``except ValueError`` is deliberately wider than the library's own
    malformed-base64 error, which it subclasses, because non-ASCII input raises
    the bare parent class. Widening is safe here: it wraps a single library
    call whose only failure mode is a bad input, not a whole dispatch path
    where an unrelated bug could hide.

    Args:
        value_b64: The producer-supplied base64 text.
        step_name: The owning step, for the raised error's context.

    Returns:
        The decoded body bytes.

    Raises:
        InlineBodyDecodeError: When the input is not decodable base64, for
            either underlying reason.
    """
    try:
        return base64.b64decode(value_b64)
    except ValueError as exc:
        raise InlineBodyDecodeError(step_name, str(exc)) from exc


class MultipartPart(Protocol):
    """Minimal multipart-part shape consumed by :func:`parse_multipart_request`.

    The runtime types in starlette/fastapi differ across versions; we
    accept any object with the three attributes below.
    """

    name: str
    filename: str | None
    content_type: str | None

    async def read(self, max_bytes: int) -> bytes:
        """Read the part body, up to ``max_bytes``."""
        ...


def _validate_static_placeholders(envelope: ChainEnvelope) -> None:
    """Verify every ``{{step.var}}`` references a prior step's declared capture.

    Walks URL, headers, JSON body string fields. Raises
    :class:`ParserError("template_unresolved")` on dangling reference.
    """
    seen_captures: dict[str, set[str]] = {}
    for step in envelope.steps:
        # Collect placeholders this step references.
        refs: list[tuple[str, str]] = []
        refs.extend(find_placeholders(step.url))
        for v in step.headers.values():
            refs.extend(find_placeholders(v))
        if isinstance(step.body, ChainBodyJson):
            refs.extend(_walk_json_for_placeholders(step.body.value))
        elif isinstance(step.body, ChainBodyText):
            refs.extend(find_placeholders(step.body.value))
        for src_step, capture_name in refs:
            available = seen_captures.get(src_step)
            if available is None or capture_name not in available:
                raise ParserError(
                    "template_unresolved",
                    (
                        f"Unresolved placeholder "
                        f"{{{{{src_step}.{capture_name}}}}} in step {step.name!r}"
                    ),
                    {"step": step.name, "ref_step": src_step, "ref_name": capture_name},
                )
        # Register this step's captures for downstream steps.
        seen_captures[step.name] = {c.name for c in step.capture}


def _walk_json_for_placeholders(value: Any) -> list[tuple[str, str]]:
    """Recursive walk over a JSON-shaped tree collecting placeholders."""
    if isinstance(value, str):
        return find_placeholders(value)
    if isinstance(value, list):
        out: list[tuple[str, str]] = []
        for v in value:
            out.extend(_walk_json_for_placeholders(v))
        return out
    if isinstance(value, dict):
        out2: list[tuple[str, str]] = []
        for v in value.values():
            out2.extend(_walk_json_for_placeholders(v))
        return out2
    return []


def _validate_no_duplicate_step_names(envelope: ChainEnvelope) -> None:
    """Reject envelopes whose steps share a name."""
    seen: set[str] = set()
    for step in envelope.steps:
        if step.name in seen:
            raise ParserError(
                "envelope_invalid",
                f"Duplicate step name {step.name!r}",
                {"step": step.name},
            )
        seen.add(step.name)


def _validate_jsonpath_syntax(envelope: ChainEnvelope) -> None:
    """Verify every capture's JSONPath compiles."""
    for step in envelope.steps:
        for capture in step.capture:
            try:
                validate_path(capture.from_path)
            except ValueError as exc:
                raise ParserError(
                    "envelope_invalid",
                    f"Invalid JSONPath in step {step.name!r} capture {capture.name!r}",
                    {"step": step.name, "capture": capture.name, "reason": str(exc)},
                ) from exc


def _validate_inline_body_base64(envelope: ChainEnvelope) -> None:
    """Reject envelopes whose inline ``bytes`` bodies are not valid base64.

    Unvalidated ``value_b64`` reaches the decoder in the executor's
    ``_render_body`` at send time, where the raised ``ValueError`` escapes the
    sender's worker loop, kills the TaskGroup, and is re-claimed first on every
    restart: one malformed payload is a permanent service crash loop.
    Rejecting at ingress means no such row is ever durably admitted.

    Catching :class:`InlineBodyDecodeError` rather than a library exception is
    what makes this layer complete: a non-ASCII ``value_b64`` raises the bare
    parent class, which would propagate out of ``_post_validate`` and out of
    the parse entry point into ``routes/send.py``'s body handler, which catches
    only its own size error and :class:`ParserError`. The producer would get an
    untyped 5xx instead of the 422 this pass promises.

    The message and details deliberately do NOT echo the offending value: it is
    producer data of unbounded size and potentially sensitive. The decoder's
    own message describes the defect without quoting the input.
    """
    for step in envelope.steps:
        if not isinstance(step.body, ChainBodyBytes):
            continue
        try:
            decode_inline_body_b64(step.body.value_b64, step_name=step.name)
        except InlineBodyDecodeError as exc:
            raise ParserError(
                "envelope_invalid",
                f"Invalid base64 in step {step.name!r} body.value_b64",
                {"step": step.name, "reason": exc.reason},
            ) from exc


def _post_validate(envelope: ChainEnvelope) -> None:
    """Run every static validation pass on a parsed envelope."""
    _validate_no_duplicate_step_names(envelope)
    _validate_jsonpath_syntax(envelope)
    _validate_static_placeholders(envelope)
    _validate_inline_body_base64(envelope)


async def parse_json_request(
    body: bytes,
    *,
    instance_id: str,
    request_id: str,
    max_buffered_bytes: int,
) -> tuple[ChainEnvelope, dict[str, bytes]]:
    """Parse a pure-JSON ``POST /v1/send`` body into an envelope.

    Args:
        body: Raw request body bytes.
        instance_id: For error-body context.
        request_id: For error-body context.
        max_buffered_bytes: Hard cap on the envelope body size.

    Returns:
        ``(envelope, {})`` — empty body_refs dict.

    Raises:
        ParserError: On any validation failure.
    """
    del instance_id, request_id  # currently used only by callers for error envelopes
    if len(body) > max_buffered_bytes:
        raise ParserError(
            "envelope_invalid",
            "Request body exceeds buffered cap",
            {"reason": "body_too_large", "limit": max_buffered_bytes},
        )
    try:
        envelope = ChainEnvelope.model_validate_json(body)
    except ValidationError as exc:
        raise ParserError(
            "envelope_invalid",
            "ChainEnvelope validation failed",
            {"errors": exc.errors()},
        ) from exc
    _post_validate(envelope)
    return envelope, {}


async def parse_multipart_request(
    multipart: AsyncIterator[MultipartPart],
    *,
    instance_id: str,
    request_id: str,
    max_buffered_bytes: int,
) -> tuple[ChainEnvelope, dict[str, bytes]]:
    """Parse a multipart ``POST /v1/send`` request.

    Expects exactly one part named ``envelope`` (the JSON-serialized
    :class:`ChainEnvelope`) plus zero-or-more parts named
    ``body_refs[<name>]`` for each declared ``ChainBodyRef``.
    """
    del instance_id, request_id
    envelope_bytes: bytes | None = None
    body_refs: dict[str, bytes] = {}

    async for part in multipart:
        name = part.name
        if name == "envelope":
            # Reject a duplicate ``envelope`` part BEFORE reading its bytes
            # (finding R3-9 — the E-1 sibling the body_refs guard missed).
            # Silently last-wins on a second envelope dropped the first with
            # no signal — and the envelope carries the chain_id (row PK), the
            # destination, and the step chain, so the producer could not even tell
            # which chain_id Phantom admitted. The parser already rejects
            # duplicate STEP names and duplicate ``body_refs[<name>]`` parts;
            # an ambiguous duplicate envelope is unprocessable for the same
            # reason. Distinct ``envelope_duplicate`` code (sibling of
            # ``body_ref_duplicate``) so the producer sees the precise ambiguity.
            if envelope_bytes is not None:
                raise ParserError(
                    "envelope_duplicate",
                    "Duplicate multipart 'envelope' part",
                    {},
                )
            envelope_bytes = await part.read(ENVELOPE_MAX_BYTES)
            if len(envelope_bytes) >= ENVELOPE_MAX_BYTES:
                raise ParserError(
                    "envelope_invalid",
                    "Envelope JSON part exceeded the 1 MiB cap",
                    {"reason": "envelope_too_large", "limit": ENVELOPE_MAX_BYTES},
                )
        elif name.startswith("body_refs[") and name.endswith("]"):
            ref_name = name[len("body_refs[") : -1]
            # Reject a duplicate part name BEFORE reading its bytes
            # (finding E-1). Silently last-wins dropped the first part
            # with no signal to the producer — inconsistent with the parser's
            # structural rejection of body_ref_missing / body_ref_orphan.
            # The producer must send exactly one part per ref name; an
            # ambiguous duplicate is unprocessable.
            if ref_name in body_refs:
                raise ParserError(
                    "body_ref_duplicate",
                    f"Duplicate multipart part for body_ref {ref_name!r}",
                    {"name": ref_name},
                )
            data = await part.read(max_buffered_bytes)
            if len(data) >= max_buffered_bytes:
                raise ParserError(
                    "body_ref_missing",
                    f"body_ref {ref_name!r} exceeded per-upload limit",
                    {
                        "reason": "body_ref_too_large",
                        "name": ref_name,
                        "limit": max_buffered_bytes,
                    },
                )
            body_refs[ref_name] = data
        else:
            logger.debug("Ignoring unknown multipart part %s", name)

    if envelope_bytes is None:
        raise ParserError(
            "envelope_invalid",
            "Missing 'envelope' part in multipart request",
        )

    try:
        envelope = ChainEnvelope.model_validate_json(envelope_bytes)
    except ValidationError as exc:
        raise ParserError(
            "envelope_invalid",
            "ChainEnvelope validation failed",
            {"errors": exc.errors()},
        ) from exc

    # Cross-check body_refs against the envelope's declared refs.
    declared = {s.body.name for s in envelope.steps if isinstance(s.body, ChainBodyRef)}
    provided = set(body_refs.keys())
    missing = declared - provided
    orphan = provided - declared
    if missing:
        raise ParserError(
            "body_ref_missing",
            f"Missing body_refs for declared names: {sorted(missing)}",
            {"missing": sorted(missing)},
        )
    if orphan:
        raise ParserError(
            "body_ref_orphan",
            f"body_refs without a matching envelope ref: {sorted(orphan)}",
            {"orphan": sorted(orphan)},
        )

    _post_validate(envelope)
    return envelope, body_refs


def envelope_from_persistence_json(blob: str) -> ChainEnvelope:
    """Deserialize a persisted envelope.

    Args:
        blob: The JSON string from ``uploads.chain_envelope_json``.

    Returns:
        The validated :class:`ChainEnvelope`.
    """
    return ChainEnvelope.model_validate_json(blob)
