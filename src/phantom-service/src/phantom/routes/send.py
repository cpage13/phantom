"""``POST /v1/send`` handler — chain ingress (plan §4.33).

The handler is HTTP-shaped only: parse headers, parse body, dispatch
to the owning instance, delegate to :func:`admit_chain`, build the
response. All admission logic (saturation gate, auth cache, idempotency
dedup, codec encode, body-hash compute, row construction, two-phase
write) lives in :mod:`phantom.routes.admission` and is unit-testable
without booting FastAPI.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from phantom.chain.parser import (
    ParserError,
    parse_json_request,
    parse_multipart_request,
)
from phantom.config.settings import InstanceCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import (
    InstanceDispatcher,
    InstanceNotFoundError,
    NoMatchingInstanceError,
    resolve_configured_instance_id,
)
from phantom.models.chain import ChainEnvelope, ChainResponse
from phantom.models.errors import STATUS_FOR_CODE, ErrorCode, error_response
from phantom.models.upload import UploadRow
from phantom.routes._version import SEND_ROUTER_PREFIX
from phantom.routes.admission import (
    AdmissionInputs,
    ChainAdmissionError,
    admit_chain,
)
from phantom.routes.envelope import build_response_headers
from phantom.runtime.startup_checks import DegradedInstance

logger = logging.getLogger(__name__)

# Polling hint echoed as X-Phantom-Suggested-Poll-After on every admission
# response. 5 seconds matches the default retry-backoff base
# (RetryStrategyCfg.base_seconds), so a client that honors the hint checks
# back no sooner than a first delivery attempt could plausibly have
# resolved, instead of hammering the status surface immediately after the
# 202. A coarse hint by design; clients may poll on their own schedule.
SUGGESTED_POLL_AFTER_SECONDS: int = 5


def get_dispatcher() -> InstanceDispatcher:
    """Dependency placeholder — wired by the composition root."""
    raise NotImplementedError("InstanceDispatcher dependency must be overridden by app factory")


def get_max_buffered_bytes() -> int:
    """Dependency placeholder — returns ``Settings.storage.max_buffered_bytes``.

    The composition root binds this to the resolved YAML value (default
    2 GiB; see :class:`phantom.config.settings.StorageCfg`). The route
    uses it both as the parser cap and as the multipart ``max_part_size``
    so the two limits move together.
    """
    raise NotImplementedError("max_buffered_bytes dependency must be overridden by app factory")


def get_instance_cfgs() -> Sequence[InstanceCfg]:
    """Configured instances (``Settings.instances``), wired by app factory.

    Plan § 4D.2. The degraded-boot guard maps a request to its CONFIGURED
    instance id over this list, NOT the dispatcher: a degraded instance
    has no live context and is absent from the dispatcher, but it is still
    present in the config. Defaults to an EMPTY sequence so a partial
    wiring that does not override it simply resolves no configured id (the
    guard then never fires, and routing proceeds via the dispatcher as
    before). The composition root overrides it with ``settings.instances``.
    """
    return ()


def get_degraded_instances() -> Sequence[DegradedInstance]:
    """The typed degraded set (wired by the composition root; seam 3).

    One :class:`DegradedInstance` per instance whose boot returned a
    classified storage fault (no live context). Defaults to an EMPTY
    sequence (the normal healthy case) so a partial wiring or a test
    harness that does not override it admits exactly as before. The
    composition root binds it to the lifespan's typed BootOutcome fold.
    """
    return ()


router = APIRouter(prefix=SEND_ROUTER_PREFIX)


@router.post("/send")
async def post_send(
    request: Request,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    max_buffered_bytes: Annotated[int, Depends(get_max_buffered_bytes)],
    instance_cfgs: Annotated[Sequence[InstanceCfg], Depends(get_instance_cfgs)],
    degraded_instances: Annotated[Sequence[DegradedInstance], Depends(get_degraded_instances)],
) -> Response:
    """Ingress endpoint — accept a chain submission and return 202.

    The handler stays HTTP-shaped: parse headers (the grouping/ordering
    trio through :func:`_parse_grouping_headers`), then
    :func:`_parse_and_resolve` (Content-Length precheck, body parsing,
    the host-route § 4D.2 degraded guard, dispatcher resolution),
    delegate to :func:`admit_chain`, build the response. Every refusal
    becomes a :class:`ChainAdmissionError` raised here or by
    ``admit_chain``; the handler maps those to the canonical
    :class:`ErrorEnvelope` response (see :func:`_degraded_guard_response`
    for the degraded-boot guard rationale).
    """
    request_id = request.headers.get("X-Request-Id") or str(uuid4())
    uid_header = request.headers.get("X-Phantom-Uid", "")
    instance_header = request.headers.get("X-Phantom-Instance")
    idempotency_header = request.headers.get("X-Phantom-Idempotency-Key")

    # § 4D.2 - explicit-route degraded guard. An X-Phantom-Instance header
    # names the configured target id directly, so check it BEFORE reading
    # any body bytes: a degraded target does not get to spike RAM with a
    # body it cannot store. Host-prefix-routed requests are guarded after
    # the URL is parsed (inside _parse_and_resolve), since their target id
    # is not known yet.
    if instance_header is not None:
        degraded = _degraded_guard_response(
            instance_cfgs=instance_cfgs,
            degraded_instances=degraded_instances,
            url="",
            instance_header=instance_header,
            request_id=request_id,
        )
        if degraded is not None:
            return degraded

    resolved = await _parse_and_resolve(
        request,
        dispatcher=dispatcher,
        max_buffered_bytes=max_buffered_bytes,
        instance_cfgs=instance_cfgs,
        degraded_instances=degraded_instances,
        instance_header=instance_header,
        request_id=request_id,
    )
    if isinstance(resolved, Response):
        return resolved
    envelope, body_refs, instance_ctx = resolved

    try:
        group_id, multifile_id, send_order = _parse_grouping_headers(
            request.headers, instance_id=instance_ctx.cfg.id
        )
        inputs = AdmissionInputs(
            request_id=request_id,
            uid_header=uid_header,
            instance_header=instance_header,
            idempotency_header=idempotency_header,
            envelope=envelope,
            body_refs=body_refs,
            authorization=request.headers.get("Authorization"),
            content_encoding=request.headers.get("Content-Encoding"),
            group_id=group_id,
            multifile_id=multifile_id,
            send_order=send_order,
        )
        outcome = await admit_chain(inputs, instance_ctx)
    except ChainAdmissionError as exc:
        return _error_response(
            exc.code,
            exc.message,
            instance_id=exc.instance_id,
            request_id=request_id,
            details=dict(exc.details) if exc.details else None,
            headers=exc.headers,
        )
    return _build_chain_response(outcome.row, envelope=envelope, status=outcome.status_code)


def _parse_grouping_headers(
    headers: Mapping[str, str],
    *,
    instance_id: str,
) -> tuple[UUID | None, UUID | None, int | None]:
    """Parse the optional grouping/ordering headers to typed values.

    Cycle-7 task 2.2: reads ``X-Phantom-Group-Id`` (UUID),
    ``X-Phantom-Multifile-Id`` (UUID), and ``X-Phantom-Order``
    (int >= 0) beside the existing header reads. An absent header yields
    ``None``; admission applies the stored defaults (chain_id / NULL /
    0). A PRESENT-but-malformed value (an empty string included) raises
    ``header_invalid``, mapping to the 400 ErrorEnvelope through the
    handler's existing :class:`ChainAdmissionError` path, so the
    producer learns its grouping intent was dropped instead of the
    upload being silently filed under the defaults.

    Args:
        headers: The request's case-insensitive header mapping.
        instance_id: The resolved owning instance, for error attribution.

    Returns:
        ``(group_id, multifile_id, send_order)``; each element is
        ``None`` when its header is absent.

    Raises:
        ChainAdmissionError: code ``header_invalid`` (HTTP 400), naming
            the offending header and the supplied value in ``details``.
    """

    def _uuid_header(name: str) -> UUID | None:
        raw = headers.get(name)
        if raw is None:
            return None
        try:
            return UUID(raw)
        except ValueError as exc:
            raise ChainAdmissionError(
                code="header_invalid",
                message=f"{name} must be a UUID; got {raw!r}",
                instance_id=instance_id,
                details={"header": name, "value": raw},
            ) from exc

    group_id = _uuid_header("X-Phantom-Group-Id")
    multifile_id = _uuid_header("X-Phantom-Multifile-Id")

    send_order: int | None = None
    order_raw = headers.get("X-Phantom-Order")
    if order_raw is not None:
        try:
            send_order = int(order_raw)
        except ValueError as exc:
            raise ChainAdmissionError(
                code="header_invalid",
                message=f"X-Phantom-Order must be a non-negative integer; got {order_raw!r}",
                instance_id=instance_id,
                details={"header": "X-Phantom-Order", "value": order_raw},
            ) from exc
        if send_order < 0:
            raise ChainAdmissionError(
                code="header_invalid",
                message=f"X-Phantom-Order must be a non-negative integer; got {order_raw!r}",
                instance_id=instance_id,
                details={"header": "X-Phantom-Order", "value": order_raw},
            )
    return group_id, multifile_id, send_order


async def _parse_and_resolve(
    request: Request,
    *,
    dispatcher: InstanceDispatcher,
    max_buffered_bytes: int,
    instance_cfgs: Sequence[InstanceCfg],
    degraded_instances: Sequence[DegradedInstance],
    instance_header: str | None,
    request_id: str,
) -> tuple[ChainEnvelope, dict[str, bytes], InstanceContext] | Response:
    """Parse the request body and resolve the owning live instance.

    The middle of the ingress flow, between the header reads and
    admission:

    1. H2 audit closure: precheck ``Content-Length`` before reading any
       body bytes. A producer declaring an oversized payload sees a 413
       immediately; without this check the body landed fully in memory
       before the post-buffering size assertion fired, allowing a
       multi-GB POST to spike RAM before the cap caught it. Chunked
       uploads (no Content-Length) bypass this check by design and are
       caught by the streaming size cap inside :func:`_parse_body`.
    2. Body parsing (JSON or multipart) into ``(envelope, body_refs)``.
    3. § 4D.2 host-prefix-route degraded guard. The target host lives in
       the parsed envelope, so this is the earliest the configured
       target id is known for host-routed requests. Fires BEFORE
       ``admit_chain`` so no durable write is attempted against a
       degraded instance. (When an ``X-Phantom-Instance`` header was
       given the handler already guarded it before any body bytes were
       read; the resolver short-circuits on the header again here
       harmlessly.)
    4. Dispatcher resolution to the owning :class:`InstanceContext`.

    Returns:
        The parsed ``(envelope, body_refs, instance_ctx)`` triple, or
        the canonical error :class:`Response` to return verbatim.
    """
    content_length_check = _check_content_length(
        request.headers.get("Content-Length"),
        max_buffered_bytes,
        request_id=request_id,
    )
    if content_length_check is not None:
        return content_length_check

    parsed = await _parse_body(request, max_buffered_bytes, request_id=request_id)
    if isinstance(parsed, Response):
        return parsed
    envelope, body_refs = parsed

    first_step_url = _resolve_first_step_url(envelope)

    degraded = _degraded_guard_response(
        instance_cfgs=instance_cfgs,
        degraded_instances=degraded_instances,
        url=first_step_url,
        instance_header=instance_header,
        request_id=request_id,
    )
    if degraded is not None:
        return degraded

    try:
        instance_ctx = dispatcher.resolve(first_step_url, instance_header)
    except InstanceNotFoundError:
        return _error_response(
            "instance_unknown",
            f"X-Phantom-Instance {instance_header!r} not configured",
            instance_id="unrouted",
            request_id=request_id,
        )
    except NoMatchingInstanceError:
        return _error_response(
            "invalid_target",
            f"No instance accepts target {first_step_url!r}",
            instance_id="unrouted",
            request_id=request_id,
        )
    return envelope, body_refs, instance_ctx


def _check_content_length(
    raw_header: str | None,
    max_buffered_bytes: int,
    *,
    request_id: str,
) -> Response | None:
    """Reject a declared ``Content-Length`` larger than the buffered-bytes cap.

    Returns a 413 :class:`Response` when the header is present and parses
    to a value greater than ``max_buffered_bytes``. Returns ``None`` (no
    early rejection) when the header is absent, malformed, or fits under
    the cap — the absent / malformed cases fall through to the streaming
    cap inside :func:`_parse_body`, which holds the contract.

    H2 audit closure: declared payload above cap → 413 before any body
    bytes are read.
    """
    if raw_header is None:
        return None
    try:
        declared = int(raw_header)
    except ValueError:
        # Malformed header — let the parser decide. Returning a 4xx
        # here would also be defensible, but the parser already covers
        # the malformed-body class of errors with proper error codes.
        return None
    if declared <= max_buffered_bytes:
        return None
    return _error_response(
        "body_too_large",
        f"Content-Length {declared} exceeds max_buffered_bytes cap of {max_buffered_bytes}",
        instance_id="unrouted",
        request_id=request_id,
        details={
            "declared": declared,
            "limit": max_buffered_bytes,
            "reason": "content_length_precheck",
        },
    )


async def _read_body_capped(request: Request, max_buffered_bytes: int) -> bytes:
    """Stream ``request`` into a single ``bytes`` object, aborting at the cap.

    Raises :class:`_BodyTooLargeError` mid-stream if the cumulative size
    exceeds ``max_buffered_bytes``. The exception bubbles up to
    :func:`_parse_body`, which maps it to a 413 response. This is the
    backstop for chunked uploads where no ``Content-Length`` was sent and
    the precheck couldn't fire.

    H2 audit closure: streaming cap aborts before a malicious chunked
    payload spikes RAM.
    """
    total = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_buffered_bytes:
            raise _BodyTooLargeError(observed=total, limit=max_buffered_bytes)
        chunks.append(chunk)
    return b"".join(chunks)


class _BodyTooLargeError(Exception):
    """Raised by :func:`_read_body_capped` when the streaming cap fires."""

    def __init__(self, *, observed: int, limit: int) -> None:
        super().__init__(f"streaming body exceeded cap {limit} bytes (observed {observed})")
        self.observed = observed
        self.limit = limit


async def _parse_body(
    request: Request, max_buffered_bytes: int, *, request_id: str
) -> tuple[ChainEnvelope, dict[str, bytes]] | Response:
    """Parse JSON or multipart body into ``(envelope, body_refs)``.

    Returns the parsed pair on success or a Response on failure. The
    Response carries the canonical ErrorEnvelope shape so the caller
    can return it verbatim.
    """
    content_type = request.headers.get("Content-Type", "")
    try:
        if content_type.startswith("multipart/"):
            # Starlette's MultiPartParser respects ``max_part_size``
            # internally and raises HTTPException(400) when a part runs
            # over — that's the streaming cap for the multipart path.
            form_iter = _multipart_iter(request, max_part_size=max_buffered_bytes)
            return await parse_multipart_request(
                form_iter,
                instance_id="unrouted",
                request_id=request_id,
                max_buffered_bytes=max_buffered_bytes,
            )
        # JSON path: stream + accumulate with a cumulative-size guard so
        # a chunked transfer-encoded body without ``Content-Length`` is
        # caught mid-stream before RAM blows up (H2 streaming cap).
        raw = await _read_body_capped(request, max_buffered_bytes)
        return await parse_json_request(
            raw,
            instance_id="unrouted",
            request_id=request_id,
            max_buffered_bytes=max_buffered_bytes,
        )
    except _BodyTooLargeError as exc:
        return _error_response(
            "body_too_large",
            f"Streaming body exceeded max_buffered_bytes cap of {exc.limit}",
            instance_id="unrouted",
            request_id=request_id,
            details={
                "observed": exc.observed,
                "limit": exc.limit,
                "reason": "streaming_cap",
            },
        )
    except ParserError as exc:
        return _error_response(
            exc.code,
            exc.message,
            instance_id="unrouted",
            request_id=request_id,
            details=exc.details,
        )
    except StarletteHTTPException as exc:
        # Starlette's MultiPartParser raises HTTPException(400) when a
        # part exceeds ``max_part_size``; map to the canonical
        # body_too_large shape (H2 closure: oversized multipart parts
        # are size-driven failures, not malformed-envelope failures).
        return _error_response(
            "body_too_large",
            exc.detail if isinstance(exc.detail, str) else "Multipart part exceeded limit",
            instance_id="unrouted",
            request_id=request_id,
            details={"reason": "multipart_part_too_large", "limit": max_buffered_bytes},
        )


def _degraded_guard_response(
    *,
    instance_cfgs: Sequence[InstanceCfg],
    degraded_instances: Sequence[DegradedInstance],
    url: str,
    instance_header: str | None,
    request_id: str,
) -> Response | None:
    """Return a 500 :class:`Response` when the target instance booted DEGRADED.

    Maps the request to its CONFIGURED instance id over ``instance_cfgs``
    (the dispatcher cannot resolve a degraded instance: it has no live
    context, § 4D.2), then checks that id against the typed degraded set.
    A degraded instance has no usable durable storage, so admission would
    fail; returning ``500`` (ADR-017 ``internal_error``) makes the caller
    fail over to the upstream endpoint directly instead.

    Args:
        instance_cfgs: Configured instances (``settings.instances``).
        degraded_instances: The typed degraded set (one
            :class:`DegradedInstance` per degraded boot, seam 3).
        url: The first-step URL (for host-prefix routing); pass ``""`` when
            only the ``X-Phantom-Instance`` header is available.
        instance_header: Optional ``X-Phantom-Instance`` value.
        request_id: Per-request correlation id for the error envelope.

    Returns:
        A 500 :class:`Response` when the resolved target is degraded; ``None``
        when there are no degraded instances, the target is not degraded, or
        the request does not resolve to a configured instance (routing /
        unknown-target errors are handled later on the normal path).
    """
    if not degraded_instances:
        return None
    target_id = resolve_configured_instance_id(instance_cfgs, url, instance_header)
    if target_id is None:
        return None
    degraded = next((d for d in degraded_instances if d.instance_id == target_id), None)
    if degraded is None:
        return None
    logger.warning(
        "Refusing POST /v1/send for instance %r: storage unavailable (degraded "
        "boot, reason=%s). Returning 500 so the caller fails over to the "
        "upstream directly. Fault: %s",
        target_id,
        degraded.reason.value,
        degraded.detail,
    )
    return _error_response(
        "internal_error",
        (
            f"Instance {target_id!r} storage unavailable (degraded boot, "
            f"reason={degraded.reason.value}): cannot durably buffer. Fail over "
            f"to the upstream endpoint directly. Fault: {degraded.detail}"
        ),
        instance_id=target_id,
        request_id=request_id,
        details={
            "reason": "storage_unavailable_degraded_boot",
            "instance": target_id,
            "degrade_reason": degraded.reason.value,
        },
    )


def _resolve_first_step_url(envelope: ChainEnvelope) -> str:
    """Resolve the first step's URL, applying ``default_target`` if needed."""
    first_step_url = envelope.steps[0].url
    if envelope.default_target and "://" not in first_step_url:
        first_step_url = str(envelope.default_target).rstrip("/") + (
            first_step_url if first_step_url.startswith("/") else "/" + first_step_url
        )
    return first_step_url


def _build_chain_response(
    row: UploadRow,
    *,
    envelope: ChainEnvelope,
    status: int,
) -> Response:
    """Build the 202 response body + X-Phantom-* headers."""
    chain_resp = ChainResponse(
        chain_id=row.chain_id,
        state=row.state,
        last_step_completed=row.last_step_completed,
        captured=[],
    )
    headers = build_response_headers(
        upload_id=row.chain_id,
        # The X-Phantom-Group-Id echo: always present (group_id is NOT
        # NULL; the supplied header value, else chain_id).
        group_id=row.group_id,
        state=row.state,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        suggested_poll_after_seconds=SUGGESTED_POLL_AFTER_SECONDS,
    )
    return Response(
        content=chain_resp.model_dump_json(),
        media_type="application/json",
        status_code=status,
        headers=headers,
    )


def _error_response(
    code: ErrorCode,
    message: str,
    *,
    instance_id: str,
    request_id: str,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Response:
    """Wrap an :class:`ErrorEnvelope` with the right HTTP status."""
    env = error_response(
        code,
        message,
        instance_id=instance_id,
        request_id=request_id,
        details=details,
    )
    status = STATUS_FOR_CODE[code]
    return Response(
        content=env.model_dump_json(),
        media_type="application/json",
        status_code=status,
        headers=headers,
    )


async def _multipart_iter(request: Request, *, max_part_size: int) -> AsyncIterator[_PartShim]:
    """Adapter — yield parts from FastAPI/starlette's form parser.

    The Pydantic parser expects an async iterator of objects with
    ``.name`` / ``.read(max_bytes)``. We adapt FastAPI's
    ``request.form()`` into that shape.

    Args:
        request: Incoming FastAPI request.
        max_part_size: Per-multipart-part byte cap passed straight to
            starlette's ``MultiPartParser`` (whose own default is 1 MiB —
            far below Phantom's per-upload cap). Driven by
            ``Settings.storage.max_buffered_bytes`` so the parser ceiling
            and the post-parse buffered-bytes cap move together.
    """
    form = await request.form(max_part_size=max_part_size)
    for name, value in form.multi_items():
        if hasattr(value, "read"):
            data = await value.read()
            content_type = getattr(value, "content_type", "application/octet-stream")
            yield _PartShim(name=name, data=data, content_type=content_type)
        else:
            yield _PartShim(name=name, data=str(value).encode("utf-8"), content_type="text/plain")


class _PartShim:
    """Match the ``MultipartPart`` Protocol the parser expects."""

    def __init__(self, *, name: str, data: bytes, content_type: str) -> None:
        self.name = name
        self.filename: str | None = None
        self.content_type = content_type
        self._data = data

    async def read(self, max_bytes: int) -> bytes:
        """Return up to ``max_bytes`` of the part body."""
        return self._data[:max_bytes]
