"""Raw-intake catch-all route — stock S3-style upload → chain envelope (Phase 1).

The producer-facing primary landing. A stock object-storage client speaks
plain ``PUT /{bucket}/{key}`` with a raw body and an ``Authorization``
header it signed with throwaway credentials; it knows nothing of Phantom's
``POST /v1/send`` chain-envelope contract. This module bridges that gap:

* TASK 1.1 — a root-mounted catch-all ``/{phantom_path:path}`` that accepts
  the upload verbs (``PUT`` / ``POST`` / ``PATCH``) and is registered LAST
  in the app so it never shadows the fixed ``/v1/*`` surface. A second
  arm answers the read/metadata verbs (``GET`` / ``HEAD`` / ``DELETE`` /
  ``OPTIONS``) with a bare 404 so an unknown ``GET`` stays 404 rather than
  flipping to 405 service-wide. A reserved-prefix guard 404s any first
  path segment in Phantom's own namespace (``v1/`` today, plus the
  forward-reserved set).

* TASK 1.3 — destination resolution. The raw request line carries no real
  host (the client's ``Host`` is Phantom itself), so the synthesized step
  URL must be rewritten to a REAL upstream BEFORE dispatch, or Phantom
  would forward the request back to itself in an infinite loop. Two
  carriers (first hit wins): an explicit ``?phantom=<full-url>`` query
  parameter, then a configured ``Settings.phantom_default_target``. Both
  carriers preserve the inbound query byte-for-byte, minus the reserved
  ``phantom`` parameter, so query-addressed operations (``?partNumber=``,
  ``?uploadId=``, ``?uploads``, a presigned credential set) survive the
  rewrite. When neither names a destination (and on an empty path) the
  request is rejected 421 ``invalid_target`` BEFORE any durable write.

* TASK 1.2 — the raw→envelope adapter. A 1-step :class:`ChainEnvelope` is
  synthesized around the resolved URL, the request method, the raw body,
  and the forwarded headers (Phantom's reserved ``X-Phantom-*`` markers and
  the host-rewriting hop headers stripped). The shared
  :func:`phantom.routes.send.resolve_and_admit` prelude then runs the
  identical degraded-guard → dispatch → :func:`admit_chain` tail that
  ``POST /v1/send`` uses, so the synthesized envelope is buffered exactly
  like a producer-supplied one. Forwarding is AS-IS — Phantom does not
  re-sign in Phase 1 (the ``aws_sigv4`` signer is Phase 2).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, Request, Response

from phantom.chain.executor import _PHANTOM_RESERVED_HEADER_PREFIX
from phantom.chain.query import filter_raw_query
from phantom.config.settings import InstanceCfg
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.chain import ChainBodyRef, ChainEnvelope, ChainStep
from phantom.routes.send import (
    _BodyTooLargeError,
    _check_content_length,
    _error_response,
    _read_body_capped,
    get_degraded_instances,
    get_dispatcher,
    get_instance_cfgs,
    get_max_buffered_bytes,
    get_phantom_default_target,
    resolve_and_admit,
)
from phantom.runtime.startup_checks import DegradedInstance

logger = logging.getLogger(__name__)

# Synthesized-envelope constants. ``ChainStep.name`` and ``ChainBodyRef.name``
# are both regex-constrained to ``^[a-z][a-z0-9_]*$`` (models/chain.py), so the
# bucket/key (which routinely contain dots, slashes, and uppercase) can NEVER
# be a step/ref name — they live ONLY in the step ``url``. These fixed literals
# satisfy the regex on both the declared (envelope) and provided (body_refs)
# sides; the adapter mints exactly ``{"payload"}`` (body present) or ``{}``
# (body absent) on both sides by construction.
_SYNTHETIC_STEP_NAME = "upload"
_SYNTHETIC_BODY_REF_NAME = "payload"

# Phantom's public API path namespace. The catch-all is mounted at root, so
# without this guard a ``PUT /v1/anything`` that does not match a fixed route
# would fall through to the raw-intake handler and be treated as an upload.
# Phantom serves ONLY ``/v1`` today; the rest are forward-reserved so a future
# surface added under one of these prefixes is never silently captured as an
# object upload. Only the FIRST path segment is checked (a bucket literally
# named ``v1`` is addressable as ``/v1/...`` via the explicit ``?phantom=``
# carrier, which bypasses this handler's resolution entirely).
_RESERVED_FIRST_SEGMENTS: frozenset[str] = frozenset(
    {"v1", "v2", "oauth", "control", ".well-known"}
)

# Hop-by-hop / host-rewriting request headers that must never be copied onto
# the synthesized upstream step. ``Host`` would re-introduce Phantom's own
# bind host (re-creating the forward loop the destination resolver exists to
# prevent); ``Content-Length`` is recomputed by the transport at forward time
# and a stale value would corrupt the upstream request. (``Authorization`` is
# deliberately NOT here: it is forwarded as-is. A header-signed client
# signature therefore reaches the upstream untouched, and since F4 so does the
# QUERY that carries a presigned signature, which is where a presigned
# credential actually lives. The one exception is an ``aws_sigv4`` route,
# where Phantom's own signature supersedes the client's and the executor
# strips the presigned parameter set before signing.)
_HOP_BY_HOP_HEADERS: frozenset[str] = frozenset({"host", "content-length"})

# Phantom's reserved query-parameter carrier, reserved exactly the way the
# ``X-Phantom-*`` header namespace is: a raw-intake request may not use a
# query parameter of this name for its own purposes, because Phantom consumes
# it as the destination carrier and strips it before forwarding.
_PHANTOM_QUERY_CARRIER = "phantom"


router = APIRouter()


def _with_forwarded_query(url: str, request: Request) -> str:
    """Attach the inbound query, minus Phantom's carrier, to ``url``.

    Byte-preserving by construction: the raw query text is split on ``&`` and
    the surviving segments are reassembled verbatim, so percent-encoding, ``+``
    versus ``%20``, parameter order and repeated keys all survive exactly. An
    S3 presigned signature is computed over the canonical query string, so a
    ``parse_qsl``/``urlencode`` round trip would silently invalidate it.

    The carrier comparison applies ``unquote_plus`` to each raw key because
    DETECTION runs on the parsed view: starlette builds ``QueryParams`` with
    ``parse_qsl(..., keep_blank_values=True)``, which percent-decodes and
    plus-decodes the KEY. Without the same normalisation here an inbound
    ``?%70hantom=<url>`` would select the destination through the parsed view
    AND survive the raw strip, so Phantom's own control parameter would reach
    the upstream and be folded into any signature it validates. The strip must
    be the exact inverse of the detection.

    Fragments are handled before the join. ``url`` is producer-supplied on the
    explicit-carrier path and may carry a ``#``; appending the query after it
    would put the whole surviving query inside the fragment, which the
    transport drops, silently losing exactly what F4 exists to preserve.

    The INBOUND half is starlette's, not Phantom's: ``request.url.query`` comes
    from a parsed view rather than the raw ``query_string``, so a ``#`` in the
    request target truncates the query before this function runs
    (``query_string=b"a=1#frag&b=2"`` yields ``"a=1"``). A ``#`` is not a legal
    part of a request target, so this is the client's error rather than
    Phantom's; nothing after it is forwarded. A percent-encoded ``%23`` is
    data, not a delimiter, and it survives byte for byte.

    Args:
        url: The resolved destination, which may already carry its own query
            and may carry a fragment.
        request: The inbound raw-intake request.

    Returns:
        ``url`` unchanged when no query survives, otherwise ``url`` with the
        surviving query joined after ``?`` or ``&`` as appropriate and any
        fragment re-attached at the end. Never emits a bare trailing ``?``.
    """
    kept = filter_raw_query(request.url.query, keep=lambda key: key != _PHANTOM_QUERY_CARRIER)
    if not kept:
        return url
    base, hash_sep, fragment = url.partition("#")
    separator = "&" if "?" in base else "?"
    joined = f"{base}{separator}{kept}"
    return f"{joined}{hash_sep}{fragment}" if hash_sep else joined


def _resolve_destination(
    phantom_path: str,
    request: Request,
    phantom_default_target: str | None,
) -> str | None:
    """Resolve the real upstream URL for a raw-intake request, or ``None``.

    Phase-1 carriers, first hit wins (``id_routes`` is deferred):

    1. ``?phantom=<full-url>`` query parameter: the carrier's value names the
       destination (the explicit carrier always wins, including on an empty
       path). Phase 1 accepts a FULL URL only; bare ids are not resolved here.
    2. A configured ``Settings.phantom_default_target`` — the path is
       appended (``{default}/{phantom_path}``) for the single-upstream
       convenience case.

    BOTH carriers preserve the rest of the inbound query byte-for-byte
    (:func:`_with_forwarded_query`), so a query-addressed operation such as a
    multipart part upload reaches the upstream as that operation rather than as
    a whole-object overwrite. The ``phantom`` parameter itself is Phantom's own
    control channel and is always stripped; the comparison that strips it is
    ``unquote_plus``-normalised, so it is the exact inverse of the parsed-view
    detection that consumed the carrier here.

    An empty / slash-only ``phantom_path`` with no ``?phantom=`` carrier is
    "no address" — a stock object PUT always names a bucket/key, so an empty
    path is unroutable; it returns ``None`` even when a default target is
    configured. ``None`` means the caller must reject 421 ``invalid_target``
    before any durable write (never forward — that is the loop hazard).

    Args:
        phantom_path: The matched catch-all path (no leading slash).
        request: The inbound request (for ``query_params``).
        phantom_default_target: The configured default upstream as a string,
            or ``None`` when unset.

    Returns:
        The resolved full upstream URL (query preserved), or ``None`` when
        nothing names a real destination.
    """
    explicit = request.query_params.get(_PHANTOM_QUERY_CARRIER)
    if explicit:
        return _with_forwarded_query(explicit, request)

    if phantom_path.strip("/") == "":
        return None

    if phantom_default_target:
        return _with_forwarded_query(
            phantom_default_target.rstrip("/") + "/" + phantom_path, request
        )

    return None


def _forwarded_headers(request: Request) -> dict[str, str]:
    """Copy the inbound headers minus Phantom markers and host-rewriting hops.

    Drops every ``X-Phantom-*`` header (those are routing INPUTS to the
    catch-all, not upstream headers) and the hop-by-hop / host-rewriting set
    (``Host``, ``Content-Length``). ``Authorization`` is kept so the client's
    presigned / SigV4 signature is forwarded as-is. The executor applies the
    same ``x-phantom-*`` strip again at forward time as a backstop; stripping
    here keeps the persisted envelope honest.

    Args:
        request: The inbound raw-intake request.

    Returns:
        The forwarded-header mapping for the synthesized step (original
        header casing preserved).
    """
    forwarded: dict[str, str] = {}
    for name, value in request.headers.items():
        lowered = name.lower()
        if lowered.startswith(_PHANTOM_RESERVED_HEADER_PREFIX):
            continue
        if lowered in _HOP_BY_HOP_HEADERS:
            continue
        forwarded[name] = value
    return forwarded


def _synthesize_envelope(
    *,
    resolved_url: str,
    method: str,
    headers: dict[str, str],
    has_body: bool,
) -> ChainEnvelope:
    """Build the 1-step :class:`ChainEnvelope` for a raw-intake upload.

    A fresh ``chain_id`` (and, via the model's before-validator, a fresh
    ``idempotency_key`` defaulting to ``str(chain_id)``) is minted per
    request, so two identical raw PUTs produce two distinct rows rather than
    colliding on an idempotency replay. The single step carries the real
    destination URL, the request method, the forwarded headers, and — only
    when the request actually carried a body — a :class:`ChainBodyRef`
    naming the constant ``payload`` ref.

    Args:
        resolved_url: The real upstream URL (TASK 1.3 resolution result).
        method: The request method (``PUT`` / ``POST`` / ``PATCH``).
        headers: The forwarded headers (markers/hops already stripped).
        has_body: Whether the request carried a body (drives the body ref).

    Returns:
        The synthesized 1-step :class:`ChainEnvelope`.
    """
    # Pydantic Field-defaulted args (kind, content_type) are omitted; without
    # the Pydantic mypy plugin (incompatible with this workspace's mypy +
    # Pydantic, see admission.py) mypy can't see those defaults and flags the
    # omission as call-arg. Behavior is correct.
    body = (
        ChainBodyRef(name=_SYNTHETIC_BODY_REF_NAME)  # type: ignore[call-arg]
        if has_body
        else None
    )
    step = ChainStep(  # type: ignore[call-arg]
        name=_SYNTHETIC_STEP_NAME,
        method=method,  # type: ignore[arg-type]
        url=resolved_url,
        headers=headers,
        body=body,
    )
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        steps=[step],
    )


@router.api_route("/{phantom_path:path}", methods=["PUT", "POST", "PATCH"])
async def raw_intake(
    phantom_path: str,
    request: Request,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    max_buffered_bytes: Annotated[int, Depends(get_max_buffered_bytes)],
    instance_cfgs: Annotated[Sequence[InstanceCfg], Depends(get_instance_cfgs)],
    degraded_instances: Annotated[Sequence[DegradedInstance], Depends(get_degraded_instances)],
    phantom_default_target: Annotated[str | None, Depends(get_phantom_default_target)],
) -> Response:
    """Accept a stock object-storage upload and buffer it as a chain.

    The raw-intake landing (TASK 1.1/1.2/1.3). Reserved-prefix guard →
    destination resolution (421 when unroutable, BEFORE any durable write)
    → Content-Length precheck → body read (capped) → raw→envelope synthesis
    → the shared :func:`resolve_and_admit` prelude (degraded guard, dispatch,
    :func:`admit_chain`). Returns the same 202 + ``X-Phantom-*`` response a
    producer-supplied chain receives, or the canonical error envelope.

    Args:
        phantom_path: The object path after the host (e.g. ``bucket/key``).
        request: The inbound raw-intake request.
        dispatcher: The live instance dispatcher (DI).
        max_buffered_bytes: The per-upload byte cap (DI).
        instance_cfgs: Configured instances, for the degraded guard (DI).
        degraded_instances: The typed degraded set, for the guard (DI).
        phantom_default_target: The configured default upstream target (DI).

    Returns:
        A 202 :class:`Response` on admission, or a canonical error
        :class:`Response` (404 reserved prefix, 421 no destination, 413
        oversized, or any admission refusal).
    """
    request_id = request.headers.get("X-Request-Id") or str(uuid4())

    first_segment = phantom_path.split("/", 1)[0].lower()
    if first_segment in _RESERVED_FIRST_SEGMENTS:
        return Response(status_code=404)

    resolved_url = _resolve_destination(phantom_path, request, phantom_default_target)
    if resolved_url is None:
        # No carrier named a real upstream (and an empty path is unroutable):
        # reject BEFORE reading the body or attempting any write. This is the
        # same 421 dispatch raises for an unroutable host — never a forward
        # loop back to Phantom.
        return _error_response(
            "invalid_target",
            (
                f"No destination resolved for raw request {phantom_path!r}: "
                "supply ?phantom=<url> or configure phantom_default_target"
            ),
            instance_id="unrouted",
            request_id=request_id,
        )

    content_length_check = _check_content_length(
        request.headers.get("Content-Length"),
        max_buffered_bytes,
        request_id=request_id,
    )
    if content_length_check is not None:
        return content_length_check

    try:
        raw_body = await _read_body_capped(request, max_buffered_bytes)
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

    has_body = len(raw_body) > 0
    envelope = _synthesize_envelope(
        resolved_url=resolved_url,
        method=request.method,
        headers=_forwarded_headers(request),
        has_body=has_body,
    )
    body_refs: dict[str, bytes] = {_SYNTHETIC_BODY_REF_NAME: raw_body} if has_body else {}

    # Structural guard mirroring chain/parser.py's declared-vs-provided
    # cross-check (which admit_chain does NOT repeat — it consumes an
    # already-validated envelope). The adapter mints both sides itself, so
    # this can only ever fail on a coding error; assert it explicitly so a
    # future edit that desyncs the two surfaces fails loudly here rather than
    # forwarding a malformed step.
    declared = {s.body.name for s in envelope.steps if isinstance(s.body, ChainBodyRef)}
    provided = set(body_refs.keys())
    if declared != provided:  # pragma: no cover - structural invariant
        raise AssertionError(
            f"raw-intake body-ref desync: declared={sorted(declared)} provided={sorted(provided)}"
        )

    result = await resolve_and_admit(
        request_id=request_id,
        # A stock client sends none of Phantom's X-Phantom-* markers, so every
        # routing/grouping/idempotency input is pinned to its absent value.
        uid_header="",
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs=body_refs,
        authorization=request.headers.get("Authorization"),
        content_encoding=request.headers.get("Content-Encoding"),
        group_id=None,
        multifile_id=None,
        send_order=None,
        dispatcher=dispatcher,
        instance_cfgs=instance_cfgs,
        degraded_instances=degraded_instances,
    )
    if isinstance(result, Response):
        return result

    chain_resp_headers = {
        "X-Phantom-Upload-Id": str(result.row.chain_id),
        "X-Phantom-Status": result.row.state,
    }
    return Response(status_code=result.status_code, headers=chain_resp_headers)


@router.api_route("/{phantom_path:path}", methods=["GET", "HEAD", "DELETE", "OPTIONS"])
async def raw_intake_unsupported(phantom_path: str) -> Response:
    """Preserve a bare 404 for read/metadata verbs on the catch-all.

    A root-mounted ``/{phantom_path:path}`` route bound to the upload verbs
    would otherwise make every unknown ``GET`` / ``HEAD`` / ``DELETE`` /
    ``OPTIONS`` return 405 (method-not-allowed) service-wide, because the
    path now matches. Registering this complementary-method arm (in the SAME
    router, AFTER :func:`raw_intake`) restores the prior behavior: an unknown
    path under a non-upload verb is 404, exactly as before the catch-all
    existed. ``DELETE`` is out of scope by design and stays 404 here — it is
    NOT an object-delete surface.

    Args:
        phantom_path: The matched path (unused; the arm answers uniformly).

    Returns:
        A bare 404 :class:`Response`.
    """
    return Response(status_code=404)
