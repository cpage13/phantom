"""``X-Phantom-*`` header constants and request-header builder.

The header set is the wire contract between phantom-client and
Phantom-the-service. This module is the single source of truth for the
literal strings; tests assert that three explicitly-excluded headers
(``X-Phantom-Route``, ``X-Phantom-Auth-Mode``, ``X-Phantom-Strategy``)
do not appear here. The cycle-7 dead-header sweep removed the
request-side batch, target, and metadata headers: the service never
read them (routing comes from the first step's URL; grouping rides
``X-Phantom-Group-Id``).

:func:`build_request_headers` is the assembly function the transport
uses to convert per-call values (uid, auth_token) plus optional
:class:`~phantom_client.config.SubmitOptions` into the outbound header
dict. ``X-Phantom-Idempotency-Key`` is always emitted (the SDK's retry
dedup key, distinct from ``envelope.idempotency_key`` which Phantom
forwards to upstreams).
"""

from __future__ import annotations

from phantom_client.config import SubmitOptions

# ---------------------------------------------------------------------------
# Request headers (caller → Phantom).
# ---------------------------------------------------------------------------

X_PHANTOM_UID = "X-Phantom-Uid"
"""Caller-supplied opaque identity for token-cache scoping (ADR-002)."""

X_PHANTOM_INSTANCE = "X-Phantom-Instance"
"""Optional override forcing a specific instance (ADR-006 advanced)."""

X_PHANTOM_GROUP_ID = "X-Phantom-Group-Id"
"""Optional query-grouping tag; defaults to ``envelope.chain_id`` server-side.

Phantom echoes the effective value back under the same name on every
202 (see :mod:`phantom_client.models.envelope`).
"""

X_PHANTOM_MULTIFILE_ID = "X-Phantom-Multifile-Id"
"""Optional multi-file set tag; omitted means standalone (NULL on the row)."""

X_PHANTOM_ORDER = "X-Phantom-Order"
"""Optional recorded position within a multi-file set (display only)."""

X_PHANTOM_IDEMPOTENCY_KEY = "X-Phantom-Idempotency-Key"
"""SDK-retry idempotency key; the SDK always emits this on submit_chain."""

# ---------------------------------------------------------------------------
# Response headers (Phantom → caller).
# ---------------------------------------------------------------------------

X_PHANTOM_UPLOAD_ID = "X-Phantom-Upload-Id"
"""Upload row id; equals ``envelope.chain_id`` for chain submissions."""

X_PHANTOM_STATUS = "X-Phantom-Status"
"""Snake-case state literal from :data:`~phantom_client.models.chain.ChainState`."""

X_PHANTOM_ATTEMPTS = "X-Phantom-Attempts"
"""Attempt count on the row."""

X_PHANTOM_NEXT_ATTEMPT_AT = "X-Phantom-Next-Attempt-At"
"""ISO-8601 UTC; absent on terminal states."""

X_PHANTOM_SUGGESTED_POLL_AFTER = "X-Phantom-Suggested-Poll-After"
"""Integer seconds; the SDK's poller uses this for next-sleep duration."""


# ---------------------------------------------------------------------------
# Builder.
# ---------------------------------------------------------------------------


def build_request_headers(
    *,
    uid: str | None,
    auth_token: str | None,
    options: SubmitOptions | None,
    sdk_idempotency_key: str,
) -> dict[str, str]:
    """Assemble outbound request headers for ``POST /v1/send``.

    Always emits ``X-Phantom-Idempotency-Key`` so Phantom dedupes any
    transport-retried submission. Per-call values override
    :class:`SubmitOptions` fields where applicable (in practice only
    ``X-Phantom-Idempotency-Key`` is sourced from options when set).

    Args:
        uid: Maps to ``X-Phantom-Uid`` (omitted if None).
        auth_token: The full ``Authorization`` header value (e.g.,
            ``"Bearer ..."``); omitted if None.
        options: Optional per-call submission options.
        sdk_idempotency_key: The fallback idempotency key. Overridden by
            ``options.idempotency_key`` when set.

    Returns:
        A fresh dict suitable for httpx's ``headers=`` kwarg.
    """
    headers: dict[str, str] = {}
    if auth_token is not None:
        headers["Authorization"] = auth_token
    if uid is not None:
        headers[X_PHANTOM_UID] = uid

    idem_key = sdk_idempotency_key
    if options is not None:
        if options.idempotency_key is not None:
            idem_key = options.idempotency_key
        if options.instance_id is not None:
            headers[X_PHANTOM_INSTANCE] = options.instance_id
        if options.group_id is not None:
            headers[X_PHANTOM_GROUP_ID] = str(options.group_id)
        if options.multifile_id is not None:
            headers[X_PHANTOM_MULTIFILE_ID] = str(options.multifile_id)
        if options.order is not None:
            headers[X_PHANTOM_ORDER] = str(options.order)
    headers[X_PHANTOM_IDEMPOTENCY_KEY] = idem_key
    return headers


__all__ = [
    "X_PHANTOM_ATTEMPTS",
    "X_PHANTOM_GROUP_ID",
    "X_PHANTOM_IDEMPOTENCY_KEY",
    "X_PHANTOM_INSTANCE",
    "X_PHANTOM_MULTIFILE_ID",
    "X_PHANTOM_NEXT_ATTEMPT_AT",
    "X_PHANTOM_ORDER",
    "X_PHANTOM_STATUS",
    "X_PHANTOM_SUGGESTED_POLL_AFTER",
    "X_PHANTOM_UID",
    "X_PHANTOM_UPLOAD_ID",
    "build_request_headers",
]
