"""Internal HTTP transport - the SDK's single source of truth for the wire.

:class:`Transport` wraps a single :class:`httpx.AsyncClient` and exposes
typed methods for every HTTP interaction the SDK supports. It is
**not** part of the SDK's public surface (callers use
:class:`~phantom_client.client.PhantomClient`), but it is testable in
isolation with an injected :class:`httpx.AsyncBaseTransport` (typically
:class:`httpx.MockTransport` or :class:`httpx.ASGITransport`).

Key behaviors:

- ``submit_chain`` selects JSON vs. multipart encoding based on whether
  ``body_refs`` is non-empty. The multipart shape uses parts named
  ``envelope`` (the JSON-serialized envelope) and ``body_refs[<name>]``
  per body_ref (ADR-010).
- Retries happen **only** for transport-class failures, and they split into
  two classes. A failure that PROVABLY never landed (connect refused, connect
  timeout, pool timeout, an unbuildable request) is always retried. A failure
  that MAY HAVE LANDED (read/write timeout, a reset, a server disconnect
  mid-response) is retried only for calls that opt in, because the server may
  have executed the request and lost only the response. 5xx responses are
  passed through to the caller untouched: Phantom IS the retry engine, so
  doubling up muddies idempotency.
- Only ``submit_chain`` carries an ``X-Phantom-Idempotency-Key``, which is why
  it is the one mutating call that opts into may-have-landed retries: its
  re-arrival is deduped by admission's atomic claim. ``get_json`` opts in
  because it is read-only and ``put_json`` because its callers overwrite one
  slot. The three mutating admin helpers do NOT, so a read timeout on a
  replay, a cancel or a bulk delete surfaces as
  :class:`~phantom_client.errors.PhantomTimeoutError` rather than being
  re-sent; the caller checks the chain's state and decides.
- Non-2xx responses are parsed as the ADR-010 ``ErrorEnvelope`` and
  raised as a typed :class:`~phantom_client.errors.PhantomHttpError`
  subclass.
- EVERY method that issues a request, streaming included, translates its
  ``httpx`` failures through the one :func:`_translate_httpx_error` mapping,
  so ``except PhantomTransportError`` around an SDK call holds whichever
  method raised. ``stream_request`` retries only the never-landed class and
  only before its first chunk reaches the caller; see its docstring.
- ``Authorization`` is never logged - the logging filter redacts it.
- A ``unix:`` ``phantom_url`` (the documented UDS form of the service
  connections table) is routed through a real
  ``httpx.AsyncHTTPTransport(uds=...)`` automatically; a missing socket
  therefore surfaces as :class:`~phantom_client.errors.PhantomConnectError`
  like any refused TCP connect, never as an unsupported-protocol error.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError

from phantom_client.config import ClientConfig, RetryPolicy, SubmitOptions
from phantom_client.errors import (
    PhantomConnectError,
    PhantomEnvelopeError,
    PhantomNetworkError,
    PhantomTimeoutError,
    PhantomTransportError,
    raise_for_error_body,
)
from phantom_client.headers import X_PHANTOM_SUGGESTED_POLL_AFTER, build_request_headers
from phantom_client.models.chain import ChainEnvelope, ChainResponse

_LOG = logging.getLogger(__name__)

# What a query-string value may be. ``str`` covers every filter, id and enum
# value the SDK sends; ``int`` is there for the one caller that passes a page
# ``limit`` as a number (U16). ``bool`` and ``None`` are deliberately EXCLUDED
# even though httpx accepts them: nothing passes them, and admitting a type no
# caller uses is how a narrowed annotation drifts back to ``Any``. The PEP 695
# ``type`` statement rather than ``TypeAlias``: it is 3.12 syntax and
# ``phantom-client`` declares ``requires-python = ">=3.12"``, and ruff's UP040
# rejects the older spelling.
type QueryParamValue = str | int

# ---------------------------------------------------------------------------
# Path constants - the single source of truth for the v1 URL space.
#
# Every path lives HERE, in the one module that owns the wire, rather than
# beside whichever caller issues it. Two modules drive requests against the
# same paths: :class:`~phantom_client.client.PhantomClient` for the one-shot
# admin calls and :mod:`phantom_client.poller` for its loops. Declaring them
# per-caller is how ``_PATH_UPLOADS`` came to name the chain COLLECTION in one
# module and ONE chain's detail template in the other - the same name with
# opposite meanings, which no reader could be expected to survive.
#
# The names carry no leading underscore because they are imported by name
# across the package's internal modules; the module itself is internal (nothing
# it defines is re-exported from the package root), so nothing here reaches SDK
# callers.
# ---------------------------------------------------------------------------

PATH_SEND = "/v1/send"

# Chain admin surface. Singular names template one chain_id; the plural
# ``PATH_CHAINS`` is the collection.
PATH_CHAIN = "/v1/admin/chains/{chain_id}"
PATH_CHAINS = "/v1/admin/chains"
PATH_CHAIN_BODY = "/v1/admin/chains/{chain_id}/body"
PATH_CHAIN_BUNDLE = "/v1/admin/chains/{chain_id}/bundle"
PATH_CHAIN_REPLAY = "/v1/admin/chains/{chain_id}/replay"
PATH_CHAIN_CANCEL = "/v1/admin/chains/{chain_id}/cancel"
PATH_CHAINS_EXTRACT = "/v1/admin/chains/extract"
PATH_EXPORT_TAR = "/v1/admin/export.tar"

# Cycle-7 group rollup + either-identifier lookups (plan § 6 task 5.1).
PATH_GROUP_STATUS = "/v1/admin/groups/{group_id}"
PATH_LOOKUP_BY_CAPTURED_ID = "/v1/admin/uploads/by-captured-id/{captured_id}"
PATH_LOOKUP_BY_LOCAL_UUID = "/v1/admin/uploads/by-local-uuid/{local_uuid}"

PATH_TOKENS = "/v1/admin/tokens"
PATH_TOKEN_FOR = "/v1/admin/tokens/{endpoint}/{uid}"

# Destination SigV4 credential push - host-keyed, the analogue of the
# per-(endpoint, uid) token slot above (the executor looks it up by host).
PATH_CREDENTIAL_FOR = "/v1/admin/credentials/{dest_host}"

PATH_STATS = "/v1/admin/stats"
# Liveness + readiness are the public, unprefixed probe paths (GET
# /v1/healthz, GET /v1/readyz). Phantom serves intake, admin, and health
# on one listener (loopback by default per ADR-004), so every path here
# rides the same base_url; these two just live outside the /v1/admin/
# prefix.
PATH_HEALTH = "/v1/healthz"
PATH_READY = "/v1/readyz"
PATH_ADMIN_STATUS = "/v1/admin/status"
PATH_INSTANCE_STATUS = "/v1/admin/instances/{instance_id}/status"
PATH_INSTANCES = "/v1/admin/instances"

# Plan § 4.2.5 observability endpoints.
PATH_OBSERVABILITY_COUNTERS = "/v1/admin/observability/counters"
PATH_OBSERVABILITY_GAUGES = "/v1/admin/observability/gauges"
PATH_OBSERVABILITY_RAM_PRESSURE = "/v1/admin/observability/ram_pressure"

# Plan § 5.2.5 quarantine inventory + § 1.5 restore.
PATH_QUARANTINE = "/v1/admin/quarantine"
PATH_QUARANTINE_RESTORE = "/v1/admin/quarantine/restore"

# Backoff jitter range; ±50% per RetryPolicy.backoff_jitter docstring.
_JITTER_HALF_RANGE = 0.5

# The documented Unix-domain-socket form of ``phantom_url``: ``unix:`` + the
# socket path (the service connections-table contract, mirroring
# ``server.bind_uds`` on the service side). Never handed to httpx as a URL -
# bare ``unix:`` is not a fetchable httpx scheme.
_UDS_URL_SCHEME = "unix:"
# Synthetic authority for request construction over UDS. httpx still needs an
# http base URL to build the request line and Host header; the UDS transport
# owns the actual connection routing, so this host token is never resolved.
_UDS_SYNTHETIC_BASE_URL = "http://phantom"


def _uds_socket_path(phantom_url: str) -> str | None:
    """Return the socket path when ``phantom_url`` is the ``unix:`` UDS form.

    ``unix:/abs/path.sock`` is the documented spelling;
    ``unix:///abs/path.sock`` (an empty URL authority) is tolerated as an
    alias. Any other URL returns ``None`` and is treated as the TCP form.
    """
    if not phantom_url.startswith(_UDS_URL_SCHEME):
        return None
    path = phantom_url[len(_UDS_URL_SCHEME) :]
    if path.startswith("//"):
        # unix:///abs/path - strip the empty authority marker.
        path = path[2:]
    return path


# ---------------------------------------------------------------------------
# Logging filter - strip Authorization from anything that reaches the logger.
# ---------------------------------------------------------------------------


class _AuthorizationRedactor(logging.Filter):
    """Filter that masks the value of any ``Authorization`` header in log records.

    Operates on the record's ``args`` and ``msg`` to avoid leaking bearer
    tokens via DEBUG/INFO records that include header dumps.
    """

    _MASK = "***redacted***"

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact in place; always emit the record."""
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(self._redact(a) for a in record.args)
            elif isinstance(record.args, Mapping):
                record.args = {k: self._redact(v) for k, v in record.args.items()}
        record.msg = self._redact_str(str(record.msg))
        return True

    def _redact(self, value: object) -> object:
        """Return ``value`` with every Authorization entry and bearer string masked."""
        if isinstance(value, dict):
            return {
                k: (self._MASK if k.lower() == "authorization" else self._redact(v))
                for k, v in value.items()
            }
        if isinstance(value, (list, tuple)):
            return type(value)(self._redact(v) for v in value)
        if isinstance(value, str):
            return self._redact_str(value)
        return value

    def _redact_str(self, value: str) -> str:
        """Return ``value`` with any ``Bearer <token>`` sequence replaced by the mask."""
        # Best-effort: replace "Bearer <token>" sequences. This catches both
        # explicit string-formatted records and stringified dicts.
        lower = value.lower()
        bearer_idx = lower.find("bearer ")
        if bearer_idx == -1:
            return value
        # Replace everything from "bearer " up to next whitespace / quote / brace.
        out = []
        i = 0
        while i < len(value):
            sub = value[i:]
            ls = sub.lower()
            if ls.startswith("bearer "):
                out.append(value[i : i + 7])
                j = i + 7
                while j < len(value) and value[j] not in " '\",}]":
                    j += 1
                out.append(self._MASK)
                i = j
            else:
                out.append(value[i])
                i += 1
        return "".join(out)


_LOG.addFilter(_AuthorizationRedactor())


# ---------------------------------------------------------------------------
# httpx -> SDK exception translation.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _TranslatedTransportError:
    """One ``httpx`` failure rendered as the SDK's typed equivalent.

    Attributes:
        error: The :class:`~phantom_client.errors.PhantomTransportError`
            subclass a caller should see.
        never_landed: True when the request PROVABLY never reached the
            server, so a retry cannot duplicate anything.
    """

    error: PhantomTransportError
    never_landed: bool


def _translate_httpx_error(exc: httpx.HTTPError) -> _TranslatedTransportError:
    """Map one ``httpx`` request failure onto the SDK exception hierarchy.

    The SINGLE mapping site. Every path that issues a request through this
    module funnels its ``httpx`` failures here, so a caller wrapping SDK calls
    in ``except PhantomTransportError`` sees the documented hierarchy no matter
    which method raised - buffered or streaming.

    The retry decision and the surfaced error TYPE are independent axes, which
    is why this returns both. A ``ConnectTimeout`` is never-landed AND a
    timeout, so it keeps its :class:`PhantomTimeoutError` mapping; the clause
    order below is load-bearing for that, because ``ConnectTimeout`` and
    ``PoolTimeout`` subclass ``TimeoutException`` and ``ConnectError`` is a
    ``NetworkError`` sibling of ``ReadError``.

    ``HTTPStatusError`` is outside the split: it comes from
    ``raise_for_status()``, never from a request call, and 5xx responses are
    deliberately passed through to the caller.

    Args:
        exc: The failure raised by an ``httpx`` request or stream call.

    Returns:
        The typed error plus whether the request provably never landed.
    """
    if isinstance(exc, (httpx.ConnectTimeout, httpx.PoolTimeout)):
        # NEVER DELIVERED, but still a timeout for callers: a dropped SYN is
        # the most common "Phantom unreachable" shape on a real producer
        # network, and an exhausted pool never reached the wire at all.
        return _TranslatedTransportError(PhantomTimeoutError(f"timeout: {exc}"), never_landed=True)
    if isinstance(exc, httpx.ConnectError):
        # Never delivered: the connection itself was refused. A missing UDS
        # socket arrives here too, which is what makes the module docstring's
        # "like any refused TCP connect" promise true.
        return _TranslatedTransportError(
            PhantomConnectError(f"connect refused: {exc}"), never_landed=True
        )
    if isinstance(exc, (httpx.LocalProtocolError, httpx.UnsupportedProtocol)):
        # No request was ever built: a malformed local request and an
        # unsupported URL scheme both fail before the wire.
        return _TranslatedTransportError(
            PhantomNetworkError(f"network error: {exc}"), never_landed=True
        )
    if isinstance(exc, httpx.TimeoutException):
        # ReadTimeout / WriteTimeout: the server MAY have received and
        # executed this request, and only the response was lost (F12).
        return _TranslatedTransportError(PhantomTimeoutError(f"timeout: {exc}"), never_landed=False)
    # Not provably undelivered: a reset or a server disconnect can land AFTER
    # the request executed (ReadError, RemoteProtocolError).
    return _TranslatedTransportError(
        PhantomNetworkError(f"network error: {exc}"), never_landed=False
    )


# ---------------------------------------------------------------------------
# Poll-hinted read result.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PollHintedResponse[T: BaseModel]:
    """A parsed admin body plus the service's suggested next-poll delay.

    Returned by :meth:`Transport.get_json_with_poll_hint`. A tuple would
    carry the same two values; a named pair is what makes ``body`` and
    ``suggested_poll_after_seconds`` readable at the call site.

    Attributes:
        body: The response body validated against the caller's model.
        suggested_poll_after_seconds: The ``X-Phantom-Suggested-Poll-After``
            hint in seconds, or ``None`` when the response carried no hint.
    """

    body: T
    suggested_poll_after_seconds: float | None


# ---------------------------------------------------------------------------
# Transport.
# ---------------------------------------------------------------------------


class Transport:
    """Internal HTTP transport. Construct via :class:`PhantomClient`."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Build the transport (does NOT start the underlying client).

        Args:
            config: The full client config.
            transport: Optional injected ``httpx.AsyncBaseTransport`` for
                test purposes. When ``None``, real network I/O is used.
        """
        self._config = config
        self._injected_transport = transport
        self._timeout = httpx.Timeout(
            connect=config.timeouts.connect,
            read=config.timeouts.read,
            write=config.timeouts.write,
            pool=config.timeouts.pool,
        )
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Open the underlying httpx client. Idempotent.

        A ``unix:`` ``phantom_url`` selects a real UDS transport and the
        synthetic http base URL (httpx cannot fetch a bare ``unix:`` URL); an
        injected test transport always wins over the automatic selection.
        """
        if self._client is not None:
            return
        transport = self._injected_transport
        base_url = self._config.phantom_url
        uds_path = _uds_socket_path(base_url)
        if uds_path is not None:
            if transport is None:
                transport = httpx.AsyncHTTPTransport(uds=uds_path)
            base_url = _UDS_SYNTHETIC_BASE_URL
        self._client = httpx.AsyncClient(
            transport=transport,
            timeout=self._timeout,
            base_url=base_url,
            headers=self._config.default_headers,
        )
        _LOG.debug("transport started: phantom_url=%s", self._config.phantom_url)

    async def aclose(self) -> None:
        """Close the underlying httpx client. Safe to call multiple times."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -----------------------------------------------------------------
    # submit_chain - the load-bearing primitive.
    # -----------------------------------------------------------------

    async def submit_chain(
        self,
        envelope: ChainEnvelope,
        body_refs: dict[str, bytes] | None,
        *,
        uid: str | None,
        auth_token: str | None,
        options: SubmitOptions | None,
    ) -> ChainResponse:
        """Submit a chain envelope to ``POST /v1/send``.

        Selects JSON vs. multipart encoding based on whether
        ``body_refs`` is non-empty. The serialized envelope uses the
        ADR-010 wire form (``by_alias=True`` so ``from_path`` emits as
        ``from``).

        Args:
            envelope: The chain to execute.
            body_refs: Bytes for each body_ref in the envelope, keyed
                by name. Required when the envelope contains any
                body_ref bodies.
            uid: Maps to ``X-Phantom-Uid``.
            auth_token: Full ``Authorization`` value (e.g.,
                ``"Bearer <token>"``).
            options: Per-call submission overrides.

        Returns:
            The parsed :class:`ChainResponse` from Phantom's 202 reply.

        Raises:
            PhantomTransportError or subclass: On transport failure
                after exhausting :class:`RetryPolicy.max_attempts`.
            PhantomHttpError or subclass: On a non-2xx with a parsable
                error envelope.
            PhantomEnvelopeError: When the response body or shape is
                unrecognizable.
        """
        sdk_idempotency_key = str(envelope.chain_id)
        headers = build_request_headers(
            uid=uid,
            auth_token=auth_token,
            options=options,
            sdk_idempotency_key=sdk_idempotency_key,
        )
        envelope_json = envelope.model_dump_json(by_alias=True)

        if body_refs:
            # Opt in: this is the ONE call that sends
            # X-Phantom-Idempotency-Key on every attempt, and admission's
            # atomic claim turns a re-arrival into a 200 replay.
            response = await self._send_with_retry(
                "POST",
                PATH_SEND,
                headers=headers,
                files=self._build_multipart(envelope_json, body_refs),
                retry_if_may_have_landed=True,
            )
        else:
            # Opt in: same call, other encoding. Both arms or neither, or one
            # encoding silently loses its retry.
            response = await self._send_with_retry(
                "POST",
                PATH_SEND,
                headers={**headers, "Content-Type": "application/json"},
                content=envelope_json,
                retry_if_may_have_landed=True,
            )
        self._raise_for_status(response)
        try:
            return ChainResponse.model_validate_json(response.content)
        except ValidationError as exc:
            raise PhantomEnvelopeError(
                f"could not parse ChainResponse from POST /v1/send body: {exc}"
            ) from exc

    # -----------------------------------------------------------------
    # Generic JSON helpers for the admin surface.
    # -----------------------------------------------------------------

    async def get_json[T: BaseModel](
        self,
        path: str,
        *,
        model: type[T],
        params: dict[str, QueryParamValue] | None = None,
    ) -> T:
        """GET ``path`` and parse the response body against ``model``.

        Args:
            path: Path on the configured ``phantom_url``.
            model: Pydantic model class to validate the response body.
            params: Optional query parameters; values are ``str`` or ``int``.

        Returns:
            An instance of ``model``.
        """
        # Opt in: read-only, so a re-arrival changes nothing.
        response = await self._send_with_retry(
            "GET", path, params=params, retry_if_may_have_landed=True
        )
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def get_json_with_poll_hint[T: BaseModel](
        self,
        path: str,
        *,
        model: type[T],
        params: dict[str, QueryParamValue] | None = None,
    ) -> PollHintedResponse[T]:
        """GET ``path``, parse it against ``model``, and read the poll hint.

        :meth:`get_json` with one addition: the service's
        ``X-Phantom-Suggested-Poll-After`` header is parsed and returned
        alongside the body. It exists because the pollers need the header and
        :meth:`get_json` throws the response away; they used to reach through
        ``_require_client`` and ``_raise_for_status`` and issue the GET on the
        raw ``httpx`` client to get it, which silently cost them
        :meth:`_send_with_retry`'s retry policy AND its exception translation.
        The hint belongs on this class's PUBLIC surface, not behind a reach-in.

        Args:
            path: Path on the configured ``phantom_url``.
            model: Pydantic model class to validate the response body.
            params: Optional query parameters; values are ``str`` or ``int``.

        Returns:
            The parsed body paired with the suggested next-poll delay
            (``None`` when the response carried no hint).

        Raises:
            PhantomTransportError or subclass: On transport failure after
                exhausting :class:`RetryPolicy.max_attempts`.
            PhantomHttpError or subclass: On a non-2xx with a parsable error
                envelope.
            PhantomEnvelopeError: When the body fails to parse against
                ``model`` or the poll hint is not an integer.
        """
        # Opt in for the same reason get_json does: read-only, so a re-arrival
        # changes nothing. A Phantom restarting mid-poll costs one retry
        # rather than aborting the whole poll.
        response = await self._send_with_retry(
            "GET", path, params=params, retry_if_may_have_landed=True
        )
        self._raise_for_status(response)
        return PollHintedResponse(
            body=self._parse_json(response, model),
            suggested_poll_after_seconds=self._parse_poll_hint(response),
        )

    async def post_json[T: BaseModel](
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        model: type[T],
        params: dict[str, QueryParamValue] | None = None,
    ) -> T:
        """POST a JSON body and parse the response against ``model``."""
        payload = self._serialize_body(body)
        # Opt OUT: its callers are replay (which re-queues a row that may have
        # succeeded since, delivering the upload twice), cancel and quarantine
        # restore. A lost response costs one manual operator retry.
        response = await self._send_with_retry(
            "POST",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
            retry_if_may_have_landed=False,
        )
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def put_json(
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        params: dict[str, QueryParamValue] | None = None,
    ) -> None:
        """PUT a JSON body; ignore the response body (204-style)."""
        payload = self._serialize_body(body)
        # Opt in: its only callers are the token and credential pushes, which
        # are pure overwrites of one slot; a second write stores the same value.
        response = await self._send_with_retry(
            "PUT",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
            retry_if_may_have_landed=True,
        )
        self._raise_for_status(response)

    async def delete_no_body(
        self,
        path: str,
        *,
        params: dict[str, QueryParamValue] | None = None,
    ) -> None:
        """DELETE ``path``; ignore the response body."""
        # Opt OUT: its callers converge, but it is kept off for consistency
        # with the other DELETE helper; the cost is one manual operator retry.
        response = await self._send_with_retry(
            "DELETE", path, params=params, retry_if_may_have_landed=False
        )
        self._raise_for_status(response)

    async def delete_json[T: BaseModel](
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        model: type[T],
        params: dict[str, QueryParamValue] | None = None,
    ) -> T:
        """DELETE ``path`` with a JSON body; parse the response against ``model``."""
        payload = self._serialize_body(body)
        # Opt OUT: its caller is bulk_delete, which is NOT convergent: the
        # filter is re-evaluated against the live table on every call, so a
        # retry sweeps up rows the first request never saw.
        response = await self._send_with_retry(
            "DELETE",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
            retry_if_may_have_landed=False,
        )
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def stream_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryParamValue] | None = None,
        content: bytes | str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[bytes]:
        """Stream the response body of ``method path`` as bytes chunks.

        Method-general because the streaming core is: open a stream, drain
        and raise on a 4xx/5xx, then yield chunks. That core is the same for
        the GET body fetches, for export.tar, and for the POST-with-a-filter
        bulk extract, which used to reach through two private members of this
        class to re-implement it.

        ``httpx`` failures are translated through the same
        :func:`_translate_httpx_error` mapping the buffered path uses, so a
        caller wrapping every SDK call in ``except PhantomTransportError``
        holds for the streaming methods too. Before the fix these three leaked
        raw ``httpx`` errors, so a missing UDS socket surfaced as
        ``httpx.ConnectError`` here while the same socket surfaced as
        :class:`~phantom_client.errors.PhantomConnectError` from every buffered
        read - contradicting this module's own docstring.

        Retry is scoped to the never-landed class and only BEFORE the first
        chunk reaches the caller. That is the deliberate reading of the
        no-retry-on-partial-stream rule: the rule protects a stream that
        already started, whereas a refused connect never landed and the caller
        has seen nothing, so re-opening cannot duplicate or interleave bytes.
        It also makes a restarting Phantom survivable here exactly as it is on
        the buffered reads. Once ANY chunk has been yielded, nothing is
        retried, whatever the failure class: the SDK cannot rewind bytes the
        caller already holds, and the upstream is the recovery surface.

        An async generator, deliberately, so the not-started check runs LAZILY
        on first iteration exactly as it did before. A caller that needs the
        check EAGERLY calls :meth:`require_started` first; the eagerness is a
        property of the caller, not of this method.

        Args:
            method: The HTTP method to stream.
            path: Path relative to the configured Phantom URL.
            params: Optional query parameters; values are ``str`` or ``int``.
            content: Optional request body.
            headers: Optional request headers.

        Yields:
            Response body chunks, in order.

        Raises:
            PhantomTransportError or subclass: On a transport failure, after
                exhausting :class:`RetryPolicy.max_attempts` for a never-landed
                failure that struck before the first chunk.
            PhantomHttpError or subclass: On a non-2xx with a parsable error
                envelope.
        """
        client = self._require_client()
        policy = self._config.retry_policy
        max_attempts = policy.max_attempts if policy.enabled else 1
        attempt = 0
        while True:
            attempt += 1
            # Reset per attempt: a retry re-opens the stream from scratch, so
            # only chunks yielded on THIS attempt bar a further retry.
            yielded_any = False
            try:
                # Stream rather than buffer so memory stays bounded.
                async with client.stream(
                    method, path, params=params, content=content, headers=headers
                ) as response:
                    if response.status_code >= 400:
                        # Drain so we can parse the error envelope.
                        await response.aread()
                        self._raise_for_status(response)
                    async for chunk in response.aiter_bytes():
                        yielded_any = True
                        yield chunk
                return
            except httpx.HTTPError as exc:
                translated = _translate_httpx_error(exc)
                if yielded_any or not translated.never_landed or attempt >= max_attempts:
                    _LOG.error(
                        "stream failure on %s %s after %d attempt(s): %s",
                        method,
                        path,
                        attempt,
                        translated.error,
                    )
                    raise translated.error from exc
                delay = _compute_backoff(policy, attempt)
                _LOG.warning(
                    "retrying stream %s %s after %.3fs (attempt %d/%d): %s",
                    method,
                    path,
                    delay,
                    attempt,
                    max_attempts,
                    translated.error,
                )
            await asyncio.sleep(delay)

    def require_started(self) -> None:
        """Raise if the transport has not been started.

        The public form of the not-started check. A caller that must raise at
        ``await`` time rather than at first iteration (``PhantomClient.extract``
        does) calls this before returning a stream, which is what it used to
        reach into ``_require_client`` for.

        Raises:
            RuntimeError: When ``start()`` has not been called.
        """
        self._require_client()

    # -----------------------------------------------------------------
    # Internals.
    # -----------------------------------------------------------------

    def _require_client(self) -> httpx.AsyncClient:
        """Return the live client; raise if start() hasn't been called."""
        if self._client is None:
            raise RuntimeError("Transport.start() must be called before use")
        return self._client

    @staticmethod
    def _serialize_body(body: BaseModel | dict[str, Any] | None) -> str:
        """Serialize a body model or dict to a JSON string."""
        if body is None:
            return ""
        if isinstance(body, BaseModel):
            return body.model_dump_json(by_alias=True)
        # Use Pydantic's TypeAdapter via a tiny round-trip for consistency.
        import json

        return json.dumps(body, default=str)

    @staticmethod
    def _parse_json[T: BaseModel](response: httpx.Response, model: type[T]) -> T:
        """Validate ``response.content`` against ``model``."""
        try:
            return model.model_validate_json(response.content)
        except ValidationError as exc:
            raise PhantomEnvelopeError(
                f"could not parse {model.__name__} from {response.request.url}: {exc}"
            ) from exc

    @staticmethod
    def _parse_poll_hint(response: httpx.Response) -> float | None:
        """Return ``X-Phantom-Suggested-Poll-After`` in seconds, or None.

        The header is documented as integer seconds; a value that is not an
        integer is a service-contract violation rather than a hint to round,
        so it raises rather than being silently ignored.

        Raises:
            PhantomEnvelopeError: When the header is present but non-integer.
        """
        raw = response.headers.get(X_PHANTOM_SUGGESTED_POLL_AFTER)
        if raw is None:
            return None
        try:
            return float(int(raw))
        except (TypeError, ValueError) as exc:
            raise PhantomEnvelopeError(
                f"non-integer {X_PHANTOM_SUGGESTED_POLL_AFTER!r}: {raw!r}"
            ) from exc

    @staticmethod
    def _build_multipart(
        envelope_json: str, body_refs: dict[str, bytes]
    ) -> list[tuple[str, tuple[str, bytes, str]]]:
        """Build httpx's ``files=`` payload for an envelope + body_refs submission.

        Each entry is ``(field_name, (filename, bytes, content_type))``;
        the field name is what receivers parse. The envelope rides as a
        single ``envelope`` part with content-type ``application/json``;
        each body_ref rides as ``body_refs[<name>]`` with the documented
        content-type (defaults to ``application/octet-stream``).

        Every part carries a non-empty filename so receivers parse it as
        an upload-file part. With ``filename=None``, starlette's
        ``MultiPartParser`` treats the part as a regular form field and
        UTF-8-decodes the bytes; that inflates binary payloads (a random
        100 KiB body grows to ~150 KiB and its hash mutates), violating
        the transparent-proxy invariant. The filename value is cosmetic
        from receivers' perspective - multipart parsing only branches on
        whether the value is non-empty, not on what it spells.
        """
        parts: list[tuple[str, tuple[str, bytes, str]]] = [
            ("envelope", ("envelope.json", envelope_json.encode("utf-8"), "application/json")),
        ]
        for name, blob in body_refs.items():
            parts.append(
                (
                    f"body_refs[{name}]",
                    (name, blob, "application/octet-stream"),
                )
            )
        return parts

    async def _send_with_retry(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, QueryParamValue] | None = None,
        content: str | bytes | None = None,
        files: list[tuple[str, tuple[str, bytes, str]]] | None = None,
        retry_if_may_have_landed: bool = False,
    ) -> httpx.Response:
        """Send a single request, retrying transport failures per policy.

        Transport failures split into two classes, and only the caller knows
        whether the second one is safe to retry:

        * **Never landed.** ``ConnectError``, ``ConnectTimeout``,
          ``PoolTimeout``, ``LocalProtocolError`` and ``UnsupportedProtocol``.
          The request was never delivered, so a retry cannot duplicate
          anything. Always retried.
        * **May have landed.** Everything else ``httpx`` raises from the
          request call: ``ReadTimeout`` and ``WriteTimeout``, and the
          ``HTTPError`` catch-all's members such as ``ReadError``,
          ``RemoteProtocolError``, ``CloseError``, ``ProxyError``,
          ``DecodingError`` and ``TooManyRedirects``. Each is reachable AFTER
          the server received bytes, so the server may have executed the
          request and only the response was lost. Retried ONLY when the caller
          passes ``retry_if_may_have_landed=True``.

        Which class a failure falls into, and which typed exception it becomes,
        are both decided by :func:`_translate_httpx_error` - the one mapping
        site, shared with :meth:`stream_request` so the buffered and streaming
        paths cannot drift apart.

        Args:
            method: The HTTP verb.
            path: The path on the configured ``phantom_url``.
            headers: Optional request headers.
            params: Optional query parameters; values are ``str`` or ``int``.
            content: Optional raw request body.
            files: Optional multipart parts.
            retry_if_may_have_landed: Whether a failure that may have executed
                server-side is safe to re-send. True only for calls that are
                read-only, that carry an idempotency key the service dedupes
                on, or whose effect is a pure overwrite.

        Returns:
            The :class:`httpx.Response`, which may still be a non-2xx.

        Raises:
            PhantomTransportError or subclass: On a never-landed failure after
                exhausting the policy, or immediately on a may-have-landed
                failure the caller did not opt in for.
        """
        client = self._require_client()
        policy = self._config.retry_policy
        last_error: Exception | None = None
        max_attempts = policy.max_attempts if policy.enabled else 1
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                _LOG.debug(
                    "request: %s %s headers=%s body_size=%s",
                    method,
                    path,
                    dict(headers) if headers else {},
                    len(content) if content is not None else 0,
                )
                response = await client.request(
                    method,
                    path,
                    headers=dict(headers) if headers else None,
                    params=dict(params) if params else None,
                    content=content,
                    files=files,
                )
                _LOG.debug(
                    "response: %s status=%d body_size=%d",
                    path,
                    response.status_code,
                    len(response.content),
                )
                return response
            except httpx.HTTPError as exc:
                translated = _translate_httpx_error(exc)
                if not translated.never_landed and not retry_if_may_have_landed:
                    raise translated.error from exc
                last_error = translated.error
            if attempt < max_attempts:
                delay = _compute_backoff(policy, attempt)
                _LOG.warning(
                    "retrying %s %s after %.3fs (attempt %d/%d): %s",
                    method,
                    path,
                    delay,
                    attempt,
                    max_attempts,
                    last_error,
                )
                await asyncio.sleep(delay)
        assert last_error is not None  # at least one attempt always runs
        _LOG.error("transport failure after %d attempts: %s", max_attempts, last_error)
        raise last_error

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        """Translate non-2xx into typed exceptions per ADR-010 error envelope."""
        if response.status_code < 400:
            return
        try:
            body: dict[str, Any] = response.json()
        except ValueError as exc:
            raise PhantomEnvelopeError(
                f"non-2xx response with non-JSON body: status={response.status_code} "
                f"body={response.content!r}"
            ) from exc
        raise_for_error_body(
            body,
            status_code=response.status_code,
            response_headers=dict(response.headers),
        )


def _compute_backoff(policy: RetryPolicy, attempt: int) -> float:
    """Exponential backoff in seconds with optional ±50% jitter."""
    base: float = policy.backoff_initial_seconds * (2 ** (attempt - 1))
    capped: float = min(base, policy.backoff_max_seconds)
    if not policy.backoff_jitter:
        return capped
    rand: float = float(random.random())
    jitter: float = capped * _JITTER_HALF_RANGE * (2.0 * rand - 1.0)
    return max(0.0, capped + jitter)


__all__ = ["PollHintedResponse", "Transport"]
