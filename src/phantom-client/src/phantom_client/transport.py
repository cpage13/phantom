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
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError

from phantom_client.config import ClientConfig, RetryPolicy, SubmitOptions
from phantom_client.errors import (
    PhantomConnectError,
    PhantomEnvelopeError,
    PhantomNetworkError,
    PhantomTimeoutError,
    raise_for_error_body,
)
from phantom_client.headers import build_request_headers
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

# Path constants - single source of truth for the v1 URL space.
_PATH_SEND = "/v1/send"

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
                _PATH_SEND,
                headers=headers,
                files=self._build_multipart(envelope_json, body_refs),
                retry_if_may_have_landed=True,
            )
        else:
            # Opt in: same call, other encoding. Both arms or neither, or one
            # encoding silently loses its retry.
            response = await self._send_with_retry(
                "POST",
                _PATH_SEND,
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

        No retry on partial-stream failures: the upstream is the recovery
        surface. That rule is about streaming in general rather than about
        GET, which is why it lives here.

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
        """
        client = self._require_client()
        # Stream rather than buffer so memory stays bounded.
        async with client.stream(
            method, path, params=params, content=content, headers=headers
        ) as response:
            if response.status_code >= 400:
                # Drain so we can parse the error envelope.
                await response.aread()
                self._raise_for_status(response)
            async for chunk in response.aiter_bytes():
                yield chunk

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

        The retry decision and the surfaced error TYPE are independent axes.
        A ``ConnectTimeout`` is never-landed AND a timeout, so it keeps its
        ``PhantomTimeoutError`` mapping; the clause order below is load-bearing
        for that, because ``ConnectTimeout`` and ``PoolTimeout`` subclass
        ``TimeoutException`` and ``ConnectError`` is a ``NetworkError`` sibling
        of ``ReadError``.

        ``HTTPStatusError`` is outside the split: it comes from
        ``raise_for_status()``, never from the request call this wraps, and 5xx
        responses are deliberately passed through to the caller.

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
            except (httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
                # NEVER DELIVERED, but still a timeout for callers: a dropped
                # SYN is the most common "Phantom unreachable" shape on a real
                # producer network, and an exhausted pool never reached the
                # wire at all. Both keep the PhantomTimeoutError mapping the
                # committed exhaustion pin requires. This clause MUST precede
                # the broad TimeoutException clause below, which they subclass.
                last_error = PhantomTimeoutError(f"timeout: {exc}")
            except httpx.ConnectError as exc:
                # Never delivered: the connection itself was refused.
                last_error = PhantomConnectError(f"connect refused: {exc}")
            except (httpx.LocalProtocolError, httpx.UnsupportedProtocol) as exc:
                # No request was ever built: a malformed local request and an
                # unsupported URL scheme both fail before the wire.
                last_error = PhantomNetworkError(f"network error: {exc}")
            except httpx.TimeoutException as exc:
                # ReadTimeout / WriteTimeout: the server MAY have received and
                # executed this request, and only the response was lost (F12).
                if not retry_if_may_have_landed:
                    raise PhantomTimeoutError(f"timeout: {exc}") from exc
                last_error = PhantomTimeoutError(f"timeout: {exc}")
            except httpx.HTTPError as exc:
                # Not provably undelivered: a reset or a server disconnect can
                # land AFTER the request executed (ReadError,
                # RemoteProtocolError). Same gate.
                if not retry_if_may_have_landed:
                    raise PhantomNetworkError(f"network error: {exc}") from exc
                last_error = PhantomNetworkError(f"network error: {exc}")
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


__all__ = ["Transport"]
