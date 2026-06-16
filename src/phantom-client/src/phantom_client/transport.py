"""Internal HTTP transport — the SDK's single source of truth for the wire.

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
- Retries happen **only** for transport-class failures (connect refused,
  read/write/pool timeouts). 5xx responses are passed through to the
  caller untouched — Phantom IS the retry engine, so doubling up muddies
  idempotency. Every attempt carries the same
  ``X-Phantom-Idempotency-Key`` so Phantom dedupes a re-arrival.
- Non-2xx responses are parsed as the ADR-010 ``ErrorEnvelope`` and
  raised as a typed :class:`~phantom_client.errors.PhantomHttpError`
  subclass.
- ``Authorization`` is never logged — the logging filter redacts it.
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

# Path constants — single source of truth for the v1 URL space.
_PATH_SEND = "/v1/send"

# Backoff jitter range; ±50% per RetryPolicy.backoff_jitter docstring.
_JITTER_HALF_RANGE = 0.5


# ---------------------------------------------------------------------------
# Logging filter — strip Authorization from anything that reaches the logger.
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
        """Open the underlying httpx client. Idempotent."""
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            transport=self._injected_transport,
            timeout=self._timeout,
            base_url=self._config.phantom_url,
            headers=self._config.default_headers,
        )
        _LOG.debug("transport started: phantom_url=%s", self._config.phantom_url)

    async def aclose(self) -> None:
        """Close the underlying httpx client. Safe to call multiple times."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -----------------------------------------------------------------
    # submit_chain — the load-bearing primitive.
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
            response = await self._send_with_retry(
                "POST",
                _PATH_SEND,
                headers=headers,
                files=self._build_multipart(envelope_json, body_refs),
            )
        else:
            response = await self._send_with_retry(
                "POST",
                _PATH_SEND,
                headers={**headers, "Content-Type": "application/json"},
                content=envelope_json,
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
        params: dict[str, Any] | None = None,
    ) -> T:
        """GET ``path`` and parse the response body against ``model``.

        Args:
            path: Path on the configured ``phantom_url``.
            model: Pydantic model class to validate the response body.
            params: Optional query parameters.

        Returns:
            An instance of ``model``.
        """
        response = await self._send_with_retry("GET", path, params=params)
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def post_json[T: BaseModel](
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        model: type[T],
        params: dict[str, Any] | None = None,
    ) -> T:
        """POST a JSON body and parse the response against ``model``."""
        payload = self._serialize_body(body)
        response = await self._send_with_retry(
            "POST",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
        )
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def put_json(
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        params: dict[str, Any] | None = None,
    ) -> None:
        """PUT a JSON body; ignore the response body (204-style)."""
        payload = self._serialize_body(body)
        response = await self._send_with_retry(
            "PUT",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
        )
        self._raise_for_status(response)

    async def delete_no_body(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> None:
        """DELETE ``path``; ignore the response body."""
        response = await self._send_with_retry("DELETE", path, params=params)
        self._raise_for_status(response)

    async def delete_json[T: BaseModel](
        self,
        path: str,
        *,
        body: BaseModel | dict[str, Any] | None,
        model: type[T],
        params: dict[str, Any] | None = None,
    ) -> T:
        """DELETE ``path`` with a JSON body; parse the response against ``model``."""
        payload = self._serialize_body(body)
        response = await self._send_with_retry(
            "DELETE",
            path,
            params=params,
            content=payload,
            headers={"Content-Type": "application/json"},
        )
        self._raise_for_status(response)
        return self._parse_json(response, model)

    async def stream_bytes(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> AsyncIterator[bytes]:
        """Stream the response body of GET ``path`` as bytes chunks.

        Used for body fetches, export.tar, and bulk extract. No retry on
        partial-stream failures — the upstream is the recovery surface.
        """
        client = self._require_client()
        # Use a streaming GET so memory stays bounded.
        async with client.stream("GET", path, params=params) as response:
            if response.status_code >= 400:
                # Drain so we can parse the error envelope.
                await response.aread()
                self._raise_for_status(response)
            async for chunk in response.aiter_bytes():
                yield chunk

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
        from receivers' perspective — multipart parsing only branches on
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
        params: Mapping[str, Any] | None = None,
        content: str | bytes | None = None,
        files: list[tuple[str, tuple[str, bytes, str]]] | None = None,
    ) -> httpx.Response:
        """Send a single request, retrying transport-class failures per policy."""
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
            except httpx.ConnectError as exc:
                last_error = PhantomConnectError(f"connect refused: {exc}")
            except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as exc:
                last_error = PhantomTimeoutError(f"timeout: {exc}")
            except httpx.HTTPError as exc:
                # Catch-all for other network-class failures.
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
