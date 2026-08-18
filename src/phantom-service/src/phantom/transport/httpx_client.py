"""httpx-backed :class:`UpstreamClient`."""

from __future__ import annotations

import logging

import httpx

from phantom.transport.interface import UpstreamRequest, UpstreamResponse

logger = logging.getLogger(__name__)


class HttpxUpstreamClient:
    """Single httpx.AsyncClient wrapping the upstream-network surface."""

    def __init__(
        self,
        *,
        timeout_seconds: float,
        verify: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Construct the client.

        Args:
            timeout_seconds: Per-request timeout. Required with no default:
                the composition root supplies it from
                ``upstream.timeout_seconds``, which is the one place the
                value is described, validated and exported.
            verify: TLS verification on/off.
            transport: Optional httpx transport (e.g., ``MockTransport``,
                ``ASGITransport``) for test injection.
        """
        self._timeout_seconds = timeout_seconds
        self._verify = verify
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Open the underlying client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout_seconds,
                verify=self._verify,
                transport=self._transport,
            )

    async def stop(self) -> None:
        """Close the underlying client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        """Issue one HTTP request and return the response.

        Honors ``req.timeout_seconds`` per-call when set (§5.2); the
        client's constructor timeout is the fallback default.
        """
        if self._client is None:
            raise RuntimeError("HttpxUpstreamClient is not started")
        # When the caller supplied a per-route override, use it; otherwise
        # httpx falls back to the AsyncClient-constructor default.
        request_kwargs: dict[str, object] = {
            "method": req.method,
            "url": req.url,
            "headers": req.headers,
            "content": req.body if req.body else None,
        }
        if req.timeout_seconds is not None:
            request_kwargs["timeout"] = req.timeout_seconds
        try:
            response = await self._client.request(**request_kwargs)  # type: ignore[arg-type]
        except httpx.HTTPError as exc:
            logger.warning("Upstream request failed: %s", exc)
            raise
        return UpstreamResponse(
            status=response.status_code,
            headers=dict(response.headers),
            body=response.content,
        )
