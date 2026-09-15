"""httpx-backed :class:`UpstreamClient`.

The one thing this module owns beyond translating shapes is the meaning of
``upstream.timeout_seconds``. httpx expands a bare float into
``Timeout(connect=t, read=t, write=t, pool=t)``, and every one of those is a
PER-SOCKET-OPERATION bound: the read timeout restarts on each chunk that
arrives. An upstream or intermediary emitting one byte just inside the window
therefore keeps an attempt alive indefinitely (finding S9-4), and there is
nothing further out that can reclaim it: the sender's worker pool is sized
``min(8, max(2, cpu_count))`` so the stalled attempt holds a worker, it holds
its saturation slot and declared bytes so admission starts refusing with 503s,
and ADR-032's send-deadline gate cannot see it because that gate runs once per
attempt BEFORE any I/O rather than during one. Recovery needed a restart.

:meth:`HttpxUpstreamClient.send` therefore wraps the request in a
whole-operation deadline of the same configured value, so the knob means what
its description says. The per-socket timeouts are kept alongside it as the
finer-grained inner bound: they still end a dead connect or a silent socket
sooner than the deadline would.
"""

from __future__ import annotations

import asyncio
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
            timeout_seconds: Per-request timeout, applied BOTH as httpx's
                per-socket-operation bound and as the whole-operation
                deadline :meth:`send` enforces (S9-4). Required with no
                default: the composition root supplies it from
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
            # A bare float here is httpx's per-socket-operation form, kept
            # deliberately as the INNER bound; the whole-operation deadline
            # lives in send(). See the module docstring for why one without
            # the other is not a timeout at all.
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
        """Issue one HTTP request under a whole-operation deadline.

        Honors ``req.timeout_seconds`` per-call when set (§5.2); the client's
        constructor timeout is the fallback default. That one value is BOTH
        bounds: httpx's per-socket-operation timeouts, and the wall-clock
        deadline on the whole attempt that the module docstring explains
        (S9-4). The deadline is the outer of the two, so a request that trips
        a socket timeout still fails with httpx's own precise exception.

        Raises:
            httpx.TimeoutException: When the attempt outlives its deadline.
                Deliberately an ``httpx.HTTPError`` rather than the
                ``TimeoutError`` :func:`asyncio.timeout` raises: the executor
                classifies ``httpx.HTTPError`` as a retryable
                ``FailedNetwork``, and a bare ``TimeoutError`` would instead
                escape the sender's worker loop.
            httpx.HTTPError: Whatever the transport raised, re-raised after
                logging.
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
        deadline_seconds = self._timeout_seconds
        if req.timeout_seconds is not None:
            request_kwargs["timeout"] = req.timeout_seconds
            deadline_seconds = req.timeout_seconds
        try:
            async with asyncio.timeout(deadline_seconds):
                response = await self._client.request(**request_kwargs)  # type: ignore[arg-type]
        except TimeoutError as exc:
            # The URL is deliberately NOT logged: a presigned step URL carries
            # the signature in its query string, and this line would be the
            # one place it reached the log.
            logger.warning(
                "Upstream %s request exceeded its %s s whole-operation deadline",
                req.method,
                deadline_seconds,
            )
            raise httpx.TimeoutException(
                f"upstream attempt exceeded its {deadline_seconds} s whole-operation deadline"
            ) from exc
        except httpx.HTTPError as exc:
            logger.warning("Upstream request failed: %s", exc)
            raise
        return UpstreamResponse(
            status=response.status_code,
            headers=dict(response.headers),
            body=response.content,
        )
