"""UpstreamClient Protocol and request/response shapes."""

from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field


class UpstreamRequest(BaseModel):
    """One outbound HTTP request emitted by the executor."""

    model_config = ConfigDict(strict=True, extra="forbid", arbitrary_types_allowed=True)

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        ..., description="HTTP method."
    )
    url: str = Field(..., description="Fully resolved URL (no placeholders).")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Outbound headers; includes Authorization when Phantom injects it.",
    )
    body: bytes = Field(b"", description="Request body bytes.")
    timeout_seconds: float | None = Field(
        None,
        description=(
            "Per-request timeout override (§5.2). None = use the client's "
            "configured default. Lets the executor pick a per-route timeout "
            "from ResolvedRoute (e.g. 600 s for an S3 PUT, 30 s for "
            "metadata POSTs)."
        ),
    )


class UpstreamResponse(BaseModel):
    """One inbound HTTP response observed by the executor."""

    model_config = ConfigDict(strict=True, extra="forbid", arbitrary_types_allowed=True)

    status: int = Field(..., ge=0, description="HTTP status code.")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Response headers (case-folded keys as httpx provides them).",
    )
    body: bytes = Field(b"", description="Response body bytes.")


class UpstreamClient(Protocol):
    """The single seam where Phantom hits the real network."""

    async def start(self) -> None:
        """Open the underlying transport (httpx.AsyncClient)."""
        ...

    async def stop(self) -> None:
        """Close the underlying transport."""
        ...

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        """Send ``req`` and return the response."""
        ...
