"""Response models shared by the emulator's two control surfaces.

``EmulatorState`` owns the projections behind ``GET /control/received`` and
``GET /control/status`` (U3), and both the HTTP router and the in-process
``Server`` oracle read them. The models therefore cannot live in the router:
``routers/control.py`` imports ``state.py``, so ``state.py`` returning a
router-defined type would close an import cycle. They live here, in a leaf
module both surfaces import, which is what lets the two tiers read ONE
projection instead of two copies of it.
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.failure.injection import FailurePolicy

logger = logging.getLogger(__name__)


class ReceivedEntry(BaseModel):
    """One row of the ``/control/received`` log."""

    model_config = ConfigDict(extra="forbid")

    upload_token: str = Field(..., description="The opaque token from the upload URL.")
    file_id: UUID = Field(..., description="The emulator-minted FileInformation.id.")
    metadata_kvs: dict[str, str] = Field(
        ...,
        description=(
            "The metadata.keyValueStore the metadata POST carried (preserves phantom_local_uuid)."
        ),
    )
    x_amz_meta_headers: dict[str, str] = Field(
        ..., description="The x-amz-meta-* headers the PUT carried."
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Every inbound HTTP header on the PUT, lowercased keys with "
            "original values. Captures the full request envelope so "
            "transparent-proxy tests can audit byte-equality of "
            "``Authorization``, absence of ``X-Phantom-*``, preservation "
            "of ``User-Agent`` and custom producer headers. Multi-value "
            'headers join on ``", "`` per Starlette\'s header-dict '
            "semantics. Authorization values are recorded verbatim — "
            "tests opt-in to assert against them."
        ),
    )
    body_size: int = Field(..., ge=0, description="Bytes accepted on the PUT.")
    body_hash: str = Field(
        ...,
        description=(
            "SHA-256 hex of the received body bytes — used by "
            "transparent-proxy E2E tests to assert byte-identity against "
            "the agent's pre-submit hash."
        ),
    )
    content_encoding: str | None = Field(
        None,
        description=(
            "Value of the PUT's ``Content-Encoding`` header (or ``None`` "
            "if the header was unset). Lets transparent-proxy tests "
            "assert header preservation alongside byte-identity."
        ),
    )
    accepted_at: datetime = Field(..., description="Server-side acceptance time.")
    idempotency_key: str | None = Field(
        None,
        description="Idempotency-Key from the create call (if any).",
    )


class ReceivedResponse(BaseModel):
    """Envelope for the received-log response."""

    model_config = ConfigDict(extra="forbid")

    received: list[ReceivedEntry] = Field(..., description="Accepted bodies, oldest first.")


class ControlStatusResponse(BaseModel):
    """``/control/status`` payload."""

    model_config = ConfigDict(extra="forbid")

    uptime_seconds: int = Field(
        ...,
        ge=0,
        description="Seconds since this emulator process started.",
    )
    issued_tokens_count: int = Field(
        ...,
        ge=0,
        description="Number of JWTs issued by ``POST /oauth/token`` since boot.",
    )
    accepted_bodies_count: int = Field(
        ...,
        ge=0,
        description="Number of upload bodies the emulator has accepted on the PUT path.",
    )
    pending_uploads_count: int = Field(
        ...,
        ge=0,
        description="Number of presigned URLs minted but not yet PUT-completed.",
    )
    global_paused: bool = Field(
        ...,
        description="True when ``POST /control/pause`` has been called and resume hasn't fired.",
    )
    policies: list[FailurePolicy] = Field(
        ...,
        description="Currently-installed policies (per ``POST /control/inject-failure``).",
    )
    auth_mode_default: AuthMode = Field(
        ...,
        description="Default auth mode applied when no per-path override matches.",
    )
    auth_mode_overrides: dict[str, AuthMode] = Field(
        ...,
        description="Path-prefix overrides of the default auth mode.",
    )
