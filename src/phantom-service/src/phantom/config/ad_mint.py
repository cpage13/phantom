"""Typed configuration for autonomous AD client-credentials minting.

When ``InstanceCfg.ad_mint`` is set, Phantom mints AD tokens
proactively via its own app registration and writes them to the
``(endpoint, uid)`` token cache. When ``InstanceCfg.ad_mint`` is
``None``, Phantom waits for the client to push tokens via the
``Authorization`` header on ingress.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class AdMintConfig(BaseModel):
    """Typed AD-mint configuration."""

    model_config = ConfigDict(strict=True, extra="forbid")

    tenant_id: str = Field(
        ...,
        description="Azure AD tenant ID for the mint request.",
    )
    client_id: str = Field(
        ...,
        description="Phantom's app-registration client ID.",
    )
    primary_client_secret_env: str = Field(
        ...,
        description="Name of the environment variable holding the primary client secret.",
    )
    secondary_client_secret_env: str | None = Field(
        None,
        description=(
            "Name of the environment variable holding the secondary "
            "(rotation) client secret. The minter tries primary first, "
            "secondary on failure. None disables rotation."
        ),
    )
    authority_url: str = Field(
        "https://login.microsoftonline.com",
        description="OAuth2 authority URL (override only for sovereign clouds).",
    )
    scope: str = Field(
        ...,
        description="OAuth2 scope to request (e.g., 'api://upstream.example.com/.default').",
    )
    refresh_seconds_before_expiry: int = Field(
        12,
        ge=1,
        description="Seconds before token expiry to mint a replacement.",
    )
    refresh_jitter_seconds: float = Field(
        0.5,
        ge=0.0,
        description="Random jitter added to refresh scheduling to spread load.",
    )
    ad_outage_retry_seconds: list[int] = Field(
        default_factory=lambda: [1, 2, 4, 8, 30],
        description=(
            "Backoff schedule for AD outage retries. List is iterated; last "
            "value repeats. Empty list means fail-fast on the first outage."
        ),
    )
    endpoint: str = Field(
        ...,
        description=(
            "Hostname of the upstream the minted token authenticates to. "
            "Primary axis of the (endpoint, uid) cache key."
        ),
    )
    uid: str = Field(
        ...,
        description=(
            "The credential-identifier value Phantom uses for cache lookup. "
            "Caller-supplied opaque string; the secondary axis of the "
            "(endpoint, uid) cache key."
        ),
    )
