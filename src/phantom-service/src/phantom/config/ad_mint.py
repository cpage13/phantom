"""Typed configuration for autonomous AD client-credentials minting.

When ``InstanceCfg.ad_mint`` is set, Phantom mints AD tokens
proactively via its own app registration and writes them to the
``(endpoint, uid)`` token cache. When ``InstanceCfg.ad_mint`` is
``None``, Phantom waits for the client to push tokens via the
``Authorization`` header on ingress.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Upper bound on ``refresh_seconds_before_expiry``. Entra ID's minimum
# CONFIGURABLE access-token lifetime is 10 minutes, so a refresh margin at or
# under that can never exceed the lifetime of a token minted by any tenant
# policy; past it the margin would be larger than the token's whole life and
# every cycle would compute a negative wait.
_MAX_REFRESH_BEFORE_EXPIRY_SECONDS: int = 600

# Upper bound on ``refresh_jitter_seconds``. Jitter exists to de-correlate
# the mint cycles of sibling instances, which a one-minute spread does; a
# larger value would be shaping the schedule rather than spreading it.
_MAX_REFRESH_JITTER_SECONDS: float = 60.0


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
        300,
        ge=1,
        le=_MAX_REFRESH_BEFORE_EXPIRY_SECONDS,
        description=(
            "Seconds before token expiry to mint a replacement. Default 300 "
            "(5 minutes), matching the refresh window the Azure Identity / MSAL "
            "clients use. The previous 12 s left no allowance for the two clocks "
            "involved: ``expires_on`` is an absolute instant from Entra ID's "
            "clock, and a few seconds of container clock skew plus the mint's own "
            "round trip to the authority could land the replacement AFTER the "
            "cached token had already expired, 401ing every request in between. "
            "Bounded above at the minimum access-token lifetime any tenant policy "
            "can configure, so the margin can never exceed the token's own life."
        ),
    )
    refresh_jitter_seconds: float = Field(
        0.5,
        ge=0.0,
        le=_MAX_REFRESH_JITTER_SECONDS,
        description=(
            "Random jitter SUBTRACTED from the refresh wait to spread the mint "
            "cycles of sibling instances. Bounded above, and further bounded by "
            "``_check_jitter_fits_refresh_window`` to at most "
            "``refresh_seconds_before_expiry``, so jitter can consume the "
            "pre-expiry margin but never more than it."
        ),
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

    @model_validator(mode="after")
    def _check_jitter_fits_refresh_window(self) -> AdMintConfig:
        """Keep the jitter draw inside the pre-expiry refresh margin.

        The minter waits ``lifetime_remaining - refresh_seconds_before_expiry -
        uniform(0, refresh_jitter_seconds)``. The two knobs were bounded
        independently and had no declared relationship, so a jitter larger than
        the margin it is subtracted from could drive the whole term negative
        every cycle. Requiring jitter to fit inside the margin makes the
        relationship a load-time error rather than a runtime symptom.

        Returns:
            ``self`` when the jitter fits inside the refresh margin.

        Raises:
            ValueError: When ``refresh_jitter_seconds`` exceeds
                ``refresh_seconds_before_expiry``.
        """
        if self.refresh_jitter_seconds > self.refresh_seconds_before_expiry:
            raise ValueError(
                f"ad_mint.refresh_jitter_seconds ({self.refresh_jitter_seconds}) must "
                f"not exceed refresh_seconds_before_expiry "
                f"({self.refresh_seconds_before_expiry}): jitter is subtracted from "
                "that margin, so a larger value would schedule the next mint before "
                "the current token was minted"
            )
        return self
