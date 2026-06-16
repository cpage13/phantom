"""Unit tests for phantom.config.ad_mint.AdMintConfig."""

from __future__ import annotations

import pytest
from phantom.config.ad_mint import AdMintConfig
from phantom.refresh.ad_client_credentials import AdMinter
from pydantic import ValidationError


def _kwargs(**overrides: object) -> dict[str, object]:
    """Build the minimal valid AdMintConfig kwargs."""
    base: dict[str, object] = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "11111111-1111-1111-1111-111111111111",
        "primary_client_secret_env": "PHANTOM_AD_PRIMARY_SECRET",
        "scope": "api://upstream.example.com/.default",
        "endpoint": "upstream.example.com",
        "uid": "phantom-mint",
    }
    base.update(overrides)
    return base


def test_ad_mint_config_required_fields() -> None:
    """Omitting any required field raises ValidationError."""
    for missing in (
        "tenant_id",
        "client_id",
        "primary_client_secret_env",
        "scope",
        "endpoint",
        "uid",
    ):
        kwargs = _kwargs()
        del kwargs[missing]
        with pytest.raises(ValidationError):
            AdMintConfig.model_validate(kwargs)


def test_ad_mint_config_descriptions_present() -> None:
    """Every Pydantic field carries a non-empty description.

    This is the per-field invariant enforced repo-wide; AdMintConfig is
    exercised here so a regression on the AD-mint surface alone surfaces
    immediately.
    """
    for name, field in AdMintConfig.model_fields.items():
        assert field.description, f"AdMintConfig.{name} missing description"
        assert field.description.strip(), f"AdMintConfig.{name} has whitespace-only description"


def test_ad_mint_config_defaults() -> None:
    """Optional fields fall to documented defaults."""
    cfg = AdMintConfig.model_validate(_kwargs())
    assert cfg.secondary_client_secret_env is None
    assert cfg.authority_url == "https://login.microsoftonline.com"
    assert cfg.refresh_seconds_before_expiry == 12
    assert cfg.refresh_jitter_seconds == 0.5
    assert cfg.ad_outage_retry_seconds == [1, 2, 4, 8, 30]


def test_ad_mint_config_extra_forbid() -> None:
    """Unknown fields are rejected (extra='forbid')."""
    with pytest.raises(ValidationError):
        AdMintConfig.model_validate(_kwargs(surprise="nope"))


def test_ad_minter_consumes_typed_config() -> None:
    """AdMinter's constructor binds the typed config.

    No background loop is started; this just verifies the constructor
    signature and the typed-attribute exposure.
    """

    class _StubCache:
        """Minimal TokenCache stub; never actually used."""

        async def get(self, endpoint: str, uid: str) -> None:  # pragma: no cover
            return None

    cfg = AdMintConfig.model_validate(_kwargs())
    minter = AdMinter(config=cfg, token_cache=_StubCache())  # type: ignore[arg-type]
    # Internal attribute exposure: the minter holds the typed config.
    assert minter._config is cfg
    assert minter._config.tenant_id == cfg.tenant_id
