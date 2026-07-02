"""Unit tests for the config ``sigv4_credentials`` acquisition route (TASK 2.4b).

The boot-time analogue of the runtime admin credential push: a deployment may
declare destination credentials in config, naming the env var that holds each
secret access key (never the secret literal — GLOBAL §1.2(a) B1 / ADR-004). At
boot (``_build_instance_context``) the named env vars are resolved to literals
and materialized into the host-keyed credential store under ``source="config"``.

These tests prove the acceptance criteria:

* a config declaration whose ``secret_access_key_env`` names an env var (set via
  ``monkeypatch``) resolves AT BOOT — the store holds a ``SigV4StaticCreds`` with
  the RESOLVED literal value (not the env-var name), under the normalized host;
* a config declaration whose named env var is missing is a CLEAR fail-fast boot
  error (:class:`ConfigCredentialError`), never a silent skip;
* the default (empty ``sigv4_credentials``) is a no-op: the store stays empty;
* host normalization matches the admin push — a mixed-case ``dest_host`` is
  materialized under the same ``_hostname``-normalized key the executor looks up.
"""

from __future__ import annotations

import pytest
from phantom.app import ConfigCredentialError, _build_instance_context
from phantom.config.settings import (
    InstanceCfg,
    RouteCfg,
    Settings,
    SigV4CredentialCfg,
    StorageCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.models.credential import HostCredKey, SigV4StaticCreds
from phantom.observability.metrics import MetricsRegistry
from pydantic import ValidationError

from .conftest import make_snapshot, track_instance, track_started

# A recognizable secret LITERAL the env var holds. The store must end up holding
# THIS, not the env-var NAME — that is the whole point of boot-time resolution.
_SECRET_LITERAL = "wJalrXUtnFEMI-RESOLVED-LITERAL-EXAMPLEKEY"
_ACCESS_KEY_LITERAL = "AKIARESOLVEDEXAMPLE"
_SECRET_ENV_NAME = "PHANTOM_TEST_SIGV4_SECRET"
_ACCESS_ENV_NAME = "PHANTOM_TEST_SIGV4_ACCESS"

# The destination host, declared MIXED CASE in config so the normalization
# (config-key == executor-lookup-key) is proven.
_DEST_HOST_MIXED = "S3.US-East-1.AmazonAWS.CoM"
_DEST_HOST_NORMALIZED = "s3.us-east-1.amazonaws.com"


def _instance_cfg() -> InstanceCfg:
    """A minimal one-route instance (the route detail is irrelevant here)."""
    return InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[
            RouteCfg(
                name="files",
                hosts=["files.example.com"],
                auth_mode="aws_sigv4",
            )
        ],
    )


def _settings(tmp_path_str: str, sigv4_credentials: list[SigV4CredentialCfg]) -> Settings:
    """Build a one-instance Settings rooted at ``tmp_path_str``."""
    return Settings(
        storage=StorageCfg(data_dir=tmp_path_str),
        instances=[_instance_cfg()],
        sigv4_credentials=sigv4_credentials,
    )


def _holder() -> SettingsHolder:
    """A SettingsHolder pre-populated with the instance's snapshot.

    ``_build_instance_context`` stores the ``current_settings`` thunk but does
    not call it during the build; the holder is populated anyway so the built
    context is well-formed.
    """
    holder = SettingsHolder()
    holder._snapshots = {"primary": make_snapshot()}
    return holder


async def _build(settings: Settings) -> InstanceContext:
    """Run ``_build_instance_context`` and assert a healthy (non-degraded) boot.

    Tracks the built context's stores AND its ``signer_creds`` store for the
    per-test aiosqlite-leak teardown (``track_instance`` covers the upload /
    token stores; the credential store is registered explicitly).
    """
    outcome = await _build_instance_context(
        settings, settings.instances[0], _holder(), MetricsRegistry()
    )
    assert isinstance(outcome, InstanceContext), f"degraded boot: {outcome!r}"
    track_instance(outcome)
    assert outcome.signer_creds is not None
    track_started(outcome.signer_creds)
    return outcome


@pytest.mark.asyncio
async def test_config_credential_resolves_env_name_at_boot(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A config entry's named env var resolves at boot; the store holds the literal.

    The config carries the env-var NAME (``secret_access_key_env``); after boot
    the store holds the RESOLVED secret literal under the normalized host. This
    is the B1 invariant in action — names on the config route, literals in the
    store.
    """
    monkeypatch.setenv(_SECRET_ENV_NAME, _SECRET_LITERAL)
    monkeypatch.setenv(_ACCESS_ENV_NAME, _ACCESS_KEY_LITERAL)
    settings = _settings(
        str(tmp_path),
        [
            SigV4CredentialCfg(
                dest_host=_DEST_HOST_MIXED,
                kind="sigv4_static",
                access_key_id_env=_ACCESS_ENV_NAME,
                secret_access_key_env=_SECRET_ENV_NAME,
                region="us-east-1",
                service="s3",
            )
        ],
    )

    ctx = await _build(settings)

    assert ctx.signer_creds is not None
    # Normalization: the mixed-case dest_host is keyed under the same form the
    # executor's forward-time _hostname lookup produces.
    row = await ctx.signer_creds.get(HostCredKey(_DEST_HOST_NORMALIZED))
    assert row is not None, "config credential was not materialized into the store"
    assert row.source == "config"
    assert row.status == "fresh"
    cred = row.credential
    assert isinstance(cred, SigV4StaticCreds)
    # The RESOLVED literals are stored — NOT the env-var names.
    assert cred.secret_access_key == _SECRET_LITERAL
    assert cred.secret_access_key != _SECRET_ENV_NAME
    assert cred.access_key_id == _ACCESS_KEY_LITERAL
    assert cred.region == "us-east-1"
    assert cred.session_token is None


@pytest.mark.asyncio
async def test_config_credential_missing_env_var_is_fail_fast(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A named env var that is absent at boot raises a clear ConfigCredentialError.

    Fail-fast, never a silent skip: a config-declared credential whose backing
    secret env var is missing is an operator misconfiguration that must crash
    boot loudly (mirrors ad_mint's posture that its secret env must exist).
    """
    monkeypatch.delenv(_SECRET_ENV_NAME, raising=False)
    monkeypatch.setenv(_ACCESS_ENV_NAME, _ACCESS_KEY_LITERAL)
    settings = _settings(
        str(tmp_path),
        [
            SigV4CredentialCfg(
                dest_host=_DEST_HOST_NORMALIZED,
                kind="sigv4_static",
                access_key_id_env=_ACCESS_ENV_NAME,
                secret_access_key_env=_SECRET_ENV_NAME,
                region="us-east-1",
                service="s3",
            )
        ],
    )

    with pytest.raises(ConfigCredentialError) as excinfo:
        await _build_instance_context(settings, settings.instances[0], _holder(), MetricsRegistry())
    # The error names the offending env var and field so the operator can fix it.
    message = str(excinfo.value)
    assert _SECRET_ENV_NAME in message
    assert "secret_access_key_env" in message
    # Fail-fast cleanup closed the already-open stores: no aiosqlite worker
    # thread leaks (the autouse tripwire in conftest would otherwise fire).


@pytest.mark.asyncio
async def test_config_credential_empty_default_is_noop(tmp_path) -> None:
    """The default (empty ``sigv4_credentials``) materializes nothing.

    A bearer-only deployment declares no credentials; boot must not write
    anything to the store, and existing configs (which never set the field) are
    unaffected.
    """
    settings = _settings(str(tmp_path), [])
    assert settings.sigv4_credentials == []

    ctx = await _build(settings)

    assert ctx.signer_creds is not None
    row = await ctx.signer_creds.get(HostCredKey(_DEST_HOST_NORMALIZED))
    assert row is None


def test_sigv4_static_entry_requires_key_envs_and_region() -> None:
    """A ``sigv4_static`` arm missing a required env-name/region fails validation.

    The per-arm validator catches the shape error at settings-load (a loud
    ``ValidationError``) so the boot materializer's static-arm assumptions stay
    total.
    """
    with pytest.raises(ValidationError):
        SigV4CredentialCfg(
            dest_host=_DEST_HOST_NORMALIZED,
            kind="sigv4_static",
            access_key_id_env=_ACCESS_ENV_NAME,
            service="s3",
            # secret_access_key_env + region omitted.
        )


def test_profile_ref_entry_rejects_static_fields() -> None:
    """A ``profile_ref`` arm carrying a static-arm field fails validation."""
    with pytest.raises(ValidationError):
        SigV4CredentialCfg(
            dest_host=_DEST_HOST_NORMALIZED,
            kind="profile_ref",
            profile="prod",
            service="s3",
            region="us-east-1",  # static-arm field on a profile_ref entry.
        )


def test_profile_ref_default_chain_entry_is_valid() -> None:
    """A bare ``profile_ref`` (default chain) entry validates with no env names."""
    cfg = SigV4CredentialCfg(dest_host=_DEST_HOST_NORMALIZED, kind="profile_ref", service="s3")
    assert cfg.profile is None
    assert cfg.access_key_id_env is None


def test_config_arm_missing_service_is_value_error() -> None:
    """A config arm that omits ``service`` fails validation (``missing``).

    The boot-time provision fail-loud, symmetric with the admin push body: a
    config-declared credential of unknown service scope is rejected at
    settings-load.
    """
    with pytest.raises(ValidationError) as excinfo:
        SigV4CredentialCfg(
            dest_host=_DEST_HOST_NORMALIZED,
            kind="sigv4_static",
            access_key_id_env=_ACCESS_ENV_NAME,
            secret_access_key_env=_SECRET_ENV_NAME,
            region="us-east-1",
            # service omitted.
        )
    types = {e["type"] for e in excinfo.value.errors() if e["loc"] == ("service",)}
    assert types == {"missing"}, excinfo.value.errors()


def test_config_arm_unknown_service_is_value_error() -> None:
    """A config arm naming an unknown service fails validation (``value_error``).

    ``"dynamodb"`` is the deliberate wire-coercion boundary; the
    before-validator rejects it cleanly.
    """
    with pytest.raises(ValidationError) as excinfo:
        SigV4CredentialCfg(
            dest_host=_DEST_HOST_NORMALIZED,
            kind="sigv4_static",
            access_key_id_env=_ACCESS_ENV_NAME,
            secret_access_key_env=_SECRET_ENV_NAME,
            region="us-east-1",
            service="dynamodb",
        )
    types = {e["type"] for e in excinfo.value.errors() if e["loc"] == ("service",)}
    assert types == {"value_error"}, excinfo.value.errors()
