"""SigV4 credential-source matrix: STS + profile_ref arms (audit T4 deferred).

The T4 restart test proved the static long-lived pair survives a restart.
These are the remaining arms of the audit's credential-source matrix, three
positives and two product-level negatives, each against the emulator's REAL
SigV4 validator, with the new expected-session-token oracle proving the STS
token MATTERED (the sink rejects a missing or unequal token before it even
recomputes the signature):

* Admin-provisioned STS with restart: a session-token credential pushed over
  admin, held buffered under a global 5xx, survives a stop + seed-free
  restart, and the restarted process signs WITH the token (armed oracle plus
  the stored request's own ``x-amz-security-token`` header).
* Config-env STS at boot: ``sigv4_credentials`` names env vars (including
  ``session_token_env``); the child resolves them at boot and delivers.
* ``profile_ref``: a named profile in ISOLATED temporary AWS files (the child
  env carries only those paths), resolved by botocore at sign time.
* Negative, wrong session token: a credential whose token mismatches the
  armed oracle attempts exactly once, receives the sink 403, parks
  ``auth_expired`` with ``last_error=auth_403``, marks the stored credential
  ``bad``, and stores no object.
* Negative, credentialless profile: a profile that EXISTS in the isolated
  config but has no credential source fails locally (``SigV4SigningError`` ->
  401 park, ``last_error=auth_401``) before any request reaches the emulator.
  A truly nonexistent profile is NOT this control (botocore raises
  ``ProfileNotFound``, the T1 unknown-fault boundary).

Environment hygiene per the audit: every standard AWS credential variable is
scrubbed from the parent (so the child cannot inherit ambient identity), both
AWS file variables point at isolated temporaries, and
``AWS_EC2_METADATA_DISABLED=true`` is always set in the child.

Credential-store reads use the restart test's non-secret metadata reader
(``dest_host``/``kind``/``source``/``status`` only; ``cred_json`` is never
selected).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    write_phantom_config,
)
from tests.e2e.helpers.timing import await_until
from tests.e2e.test_sigv4_credential_persistence_restart import (
    _emulator_host,
    _raw_intake_put,
    _read_credential_metadata,
    _sigv4_config_overrides,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

# The AWS documentation example pair the emulator's SigV4 sink validates against.
_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
_REGION = "us-east-1"

# Distinctive session-token sentinels (greppable, not secret).
_SESSION_TOKEN = "t4-sts-session-token-8c3f1e"
_WRONG_TOKEN = "t4-wrong-session-token-0d9b2a"

# The named profile the profile_ref positive resolves, and the region-only
# credentialless profile the local-failure negative parks on.
_PROFILE_NAME = "t4profile"
_EMPTY_PROFILE_NAME = "t4empty"

_BUCKET = "mybucket"

# Every standard credential-source variable botocore consults; scrubbed from
# the parent so the child sees ONLY each case's explicit environment.
_AWS_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)

# One immediate attempt then 20 s rungs: the held STS row stays non-terminal
# across the stop/restart and retries within one rung once the fault clears.
_HOLD_RETRY_LADDER = {"type": "fixed_intervals", "intervals_seconds": [0] + [20] * 100}

_ATTEMPT_BUDGET_SECONDS = 20.0
_PARK_BUDGET_SECONDS = 20.0
_SUCCEEDED_BUDGET_SECONDS = 60.0
_CRED_PUSH_STATUS = 204


def _scrub_parent_aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove every standard AWS credential variable from the parent env."""
    for name in _AWS_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _isolated_aws_env(
    tmp_path: Path, *, credentials_text: str = "", config_text: str = ""
) -> dict[str, str]:
    """Write isolated AWS files and return the child env pointing ONLY at them."""
    credentials_file = tmp_path / "aws-credentials"
    config_file = tmp_path / "aws-config"
    credentials_file.write_text(credentials_text)
    config_file.write_text(config_text)
    return {
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file),
        "AWS_CONFIG_FILE": str(config_file),
        "AWS_EC2_METADATA_DISABLED": "true",
    }


async def _push_credential(admin_url: str, dest_host: str, body: dict[str, object]) -> None:
    """Provision one credential through the public admin push."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.put(f"{admin_url}/v1/admin/credentials/{dest_host}", json=body)
    assert response.status_code == _CRED_PUSH_STATUS, (
        f"credential push expected {_CRED_PUSH_STATUS}, "
        f"got {response.status_code}: {response.text!r}"
    )


def _static_sts_body(session_token: str | None) -> dict[str, object]:
    """The sigv4_static admin body with an optional STS session token."""
    return {
        "kind": "sigv4_static",
        "access_key_id": _ACCESS_KEY_ID,
        "secret_access_key": _SECRET_ACCESS_KEY,
        "region": _REGION,
        "service": "s3",
        "session_token": session_token,
    }


def _inject_global_5xx(emulator: EmulatorHandle) -> None:
    """Hold every non-control emulator path (the S3 sink included) at 503."""
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; plugin unavailable
            scope=FailureScope.GLOBAL,
            error_rate_5xx=1.0,
        )
    )


async def _await_state(
    client: PhantomClient, chain_id: UUID, state: str, *, budget_seconds: float
) -> None:
    """Poll admin until the chain reaches ``state``."""

    async def _reached() -> bool:
        detail = await client.get_upload(chain_id)
        return str(detail.state) == state

    await await_until(_reached, timeout_seconds=budget_seconds)


async def test_admin_sts_credential_survives_restart_and_signs_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Admin-pushed STS credential: held, restarted seed-free, token-signed.

    Objective: the 5xx hold lands BEFORE submission and provisioning; the
    session-token credential is durable (non-secret metadata only), the env
    carries no AWS seed on either boot, and the restarted process delivers
    with the token PROVEN by the armed oracle plus the stored request's own
    x-amz-security-token header.
    """
    _scrub_parent_aws_env(monkeypatch)
    key = "t4/admin-sts-restart.bin"
    payload = b"phantom-t4-admin-sts\x00\xff\xfe-restart-token-signing"
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    emulator.server.state.expected_session_token = _SESSION_TOKEN
    child_env = _isolated_aws_env(tmp_path)
    port = allocate_port()
    overrides = _sigv4_config_overrides(emulator.url)
    overrides["retry"] = {"default_strategy": _HOLD_RETRY_LADDER}
    # The held chain must SURVIVE the restart: hybrid mode would leave its
    # young body RAM-resident and the recovery sweep would quarantine the row
    # to `corrupted` (the designed crash-safety). All-disk makes the body the
    # durable artifact the case is about.
    overrides["storage"] = {"body_store": {"mode": "all_disk"}}
    config_path = write_phantom_config(
        data_dir=data_dir, bind_port=port, config_overrides=overrides
    )
    dest_host = _emulator_host(emulator)

    first = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    second: PhantomSubprocess | None = None
    try:
        await first.start()
        # Hold FIRST, so nothing can deliver before the replacement dance.
        _inject_global_5xx(emulator)
        chain_id = await _raw_intake_put(first.url, f"{_BUCKET}/{key}", payload)
        await _push_credential(first.url, dest_host, _static_sts_body(_SESSION_TOKEN))

        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None, "STS credential row was not persisted"
        assert (metadata.kind, metadata.source, metadata.status) == (
            "sigv4_static",
            "admin_push",
            "fresh",
        )

        async with PhantomClient(first.url) as client:

            async def _attempted() -> bool:
                detail = await client.get_upload(chain_id)
                return detail.attempts >= 1

            await await_until(_attempted, timeout_seconds=_ATTEMPT_BUDGET_SECONDS)

        first.terminate()
        assert _read_credential_metadata(data_dir, dest_host) is not None, (
            "credential row vanished after the first process stopped"
        )

        # Seed-free restart: same data root, same scrubbed env, NO re-push.
        second = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
        await second.start()
        emulator.clear_failures()

        async with PhantomClient(second.url) as client:
            await _await_state(
                client, chain_id, "succeeded", budget_seconds=_SUCCEEDED_BUDGET_SECONDS
            )

        stored = emulator.server.state.s3_objects.get((_BUCKET, key))
        assert stored is not None, "no object stored: the restarted process never re-signed"
        assert stored.body == payload, "byte round-trip broke across the STS restart"
        assert stored.all_headers.get("x-amz-content-sha256") == (
            hashlib.sha256(payload).hexdigest()
        )
        # The armed oracle already rejected any token-less request; the stored
        # request's own header is the direct artifact.
        assert stored.all_headers.get("x-amz-security-token") == _SESSION_TOKEN
    finally:
        first.terminate()
        if second is not None:
            second.terminate()
        emulator.server.state.expected_session_token = None
        await emulator.stop()


async def test_config_env_sts_credential_signs_at_boot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Config-declared STS credential resolves its env vars at boot and signs.

    Objective: the ``sigv4_credentials`` config arm (env-var indirection,
    ``session_token_env`` included) provisions the store at boot
    (``source=config``) and the delivery carries the signed session token.
    """
    _scrub_parent_aws_env(monkeypatch)
    key = "t4/config-env-sts.bin"
    payload = b"phantom-t4-config-env-sts\x00\xfe-boot-resolution"
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    emulator.server.state.expected_session_token = _SESSION_TOKEN
    dest_host = _emulator_host(emulator)
    child_env = _isolated_aws_env(tmp_path)
    child_env.update(
        {
            "T4_STS_AKID": _ACCESS_KEY_ID,
            "T4_STS_SECRET": _SECRET_ACCESS_KEY,
            "T4_STS_TOKEN": _SESSION_TOKEN,
        }
    )
    port = allocate_port()
    overrides = _sigv4_config_overrides(emulator.url)
    overrides["sigv4_credentials"] = [
        {
            "dest_host": dest_host,
            "kind": "sigv4_static",
            "access_key_id_env": "T4_STS_AKID",
            "secret_access_key_env": "T4_STS_SECRET",
            "session_token_env": "T4_STS_TOKEN",
            "region": _REGION,
            "service": "s3",
        }
    ]
    config_path = write_phantom_config(
        data_dir=data_dir, bind_port=port, config_overrides=overrides
    )

    proc = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    try:
        await proc.start()
        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None, "config credential was not provisioned at boot"
        assert (metadata.kind, metadata.source, metadata.status) == (
            "sigv4_static",
            "config",
            "fresh",
        )

        chain_id = await _raw_intake_put(proc.url, f"{_BUCKET}/{key}", payload)
        async with PhantomClient(proc.url) as client:
            await _await_state(
                client, chain_id, "succeeded", budget_seconds=_SUCCEEDED_BUDGET_SECONDS
            )

        stored = emulator.server.state.s3_objects.get((_BUCKET, key))
        assert stored is not None, "no object stored via the config-env STS credential"
        assert stored.body == payload
        assert stored.all_headers.get("x-amz-security-token") == _SESSION_TOKEN
    finally:
        proc.terminate()
        emulator.server.state.expected_session_token = None
        await emulator.stop()


async def test_profile_ref_resolves_isolated_profile_and_delivers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A named profile in isolated AWS files resolves at sign time and delivers.

    Objective: the ``profile_ref`` public arm end to end. The child env
    carries ONLY the isolated file paths (no ambient fallback possible); the
    long-lived pair signs without a session token.
    """
    _scrub_parent_aws_env(monkeypatch)
    key = "t4/profile-ref.bin"
    payload = b"phantom-t4-profile-ref\x00\xfd-botocore-chain"
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    assert emulator.server.state.expected_session_token is None
    dest_host = _emulator_host(emulator)
    child_env = _isolated_aws_env(
        tmp_path,
        credentials_text=(
            f"[{_PROFILE_NAME}]\n"
            f"aws_access_key_id = {_ACCESS_KEY_ID}\n"
            f"aws_secret_access_key = {_SECRET_ACCESS_KEY}\n"
        ),
        config_text=f"[profile {_PROFILE_NAME}]\nregion = {_REGION}\n",
    )
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_sigv4_config_overrides(emulator.url),
    )

    proc = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    try:
        await proc.start()
        await _push_credential(
            proc.url,
            dest_host,
            {"kind": "profile_ref", "profile": _PROFILE_NAME, "region": _REGION, "service": "s3"},
        )
        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None
        assert (metadata.kind, metadata.source, metadata.status) == (
            "profile_ref",
            "admin_push",
            "fresh",
        )

        chain_id = await _raw_intake_put(proc.url, f"{_BUCKET}/{key}", payload)
        async with PhantomClient(proc.url) as client:
            await _await_state(
                client, chain_id, "succeeded", budget_seconds=_SUCCEEDED_BUDGET_SECONDS
            )

        stored = emulator.server.state.s3_objects.get((_BUCKET, key))
        assert stored is not None, "no object stored via the resolved profile"
        assert stored.body == payload
        assert stored.all_headers.get("x-amz-content-sha256") == (
            hashlib.sha256(payload).hexdigest()
        )
        assert "x-amz-security-token" not in stored.all_headers, (
            "long-lived profile pair must not carry a session token"
        )
    finally:
        proc.terminate()
        await emulator.stop()


async def test_wrong_session_token_parks_auth_expired_with_403(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mismatched session token is rejected BY THE SINK and parks the row.

    Objective: the oracle-armed emulator 403s the token-mismatched request;
    exactly one attempt, ``auth_expired`` with ``last_error=auth_403``, the
    stored credential marked ``bad``, and no accepted object.
    """
    _scrub_parent_aws_env(monkeypatch)
    key = "t4/wrong-token.bin"
    payload = b"phantom-t4-wrong-token-negative"
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    emulator.server.state.expected_session_token = _SESSION_TOKEN
    dest_host = _emulator_host(emulator)
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_sigv4_config_overrides(emulator.url),
    )

    proc = PhantomSubprocess.make(config_path, port, env_overrides=_isolated_aws_env(tmp_path))
    try:
        await proc.start()
        await _push_credential(proc.url, dest_host, _static_sts_body(_WRONG_TOKEN))

        chain_id = await _raw_intake_put(proc.url, f"{_BUCKET}/{key}", payload)
        async with PhantomClient(proc.url) as client:
            await _await_state(
                client, chain_id, "auth_expired", budget_seconds=_PARK_BUDGET_SECONDS
            )
            detail = await client.get_upload(chain_id)
        assert detail.last_error == "auth_403", (
            f"expected the sink 403 park, got last_error={detail.last_error!r}"
        )
        assert detail.attempts == 1, "the 403 park must happen on exactly one attempt"

        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None
        assert metadata.status == "bad", "rejected credential was not marked bad"
        assert (_BUCKET, key) not in emulator.server.state.s3_objects, (
            "the sink stored an object despite the token mismatch"
        )
    finally:
        proc.terminate()
        emulator.server.state.expected_session_token = None
        await emulator.stop()


async def test_credentialless_profile_fails_locally_and_parks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A region-only profile with no credential source fails BEFORE any request.

    Objective: botocore resolves the named profile (it EXISTS, so this is not
    the ProfileNotFound unknown-fault boundary) but yields no credentials;
    Phantom maps ``SigV4SigningError`` locally: one attempt, ``auth_expired``
    with ``last_error=auth_401``, credential marked ``bad``, and NOTHING
    reaches the emulator.
    """
    _scrub_parent_aws_env(monkeypatch)
    key = "t4/credentialless-profile.bin"
    payload = b"phantom-t4-credentialless-profile-negative"
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    dest_host = _emulator_host(emulator)
    child_env = _isolated_aws_env(
        tmp_path,
        config_text=f"[profile {_EMPTY_PROFILE_NAME}]\nregion = {_REGION}\n",
    )
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_sigv4_config_overrides(emulator.url),
    )

    proc = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    try:
        await proc.start()
        await _push_credential(
            proc.url,
            dest_host,
            {
                "kind": "profile_ref",
                "profile": _EMPTY_PROFILE_NAME,
                "region": _REGION,
                "service": "s3",
            },
        )

        chain_id = await _raw_intake_put(proc.url, f"{_BUCKET}/{key}", payload)
        async with PhantomClient(proc.url) as client:
            await _await_state(
                client, chain_id, "auth_expired", budget_seconds=_PARK_BUDGET_SECONDS
            )
            detail = await client.get_upload(chain_id)
        assert detail.last_error == "auth_401", (
            f"expected the local signing park, got last_error={detail.last_error!r}"
        )
        assert detail.attempts == 1, "the local failure must park on exactly one attempt"

        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None
        assert metadata.status == "bad", "unresolvable profile credential was not marked bad"
        assert emulator.server.state.s3_objects == {}, "an object reached the sink"
        assert emulator.upstream_events() == [], (
            "the local signing failure must not produce ANY upstream request"
        )
    finally:
        proc.terminate()
        await emulator.stop()
