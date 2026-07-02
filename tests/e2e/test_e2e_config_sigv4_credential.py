"""Config-cred boot-path e2e — a re-sign succeeds from a CONFIG credential, no push.

The keystone (:mod:`tests.e2e.test_e2e_sigv4_resign_round_trip`) proves the
RUNTIME provisioning path (admin ``PUT /v1/admin/credentials`` carrying resolved
literals) end to end. The OTHER provisioning route is the boot-time
``sigv4_credentials`` config block, whose entries name ENV VARS holding the secret
literals; at lifespan startup ``_materialize_config_credentials`` resolves those
names from ``os.environ`` and writes the SAME credential-store object the keystone
re-signs from. That config→store materialization was unit-only (see
``phantom.tests.unit.test_config_sigv4_credentials``); this test closes the last
unit-only seam by proving a config-provisioned credential drives a successful
re-sign with NO admin push.

Mechanics that make it self-contained (verified against the harness):

* The stack runs IN-PROCESS, so env vars set here (before ``boot_stack``) are
  visible when the lifespan reads them. Materialization reads ``os.environ`` at
  startup, AFTER the env is set and BEFORE the first request — no timing risk.
* ``config_overrides`` deep-merges generically, and ``sigv4_credentials`` is a
  declared ``Settings`` field, so the block validates and merges.
* Credentials key on the destination HOST ALONE (port-stripped via the executor's
  ``_hostname``), so ``dest_host: "127.0.0.1"`` matches the ephemeral
  ``http://127.0.0.1:PORT`` emulator target without any URL-substitution token.

The SAME AWS-doc example pair the keystone uses is supplied here through the env
vars the config names, so the emulator's ``S3Cfg`` validates the re-signed
signature exactly as in the keystone.
"""

from __future__ import annotations

from uuid import UUID

import httpx
import pytest

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, boot_stack
from .test_e2e_sigv4_resign_round_trip import (
    ACCESS_KEY_ID,
    BUCKET,
    INTAKE_ACCEPTED_STATUS,
    KEY,
    OBJECT_PATH,
    PAYLOAD,
    REGION,
    SECRET_ACCESS_KEY,
    SUCCEEDED_BUDGET_SECONDS,
    _sigv4_overrides,
)

# Distinct env-var NAMES the config materialization resolves (set below). They
# must NOT start with ``PHANTOM_``: ``load_settings`` applies a ``PHANTOM_*`` env
# overlay onto the Settings model (``extra="forbid"``), so a ``PHANTOM_*`` name
# would be swept into Settings and rejected. These are read ONLY by the
# credential materializer's raw ``os.environ.get`` — outside the Settings overlay
# — and are named to avoid any real ``AWS_*`` env on the runner too.
_ACCESS_KEY_ENV = "E2E_CFG_SIGV4_ACCESS_KEY_ID"
_SECRET_KEY_ENV = "E2E_CFG_SIGV4_SECRET_ACCESS_KEY"

# The destination host the credential keys on. Credentials are host-keyed
# (port-stripped), and the emulator binds loopback, so the bare host matches the
# ephemeral ``http://127.0.0.1:PORT`` target — no ``{EMULATOR_URL}`` token needed.
_DEST_HOST = "127.0.0.1"


def _config_cred_overrides() -> dict[str, object]:
    """The keystone ``aws_sigv4`` route + a boot-time ``sigv4_credentials`` block.

    Reuses the keystone's route/target overlay (``aws_sigv4`` on the loopback
    host + the bare ``{EMULATOR_URL}`` target) and adds a single static
    ``sigv4_credentials`` entry naming the env vars set in the test. NO admin
    push happens — the config entry is materialized at boot.
    """
    overrides = _sigv4_overrides()
    overrides["sigv4_credentials"] = [
        {
            "dest_host": _DEST_HOST,
            "kind": "sigv4_static",
            "access_key_id_env": _ACCESS_KEY_ENV,
            "secret_access_key_env": _SECRET_KEY_ENV,
            "region": REGION,
            "service": "s3",
            "session_token_env": None,
        }
    ]
    return overrides


async def _raw_put(stack: E2EStack, *, path: str, body: bytes) -> UUID:
    """Drive a stock ``httpx.put`` through the catch-all, return the chain id."""
    async with httpx.AsyncClient() as client:
        resp = await client.put(f"{stack.phantom_url}/{path}", content=body)
    assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
        f"raw intake expected {INTAKE_ACCEPTED_STATUS} ack, got {resp.status_code}: {resp.text!r}"
    )
    upload_id = resp.headers.get("X-Phantom-Upload-Id")
    assert upload_id, "raw-intake ack must carry X-Phantom-Upload-Id (the minted chain id)"
    return UUID(upload_id)


@pytest.mark.e2e
async def test_config_provisioned_credential_resigns_without_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A config-provisioned SigV4 credential drives a successful re-sign, no admin push.

    Env vars holding the correct AWS example pair are set; a ``sigv4_credentials``
    config entry NAMES them; the stack boots and materializes the credential into
    the store at lifespan startup; a stock ``httpx.put`` hits the ``aws_sigv4``
    catch-all; Phantom re-signs the buffered body from the CONFIG credential; the
    emulator's SigV4 sink validates and stores it. The row reaches ``succeeded``
    and the bytes are byte-identical — proof the config→store path re-signs end
    to end with NO admin push on the path.
    """
    # Set the env vars the config names BEFORE booting (in-process: visible to the
    # lifespan materialization). The correct example pair so the sink validates.
    monkeypatch.setenv(_ACCESS_KEY_ENV, ACCESS_KEY_ID)
    monkeypatch.setenv(_SECRET_KEY_ENV, SECRET_ACCESS_KEY)

    stack = await boot_stack(config_overrides=_config_cred_overrides())
    try:
        # NO _push_credential — the credential comes solely from config.
        chain_id = await _raw_put(stack, path=OBJECT_PATH, body=PAYLOAD)

        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        stored = stack.emulator.s3_object(BUCKET, KEY)
        assert stored is not None, (
            f"no S3 object stored under {OBJECT_PATH!r}; the config-provisioned "
            "credential did not drive a valid re-sign"
        )
        assert stored.body == PAYLOAD, (
            "byte round-trip broke: bytes stored at the SigV4 sink differ from the PUT body "
            f"(sent {len(PAYLOAD)} bytes, stored {len(stored.body)} bytes)"
        )
    finally:
        await stack.tear_down()
