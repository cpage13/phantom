"""HTTPS-over-TLS e2e — the same re-sign keystone, served over a real TLS listener.

The SigV4 re-sign path is ALREADY e2e-proven over PLAINTEXT by the keystone
(:mod:`tests.e2e.test_e2e_sigv4_resign_round_trip`). The ONE untested e2e axis
is HTTPS-over-TLS: the only TLS coverage on ``main`` is the in-process unit test
(:mod:`phantom.tests.unit.test_tls_listener`), which self-builds
``uvicorn.Config`` and does only a healthz GET — it never exercises the
``server.tls`` → ``ssl_kwargs`` harness path nor an upload over TLS.

This test closes that axis: it boots the keystone's ``aws_sigv4`` stack with
``server.tls.enabled: true`` (the harness now resolves a self-signed cert and
serves the ONE listener over HTTPS), pushes the correct credential over the TLS
listener, drives the keystone's stock-PUT upload over HTTPS, and asserts the SAME
byte-identical landing in the emulator SigV4 sink (body + the signed
``x-amz-content-sha256`` header). So ONE test proves HTTPS + re-sign + emulator
sink together, and exercises the admin cred-push against TLS.

The cert is self-signed (auto-gen: ``localhost`` / ``127.0.0.1`` SANs), so every
client that reaches the HTTPS listener does so with verification disabled — the
harness ``PhantomClient`` via an injected ``verify=False`` transport, and the two
stock ``httpx`` clients here via ``verify=False`` directly. This is a test-harness
concern only; the SDK needs no HTTPS field (``phantom_url`` already accepts
``https://``).

The unit TLS test (:mod:`phantom.tests.unit.test_tls_listener`) stays the home
for cert-generation / rotation / XOR-validator coverage; this test owns only the
upload-over-TLS forward path.
"""

from __future__ import annotations

import hashlib
from uuid import UUID

import httpx
import pytest

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, boot_stack
from .test_e2e_sigv4_resign_round_trip import (
    ACCESS_KEY_ID,
    BUCKET,
    CRED_PUSH_STATUS,
    INTAKE_ACCEPTED_STATUS,
    KEY,
    OBJECT_PATH,
    PAYLOAD,
    REGION,
    SECRET_ACCESS_KEY,
    SUCCEEDED_BUDGET_SECONDS,
    _emulator_host,
    _sigv4_overrides,
)

# The TLS overlay merged onto the keystone's aws_sigv4 overrides. The
# config_overrides deep-merge makes ``server.tls.enabled`` VALIDATE for free;
# the harness additions (ssl_* splat, https:// URL, verify=False seam) are what
# make it actually serve + reach over TLS. Auto-gen mode (no cert/key paths)
# mints a self-signed localhost/127.0.0.1 cert under the data dir.
_TLS_OVERRIDE: dict[str, object] = {"server": {"tls": {"enabled": True}}}


def _https_tls_overrides() -> dict[str, object]:
    """The keystone ``aws_sigv4`` overrides plus ``server.tls.enabled: true``."""
    overrides = _sigv4_overrides()
    overrides.update(_TLS_OVERRIDE)
    return overrides


async def _push_credential_tls(stack: E2EStack, *, secret_access_key: str) -> None:
    """Admin-push a static SigV4 credential over the HTTPS (self-signed) listener.

    Mirrors the keystone's ``_push_credential`` but reaches the admin surface
    over TLS with ``verify=False`` (the auto-gen cert is self-signed). Provisions
    the slot the executor's signer arm reads, keyed on the emulator host.
    """
    url = f"{stack.phantom_admin_url}/v1/admin/credentials/{_emulator_host(stack)}"
    async with httpx.AsyncClient(verify=False) as client:
        resp = await client.put(
            url,
            json={
                "kind": "sigv4_static",
                "access_key_id": ACCESS_KEY_ID,
                "secret_access_key": secret_access_key,
                "region": REGION,
                "service": "s3",
                "session_token": None,
            },
        )
    assert resp.status_code == CRED_PUSH_STATUS, (
        f"admin cred-push over TLS expected {CRED_PUSH_STATUS}, "
        f"got {resp.status_code}: {resp.text!r}"
    )


@pytest.mark.e2e
async def test_https_listener_resign_round_trip() -> None:
    """Stock PUT over a real TLS listener re-signs and lands byte-identically.

    The keystone happy path, served over HTTPS: correct creds are pushed over
    TLS; a stock ``httpx.put`` (``verify=False``) hits the catch-all on the
    HTTPS listener; Phantom re-signs the buffered body; the emulator's SigV4 sink
    validates and stores it. The row reaches ``succeeded`` and the stored bytes
    are byte-identical — proof HTTPS + re-sign + emulator sink work together (the
    sink stores ONLY on a faithful signature match). The signed
    ``x-amz-content-sha256`` header is asserted too, exactly as the plaintext
    keystone does.
    """
    stack = await boot_stack(config_overrides=_https_tls_overrides())
    try:
        # Sanity: the listener is genuinely HTTPS now.
        assert stack.phantom_url.startswith("https://"), (
            f"TLS-enabled stack must reach Phantom over https; got {stack.phantom_url!r}"
        )

        await _push_credential_tls(stack, secret_access_key=SECRET_ACCESS_KEY)

        # Stock upload over the TLS listener (verify=False: self-signed cert).
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.put(f"{stack.phantom_url}/{OBJECT_PATH}", content=PAYLOAD)
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"raw intake over TLS expected {INTAKE_ACCEPTED_STATUS} ack, "
            f"got {resp.status_code}: {resp.text!r}"
        )
        upload_id = resp.headers.get("X-Phantom-Upload-Id")
        assert upload_id, "raw-intake ack must carry X-Phantom-Upload-Id (the minted chain id)"

        # The re-signed PUT was accepted (200) by the SigV4 sink; the row reached
        # terminal success. (get_upload rides the harness client, which uses the
        # verify=False transport against the HTTPS listener.)
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            UUID(upload_id),
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        # Byte round-trip off the sink's typed store (the sink stored ONLY because
        # the re-signed signature recomputed and matched).
        stored = stack.emulator.s3_object(BUCKET, KEY)
        assert stored is not None, (
            f"no S3 object stored under {OBJECT_PATH!r}; the re-signed PUT over TLS "
            "was not validated"
        )
        assert stored.body == PAYLOAD, (
            "byte round-trip broke over TLS: bytes stored at the SigV4 sink differ "
            f"from the PUT body (sent {len(PAYLOAD)} bytes, stored {len(stored.body)} bytes)"
        )

        # The signed x-amz-content-sha256 == the real body hash, exactly as the
        # plaintext keystone asserts — proof the re-sign path is unchanged over TLS.
        expected_sha256 = hashlib.sha256(PAYLOAD).hexdigest()
        assert stored.all_headers.get("x-amz-content-sha256") == expected_sha256, (
            "the re-signed PUT over TLS must carry x-amz-content-sha256 == the real "
            f"body hash; got {stored.all_headers.get('x-amz-content-sha256')!r}"
        )
    finally:
        await stack.tear_down()
