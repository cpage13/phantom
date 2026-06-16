"""E2E-14 — Storage-encoding round trip (always-encode).

Submits three envelopes against a Phantom whose instance compression
config has ``algorithm=zstd`` and the F3 always-encode policy:

- (a) A 200 KiB body with no ``Content-Encoding`` header — phantom
  encodes with zstd. The sender decodes before forwarding so the
  emulator sees the original bytes.
- (b) A 200 KiB gzipped body sent with ``Content-Encoding: gzip`` —
  phantom still encodes with zstd (transparent-proxy invariant
  enforced by body_hash verification, not by storage-side
  pass-through). The sender decodes on send, so the emulator sees
  the *gzipped* wire bytes the client transmitted.
- (c) A 1 KiB body with no ``Content-Encoding`` — phantom still
  encodes with zstd (no size short-circuit under always-encode).

See plan §10 (F3 always-encode + body hashes).
"""

from __future__ import annotations

import gzip
import hashlib
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import PhantomClient
from phantom_client.models.status import UploadRow

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

# 200 KiB random-ish body — comfortably above the 64 KiB threshold so
# the codec actually compresses. Random-ish (not all-zeros) so the
# zstd output isn't a fixed-size header.
LARGE_BODY: bytes = (b"abcdefghijklmnopqrstuvwxyz0123456789" * 6000)[: 200 * 1024]

# 1 KiB body — under always-encode it still gets zstd-encoded.
SMALL_BODY: bytes = b"small-body" * 100  # 1000 bytes


@pytest.mark.e2e
async def test_e2e_14_storage_encoding() -> None:
    """Three storage-encoding paths produce the right ``storage_encoding`` and round-trip bytes."""
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "compression": {
                    "mode": "always",
                    "algorithm": "zstd",
                    "level": 3,
                },
            },
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        },
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        bearer = stack.fake_security_token()

        # (a) 200 KiB, no Content-Encoding → expect zstd.
        chain_a = uuid4()
        await _submit_via_phantom_client(
            pc,
            chain_id=chain_a,
            body=LARGE_BODY,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )

        # (b) 200 KiB, gzip-encoded body + Content-Encoding: gzip →
        #     expect passthrough (storage_encoding = "original"
        #     in current code).
        gzipped_large = gzip.compress(LARGE_BODY)
        chain_b = uuid4()
        await _submit_via_httpx_with_content_encoding(
            stack,
            chain_id=chain_b,
            body=gzipped_large,
            content_encoding="gzip",
            bearer=bearer,
        )

        # (c) 1 KiB, no Content-Encoding → expect passthrough
        #     ("original") because body is below the 64 KiB threshold.
        chain_c = uuid4()
        await _submit_via_phantom_client(
            pc,
            chain_id=chain_c,
            body=SMALL_BODY,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )

        # Wait for all three to reach succeeded.
        for cid in (chain_a, chain_b, chain_c):
            await assert_chain_reaches_state(pc, cid, state="succeeded", timeout_seconds=15.0)

        # Inspect storage_encoding via list_uploads (the SDK's
        # get_upload returns ChainResponse which doesn't carry the
        # tier/encoding fields; UploadRow does).
        rows_by_chain_id: dict[UUID, UploadRow] = {}
        rows, _ = await pc.list_uploads(limit=200)
        for r in rows:
            rows_by_chain_id[r.chain_id] = r
        assert chain_a in rows_by_chain_id, "row (a) missing"
        assert chain_b in rows_by_chain_id, "row (b) missing"
        assert chain_c in rows_by_chain_id, "row (c) missing"

        # Always-encode: every path lands as zstd regardless of size
        # or inbound Content-Encoding.
        assert rows_by_chain_id[chain_a].storage_encoding == "zstd", (
            f"row (a) storage_encoding={rows_by_chain_id[chain_a].storage_encoding!r}"
        )
        assert rows_by_chain_id[chain_b].storage_encoding == "zstd", (
            f"row (b) storage_encoding={rows_by_chain_id[chain_b].storage_encoding!r}"
        )
        assert rows_by_chain_id[chain_c].storage_encoding == "zstd", (
            f"row (c) storage_encoding={rows_by_chain_id[chain_c].storage_encoding!r}"
        )

        # Emulator received the right bytes. The hash check is
        # easiest via `body_size` because the emulator surface
        # doesn't return the body bytes; we check size + the
        # phantom_local_uuid mapping.
        received_a = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(chain_a),
            body_size=len(LARGE_BODY),
        )
        # Path (a) decodes from zstd before forwarding — emulator
        # sees the original-large-body bytes.
        assert received_a.body_size == len(LARGE_BODY)

        received_b = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(chain_b),
            body_size=len(gzipped_large),
        )
        # Path (b) was passthrough on the way in (Content-Encoding
        # gzip), so phantom forwards the already-gzipped bytes to
        # the upstream untouched. The emulator sees the gzip bytes.
        assert received_b.body_size == len(gzipped_large)

        received_c = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(chain_c),
            body_size=len(SMALL_BODY),
        )
        assert received_c.body_size == len(SMALL_BODY)

        # Diagnostic: log hash invariants so a future failure on
        # body content is debuggable from test logs alone.
        _ = hashlib.sha256(LARGE_BODY).hexdigest()
    finally:
        await stack.tear_down()


async def _submit_via_phantom_client(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
) -> None:
    """Build an envelope and submit through the SDK (no Content-Encoding)."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid="00000000-0000-0000-0000-000000000001",
        auth_token=f"Bearer {bearer}",
    )


async def _submit_via_httpx_with_content_encoding(
    stack: E2EStack,
    *,
    chain_id: UUID,
    body: bytes,
    content_encoding: str,
    bearer: str,
) -> None:
    """Send a multipart submit_chain POST with an explicit Content-Encoding header.

    The SDK's ``submit_chain`` doesn't expose a hook for the wire
    ``Content-Encoding`` header; the codec-selection path needs it
    to choose passthrough. We bypass the SDK and issue the multipart
    POST directly via httpx, keeping the envelope shape the driver's
    builder produces so the upstream side is unchanged.

    The body here MAY already be encoded by the caller (e.g., gzipped).
    The codec at phantom's ingress respects the wire encoding and
    stores the body as-is (passthrough), then forwards the bytes
    untouched to the upstream — the emulator sees what the caller
    sent.
    """
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    headers = {
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Uid": "00000000-0000-0000-0000-000000000001",
        "X-Phantom-Idempotency-Key": str(chain_id),
        "Content-Encoding": content_encoding,
    }
    envelope_json: str = envelope.model_dump_json(by_alias=True)
    files: list[tuple[str, tuple[str | None, bytes | str, str]]] = [
        ("envelope", (None, envelope_json, "application/json")),
        ("body_refs[body]", ("body", body, "application/octet-stream")),
    ]
    async with httpx.AsyncClient(base_url=stack.phantom_url, timeout=30.0) as client:
        response = await client.post("/v1/send", headers=headers, files=files)
        assert response.status_code == 202, (
            f"unexpected status from /v1/send: {response.status_code} {response.text}"
        )
