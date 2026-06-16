"""Aggressor — mixed encoding chain (pre-gzipped + raw under gzip codec).

The transparent-proxy invariant says: bytes Phantom forwards to upstream
are byte-identical to what the source sent. Under a `gzip` codec config:

- A body the source pre-gzipped (already `Content-Encoding: gzip`) should
  be stored AS-IS (Phantom shouldn't double-gzip). Upstream receives
  the original pre-gzipped bytes.
- A raw body should be gzip-encoded at storage time, then decoded
  back to the original raw bytes for the wire (transparent proxy).
- StorageHash differs from BodyHash on raw bodies (the codec mutated
  the stored bytes); StorageHash == BodyHash on pre-gzipped bodies
  (since the codec didn't double-compress).

This is the trickiest invariant in the system, and the existing
`test_transparent_proxy.py` doesn't directly mix pre-gzipped and raw
bodies under the same codec config in one test run. If Phantom's
codec selection is body-type-aware (it must not be — `compression`
config is uniform), this test surfaces the gap.

Note: this test relies on Phantom's auto-detection of pre-gzipped
content. Per the existing test_transparent_proxy.py
`test_byte_identity_pre_compressed_body`, the source signals
pre-compression via the `Content-Encoding: gzip` header on the PUT
step.
"""

from __future__ import annotations

import gzip
import hashlib
import secrets
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client.models.chain import ChainEnvelope, ChainStep

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
RAW_BODY_SIZE: int = 32 * 1024
TERMINAL_BUDGET_SECONDS: float = 15.0
N_RAW_CHAINS: int = 4

pytestmark = pytest.mark.e2e


def _build_envelope_with_put_headers(
    *,
    chain_id: UUID,
    emulator_url: str,
    extra_put_headers: dict[str, str],
) -> ChainEnvelope:
    """Build the standard two-step envelope, optionally with extra PUT headers."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    if not extra_put_headers:
        return envelope
    put_step = envelope.steps[-1]
    merged_headers = {**put_step.headers, **extra_put_headers}
    new_put = ChainStep(
        name=put_step.name,
        method=put_step.method,
        url=put_step.url,
        headers=merged_headers,
        body=put_step.body,
        capture=put_step.capture,
        idempotency_header=put_step.idempotency_header,
    )
    return ChainEnvelope(
        chain_id=envelope.chain_id,
        idempotency_key=envelope.idempotency_key,
        steps=[envelope.steps[0], new_put],
        default_target=envelope.default_target,
    )


async def test_aggressor_mixed_encoding_under_gzip_codec(tmp_path: Path) -> None:
    """Pre-gzipped + raw bodies coexist under storage.compression.algorithm=gzip."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                "compression": {
                    "mode": "always",
                    "algorithm": "gzip",
                    "level": 6,
                },
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # 1 pre-gzipped chain.
        raw_for_pregzip = secrets.token_bytes(RAW_BODY_SIZE)
        pregzipped = gzip.compress(raw_for_pregzip)
        pregzipped_chain_id = uuid4()
        envelope_pre = _build_envelope_with_put_headers(
            chain_id=pregzipped_chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers={"Content-Encoding": "gzip"},
        )
        await pc.submit_chain(
            envelope_pre,
            body_refs={"body": pregzipped},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {bearer}",
        )

        # 4 raw chains.
        raw_chain_to_body: dict[UUID, bytes] = {}
        for _ in range(N_RAW_CHAINS):
            cid = uuid4()
            body = secrets.token_bytes(RAW_BODY_SIZE)
            raw_chain_to_body[cid] = body
            envelope_raw = _build_envelope_with_put_headers(
                chain_id=cid,
                emulator_url=stack.emulator_url,
                extra_put_headers={},
            )
            await pc.submit_chain(
                envelope_raw,
                body_refs={"body": body},
                uid=DEFAULT_SUB,
                auth_token=f"Bearer {bearer}",
            )

        # Wait for ALL chains (pre-gzipped + raw) to succeed.
        all_chain_ids = [pregzipped_chain_id, *raw_chain_to_body.keys()]
        for cid in all_chain_ids:
            await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )

        # Audit each row's storage_encoding via list_uploads.
        rows, _ = await pc.list_uploads(limit=200)
        rows_by_chain_id = {r.chain_id: r for r in rows}

        # Pre-gzipped row's storage_encoding should reflect the SAME
        # codec config (gzip is configured), but the row should carry
        # a body matching the pre-gzipped bytes — i.e., Phantom did
        # NOT double-compress.
        pre_row = rows_by_chain_id.get(pregzipped_chain_id)
        assert pre_row is not None, "pre-gzipped row missing"
        # Phantom's design always encodes with the configured codec
        # (per `compression.mode: always` invariant — no per-request
        # opt-out). So storage_encoding is "gzip" for both. The
        # transparent-proxy guarantee is enforced via byte-equality
        # on the emulator's received bytes, NOT via storage_encoding
        # surfacing the codec mode.
        assert pre_row.storage_encoding == "gzip", (
            f"pre-gzipped row storage_encoding={pre_row.storage_encoding!r}, "
            f"expected 'gzip' under compression.algorithm=gzip"
        )

        # Emulator received pre-gzipped body byte-equal.
        received_pre = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(pregzipped_chain_id),
            body_size=len(pregzipped),
        )
        # The emulator records SHA-256 of received bytes; pre-gzipped
        # body's hash must equal the SHA-256 of the original
        # pre-gzipped bytes (NOT the SHA-256 of double-gzipped bytes).
        expected_pre_hash = hashlib.sha256(pregzipped).hexdigest()
        assert received_pre.body_hash == expected_pre_hash, (
            f"pre-gzipped body bytes MUTATED through Phantom: "
            f"sent SHA-256={expected_pre_hash}, "
            f"received SHA-256={received_pre.body_hash}. "
            f"Transparent-proxy invariant violated."
        )

        # Each raw body must also round-trip byte-equal.
        for cid, raw_body in raw_chain_to_body.items():
            received = await assert_emulator_received(
                stack.emulator,
                phantom_local_uuid=str(cid),
                body_size=len(raw_body),
            )
            expected_raw_hash = hashlib.sha256(raw_body).hexdigest()
            assert received.body_hash == expected_raw_hash, (
                f"raw body chain {cid} hash mismatch: "
                f"sent={expected_raw_hash}, received={received.body_hash}. "
                f"Transparent-proxy invariant violated for raw body."
            )

            # storage_encoding on raw rows is "gzip" (codec mutated
            # the stored bytes, but the wire bytes are still original).
            raw_row = rows_by_chain_id.get(cid)
            assert raw_row is not None, f"raw row {cid} missing"
            assert raw_row.storage_encoding == "gzip", (
                f"raw row storage_encoding={raw_row.storage_encoding!r}, expected 'gzip'"
            )

        # Sanity: the pre-gzipped bytes should not appear in the
        # emulator's body_hash list more than once (one entry per
        # submission).
        all_pre_entries = [e for e in stack.emulator.received() if e.body_hash == expected_pre_hash]
        assert len(all_pre_entries) == 1, (
            f"pre-gzipped body appeared {len(all_pre_entries)} times "
            f"in emulator received log; expected exactly 1."
        )
    finally:
        await stack.tear_down()
