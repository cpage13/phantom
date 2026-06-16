"""Aggressor — body retrievable via admin body endpoint with byte-equality.

A normal-user retrieval scenario:

1. Configure persist-on-receipt (``after_attempts: 0``) + passthrough
   compression so each body lands on disk as exactly the bytes the
   client submitted (no codec round-trip in the way).
2. Send N uploads of varying sizes (1 KiB to 256 KiB).
3. For each, after success, request ``GET /v1/admin/chains/{chain_id}/body``
   and verify the retrieved bytes equal what was sent.

This pins the admin body endpoint's read path. If the endpoint returns
truncated bytes, the wrong bytes, or fails entirely, this catches it. If
the storage encoding doesn't round-trip cleanly under passthrough, this
catches it.

We pin retention (``succeeded_body_seconds: 60``) so the body is still
present on disk when we fetch it after the chain succeeds. (The base
config sets ``succeeded_body_seconds: 0``, which would drop the body
immediately on success and make the read endpoint return 404.)
"""

from __future__ import annotations

import secrets
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Varying body sizes — small, medium, larger. Each size is exercised
# once; 4 chains total. Sizes chosen to cover the typical source payload
# spectrum without hitting the suite's < 60 s budget on slow runners.
BODY_SIZES: list[int] = [1024, 8 * 1024, 64 * 1024, 256 * 1024]

# Per-chain terminal-state wait. Generous to absorb sender poll plus the
# emulator's response.
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
    body: bytes,
) -> None:
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
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_aggressor_body_retrievable_after_succeed(tmp_path: Path) -> None:
    """Submit N varying-size uploads; retrieve each via admin body endpoint with byte-equality."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # Phase 1: all-disk mode replaces ``after_attempts: 0``
                # (persist-on-receipt). Every body lands directly on
                # disk; the admin body endpoint reads from there.
                "body_store": {"mode": "all_disk"},
                # Passthrough so on-disk = on-wire = original bytes.
                # We're auditing the admin read path, not the codec —
                # passthrough takes the codec out of the picture.
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
            # Keep succeeded bodies for 60 s so we can retrieve them
            # after the chain transitions to succeeded.
            "retention": {
                "succeeded_metadata_seconds": 120,
                "succeeded_body_seconds": 60,
            },
        },
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Generate distinct deterministic bodies of varying sizes.
        chain_to_body: dict[UUID, bytes] = {}
        for size in BODY_SIZES:
            cid = uuid4()
            # Use secrets.token_bytes so each body is unique — eliminates
            # accidental hash collision masking a bug.
            body = secrets.token_bytes(size)
            chain_to_body[cid] = body
            await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                chain_id=cid,
                body=body,
            )

        # Wait for all to reach succeeded.
        for cid in chain_to_body:
            detail = await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )
            assert detail.state == "succeeded"

        # Retrieve each body via admin endpoint and verify byte-equality.
        for cid, expected_body in chain_to_body.items():
            chunks: list[bytes] = []
            async for chunk in await pc.fetch_body(cid):
                chunks.append(chunk)
            retrieved = b"".join(chunks)
            assert retrieved == expected_body, (
                f"body byte-equality failed for chain {cid}: "
                f"expected len={len(expected_body)} sha={hash(expected_body):x}, "
                f"got len={len(retrieved)} sha={hash(retrieved):x}"
            )
    finally:
        await stack.tear_down()
