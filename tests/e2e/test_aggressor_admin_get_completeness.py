"""Aggressor — admin GET surfaces full metadata + headers + body.

The user's requirement: "if it is ever persisted, all that metadata is
also retrievable with the files and the body."

For each persisted chain, ``GET /v1/admin/chains/{chain_id}`` should
return enough information to reconstruct the upstream request — the
metadata key-value store, the request headers, the captured values.
The body is then retrievable separately via
``GET /v1/admin/chains/{chain_id}/body``.

Round-2 contract (post-defender): ``ChainAdminDetail`` carries:

- chain_id, state, tier, committed, last_step_completed, captured,
  attempts, last_error (round-1 baseline);
- ``metadata`` (round-2): the create-file step's
  ``metadata.key_value_store``, byte-equal to what was submitted;
- ``steps`` (round-2): per-step ``name``, ``method``, ``url``,
  ``headers``, ``has_body`` projection from the persisted envelope.

These tests pin both: (a) the metadata round-trips on admin GET, and
(b) the per-step request envelope is reconstructable from the admin
surface. Together they satisfy the user's "retrievable" promise.
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
TERMINAL_BUDGET_SECONDS: float = 15.0
NON_TRIVIAL_BODY_BYTES: int = 64 * 1024  # 64 KiB — distinguishable

# Representative metadata KVS carrying a spread of generic made-up
# key/value pairs - proves Phantom's opaque-KVS round-trip without any
# domain-specific schema.
PROD_METADATA: dict[str, str] = {
    "ref_id": "1d2e3f4a-5b6c-7d8e-9f01-234567890abc",
    "label": "alpha",
    "uploader_id": "12345",
    # Representative extras.
    "parcel_id": "PARCEL-0001-NA-07",
    "group_b": "PVC-BLACK",
    "group_a": "S-12345-A",
    "order_number": "ORD-2026-05-15-NA-001",
    "line_number": "1",
    "label_name": "sequential_batch_v2",
}

# Custom source-side headers (producer-injected for tracing).
CUSTOM_HEADERS: dict[str, str] = {
    "x-amz-meta-tracker": "source-supplied-tracker-abc-123",
    "x-amz-meta-correlation-id": "corr-2026-05-15-aabb-001",
}

pytestmark = pytest.mark.e2e


async def _submit_chain(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
    body: bytes,
    metadata: dict[str, str],
) -> None:
    """Submit one chain with a full generic metadata KVS."""
    request = build_create_file_request(
        file_name=f"e2e_{chain_id.hex[:12]}",
        uploader_id=metadata["uploader_id"],
        extra_metadata={k: v for k, v in metadata.items() if k != "uploader_id"},
    )
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


async def test_aggressor_admin_get_returns_full_metadata(tmp_path: Path) -> None:
    """Persisted chain's admin GET surfaces metadata + body byte-equal."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # All-disk mode — every row lands on disk directly at
                # admission (no RAM tier). Replaces the pre-Phase-1
                # ``default_tier: persisted`` + ``after_attempts: 0``
                # pair per plan § 0.8 config-knob migration audit.
                "body_store": {"mode": "all_disk"},
                # Passthrough — the on-disk body file is literally the
                # bytes the producer submitted.
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
            # Retain succeeded bodies + metadata long enough to fetch.
            "retention": {
                "succeeded_metadata_seconds": 300,
                "succeeded_body_seconds": 300,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        body = secrets.token_bytes(NON_TRIVIAL_BODY_BYTES)
        await _submit_chain(
            pc,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            chain_id=chain_id,
            body=body,
            metadata=PROD_METADATA,
        )

        # Wait for chain to succeed.
        detail = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"
        assert detail.body_location == "file", (
            f"under body_store.mode='all_disk', row should be on disk; "
            f"got body_location={detail.body_location!r}"
        )

        # Capture surfaces should include the file information from
        # the create-file step's response. The captured values dict
        # is the SDK's `captured` field.
        captured_step_names = {cs.step_name for cs in detail.captured}
        # The driver's envelope names the create step "create_file" and
        # the PUT step "put_s3" (per the build_in_memory_upload_envelope
        # source).
        assert captured_step_names, (
            "ChainAdminDetail.captured is empty — captured values "
            "from the create_file step did not surface"
        )

        # Audit the metadata round-trip via the emulator's received
        # log — this is the structural assertion that ALL metadata
        # keys reached the upstream byte-equal.
        received_entries = stack.emulator.received()
        matching = [
            e for e in received_entries if e.metadata_kvs.get("phantom_local_uuid") == str(chain_id)
        ]
        assert matching, f"emulator did not record chain {chain_id}"
        recv = matching[0]
        for prod_key, expected_value in PROD_METADATA.items():
            recv_value = recv.metadata_kvs.get(prod_key)
            assert recv_value == expected_value, (
                f"metadata key {prod_key!r} did not surface on emulator: "
                f"expected={expected_value!r}, received={recv_value!r}"
            )

        # Body must be retrievable byte-equal.
        chunks: list[bytes] = []
        async for chunk in await pc.fetch_body(chain_id):
            chunks.append(chunk)
        retrieved = b"".join(chunks)
        assert retrieved == body, (
            f"body byte-equality failed: expected len={len(body)}, got len={len(retrieved)}"
        )

        # Round-2 contract: ChainAdminDetail.metadata surfaces the
        # request-envelope key-value store byte-equal. The user's
        # explicit requirement: "if it is ever persisted, all that
        # metadata is also retrievable with the files and the body."
        for prod_key, expected_value in PROD_METADATA.items():
            actual = detail.metadata.get(prod_key)
            assert actual == expected_value, (
                f"ChainAdminDetail.metadata missing or wrong for "
                f"{prod_key!r}: expected={expected_value!r}, "
                f"actual={actual!r}. Full metadata: {detail.metadata!r}"
            )
        # The synthetic ``phantom_local_uuid`` key (stamped by the
        # driver) should also surface - it's part of the KVS the
        # client sees in the persisted envelope.
        assert detail.metadata.get("phantom_local_uuid") == str(chain_id), (
            "ChainAdminDetail.metadata should include the phantom_local_uuid "
            "stamped by the driver at envelope build time"
        )

        # Round-2 contract: ChainAdminDetail.steps surfaces each
        # step's request envelope shape (name/method/url/headers/has_body).
        assert len(detail.steps) == 2, (
            f"the upload envelope has 2 steps (create_file + put_s3); admin "
            f"detail surfaced {len(detail.steps)}"
        )
        create_step = next((s for s in detail.steps if s.name == "create_file"), None)
        put_step = next((s for s in detail.steps if s.name == "put_s3"), None)
        assert create_step is not None, "create_file step missing from detail.steps"
        assert put_step is not None, "put_s3 step missing from detail.steps"
        assert create_step.method == "POST"
        assert put_step.method == "PUT"
        assert create_step.has_body is True
        assert put_step.has_body is True
        # The create-file step carries Content-Type: application/json.
        assert create_step.headers.get("Content-Type") == "application/json"
        # The put_s3 step carries the x-amz-meta-* headers derived
        # from the KVS (with underscore-to-hyphen substitution applied
        # to each metadata key).
        for prod_key, expected_value in PROD_METADATA.items():
            header_name = "x-amz-meta-" + prod_key.replace("_", "-")
            actual_value = put_step.headers.get(header_name)
            assert actual_value == expected_value, (
                f"x-amz-meta header missing or wrong on put_s3 step: "
                f"{header_name!r} expected={expected_value!r}, "
                f"actual={actual_value!r}"
            )

        # The captured value from the create-file step should still
        # surface (existing behavior; the round-2 change is additive).
        captured_dicts = [cs.values for cs in detail.captured]
        all_captured_text = str(captured_dicts).lower()
        metadata_visible_via_captures = any(
            substring in all_captured_text
            for substring in [
                PROD_METADATA["ref_id"].lower(),
                PROD_METADATA["label"].lower(),
                PROD_METADATA["order_number"].lower(),
            ]
        )
        assert metadata_visible_via_captures, (
            "Captured values (from create_file step response) should "
            "still echo the submitted metadata keys. "
            f"Captured values: {captured_dicts}"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_admin_get_lists_uploads_metadata(tmp_path: Path) -> None:
    """``list_uploads`` surfaces row metadata that lets the operator filter.

    The user's "retrievable with the files and the body" promise
    implies a tooling-level surface: an operator should be able to
    LIST chains with their metadata visible (without having to fetch
    bodies). The UploadRow currently exposes `endpoint`, `uid`,
    `idempotency_key`, `route_name`, `instance_id`, `group_id`,
    `multifile_id`, `send_order`, `state`, `body_location`,
    `attempts`, etc. — but NOT the metadata KVS.

    This test pins that list_uploads returns enough info to identify
    a chain in production — and surfaces the gap if it doesn't.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # Phase 1: all-disk mode replaces ``default_tier:
                # persisted`` + ``after_attempts: 0``.
                "body_store": {"mode": "all_disk"},
            },
            "retention": {
                "succeeded_metadata_seconds": 300,
                "succeeded_body_seconds": 300,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Submit 3 chains with distinguishable metadata.
        chain_ids: list[UUID] = []
        for i in range(3):
            cid = uuid4()
            chain_ids.append(cid)
            metadata = {**PROD_METADATA, "ref_id": f"history-{i:03d}"}
            await _submit_chain(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                chain_id=cid,
                body=secrets.token_bytes(NON_TRIVIAL_BODY_BYTES),
                metadata=metadata,
            )

        # Wait for all to succeed.
        for cid in chain_ids:
            await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )

        # list_uploads should return all 3 with full row metadata.
        rows, _ = await pc.list_uploads(limit=100)
        row_by_id = {r.chain_id: r for r in rows if r.chain_id in chain_ids}
        assert len(row_by_id) == 3, (
            f"list_uploads returned {len(row_by_id)} of 3 submitted "
            f"chains. Row metadata visibility is the load-bearing "
            f"operator-tooling promise."
        )

        # Every row should carry the production-shape fields needed to
        # identify it in admin tooling.
        for cid, row in row_by_id.items():
            assert row.endpoint, f"row {cid} missing endpoint"
            assert row.uid, f"row {cid} missing uid"
            assert row.idempotency_key, f"row {cid} missing idempotency_key"
            assert row.route_name, f"row {cid} missing route_name"
            assert row.instance_id, f"row {cid} missing instance_id"
            assert row.state == "succeeded", f"row {cid} unexpected state"
            assert row.body_location == "file", f"row {cid} unexpected body_location"
    finally:
        await stack.tear_down()
