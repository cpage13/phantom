"""Aggressor — cross-instance body isolation.

Two instances configured with distinct ``data_dir`` values must keep
their bodies separated on disk. A body submitted via instance A must
not appear when the operator fetches bodies via instance B's admin
scope.

Normal-user scenario: an operator runs a Phantom container with two
configured instances (prod / staging), submits one upload via each
instance, then audits each instance's row inventory. The two instances
must not cross-pollinate — neither rows nor bodies leak across.

What this pins:

- ``list_uploads?instance=prod`` returns only prod's row.
- ``list_uploads?instance=staging`` returns only staging's row.
- ``fetch_body(prod_chain)`` returns prod's body bytes; not staging's.
- ``fetch_body(staging_chain)`` returns staging's body bytes.
- The two on-disk body files live under distinct per-instance trees
  (``data_dir/<instance.data_dir>/bodies/...``).

If a regression causes instance A's admission code to write to
instance B's store, this catches it.

E2E-20 covers basic dispatch + ``list_uploads`` scoping; this test
extends that contract to retrieved-body byte-equality and on-disk
file-tree isolation. The plan ladder is: dispatch → scope → bytes →
file tree.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.auth.modes import AuthMode

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

PROD_BODY: bytes = b"phantom-aggressor-cross-instance-prod-body-content-001"
STAGING_BODY: bytes = b"phantom-aggressor-cross-instance-staging-body-content-002"
SHARED_SUB: str = "00000000-0000-0000-0000-000000000099"
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


async def _submit_to_instance(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
    instance_id: str,
) -> None:
    """Submit one chain to a named instance via the X-Phantom-Instance header."""
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
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(instance_id=instance_id),  # type: ignore[call-arg]
    )


async def test_aggressor_cross_instance_body_isolation(tmp_path: Path) -> None:
    """Two-instance topology: row + body files stay in their own per-instance tree."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=1,
        config_overrides={
            "storage": {
                # All-disk mode so the on-disk file tree is observable
                # immediately (Phase 1 replaces persist-on-receipt /
                # ``after_attempts: 0``).
                "body_store": {"mode": "all_disk"},
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
            "retention": {
                "succeeded_metadata_seconds": 120,
                "succeeded_body_seconds": 60,
            },
            "instances": [
                {
                    "id": "prod",
                    "host_prefixes": ["emulator-prod"],
                    "data_dir": "prod",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "prod_emulator",
                            "hosts": ["emulator-prod", "127.0.0.1", "localhost"],
                            "auth_mode": "none",
                        },
                    ],
                },
                {
                    "id": "staging",
                    "host_prefixes": ["emulator-staging"],
                    "data_dir": "staging",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "staging_emulator",
                            "hosts": ["emulator-staging", "127.0.0.1", "localhost"],
                            "auth_mode": "none",
                        },
                    ],
                },
            ],
        },
    )
    try:
        pc = stack.phantom_client
        prod_emulator = stack.emulator
        staging_emulator = stack.extra_emulators[0]
        prod_url = stack.emulator_url
        staging_url = stack.extra_emulator_urls[0]

        prod_emulator.clear_received()
        prod_emulator.clear_failures()
        staging_emulator.clear_received()
        staging_emulator.clear_failures()

        # Both emulators bind to 127.0.0.1; flip auth_mode to NONE so a
        # single bearer satisfies both. The dispatch test (E2E-20) does
        # the same; we follow the same pattern.
        prod_emulator.set_auth_mode(AuthMode.NONE)
        staging_emulator.set_auth_mode(AuthMode.NONE)
        bearer = stack.fake_security_token(sub=SHARED_SUB)

        # Submit one chain to each instance.
        chain_prod = uuid4()
        await _submit_to_instance(
            pc,
            chain_id=chain_prod,
            body=PROD_BODY,
            emulator_url=prod_url,
            bearer=bearer,
            instance_id="prod",
        )
        chain_staging = uuid4()
        await _submit_to_instance(
            pc,
            chain_id=chain_staging,
            body=STAGING_BODY,
            emulator_url=staging_url,
            bearer=bearer,
            instance_id="staging",
        )

        # Both must reach succeeded.
        await assert_chain_reaches_state(
            pc,
            chain_prod,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        await assert_chain_reaches_state(
            pc,
            chain_staging,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )

        # Scope: list_uploads filtered by instance.
        prod_rows, _ = await pc.list_uploads(instance="prod")
        prod_ids = {r.chain_id for r in prod_rows}
        assert chain_prod in prod_ids
        assert chain_staging not in prod_ids, (
            f"chain_staging {chain_staging} leaked into prod scope"
        )

        staging_rows, _ = await pc.list_uploads(instance="staging")
        staging_ids = {r.chain_id for r in staging_rows}
        assert chain_staging in staging_ids
        assert chain_prod not in staging_ids, f"chain_prod {chain_prod} leaked into staging scope"

        # Body fetch — each instance returns its own bytes.
        prod_chunks: list[bytes] = []
        async for chunk in await pc.fetch_body(chain_prod):
            prod_chunks.append(chunk)
        prod_retrieved = b"".join(prod_chunks)
        assert prod_retrieved == PROD_BODY, (
            f"prod body mismatch: got {len(prod_retrieved)} bytes, "
            f"expected {len(PROD_BODY)} ({PROD_BODY!r:.50})"
        )

        staging_chunks: list[bytes] = []
        async for chunk in await pc.fetch_body(chain_staging):
            staging_chunks.append(chunk)
        staging_retrieved = b"".join(staging_chunks)
        assert staging_retrieved == STAGING_BODY, (
            f"staging body mismatch: got {len(staging_retrieved)} bytes, "
            f"expected {len(STAGING_BODY)} ({STAGING_BODY!r:.50})"
        )

        # On-disk: prod's body file lives under prod's tree; staging's
        # under staging's tree. Cross-checks: prod's body file is NOT
        # in the staging tree, and vice versa.
        prod_path = stack.body_path(chain_prod, "body", instance_id="prod")
        staging_path = stack.body_path(chain_staging, "body", instance_id="staging")
        assert prod_path.is_file(), f"prod body file missing at {prod_path}"
        assert staging_path.is_file(), f"staging body file missing at {staging_path}"

        # Cross-bleed check: chain_prod's file under staging's tree
        # must NOT exist (and vice versa).
        prod_under_staging = stack.body_path(chain_prod, "body", instance_id="staging")
        staging_under_prod = stack.body_path(chain_staging, "body", instance_id="prod")
        assert not prod_under_staging.exists(), (
            f"chain_prod body leaked into staging tree: {prod_under_staging}"
        )
        assert not staging_under_prod.exists(), (
            f"chain_staging body leaked into prod tree: {staging_under_prod}"
        )
    finally:
        await stack.tear_down()
