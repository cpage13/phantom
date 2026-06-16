"""Aggressor — ``list_uploads`` pagination surfaces every row, no dupes.

Send 50 uploads. Iterate ``list_uploads`` with ``limit=10`` and follow
the ``next_cursor`` until exhausted. The collected chain_ids must:

- Be a superset of the 50 submitted chain_ids (no row missed).
- Have no duplicates (each chain appears at most once across pages).

This pins the admin pagination contract that operators rely on for
"show me every buffered upload" workflows. If pagination loses a row,
operators lose visibility on it. If it duplicates, the operator-side
count is wrong and downstream tooling double-counts.

Phase 1 single-store collapse means pagination cursors no longer
straddle two stores — the dual-store admin walker is gone with the
``_admin_pagination`` module. The test still validates the single-store
contract: a strict-monotonic cursor walk over the persistent
``uploads`` table.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Number of uploads to send and then enumerate.
N_UPLOADS: int = 50

# Page size for the cursor walk. Small relative to N so the test
# actually exercises multiple pages.
PAGE_LIMIT: int = 10

# Per-chain reach-succeeded budget. With 50 chains and 2 workers,
# completion takes longer than a single chain. Generous enough for
# slow runners.
TERMINAL_BUDGET_SECONDS: float = 30.0

# Maximum total pages we'll walk before giving up. Defensive against
# a pagination bug that loops cursors back to a previous page.
MAX_PAGES: int = 50

BODY: bytes = b"phantom-aggressor-pagination-body"

pytestmark = pytest.mark.e2e


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
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
        body_refs={"body": BODY},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_aggressor_list_pagination_complete(tmp_path: Path) -> None:
    """50 uploads enumerated via cursor pagination return every chain exactly once."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "saturation": {
                "max_in_flight": 100,
                "max_in_flight_bytes": 64 * 1024 * 1024,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        submitted: set[UUID] = set()
        for _ in range(N_UPLOADS):
            cid = uuid4()
            submitted.add(cid)
            await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                chain_id=cid,
            )
        assert len(submitted) == N_UPLOADS, "uuid4 collision (extremely unlikely)"

        # Wait for all chains to reach succeeded. The terminal-state
        # filter is unrelated to pagination — we audit pagination after
        # success so the rows are stable (no concurrent state changes
        # during the cursor walk).
        for cid in submitted:
            await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )

        # Walk pagination with limit=10 and collect chain_ids.
        seen: list[UUID] = []
        cursor: str | None = None
        pages_walked = 0
        while True:
            pages_walked += 1
            assert pages_walked <= MAX_PAGES, (
                f"pagination did not terminate after {MAX_PAGES} pages; "
                f"possible cursor loop. seen so far: {len(seen)}"
            )
            rows, cursor = await pc.list_uploads(limit=PAGE_LIMIT, cursor=cursor)
            assert len(rows) <= PAGE_LIMIT, (
                f"page returned {len(rows)} rows, expected <= {PAGE_LIMIT}"
            )
            for r in rows:
                seen.append(r.chain_id)
            if cursor is None:
                break

        # Assert: every submitted chain appears.
        seen_set = set(seen)
        missing = submitted - seen_set
        assert not missing, (
            f"{len(missing)} chains missing from pagination walk: "
            f"{sorted(str(c) for c in missing)[:10]}{'...' if len(missing) > 10 else ''}"
        )

        # Assert: no duplicates within the submitted set.
        # (Filter by submitted because the test's per-test stack has
        # no other rows. If a regression causes duplicate emissions
        # of submitted rows, this catches it.)
        seen_submitted = [c for c in seen if c in submitted]
        duplicates = [c for c in seen_submitted if seen_submitted.count(c) > 1]
        assert not duplicates, (
            f"pagination returned duplicate chain_ids: "
            f"{sorted(str(c) for c in set(duplicates))[:10]}"
        )
    finally:
        await stack.tear_down()
