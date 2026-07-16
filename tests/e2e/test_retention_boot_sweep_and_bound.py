"""Retention boot-sweep + the ``max_rows`` count bound, end to end (§ 5.D).

Plan § 5.2 Part 5.D, the retention + bound bundle. The store primitives
(``evict_terminal_over_limit`` oldest-terminal-first / never-undelivered; time-based
``delete_terminal_older_than``) are pinned by the unit tier
(``test_v3_retention_table_growth_and_cleanup.py``); this file proves the BEHAVIOUR
through a real boot:

* :func:`test_boot_time_sweep_reclaims_expired_rows` - SUBPROCESS harness. An old terminal
  row is seeded directly into the on-disk DB via the production ``SqliteUploadStore.insert``
  write path while no process is running, then a fresh Phantom subprocess boots with a tight
  ``failed_metadata_seconds`` window. The reaper runs its first sweep at boot (``Reaper.run``
  sweeps BEFORE it waits), so the expired row is reclaimed shortly after the service answers
  health — proving the boot-time sweep, not merely the periodic loop.

* :func:`test_max_rows_cap_holds_oldest_terminal_first_never_undelivered` - in-process stack +
  SDK. A mix of terminal ``failed`` rows (evictable) plus one still-deliverable
  ``auth_expired`` row (never evictable) is seeded over a low ``max_rows`` cap. The reaper's
  count-cap backstop drives the table down to the cap by evicting the OLDEST terminal rows
  first, and the ``auth_expired`` row survives even though the table was over the cap:
  durability wins over the count bound (invariant #1). Asserted via the on-disk census and
  the admin ``GET /v1/admin/stats`` surface.

Public e2e-light lane (§ 5.0): generic seed shapes.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    DEFAULT_INSTANCE,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    instance_dir,
    open_store_readonly,
    write_phantom_config,
)

from .helpers.stack import E2EStack, boot_stack

pytestmark = [pytest.mark.conformance, pytest.mark.e2e]

_SEED_BODY: bytes = b"phantom-5D-retention-seed-body"
_DIGEST: str = hashlib.sha256(_SEED_BODY).hexdigest()


def _seed_row(
    chain_id: UUID,
    *,
    state: str,
    updated_at: datetime,
    instance_id: str = DEFAULT_INSTANCE,
) -> UploadRow:
    """Build a terminal-state ``UploadRow`` with a controllable ``updated_at``.

    Mirrors the seed shape the retention unit tests use; the ``updated_at`` drives both
    the time-based reaper window and the count-cap oldest-first ordering.
    """
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": instance_id,
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "emulator",
            "state": state,
            "body_location": "ram",
            "received_at": updated_at,
            "updated_at": updated_at,
            "endpoint": "e",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": f"idem-{chain_id}",
            "chain_id_at_ingress": str(chain_id),
            "capture_reexecution_active": False,
            "body_hashes": {"body": {"body_hash": _DIGEST, "storage_hash": _DIGEST}},
            "body_size_bytes": len(_SEED_BODY),
        },
    )


# ---------------------------------------------------------------------------
# Boot-time sweep (subprocess harness).
# ---------------------------------------------------------------------------

# A very short failed-row metadata window so the boot sweep reaps the seeded expired row.
_FAILED_METADATA_SECONDS: int = 1
# A short reaper interval so the boot sweep (and any follow-up) lands quickly.
_REAPER_INTERVAL_SECONDS: int = 1
# How long after boot to wait for the boot sweep to clear the expired row.
_BOOT_SWEEP_BUDGET_SECONDS: float = 15.0
_BOOT_SWEEP_POLL_SECONDS: float = 0.3


async def test_boot_time_sweep_reclaims_expired_rows(tmp_path: Path) -> None:
    """An expired terminal row seeded on disk is reclaimed by the boot-time sweep.

    Falsifier: make ``Reaper.run`` wait BEFORE its first sweep (or skip the boot sweep) ->
    the expired row lingers past the boot budget -> RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    overrides = {
        "retention": {
            "failed_metadata_seconds": _FAILED_METADATA_SECONDS,
            "failed_body_seconds": 0,
            "reaper_interval_seconds": _REAPER_INTERVAL_SECONDS,
        }
    }
    proc: PhantomSubprocess | None = None
    try:
        # 1. Write the config (creates the per-instance dir layout the store expects),
        #    then seed an OLD failed row straight into the on-disk DB via the production
        #    store write path while NO process is running.
        port = allocate_port()
        cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=overrides)
        # The lifespan creates each per-instance dir at boot; here we seed BEFORE boot, so
        # create the instance subdir (where uploads.db lives) ourselves first.
        instance_dir(data_dir).mkdir(parents=True, exist_ok=True)
        store = await open_store_readonly(data_dir)
        try:
            old = datetime.now(UTC) - timedelta(seconds=3600)
            await store.insert(_seed_row(uuid4(), state="failed", updated_at=old))
        finally:
            await store.stop()
        # Sanity: the seed landed.
        assert sum((await count_rows_by_state(data_dir)).values()) == 1

        # 2. Boot a fresh subprocess on the same data_dir. Its reaper sweeps at boot.
        proc = PhantomSubprocess.make(cfg, port)
        await proc.start()

        # 3. The boot sweep reclaims the expired row.
        deadline = time.monotonic() + _BOOT_SWEEP_BUDGET_SECONDS
        remaining = -1
        while time.monotonic() < deadline:
            remaining = sum((await count_rows_by_state(data_dir)).values())
            if remaining == 0:
                break
            await asyncio.sleep(_BOOT_SWEEP_POLL_SECONDS)  # pre-commit-allow: sleep
        assert remaining == 0, (
            f"boot-time sweep did not reclaim the expired failed row; "
            f"{remaining} row(s) still on disk after {_BOOT_SWEEP_BUDGET_SECONDS}s"
        )

        # The service is healthy and serves after the boot sweep.
        async with PhantomClient(proc.url) as client:
            health = await client.get_health()
            assert health.status == "ok"
    finally:
        if proc is not None:
            proc.terminate()
        await emu.stop()


# ---------------------------------------------------------------------------
# max_rows count bound (in-process stack + SDK).
# ---------------------------------------------------------------------------

# Cap small enough that a handful of seeded terminal rows clearly exceeds it.
_MAX_ROWS: int = 3
# More terminal rows than the cap, plus one undelivered row that must survive.
_TERMINAL_SEED_COUNT: int = 6
# Reaper interval short so the count-cap backstop fires promptly.
_BOUND_REAPER_INTERVAL_SECONDS: int = 1
_BOUND_BUDGET_SECONDS: float = 15.0


async def test_max_rows_cap_holds_oldest_terminal_first_never_undelivered(
    tmp_path: Path,
) -> None:
    """The max_rows cap evicts oldest-terminal-first and never drops an undelivered row.

    Seeds ``_TERMINAL_SEED_COUNT`` ``failed`` rows (ascending ``updated_at``) plus ONE
    ``auth_expired`` row (still deliverable). With ``max_rows`` low, the count-cap backstop
    drives the table to the cap by evicting the OLDEST failed rows; the auth_expired row
    survives because durability wins over the count bound (invariant #1).

    Falsifier: drop the terminal-only guard in ``evict_terminal_over_limit`` so it evicts by
    age regardless of state -> the auth_expired row is evicted (lost) -> RED. Or evict
    newest-first -> the youngest failed rows vanish while the oldest persist -> RED.
    """
    # auth_expired is forever-retained by default (no time-based reap), so only the
    # count cap can touch it; failed metadata is 30 days by default, so within the test
    # window the time-based pass deletes nothing and the cap is the sole actor.
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retention": {
                "max_rows": _MAX_ROWS,
                "reaper_interval_seconds": _BOUND_REAPER_INTERVAL_SECONDS,
            }
        },
    )
    try:
        instance = stack.get_instance("primary")
        store = instance.store

        base = datetime.now(UTC) - timedelta(hours=1)
        # Ascending updated_at: failed_00 is OLDEST -> evicted first.
        failed_ids: list[UUID] = []
        for i in range(_TERMINAL_SEED_COUNT):
            cid = uuid4()
            failed_ids.append(cid)
            await store.insert(
                _seed_row(cid, state="failed", updated_at=base + timedelta(seconds=i))
            )
        # The undelivered (still-deliverable) row: newest timestamp, but state protects it.
        auth_expired_id = uuid4()
        await store.insert(
            _seed_row(
                auth_expired_id,
                state="auth_expired",
                updated_at=base + timedelta(seconds=_TERMINAL_SEED_COUNT + 100),
            )
        )

        # NOTE: the live reaper runs on the 1s interval concurrently with these inserts, so
        # it may begin evicting the oldest failed rows before the last seed lands. We do NOT
        # assert the pre-eviction total (that would race the reaper); we assert the END state:
        # the cap holds, oldest-terminal-first, and the undelivered row survives.

        # The count-cap backstop drives the table down to the cap.
        deadline = time.monotonic() + _BOUND_BUDGET_SECONDS
        census: dict[str, int] = {}
        while time.monotonic() < deadline:
            census = await count_rows_by_state(stack.data_dir)
            if sum(census.values()) <= _MAX_ROWS:
                break
            await asyncio.sleep(0.3)  # pre-commit-allow: sleep
        total_after = sum(census.values())
        assert total_after <= _MAX_ROWS, (
            f"max_rows={_MAX_ROWS} cap not held: {total_after} rows remain; census={census}"
        )

        # DURABILITY: the still-deliverable auth_expired row was NOT evicted.
        assert census.get("auth_expired", 0) == 1, (
            f"the still-deliverable auth_expired row was evicted by the count cap; census={census}"
        )

        # OLDEST-FIRST: the surviving failed rows are the NEWEST ones; the oldest are gone.
        remaining_failed = await _surviving_chain_ids(store, state="failed")
        surviving_count = len(remaining_failed)
        # cap holds the auth_expired row (1) + (cap - 1) newest failed rows.
        expected_surviving_failed = _MAX_ROWS - 1
        assert surviving_count == expected_surviving_failed, (
            f"expected {expected_surviving_failed} newest failed rows to survive, "
            f"got {surviving_count}"
        )
        newest_failed = set(failed_ids[-expected_surviving_failed:])
        assert remaining_failed == newest_failed, (
            "count cap did not evict oldest-terminal-first: "
            f"survivors={remaining_failed}, expected newest set={newest_failed}"
        )

        # The admin stats surface reflects the bounded reality (auth_expired still parked).
        stats = await stack.phantom_client.get_stats(instance="primary")
        assert stats.by_state.auth_expired.count == 1, (
            f"admin stats lost the parked auth_expired row: {stats.by_state.auth_expired!r}"
        )
        assert stats.parked_total >= 1, (
            f"parked_total must count the surviving auth_expired row; got {stats.parked_total}"
        )
        assert stats.auth.auth_expired_count == 1, (
            f"auth.auth_expired_count != 1 after the bound held: {stats.auth.auth_expired_count}"
        )
    finally:
        await stack.tear_down()


async def _surviving_chain_ids(store: object, *, state: str) -> set[UUID]:
    """Return the chain_ids still present in ``state`` (reads via the live store)."""
    survivors: set[UUID] = set()
    async for row in store.iter_rows():  # type: ignore[attr-defined]  # InstanceContext.store is internal
        if row.state == state:
            survivors.add(row.chain_id)
    return survivors
