"""Admin reads stay correct and prompt during a sustained ingest+delivery storm.

Cycle-7 plan 06_09 task 7.1(e), the wire variant of the unit-tier
reader-beside-writer storm (``test_reader_connection.py``): that suite
drives the store object directly; this module drives a REAL
``python -m phantom`` daemon (subprocess harness) over HTTP while the
emulator serves deliveries, so the read-only admin connection is
exercised through the full stack: uvicorn, the routes, the dispatcher,
and the store's reader connection, all while the writer connection
churns admissions, attempt results, and sent_at stamps.

The storm: several concurrent submitters push a bounded burst of
uploads (all sharing one query group so the rollup is a real multi-row
read) while the daemon's sender delivers them upstream; concurrent
readers hammer the admin surface the whole time, including the drain.

Correctness asserted per read: details belong to the queried chain;
list pages contain only submitted rows; the group rollup's total stays
inside the submitted floor/ceiling bracket (mirroring the unit storm's
visibility bound) with an internally consistent histogram; the
local-uuid lookup finds what was submitted. Health asserted at the
end: ZERO read errors, no read ever stalled past a generous ceiling
(a lock-contention/deadlock backstop, NOT a latency benchmark; the
perf lane owns wall-clock gates), and every submitted upload still
delivered (readers never starve the writer).
"""

from __future__ import annotations

import asyncio
import logging
import random
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e._harness.subprocess_harness import (
    DEFAULT_SUB,
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    write_phantom_config,
)

from .helpers.payloads import build_create_file_request
from .helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# Concurrent submitter coroutines; with the uploads-per-writer quota
# below this keeps a sustained multi-lane ingest going while delivery
# and the readers churn.
STORM_WRITERS: int = 4

# Uploads per submitter. 4 x 30 = 120 total: enough rows that the group
# rollup and list reads are real multi-row scans, bounded enough that
# the drain finishes briskly in the default lane.
UPLOADS_PER_WRITER: int = 30

# Total storm volume (derived; named for the assertions below).
STORM_TOTAL_UPLOADS: int = STORM_WRITERS * UPLOADS_PER_WRITER

# Concurrent admin-reader coroutines.
STORM_READERS: int = 3

# Per-upload body size: small keeps the storm fast; the reader is
# exercised by ROW volume, not body volume.
STORM_BODY_BYTES: int = 1024

# A read that takes longer than this against a loopback daemon means
# the reader is blocked behind the writer (the historical
# cursor-vs-checkpoint class) or the service deadlocked. This is a
# regression BACKSTOP, deliberately generous; wall-clock latency gates
# live in the perf lane (test_perf_baseline_gates.py).
READ_STALL_CEILING_SECONDS: float = 5.0

# The readers must have genuinely exercised the surface while the storm
# ran; far below what three sub-millisecond readers produce in seconds,
# so it only catches a broken reader loop.
MIN_TOTAL_READS: int = 30

# Budget for the full backlog to deliver after submission ends.
DRAIN_BUDGET_SECONDS: float = 90.0

# Page size for list reads during the storm; spans the whole burst so
# the subset check sees every row the service reports.
LIST_PAGE_LIMIT: int = 300

# Seeded RNG so the reader op mix is reproducible across runs.
READER_RNG_SEED: int = 7

# Reaper interval override that effectively disables sweeps mid-test,
# so rows the readers inspect cannot vanish under them.
REAPER_DISABLED_INTERVAL_SECONDS: int = 3600

# Sender worker count + poll for a brisk drain (mirrors the
# multi-instance one-host-down override).
STORM_SENDER_WORKERS: int = 4
STORM_SENDER_POLL_MS: int = 50

# The nine canonical states; every read's state field must be one.
VALID_STATES: frozenset[str] = frozenset(
    {
        "queued",
        "attempting",
        "succeeded",
        "failed",
        "auth_expired",
        "stored",
        "cancelled",
        "corrupted",
        "expired",
    }
)


@dataclass
class _StormLedger:
    """Shared truth between submitters and readers.

    Attributes:
        group_id: The one query group every storm upload joins.
        started: Chain ids whose submission coroutine has STARTED
            (the rollup ceiling: the service can never report more
            group members than submissions started).
        admitted: Chain ids whose 202 has RETURNED (the rollup floor
            snapshot source: a read that starts now must see at least
            the rows already durably admitted).
        storm_over: Set once submission AND the delivery drain both
            finished; the reader lanes run until then, so reads cover
            ingest, delivery churn, and the drain.
    """

    group_id: UUID
    started: list[UUID]
    admitted: list[UUID]
    storm_over: asyncio.Event


@dataclass(frozen=True)
class _ReadSample:
    """One timed admin read: which surface and how long it took."""

    op: str
    seconds: float


async def _submit_storm_upload(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
    group_id: UUID,
) -> None:
    """Submit one upload tagged into the storm group."""
    request = build_create_file_request(file_name=f"storm-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": secrets.token_bytes(STORM_BODY_BYTES)},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(group_id=group_id),  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
    )


async def _submitter(
    pc: PhantomClient,
    ledger: _StormLedger,
    *,
    emulator_url: str,
    bearer: str,
) -> None:
    """One submitter lane: push its quota, recording start + admit."""
    for _ in range(UPLOADS_PER_WRITER):
        chain_id = uuid4()
        ledger.started.append(chain_id)
        await _submit_storm_upload(
            pc,
            emulator_url=emulator_url,
            bearer=bearer,
            chain_id=chain_id,
            group_id=ledger.group_id,
        )
        ledger.admitted.append(chain_id)


async def _reader(
    pc: PhantomClient,
    ledger: _StormLedger,
    rng: random.Random,
    samples: list[_ReadSample],
    errors: list[str],
) -> None:
    """One reader lane: hammer the admin surface until the storm ends.

    Every read is timed and correctness-checked; faults are collected
    (not raised) so one bad read cannot hide the others, and the final
    assertions surface the whole harvest.
    """
    ops = ("detail", "list", "rollup", "lookup", "stats")
    while not ledger.storm_over.is_set() or not samples:
        op = rng.choice(ops)
        # Snapshot the floor BEFORE the read starts: rows already
        # admitted then must be visible to it (the unit storm's
        # min-over-active-read-floors bound, single-read form).
        floor_ids = list(ledger.admitted)
        begin = time.perf_counter()
        try:
            if op == "detail" and floor_ids:
                target = rng.choice(floor_ids)
                detail = await pc.get_upload(target)
                assert detail.chain_id == target
                assert detail.state in VALID_STATES
                assert detail.group_id == ledger.group_id
            elif op == "list":
                rows, _cursor = await pc.list_uploads(limit=LIST_PAGE_LIMIT)
                listed = {r.chain_id for r in rows}
                foreign = listed - set(ledger.started)
                assert not foreign, f"list returned rows never submitted: {foreign}"
            elif op == "rollup" and floor_ids:
                rollup = await pc.get_group_status(ledger.group_id)
                ceiling = len(ledger.started)
                assert len(floor_ids) <= rollup.total <= ceiling, (
                    f"rollup total {rollup.total} outside the submitted bracket "
                    f"[{len(floor_ids)}, {ceiling}]"
                )
                assert sum(rollup.counts_by_state.values()) == rollup.total
                assert len(rollup.members) == rollup.total
            elif op == "lookup" and floor_ids:
                target = rng.choice(floor_ids)
                found = await pc.find_by_local_uuid(target)
                assert found.found is True, f"admitted row {target} must be findable"
                assert found.matches[0].chain_id == target
            elif op == "stats":
                stats = await pc.get_stats()
                assert stats is not None
            else:
                # No row admitted yet for a row-addressed op; yield and retry.
                await asyncio.sleep(0)  # pre-commit-allow: sleep (zero-second yield)
                continue
        except AssertionError as exc:
            errors.append(f"{op}: {exc}")
        except Exception as exc:  # storm collector; re-surfaced in the final assertion
            errors.append(f"{op}: {type(exc).__name__}: {exc}")
        finally:
            elapsed = time.perf_counter() - begin
            samples.append(_ReadSample(op=op, seconds=elapsed))
        await asyncio.sleep(0)  # pre-commit-allow: sleep (zero-second yield)


async def test_admin_reads_stay_correct_during_ingest_and_delivery_storm(
    tmp_path: Path,
) -> None:
    """The full-stack reader storm: correct snapshots, no stalls, no starved writes."""
    emulator: EmulatorHandle = await boot_emulator()
    proc: PhantomSubprocess | None = None
    try:
        port = allocate_port()
        config_path = write_phantom_config(
            data_dir=tmp_path,
            bind_port=port,
            config_overrides={
                "retention": {"reaper_interval_seconds": REAPER_DISABLED_INTERVAL_SECONDS},
                "retry": {
                    "worker_count": STORM_SENDER_WORKERS,
                    "poll_interval_ms": STORM_SENDER_POLL_MS,
                },
            },
        )
        proc = PhantomSubprocess.make(config_path, port)
        await proc.start()
        bearer = fake_security_token(emulator)

        ledger = _StormLedger(
            group_id=uuid4(),
            started=[],
            admitted=[],
            storm_over=asyncio.Event(),
        )
        samples: list[_ReadSample] = []
        errors: list[str] = []
        rng = random.Random(READER_RNG_SEED)

        # The readers exercise admin routes (get_upload, list_uploads,
        # get_group_status, get_stats) and submitters use intake - all on the
        # single listener, so one client covers both. The "reads don't starve
        # writes" property is unchanged - one client drives the same Phantom
        # process under the same storm.
        async with PhantomClient(proc.url) as pc:
            # Reader lanes run for the WHOLE storm: submission, delivery
            # churn, and the drain; storm_over releases them at the end.
            async with asyncio.TaskGroup() as tg:
                for _ in range(STORM_READERS):
                    tg.create_task(_reader(pc, ledger, rng, samples, errors))
                try:
                    async with asyncio.TaskGroup() as submit_tg:
                        for _ in range(STORM_WRITERS):
                            submit_tg.create_task(
                                _submitter(pc, ledger, emulator_url=emulator.url, bearer=bearer)
                            )
                    assert len(ledger.admitted) == STORM_TOTAL_UPLOADS

                    # The drain: every submitted upload must still deliver
                    # while the readers keep hammering, proving reads never
                    # starve the write path.
                    async def _all_delivered() -> bool:
                        rollup = await pc.get_group_status(ledger.group_id)
                        return rollup.counts_by_state["succeeded"] == STORM_TOTAL_UPLOADS

                    await await_until(
                        _all_delivered,
                        timeout_seconds=DRAIN_BUDGET_SECONDS,
                        message="the storm backlog never fully delivered",
                    )
                finally:
                    ledger.storm_over.set()

            # Final settled truth over the wire.
            final = await pc.get_group_status(ledger.group_id)
            assert final.total == STORM_TOTAL_UPLOADS
            assert final.all_finished is True
            assert final.last_sent_at is not None
            rows, _ = await pc.list_uploads(state="succeeded", limit=LIST_PAGE_LIMIT)
            assert {r.chain_id for r in rows} >= set(ledger.started)

        # Read health: zero faults, a real read volume, and no stall.
        assert not errors, f"{len(errors)} reader fault(s) during the storm: {errors[:10]}"
        assert len(samples) >= MIN_TOTAL_READS, (
            f"the readers only completed {len(samples)} reads; the storm was not exercised"
        )
        slowest = max(samples, key=lambda s: s.seconds)
        assert slowest.seconds < READ_STALL_CEILING_SECONDS, (
            f"admin read '{slowest.op}' stalled {slowest.seconds:.2f}s during the storm "
            f"(ceiling {READ_STALL_CEILING_SECONDS}s): the reader is blocking behind writes"
        )
        logger.info(
            "reader storm: %d reads, slowest %.4fs (%s), %d uploads delivered",
            len(samples),
            slowest.seconds,
            slowest.op,
            STORM_TOTAL_UPLOADS,
        )
    finally:
        if proc is not None:
            proc.terminate()
        await emulator.stop()
