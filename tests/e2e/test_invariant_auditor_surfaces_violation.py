"""InvariantAuditor detect-and-surface loop, end to end (TEST-1).

The unit suite (``test_invariant_audit*``) proves the row-walk LOGIC in
isolation: it constructs an :class:`InvariantAuditor`, calls
``_sweep_once`` directly, and reads the violation counter off a registry
it owns. What no unit test can cover is the RUNTIME WIRING that the CI
build-fail rule actually depends on:

* the auditor coroutine spawned under the live lifespan's supervising
  TaskGroup (one per instance), sweeping on the configured cadence,
* against the live ``uploads`` table + the mode-selected body store, and
* bumping the SAME process-wide :class:`MetricsRegistry` that the admin
  ``GET /v1/admin/observability/counters`` route reads.

The architecture-intent treats a non-zero ``invariant_violation_total``
bucket as a build-failing signal, so the path from "a real row violates
an audited invariant in a running process" to "the operator sees the
bump on the admin wire" must be proven full-stack. This module is that
proof.

How the violation is planted (deterministic, no sender race). The
auditor SKIPS terminal rows (their bodies legitimately drop on delivery)
and ``body_discarded_at``-stamped rows (the H4 retention carve-out). It
audits live, non-terminal, un-discarded rows. The single state that is
non-terminal AND never re-claimed by the sender is ``auth_expired``:
:meth:`SqliteUploadStore.claim_due` only flips ``queued`` rows, so an
``auth_expired`` row with ``next_attempt_at=None`` sits untouched by the
sender pool. That removes the race where the sender would otherwise
read the (deleted) body, hit :class:`BodyMissingError`, and route the
row to ``corrupted`` (terminal) before the auditor's next sweep. The row
is inserted directly into the live store (the same construction the unit
tests use, lifted into a booted process), with a declared ``body_hashes``
ref and NO backing body in the body store - a true invariant #1 / #3
violation that a live, healthy runtime must never produce on its own.

Two cases pin the two enforcement arms + all three label buckets:

* ``body_location='file'`` with no file on disk -> ``missing_body_file``
  (invariant #1, the headline disk-loss bucket).
* ``body_location='ram'`` with no body in RAM -> ``missing_body_in_ram``
  (the RAM arm) PLUS ``body_hash_set_mismatch`` (invariant #3 emptiness:
  every declared body_hash absent from the store).

Each case also asserts the live sweep counter (``invariant_audit_runs_total``)
advances - proof the coroutine is genuinely running, not that the bucket
was pre-seeded - and that the target bucket reads zero BEFORE the plant,
so the post-plant bump is falsifiable.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# Stable label-value bucket keys the auditor bumps. Mirrors the private
# constants in phantom.workers.invariant_audit; duplicated here (not
# imported) so this e2e pins the WIRE-OBSERVED bucket names an operator
# and the CI rule see, decoupled from the source's internal symbols.
VIOLATION_MISSING_BODY_FILE: str = "missing_body_file"
VIOLATION_MISSING_BODY_IN_RAM: str = "missing_body_in_ram"
VIOLATION_BODY_HASH_SET_MISMATCH: str = "body_hash_set_mismatch"

# Canonical counter names on the observability surface.
COUNTER_VIOLATION_TOTAL: str = "invariant_violation_total"
COUNTER_AUDIT_RUNS_TOTAL: str = "invariant_audit_runs_total"

# Tight audit cadence so the live coroutine sweeps within the test
# window. 1s is the floor that keeps the loop unambiguously periodic
# (the field validator requires >= 1) while still firing several times
# inside the poll budget below.
FAST_AUDIT_PERIOD_SECONDS: int = 1

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# Representative body bytes used only to compute realistic hash pairs for
# the planted row. The bytes are never written to any store - the whole
# point is that the declared ref has no backing body.
PLANTED_BODY_BYTES: bytes = b"phantom-invariant-auditor-e2e-planted-violation"

# Budget for the live auditor to sweep the planted row and surface the
# bump on the admin wire. Generously larger than several FAST_AUDIT
# periods so a slow host still observes the bump deterministically.
SURFACE_BUDGET_SECONDS: float = 15.0

# Budget for one extra live sweep to be observed (proves the coroutine
# is running before we assert anything about its output).
SWEEP_OBSERVED_BUDGET_SECONDS: float = 10.0


def _build_planted_row(
    *,
    body_location: str,
    instance_id: str = "primary",
) -> object:
    """Construct a non-terminal ``auth_expired`` row with an unbacked body.

    The row declares one ``body_hashes`` entry (realistic hash pair) but
    the caller writes NO body to the store, so the auditor's
    ``has_body_ref`` miss is a genuine invariant #1 / #3 violation. The
    ``auth_expired`` state with ``next_attempt_at=None`` keeps the sender
    pool away from it (``claim_due`` only takes ``queued`` rows), so the
    violation persists deterministically across the audit window.

    Args:
        body_location: ``"file"`` or ``"ram"`` - selects which violation
            bucket the auditor bumps.
        instance_id: Instance the row belongs to (the suite default
            instance is ``"primary"``).

    Returns:
        An :class:`UploadRow` ready for ``store.insert``. Typed ``object``
        so this test module imports no internal ``phantom`` model at the
        signature boundary; the concrete construction is local.
    """
    # Local import: the row model is a service-internal type. Importing
    # it inside the helper keeps the module's public surface free of
    # service internals while still letting the test build a faithful row
    # exactly as the unit suite does.
    from phantom.models.upload import (
        BodyHash,
        BodyHashes,
        StorageHash,
        UploadRow,
    )

    body_hash = hashlib.sha256(PLANTED_BODY_BYTES).hexdigest()
    storage_hash = hashlib.sha256(PLANTED_BODY_BYTES + b"-stored").hexdigest()
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": instance_id,
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "emulator",
            # auth_expired: non-terminal (audited) AND never re-claimed by
            # the sender (claim_due takes only queued).
            "state": "auth_expired",
            "body_location": body_location,
            # No next attempt scheduled - the row is parked, exactly as
            # the _on_auth_failure path leaves it.
            "next_attempt_at": None,
            "received_at": now,
            "updated_at": now,
            "endpoint": "upstream.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": str(chain_id),
            "capture_reexecution_active": False,
            "body_hashes": {
                BODY_REF_NAME: BodyHashes(
                    body_hash=BodyHash(body_hash),
                    storage_hash=StorageHash(storage_hash),
                ),
            },
        }
    )


async def _violation_bucket(stack: E2EStack, label_value: str) -> int:
    """Read one ``invariant_violation_total`` bucket off the admin wire.

    Routes through the SDK ``get_observability_counters`` - the exact
    path the CI build-fail rule and any operator dashboard consume. A
    bucket that was never bumped is simply absent from ``values`` (the
    counter only materializes a label on first increment), so an absent
    bucket reads as zero.
    """
    response = await stack.phantom_client.get_observability_counters()
    for counter in response.counters:
        if counter.name == COUNTER_VIOLATION_TOTAL:
            return counter.values.get(label_value, 0)
    raise AssertionError(
        f"{COUNTER_VIOLATION_TOTAL} counter missing from the observability surface; "
        f"present counters={[c.name for c in response.counters]}"
    )


async def _audit_runs_total(stack: E2EStack) -> int:
    """Read the live sweep-iteration counter off the admin wire."""
    response = await stack.phantom_client.get_observability_counters()
    for counter in response.counters:
        if counter.name == COUNTER_AUDIT_RUNS_TOTAL:
            # The no-label bucket carries the total sweep count.
            return counter.values.get("", 0)
    raise AssertionError(
        f"{COUNTER_AUDIT_RUNS_TOTAL} counter missing from the observability surface; "
        f"present counters={[c.name for c in response.counters]}"
    )


async def _await_one_more_sweep(stack: E2EStack) -> None:
    """Block until the live auditor records at least one more sweep.

    Proves the coroutine is genuinely periodic in the booted process
    before the test reasons about its output - if the loop were dead the
    counter would never advance and this raises a clear timeout.
    """
    start = await _audit_runs_total(stack)

    async def _advanced() -> bool:
        return await _audit_runs_total(stack) > start

    await await_until(
        _advanced,
        timeout_seconds=SWEEP_OBSERVED_BUDGET_SECONDS,
        message="invariant_audit_runs_total never advanced; the auditor coroutine is not sweeping",
    )


async def _plant_and_assert(
    stack: E2EStack,
    *,
    body_location: str,
    expected_buckets: tuple[str, ...],
) -> None:
    """Plant one unbacked-body row and assert each expected bucket bumps.

    Asserts each target bucket reads zero first (falsifiability), inserts
    the planted row into the LIVE store, then polls the admin wire until
    every ``expected_buckets`` entry is non-zero within the budget.
    """
    instance = stack.get_instance("primary")

    # Baseline: every target bucket is zero on a healthy runtime BEFORE
    # the plant, so the post-plant bump is unambiguously ours.
    for bucket in expected_buckets:
        baseline = await _violation_bucket(stack, bucket)
        assert baseline == 0, (
            f"bucket {bucket!r} was already non-zero ({baseline}) before planting; "
            "the healthy runtime should report zero invariant violations"
        )

    row = _build_planted_row(body_location=body_location)
    chain_id: UUID = row.chain_id  # type: ignore[attr-defined]
    await instance.store.insert(row)
    # Sanity: the body store genuinely has no backing ref for the row, so
    # the auditor miss is a real violation and not a test-setup artifact.
    assert not await instance.body_store.has_body_ref(chain_id, BODY_REF_NAME), (
        "planted row unexpectedly has a backing body; the violation would not fire"
    )
    logger.info(
        "planted auth_expired row chain_id=%s body_location=%s with no backing body",
        chain_id,
        body_location,
    )

    async def _all_buckets_bumped() -> bool:
        for bucket in expected_buckets:
            if await _violation_bucket(stack, bucket) < 1:
                return False
        return True

    await await_until(
        _all_buckets_bumped,
        timeout_seconds=SURFACE_BUDGET_SECONDS,
        message=(
            "the live InvariantAuditor never surfaced the planted violation on the "
            f"admin observability wire; expected buckets {expected_buckets} to bump"
        ),
    )


async def test_auditor_surfaces_missing_body_file_on_admin_wire() -> None:
    """A live ``body_location='file'`` row with no file bumps the wire counter.

    Boots a real instance with a tight audit cadence, confirms the live
    auditor coroutine is sweeping, plants an ``auth_expired`` row whose
    declared body file does not exist, and asserts
    ``invariant_violation_total{missing_body_file}`` increments through
    ``GET /v1/admin/observability/counters`` - the invariant #1
    detect-and-surface path the CI build-fail rule depends on.
    """
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {"invariant_audit_period_seconds": FAST_AUDIT_PERIOD_SECONDS},
            },
        },
    )
    try:
        await _await_one_more_sweep(stack)
        await _plant_and_assert(
            stack,
            body_location="file",
            expected_buckets=(VIOLATION_MISSING_BODY_FILE,),
        )
    finally:
        await stack.tear_down()


async def test_auditor_surfaces_ram_and_hash_set_mismatch_on_admin_wire() -> None:
    """A live ``body_location='ram'`` row with no RAM body bumps both arms.

    The RAM arm of invariant #1 (``missing_body_in_ram``) and the
    invariant #3 emptiness check (``body_hash_set_mismatch``: every
    declared body_hash absent from the store) both fire for a parked
    ``auth_expired`` row whose RAM body is gone. Asserts BOTH buckets
    surface on the admin wire - the second enforcement arm the
    file-only case does not reach.
    """
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {"invariant_audit_period_seconds": FAST_AUDIT_PERIOD_SECONDS},
            },
        },
    )
    try:
        await _await_one_more_sweep(stack)
        await _plant_and_assert(
            stack,
            body_location="ram",
            expected_buckets=(
                VIOLATION_MISSING_BODY_IN_RAM,
                VIOLATION_BODY_HASH_SET_MISMATCH,
            ),
        )
    finally:
        await stack.tear_down()
