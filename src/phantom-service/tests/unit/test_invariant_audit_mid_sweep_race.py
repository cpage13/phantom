"""InvariantAuditor vs a delivery completing mid-sweep (round 5, R5-1).

The auditor walks ``iter_rows()`` on the dedicated read-only connection:
the open cursor holds ONE consistent snapshot for the whole walk, while
``has_body_ref`` consults the LIVE body store. A row that delivers
between sweep start and its own audit check is therefore judged on
STALE state: the snapshot says ``attempting`` with ``body_discarded_at``
NULL, the live body store has legally deleted the bytes (the sender's
success sequence: ``record_attempt_result`` commits ``succeeded`` FIRST,
then ``body_store.delete``, then ``discard_body_and_zero_accounting``).
Both carve-outs (terminal, H4 discarded) read the stale snapshot fields,
so neither fires, and the sweep bumps a FALSE ``missing_body_file``
violation at ERROR level on a perfectly healthy store.

The module's two tests:

* ``test_delivery_completing_mid_sweep_is_not_a_violation`` - the
  deterministic interleave repro, now the passing pin of the R5-1 fix:
  on a body-ref miss the auditor re-reads the LIVE row and skips the
  row when it is gone, terminal, or ``body_discarded_at``-stamped.
* ``test_delivery_completed_before_sweep_is_not_a_violation`` - the
  at-rest control: the SAME delivery sequence finished before the sweep
  starts is already clean (terminal carve-out). Passing pin proving the
  defect is purely the stale-snapshot window.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from uuid import UUID

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.invariant_audit import InvariantAuditor

from .conftest import make_snapshot, snapshot_thunk

# The one body_ref name each row declares; presence is what the auditor
# checks, content is irrelevant.
_BODY_REF_NAME = "part0"
# Body payload bytes for the seeded rows.
_BODY_BYTES = b"mid-sweep-race-body"
# Shard prefix width for the FileBodyStore fixture (matches the unit
# auditor_stack fixture in test_invariant_audit.py).
_SHARD_PREFIX_CHARS = 2
# Attempt count recorded by the simulated successful delivery.
_DELIVERY_ATTEMPTS = 1
# Upstream status recorded by the simulated successful delivery.
_UPSTREAM_OK = 200


class _DeliveryMidSweepBodyStore(FileBodyStore):
    """FileBodyStore that runs a delivery callback inside the sweep.

    The FIRST ``has_body_ref`` call (the sweep auditing the row ahead of
    the victim) awaits ``on_first_check`` before answering, which is
    exactly "a delivery completed while the auditor was mid-walk".
    """

    def __init__(self, root: Path, *, on_first_check: Callable[[], Awaitable[None]]) -> None:
        super().__init__(root, shard_prefix_chars=_SHARD_PREFIX_CHARS)
        self._on_first_check = on_first_check
        self._fired = False

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        if not self._fired:
            self._fired = True
            await self._on_first_check()
        return await super().has_body_ref(chain_id, name)


def _declared_hashes() -> dict[str, BodyHashes]:
    """Declared hash set for the one body_ref each seeded row carries."""
    return {
        _BODY_REF_NAME: BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
    }


def _violations(registry: MetricsRegistry) -> dict[str, int]:
    """Current label -> count map of the violation counter."""
    return dict(registry.counters["invariant_violation_total"].snapshot())


async def _deliver_successfully(
    store: SqliteUploadStore, body_store: FileBodyStore, row: UploadRow
) -> None:
    """Mirror the sender's success sequence exactly (workers/sender.py).

    1. ``record_attempt_result`` commits ``succeeded`` + stamps sent_at.
    2. ``body_store.delete`` drops the bytes (immediate leg, default
       ``succeeded_body_seconds == 0``).
    3. ``discard_body_and_zero_accounting`` stamps ``body_discarded_at``.
    """
    rowcount = await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=_DELIVERY_ATTEMPTS,
        next_attempt_at=None,
        last_error=None,
        upstream_status=_UPSTREAM_OK,
        upstream_headers_json="{}",
        captured_values=None,
        current_step_index=None,
        last_step_completed="create",
        stamp_sent_at=True,
    )
    assert rowcount == 1, "delivery UPDATE must hit the attempting row"
    await body_store.delete(row.chain_id)
    await store.discard_body_and_zero_accounting(row.chain_id, expected_state="succeeded")


@pytest.mark.asyncio
async def test_delivery_completing_mid_sweep_is_not_a_violation(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A legal mid-sweep delivery must not bump any violation counter."""
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    await store.start()
    # try/finally so the strict-xfail raise still stops the store: a
    # leaked aiosqlite worker thread outliving this test's event loop
    # fails whatever test it lands on (observed on the fault-injection
    # module before this guard existed).
    try:
        ahead = make_upload_row(
            state="attempting", body_location="file", body_hashes=_declared_hashes()
        )
        victim = make_upload_row(
            state="attempting", body_location="file", body_hashes=_declared_hashes()
        )

        async def deliver_victim() -> None:
            await _deliver_successfully(store, body_store, victim)

        body_store = _DeliveryMidSweepBodyStore(tmp_path / "bodies", on_first_check=deliver_victim)
        await body_store.start()
        for row in (ahead, victim):
            await store.insert(row)
            await body_store.put(row.chain_id, {_BODY_REF_NAME: _BODY_BYTES})

        auditor = InvariantAuditor(
            store=store,
            body_store=body_store,
            current_settings=snapshot_thunk(make_snapshot()),
            metrics_registry=registry,
        )
        await auditor._sweep_once()  # type: ignore[reportPrivateUsage]

        # The delivery itself was fully legal and is fully recorded.
        live = await store.get(victim.chain_id)
        assert live is not None
        assert live.state == "succeeded"
        assert live.sent_at is not None
        assert live.body_discarded_at is not None

        # A healthy store on a busy day must audit clean: no label may move.
        assert _violations(registry) == {"": 0}
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_delivery_completed_before_sweep_is_not_a_violation(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Control: the same delivery at rest audits clean (terminal carve-out)."""
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    await store.start()
    try:
        body_store = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=_SHARD_PREFIX_CHARS)
        await body_store.start()

        row = make_upload_row(
            state="attempting", body_location="file", body_hashes=_declared_hashes()
        )
        await store.insert(row)
        await body_store.put(row.chain_id, {_BODY_REF_NAME: _BODY_BYTES})
        await _deliver_successfully(store, body_store, row)

        auditor = InvariantAuditor(
            store=store,
            body_store=body_store,
            current_settings=snapshot_thunk(make_snapshot()),
            metrics_registry=registry,
        )
        await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
        assert _violations(registry) == {"": 0}
    finally:
        await store.stop()
