"""AuthKicker vs a body-discarded ``auth_expired`` row (round 6, R6-3).

The H4 carve-out (``body_discarded_at IS NOT NULL``) marks a row whose
body bytes were intentionally removed: the reaper's scheduled
body-discard pass deletes the body files of an ``auth_expired`` row once
its ``auth_expired_body_seconds`` window elapses, stamps
``body_discarded_at``, and zeroes ``body_size_bytes`` - the body is gone
ON PURPOSE, the metadata is retained for forensics. Every other consumer
of that carve-out respects it:

* recovery (``run_recovery``) skips ``body_discarded_at`` rows so it
  never re-quarantines them as corrupt (recovery.py).
* the :class:`InvariantAuditor` skips them so it never bumps a false
  ``missing_body_*`` violation (invariant_audit.py).
* ``replay`` refuses them UP FRONT with :class:`ReplayBodyDiscardedError`
  - a clean 409 - because a re-queue would land the row in ``corrupted``
  on the sender's next claim (sqlite_store.py).

The :class:`AuthKicker` is the lone consumer that does NOT guard
``body_discarded_at``. ``auth_expired`` is non-terminal, so
``list_non_terminal`` returns a body-discarded ``auth_expired`` row, and
``_rescan`` re-queues it (``auth_expired -> queued``) the moment a fresh
token lands for its ``(endpoint, uid)`` - no body-presence check. The
sender then claims a row with no body, ``load_body_refs`` raises
``BodyMissingError``, and the row dies in ``corrupted`` with
``last_error="storage_corruption:bodies_missing"``.

Why it matters: this is DETERMINISTIC, not a race. An operator who, on
space-constrained Pi-class hardware, sets a short ``auth_expired_body_
seconds`` ("discard the body after an hour, keep the metadata") and then
finally pushes a good token sees the parked upload's honest
"waiting-for-auth, body discarded by policy" record destroyed and
replaced with a STORAGE-CORRUPTION terminal state - a misleading
hardware-fault diagnostic for a row that was aged out by configuration.
The wake also burns a saturation slot and a delivery attempt that a
genuinely deliverable upload could have used. The fix mirrors ``replay``:
``_rescan`` must skip rows with ``body_discarded_at`` stamped (there is
no body left to deliver), leaving the row in ``auth_expired`` until the
metadata-retention pass reaps it.

The repro builds the post-discard state directly (body files never
present, ``body_discarded_at`` stamped, ``body_size_bytes=0``), lands a
fresh token, runs one ``_rescan``, and asserts the kicker leaves the row
parked. Before the R6-3 guard the kicker re-queued it; the fix skips
body-discarded rows up front, before the token-cache lookup.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

# Endpoint + uid the parked row and the freshly-landed token share.
_ENDPOINT = "files.example.com"
_UID = "user-1"
# A non-zero body size the row carried BEFORE the reaper zeroed it; the
# seeded row uses 0 (post-discard accounting), this documents intent.
_DISCARDED_BODY_SIZE_BYTES = 0


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a hybrid single-store InstanceContext for the kicker test.

    Mirrors the ``_build`` helper in ``test_auth_kicker.py`` (same
    component set), tracked for R5-3 teardown.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="none")],
    )
    sat = SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000,
        max_disk_bytes=1_000_000_000,
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=MagicMock(),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=sat,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


@pytest.mark.asyncio
async def test_kicker_skips_body_discarded_auth_expired_row(tmp_path: Path) -> None:
    """A body-discarded ``auth_expired`` row must stay parked on a fresh token.

    The reaper already discarded this row's body by retention policy
    (``body_discarded_at`` stamped, ``body_size_bytes=0``, no body in the
    store). A fresh token landing must NOT revive it: there is no body to
    deliver, so re-queuing only burns a saturation slot + an attempt and
    overwrites the honest auth-waiting record with a storage-corruption
    one. The kicker must skip it (mirroring ``replay``'s
    ``ReplayBodyDiscardedError`` refusal), leaving the row in
    ``auth_expired`` for the metadata-retention reaper.

    Before the R6-3 guard the kicker re-queued the row (``-> queued``)
    because it never checked ``body_discarded_at``; the fix skips the
    row before the token-cache lookup.
    """
    instance = await _build_instance(tmp_path)
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    # The post-discard state: parked in auth_expired, body intentionally
    # discarded by the reaper. No body_refs are written to the store, and
    # body_discarded_at is stamped (the H4 carve-out marker).
    row = UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=None,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="file",
        body_size_bytes=_DISCARDED_BODY_SIZE_BYTES,
        body_discarded_at=now,
        received_at=now,
        updated_at=now,
        endpoint=_ENDPOINT,
        uid=_UID,
        chain_envelope_json="{}",
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    await instance.store.insert(row)
    # A good token finally lands for this slot.
    await instance.token_cache.set(
        _ENDPOINT,
        _UID,
        "Bearer fresh-token",
        source="inbound_request",
    )

    kicker = AuthKicker(instance=instance)
    # Drive one rescan directly: deterministic, no polling.
    await kicker._rescan()

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "auth_expired", (
        "the kicker must leave a body-discarded auth_expired row parked "
        "(no body to deliver); re-queuing it sends it to a misleading "
        f"corrupted state. Observed state={fresh.state!r}"
    )
    # Corroboration: the kicker must not have charged the gate for a row
    # it cannot deliver.
    assert instance.saturation.in_flight == 0, (
        "the kicker must not admit a body-discarded row through the "
        f"saturation gate. Observed in_flight={instance.saturation.in_flight}"
    )
