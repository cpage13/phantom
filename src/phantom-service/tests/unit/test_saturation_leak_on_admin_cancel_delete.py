"""Admin cancel/delete of an in-flight row must release its saturation slot (R8-4).

The SaturationGate is an enforced bound: ingress charges a slot +
declared bytes on admission, and EVERY path that takes a row out of the
in-flight set must release them, or the gate drifts upward and never
returns - eventually 503-refusing fresh ingress (``saturation_cap``)
while actual in-flight is far lower. The sender already understands this
for its own transitions (it releases on succeeded / failed / corrupted /
auth_expired) and the architecture-intent reliability table names the
gate's balance an invariant ("saturation-bytes basis matches
body_size_bytes"; ``saturation_balance`` returns to zero when idle). The
auth_expired release carries the rationale verbatim: "an AD outage with
N flapping rows accumulates N forever and eventually 503-rejects fresh
ingress when actual in-flight is zero."

Three operator paths break that balance because they are PURE store
methods with no gate release, while the rows they act on are still
holding slots:

* ``POST /v1/admin/chains/{id}/cancel`` on a ``queued`` row (admitted,
  not yet released).
* ``DELETE /v1/admin/chains/{id}`` and ``DELETE /v1/admin/chains``
  (bulk) on a ``stored`` row. ``stored`` rows DELIBERATELY keep their
  slot (the sender's ``_record_stored`` documents "Saturation is
  deliberately NOT released for stored: the body still occupies space
  until export or replay resolves the row") - so deleting one without
  releasing strands the slot permanently. ``state=stored`` /
  ``state=queued`` bulk delete is the routine "clear the stuck uploads"
  cleanup.

The ``attempting`` case is worse because BOTH ends decline: when admin
cancel takes an ``attempting`` row, the sender's next
``record_attempt_result`` no-ops (rowcount 0, state no longer
``attempting``) and explicitly skips release - "Do not release
saturation ... those side-effects are tied to a successful state
transition" - handing the release to the cancel path, which never does
it. The M-W4-F7 protection that keeps the sender from double-acting
leaves the slot owned by nobody.

Net: an operator who cancels a stuck queued upload, or bulk-deletes
stored/queued rows, or cancels a row mid-attempt, permanently shrinks
the gate's usable capacity. On a long-running Pi deployment these are
routine maintenance actions; the drift accumulates until ``/v1/send``
starts answering 503 ``saturation_cap`` with the gauge showing in-flight
bytes that correspond to no live row. No upload is lost, so this is an
availability defect, not a durability one - the same severity class as
the auth_expired leak the sender fixes inline.

The tests drive the REAL admin routes (``cancel_upload``,
``delete_upload``, ``bulk_delete_uploads``) over a REAL SqliteUploadStore +
SaturationGate + HybridBodyStore through the InstanceDispatcher, exactly
as the ASGI layer calls them. They pin the operator-observable
invariant: after the operation the gate's in-flight count and bytes
return to zero. The natural fix releases the gate for the rows these
paths remove (the cancel/delete routes read each row's
``body_size_bytes`` and state, then release for rows that still held a
slot), mirroring the sender's own release discipline.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

from phantom.chain.executor import Succeeded
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.admin import DeleteFilter
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import track_instance

# Declared body size for the in-flight rows. A few KiB so the leak is a
# visibly non-zero byte residue, not a 0-byte edge.
_DECLARED_BYTES: int = 5_000

# Generous gate caps so no admission is refused for capacity reasons -
# the test is about RELEASE, not the admit threshold.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

# R8-4 (fixed): every row-removing path releases through the one shared
# row_holds_slot predicate, on accounting captured atomically with the
# removal; cancel owns the release for the row it cancels (M-W4-F7).


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose saturation gate the routes must keep exact.

    The store, body stores, and gate are REAL (the routes and the
    sender's release path touch all three); the executor / upstream /
    token cache are inert mocks the cancel/delete paths never call.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await ram.start()
    await fbs.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    saturation = SaturationGate(
        max_in_flight=_GATE_ROW_CAP,
        max_in_flight_bytes=_GATE_BYTE_CAP,
        max_disk_bytes=_GATE_DISK_CAP,
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=MagicMock(),
    )
    return track_instance(instance)


async def _seed_in_flight_row(
    instance: InstanceContext,
    make_upload_row: Callable[..., UploadRow],
    *,
    state: str,
) -> UploadRow:
    """Insert a row in ``state`` and charge the gate for it (as admission did).

    ``queued`` / ``attempting`` / ``stored`` all hold a slot at this
    point in their lifecycle (admission charged it; the sender has not
    released it - and for ``stored`` deliberately never will until
    export/replay). The admit here reproduces that charge exactly.
    """
    row = make_upload_row(state=state, route_name="files", body_size_bytes=_DECLARED_BYTES)
    await instance.store.insert(row)
    granted = await instance.saturation.admit(_DECLARED_BYTES)
    assert granted.__class__.__name__ == "AdmissionGranted", granted
    return row


async def test_cancel_of_queued_row_releases_its_saturation_slot(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Cancelling a queued upload must return the gate to idle.

    Attack: admit a queued row (the gate now holds one slot +
    ``_DECLARED_BYTES``), then drive the REAL ``cancel_upload`` route.
    The row leaves the in-flight set (``cancelled`` is terminal; the
    sender never claims it), so its slot must be released. Today the
    pure ``store.cancel`` leaves the gate charged forever.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    row = await _seed_in_flight_row(instance, make_upload_row, state="queued")

    await admin_routes.cancel_upload(row.chain_id, dispatcher)

    assert instance.saturation.in_flight == 0, (
        "cancelling a queued row leaked its saturation slot: in_flight is "
        f"{instance.saturation.in_flight}, not 0 - the gate will 503-refuse "
        "fresh ingress sooner than its real capacity warrants"
    )
    assert instance.saturation.in_flight_bytes == 0, (
        f"cancelling a queued row leaked {instance.saturation.in_flight_bytes} "
        "in-flight bytes that correspond to no live row"
    )


async def test_bulk_delete_of_stored_row_releases_its_saturation_slot(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Bulk-deleting a stored upload must return the gate to idle.

    Attack: admit a ``stored`` row (which DELIBERATELY keeps its slot -
    the body still occupies space), then drive the REAL
    ``bulk_delete_uploads`` route with ``state=stored`` (the routine
    "clear the stuck uploads" cleanup). The row and its body are gone, so
    the slot it was holding must be released. Today the pure
    ``store.bulk_delete`` + body delete leaves the gate charged.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    await _seed_in_flight_row(instance, make_upload_row, state="stored")

    response = await admin_routes.bulk_delete_uploads(DeleteFilter(state="stored"), dispatcher)
    assert response.deleted == 1, "precondition: the stored row was deleted"

    assert instance.saturation.in_flight == 0, (
        "bulk-deleting a stored row leaked its saturation slot: in_flight is "
        f"{instance.saturation.in_flight}, not 0"
    )
    assert instance.saturation.in_flight_bytes == 0, (
        f"bulk-deleting a stored row leaked {instance.saturation.in_flight_bytes} "
        "in-flight bytes that correspond to no live row"
    )


async def test_cancel_of_attempting_row_releases_the_slot_on_some_side(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Cancelling a row mid-attempt must not leak the slot on both ends.

    Attack: admit an ``attempting`` row, cancel it via the REAL route
    (the row the sender is driving), THEN run the sender's success
    handler - whose ``record_attempt_result`` no-ops (state is now
    ``cancelled``) and deliberately skips release, deferring to the
    cancel path. The cancel path never released either, so the slot is
    owned by nobody. After the dust settles the gate must be idle; today
    it permanently holds the slot.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    row = await _seed_in_flight_row(instance, make_upload_row, state="attempting")

    # Admin cancels the row the sender is driving.
    await admin_routes.cancel_upload(row.chain_id, dispatcher)
    # The sender finishes its in-flight step; record_attempt_result
    # no-ops (state no longer 'attempting') and skips release per
    # M-W4-F7, expecting the cancel path to have owned it.
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    cancelled = await instance.store.get(row.chain_id)
    assert cancelled is not None
    await sender._on_succeeded(
        instance.store,
        cancelled,
        Succeeded(
            captured=CapturedValues(),
            next_step_index=1,
            chain_done=True,
            step_name="s",
            upstream_status=200,
            upstream_headers={},
        ),
    )

    assert instance.saturation.in_flight == 0, (
        "an admin-cancelled attempting row leaked its slot on BOTH ends: the "
        "sender's M-W4-F7 no-op skipped release and the cancel route never "
        f"released either, so in_flight is {instance.saturation.in_flight}"
    )
    assert instance.saturation.in_flight_bytes == 0, (
        f"the cancelled attempting row leaked {instance.saturation.in_flight_bytes} "
        "in-flight bytes owned by no live row"
    )
