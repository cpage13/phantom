"""The kicker rescan's fault posture, its work bound, and its inertness.

Four findings from the full-coverage review share this loop, and each has its
own test below.

S7-5, the swallowed fault. ``Kicker.run`` wrapped the whole rescan in a bare
``except Exception`` that logged a traceback and never re-raised, so a
PERMANENT fault (a schema-class ``OperationalError`` after a partial schema
change, an ``OSError`` from failing storage, any bug raising out of
``_rescan``) was retried at 1 Hz forever. The service stayed up in a state
where no parked row could ever wake, the operator got one traceback per second
instead of a restart, and no supervisor-visible signal was produced. The
sibling ``Sender._worker_loop`` re-raises a non-transient
``OperationalError`` for exactly this reason.

S7-6, the unbounded pass. Each rescan fetched the ENTIRE parked backlog, which
nothing bounds, and walked it with a synchronous ``resolve_route`` per row and
no await on the non-matching path. A credential outage parking 100 000 rows had
each of two kicker flavours run a 100 000-iteration uninterruptible block once
per second, stalling ingress admission and every other worker.

S7-4, the log flood. An un-routable parked row can never wake and never leaves
the table, and BOTH flavours warned about it on every tick: two WARNING lines
per second per row, roughly 63 million lines per row per year.

E6, the inertness that never held. ``CredentialStoreOracle.configured`` gated
on ``store is not None``, but the composition root constructs a credential
store for every instance unconditionally, so the sigv4 flavour was never inert
and scanned the parked backlog every second on a bearer-only deployment purely
to discard every row on the auth_mode guard.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

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
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.interface import ParkedCandidate
from phantom.workers.kicker import (
    _RESCAN_PAGE_SIZE,
    AWS_SIGV4_FLAVOUR,
    PHANTOM_BEARER_FLAVOUR,
    Kicker,
)
from phantom.workers.saturation import AdmissionGranted, SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started

pytestmark = pytest.mark.asyncio

_ROUTED_HOST = "files.example.com"
_UNROUTABLE_HOST = "nowhere.invalid"
_UID = "user-1"

# One row more than a page, so a single pass provably cannot finish the walk.
_OVER_A_PAGE = _RESCAN_PAGE_SIZE + 3

# Un-routable rows the log-volume test inserts. More than one, because the
# assertion is that the pass emits ONE line for all of them rather than one
# line each.
_UNROUTABLE_ROWS = 3

# How long the fault test lets ``run`` sit before calling it swallowed. The
# loop's own tick is one second and the fault fires on the FIRST pass, so a
# posture that re-raises returns in milliseconds; this only has to be longer
# than that and shorter than the suite's patience.
_FAULT_PROPAGATION_BUDGET_SECONDS = 3.0

# Roomy enough that nothing is refused for a reason a test did not ask for.
_ROOMY_ROWS = 10_000
_ROOMY_BYTES = 1_000_000_000


async def _build_instance(
    tmp_path: Path,
    *,
    routes: list[RouteCfg],
    saturation: SaturationGate | None = None,
    with_signer_creds: bool = False,
) -> InstanceContext:
    """A real-store instance whose route table the test chooses."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    signer_creds: SqliteCredentialStore | None = None
    if with_signer_creds:
        signer_creds = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
        await signer_creds.start()
        track_started(signer_creds)
    instance = InstanceContext(
        cfg=InstanceCfg(
            id="primary",
            host_prefixes=["*"],
            data_dir="primary",
            routes=routes,
        ),
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
        saturation=saturation
        or SaturationGate(
            max_in_flight=_ROOMY_ROWS,
            max_in_flight_bytes=_ROOMY_BYTES,
            max_disk_bytes=_ROOMY_BYTES,
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
        signer_creds=signer_creds,
    )
    return track_instance(instance)


def _parked_row(*, endpoint: str, received_at: datetime, idempotency_key: str) -> UploadRow:
    """One ``auth_expired`` row blocked on ``endpoint``."""
    chain_id = uuid4()
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=None,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=1,
        auth_blocked_host=endpoint,
        received_at=received_at,
        updated_at=received_at,
        endpoint=endpoint,
        uid=_UID,
        chain_envelope_json="{}",
        idempotency_key=idempotency_key,
        capture_reexecution_active=False,
    )


class _RaisingScanStore:
    """A store whose parked-row scan raises whatever the test hands it.

    Delegates everything else to the real store. ``list_parked_candidates``
    takes ``**kwargs`` so the fake is indifferent to the scan's signature and
    the test measures BEHAVIOUR rather than a call shape.
    """

    def __init__(self, real: SqliteUploadStore, fault: Exception) -> None:
        self._real = real
        self._fault = fault
        self.calls = 0

    def __getattr__(self, name: str) -> Any:
        """Delegate every untouched store method to the real store."""
        return getattr(self._real, name)

    async def list_parked_candidates(self, **kwargs: Any) -> list[ParkedCandidate]:
        """Count the scan and raise the configured fault."""
        del kwargs
        self.calls += 1
        raise self._fault


class _CountingScanStore:
    """A store that counts parked-row scans and otherwise delegates."""

    def __init__(self, real: SqliteUploadStore) -> None:
        self._real = real
        self.calls = 0

    def __getattr__(self, name: str) -> Any:
        """Delegate every untouched store method to the real store."""
        return getattr(self._real, name)

    async def list_parked_candidates(self, **kwargs: Any) -> list[ParkedCandidate]:
        """Count the scan, then delegate it."""
        self.calls += 1
        result: list[ParkedCandidate] = await self._real.list_parked_candidates(**kwargs)
        return result


async def test_a_permanent_rescan_fault_reaches_supervision(tmp_path: Path) -> None:
    """Objective: a non-transient storage fault escapes ``run`` instead of looping.

    Expected: ``run`` raises the ``OperationalError`` out to its TaskGroup, so
    the CLI's fatal-worker bridge sees it and the supervisor restarts the
    process. Before the fix the bare ``except Exception`` swallowed it and the
    loop kept scanning at 1 Hz forever, which is why this test asserts on the
    exception escaping rather than on a log line: a traceback per second is
    exactly what the defect produced.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="phantom_bearer")],
    )
    faulting = _RaisingScanStore(
        instance.store,  # type: ignore[arg-type]
        sqlite3.OperationalError("no such column: auth_blocked_host"),
    )
    instance.store = faulting  # type: ignore[assignment]
    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)

    with pytest.raises(sqlite3.OperationalError):
        await asyncio.wait_for(
            kicker.run(asyncio.Event()),
            timeout=_FAULT_PROPAGATION_BUDGET_SECONDS,
        )


async def test_a_transient_lock_is_ridden_out_rather_than_fatal(tmp_path: Path) -> None:
    """Objective: classified contention keeps the same ride-it-out posture.

    Expected: a ``database is locked`` error does NOT escape ``run``; the loop
    logs it and scans again on the next tick, exactly as
    ``Sender._worker_loop`` treats the same classification (ADR-023). This is
    the other half of the fault posture, here so the S7-5 fix cannot be
    over-applied into a crash on ordinary contention.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="phantom_bearer")],
    )
    faulting = _RaisingScanStore(
        instance.store,  # type: ignore[arg-type]
        sqlite3.OperationalError("database is locked"),
    )
    instance.store = faulting  # type: ignore[assignment]
    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    stop_event = asyncio.Event()

    task = asyncio.create_task(kicker.run(stop_event))
    kicker._wake_event.set()
    while faulting.calls < 1:
        await asyncio.sleep(0)
    stop_event.set()
    kicker._wake_event.set()
    await asyncio.wait_for(task, timeout=_FAULT_PROPAGATION_BUDGET_SECONDS)

    assert faulting.calls >= 1, "the loop reached the scan"


async def test_one_pass_walks_at_most_a_page_and_the_next_resumes(tmp_path: Path) -> None:
    """Objective: a rescan pass is O(page), not O(backlog), and still sweeps it all.

    Expected: with one page plus three parked rows and a saturated gate, the
    first pass reaches exactly ``_RESCAN_PAGE_SIZE`` rows and the second
    reaches the remaining three, and between them every row is reached exactly
    once. Before the fix the first pass reached all of them, which is the
    uninterruptible O(backlog) block the finding is about; at the 100 000-row
    backlog the finding describes, that block ran twice a second.

    The gate is deliberately pre-saturated so no row is actually woken: the
    refusal log line is the per-row observable, and leaving the rows parked is
    what makes the second pass's page the NEXT rows rather than the same ones.
    """
    # Saturated on the ROW cap alone, with room everywhere else, so every
    # refusal below is the one the test asked for.
    gate = SaturationGate(
        max_in_flight=1,
        max_in_flight_bytes=_ROOMY_BYTES,
        max_disk_bytes=_ROOMY_BYTES,
    )
    instance = await _build_instance(
        tmp_path,
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="phantom_bearer")],
        saturation=gate,
    )
    assert isinstance(await gate.admit(1), AdmissionGranted), "gate pre-saturated on purpose"

    base = datetime.now(tz=UTC)
    inserted: list[UUID] = []
    for i in range(_OVER_A_PAGE):
        # Distinct, increasing ``received_at``: the walk's declared order is
        # oldest-first, so a tie across the page boundary would make the test
        # depend on the tiebreak rather than on the page.
        row = _parked_row(
            endpoint=_ROUTED_HOST,
            received_at=base + timedelta(milliseconds=i),
            idempotency_key=f"k{i}",
        )
        await instance.store.insert(row)
        inserted.append(row.chain_id)
    await instance.token_cache.set(_ROUTED_HOST, _UID, "Bearer fresh", source="inbound_request")

    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    first = await _refused_chain_ids(kicker, instance)
    second = await _refused_chain_ids(kicker, instance)

    assert len(first) == _RESCAN_PAGE_SIZE, "the first pass is bounded at one page"
    assert len(second) == _OVER_A_PAGE - _RESCAN_PAGE_SIZE, "the second pass resumes after it"
    assert first + second == inserted, "the two passes sweep every row exactly once, in order"


async def _refused_chain_ids(kicker: Kicker, instance: InstanceContext) -> list[UUID]:
    """Run one rescan and return the chain_ids whose wake the gate refused.

    The refusal WARNING is the only per-row observable on a saturated gate, and
    it names the row, so the caller can assert on WHICH rows a pass reached and
    not merely how many.
    """
    caplog = logging.getLogger("phantom.workers.kicker")
    records: list[logging.LogRecord] = []
    handler = _Collector(records)
    caplog.addHandler(handler)
    try:
        await kicker._rescan()
    finally:
        caplog.removeHandler(handler)
    del instance
    return [
        UUID(str(record.args[1]))
        for record in records
        if isinstance(record.args, tuple) and "refused wake" in record.msg
    ]


class _Collector(logging.Handler):
    """Append every emitted record to a list, un-formatted."""

    def __init__(self, sink: list[logging.LogRecord]) -> None:
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        """Collect the record verbatim."""
        self._sink.append(record)


async def test_unroutable_rows_are_reported_once_per_pass_by_one_flavour(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Objective: a permanently un-wakeable row costs ONE log line, not two per row.

    Expected: three parked rows whose blocked host matches no route produce
    exactly ONE warning from the bearer flavour and NONE from the sigv4
    flavour, even though both flavours are live on this instance and both walk
    the same candidate list. Before the fix each flavour warned per row per
    tick: six lines per second for these three rows, roughly 63 million per row
    per year.

    The rows are left parked either way. The ADR-032 backstop still cannot
    reach them, because the send-deadline is a per-ROUTE field and these rows
    have no route; what this pins is the volume, not a new disposition.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="bearer", hosts=[_ROUTED_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="sigv4", hosts=["s3.example.com"], auth_mode="aws_sigv4"),
        ],
        with_signer_creds=True,
    )
    base = datetime.now(tz=UTC)
    parked: list[UUID] = []
    for i in range(_UNROUTABLE_ROWS):
        row = _parked_row(
            endpoint=_UNROUTABLE_HOST,
            received_at=base + timedelta(milliseconds=i),
            idempotency_key=f"k{i}",
        )
        await instance.store.insert(row)
        parked.append(row.chain_id)

    bearer = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    sigv4 = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)

    with caplog.at_level(logging.WARNING, logger="phantom.workers.kicker"):
        await bearer._rescan()
        await sigv4._rescan()

    # Every WARNING that names one of these rows, whatever its wording: the
    # measurement is the VOLUME one tick costs, not one message's text. Before
    # the fix this was one record per row per flavour, six for this scenario.
    naming_a_parked_row = [
        record
        for record in caplog.records
        if any(str(chain_id) in record.getMessage() for chain_id in parked)
    ]
    assert len(naming_a_parked_row) == 1, (
        "one line per tick for the whole page, from the one flavour that owns "
        f"the report; got {[r.getMessage() for r in naming_a_parked_row]}"
    )
    message = naming_a_parked_row[0].getMessage()
    assert message.startswith("AuthKicker:"), "the sigv4 flavour reports none of them"
    assert f"{_UNROUTABLE_ROWS} parked row(s)" in message, "the line carries the full count"


async def test_the_sigv4_flavour_is_inert_without_an_aws_sigv4_route(tmp_path: Path) -> None:
    """Objective: a bearer-only deployment does not scan for the sigv4 flavour.

    Expected: the sigv4 kicker issues ZERO parked-row scans on an instance that
    declares no ``aws_sigv4`` route, even though a credential store IS wired
    (which is what the composition root always does, and what made the
    documented inertness never hold). The bearer kicker on the same instance
    still scans, so the assertion is about the route gate rather than about the
    kicker being broken.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[RouteCfg(name="bearer", hosts=[_ROUTED_HOST], auth_mode="phantom_bearer")],
        with_signer_creds=True,
    )
    assert instance.signer_creds is not None, "the store IS wired; that is the point"
    counting = _CountingScanStore(instance.store)  # type: ignore[arg-type]
    instance.store = counting  # type: ignore[assignment]

    await Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)._rescan()
    assert counting.calls == 0, "no aws_sigv4 route, so the flavour reads nothing"

    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()
    assert counting.calls == 1, "the flavour that owns a route still scans"
