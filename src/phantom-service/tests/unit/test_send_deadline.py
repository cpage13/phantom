"""Unit tests for the send-deadline (ADR-032) — the two transition sites → ``expired``.

Phase 3 TASK 3.2. The send-deadline is a per-route ``RouteCfg.send_deadline_seconds``
ceiling (max wall-time a buffered upload may keep trying) enforced at TWO sites,
both routing through the shared ``expire_row`` writer (``workers/_expire.py``):

* **Path A — the executor give-up gate (belt).** A claimed/``attempting`` row past
  its deadline → ``ChainExecutor`` returns a ``SendDeadlineExpired`` result →
  the sender's ``_on_send_deadline_expired`` arm → ``expire_row`` → ``expired``.
  Strategy-agnostic (``fixed_intervals`` discards ``since_received``), so the gate
  holds regardless of the retry strategy.
* **Path B — the kicker parked-row sweep (suspenders).** A row PARKED in
  ``auth_expired`` (which the executor gate never sees, since the sender only
  claims ``queued`` rows) past its deadline → the kicker's ``_rescan`` sweep →
  ``expire_row`` → ``expired``. Both the sigv4 ``Kicker`` (``aws_sigv4``) and
  the bearer ``Kicker`` (``phantom_bearer``) carry the symmetric backstop.

Also pins the LOUD dispatch guard: the sender's ``isinstance`` result chain has
no static ``assert_never``, so a forgotten arm would silently wedge the row;
the explicit fall-through tail makes an unhandled result crash loudly instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import Failed5xx, SendDeadlineExpired
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.credential import HostCredKey
from phantom.models.upload import UploadRow
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.kicker import AWS_SIGV4_FLAVOUR, PHANTOM_BEARER_FLAVOUR, Kicker
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedSaturation,
    SaturationGate,
)
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started
from .test_executor import FakeTokenCache, FakeUpstreamClient, _instance, _row

# Distinct hosts so the route table maps each to a different auth_mode.
_SIGV4_HOST = "s3.us-east-1.amazonaws.com"
_BEARER_HOST = "files.example.com"


# ====================================================================
# Path A — the executor give-up gate produces SendDeadlineExpired.
# ====================================================================


@pytest.mark.asyncio
async def test_executor_gate_returns_send_deadline_expired_when_over_deadline() -> None:
    """A row older than its route's ``send_deadline_seconds`` → SendDeadlineExpired."""
    from phantom.chain.executor import ChainExecutor

    chain_id = uuid4()
    instance = _instance(
        [
            RouteCfg(
                name="files",
                hosts=["files.example.com"],
                auth_mode="none",
                send_deadline_seconds=60,
            ),
        ]
    )
    row = await _row(chain_id)
    # received_at is two hours old; the clock is frozen one hour after that —
    # 3600 s elapsed > the 60 s deadline.
    received = datetime.now(tz=UTC) - timedelta(hours=2)
    row = row.model_copy(update={"received_at": received})
    now = received + timedelta(hours=1)

    executor = ChainExecutor(
        token_cache=FakeTokenCache(),
        upstream_client=FakeUpstreamClient(),
        resolve_route=resolve_route,
        clock=lambda: now,
        instance=instance,
    )

    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, SendDeadlineExpired)
    assert result.deadline_seconds == 60


@pytest.mark.asyncio
async def test_executor_gate_silent_when_within_deadline() -> None:
    """A row still inside its deadline is NOT classified SendDeadlineExpired."""
    from phantom.chain.executor import ChainExecutor

    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/upload"}')
    instance = _instance(
        [
            RouteCfg(
                name="files",
                hosts=["files.example.com"],
                auth_mode="none",
                send_deadline_seconds=3600,
            ),
        ]
    )
    row = await _row(chain_id)
    # Frozen at admission time → 0 s elapsed, well under the 3600 s deadline.
    now = row.received_at
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: now,
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert not isinstance(result, SendDeadlineExpired)


@pytest.mark.asyncio
async def test_executor_gate_silent_when_no_deadline_configured() -> None:
    """``send_deadline_seconds=None`` (the default) never fires the gate, however old the row."""
    from phantom.chain.executor import ChainExecutor

    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/upload"}')
    instance = _instance([RouteCfg(name="files", hosts=["files.example.com"], auth_mode="none")])
    row = await _row(chain_id)
    row = row.model_copy(update={"received_at": datetime.now(tz=UTC) - timedelta(days=30)})
    now = datetime.now(tz=UTC)
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: now,
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert not isinstance(result, SendDeadlineExpired)


# ====================================================================
# Path A — SendDeadlineExpired ROUTES through the sender to expire_row.
# ====================================================================


async def _build_sender_instance(
    tmp_path: Path, *, executor: object | None = None
) -> InstanceContext:
    """A real-store instance for driving the sender's result dispatch."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="none")],
    )
    sat = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=1_000_000_000
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
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=MagicMock(),
        executor=executor if executor is not None else MagicMock(),
        saturation=sat,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


class _FakeExecutorReturning:
    """An executor stub whose ``execute_one_step`` always returns ``result``."""

    def __init__(self, result: object) -> None:
        self._result = result

    async def execute_one_step(self, row: UploadRow, body_refs: dict[str, bytes]) -> object:
        return self._result


@pytest.mark.asyncio
async def test_send_deadline_expired_routes_through_dispatch_to_expired(
    tmp_path: Path, make_upload_row
) -> None:
    """The ``SendDeadlineExpired`` dispatch ARM drives a claimed row to ``expired``.

    Drives the real ``_drive_one`` dispatch with an executor that returns
    ``SendDeadlineExpired`` (no static ``assert_never`` enforces the arm — this is
    the REAL routing guarantee, not just that the writer exists). Asserts the row
    flips to the terminal ``expired`` state, the body is released, and ``last_error``
    carries the ``send_deadline:`` token.
    """
    fake_executor = _FakeExecutorReturning(SendDeadlineExpired(deadline_seconds=42))
    instance = await _build_sender_instance(tmp_path, executor=fake_executor)
    # A claimed (attempting) row with no body_hashes — _load_body_refs returns {}.
    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=500)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.last_error == "send_deadline:42s"
    assert fresh.next_attempt_at is None
    # Body discarded on the transition (the inverse of `stored`).
    assert fresh.body_discarded_at is not None
    assert fresh.body_size_bytes == 0


@pytest.mark.asyncio
async def test_on_send_deadline_expired_handler_lands_expired(
    tmp_path: Path, make_upload_row
) -> None:
    """The ``_on_send_deadline_expired`` handler itself writes the ``expired`` transition."""
    instance = await _build_sender_instance(tmp_path)
    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=500)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_send_deadline_expired(
        instance.store, row, SendDeadlineExpired(deadline_seconds=120)
    )

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.last_error == "send_deadline:120s"


@pytest.mark.asyncio
async def test_dispatch_loud_tail_crashes_on_unhandled_result(
    tmp_path: Path, make_upload_row
) -> None:
    """An unhandled ``ExecuteStepResult`` member CRASHES loudly (no silent wedge).

    The sender's dispatch is a fall-through ``isinstance`` chain with no static
    ``assert_never``. The explicit tail converts a forgotten arm from a silent
    return-None (row wedged in ``attempting`` holding a slot) into a loud
    ``AssertionError``. Feed a result no arm handles and assert it raises.
    """

    class _UnknownResult:
        pass

    fake_executor = _FakeExecutorReturning(_UnknownResult())
    instance = await _build_sender_instance(tmp_path, executor=fake_executor)
    row = make_upload_row(state="attempting", route_name="files")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    with pytest.raises(AssertionError, match="unhandled ExecuteStepResult"):
        await sender._drive_one(instance.store, row)


# ====================================================================
# Path B — the kicker parked-row sweep.
# ====================================================================


async def _build_kicker_instance(
    tmp_path: Path,
    *,
    send_deadline_seconds: int | None,
    with_signer_creds: bool = True,
) -> tuple[InstanceContext, SqliteCredentialStore | None]:
    """An InstanceContext with an aws_sigv4 + a phantom_bearer route, both carrying a deadline."""
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

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[
            RouteCfg(
                name="sigv4",
                hosts=[_SIGV4_HOST],
                auth_mode="aws_sigv4",
                send_deadline_seconds=send_deadline_seconds,
            ),
            RouteCfg(
                name="bearer",
                hosts=[_BEARER_HOST],
                auth_mode="phantom_bearer",
                send_deadline_seconds=send_deadline_seconds,
            ),
        ],
    )
    sat = SaturationGate(
        max_in_flight=100, max_in_flight_bytes=10_000_000, max_disk_bytes=1_000_000_000
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
        signer_creds=signer_creds,
    )
    return track_instance(instance), signer_creds


def _parked_row(*, endpoint: str, received_at: datetime, body_size_bytes: int = 500) -> UploadRow:
    """An ``auth_expired`` row pinned to ``endpoint`` with a controllable ``received_at``."""
    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    return UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=body_size_bytes,
        received_at=received_at,
        updated_at=now,
        endpoint=endpoint,
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key=str(row_uid),
        capture_reexecution_active=False,
    )


@pytest.mark.asyncio
async def test_credential_kicker_sweeps_over_deadline_parked_row_to_expired(
    tmp_path: Path,
) -> None:
    """A parked ``aws_sigv4`` row past its deadline → ``expired`` via the sweep (NOT re-queued).

    The cred slot is absent (the "re-push never came" case), so the row would
    otherwise park forever. With ``send_deadline_seconds=1`` and a ``received_at``
    well in the past, the sweep fires BEFORE the freshness gate.
    """
    instance, signer_creds = await _build_kicker_instance(tmp_path, send_deadline_seconds=1)
    assert signer_creds is not None  # no cred slot seeded — slot absent on purpose
    received = datetime.now(tz=UTC) - timedelta(hours=1)
    row = _parked_row(endpoint=_SIGV4_HOST, received_at=received)
    await instance.store.insert(row)

    kicker = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)
    await kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"  # swept, NOT woken to "queued"
    assert fresh.last_error == "send_deadline:1s"
    assert fresh.body_discarded_at is not None


@pytest.mark.asyncio
async def test_credential_kicker_does_not_sweep_within_deadline(tmp_path: Path) -> None:
    """A fresh parked row inside its deadline is NOT swept (stays ``auth_expired``)."""
    instance, signer_creds = await _build_kicker_instance(tmp_path, send_deadline_seconds=3600)
    assert signer_creds is not None
    row = _parked_row(endpoint=_SIGV4_HOST, received_at=datetime.now(tz=UTC))
    await instance.store.insert(row)

    kicker = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)
    await kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    # No cred slot is fresh, so it stays parked — neither swept nor woken.
    assert fresh.state == "auth_expired"


@pytest.mark.asyncio
async def test_auth_kicker_sweeps_over_deadline_parked_bearer_row_to_expired(
    tmp_path: Path,
) -> None:
    """Symmetric path B: a parked ``phantom_bearer`` row past its deadline → ``expired``.

    The bearer forever-park hole predates SigV4: a producer that never re-pushes
    a token leaves the row parked indefinitely. The symmetric bearer kicker sweep
    closes it.
    """
    instance, _ = await _build_kicker_instance(
        tmp_path, send_deadline_seconds=1, with_signer_creds=False
    )
    received = datetime.now(tz=UTC) - timedelta(hours=1)
    row = _parked_row(endpoint=_BEARER_HOST, received_at=received)
    await instance.store.insert(row)

    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    await kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.last_error == "send_deadline:1s"


@pytest.mark.asyncio
async def test_kicker_does_not_wake_an_expired_row(tmp_path: Path) -> None:
    """An ``expired`` row is invisible to ``list_non_terminal`` — the kicker never touches it.

    ``expired`` is in ``TERMINAL_STATES`` (TASK 3.1), so ``list_non_terminal``
    (``WHERE state NOT IN (TERMINAL_STATES)``) drops it. A rescan must leave an
    ``expired`` row exactly as it found it.
    """
    instance, signer_creds = await _build_kicker_instance(tmp_path, send_deadline_seconds=1)
    assert signer_creds is not None
    # Seed a fresh cred slot so, were the row visible, the kicker WOULD try to wake it.
    from phantom.models.credential import SigningService, SigV4StaticCreds

    await signer_creds.set(
        HostCredKey(_SIGV4_HOST),
        SigV4StaticCreds(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="secret",
            region="us-east-1",
            service=SigningService.S3,
        ),
        source="admin_push",
    )
    row = _parked_row(endpoint=_SIGV4_HOST, received_at=datetime.now(tz=UTC) - timedelta(hours=1))
    row = row.model_copy(update={"state": "expired"})
    await instance.store.insert(row)

    cred_kicker = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)
    auth_kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    await cred_kicker._rescan()
    await auth_kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"  # untouched — not queued, not re-expired


# ====================================================================
# Path B saturation double-release (Task 3.6 fix). A parked
# ``auth_expired`` row already RELEASED its slot at park
# (``_on_auth_failure``); the kicker sweep that expires it must NOT
# release again, or it transiently UNDER-counts ``in_flight`` and
# over-admits past the cap while another row is concurrently in flight.
# ====================================================================


async def _build_tight_gate_kicker_instance(
    tmp_path: Path,
    *,
    max_in_flight: int,
) -> InstanceContext:
    """A path-B kicker instance whose REAL gate has a tight ``max_in_flight``.

    Mirrors ``_build_kicker_instance`` but parameterises the saturation cap so
    the over-admission boundary is observable: with ``max_in_flight=1`` a single
    genuinely-in-flight row fills the gate, and any spurious release would drop
    ``in_flight`` to 0 and let a fresh admit slip past the cap.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    signer_creds = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await signer_creds.start()
    track_started(signer_creds)
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[
            RouteCfg(
                name="sigv4",
                hosts=[_SIGV4_HOST],
                auth_mode="aws_sigv4",
                send_deadline_seconds=1,
            ),
            RouteCfg(
                name="bearer",
                hosts=[_BEARER_HOST],
                auth_mode="phantom_bearer",
                send_deadline_seconds=1,
            ),
        ],
    )
    sat = SaturationGate(
        max_in_flight=max_in_flight,
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
        signer_creds=signer_creds,
    )
    return track_instance(instance)


@pytest.mark.asyncio
async def test_credential_kicker_sweep_does_not_double_release_slot(tmp_path: Path) -> None:
    """Path B sweep of a parked row must NOT re-release the already-released slot.

    Scenario (the over-admission bug, fixed): one row is genuinely in flight
    (holding the only slot of a ``max_in_flight=1`` gate). A SECOND row sits
    parked in ``auth_expired`` — its slot was released back at park time, so it
    holds NOTHING. When the sigv4 ``Kicker`` sweep expires the parked row,
    a spurious ``release`` would decrement ``in_flight`` from 1 to 0, falsely
    freeing the in-flight row's slot and admitting a third upload past the cap.

    After the fix (``row_holds_slot`` is False for a parked ``auth_expired``
    row, so the sweep releases nothing): the body is still
    discarded, but ``in_flight`` stays at 1 and the gate still refuses a fresh
    admit — no over-admission.
    """
    instance = await _build_tight_gate_kicker_instance(tmp_path, max_in_flight=1)
    gate = instance.saturation

    # One row genuinely in flight: fill the single slot.
    in_flight_grant = await gate.admit(500)
    assert isinstance(in_flight_grant, AdmissionGranted)
    assert gate.in_flight == 1
    assert gate.saturated  # the cap is full; a fresh admit must be refused

    # A SEPARATE row parked in auth_expired — its slot was released at park, so
    # it is NOT reflected in the gate. (We did not admit it; that models the
    # post-park state where _on_auth_failure already released.)
    received = datetime.now(tz=UTC) - timedelta(hours=1)
    parked = _parked_row(endpoint=_SIGV4_HOST, received_at=received)
    await instance.store.insert(parked)

    kicker = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)
    await kicker._rescan()

    fresh = await instance.store.get(parked.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"  # swept to terminal
    assert fresh.body_discarded_at is not None  # body discarded in path B too
    assert fresh.body_size_bytes == 0

    # THE FIX: in_flight is NOT double-decremented — the in-flight row's slot
    # is untouched, the gate is still full, and a fresh admit is still refused.
    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 500
    assert gate.saturated
    refused = await gate.admit(500)
    assert isinstance(refused, AdmissionRefusedSaturation)  # no over-admission past the cap


@pytest.mark.asyncio
async def test_auth_kicker_sweep_does_not_double_release_slot(tmp_path: Path) -> None:
    """Symmetric path B: the ``phantom_bearer`` sweep must not double-release either."""
    instance = await _build_tight_gate_kicker_instance(tmp_path, max_in_flight=1)
    gate = instance.saturation

    in_flight_grant = await gate.admit(500)
    assert isinstance(in_flight_grant, AdmissionGranted)
    assert gate.in_flight == 1

    received = datetime.now(tz=UTC) - timedelta(hours=1)
    parked = _parked_row(endpoint=_BEARER_HOST, received_at=received)
    await instance.store.insert(parked)

    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    await kicker._rescan()

    fresh = await instance.store.get(parked.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.body_discarded_at is not None

    assert gate.in_flight == 1  # the in-flight row's slot survives the sweep
    refused = await gate.admit(500)
    assert isinstance(refused, AdmissionRefusedSaturation)


@pytest.mark.asyncio
async def test_path_a_executor_gate_expiry_still_releases_its_held_slot(
    tmp_path: Path, make_upload_row
) -> None:
    """Path A still releases: an ``attempting`` row HELD a slot, so expiring it frees it.

    The fix only suppresses the path-B re-release; path A's ``attempting`` row
    was admitted and still holds its slot, so ``_on_send_deadline_expired``
    (``row_holds_slot`` is True for an ``attempting`` row) must drop
    ``in_flight`` back to 0.
    """
    instance = await _build_sender_instance(tmp_path)
    gate = instance.saturation
    # Model the admitted, in-flight (attempting) row: it holds a real slot.
    grant = await gate.admit(500)
    assert isinstance(grant, AdmissionGranted)
    assert gate.in_flight == 1

    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=500)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_send_deadline_expired(
        instance.store, row, SendDeadlineExpired(deadline_seconds=30)
    )

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.body_discarded_at is not None
    # Path A released the slot it held: in_flight back to 0.
    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


# Keep the Failed5xx import meaningful: a sanity anchor that the NON-deadline
# retryable arm is unaffected by the new SendDeadlineExpired arm/loud tail.
@pytest.mark.asyncio
async def test_retryable_arm_still_routes_after_new_arm(tmp_path: Path, make_upload_row) -> None:
    """Adding the SendDeadlineExpired arm + loud tail does not break the 5xx retry arm."""
    fake_executor = _FakeExecutorReturning(Failed5xx(status=503))
    instance = await _build_sender_instance(tmp_path, executor=fake_executor)
    row = make_upload_row(state="attempting", route_name="files", attempts=0)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    # 503 is retryable → back to queued (not expired, not crashed).
    assert fresh.state == "queued"
