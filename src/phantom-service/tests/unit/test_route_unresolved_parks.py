"""F1: an unroutable step is CLASSIFIED, and the sender parks the row in ``stored``.

``phantom.routing.resolve_route`` raises ``ValueError`` when no configured
``RouteCfg`` host pattern matches. Admission route-checks only the FIRST step's
URL and swallows a miss on it, so a chain whose LATER step targets an unrouted
host is durably admitted with a 202. Before F1 the miss surfaced at send time as
a bare ``ValueError`` out of ``execute_one_step``; the sender's worker loop
catches only ``sqlite3.OperationalError``, so the exception cancelled the sender
TaskGroup and (in production) stopped the process. Recovery re-claimed the same
row first on every restart, so one row crash-looped the whole service.

F1 classifies the miss as :class:`RouteUnresolved` and the sender routes it to
terminal ``stored``: body retained, saturation slot retained, never re-claimed,
replay-eligible once an operator repairs the route config.

**The envelope shape here is load-bearing, not incidental.** Three adaptations of
the house two-step template are each required for the guard to fire at all:

1. The SECOND step points at ``unrouted.example.com``; the first stays on
   ``files.example.com`` so only the later step misses.
2. The second step's body is INLINE JSON rather than a ``body_ref``.
   ``_render_body`` runs at ``chain/executor.py`` BEFORE the ``resolve_route``
   guard, and its ``ChainBodyRef`` arm returns not-ok when the named ref is
   absent, so ``execute_one_step`` would return ``TemplateUnresolved`` and the
   guard would never be reached. Do not put a ``body_ref`` back here.
3. The row is built with ``current_step_index=1``. At index 0 the executor runs
   the ``files.example.com`` step, which resolves cleanly.

With those three the row needs no ``body_refs`` and no declared ``body_hashes``:
``_load_body_refs`` returns ``{}`` immediately for a row with empty
``body_hashes``, and the inline body renders without refs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor, RouteUnresolved
from phantom.chain.parser import parse_json_request
from phantom.config.settings import InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# The routed host, which the instance's single RouteCfg matches.
ROUTED_HOST = "files.example.com"

# The host no RouteCfg matches, carried by the chain's SECOND step.
UNROUTED_HOST = "unrouted.example.com"

# The unroutable step's name, which the classification reports back and the
# ``last_error`` token embeds.
UNROUTED_STEP_NAME = "put_unrouted"

# Declared body size for the saturation test. The driven row carries no body and
# ``body_size_bytes`` defaults to 0, so the slot assertion needs an explicit
# non-zero declaration to be meaningful.
_DECLARED_BYTES = 1024

# A bare-path step URL carrying a presigned-style query string. The sanitisation
# test proves none of it can reach ``last_error``.
_SECRET_PATH_URL = "/v1/files/x?sig=SECRET"


class _FakeUpstream:
    """Stub upstream client. No test here reaches the transport."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a real-store instance whose single route matches ``files.example.com`` only.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The tracked :class:`InstanceContext`.
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
        host_prefixes=[ROUTED_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="files", hosts=[ROUTED_HOST], auth_mode="none")],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
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
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    return track_instance(instance)


def _envelope_json(chain_id: UUID, *, second_step_url: str) -> bytes:
    """Build a two-step envelope whose second step targets ``second_step_url``.

    Both steps carry inline bodies so ``_render_body`` resolves without any
    ``body_refs``, which is what lets control reach the ``resolve_route`` guard.

    Args:
        chain_id: The chain's identity.
        second_step_url: The URL for the second (unroutable) step.

    Returns:
        The envelope as JSON bytes, ready for ``parse_json_request``.
    """
    return (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"create_file","method":"POST","url":"https://'
        + ROUTED_HOST.encode()
        + b'/v2/files",'
        + b'"body":{"kind":"json","value":{"name":"f"}}},'
        + b'{"name":"'
        + UNROUTED_STEP_NAME.encode()
        + b'","method":"PUT","url":"'
        + second_step_url.encode()
        + b'","body":{"kind":"json","value":{"k":"v"}}}'
        + b"]}"
    )


async def _row(
    chain_id: UUID,
    *,
    second_step_url: str,
    step_index: int = 1,
    body_size_bytes: int = 0,
) -> UploadRow:
    """Build an ``attempting`` row whose persisted envelope drives ``step_index``.

    Args:
        chain_id: The chain's identity and the row's primary key.
        second_step_url: The URL for the second step.
        step_index: Which step the executor will run. Defaults to the second.
        body_size_bytes: Declared body size, for the saturation assertion.

    Returns:
        The persisted-shape :class:`UploadRow`. The envelope carries no
        ``default_target``, so a bare-path step URL stays a bare path and the
        sanitisation test's hostless case is reachable.
    """
    envelope, _ = await parse_json_request(
        _envelope_json(chain_id, second_step_url=second_step_url),
        instance_id="primary",
        request_id="r",
        max_buffered_bytes=10_000,
    )
    now = datetime.now(tz=UTC)
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="files",
        state="attempting",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint=ROUTED_HOST,
        uid="user-1",
        chain_envelope_json=envelope.model_dump_json(),
        current_step_index=step_index,
        idempotency_key="k",
        capture_reexecution_active=False,
        body_size_bytes=body_size_bytes,
    )


async def test_executor_returns_route_unresolved_for_unmatched_host(tmp_path: Path) -> None:
    """An unmatched host is CLASSIFIED, not raised.

    Objective: ``resolve_route``'s ``ValueError`` never escapes
    ``execute_one_step``. Success: the call returns a :class:`RouteUnresolved`
    naming the unmatched host and the step that carried it.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(chain_id, second_step_url=f"https://{UNROUTED_HOST}/v1/upload")

    result = await instance.executor.execute_one_step(row, {})

    assert isinstance(result, RouteUnresolved), (
        f"an unmatched host must classify as RouteUnresolved; got {type(result).__name__}"
    )
    assert result.host == UNROUTED_HOST
    assert result.step_name == UNROUTED_STEP_NAME


async def test_drive_one_parks_unroutable_row_in_stored(tmp_path: Path) -> None:
    """The sender resolves the claimed row instead of letting the exception escape.

    Objective: ``_drive_one`` returns normally and the row lands in terminal
    ``stored``. Success: the re-read row is ``stored`` with the
    ``route_unresolved:`` token, unchanged attempts (the row never reached the
    upstream), and no scheduled retry.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(chain_id, second_step_url=f"https://{UNROUTED_HOST}/v1/upload")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "stored", f"an unroutable row must park in stored; got {fresh.state!r}"
    assert fresh.last_error == f"route_unresolved:{UNROUTED_HOST}:{UNROUTED_STEP_NAME}"
    assert fresh.attempts == row.attempts, (
        "the row never reached the upstream, so it burns no retry budget"
    )
    assert fresh.next_attempt_at is None, "stored is terminal; claim_due must never re-claim it"


async def test_unroutable_row_keeps_its_saturation_slot(tmp_path: Path) -> None:
    """The ``stored`` classification leaves the ledger consistent with ``row_holds_slot``.

    Objective: ``stored`` is in ``SLOT_HOLDING_STATES``, so the slot must stay
    held (the body is retained, so the space really is still occupied). Success:
    the gate still reports one in-flight row of the declared size.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(
        chain_id,
        second_step_url=f"https://{UNROUTED_HOST}/v1/upload",
        body_size_bytes=_DECLARED_BYTES,
    )
    await instance.store.insert(row)
    admitted = await instance.saturation.admit(_DECLARED_BYTES)
    assert admitted.__class__.__name__ == "AdmissionGranted", admitted

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    assert instance.saturation.in_flight == 1, "stored holds its slot (SLOT_HOLDING_STATES)"
    assert instance.saturation.in_flight_bytes == _DECLARED_BYTES


async def test_route_unresolved_bumps_the_detection_counter(tmp_path: Path) -> None:
    """The ``route_unresolved_total`` counter is registered and bumps on detection.

    Objective: settle the counter acceptance criterion at unit level, since the
    metrics registry is reachable here and the admin endpoint is not. Success:
    the registry holds the counter and its unlabelled bucket reads 1.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(chain_id, second_step_url=f"https://{UNROUTED_HOST}/v1/upload")
    await instance.store.insert(row)

    registry = MetricsRegistry()
    sender = Sender(
        instance=instance, worker_count=1, poll_interval_ms=250, metrics_registry=registry
    )
    await sender._drive_one(instance.store, row)

    assert "route_unresolved_total" in registry.counters
    assert registry.counters["route_unresolved_total"].snapshot()[""] == 1


async def test_route_present_still_resolves_normally(tmp_path: Path) -> None:
    """Counter-test: the guard must not swallow a route that DOES match.

    Objective: the try/except is narrow enough that a resolvable step keeps its
    normal classification. Success: driving step 0, which targets the routed
    host, produces a non-``RouteUnresolved`` result.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(chain_id, second_step_url=f"https://{UNROUTED_HOST}/v1/upload", step_index=0)

    result = await instance.executor.execute_one_step(row, {})

    assert not isinstance(result, RouteUnresolved), (
        "a step whose host matches a configured route must resolve normally"
    )


async def test_hostless_step_url_never_leaks_the_path_or_query_into_last_error(
    tmp_path: Path,
) -> None:
    """A bare-path step URL yields the fixed ``<no-host>`` token, never the URL.

    Objective: pin the token sanitisation rule. ``ChainStep.url`` may legally be
    a bare path, and ``_absolute_url`` returns it unchanged when the envelope
    carries no ``default_target``, so a naive reuse of ``host_key_for`` (which
    returns the WHOLE INPUT when urlparse finds no host) would splice a
    presigned query string into ``last_error``, which the admin API surfaces
    verbatim.

    Success: the classification's host is the fixed literal, and the persisted
    ``last_error`` contains no fragment of the URL.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = await _row(chain_id, second_step_url=_SECRET_PATH_URL)
    await instance.store.insert(row)

    result = await instance.executor.execute_one_step(row, {})
    assert isinstance(result, RouteUnresolved)
    assert result.host == "<no-host>"

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.last_error == f"route_unresolved:<no-host>:{UNROUTED_STEP_NAME}"
    for leaked in ("?", "sig", "SECRET", "/v1/files"):
        assert leaked not in fresh.last_error, (
            f"last_error must not carry {leaked!r} from the step URL; got {fresh.last_error!r}"
        )


pytestmark = pytest.mark.asyncio
