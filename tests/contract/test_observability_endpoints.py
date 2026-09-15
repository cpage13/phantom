"""Contract tests for the Phase 3 § 4.2.5 observability admin endpoints.

Three new endpoints under ``/v1/admin/observability/``:

* ``GET /counters``     — serialize :class:`MetricsRegistry.counters`
* ``GET /gauges``       — serialize :class:`MetricsRegistry.gauges`,
                         with ``body_location_distribution`` computed
                         on demand from the live store.
* ``GET /ram_pressure`` — aggregated RAM-pressure status across
                         configured instances.

Each test builds a FastAPI app exposing only the admin router, wires
the dispatcher + metrics registry via dependency_overrides, and
exercises the route via :class:`fastapi.testclient.TestClient`.

The fixture constructs an :class:`InvariantAuditor` purely so the
operator-visible invariant counters are registered through the REAL
production registration site. Without a registered counter the counters
endpoint legitimately returns an empty list, and a test written against
an empty list cannot fail (finding S12-7).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
    SaturationCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.observability.metrics import MetricsRegistry
from phantom.routes import admin as admin_routes
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
from phantom.workers.invariant_audit import InvariantAuditor
from phantom.workers.saturation import SaturationGate

# Counters the fixture's :class:`InvariantAuditor` registers. Spelled out
# rather than derived from the registry the endpoint serializes: deriving
# the expectation from the same object under test is what made this file's
# counters assertion unfailable (S12-7). ``invariant_violation_total`` is
# the reliability-invariant surface named in CONTEXT.md.
_EXPECTED_COUNTER_NAMES: frozenset[str] = frozenset(
    {"invariant_violation_total", "invariant_audit_runs_total"}
)

# Label bucket the metrics module uses for an unlabeled value.
_NO_LABEL_BUCKET: str = ""
# Seeded counts, so the contract covers VALUE serialization and not only
# the presence of a name. Arbitrary but distinct, so a transposed or
# dropped bucket is visible in the failure message.
_SEEDED_VIOLATION_LABEL: str = "body_missing"
_SEEDED_VIOLATION_COUNT: int = 3
_SEEDED_AUDIT_RUNS: int = 2


def _make_snapshot(persist_trigger: PersistTriggerCfg) -> InstanceSettingsSnapshot:
    """Local snapshot factory mirroring src/phantom-service/tests/unit/conftest.py.

    The contract test cannot import the per-package conftest, so the
    minimal-yet-validator-satisfying snapshot is built inline.
    """
    return InstanceSettingsSnapshot(
        persist_trigger=persist_trigger,
        body_store=BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
        retention=RetentionCfg(
            succeeded_metadata_seconds=300,
            failed_body_seconds=14 * 86_400,
            auth_expired_body_seconds=60 * 86_400,
            stored_body_seconds=60 * 86_400,
        ),
        compression=CompressionCfg(),
        saturation=SaturationCfg(
            max_in_flight=100,
            max_in_flight_bytes=1_073_741_824,
            max_disk_bytes=137_438_953_472,
            large_body_threshold_bytes=100 * 1024 * 1024,
            max_large_in_flight=4,
        ),
        capture_reexecution=False,
    )


def _snapshot_thunk(snapshot: InstanceSettingsSnapshot) -> Callable[[], InstanceSettingsSnapshot]:
    return lambda: snapshot


class _FakeUpstream:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


@pytest.fixture
async def app_with_observability(
    tmp_path: Path,
) -> Iterable[tuple[FastAPI, MetricsRegistry, InstanceContext]]:
    """Build a minimal admin app with metrics registry + one instance."""
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()

    saturation = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=1_000_000,
        max_disk_bytes=10_000_000,
        metrics_registry=registry,
    )

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
        ],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=default_clock,
        instance=cfg,
    )
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    persist_trigger = PersistTriggerCfg(body_size_threshold_bytes=0)
    snapshot = _make_snapshot(persist_trigger)
    # Registration side effect only: the auditor's sweep loop is never
    # started. Constructing it is how production registers
    # invariant_violation_total, so the endpoint under test serializes a
    # counter surface an operator would actually see.
    _auditor = InvariantAuditor(
        store=store,
        body_store=body_store,
        current_settings=_snapshot_thunk(snapshot),
        metrics_registry=registry,
    )
    # Seed distinct counts so the counters test pins value serialization,
    # including the labeled-bucket shape, not just the set of names.
    await registry.counters["invariant_violation_total"].inc(
        label_value=_SEEDED_VIOLATION_LABEL, n=_SEEDED_VIOLATION_COUNT
    )
    await registry.counters["invariant_audit_runs_total"].inc(n=_SEEDED_AUDIT_RUNS)
    ctx = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,  # all_disk-style: no controller wired
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=_passthrough_factory,
        current_settings=_snapshot_thunk(snapshot),
    )
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(admin_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture observes the same wire shape production does (round 3
    # fix R3-1; this fixture previously registered no handlers at all).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: registry
    yield app, registry, ctx
    # Stop everything this fixture started. Without it the aiosqlite worker
    # threads outlive the test, and the service unit suite's leak tripwire
    # then blames whichever test happens to run next in the same process.
    # The composed body store stops its own halves and every storage stop()
    # is idempotent, so this order is safe.
    await body_store.stop()
    await fbs.stop()
    await ram.stop()
    await tokens.stop()
    await store.stop()


def test_get_observability_counters_returns_registered_counters(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """Objective: the counters endpoint reports every registered counter, with its values.

    Expected outcome: the response names at least the counters the
    fixture's :class:`InvariantAuditor` registered, every entry carries a
    name, a description and a non-empty ``label_value`` to count map, and
    the two seeded buckets come back with exactly the counts they were
    bumped to.

    Finding S12-7: this assertion used to be ``isinstance(names, set)``
    over a set comprehension, which holds unconditionally, and the
    per-entry loop never ran when the list was empty. An endpoint that
    returned ``{"counters": []}`` passed, so a regression dropping the
    operator's entire counter surface, ``invariant_violation_total`` and
    ``db_quarantine_total`` included, shipped green.

    Falsifier: drop the counters from the registry, or have the route
    return an empty list, and this goes RED on the missing-names
    assertion.
    """
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/counters")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "counters" in body
    entries = {entry["name"]: entry for entry in body["counters"]}
    missing = _EXPECTED_COUNTER_NAMES - set(entries)
    assert not missing, (
        f"registered counters absent from the endpoint: {sorted(missing)}; "
        f"endpoint returned {sorted(entries)}"
    )
    for name, entry in entries.items():
        assert entry["description"], f"counter {name} serialized an empty description"
        assert isinstance(entry["values"], dict)
        assert entry["values"], f"counter {name} serialized no label buckets"
    # Values, not just names: the labeled bucket and the no-label bucket
    # must both survive the round trip with their exact counts.
    assert entries["invariant_violation_total"]["values"] == {
        _NO_LABEL_BUCKET: 0,
        _SEEDED_VIOLATION_LABEL: _SEEDED_VIOLATION_COUNT,
    }
    assert entries["invariant_audit_runs_total"]["values"] == {_NO_LABEL_BUCKET: _SEEDED_AUDIT_RUNS}


def test_get_observability_gauges_returns_registered_gauges(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """``GET /observability/gauges`` returns every registered gauge."""
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/gauges")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "gauges" in body
    names = {entry["name"] for entry in body["gauges"]}
    # saturation_balance is registered by the SaturationGate in the
    # fixture; body_location_distribution is registered by the store.
    assert "saturation_balance" in names
    assert "body_location_distribution" in names
    # body_location_distribution should expose ram + file buckets
    # populated by the on-demand SQL grouping (zero in a fresh fixture).
    bl_entry = next(g for g in body["gauges"] if g["name"] == "body_location_distribution")
    assert "ram" in bl_entry["values"]
    assert "file" in bl_entry["values"]
    assert bl_entry["values"]["ram"] == 0
    assert bl_entry["values"]["file"] == 0


def test_get_observability_ram_pressure_returns_status(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """``GET /observability/ram_pressure`` returns the aggregated status."""
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/ram_pressure")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "ram_body_store_bytes" in body
    assert "ram_ceiling_bytes" in body
    assert "pending_migrations" in body
    assert "persist_controller_queue_depth" in body
    assert body["ram_body_store_bytes"] == 0
    # No persist_controller in this fixture → queue depth = 0.
    assert body["persist_controller_queue_depth"] == 0
