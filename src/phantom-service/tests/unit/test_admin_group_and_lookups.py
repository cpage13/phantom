"""Cycle-7 phase 4 acceptance: group rollup + either-identifier lookups.

Covers plan section 5 tasks 4.2 (new store methods), 4.3 (per-instance
lookup config), 4.4 (routes), and 4.5 (models), including the named
acceptance legs:

* the F1 regression: a group with one corrupted member and the rest
  succeeded MUST report ``all_finished=true``;
* rollup counts / first_received_at / last_sent_at correct across
  instances and with ``?instance=``;
* lookups: hit, miss-as-found-false, multi-match honesty, and the
  unconfigured-instance 400 for the captured-id axis;
* ``ChainAdminDetail`` carries the seven row-sourced fields.

Two instances are wired: ``alpha`` carries the ``admin_lookup`` binding
(generic emulator-style capture shapes; the binding values are
deployment-supplied), ``beta`` deliberately does not.
"""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    AdminLookupCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
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
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot

# The deployment-supplied binding the alpha instance carries: the
# capturing step's key under the captured-values steps map, and the
# dotted path within its values down to the identifier (generic
# emulator-style shape).
_CAPTURE_NAME = "create_file"
_JSON_PATH = "file_information.id"

_ALL_STATES: tuple[str, ...] = (
    "queued",
    "attempting",
    "succeeded",
    "failed",
    "auth_expired",
    "stored",
    "cancelled",
    "corrupted",
    "expired",
)


class _FakeUpstream:
    """Stub UpstreamClient: these admin tests never call upstream."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(
    tmp_path: Path,
    instance_id: str,
    *,
    admin_lookup: AdminLookupCfg | None,
) -> tuple[InstanceContext, list[InstanceSettingsSnapshot]]:
    """One started instance context rooted under ``tmp_path/instance_id``.

    Returns the context AND the one-element snapshot box its
    ``current_settings`` thunk reads. F5 moved ``admin_lookup`` onto the
    live snapshot, so a test that wants to simulate a reload swaps the
    box's element; there is no ``SettingsHolder`` in this module to swap
    and ``snapshot_thunk`` deliberately wraps a STATIC snapshot. The box
    is constructed here, so it has to come back with the context.
    """
    root = tmp_path / instance_id
    root.mkdir()
    store = SqliteUploadStore(str(root / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(root / "bodies")
    tokens = SqliteTokenCache(str(root / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()
    cfg = InstanceCfg(
        id=instance_id,
        host_prefixes=[f"files-{instance_id}.example.com"],
        data_dir=instance_id,
        routes=[
            RouteCfg(
                name="files",
                hosts=[f"files-{instance_id}.example.com"],
                auth_mode="phantom_bearer",
            ),
        ],
        admin_lookup=admin_lookup,
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=default_clock,
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=1_000_000, max_disk_bytes=10_000_000
    )

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
    snapshot_box: list[InstanceSettingsSnapshot] = [
        make_snapshot(
            persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
            admin_lookup=admin_lookup,
        )
    ]
    ctx = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=_passthrough_factory,
        current_settings=lambda: snapshot_box[0],
    )
    return ctx, snapshot_box


@pytest.fixture
async def lookup_app(
    tmp_path: Path,
) -> Iterable[tuple[FastAPI, InstanceContext, InstanceContext]]:
    """Two-instance admin app: alpha configured for lookups, beta not."""
    alpha, _alpha_box = await _build_instance(
        tmp_path,
        "alpha",
        admin_lookup=AdminLookupCfg(capture_name=_CAPTURE_NAME, json_path=_JSON_PATH),
    )
    beta, _beta_box = await _build_instance(tmp_path, "beta", admin_lookup=None)
    dispatcher = InstanceDispatcher([alpha, beta])
    app = FastAPI()
    app.include_router(admin_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture cannot drift from production app.py (round 3 fix R3-1).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    yield app, alpha, beta
    await alpha.store.stop()
    await beta.store.stop()
    await alpha.token_cache.stop()
    await beta.token_cache.stop()


def _captured_with_file_id(file_id: str) -> CapturedValues:
    """Captured values carrying a generic upstream identifier object."""
    now = datetime.now(tz=UTC)
    return CapturedValues(
        steps={
            _CAPTURE_NAME: CapturedStepValues(
                values={"file_information": {"id": file_id, "size": 42}},
                captured_at=now,
                expires_at={"file_information": None},
            )
        }
    )


def _envelope_with_local_uuid(local_uuid: UUID) -> str:
    """A minimal envelope whose step-0 metadata KVS carries the local uuid."""
    return json.dumps(
        {
            "steps": [
                {
                    "name": _CAPTURE_NAME,
                    "method": "POST",
                    "url": "https://files-alpha.example.com/v2/files",
                    "body": {
                        "kind": "json",
                        "value": {
                            "metadata": {"keyValueStore": {"phantom_local_uuid": str(local_uuid)}}
                        },
                    },
                }
            ]
        }
    )


# ---------------------------------------------------------------------------
# Store methods (task 4.2).
# ---------------------------------------------------------------------------


@pytest.fixture
async def file_store(tmp_path: Path) -> Iterable[SqliteUploadStore]:
    """A started file-backed store (reads run on the dedicated reader)."""
    store = SqliteUploadStore(str(tmp_path / "store.db"))
    await store.start()
    yield store
    await store.stop()


@pytest.mark.asyncio
async def test_store_list_by_group_id_scan(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """list_by_group_id returns exactly the group, receipt-ordered, un-paginated."""
    group = uuid4()
    base = datetime.now(tz=UTC)
    in_group = [
        make_upload_row(group_id=group, received_at=base + timedelta(seconds=i)) for i in range(3)
    ]
    # Insert out of receipt order to prove the ORDER BY.
    for row in (in_group[2], in_group[0], in_group[1]):
        await file_store.insert(row)
    await file_store.insert(make_upload_row())  # different group
    got = await file_store.list_by_group_id(group)
    assert [r.chain_id for r in got] == [r.chain_id for r in in_group]
    assert await file_store.list_by_group_id(uuid4()) == []


@pytest.mark.asyncio
async def test_store_list_by_group_id_never_blends_on_chain_id(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Round 1 adversary (suite hardening): the rollup keys on group_id only.

    group_id and chain_id share one value space (admission defaults
    group_id to chain_id), so an adversary can submit one upload whose
    group_id EQUALS another upload's chain_id. The scan must key on the
    group_id column alone and never blend a row in because its chain_id
    coincides with the queried value. Construct:

    * row A: ``chain_id = shared``, ``group_id = group_a`` (distinct), so
      A's own id collides with the value we will query as a group;
    * row B: ``chain_id = other``, ``group_id = shared`` (the real member
      of the queried group).

    Querying ``shared`` as a group id must return ONLY B; querying
    ``group_a`` must return ONLY A. No blend in either direction.
    """
    shared = uuid4()
    group_a = uuid4()
    other = uuid4()
    row_a = make_upload_row(chain_id=shared, group_id=group_a)
    row_b = make_upload_row(chain_id=other, group_id=shared)
    await file_store.insert(row_a)
    await file_store.insert(row_b)

    by_shared = await file_store.list_by_group_id(shared)
    assert [r.chain_id for r in by_shared] == [other]

    by_group_a = await file_store.list_by_group_id(group_a)
    assert [r.chain_id for r in by_group_a] == [shared]


@pytest.mark.asyncio
async def test_group_rollup_never_blends_foreign_chain_id(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Round 1 adversary (suite hardening): the route rollup never blends.

    The store-level no-blend property surfaced through the HTTP rollup:
    a row whose chain_id coincides with the queried group id but whose
    group_id differs is NOT a member. ``GET /v1/admin/groups/{shared}``
    reports total=1 with only the true member, even though a foreign
    row carries ``shared`` as its chain_id.
    """
    app, alpha, _beta = lookup_app
    shared = uuid4()
    other = uuid4()
    # The decoy: its chain_id IS the queried value, but it lives in a
    # different group.
    await alpha.store.insert(
        make_upload_row(chain_id=shared, group_id=uuid4(), instance_id="alpha")
    )
    # The real lone member of group ``shared``.
    await alpha.store.insert(make_upload_row(chain_id=other, group_id=shared, instance_id="alpha"))
    body = TestClient(app).get(f"/v1/admin/groups/{shared}").json()
    assert body["total"] == 1
    assert body["members"][0]["chain_id"] == str(other)


@pytest.mark.asyncio
async def test_store_list_by_group_id_instance_filter(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The optional instance keyword scopes within the store."""
    group = uuid4()
    await file_store.insert(make_upload_row(group_id=group, instance_id="alpha"))
    await file_store.insert(make_upload_row(group_id=group, instance_id="beta"))
    assert len(await file_store.list_by_group_id(group)) == 2
    scoped = await file_store.list_by_group_id(group, instance="alpha")
    assert [r.instance_id for r in scoped] == ["alpha"]


@pytest.mark.asyncio
async def test_store_find_by_captured_value(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """JSON1 extract over captured_values_json: hit, miss, multi-match."""
    hit_a = make_upload_row(captured_values=_captured_with_file_id("FILE-123"))
    hit_b = make_upload_row(captured_values=_captured_with_file_id("FILE-123"))
    other = make_upload_row(captured_values=_captured_with_file_id("FILE-999"))
    uncaptured = make_upload_row()
    for row in (hit_a, hit_b, other, uncaptured):
        await file_store.insert(row)
    got = await file_store.find_by_captured_value(_CAPTURE_NAME, _JSON_PATH, "FILE-123")
    assert {r.chain_id for r in got} == {hit_a.chain_id, hit_b.chain_id}
    assert await file_store.find_by_captured_value(_CAPTURE_NAME, _JSON_PATH, "FILE-404") == []


@pytest.mark.asyncio
async def test_store_find_by_local_uuid_pinned_path(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """find_by_local_uuid matches the EXACT path list_by_key_value builds.

    The pinned-path promise: for the ``phantom_local_uuid`` key the new
    named lookup and the generic key-value match return the same rows,
    so callers never spell a path and the two can never drift.
    """
    local = uuid4()
    stamped = make_upload_row(chain_envelope_json=_envelope_with_local_uuid(local))
    unstamped = make_upload_row()
    await file_store.insert(stamped)
    await file_store.insert(unstamped)
    named = await file_store.find_by_local_uuid(local)
    generic = await file_store.list_by_key_value("phantom_local_uuid", str(local))
    assert [r.chain_id for r in named] == [stamped.chain_id]
    assert [r.chain_id for r in generic] == [r.chain_id for r in named]
    assert await file_store.find_by_local_uuid(uuid4()) == []


@pytest.mark.asyncio
async def test_store_find_by_local_uuid_multi_match(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """No uniqueness is enforced on the key; every match comes back."""
    local = uuid4()
    rows = [make_upload_row(chain_envelope_json=_envelope_with_local_uuid(local)) for _ in range(2)]
    for row in rows:
        await file_store.insert(row)
    got = await file_store.find_by_local_uuid(local)
    assert {r.chain_id for r in got} == {r.chain_id for r in rows}


# ---------------------------------------------------------------------------
# Group rollup route (task 4.4).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_rollup_f1_one_corrupted_rest_succeeded_is_finished(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """THE F1 regression: corrupted counts as finished-for-the-client.

    A group with one corrupted member and the rest succeeded MUST report
    ``all_finished=true``: a corrupted member never progresses without
    intervention, so a client waiting on this flag would otherwise wait
    forever.
    """
    app, alpha, _beta = lookup_app
    group = uuid4()
    for state in ("succeeded", "succeeded", "corrupted"):
        await alpha.store.insert(make_upload_row(group_id=group, state=state, instance_id="alpha"))
    response = TestClient(app).get(f"/v1/admin/groups/{group}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["all_finished"] is True
    assert body["counts_by_state"]["corrupted"] == 1
    assert body["counts_by_state"]["succeeded"] == 2


@pytest.mark.asyncio
async def test_group_rollup_auth_expired_finished_queued_not(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """auth_expired counts finished; a queued member flips the flag back."""
    app, alpha, _beta = lookup_app
    group = uuid4()
    await alpha.store.insert(
        make_upload_row(group_id=group, state="auth_expired", instance_id="alpha")
    )
    await alpha.store.insert(make_upload_row(group_id=group, state="stored", instance_id="alpha"))
    client = TestClient(app)
    assert client.get(f"/v1/admin/groups/{group}").json()["all_finished"] is True
    # The honest wrinkle: a member moving again flips the flag to false.
    await alpha.store.insert(make_upload_row(group_id=group, state="queued", instance_id="alpha"))
    assert client.get(f"/v1/admin/groups/{group}").json()["all_finished"] is False


@pytest.mark.asyncio
async def test_group_rollup_counts_and_timestamps_across_instances(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The rollup folds members from every instance with correct joins."""
    app, alpha, beta = lookup_app
    group = uuid4()
    base = datetime.now(tz=UTC).replace(microsecond=0)
    earliest = base - timedelta(minutes=10)
    latest_sent = base + timedelta(minutes=5)
    await alpha.store.insert(
        make_upload_row(
            group_id=group,
            state="succeeded",
            instance_id="alpha",
            received_at=earliest,
            sent_at=base,
        )
    )
    await alpha.store.insert(
        make_upload_row(
            group_id=group,
            state="queued",
            instance_id="alpha",
            received_at=base,
        )
    )
    await beta.store.insert(
        make_upload_row(
            group_id=group,
            state="succeeded",
            instance_id="beta",
            received_at=base - timedelta(minutes=5),
            sent_at=latest_sent,
        )
    )
    response = TestClient(app).get(f"/v1/admin/groups/{group}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["group_id"] == str(group)
    assert body["total"] == 3
    assert body["all_finished"] is False
    # The histogram carries ALL EIGHT canonical states, zeros included.
    assert set(body["counts_by_state"]) == set(_ALL_STATES)
    assert body["counts_by_state"]["succeeded"] == 2
    assert body["counts_by_state"]["queued"] == 1
    assert body["counts_by_state"]["failed"] == 0
    assert datetime.fromisoformat(body["first_received_at"]) == earliest
    assert datetime.fromisoformat(body["last_sent_at"]) == latest_sent
    # Members are merged receipt-ordered across instances.
    received = [datetime.fromisoformat(m["received_at"]) for m in body["members"]]
    assert received == sorted(received)
    member_fields = set(body["members"][0])
    assert {"multifile_id", "send_order", "sent_at", "last_error"} <= member_fields


@pytest.mark.asyncio
async def test_group_rollup_instance_scope(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """?instance= narrows the rollup; 404 when the group has no rows there."""
    app, alpha, beta = lookup_app
    group = uuid4()
    await alpha.store.insert(
        make_upload_row(group_id=group, state="succeeded", instance_id="alpha")
    )
    await beta.store.insert(make_upload_row(group_id=group, state="queued", instance_id="beta"))
    client = TestClient(app)
    scoped = client.get(f"/v1/admin/groups/{group}", params={"instance": "alpha"})
    assert scoped.status_code == 200
    assert scoped.json()["total"] == 1
    assert scoped.json()["all_finished"] is True
    # The other instance still reports its member.
    other = client.get(f"/v1/admin/groups/{group}", params={"instance": "beta"})
    assert other.json()["all_finished"] is False
    # A group with no rows on the scoped instance is a 404 there.
    lonely = uuid4()
    await alpha.store.insert(make_upload_row(group_id=lonely, instance_id="alpha"))
    missing = client.get(f"/v1/admin/groups/{lonely}", params={"instance": "beta"})
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_group_rollup_404_only_when_no_row(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
) -> None:
    """An id matching zero rows 404s with the canonical envelope."""
    app, _alpha, _beta = lookup_app
    response = TestClient(app).get(f"/v1/admin/groups/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_group_rollup_chain_id_resolves_to_singleton(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A chain_id queried as a group id returns its self-evident singleton."""
    app, alpha, _beta = lookup_app
    chain_id = uuid4()
    await alpha.store.insert(
        make_upload_row(chain_id=chain_id, group_id=chain_id, instance_id="alpha")
    )
    body = TestClient(app).get(f"/v1/admin/groups/{chain_id}").json()
    assert body["total"] == 1
    assert body["members"][0]["chain_id"] == str(chain_id)


# ---------------------------------------------------------------------------
# Identifier lookup routes (tasks 4.3 + 4.4).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lookup_by_captured_id_hit(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A configured instance resolves the captured id to a status summary."""
    app, alpha, _beta = lookup_app
    local = uuid4()
    row = make_upload_row(
        state="succeeded",
        instance_id="alpha",
        sent_at=datetime.now(tz=UTC),
        captured_values=_captured_with_file_id("FILE-123"),
        chain_envelope_json=_envelope_with_local_uuid(local),
    )
    await alpha.store.insert(row)
    response = TestClient(app).get(
        "/v1/admin/uploads/by-captured-id/FILE-123", params={"instance": "alpha"}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["kind"] == "captured_file_id"
    assert body["value"] == "FILE-123"
    assert body["found"] is True
    assert len(body["matches"]) == 1
    match = body["matches"][0]
    assert match["chain_id"] == str(row.chain_id)
    assert match["instance_id"] == "alpha"
    assert match["captured_file_id"] == "FILE-123"
    assert match["local_uuid"] == str(local)
    assert match["sent_at"] is not None


@pytest.mark.asyncio
async def test_lookup_by_captured_id_miss_is_found_false(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
) -> None:
    """A miss is 200 with found=false: a membership test, not a fetch."""
    app, _alpha, _beta = lookup_app
    response = TestClient(app).get(
        "/v1/admin/uploads/by-captured-id/FILE-404", params={"instance": "alpha"}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["found"] is False
    assert body["matches"] == []


@pytest.mark.asyncio
async def test_lookup_by_captured_id_multi_match_honesty(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Two rows with the same captured id both come back (no uniqueness lie)."""
    app, alpha, _beta = lookup_app
    rows = [
        make_upload_row(instance_id="alpha", captured_values=_captured_with_file_id("FILE-DUP"))
        for _ in range(2)
    ]
    for row in rows:
        await alpha.store.insert(row)
    body = (
        TestClient(app)
        .get("/v1/admin/uploads/by-captured-id/FILE-DUP", params={"instance": "alpha"})
        .json()
    )
    assert body["found"] is True
    assert {m["chain_id"] for m in body["matches"]} == {str(r.chain_id) for r in rows}


@pytest.mark.asyncio
async def test_lookup_by_captured_id_unconfigured_instance_400(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
) -> None:
    """An instance without the admin_lookup block refuses; it never guesses."""
    app, _alpha, _beta = lookup_app
    client = TestClient(app)
    # Scoped straight at the unconfigured instance.
    scoped = client.get("/v1/admin/uploads/by-captured-id/FILE-1", params={"instance": "beta"})
    assert scoped.status_code == 400, scoped.text
    envelope = scoped.json()["error"]
    assert envelope["code"] == "lookup_not_configured"
    assert envelope["details"]["unconfigured_instances"] == ["beta"]
    # Un-scoped fan-out includes beta, so the lookup refuses rather than
    # silently skipping an instance (a found=false would be a lie).
    fanned = client.get("/v1/admin/uploads/by-captured-id/FILE-1")
    assert fanned.status_code == 400
    assert fanned.json()["error"]["details"]["unconfigured_instances"] == ["beta"]


@pytest.mark.asyncio
async def test_lookup_by_local_uuid_hit_across_instances(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The local-uuid lookup needs no config and fans out everywhere."""
    app, alpha, beta = lookup_app
    local = uuid4()
    on_alpha = make_upload_row(
        instance_id="alpha",
        chain_envelope_json=_envelope_with_local_uuid(local),
        captured_values=_captured_with_file_id("FILE-123"),
    )
    on_beta = make_upload_row(
        instance_id="beta",
        chain_envelope_json=_envelope_with_local_uuid(local),
    )
    await alpha.store.insert(on_alpha)
    await beta.store.insert(on_beta)
    response = TestClient(app).get(f"/v1/admin/uploads/by-local-uuid/{local}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["kind"] == "local_uuid"
    assert body["value"] == str(local)
    assert body["found"] is True
    by_chain = {m["chain_id"]: m for m in body["matches"]}
    assert set(by_chain) == {str(on_alpha.chain_id), str(on_beta.chain_id)}
    # captured_file_id resolves through alpha's binding; beta has none.
    assert by_chain[str(on_alpha.chain_id)]["captured_file_id"] == "FILE-123"
    assert by_chain[str(on_beta.chain_id)]["captured_file_id"] is None
    assert by_chain[str(on_beta.chain_id)]["local_uuid"] == str(local)


@pytest.mark.asyncio
async def test_lookup_by_local_uuid_miss_is_found_false(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
) -> None:
    """A miss is 200 with found=false on the local-uuid axis too."""
    app, _alpha, _beta = lookup_app
    response = TestClient(app).get(f"/v1/admin/uploads/by-local-uuid/{uuid4()}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["found"] is False
    assert body["matches"] == []


# ---------------------------------------------------------------------------
# Export manifest fields (task 4.6).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_export_manifest_carries_group_id_and_sent_at(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """export.tar manifest entries carry the two cycle-7 fields.

    A delivered row carries its ISO sent_at; a parked (never-delivered)
    row carries an explicit null; both carry the group_id handle so the
    archive correlates back to the group surface.
    """
    app, alpha, _beta = lookup_app
    group = uuid4()
    delivered_at = datetime.now(tz=UTC).replace(microsecond=0)
    delivered = make_upload_row(
        state="succeeded", instance_id="alpha", group_id=group, sent_at=delivered_at
    )
    parked = make_upload_row(state="stored", instance_id="alpha", group_id=group)
    await alpha.store.insert(delivered)
    await alpha.store.insert(parked)
    response = TestClient(app).get("/v1/admin/export.tar")
    assert response.status_code == 200, response.text
    with tarfile.open(fileobj=io.BytesIO(response.content)) as tf:
        manifest_member = tf.extractfile("manifest.json")
        assert manifest_member is not None
        manifest = json.loads(manifest_member.read())
    by_chain = {entry["chain_id"]: entry for entry in manifest}
    assert by_chain[str(delivered.chain_id)]["group_id"] == str(group)
    assert by_chain[str(parked.chain_id)]["group_id"] == str(group)
    assert datetime.fromisoformat(by_chain[str(delivered.chain_id)]["sent_at"]) == delivered_at
    assert by_chain[str(parked.chain_id)]["sent_at"] is None


# ---------------------------------------------------------------------------
# ChainAdminDetail row-sourced fields (task 4.5).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chain_detail_carries_row_sourced_fields(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """GET /chains/{id} surfaces the seven cycle-7 fields off the row."""
    app, alpha, _beta = lookup_app
    group = uuid4()
    multifile = uuid4()
    base = datetime.now(tz=UTC).replace(microsecond=0)
    row = make_upload_row(
        state="succeeded",
        instance_id="alpha",
        group_id=group,
        multifile_id=multifile,
        send_order=2,
        received_at=base - timedelta(minutes=1),
        sent_at=base,
        next_attempt_at=None,
    )
    await alpha.store.insert(row)
    response = TestClient(app).get(f"/v1/admin/chains/{row.chain_id}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert datetime.fromisoformat(body["received_at"]) == row.received_at
    assert datetime.fromisoformat(cast(str, body["updated_at"])) == row.updated_at
    assert body["next_attempt_at"] is None
    assert datetime.fromisoformat(body["sent_at"]) == base
    assert body["group_id"] == str(group)
    assert body["multifile_id"] == str(multifile)
    assert body["send_order"] == 2


# ---------------------------------------------------------------------------
# Round 2 adversary additions (iteration loop, task 7.3).
# ---------------------------------------------------------------------------

# Group-rollup bound probe: a few hundred members exercises the
# un-paginated rollup at the scale the design envelope names (a
# producer's largest realistic burst), split across four states.
_BOUND_SUCCEEDED = 120
_BOUND_CORRUPTED = 60
_BOUND_AUTH_EXPIRED = 60
_BOUND_QUEUED = 60
_BOUND_TOTAL = _BOUND_SUCCEEDED + _BOUND_CORRUPTED + _BOUND_AUTH_EXPIRED + _BOUND_QUEUED

# Spacing between member received_at stamps; any positive spacing works
# (the rollup reads min/max, the member list orders by received_at).
_BOUND_SPACING = timedelta(milliseconds=10)

# Gate timeouts for the reload-race probe below; generous because the
# gated path is two event waits on one loop, not real I/O.
_RACE_GATE_TIMEOUT_SECONDS = 5.0


async def test_group_rollup_correct_at_a_few_hundred_members(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The un-paginated rollup stays truthful at the few-hundred bound.

    Round 2 adversary hardening: ``list_by_group_id`` carries no LIMIT
    by design (the rollup is a whole-group truth, not a page), so the
    math must hold at the largest realistic group size. Seeds 300
    members across four states, then settles the queued tail and
    re-asserts the F1 regression at the bound (corrupted members count
    as finished; ``all_finished`` flips only when nothing is moving).
    """
    app, alpha, _beta = lookup_app
    group = uuid4()
    base = datetime(2026, 6, 9, 12, 0, 0, tzinfo=UTC)
    members: list[UploadRow] = []
    states: list[str] = (
        ["succeeded"] * _BOUND_SUCCEEDED
        + ["corrupted"] * _BOUND_CORRUPTED
        + ["auth_expired"] * _BOUND_AUTH_EXPIRED
        + ["queued"] * _BOUND_QUEUED
    )
    for index, state in enumerate(states):
        received = base + _BOUND_SPACING * index
        sent = received + _BOUND_SPACING if state == "succeeded" else None
        row = make_upload_row(
            instance_id="alpha",
            group_id=group,
            state=state,
            received_at=received,
            updated_at=received,
            sent_at=sent,
        )
        members.append(row)
        await alpha.store.insert(row)

    client = TestClient(app)
    body = client.get(f"/v1/admin/groups/{group}").json()
    assert body["total"] == _BOUND_TOTAL
    assert len(body["members"]) == _BOUND_TOTAL
    # The histogram carries the COMPLETE nine-state vocabulary, zeros
    # included (the absent states are an explicit 0, never omitted).
    assert body["counts_by_state"] == {
        "queued": _BOUND_QUEUED,
        "attempting": 0,
        "succeeded": _BOUND_SUCCEEDED,
        "failed": 0,
        "corrupted": _BOUND_CORRUPTED,
        "cancelled": 0,
        "auth_expired": _BOUND_AUTH_EXPIRED,
        "stored": 0,
        "expired": 0,
    }
    assert body["all_finished"] is False, "a queued tail means the group is still moving"
    expected_last_sent = max(row.sent_at for row in members if row.sent_at is not None)
    assert datetime.fromisoformat(body["last_sent_at"]) == expected_last_sent
    assert datetime.fromisoformat(body["first_received_at"]) == base
    # Parse before comparing: wire stamps omit a zero microsecond field
    # ("...:00Z" vs "...:00.010000Z"), so STRING order is not temporal
    # order; the member ordering contract is temporal.
    received_order = [datetime.fromisoformat(member["received_at"]) for member in body["members"]]
    assert received_order == sorted(received_order), "members must order by received_at"

    # Settle the moving tail; the F1 regression must hold at the bound
    # (corrupted + auth_expired count as finished, nothing is queued).
    for row in members[-_BOUND_QUEUED:]:
        await alpha.store.cancel(row.chain_id)
    settled = client.get(f"/v1/admin/groups/{group}").json()
    assert settled["all_finished"] is True
    assert settled["counts_by_state"]["cancelled"] == _BOUND_QUEUED
    assert datetime.fromisoformat(settled["last_sent_at"]) == expected_last_sent


async def test_lookup_fanout_racing_binding_removal_stays_honest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """An in-flight by-captured-id fan-out must not silently skip an instance.

    Round 2 adversary probe (R2-4; seed: an in-flight lookup racing the
    reload snapshot swap). ``apply_reload`` swaps each instance's live
    settings snapshot with awaits between instances, and the route used
    to re-read the binding after its own awaits: the guard saw both
    instances configured, the reload then removed the second binding
    while the first store query was in flight, and the loop's defensive
    ``continue`` silently skipped the instance that holds the match. The
    honest outcomes are refusing (the 400 the post-reload config implies)
    or answering with the guard-time bindings; a found=false built from a
    silent skip matches neither boundary state. The round 2 defender fix
    snapshots the bindings ONCE before the unconfigured guard and fans
    out over the snapshot, so this race now answers with the guard-time
    truth (``found=True`` here). The simulation swaps the instance's
    snapshot box for one built with ``admin_lookup=None``, which is what
    a real reload does to that instance's live read after F5 moved the
    binding off the frozen ``cfg``; the swap is timed deterministically.
    """
    binding = AdminLookupCfg(capture_name=_CAPTURE_NAME, json_path=_JSON_PATH)
    left, _left_box = await _build_instance(tmp_path, "left", admin_lookup=binding)
    right, right_box = await _build_instance(tmp_path, "right", admin_lookup=binding)
    try:
        dispatcher = InstanceDispatcher([left, right])
        match_row = make_upload_row(
            instance_id="right",
            captured_values=_captured_with_file_id("FILE-RACE"),
        )
        await right.store.insert(match_row)

        entered = asyncio.Event()
        release = asyncio.Event()
        original = left.store.find_by_captured_value

        async def paused(capture_name: str, subpath: str, value: str) -> list[UploadRow]:
            entered.set()
            await release.wait()
            return await original(capture_name, subpath, value)

        monkeypatch.setattr(left.store, "find_by_captured_value", paused)
        lookup_task = asyncio.create_task(
            admin_routes.lookup_by_captured_id("FILE-RACE", dispatcher, None)
        )
        await asyncio.wait_for(entered.wait(), timeout=_RACE_GATE_TIMEOUT_SECONDS)
        # The reload's exact live-state effect, deterministically timed:
        # the operator's new YAML carries no admin_lookup for ``right``,
        # so its next live snapshot is built with the binding removed.
        right_box[0] = make_snapshot(
            persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
            admin_lookup=None,
        )
        release.set()
        try:
            response = await asyncio.wait_for(lookup_task, timeout=_RACE_GATE_TIMEOUT_SECONDS)
        except admin_routes.LookupNotConfiguredError:
            # Refusing per the post-reload config is an honest outcome.
            return
        assert response.found is True, (
            "the fan-out silently skipped the instance holding the match: "
            f"matches={response.matches!r}"
        )
    finally:
        for ctx in (left, right):
            await ctx.store.stop()
            await ctx.token_cache.stop()


def _envelope_with_kvs_pair(key: str, value: str) -> str:
    """A minimal envelope whose step-0 metadata KVS carries one pair."""
    return json.dumps(
        {
            "steps": [
                {
                    "name": _CAPTURE_NAME,
                    "method": "POST",
                    "url": "https://files-alpha.example.com/v2/files",
                    "body": {
                        "kind": "json",
                        "value": {"metadata": {"keyValueStore": {key: value}}},
                    },
                }
            ]
        }
    )


async def test_colon_bearing_kvs_key_is_addressable_by_key_value_match(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A KVS key containing a colon must remain addressable.

    Round 2 adversary probe (R2-3): the metadata key-value store takes
    user-defined dynamic keys, and ``tag:env`` is a legal one.
    Pre-fix, the wire encoding split at the FIRST colon (key
    ``tag``, value ``env:prod``) and the unquoted json-path
    interpolation broke dot/quote keys, so such rows were silently
    unfindable. The round 2 defender fix's chosen shape, of the three
    the probe delegated to the fix owner, is the escaped encoding: the
    store quotes every JSON1 key segment, and a colon-bearing key rides
    the quoted-key wire form ``"<key>":<value>`` (the SDK's
    ``find_by_metadata`` emits it automatically). This pin asserts the
    row IS findable through the surface, that the established
    first-colon split still serves plain keys with colon-bearing
    values, and that the plain form stays an EXACT match for key
    ``tag`` (no union across ambiguous readings; the pre-fix string
    is an honest miss, never a wrong-key hit).
    """
    app, alpha, _beta = lookup_app
    row = make_upload_row(
        instance_id="alpha",
        chain_envelope_json=_envelope_with_kvs_pair("tag:env", "prod"),
    )
    await alpha.store.insert(row)
    client = TestClient(app)
    response = client.get(
        "/v1/admin/chains",
        params={"key_value_match": '"tag:env":prod'},
    )
    assert response.status_code == 200, response.text
    found_ids = [upload["chain_id"] for upload in response.json()["uploads"]]
    assert str(row.chain_id) in found_ids, (
        "the colon-bearing key's row is unfindable through key_value_match"
    )

    # The pre-fix reading of the unquoted string (key 'tag', value
    # 'env:prod') stays an exact first-colon-split query: an honest
    # miss for this row, never a blended union across readings.
    unquoted = client.get(
        "/v1/admin/chains",
        params={"key_value_match": "tag:env:prod"},
    )
    assert unquoted.status_code == 200, unquoted.text
    assert unquoted.json()["uploads"] == []

    # The established plain form is untouched: a plain key pairs with a
    # colon-bearing value via the first-colon split.
    colon_value_row = make_upload_row(
        instance_id="alpha",
        chain_envelope_json=_envelope_with_kvs_pair("ts", "12:30:00"),
    )
    await alpha.store.insert(colon_value_row)
    plain = client.get(
        "/v1/admin/chains",
        params={"key_value_match": "ts:12:30:00"},
    )
    assert plain.status_code == 200, plain.text
    assert [u["chain_id"] for u in plain.json()["uploads"]] == [str(colon_value_row.chain_id)]


async def test_dot_and_quote_bearing_kvs_keys_are_addressable(
    lookup_app: tuple[FastAPI, InstanceContext, InstanceContext],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Dot-bearing and quote-bearing KVS keys are addressable (R2-3).

    The pre-fix unquoted json-path interpolation re-segmented the path
    at a ``.`` and could not express a ``"`` at all; the store now
    quotes every key segment. A dotted key works through the plain
    first-colon wire form; a key beginning with a double quote rides
    the quoted-key form with its escape.
    """
    app, alpha, _beta = lookup_app
    dotted_row = make_upload_row(
        instance_id="alpha",
        chain_envelope_json=_envelope_with_kvs_pair("telemetry.v2", "on"),
    )
    quoted_row = make_upload_row(
        instance_id="alpha",
        chain_envelope_json=_envelope_with_kvs_pair('"odd', "yes"),
    )
    await alpha.store.insert(dotted_row)
    await alpha.store.insert(quoted_row)
    client = TestClient(app)

    dotted = client.get(
        "/v1/admin/chains",
        params={"key_value_match": "telemetry.v2:on"},
    )
    assert dotted.status_code == 200, dotted.text
    assert [u["chain_id"] for u in dotted.json()["uploads"]] == [str(dotted_row.chain_id)]

    quoted = client.get(
        "/v1/admin/chains",
        params={"key_value_match": '"\\"odd":yes'},
    )
    assert quoted.status_code == 200, quoted.text
    assert [u["chain_id"] for u in quoted.json()["uploads"]] == [str(quoted_row.chain_id)]


# -----------------------------------------------------------------------------
# Round 4 adversary hardening: token-push wake storm vs rollup coherence.
# -----------------------------------------------------------------------------

# Wake-storm sizing: enough members that wake UPDATEs and rollup reads
# interleave, small enough to stay instant.
_STORM_GROUP_MEMBERS = 8
_STORM_ROLLUP_READS = 24


@pytest.mark.asyncio
async def test_wake_storm_rollup_reads_stay_internally_consistent(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Concurrent auth_expired wakes never tear a group read.

    A token-push storm drives the kicker's auth_expired -> queued
    transition on every member of one group while rollup-backing reads
    (``list_by_group_id`` on the dedicated read connection) run
    concurrently. Each read must be internally consistent: every member
    present exactly once, every observed state inside the
    {auth_expired, queued} transition vocabulary, never a torn or
    duplicated row. Wakes ride ``expected_state="auth_expired"`` so
    each transition fires exactly once (the kicker's predicate).
    """
    group_id = uuid4()
    members = [
        make_upload_row(group_id=group_id, state="auth_expired", send_order=i)
        for i in range(_STORM_GROUP_MEMBERS)
    ]
    for member in members:
        await file_store.insert(member)
    member_ids = sorted(m.chain_id for m in members)

    async def _wake(chain_id: UUID) -> int:
        return await file_store.record_attempt_result(
            chain_id,
            new_state="queued",
            attempts=0,
            next_attempt_at=datetime.now(tz=UTC),
            last_error=None,
            upstream_status=None,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
            expected_state="auth_expired",
        )

    read_faults: list[str] = []

    async def _read_rollup() -> None:
        rows = await file_store.list_by_group_id(group_id)
        seen = sorted(r.chain_id for r in rows)
        if seen != member_ids:
            read_faults.append(f"member set torn: {len(seen)} of {len(member_ids)}")
        bad_states = {r.state for r in rows} - {"auth_expired", "queued"}
        if bad_states:
            read_faults.append(f"foreign states observed: {bad_states}")

    wake_results = await asyncio.gather(
        *[_wake(m.chain_id) for m in members],
        *[_read_rollup() for _ in range(_STORM_ROLLUP_READS)],
    )
    assert read_faults == []
    # Every wake transitioned exactly once (rowcount 1 per member).
    assert [r for r in wake_results if isinstance(r, int)] == [1] * _STORM_GROUP_MEMBERS

    # Settled truth: all members queued, group read whole.
    settled = await file_store.list_by_group_id(group_id)
    assert sorted(r.chain_id for r in settled) == member_ids
    assert {r.state for r in settled} == {"queued"}
