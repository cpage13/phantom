"""The admin decision surface must report what is, not a literal.

Five fields on ``GET /v1/admin/status``, ``GET /v1/admin/instances/{id}/
status`` and ``GET /v1/admin/observability/ram_pressure`` were structurally
disconnected from reality (review 2026-09-08, the hardcoded-literal /
dead-field cluster):

* S1-6 ``ready`` was the literal ``True`` and ``degraded_durability`` the
  literal ``False``, so a deployment whose every instance failed its storage
  boot answered ``{"ready": true, "instances": []}`` here while
  ``/v1/readyz`` on the SAME process answered false. ADR-007 designates this
  the "keep sending or back off" surface.
* S1-7 ``ad_reachability`` was the literal ``"not_configured"`` with no
  producer anywhere, so an instance reported
  ``refresh_strategy="ad_client_credentials"`` and
  ``ad_reachability="not_configured"`` in the same body.
* S1-3 ``succeeded_recent`` / ``failed_recent`` were initialized to zero and
  written nowhere, while the aggregate holding their numbers was fetched
  three lines later and discarded.
* S1-5 ``pending_migrations`` accumulated the RUNNING TOTAL queue depth, so
  it over-counted quadratically across instances.
* SW-5 ``ram_ceiling_bytes`` summed ONE GLOBAL cap across N instances, so
  the published denominator was N times the number any enforcement point
  uses.

Every test here drives the real routes over real stores.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from phantom.config.ad_mint import AdMintConfig
from phantom.config.settings import BodyStoreCfg
from phantom.instances.context import InstanceContext
from phantom.refresh.ad_client_credentials import AdMinter, AdReachability
from phantom.runtime.startup_checks import DegradedInstance, DegradeReason
from phantom.storage.interface import TokenCache
from phantom.workers.persist_controller import PersistController

from ._admin_truth_app import build_admin_app, build_instance

# A RAM ceiling distinct per test purpose, so a summed denominator is
# visibly wrong rather than coincidentally right.
_CEILING_BYTES = 2 * 1024 * 1024 * 1024


def _minter(endpoint: str = "files.upstream.example") -> AdMinter:
    """An AdMinter over a cache these tests never write through."""
    cache: TokenCache = None  # type: ignore[assignment]  # never read on these paths
    config = AdMintConfig.model_validate(
        {
            "tenant_id": "00000000-0000-0000-0000-000000000000",
            "client_id": "11111111-1111-1111-1111-111111111111",
            "primary_client_secret_env": "PHANTOM_UPSTREAM_CLIENT_SECRET",
            "scope": "api://files.upstream.example/.default",
            "endpoint": endpoint,
            "uid": "upstream-sp",
        }
    )
    return AdMinter(config=config, token_cache=cache)


def _degraded(instance_id: str) -> DegradedInstance:
    """One typed degraded-boot outcome for ``instance_id``."""
    return DegradedInstance(
        instance_id=instance_id,
        reason=DegradeReason.SUBSTRATE_UNWRITABLE,
        detail=f"data_dir for {instance_id} is not writable",
    )


@pytest.mark.asyncio
async def test_status_is_not_ready_when_every_instance_booted_degraded(
    tmp_path: Path,
) -> None:
    """S1-6: a dead deployment must not answer ``ready: true``.

    Objective: reproduce the exact divergence the review recorded - every
    instance degraded, so none reaches the dispatcher, and the two readiness
    surfaces on one process disagree. Success: ``/v1/admin/status`` reports
    ``ready`` false and ``/v1/readyz`` agrees, so an operator polling either
    one is told to back off.
    """
    app = build_admin_app([], degraded=[_degraded("primary")])
    client = TestClient(app)

    status = client.get("/v1/admin/status")
    readyz = client.get("/v1/readyz")

    assert status.status_code == 200, status.text
    assert status.json()["instances"] == []
    assert status.json()["ready"] is False, (
        "a deployment with no instance able to buffer anything reported ready"
    )
    assert readyz.json()["ready"] is False
    assert status.json()["ready"] == readyz.json()["ready"]


@pytest.mark.asyncio
async def test_status_is_not_ready_while_one_instance_of_two_is_degraded(
    tmp_path: Path,
) -> None:
    """S1-6: one degraded instance is enough to stop saying ready.

    Objective: the partial case, where a live instance would otherwise mask
    a dead sibling. ``ready`` is a whole-deployment claim ("every instance's
    storage layer is open and accepting writes"), so it must be worst-wins.
    Success: ``ready`` is false while the healthy instance still appears in
    the summary list.
    """
    live = await build_instance(tmp_path, "primary")
    app = build_admin_app([live], degraded=[_degraded("secondary")])
    client = TestClient(app)

    body = client.get("/v1/admin/status").json()

    assert body["ready"] is False
    assert [summary["id"] for summary in body["instances"]] == ["primary"]


@pytest.mark.asyncio
async def test_status_is_ready_when_every_instance_booted_clean(tmp_path: Path) -> None:
    """S1-6: the healthy case still reports ready.

    Objective: a readiness signal that is always false is no better than one
    that is always true; pin the positive arm too. Success: a live instance
    with an empty degraded set reports ``ready`` true.
    """
    live = await build_instance(tmp_path, "primary")
    app = build_admin_app([live])

    body = TestClient(app).get("/v1/admin/status").json()

    assert body["ready"] is True


@pytest.mark.asyncio
async def test_degraded_instance_status_reports_degraded_durability(
    tmp_path: Path,
) -> None:
    """S1-6: the per-instance surface must be able to say "degraded".

    Objective: ``degraded_durability`` was the literal ``False`` and a
    degraded instance never enters the dispatcher, so the ONLY answer this
    route could give for one was 421 ``instance_unknown`` - "no such
    instance" for an instance that exists and cannot write. Success: the
    configured-but-degraded instance answers 200 with ``ready`` false and
    ``degraded_durability`` true, while a genuinely unconfigured id still
    gets the 421 envelope.
    """
    live = await build_instance(tmp_path, "primary")
    app = build_admin_app([live], degraded=[_degraded("secondary")])
    client = TestClient(app)

    degraded = client.get("/v1/admin/instances/secondary/status")
    healthy = client.get("/v1/admin/instances/primary/status")
    unknown = client.get("/v1/admin/instances/never-configured/status")

    assert degraded.status_code == 200, degraded.text
    assert degraded.json()["degraded_durability"] is True
    assert degraded.json()["ready"] is False
    assert healthy.json()["degraded_durability"] is False
    assert healthy.json()["ready"] is True
    assert unknown.status_code == 421
    assert unknown.json()["error"]["code"] == "instance_unknown"


@pytest.mark.asyncio
async def test_ad_reachability_follows_the_minter_not_a_literal(tmp_path: Path) -> None:
    """S1-7: the ADR-007 AD signal must actually fire.

    Objective: the field was a hardcoded ``"not_configured"`` with no
    producer, so an operator whose app registration was unreachable (and
    whose rows were therefore all parking in ``auth_expired``) got nothing
    from the one field designed to tell them. Success: the response tracks
    what the minter observed, and no longer contradicts the
    ``refresh_strategy`` reported in the same body.
    """
    minter = _minter()
    ctx = await build_instance(tmp_path, "primary", minter=minter)
    client = TestClient(build_admin_app([ctx]))

    minter._reachability = AdReachability.REACHABLE
    reachable = client.get("/v1/admin/status").json()
    minter._reachability = AdReachability.UNREACHABLE
    unreachable = client.get("/v1/admin/status").json()

    assert reachable["ad_reachability"] == "reachable"
    assert unreachable["ad_reachability"] == "unreachable"
    assert reachable["instances"][0]["refresh_strategy"] == "ad_client_credentials"


@pytest.mark.asyncio
async def test_ad_reachability_is_not_configured_without_a_minter(tmp_path: Path) -> None:
    """S1-7: ``not_configured`` still means exactly that.

    Objective: the literal was not wrong for a deployment with no ``ad_mint``
    block, and the fix must not turn that case into a false alarm. Success: an
    instance with no minter reports ``not_configured``.
    """
    ctx = await build_instance(tmp_path, "primary")
    client = TestClient(build_admin_app([ctx]))

    assert client.get("/v1/admin/status").json()["ad_reachability"] == "not_configured"


@pytest.mark.asyncio
async def test_ad_reachability_is_worst_wins_across_instances(tmp_path: Path) -> None:
    """S1-7: one unreachable minter is not masked by a healthy sibling.

    Objective: the signal exists to catch the instance whose credentials are
    failing; averaging it away would reproduce the original blindness on a
    multi-instance container. Success: a reachable and an unreachable minter
    together report ``unreachable``.
    """
    good = _minter()
    bad = _minter()
    good._reachability = AdReachability.REACHABLE
    bad._reachability = AdReachability.UNREACHABLE
    first = await build_instance(tmp_path, "primary", minter=good)
    second = await build_instance(tmp_path, "secondary", minter=bad)
    client = TestClient(build_admin_app([first, second]))

    assert client.get("/v1/admin/status").json()["ad_reachability"] == "unreachable"


@pytest.mark.asyncio
async def test_stats_report_succeeded_and_failed_counts(
    tmp_path: Path,
    make_upload_row,
) -> None:
    """S1-3: the two terminal tallies must not be structurally zero.

    Objective: ``succeeded_recent`` and ``failed_recent`` were written
    nowhere, so an operator watching delivery health on the stats surface saw
    a permanent zero whatever the buffer held. Success: two succeeded rows
    and one failed row are counted, with their bytes, alongside the
    non-terminal tallies that always worked.
    """
    ctx = await build_instance(tmp_path, "primary")
    for state, size in (("succeeded", 10), ("succeeded", 30), ("failed", 7), ("queued", 5)):
        await ctx.store.insert(make_upload_row(chain_id=uuid4(), state=state, body_size_bytes=size))
    client = TestClient(build_admin_app([ctx]))

    by_state = client.get("/v1/admin/stats").json()["by_state"]

    assert by_state["succeeded_recent"] == {"count": 2, "bytes": 40}
    assert by_state["failed_recent"] == {"count": 1, "bytes": 7}
    assert by_state["queued"] == {"count": 1, "bytes": 5}


@pytest.mark.asyncio
async def test_ram_pressure_ceiling_is_not_multiplied_by_the_instance_count(
    tmp_path: Path,
) -> None:
    """SW-5: the published ceiling must be the one enforcement uses.

    Objective: ``_build_snapshot`` shares ONE ``BodyStoreCfg`` across every
    instance and ``RamPressureWatcher`` enforces that ceiling per instance,
    but the endpoint summed it, so four instances at 2 GiB published an 8 GiB
    denominator and an operator diagnosing migration churn read a third of
    the real utilisation. Success: two instances sharing a 2 GiB ceiling
    publish 2 GiB, not 4.
    """
    shared = BodyStoreCfg(ram_ceiling_bytes=_CEILING_BYTES)
    first = await build_instance(tmp_path, "primary", body_store_cfg=shared)
    second = await build_instance(tmp_path, "secondary", body_store_cfg=shared)
    client = TestClient(build_admin_app([first, second]))

    body = client.get("/v1/admin/observability/ram_pressure").json()

    assert body["ram_ceiling_bytes"] == _CEILING_BYTES


def _enqueue_migrations(ctx: InstanceContext, *, count: int) -> None:
    """Put ``count`` chain_ids on a real PersistController's queue.

    The queue is filled directly rather than through ``enqueue`` so no
    migration worker is running to drain it: the admin endpoint reads a
    depth, and these tests are about the arithmetic over that depth, not
    about migration itself.

    Args:
        ctx: The instance to give a controller to.
        count: How many chain_ids to enqueue.
    """
    controller = PersistController(
        store=ctx.store,  # type: ignore[arg-type]  # UploadStore Protocol vs the sqlite binding
        ram_body_store=ctx.ram_body_store,
        file_body_store=ctx.file_body_store,
    )
    for _ in range(count):
        controller._queue.put_nowait(uuid4())
    ctx.persist_controller = controller


@pytest.mark.asyncio
async def test_pending_migrations_counts_each_instance_once(
    tmp_path: Path,
) -> None:
    """S1-5: the migration backlog must not compound across instances.

    Objective: ``pending`` added the RUNNING TOTAL queue depth on every
    iteration, so two instances holding three and two enqueued migrations
    reported nine instead of five, and the operator watching RAM pressure saw
    a fabricated backlog. Success: the endpoint reports the five that exist,
    and the queue-depth field reports the same five.
    """
    first = await build_instance(tmp_path, "primary")
    second = await build_instance(tmp_path, "secondary")
    _enqueue_migrations(first, count=3)
    _enqueue_migrations(second, count=2)
    client = TestClient(build_admin_app([first, second]))

    body = client.get("/v1/admin/observability/ram_pressure").json()

    assert body["pending_migrations"] == 5
    assert body["persist_controller_queue_depth"] == 5


@pytest.mark.asyncio
async def test_pending_migrations_counts_in_flight_alongside_the_queue(
    tmp_path: Path,
) -> None:
    """S1-5: ``pending`` is queue depth PLUS the active set, per instance.

    Objective: the documented shape is "queue depth + active", and the fix
    must keep the active half rather than dropping it while correcting the
    accumulator. Success: one queued plus one in-flight migration reports
    two pending against a queue depth of one.
    """
    ctx = await build_instance(tmp_path, "primary")
    _enqueue_migrations(ctx, count=1)
    assert ctx.persist_controller is not None
    ctx.persist_controller._in_flight[UUID(int=1)] = asyncio.get_running_loop().create_future()
    client = TestClient(build_admin_app([ctx]))

    body = client.get("/v1/admin/observability/ram_pressure").json()

    assert body["persist_controller_queue_depth"] == 1
    assert body["pending_migrations"] == 2
    ctx.persist_controller._in_flight.clear()


@pytest.mark.asyncio
async def test_status_backlog_and_disk_usage_still_aggregate(tmp_path: Path) -> None:
    """The truthful fields on the same response must not regress.

    Objective: ``ready`` and ``ad_reachability`` are read from new seams in
    the same handler that sums the backlog and the disk usage; pin those two
    so a refactor of the surrounding loop cannot quietly drop them. Success:
    a non-terminal row in each of two instances totals two, and the bytes
    written through each file body store are summed.
    """
    first = await build_instance(tmp_path, "primary")
    second = await build_instance(tmp_path, "secondary")
    await first.file_body_store.put(uuid4(), {"body": b"x" * 64})
    await second.file_body_store.put(uuid4(), {"body": b"x" * 32})
    client = TestClient(build_admin_app([first, second]))

    body = client.get("/v1/admin/status").json()

    assert body["total_backlog"] == 0
    assert body["disk_usage_bytes"] == 96
    assert body["ready"] is True


@pytest.mark.asyncio
async def test_instance_status_dates_are_not_required_for_the_degraded_arm(
    tmp_path: Path,
) -> None:
    """The degraded per-instance answer must be a valid response body.

    Objective: the degraded arm builds an ``InstanceStatusResponse`` without
    an open store, so every tally is zero; a strict-pinned model would reject
    a half-built body and turn the honest answer into a 500. Success: the
    response validates and reports zeros for everything it cannot read.
    """
    app = build_admin_app([], degraded=[_degraded("primary")])

    body = TestClient(app).get("/v1/admin/instances/primary/status").json()

    assert body["id"] == "primary"
    assert body["in_flight"] == {"count": 0, "bytes": 0}
    assert body["by_state"]["queued"] == {"count": 0, "bytes": 0}
    assert body["by_state"]["succeeded_recent"] == {"count": 0, "bytes": 0}
    assert body["disk_usage_bytes"] == 0
    assert body["auth"]["auth_expired_count"] == 0
