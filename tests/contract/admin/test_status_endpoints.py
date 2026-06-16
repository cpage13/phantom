"""Contract tests for the status-family endpoints.

Covers:

- ``GET /v1/healthz`` - liveness (R12-1: public, off the admin router).
- ``GET /v1/readyz`` - readiness (R12-1: public, off the admin router).
- ``GET /v1/admin/stats`` — aggregate counters.
- ``GET /v1/admin/status`` — admin status summary.
- ``GET /v1/admin/instances`` — instance summary list.
- ``GET /v1/admin/instances/{id}/status`` — per-instance status.
- ``POST /v1/admin/reload`` — hot-reload trigger.

Tests assert the response shape matches the Pydantic model
documented on the route, exercises both happy-path 200 returns and
error-path responses (404 for unknown instance, 500 with no
settings path).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow, UploadState


async def _seed_row(ctx: InstanceContext, *, state: UploadState, body_size_bytes: int) -> None:
    """Insert one row in the given state with the given body size."""
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": state,
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "test-uid",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_size_bytes": body_size_bytes,
        },
    )
    await ctx.store.insert(row)


def test_health_returns_ok(health_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /v1/healthz`` returns ``{"status": "ok", ...}`` with healthy storage.

    The ``health_app`` fixture does not override ``get_degraded_instances``,
    so the route's safe-default empty map applies and storage reports ``ok``
    (the § 4D.2 fields are present and benign on a healthy stack).
    """
    app, _ctx = health_app
    client = TestClient(app)
    response = client.get("/v1/healthz")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert body["version"] == "0.1.0"
    assert body["storage"] == "ok"
    assert body["storage_detail"] is None


def test_ready_returns_ready(health_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /v1/readyz`` returns ``{"ready": true, "detail": null}`` when instances exist."""
    app, _ctx = health_app
    client = TestClient(app)
    response = client.get("/v1/readyz")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ready"] is True
    assert body["detail"] is None


def test_ready_reports_degraded_instance(health_app: tuple[FastAPI, InstanceContext]) -> None:
    """A degraded instance makes /v1/readyz report ready=false + naming detail (seam 3)."""
    from phantom.routes import health as health_routes
    from phantom.runtime.startup_checks import DegradedInstance, DegradeReason

    app, _ctx = health_app
    fault = "isolate failed: [Errno 30] Read-only file system"
    degraded = (
        DegradedInstance(
            instance_id="primary",
            reason=DegradeReason.SUBSTRATE_UNWRITABLE,
            detail=fault,
        ),
    )
    app.dependency_overrides[health_routes.get_degraded_instances] = lambda: degraded
    client = TestClient(app)
    response = client.get("/v1/readyz")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ready"] is False
    # The detail names the instance, the typed reason, and the fault.
    assert "primary" in body["detail"]
    assert "substrate_unwritable" in body["detail"]
    assert fault in body["detail"]


def test_health_reports_degraded_storage(health_app: tuple[FastAPI, InstanceContext]) -> None:
    """A degraded instance makes /v1/healthz report storage=degraded (seam 3)."""
    from phantom.routes import health as health_routes
    from phantom.runtime.startup_checks import DegradedInstance, DegradeReason

    app, _ctx = health_app
    fault = "isolate failed: [Errno 30] Read-only file system"
    degraded = (
        DegradedInstance(
            instance_id="primary",
            reason=DegradeReason.SUBSTRATE_UNWRITABLE,
            detail=fault,
        ),
    )
    app.dependency_overrides[health_routes.get_degraded_instances] = lambda: degraded
    client = TestClient(app)
    response = client.get("/v1/healthz")
    assert response.status_code == 200, response.text
    body = response.json()
    # Liveness unchanged; storage carries the typed fault.
    assert body["status"] == "ok"
    assert body["storage"] == "degraded"
    assert "primary" in body["storage_detail"]
    assert "substrate_unwritable" in body["storage_detail"]
    assert fault in body["storage_detail"]


def test_stats_returns_zero_breakdowns(admin_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /stats`` returns zero counts on a fresh stack."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/stats")
    assert response.status_code == 200, response.text
    body = response.json()
    # The response carries in_flight + by_state + body_location +
    # saturation. Shape assertion only — concrete numbers are
    # non-load-bearing on a fresh fixture.
    assert "in_flight" in body
    assert "by_state" in body
    assert "saturation" in body
    assert body["in_flight"]["count"] == 0
    assert body["in_flight"]["bytes"] == 0
    # Parked backlog is zero on a fresh stack.
    assert body["parked_total"] == 0
    assert body["by_state"]["stored"]["count"] == 0
    assert body["by_state"]["stored"]["bytes"] == 0


async def test_stats_reports_parked_backlog(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /stats`` surfaces stored rows in by_state.stored and parked_total.

    ``stored`` is terminal, so the historical ``list_non_terminal``-only
    aggregation left ``by_state.stored`` structurally zero; the
    ``counts_by_state`` read now populates it (count AND bytes).
    ``parked_total`` = stored + auth_expired.
    """
    app, ctx = admin_app
    # Two stored rows (terminal; recoverable body) and one auth_expired
    # (non-terminal; waiting for a token). One queued row proves the
    # non-terminal aggregation still works alongside the stored read.
    await _seed_row(ctx, state="stored", body_size_bytes=100)
    await _seed_row(ctx, state="stored", body_size_bytes=250)
    await _seed_row(ctx, state="auth_expired", body_size_bytes=10)
    await _seed_row(ctx, state="queued", body_size_bytes=5)

    client = TestClient(app)
    response = client.get("/v1/admin/stats")
    assert response.status_code == 200, response.text
    body = response.json()

    # stored: terminal, now visible with count + summed bytes.
    assert body["by_state"]["stored"]["count"] == 2
    assert body["by_state"]["stored"]["bytes"] == 350
    # parked_total = stored.count (2) + auth_expired_count (1).
    assert body["parked_total"] == 3
    assert body["auth"]["auth_expired_count"] == 1
    # The non-terminal loop is unchanged: queued + auth_expired counted,
    # body_location reflects the non-terminal rows only.
    assert body["by_state"]["queued"]["count"] == 1
    assert body["by_state"]["auth_expired"]["count"] == 1
    assert body["in_flight"]["count"] == 2  # queued + auth_expired
    assert body["body_location"]["ram"]["count"] == 2  # the two non-terminal rows


def test_status_returns_summary(admin_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /status`` returns the admin summary with one instance."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/status")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "instances" in body
    assert isinstance(body["instances"], list)
    assert len(body["instances"]) == 1
    assert body["instances"][0]["id"] == "primary"


def test_list_instances_returns_one(admin_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /instances`` returns the configured instance list as an envelope.

    The route returns an ``{"instances": [...]}`` envelope (not a bare
    array) to match the SDK ``list_instances`` model and the ``/chains`` +
    ``/tokens`` list convention (R-EX4).
    """
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/instances")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "instances" in body
    instances = body["instances"]
    assert isinstance(instances, list)
    assert len(instances) == 1
    assert instances[0]["id"] == "primary"
    assert "host_prefixes" in instances[0]
    assert "in_flight" in instances[0]


def test_get_instance_status_known_id(admin_app: tuple[FastAPI, InstanceContext]) -> None:
    """``GET /instances/{id}/status`` returns 200 for a known instance."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/instances/primary/status")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["id"] == "primary"


def test_get_instance_status_unknown_id_returns_421(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /instances/{id}/status`` returns 421 + ``instance_unknown`` envelope.

    Per plan §5.6 error table the admin route raises
    :class:`UnknownInstanceError`; the app-level handler translates it
    to a 421 ErrorEnvelope with ``error.code='instance_unknown'``.
    """
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/instances/nonexistent/status")
    assert response.status_code == 421, response.text
    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "instance_unknown"
