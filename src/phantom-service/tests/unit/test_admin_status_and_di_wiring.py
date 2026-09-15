"""The admin surface reports live settings, from one dependency table.

S3-6. ``GET /v1/admin/status`` served a ``ResolvedDefaultsSummary`` built
once during boot and bound to the route by value (``lambda:
resolved_defaults_summary``). An operator who raised
``saturation.max_in_flight`` and reloaded was therefore told the OLD value
for the rest of the process lifetime while admission enforced the new one -
on the endpoint an operator consults precisely to confirm a reload landed.
The route's own docstring promised a per-request probe the wiring did not
perform.

S3-10. The dependency-override wiring was written twice - once inside the
lifespan and once as a ``setdefault`` fallback - and had already drifted:
``get_resolved_defaults_summary``, ``get_metrics_registry`` and
``get_data_root`` appeared ONLY in the lifespan copy, so a TestClient that
never enters the lifespan turned every observability and quarantine request
into a ``NotImplementedError`` and a naked 500. One table, applied once,
removes the class of bug rather than the three instances of it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    InstanceCfg,
    RouteCfg,
    SaturationCfg,
    Settings,
    StorageCfg,
)
from phantom.instances.snapshot import _build_snapshot

_INSTANCE_ID = "primary"
# The boot cap and the cap an operator raises it to by hot reload. Any two
# distinguishable values work; these mirror the finding's own example.
_BOOT_MAX_IN_FLIGHT = 32
_RELOADED_MAX_IN_FLIGHT = 128
_LIFESPAN_TIMEOUT_SECONDS = 30.0
_RAM_CEILING_BYTES = 1024 * 1024

# The two endpoints whose dependencies existed only in the lifespan copy of
# the wiring, and so answered 500 to a non-lifespan client.
_COUNTERS_PATH = "/v1/admin/observability/counters"
_QUARANTINE_PATH = "/v1/admin/quarantine"
_STATUS_PATH = "/v1/admin/status"


def _settings(data_root: Path, max_in_flight: int) -> Settings:
    """Production-shaped settings with a pinned in-flight cap."""
    hosts = ["files.example.com"]
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=_RAM_CEILING_BYTES),
        ),
        saturation=SaturationCfg(max_in_flight=max_in_flight),
        instances=[
            InstanceCfg(
                id=_INSTANCE_ID,
                host_prefixes=hosts,
                data_dir=_INSTANCE_ID,
                routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
            )
        ],
    )


async def _get(app: FastAPI, path: str) -> httpx.Response:
    """GET ``path`` from the app in-process, without touching the lifespan."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get(path)


async def test_admin_status_reports_the_reloaded_cap(tmp_path: Path) -> None:
    """S3-6. A hot-reloaded saturation cap reaches ``GET /v1/admin/status``.

    Objective: boot with ``max_in_flight=32``, then install the snapshot a
    reload to ``128`` installs, and ask the status endpoint. The endpoint
    describes the settings in force, so it must answer with what admission
    is now enforcing.

    Expected outcome: ``resolved_defaults.max_in_flight == 128``. Bound by
    value at boot it answers 32 for the process lifetime, so the operator
    checking whether their reload landed is told it did not.
    """
    boot_settings = _settings(tmp_path, _BOOT_MAX_IN_FLIGHT)
    app = create_app(boot_settings)
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), app.router.lifespan_context(app):
        before = await _get(app, _STATUS_PATH)
        assert before.status_code == 200, before.text
        assert before.json()["resolved_defaults"]["max_in_flight"] == _BOOT_MAX_IN_FLIGHT

        # Exactly what apply_reload installs: a fresh per-instance snapshot
        # built from the newly-loaded settings.
        reloaded_settings = _settings(tmp_path, _RELOADED_MAX_IN_FLIGHT)
        await app.state.settings_holder.replace(
            {cfg.id: _build_snapshot(reloaded_settings, cfg) for cfg in reloaded_settings.instances}
        )

        after = await _get(app, _STATUS_PATH)

    assert after.status_code == 200, after.text
    reported = after.json()["resolved_defaults"]["max_in_flight"]
    assert reported == _RELOADED_MAX_IN_FLIGHT, (
        "GET /v1/admin/status must report the cap admission is enforcing now, "
        f"not the boot-time one; reported {reported}"
    )


def test_observability_and_quarantine_answer_without_the_lifespan(tmp_path: Path) -> None:
    """S3-10. The non-lifespan wiring binds every dependency the routes need.

    Objective: a ``TestClient`` that never enters the lifespan is the
    documented way to exercise the app's routes, and three admin
    dependencies existed only in the lifespan's copy of the wiring.

    Expected outcome: the counters, quarantine and status endpoints all
    answer 200. With the drifted second copy each of them resolved a
    placeholder whose body raises ``NotImplementedError``, which surfaces
    as a naked 500.
    """
    app = create_app(_settings(tmp_path, _BOOT_MAX_IN_FLIGHT))
    client = TestClient(app)

    for path in (_COUNTERS_PATH, _QUARANTINE_PATH, _STATUS_PATH):
        response = client.get(path)
        assert response.status_code == 200, (
            f"{path} must answer without the lifespan; got {response.status_code}: {response.text}"
        )


def test_one_dependency_table_binds_both_boot_paths(tmp_path: Path) -> None:
    """S3-10. Entering the lifespan changes no binding except the dispatcher.

    Objective: the two copies of the wiring cannot drift if there is only
    one copy. The structural pin: every override the app carries before the
    lifespan is the SAME object afterwards, because the lifespan publishes
    its dispatcher into a slot the table already reads through.

    Expected outcome: an identical override mapping, key for key and
    provider for provider, before and after a lifespan cycle.
    """
    app = create_app(_settings(tmp_path, _BOOT_MAX_IN_FLIGHT))
    before = dict(app.dependency_overrides)

    with TestClient(app):
        during = dict(app.dependency_overrides)

    assert during == before, (
        "the lifespan must not re-bind the dependency table; a second copy of "
        "the wiring is what drifted and left three routes unbound off the "
        "lifespan path"
    )
