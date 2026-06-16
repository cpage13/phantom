"""Smoke tests for phantom.app factory."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from phantom.app import (
    _DEFAULT_EXECUTOR_MAX_WORKERS,
    _DEFAULT_EXECUTOR_MIN_WORKERS,
    _THREADS_PER_INFLIGHT_REQUEST,
    _default_executor_worker_count,
    create_app,
)
from phantom.config.settings import (
    InstanceCfg,
    RouteCfg,
    SaturationCfg,
    Settings,
    StorageCfg,
)


def test_create_app_no_instances() -> None:
    """``create_app`` builds the single app even with zero instances."""
    app = create_app(Settings())
    assert app.title == "phantom"


def _settings_with(
    *,
    max_in_flight: int | None,
    instance_count: int,
) -> Settings:
    """Build a minimal Settings with the requested saturation cap and N instances.

    Helper for the executor-sizing tests below. Each instance carries
    only the fields the Settings model validator requires; the test
    only inspects ``settings.saturation.max_in_flight`` and
    ``len(settings.instances)``, so route/host detail is generic.
    """
    instances = [
        InstanceCfg(
            id=f"inst-{i}",
            host_prefixes=[f"host-{i}.example.com"],
            data_dir=f"inst-{i}",
            routes=[
                RouteCfg(
                    name="files",
                    hosts=[f"host-{i}.example.com"],
                    auth_mode="phantom_bearer",
                )
            ],
        )
        for i in range(instance_count)
    ]
    return Settings(
        instances=instances,
        saturation=SaturationCfg(max_in_flight=max_in_flight),
    )


def test_default_executor_worker_count_clamps_to_min() -> None:
    """Small caps clamp up to the minimum worker count.

    A 1-instance deployment with ``max_in_flight=1`` would compute
    ``1*1*4 = 4`` desired workers; the clamp at
    ``_DEFAULT_EXECUTOR_MIN_WORKERS`` keeps the pool at the floor.
    """
    settings = _settings_with(max_in_flight=1, instance_count=1)
    workers = _default_executor_worker_count(settings)
    assert workers == _DEFAULT_EXECUTOR_MIN_WORKERS


def test_default_executor_worker_count_scales_within_range() -> None:
    """A moderate cap lands above the floor, below the ceiling.

    ``max_in_flight=12`` x 1 instance x ``_THREADS_PER_INFLIGHT_REQUEST``
    = 48. With min=32 and max=64, the desired value lands at 48 unclamped.
    """
    settings = _settings_with(max_in_flight=12, instance_count=1)
    workers = _default_executor_worker_count(settings)
    assert workers == 12 * _THREADS_PER_INFLIGHT_REQUEST
    assert _DEFAULT_EXECUTOR_MIN_WORKERS < workers < _DEFAULT_EXECUTOR_MAX_WORKERS


def test_default_executor_worker_count_clamps_to_max() -> None:
    """Very large caps clamp down to the maximum worker count.

    32 max_in_flight x 2 instances x 4 threads = 256 desired; the clamp
    at ``_DEFAULT_EXECUTOR_MAX_WORKERS`` keeps the pool at the ceiling.
    """
    settings = _settings_with(max_in_flight=32, instance_count=2)
    workers = _default_executor_worker_count(settings)
    assert workers == _DEFAULT_EXECUTOR_MAX_WORKERS


def test_default_executor_worker_count_no_instances() -> None:
    """Zero instances clamps the count to the minimum.

    Edge case: an empty ``Settings`` (no instances) — instance_count
    becomes ``max(1, 0) = 1`` and the clamp keeps the pool at the
    floor. The lifespan won't actually start with no instances under
    production config, but the sizing function must not crash.
    """
    settings = _settings_with(max_in_flight=8, instance_count=0)
    workers = _default_executor_worker_count(settings)
    assert workers == _DEFAULT_EXECUTOR_MIN_WORKERS


@pytest.mark.asyncio
async def test_create_app_one_instance(tmp_path: Path) -> None:
    """A one-instance Settings boots the app's lifespan cleanly.

    The single app owns the lifespan and serves the liveness/readiness
    probes (``GET /v1/healthz`` / ``GET /v1/readyz``) on the one listener.
    """
    settings = Settings(
        storage=StorageCfg(data_dir=str(tmp_path)),
        instances=[
            InstanceCfg(
                id="primary",
                host_prefixes=["files.example.com"],
                data_dir="primary",
                routes=[
                    RouteCfg(
                        name="files",
                        hosts=["files.example.com"],
                        auth_mode="phantom_bearer",
                    )
                ],
            )
        ],
    )
    app = create_app(settings)
    with TestClient(app) as client:
        # Lifespan startup runs recovery; liveness should respond immediately.
        response = client.get("/v1/healthz")
        assert response.status_code == 200
        ready = client.get("/v1/readyz")
        assert ready.status_code == 200
        assert ready.json()["ready"] is True
