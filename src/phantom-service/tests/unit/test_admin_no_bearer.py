"""Bearer-not-in-admin-response regression test (plan §9 AC8 + ADR-004).

ADR-004 makes the invariant load-bearing: bearer values stored in the
token cache must NEVER appear in any admin response body. The
``TokenSlot`` model has no bearer field by design — this test guards
against future field additions accidentally leaking one.

The test exercises every GET endpoint on the admin router with a
populated token cache (a real bearer pushed in via ``POST /v1/send``'s
inbound-Authorization side-effect), then iterates every response body
and asserts no substring matches the recognizable bearer / JWT
patterns.

This is a structural test: writing a new admin endpoint won't break
it, but the new endpoint will be checked the next time the suite
runs.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.routes import admin as admin_routes
from phantom.routes import send as send_routes
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

# A recognizable bearer that the test pushes into the cache, then
# greps for in every admin response. The token is shaped like a real
# JWT (three base64url chunks joined by dots) so the JWT pattern also
# trips if any field leaks the raw value.
_SENTINEL_BEARER = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.fake-signature-12345"


class _FakeUpstream:
    """Stub UpstreamClient — admin tests never call upstream."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


@pytest.fixture
async def app_with_token(tmp_path: Path) -> Iterable[tuple[FastAPI, str]]:
    """Build an app + populated token cache; yield ``(app, chain_id)``.

    The token cache is seeded by submitting a chain with an
    Authorization header — that's the production code path that lands
    bearers in the cache, so the test exercises the same flow.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()
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
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=1_000_000, max_disk_bytes=10_000_000
    )

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    persist_trigger = PersistTriggerCfg(body_size_threshold_bytes=0)
    from phantom.storage.hybrid_body_store import HybridBodyStore

    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
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
        current_settings=snapshot_thunk(make_snapshot(persist_trigger=persist_trigger)),
    )
    track_instance(ctx)
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(send_routes.router)
    app.include_router(admin_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture cannot drift from production app.py (round 3 fix R3-1).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[send_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: 2_147_483_648
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    # Plan § 4.2.5 — observability endpoints depend on MetricsRegistry.
    from phantom.observability.metrics import MetricsRegistry  # local import

    app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: MetricsRegistry()
    # Plan § 5.2.5 — quarantine endpoint depends on data_root for the walk.
    app.dependency_overrides[admin_routes.get_data_root] = lambda: tmp_path

    # Seed the cache via the real ingress path.
    chain_id = str(uuid4())
    client = TestClient(app)
    response = client.post(
        "/v1/send",
        json={
            "chain_id": chain_id,
            "idempotency_key": "k",
            "steps": [
                {
                    "name": "create_file",
                    "method": "POST",
                    "url": "https://files.example.com/v2/files",
                }
            ],
        },
        headers={
            "X-Phantom-Uid": "user-1",
            "Authorization": f"Bearer {_SENTINEL_BEARER}",
        },
    )
    assert response.status_code == 202, response.text

    yield app, chain_id


# Endpoints to skip from the GET-iteration: streaming bodies that
# emit binary payloads (the test scans text), endpoints that take a
# body or require parameters we can't synthesize here. The bundle
# endpoint requires the seeded chain to have body_refs (the chain
# we seed has none — it would error out before the body inspection).
# The export.tar endpoint streams binary tar bytes and is exercised
# separately by ``test_no_bearer_in_export_tar_manifest``.
_SKIP_PATHS: frozenset[str] = frozenset(
    {
        "/v1/admin/chains/{chain_id}/body",
        "/v1/admin/chains/{chain_id}/bundle",
        "/v1/admin/export.tar",
    }
)


# JWT-shaped string: three base64url chunks separated by dots, each
# at least one character. Sufficient to catch both the sentinel and
# any organically-leaked JWT-shaped substring.
_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")


def _gather_get_route_paths(app: FastAPI) -> list[str]:
    """Return the path templates of every admin GET route on ``app``.

    Includes both static-path GETs (``/v1/admin/stats``) and
    parametrized GETs (``/v1/admin/chains/{chain_id}``). The caller
    substitutes path params before requesting. Endpoints in
    ``_SKIP_PATHS`` are excluded. (R12-1 moved liveness/readiness off the
    admin router to the public ``/v1/healthz`` / ``/v1/readyz`` paths, so
    they are no longer among the admin GET routes this gathers.)
    """
    out: list[str] = []
    for route in app.router.routes:
        if not isinstance(route, APIRoute):
            continue
        if "GET" not in route.methods:
            continue
        if not route.path.startswith("/v1/admin/"):
            continue
        if route.path in _SKIP_PATHS:
            continue
        out.append(route.path)
    return out


def _substitute_path(template: str, *, chain_id: str) -> str | None:
    """Substitute known path params; return None if the template has unknowns."""
    s = template
    s = s.replace("{chain_id}", chain_id)
    s = s.replace("{instance_id}", "primary")
    # Bear-cache lookup paths also exist (PUT/DELETE only — not GET);
    # this stays None if any unhandled placeholder remains.
    if "{" in s:
        return None
    return s


def test_no_bearer_in_any_admin_get_response(app_with_token: tuple[FastAPI, str]) -> None:
    """No admin GET response body contains a bearer value or JWT-shaped string.

    Iterates every GET route on the admin router (resolved via the
    FastAPI route table), requests it with a populated token cache,
    and asserts no response body contains the sentinel bearer, the
    literal "Bearer " prefix in a value position, or a JWT-shaped
    substring. Path params are substituted from the seeded chain id
    and the configured instance id.
    """
    app, chain_id = app_with_token
    client = TestClient(app)
    routes = _gather_get_route_paths(app)
    assert routes, "admin router should have at least one GET endpoint"

    # Track inspected paths for the failure message.
    inspected: list[str] = []
    for path_template in routes:
        path = _substitute_path(path_template, chain_id=chain_id)
        if path is None:
            # Cannot synthesize params — skip with a noted reason.
            continue
        response = client.get(path)
        inspected.append(f"{path_template} -> {response.status_code}")
        # 4xx is acceptable (the test doesn't care about happy-path
        # semantics, only about bearer leakage). Inspect the body
        # regardless: a 422 error body is just as bad as a 200 body
        # if it leaks the bearer.
        body_text = response.text
        assert _SENTINEL_BEARER not in body_text, (
            f"Bearer sentinel leaked from {path_template} (status "
            f"{response.status_code}); response body:\n{body_text[:500]}"
        )
        assert "Bearer " not in body_text, (
            f"'Bearer ' substring appeared in response from {path_template} "
            f"(status {response.status_code}); response body:\n{body_text[:500]}"
        )
        # Authorization header name appearing as a JSON key is also
        # treated as a leak — the admin surface should never echo it.
        assert "Authorization" not in body_text, (
            f"'Authorization' substring appeared in response from "
            f"{path_template} (status {response.status_code}); body:\n"
            f"{body_text[:500]}"
        )
        # JWT-shaped catch-all (independent of the sentinel).
        match = _JWT_RE.search(body_text)
        assert match is None, (
            f"JWT-shaped substring appeared in response from {path_template} "
            f"(status {response.status_code}); match: {match.group()!r}; "
            f"body:\n{body_text[:500]}"
        )
    # Sanity: a non-trivial number of routes were inspected.
    assert len(inspected) >= 5, f"too few admin routes inspected: {inspected}"


def test_no_bearer_in_export_tar_manifest(app_with_token: tuple[FastAPI, str]) -> None:
    """``/v1/admin/export.tar`` doesn't include bearer values in manifest.json.

    The streaming binary response is excluded from the GET sweep, so
    inspect it here explicitly: stream the tar, decode the
    ``manifest.json`` entry, and assert it contains no bearer-shaped
    strings.
    """
    import io
    import tarfile

    app, _chain_id = app_with_token
    client = TestClient(app)
    response = client.get("/v1/admin/export.tar")
    assert response.status_code == 200
    # The streaming response delivers a tar archive.
    buf = io.BytesIO(response.content)
    with tarfile.open(fileobj=buf, mode="r") as tar:
        for member in tar.getmembers():
            extracted = tar.extractfile(member)
            if extracted is None:
                continue
            data = extracted.read()
            text = data.decode("utf-8", errors="replace")
            assert _SENTINEL_BEARER not in text, (
                f"export.tar entry {member.name!r} contains the sentinel bearer"
            )
            assert "Bearer " not in text, (
                f"export.tar entry {member.name!r} contains 'Bearer ' substring"
            )
            assert "Authorization" not in text, (
                f"export.tar entry {member.name!r} contains 'Authorization' substring"
            )
            assert _JWT_RE.search(text) is None, (
                f"export.tar entry {member.name!r} contains JWT-shaped substring"
            )


def test_admin_token_list_omits_bearer_field(app_with_token: tuple[FastAPI, str]) -> None:
    """The TokenSlot list response has no bearer/token key in any slot.

    Structural counterpart to the substring scan: even if a future
    refactor renames the bearer field, this catches the rename if
    it doesn't drop the value entirely.
    """
    app, _chain_id = app_with_token
    client = TestClient(app)
    response = client.get("/v1/admin/tokens")
    assert response.status_code == 200
    payload: dict[str, Any] = response.json()
    assert "tokens" in payload
    for slot in payload["tokens"]:
        assert "bearer" not in slot, f"TokenSlot has a 'bearer' key: {slot}"
        assert "token" not in slot, f"TokenSlot has a 'token' key: {slot}"
        # The known-good keys are exactly these (ADR-004).
        assert set(slot.keys()) == {"endpoint", "uid", "last_updated", "status"}
