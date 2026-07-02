"""Integration + unit tests for the raw-intake catch-all (Phase 1 TASK 1.1-1.3).

Covers the three cohesive pieces of the routing core:

* TASK 1.1 — the catch-all ``/{phantom_path:path}`` route: upload verbs land,
  the fixed ``/v1/*`` routes stay unshadowed, the reserved-prefix guard 404s
  ``/v1/...``, and unknown GET/HEAD/DELETE/OPTIONS stay 404 (not 405).
* TASK 1.2 — the raw->envelope adapter: a 1-step envelope is synthesized,
  buffered through the shared ``resolve_and_admit`` prelude, the persisted
  step strips ``X-Phantom-*`` markers but keeps ``Authorization``, and a fresh
  per-request dedup key means identical raw PUTs never replay.
* TASK 1.3 — destination resolution: ``?phantom=<url>`` and
  ``phantom_default_target`` carriers (first-hit precedence), and the
  no-destination / empty-path 421 BEFORE any durable write.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import CompressionCfg, InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.routes import admin as admin_routes
from phantom.routes import catch_all as catch_all_routes
from phantom.routes import health as health_routes
from phantom.routes import send as send_routes
from phantom.routing import resolve_route
from phantom.storage import FileBodyStore, RamBodyStore, SqliteTokenCache, SqliteUploadStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

# The resolved-host the carriers point at; matches the fixture instance's
# host_prefixes so dispatch routes to "primary".
_TARGET_HOST = "files.example.com"
_DEFAULT_TARGET = f"https://{_TARGET_HOST}"


class FakeUpstream:
    """Stub UpstreamClient (admission never forwards; this is unused at ingress)."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_app(
    tmp_path: Path,
    *,
    default_target: str | None = None,
) -> tuple[FastAPI, InstanceContext]:
    """Build a FastAPI app with the catch-all router mounted, plus its ctx.

    Mirrors ``test_send_route._build_app`` but additionally mounts the
    raw-intake catch-all LAST and wires its ``get_phantom_default_target``
    DI. ``default_target`` parameterizes the second destination carrier.
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
        host_prefixes=[_TARGET_HOST, "*.amazonaws.com"],
        data_dir="primary",
        routes=[
            RouteCfg(name="files", hosts=[_TARGET_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="s3", hosts=["*.amazonaws.com"], auth_mode="none"),
        ],
    )
    upstream = FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=default_clock,
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=100_000_000
    )

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

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
        current_settings=snapshot_thunk(
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    track_instance(ctx)
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(send_routes.router)
    app.include_router(admin_routes.router)
    app.include_router(health_routes.router)
    # The catch-all registers LAST, exactly as production app.py does, so the
    # fixed /v1 routers win first-match for their paths.
    app.include_router(catch_all_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[send_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: 2_147_483_648
    app.dependency_overrides[send_routes.get_phantom_default_target] = lambda: default_target
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[health_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[health_routes.get_version] = lambda: "0.1.0"
    return app, ctx


# ---------------------------------------------------------------------------
# TASK 1.1 — the route shell: landing, registration order, guards, 404 arm.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_put_lands_and_buffers(tmp_path: Path) -> None:
    """A raw PUT reaches the adapter, admits, and persists a queued row."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    body = b"hello-object-bytes"
    response = client.put("/mybucket/mykey", content=body)
    assert response.status_code == 202, response.text
    upload_id = response.headers["X-Phantom-Upload-Id"]
    row = await ctx.store.get(UUID(upload_id))
    assert row is not None
    assert row.state == "queued"
    # The destination host was rewritten to the REAL upstream before dispatch.
    assert row.endpoint == _TARGET_HOST
    assert row.route_name == "files"
    assert row.body_size_bytes == len(body)


@pytest.mark.asyncio
async def test_fixed_send_route_unshadowed(tmp_path: Path) -> None:
    """POST /v1/send still admits 202 — the catch-all does not shadow it."""
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    envelope = {
        "chain_id": "00000000-0000-4000-8000-000000000001",
        "idempotency_key": "k",
        "steps": [{"name": "create", "method": "POST", "url": f"https://{_TARGET_HOST}/v2/files"}],
    }
    response = client.post("/v1/send", json=envelope, headers={"X-Phantom-Uid": "u"})
    assert response.status_code == 202, response.text


@pytest.mark.asyncio
async def test_fixed_health_route_unshadowed(tmp_path: Path) -> None:
    """GET /v1/healthz resolves to the health route, not the catch-all.

    GET is unbound on the upload arm, so even an exact path collision could
    not steal it; this pins the fixed-route precedence end to end.
    """
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.get("/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_reserved_prefix_put_is_404(tmp_path: Path) -> None:
    """PUT /v1/anything 404s via the reserved-prefix guard (not treated as upload)."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put("/v1/anything", content=b"x")
    assert response.status_code == 404
    # No row was written for the reserved-prefix request.
    assert await _store_is_empty(ctx)


@pytest.mark.asyncio
@pytest.mark.parametrize("segment", ["v2", "oauth", "control", ".well-known"])
async def test_forward_reserved_prefixes_are_404(tmp_path: Path, segment: str) -> None:
    """The forward-reserved first segments also 404 (never captured as uploads)."""
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put(f"/{segment}/whatever", content=b"x")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_unknown_get_is_404_not_405(tmp_path: Path) -> None:
    """GET /bucket/key returns 404, not 405 — the complementary-method arm.

    The root :path route is bound to the upload verbs; without the 404 arm a
    GET to a matching path would surface 405 service-wide. The arm restores
    the prior 404 behavior for unknown non-upload requests.
    """
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    for method in ("GET", "HEAD", "DELETE", "OPTIONS"):
        response = client.request(method, "/some/unknown/path")
        assert response.status_code == 404, f"{method} expected 404, got {response.status_code}"


# ---------------------------------------------------------------------------
# TASK 1.3 — destination resolution + the no-destination 421.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phantom_query_carrier_resolves(tmp_path: Path) -> None:
    """?phantom=<full-url> rewrites the destination to the real host."""
    app, ctx = await _build_app(tmp_path, default_target=None)
    client = TestClient(app)
    response = client.put(
        f"/mybucket/mykey?phantom=https://{_TARGET_HOST}/mybucket/mykey",
        content=b"abc",
    )
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.endpoint == _TARGET_HOST


@pytest.mark.asyncio
async def test_default_target_resolves(tmp_path: Path) -> None:
    """With phantom_default_target set and no ?phantom, the default is used."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put("/mybucket/mykey", content=b"abc")
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.endpoint == _TARGET_HOST
    # The full object path was appended after the default host-prefix.
    from phantom.models.chain import ChainEnvelope

    env = ChainEnvelope.model_validate_json(row.chain_envelope_json)
    assert env.steps[0].url == f"https://{_TARGET_HOST}/mybucket/mykey"


@pytest.mark.asyncio
async def test_phantom_query_wins_over_default(tmp_path: Path) -> None:
    """?phantom= takes first-hit precedence over a configured default target."""
    # Default points at the amazonaws route; the explicit carrier points at
    # files.example.com. The persisted endpoint proves which one won.
    app, ctx = await _build_app(tmp_path, default_target="https://other.amazonaws.com")
    client = TestClient(app)
    response = client.put(
        f"/b/k?phantom=https://{_TARGET_HOST}/b/k",
        content=b"abc",
    )
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.endpoint == _TARGET_HOST  # the ?phantom host, not other.amazonaws.com


@pytest.mark.asyncio
async def test_no_destination_is_421_no_write(tmp_path: Path) -> None:
    """No ?phantom and no default target -> 421 invalid_target, nothing persisted."""
    app, ctx = await _build_app(tmp_path, default_target=None)
    client = TestClient(app)
    response = client.put("/mybucket/mykey", content=b"abc")
    assert response.status_code == 421
    assert response.json()["error"]["code"] == "invalid_target"
    assert await _store_is_empty(ctx)


@pytest.mark.asyncio
async def test_empty_path_is_421_no_write(tmp_path: Path) -> None:
    """Bare PUT / (empty path) is unroutable -> 421, even with a default set."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put("/", content=b"abc")
    assert response.status_code == 421
    assert response.json()["error"]["code"] == "invalid_target"
    assert await _store_is_empty(ctx)


@pytest.mark.asyncio
async def test_empty_path_with_phantom_carrier_resolves(tmp_path: Path) -> None:
    """PUT /?phantom=<url> still resolves — the explicit carrier wins on empty path."""
    app, ctx = await _build_app(tmp_path, default_target=None)
    client = TestClient(app)
    response = client.put(f"/?phantom=https://{_TARGET_HOST}/b/k", content=b"abc")
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.endpoint == _TARGET_HOST


@pytest.mark.asyncio
async def test_resolved_host_never_loops_back(tmp_path: Path) -> None:
    """The persisted endpoint is the real upstream, never Phantom's own host.

    The loop hazard the resolver exists to prevent: if steps[0].url kept
    Phantom's bind host, the executor would forward back to Phantom forever.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put("/mybucket/mykey", content=b"abc")
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.endpoint not in {"phantom", "testserver", "localhost"}
    assert row.endpoint == _TARGET_HOST


# ---------------------------------------------------------------------------
# TASK 1.2 — the raw->envelope adapter: synthesis, marker strip, dedup key.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_synthesized_envelope_shape_and_marker_strip(tmp_path: Path) -> None:
    """The persisted step uses the constant names, strips markers, keeps Authorization."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put(
        "/mybucket/My.Object-Key",
        content=b"payload-bytes",
        headers={
            "Authorization": "AWS4-HMAC-SHA256 Credential=throwaway/...",
            "X-Phantom-Uid": "should-be-stripped",
            "X-Phantom-Instance": "should-be-stripped",
            "X-Amz-Content-Sha256": "abc123",
        },
    )
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    from phantom.models.chain import ChainBodyRef, ChainEnvelope

    env = ChainEnvelope.model_validate_json(row.chain_envelope_json)
    step = env.steps[0]
    # Constant regex-valid names; bucket/key live only in the URL.
    assert step.name == "upload"
    assert step.method == "PUT"
    assert step.url == f"https://{_TARGET_HOST}/mybucket/My.Object-Key"
    assert isinstance(step.body, ChainBodyRef)
    assert step.body.name == "payload"
    # HTTP header names are case-insensitive; Starlette lowercases them on
    # iteration, so assert against a lowercased view of the forwarded set.
    lowered = {name.lower(): value for name, value in step.headers.items()}
    # Marker strip: no X-Phantom-* survived onto the forwarded step headers.
    assert not any(name.startswith("x-phantom-") for name in lowered)
    # Authorization is forwarded as-is (Phase-1 forward-as-is).
    assert lowered.get("authorization") == "AWS4-HMAC-SHA256 Credential=throwaway/..."
    # A non-reserved upstream header rides through untouched.
    assert lowered.get("x-amz-content-sha256") == "abc123"
    # Host / Content-Length (host-rewriting hops) are not copied onto the step.
    assert "host" not in lowered
    assert "content-length" not in lowered


@pytest.mark.asyncio
async def test_authorization_seeds_token_cache(tmp_path: Path) -> None:
    """The inbound Authorization is cached against the resolved endpoint (uid='')."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.put(
        "/mybucket/mykey",
        content=b"abc",
        headers={"Authorization": "Bearer raw-intake-token"},
    )
    assert response.status_code == 202, response.text
    # uid_header is pinned to "" for the stock client; the cache is keyed on
    # (endpoint, uid="").
    cached = await ctx.token_cache.get(endpoint=_TARGET_HOST, uid="")
    assert cached is not None


@pytest.mark.asyncio
async def test_body_less_request_has_empty_body_refs(tmp_path: Path) -> None:
    """A raw request with no body synthesizes no body ref and stores zero bytes."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    response = client.request("PUT", "/mybucket/mykey")  # no content
    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.body_size_bytes == 0
    from phantom.models.chain import ChainEnvelope

    env = ChainEnvelope.model_validate_json(row.chain_envelope_json)
    assert env.steps[0].body is None


@pytest.mark.asyncio
async def test_fresh_dedup_key_no_replay(tmp_path: Path) -> None:
    """Two identical raw PUTs produce two distinct rows — never an idempotency replay.

    The adapter mints a fresh chain_id (and dedup key) per request, so a stock
    client that cannot send X-Phantom-Idempotency-Key never trips the replay
    path: both calls 202 with different upload ids and both rows persist.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    first = client.put("/mybucket/mykey", content=b"identical-bytes")
    second = client.put("/mybucket/mykey", content=b"identical-bytes")
    assert first.status_code == 202, first.text
    assert second.status_code == 202, second.text
    first_id = first.headers["X-Phantom-Upload-Id"]
    second_id = second.headers["X-Phantom-Upload-Id"]
    assert first_id != second_id  # fresh chain_id per request, not a 200 replay
    assert await ctx.store.get(UUID(first_id)) is not None
    assert await ctx.store.get(UUID(second_id)) is not None


@pytest.mark.asyncio
async def test_post_and_patch_verbs_land(tmp_path: Path) -> None:
    """POST and PATCH also reach the adapter; the synthesized step echoes the method."""
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)
    for method in ("POST", "PATCH"):
        response = client.request(method, "/mybucket/mykey", content=b"x")
        assert response.status_code == 202, response.text
        row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
        assert row is not None
        from phantom.models.chain import ChainEnvelope

        env = ChainEnvelope.model_validate_json(row.chain_envelope_json)
        assert env.steps[0].method == method


# ---------------------------------------------------------------------------
# Unit-level resolver coverage (pure function, no app boot).
# ---------------------------------------------------------------------------


def test_resolver_unit_precedence_and_no_destination() -> None:
    """_resolve_destination: ?phantom wins; empty path is None; default appends path."""
    from starlette.datastructures import Headers
    from starlette.requests import Request

    def _req(query: str) -> Request:
        scope = {
            "type": "http",
            "method": "PUT",
            "path": "/b/k",
            "query_string": query.encode(),
            "headers": Headers({}).raw,
        }
        return Request(scope)

    # ?phantom wins, verbatim, even with a default set.
    assert (
        catch_all_routes._resolve_destination(
            "b/k", _req("phantom=https://files.example.com/b/k"), "https://default.example.com"
        )
        == "https://files.example.com/b/k"
    )
    # Default target appends the path.
    assert (
        catch_all_routes._resolve_destination("b/k", _req(""), "https://default.example.com")
        == "https://default.example.com/b/k"
    )
    # Empty path, no carrier -> None even with a default.
    assert (
        catch_all_routes._resolve_destination("", _req(""), "https://default.example.com") is None
    )
    # No carrier, no default -> None.
    assert catch_all_routes._resolve_destination("b/k", _req(""), None) is None


async def _store_is_empty(ctx: InstanceContext) -> bool:
    """True when the instance's upload store holds no rows."""
    chain_ids = await ctx.store.list_all_chain_ids()
    return len(chain_ids) == 0
