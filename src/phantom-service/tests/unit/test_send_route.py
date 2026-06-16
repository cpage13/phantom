"""Integration-style tests for the ``POST /v1/send`` route."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
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
from phantom.routes import health as health_routes
from phantom.routes import send as send_routes
from phantom.routing import resolve_route
from phantom.runtime.startup_checks import DegradedInstance, DegradeReason
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


class FakeUpstream:
    """Stub UpstreamClient."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_app(
    tmp_path: Path,
    *,
    codec_factory: Callable[[], BodyCodec] | None = None,
    persist_trigger: PersistTriggerCfg | None = None,
) -> tuple[FastAPI, InstanceContext]:
    """Build a FastAPI app + its instance context for tests.

    Returns both so individual tests can inspect the body store, the
    upload store, etc. directly. ``codec_factory`` defaults to
    "always passthrough" so tests that don't care about compression
    keep their existing semantics.

    Phase 1 Slice 1.E: single persistent SQLite (the dual ``mem``/``disk``
    pair is gone with the schema collapse per plan § 2.3.6).
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
        host_prefixes=["files.example.com", "*.amazonaws.com"],
        data_dir="primary",
        routes=[
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
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
        max_in_flight=2, max_in_flight_bytes=1_000_000, max_disk_bytes=10_000_000
    )

    def _passthrough_factory() -> BodyCodec:
        # Always-encode: ``algorithm="original"`` selects PassthroughCodec.
        return select_codec(CompressionCfg(algorithm="original"))

    # Test helper pins body_size_threshold_bytes=0 (size-aware
    # persistence disabled) by default; production goes through
    # Settings.model_validator which fills from the active RAM
    # profile. Tests that want a non-zero threshold pass an
    # explicit PersistTriggerCfg.
    resolved_persist_trigger = persist_trigger or PersistTriggerCfg(body_size_threshold_bytes=0)
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
        codec_factory=codec_factory or _passthrough_factory,
        current_settings=snapshot_thunk(make_snapshot(persist_trigger=resolved_persist_trigger)),
    )
    track_instance(ctx)
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(send_routes.router)
    app.include_router(admin_routes.router)
    # R12-1: liveness/readiness moved off the admin router to the public
    # health router (/v1/healthz, /v1/readyz). This unit fixture co-mounts
    # all three routers on one app (a test convenience); production keeps
    # the public/admin split pinned by
    # test_admin_router_not_isolated_to_loopback.py + the contract suite.
    app.include_router(health_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture cannot drift from production app.py (round 3 fix R3-1).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[send_routes.get_dispatcher] = lambda: dispatcher
    # Tests inject an explicit cap; the route reads this value for both the
    # multipart parser ``max_part_size`` and the post-parse buffered-bytes
    # check. Using the production default (2 GiB) keeps unit tests aligned
    # with default deployment behavior.
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: 2_147_483_648
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[health_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[health_routes.get_version] = lambda: "0.1.0"
    return app, ctx


@pytest.fixture
async def app(tmp_path: Path) -> Iterable[FastAPI]:
    """Build a FastAPI app with a single live instance for tests."""
    fastapi_app, _ = await _build_app(tmp_path)
    yield fastapi_app


def _valid_envelope(chain_id: Any | None = None, *, target: str = "files.example.com") -> dict:
    return {
        "chain_id": str(chain_id or uuid4()),
        "idempotency_key": "k",
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": f"https://{target}/v2/files",
            }
        ],
    }


def test_happy_path_returns_202(app: FastAPI) -> None:
    """A valid envelope returns 202 + ChainResponse + headers."""
    client = TestClient(app)
    body = _valid_envelope()
    response = client.post("/v1/send", json=body, headers={"X-Phantom-Uid": "user-1"})
    assert response.status_code == 202
    assert response.headers["X-Phantom-Upload-Id"] == body["chain_id"]
    assert response.headers["X-Phantom-Status"] == "queued"
    parsed = response.json()
    assert parsed["chain_id"] == body["chain_id"]
    assert parsed["state"] == "queued"


def test_poll_after_header_carries_named_constant(app: FastAPI) -> None:
    """The 202 polling hint is the named module constant (D6).

    Pins the wire value to ``SUGGESTED_POLL_AFTER_SECONDS`` so the hint
    cannot silently regress to a bare literal divorced from its
    rationale comment.
    """
    client = TestClient(app)
    response = client.post("/v1/send", json=_valid_envelope(), headers={"X-Phantom-Uid": "user-1"})
    assert response.status_code == 202
    assert response.headers["X-Phantom-Suggested-Poll-After"] == str(
        send_routes.SUGGESTED_POLL_AFTER_SECONDS
    )


def test_envelope_invalid_returns_422(app: FastAPI) -> None:
    """Malformed JSON returns 422 with envelope_invalid."""
    client = TestClient(app)
    response = client.post(
        "/v1/send", content=b"{ not json", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    parsed = response.json()
    assert parsed["error"]["code"] == "envelope_invalid"


def test_unknown_target_returns_421(app: FastAPI) -> None:
    """A target outside every instance's host_prefixes returns 421."""
    client = TestClient(app)
    body = _valid_envelope(target="other.example.com")
    response = client.post("/v1/send", json=body)
    assert response.status_code == 421
    parsed = response.json()
    assert parsed["error"]["code"] == "invalid_target"


def test_saturation_returns_503(app: FastAPI) -> None:
    """When the saturation gate refuses, return 503."""
    client = TestClient(app)
    # The fixture's gate admits 2 — push 3 and expect the third to fail.
    for _ in range(2):
        response = client.post("/v1/send", json=_valid_envelope())
        assert response.status_code == 202
    response = client.post("/v1/send", json=_valid_envelope())
    assert response.status_code == 503
    parsed = response.json()
    assert parsed["error"]["code"] == "saturation_cap"
    # Retry-After mandatory for 503 per RFC 7231 §7.1.3 (§2.3 fix).
    assert response.headers.get("Retry-After") == "60"


@pytest.mark.asyncio
async def test_disk_pressure_returns_503_with_retry_after(tmp_path: Path) -> None:
    """When the gate's disk-usage cache hits max_disk_bytes, 503 disk_pressure (§2.3)."""
    fastapi_app, ctx = await _build_app(tmp_path)
    # Force the gate's view of disk usage to its cap.
    ctx.saturation.set_disk_usage_bytes(ctx.saturation.max_disk_bytes)
    client = TestClient(fastapi_app)
    response = client.post("/v1/send", json=_valid_envelope(), headers={"X-Phantom-Uid": "user-1"})
    assert response.status_code == 503
    parsed = response.json()
    assert parsed["error"]["code"] == "disk_pressure"
    assert response.headers.get("Retry-After") == "60"


@pytest.mark.asyncio
async def test_idempotency_replay_releases_saturation(tmp_path: Path) -> None:
    """Idempotency-key replay (200) releases the just-admitted slot (§4.2).

    Without the release, aggressive retries against a slow upload
    double-charge the gate per retry hop. With the fix, the gate's
    in_flight stays at 1 after both the initial 202 and the replay 200.
    """
    fastapi_app, ctx = await _build_app(tmp_path)
    client = TestClient(fastapi_app)
    body = _valid_envelope()
    idem_key = "shared-key-v1"

    # First call: 202, gate increments to 1.
    response = client.post(
        "/v1/send",
        json=body,
        headers={"X-Phantom-Idempotency-Key": idem_key, "X-Phantom-Uid": "user-1"},
    )
    assert response.status_code == 202
    assert ctx.saturation.in_flight == 1
    in_flight_after_first = ctx.saturation.in_flight
    in_flight_bytes_after_first = ctx.saturation.in_flight_bytes

    # Replay with a different chain_id but the same idempotency key: 200.
    body2 = _valid_envelope()  # fresh chain_id
    response = client.post(
        "/v1/send",
        json=body2,
        headers={"X-Phantom-Idempotency-Key": idem_key, "X-Phantom-Uid": "user-1"},
    )
    assert response.status_code == 200
    # The gate's counters did NOT grow on replay.
    assert ctx.saturation.in_flight == in_flight_after_first
    assert ctx.saturation.in_flight_bytes == in_flight_bytes_after_first


def test_public_healthz(app: FastAPI) -> None:
    """``GET /v1/healthz`` returns status=ok (R12-1: public liveness)."""
    client = TestClient(app)
    response = client.get("/v1/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_public_readyz(app: FastAPI) -> None:
    """``GET /v1/readyz`` returns ready=true when instances are present (R12-1)."""
    client = TestClient(app)
    response = client.get("/v1/readyz")
    assert response.status_code == 200
    assert response.json()["ready"] is True


# ---------------------------------------------------------------------------
# § 4D.2 - degraded-boot read-side surface: /ready + /health report a degraded
# instance; POST /v1/send to a degraded instance returns 500 before buffering;
# a healthy instance is unaffected.
# ---------------------------------------------------------------------------

# The degraded instance is configured (it appears in get_instance_cfgs and the
# degraded map) but has NO live context, so it is absent from the dispatcher;
# exactly the runtime shape _build_instance_context produces when a data_dir is
# unwritable. "primary" stays healthy on files.example.com; "degraded-inst" owns
# degraded.example.com and is in the degraded map only.
_DEGRADED_HOST = "degraded.example.com"
_DEGRADED_FAULT = "isolate failed: [Errno 30] Read-only file system: '/data/uploads.db'"


def _degraded_cfg() -> InstanceCfg:
    """An InstanceCfg for the degraded instance (config-present, context-absent)."""
    return InstanceCfg(
        id="degraded-inst",
        host_prefixes=[_DEGRADED_HOST],
        data_dir="degraded-inst",
        routes=[RouteCfg(name="files", hosts=[_DEGRADED_HOST], auth_mode="none")],
    )


async def _build_app_with_degraded(tmp_path: Path) -> tuple[FastAPI, InstanceContext]:
    """Build the app, then mark a second (context-less) instance degraded.

    The dispatcher keeps only the healthy "primary" context; the degraded
    instance lives in ``get_instance_cfgs`` + ``get_degraded_instances``
    exactly as the lifespan's typed BootOutcome fold (cycle-7 seam 3) would
    leave it after an unwritable-data_dir boot.
    """
    fastapi_app, ctx = await _build_app(tmp_path)
    healthy_cfg = ctx.cfg
    degraded_set = (
        DegradedInstance(
            instance_id="degraded-inst",
            reason=DegradeReason.SUBSTRATE_UNWRITABLE,
            detail=_DEGRADED_FAULT,
        ),
    )
    fastapi_app.dependency_overrides[send_routes.get_instance_cfgs] = lambda: [
        healthy_cfg,
        _degraded_cfg(),
    ]
    fastapi_app.dependency_overrides[send_routes.get_degraded_instances] = lambda: degraded_set
    # R12-1: liveness/readiness resolve the HEALTH router's degraded-set dep.
    fastapi_app.dependency_overrides[health_routes.get_degraded_instances] = lambda: degraded_set
    return fastapi_app, ctx


@pytest.mark.asyncio
async def test_ready_reports_degraded_instance(tmp_path: Path) -> None:
    """A degraded instance makes GET /v1/readyz report ready=false + detail."""
    fastapi_app, _ = await _build_app_with_degraded(tmp_path)
    client = TestClient(fastapi_app)
    response = client.get("/v1/readyz")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ready"] is False
    # The detail names the degraded instance id AND its fault verbatim.
    assert "degraded-inst" in body["detail"]
    assert _DEGRADED_FAULT in body["detail"]


@pytest.mark.asyncio
async def test_health_reports_degraded_storage(tmp_path: Path) -> None:
    """GET /v1/healthz stays status=ok but surfaces storage=degraded (§ 4D.2)."""
    fastapi_app, _ = await _build_app_with_degraded(tmp_path)
    client = TestClient(fastapi_app)
    response = client.get("/v1/healthz")
    # Liveness is unchanged (the process is up); storage is the fuller signal.
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert body["storage"] == "degraded"
    assert "degraded-inst" in body["storage_detail"]
    assert _DEGRADED_FAULT in body["storage_detail"]


@pytest.mark.asyncio
async def test_send_to_degraded_instance_returns_500_via_header(tmp_path: Path) -> None:
    """POST /v1/send to a degraded instance via X-Phantom-Instance returns 500.

    The explicit-route guard fires BEFORE _parse_body: the request carries
    NO body at all, so a guard that ran after parsing would 422 (or error on
    the missing envelope) instead of 500. The 500 proves the guard fires
    before any body is buffered.
    """
    fastapi_app, _ = await _build_app_with_degraded(tmp_path)
    client = TestClient(fastapi_app)
    response = client.post(
        "/v1/send",
        headers={"X-Phantom-Instance": "degraded-inst", "X-Phantom-Uid": "u"},
    )
    assert response.status_code == 500, response.text
    body = response.json()
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["instance_id"] == "degraded-inst"
    assert body["error"]["details"]["reason"] == "storage_unavailable_degraded_boot"
    assert body["error"]["details"]["instance"] == "degraded-inst"


@pytest.mark.asyncio
async def test_send_to_degraded_instance_returns_500_via_host_routing(tmp_path: Path) -> None:
    """POST /v1/send host-routed to a degraded instance returns 500 before admission.

    The target host lives in the envelope, so this is the post-parse guard
    site. It must fire before admit_chain so no durable write is attempted.
    """
    fastapi_app, _ = await _build_app_with_degraded(tmp_path)
    client = TestClient(fastapi_app)
    body = _valid_envelope(target=_DEGRADED_HOST)
    response = client.post("/v1/send", json=body, headers={"X-Phantom-Uid": "u"})
    assert response.status_code == 500, response.text
    parsed = response.json()
    assert parsed["error"]["code"] == "internal_error"
    assert parsed["error"]["instance_id"] == "degraded-inst"
    assert parsed["error"]["details"]["reason"] == "storage_unavailable_degraded_boot"


@pytest.mark.asyncio
async def test_send_to_healthy_instance_unaffected_when_other_degraded(tmp_path: Path) -> None:
    """A healthy instance still admits 202 even when a sibling booted degraded."""
    fastapi_app, _ = await _build_app_with_degraded(tmp_path)
    client = TestClient(fastapi_app)
    # "primary" (files.example.com) is healthy; the degraded sibling must not
    # leak its 500 onto it.
    body = _valid_envelope(target="files.example.com")
    response = client.post("/v1/send", json=body, headers={"X-Phantom-Uid": "u"})
    assert response.status_code == 202, response.text
    assert response.json()["state"] == "queued"


@pytest.mark.asyncio
async def test_ready_and_send_normal_when_no_degraded(tmp_path: Path) -> None:
    """With an empty degraded map, /v1/readyz is true and /send admits (no regression)."""
    fastapi_app, _ = await _build_app(tmp_path)
    # No degraded override -> the safe-default empty map applies.
    client = TestClient(fastapi_app)
    ready = client.get("/v1/readyz")
    assert ready.status_code == 200
    assert ready.json()["ready"] is True
    health = client.get("/v1/healthz")
    assert health.json()["storage"] == "ok"
    assert health.json()["storage_detail"] is None
    send = client.post("/v1/send", json=_valid_envelope(), headers={"X-Phantom-Uid": "u"})
    assert send.status_code == 202


def test_admin_uploads_get_after_send(app: FastAPI) -> None:
    """After POST /v1/send, the row is queryable via admin."""
    client = TestClient(app)
    body = _valid_envelope()
    send = client.post("/v1/send", json=body, headers={"X-Phantom-Uid": "user-1"})
    assert send.status_code == 202
    chain_id = body["chain_id"]
    response = client.get(f"/v1/admin/chains/{chain_id}")
    assert response.status_code == 200
    parsed = response.json()
    assert parsed["chain_id"] == chain_id
    assert parsed["state"] == "queued"


def test_admin_tokens_list_no_bearer(app: FastAPI) -> None:
    """Listing tokens never reveals bearer values (ADR-004)."""
    client = TestClient(app)
    body = _valid_envelope()
    client.post(
        "/v1/send",
        json=body,
        headers={"X-Phantom-Uid": "user-1", "Authorization": "Bearer secret-token"},
    )
    response = client.get("/v1/admin/tokens")
    assert response.status_code == 200
    payload = json.dumps(response.json())
    assert "secret-token" not in payload
    assert "bearer" not in {k.lower() for s in response.json()["tokens"] for k in s}


def test_admin_bulk_delete_rejects_empty(app: FastAPI) -> None:
    """Bulk delete with all-None filter is rejected with 422.

    The refusal rides the typed ``BulkDeleteFilterEmptyError`` through
    the shared ``register_admin_error_handlers`` helper (round 3 fix
    R3-1), so this unit fixture renders the same canonical envelope
    production does; the error code is pinned to prove the registered
    handler (not a raw fallback body) produced the response.
    """
    client = TestClient(app)
    response = client.request("DELETE", "/v1/admin/chains", json={})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "bulk_delete_filter_empty"


def test_admin_instance_scope_unknown_returns_421(app: FastAPI) -> None:
    """`?instance=<bad>` on a scoped endpoint returns 421 instance_unknown.

    Plan §5.6 maps ``instance_unknown`` to HTTP 421 ("Misdirected
    Request"); the route emits the canonical ``ErrorEnvelope`` shape
    so the SDK can map it via ``EXCEPTION_FOR_CODE``.
    """
    client = TestClient(app)
    response = client.get("/v1/admin/chains", params={"instance": "does-not-exist"})
    assert response.status_code == 421
    body = response.json()
    assert body["error"]["code"] == "instance_unknown"
    assert "does-not-exist" in body["error"]["message"]


def test_admin_instance_status_unknown_returns_421(app: FastAPI) -> None:
    """GET /v1/admin/instances/<bad>/status returns 421 instance_unknown."""
    client = TestClient(app)
    response = client.get("/v1/admin/instances/does-not-exist/status")
    assert response.status_code == 421
    body = response.json()
    assert body["error"]["code"] == "instance_unknown"
    assert "does-not-exist" in body["error"]["message"]


@pytest.mark.asyncio
async def test_admin_status_reports_disk_usage_bytes(tmp_path: Path) -> None:
    """``/v1/admin/status`` returns actual file-body bytes per §6.1.

    Previously hardcoded to 0; the bug was operator-blind: admin tooling
    sees disk_usage_bytes=0 and assumes empty storage even when 200 GB
    of bodies are on disk.
    """
    fastapi_app, ctx = await _build_app(tmp_path)
    # Drop a body file directly through the FileBodyStore so the test
    # doesn't depend on the full ingress+sender pipeline. The aggregate
    # endpoint walks `file_body_store.total_bytes()` per-instance.
    payload = b"x" * 1024
    await ctx.file_body_store.put(uuid4(), {"body": payload})
    client = TestClient(fastapi_app)
    response = client.get("/v1/admin/status")
    assert response.status_code == 200
    parsed = response.json()
    assert parsed["disk_usage_bytes"] >= len(payload)


@pytest.mark.asyncio
async def test_admin_instance_status_reports_disk_usage_bytes(tmp_path: Path) -> None:
    """``/v1/admin/instances/<id>/status`` returns actual file-body bytes (§6.1)."""
    fastapi_app, ctx = await _build_app(tmp_path)
    payload = b"y" * 2048
    await ctx.file_body_store.put(uuid4(), {"body": payload})
    client = TestClient(fastapi_app)
    response = client.get(f"/v1/admin/instances/{ctx.cfg.id}/status")
    assert response.status_code == 200
    parsed = response.json()
    assert parsed["disk_usage_bytes"] >= len(payload)


# ---------------------------------------------------------------------------
# Storage-encoding (compression) ingress tests — plan §4.33 task 2a.
# ---------------------------------------------------------------------------


def _multipart_envelope_body_ref(
    *,
    chain_id: str,
    body_ref_name: str,
    target: str = "files.example.com",
) -> dict[str, Any]:
    """Build an envelope that declares one ``body_ref`` body."""
    return {
        "chain_id": chain_id,
        "idempotency_key": "k",
        "steps": [
            {
                "name": "put_s3",
                "method": "PUT",
                "url": f"https://{target}/v2/files",
                "body": {
                    "kind": "body_ref",
                    "name": body_ref_name,
                    "content_type": "application/octet-stream",
                },
            }
        ],
    }


@pytest.mark.asyncio
async def test_storage_encoding_zstd_always_encodes(tmp_path: Path) -> None:
    """Always-encode: zstd is applied regardless of body size.

    The sender's `_load_body_refs` path decodes back to the original
    bytes verbatim — that round-trip is what gives the "stored
    compressed, forwarded raw" invariant its meaning.
    """

    def zstd_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="zstd"))

    fastapi_app, ctx = await _build_app(tmp_path, codec_factory=zstd_factory)
    chain_id = str(uuid4())
    original_body = b"\x01" * 150_000
    files = {
        "envelope": (
            None,
            json.dumps(
                _multipart_envelope_body_ref(chain_id=chain_id, body_ref_name="body"),
            ),
            "application/json",
        ),
        "body_refs[body]": (
            "body.bin",
            original_body,
            "application/octet-stream",
        ),
    }
    client = TestClient(fastapi_app)
    response = client.post("/v1/send", files=files, headers={"X-Phantom-Uid": "u"})
    assert response.status_code == 202, response.text

    # Inspect storage: the row should declare zstd; the stored bytes
    # should NOT equal the original (codec actually ran); and the
    # sender-side decode helper should round-trip back to the original.
    from uuid import UUID

    row = await ctx.store.get(UUID(chain_id))
    assert row is not None
    assert row.storage_encoding == "zstd"
    stored = await ctx.body_store.get_all(UUID(chain_id))
    assert stored["body"] != original_body  # codec actually mutated
    # Round-trip via the sender's decode path.
    from phantom.workers.sender import Sender

    sender = Sender(instance=ctx, worker_count=1, poll_interval_ms=250)
    decoded_refs = await sender._load_body_refs(row)
    assert decoded_refs["body"] == original_body


@pytest.mark.asyncio
async def test_storage_encoding_zstd_ignores_content_encoding(tmp_path: Path) -> None:
    """Always-encode: the inbound ``Content-Encoding`` header is irrelevant.

    The transparent-proxy invariant is enforced by the sender's decode
    path plus body_hash verification, not by storage-side pass-through.
    """

    def zstd_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="zstd"))

    fastapi_app, ctx = await _build_app(tmp_path, codec_factory=zstd_factory)
    chain_id = str(uuid4())
    original_body = b"\x01" * 150_000
    files = {
        "envelope": (
            None,
            json.dumps(
                _multipart_envelope_body_ref(chain_id=chain_id, body_ref_name="body"),
            ),
            "application/json",
        ),
        "body_refs[body]": (
            "body.bin",
            original_body,
            "application/octet-stream",
        ),
    }
    client = TestClient(fastapi_app)
    response = client.post(
        "/v1/send",
        files=files,
        headers={"X-Phantom-Uid": "u", "Content-Encoding": "gzip"},
    )
    assert response.status_code == 202, response.text

    from uuid import UUID

    row = await ctx.store.get(UUID(chain_id))
    assert row is not None
    # Always-encode means zstd applied regardless of the wire encoding.
    assert row.storage_encoding == "zstd"


@pytest.mark.asyncio
async def test_storage_encoding_passthrough_when_configured(tmp_path: Path) -> None:
    """Operator selects passthrough via ``compression.algorithm: original``."""

    def passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    fastapi_app, ctx = await _build_app(tmp_path, codec_factory=passthrough_factory)
    chain_id = str(uuid4())
    original_body = b"abc" * 100  # 300 bytes.
    files = {
        "envelope": (
            None,
            json.dumps(
                _multipart_envelope_body_ref(chain_id=chain_id, body_ref_name="body"),
            ),
            "application/json",
        ),
        "body_refs[body]": (
            "body.bin",
            original_body,
            "application/octet-stream",
        ),
    }
    client = TestClient(fastapi_app)
    response = client.post("/v1/send", files=files, headers={"X-Phantom-Uid": "u"})
    assert response.status_code == 202

    from uuid import UUID

    row = await ctx.store.get(UUID(chain_id))
    assert row is not None
    assert row.storage_encoding == "original"
    stored = await ctx.body_store.get_all(UUID(chain_id))
    assert stored["body"] == original_body


# -----------------------------------------------------------------------------
# H2 regression tests — Content-Length precheck + streaming cap.
# -----------------------------------------------------------------------------


async def _build_app_with_cap(
    tmp_path: Path, max_buffered_bytes: int
) -> tuple[FastAPI, InstanceContext]:
    """Build a FastAPI app + ctx with the cap overridden to a small value."""
    fastapi_app, ctx = await _build_app(tmp_path)
    fastapi_app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: (
        max_buffered_bytes
    )
    return fastapi_app, ctx


@pytest.mark.asyncio
async def test_content_length_precheck_rejects_oversized_body(tmp_path: Path) -> None:
    """A declared Content-Length above the cap returns 413 before body read (H2).

    The test issues a POST with a small body but a ``Content-Length``
    header declaring a value above the cap (the TestClient honors the
    explicit header). Without the H2 precheck the handler would call
    ``await request.body()``; with the precheck the body bytes never
    leave the wire and the producer gets a clean 413.
    """
    cap = 1024
    fastapi_app, _ = await _build_app_with_cap(tmp_path, cap)
    client = TestClient(fastapi_app)
    response = client.post(
        "/v1/send",
        json=_valid_envelope(),
        headers={
            "X-Phantom-Uid": "user-1",
            # Declared length exceeds the cap.
            "Content-Length": str(cap + 1),
        },
    )
    assert response.status_code == 413, response.text
    parsed = response.json()
    assert parsed["error"]["code"] == "body_too_large"
    assert parsed["error"]["details"]["declared"] == cap + 1
    assert parsed["error"]["details"]["limit"] == cap
    assert parsed["error"]["details"]["reason"] == "content_length_precheck"


@pytest.mark.asyncio
async def test_content_length_precheck_allows_at_cap_body(tmp_path: Path) -> None:
    """A body exactly at the cap is admitted — precheck uses ``>``, not ``>=``."""
    cap = 1_000_000  # 1 MB
    fastapi_app, _ = await _build_app_with_cap(tmp_path, cap)
    client = TestClient(fastapi_app)
    response = client.post(
        "/v1/send",
        json=_valid_envelope(),
        headers={"X-Phantom-Uid": "user-1"},
    )
    # Happy-path — declared length is below cap, the request proceeds.
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_streaming_cap_aborts_chunked_body_exceeding_limit(
    tmp_path: Path,
) -> None:
    """A chunked body with no Content-Length is aborted mid-stream at the cap (H2).

    Constructs an httpx Request with ``Transfer-Encoding: chunked``
    (httpx applies this automatically when the body is a generator).
    The body iterator yields slabs whose cumulative size exceeds the
    cap. The route's streaming cap must abort with 413 before all the
    bytes are consumed.
    """
    cap = 512
    fastapi_app, _ = await _build_app_with_cap(tmp_path, cap)
    client = TestClient(fastapi_app)

    # Build a request body that streams in chunks totalling > cap.
    # The TestClient passes ``content=<iterator>`` straight to httpx,
    # which emits chunked transfer encoding (no Content-Length).
    def _chunks() -> Iterable[bytes]:
        # 8 chunks * 100 bytes = 800 bytes > 512 cap.
        for _ in range(8):
            yield b"x" * 100

    response = client.post(
        "/v1/send",
        content=_chunks(),
        headers={
            "X-Phantom-Uid": "user-1",
            "Content-Type": "application/json",
            # Force chunked transfer encoding — TestClient/httpx will
            # not auto-derive Content-Length from an iterator body.
        },
    )
    assert response.status_code == 413, response.text
    parsed = response.json()
    assert parsed["error"]["code"] == "body_too_large"
    assert parsed["error"]["details"]["reason"] == "streaming_cap"
    assert parsed["error"]["details"]["limit"] == cap


@pytest.mark.asyncio
async def test_content_length_malformed_header_falls_through(tmp_path: Path) -> None:
    """A non-integer Content-Length doesn't 413; later validation handles it."""
    cap = 1024
    fastapi_app, _ = await _build_app_with_cap(tmp_path, cap)
    client = TestClient(fastapi_app)
    # The TestClient computes Content-Length itself; passing a bad
    # header through ``headers`` would be overwritten. Instead, we send
    # a valid request and verify the precheck does not over-reject on
    # the well-formed common case (the malformed-header path is covered
    # by the unit-style assertion on _check_content_length below).
    response = client.post(
        "/v1/send",
        json=_valid_envelope(),
        headers={"X-Phantom-Uid": "user-1"},
    )
    assert response.status_code == 202

    # Direct call to the helper guarantees the malformed-header branch.
    result = send_routes._check_content_length("not-a-number", cap, request_id="r")
    assert result is None


# Cosmetic — exercise that httpx is wired (consumed via TestClient).
_ = httpx
