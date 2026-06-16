"""Operational-chaos E2E: a read-only data_dir boots DEGRADED, not crash-looped (§ 5.C / § 4D).

Adversary 2, the unwritable-substrate chaos. This is the ONE physics boundary
the cycle's "always boot durably" property cannot fully honor: with no writable
disk the service can neither isolate, recreate, nor buffer. § 4D defines a LOUD
DEGRADED BOOT instead of a crash-loop: the service STARTS, ``GET /v1/readyz``
-> ``ready:false`` + detail, ``GET /v1/healthz`` -> ``status:"ok"`` but
``storage:"degraded"`` + ``storage_detail``, and ``POST /v1/send`` -> ``500``
``internal_error`` with ``details.reason=="storage_unavailable_degraded_boot"``
so the caller fails over to the upstream endpoint directly.

These drive the REAL ``create_app`` lifespan (entered via
``app.router.lifespan_context``, the same path the unit refusal tests use) with
a genuinely read-only per-instance ``data_root`` (``chmod 0o555``), and make HTTP
requests through an in-process ASGI transport. The read-only dir makes the
boot-open ``store.start()`` (and the post-isolate recreate) raise
``sqlite3.OperationalError("unable to open database file")``; the guard
classifies the unwritable substrate by PROBING ``data_root`` (not by the
exception type), raises ``_StorageSubstrateUnwritableError`` ->
a typed ``DegradedInstance`` outcome, no context built (the degraded path).

* :func:`test_single_instance_readonly_datadir_boots_degraded` - the service
  comes up degraded; ``/ready`` + ``/health`` report the fault; both the
  host-routed and the ``X-Phantom-Instance``-header ``POST /send`` paths 500
  with the exact code + reason.
* :func:`test_multi_instance_one_degraded_one_healthy_isolation` - the SHARPEST
  falsifier: one instance on a read-only dir (degraded) + one on a writable dir
  (healthy). The healthy ``POST /send`` still 202s; the degraded one 500s;
  ``/ready`` is false (any degraded -> not ready); ``/health`` names ONLY the
  degraded id. Proves the per-instance typed degraded set and that the
  guard does not leak across instances.

Public e2e-light lane (plan § 5.0): generic driver shapes + the emulator.

Falsifier: drop the unwritable-substrate degrade branch -> the read-only dir
crashes the lifespan boot (or the guard 500 never fires) -> RED.

SKIP NOTE: a ``chmod 0o555`` dir is bypassable by root, so the read-only fault
would not materialize when the suite runs as uid 0 (some CI images do). The
tests skip under root to stay deterministic.

HISTORY: these tests caught a real service bug (logged in execution_06_07.md
§ 5 Part C). A read-only ``data_dir`` makes ``aiosqlite`` raise
``sqlite3.OperationalError("unable to open database file")``, which is NOT a
subclass of ``OSError``; the boot-open guard's original post-isolate "substrate
unwritable" branch caught only ``OSError``, so the ``OperationalError`` from the
recreate ``start()`` escaped and CRASH-LOOPED the boot - the exact failure § 4D
exists to prevent. The fix (``app._open_db_with_retry_then_isolate`` +
``_data_root_is_unwritable``) classifies the unwritable substrate by PROBING
``data_root`` (a create-then-unlink), not by the Python exception type, so the
real read-only dir now degrades. The § 4D.2 unit test
``test_unwritable_substrate_degrades_boot_no_crash`` was hardened to drive the
same real condition (it had false-passed by monkeypatching the isolate to raise
``OSError`` over a writable dir). These tests now PASS un-xfailed.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import httpx
import pytest
import yaml
from phantom.app import create_app
from phantom.config.settings import Settings, load_settings

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.payloads import build_create_file_request
from .helpers.stack import PHANTOM_CONFIG_PATH, _deep_merge_dict

if TYPE_CHECKING:
    from collections.abc import Mapping

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="chmod 0o555 read-only dir is bypassable by root; the degraded fault "
        "cannot be reproduced as uid 0",
    ),
]

_DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"
_READ_ONLY_MODE = 0o555
_READ_WRITE_MODE = 0o755
# An instance routed to this loopback host; no emulator binds here. The degraded
# guard returns 500 BEFORE any upstream call, so the host need not be live.
_DEGRADED_HOST = "127.0.0.3"


def _prod_settings_from_e2e_yaml(
    *, data_dir: Path, config_overrides: Mapping[str, Any] | None = None
) -> Settings:
    """Build production Settings from the pinned e2e YAML + overrides.

    Mirrors ``test_startup_guards_e2e._prod_settings_from_e2e_yaml``: the YAML on
    disk is untouched; the overlay lands on an in-memory copy serialized to a
    temp file so the full ``load_settings`` validator path runs.
    """
    raw = yaml.safe_load(PHANTOM_CONFIG_PATH.read_text())
    assert isinstance(raw, dict)
    raw.setdefault("storage", {})["data_dir"] = str(data_dir)
    if config_overrides is not None:
        _deep_merge_dict(raw, config_overrides)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False, encoding="utf-8") as f:
        yaml.safe_dump(raw, f)
        merged_path = Path(f.name)
    return load_settings(merged_path)


def _envelope_for_host(*, host: str, port: int, chain_id: UUID) -> dict[str, Any]:
    """Build a serialized 2-step envelope whose first-step host is ``host``.

    The first-step URL's hostname is what the no-header dispatcher resolves an
    instance by, so pointing it at ``host`` routes the request to the instance
    that owns that host_prefix.
    """
    files_api_base = f"http://{host}:{port}"
    request = build_create_file_request(file_name=f"degraded_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=files_api_base,
        local_uuid=chain_id,
    )
    return envelope.model_dump(mode="json")


async def _post_send_multipart(
    client: httpx.AsyncClient,
    *,
    envelope: dict[str, Any],
    body: bytes,
    bearer: str,
    instance_header: str | None = None,
) -> httpx.Response:
    """POST a multipart chain submission (envelope + one body_ref) to ``/v1/send``."""
    files = {
        "envelope": ("envelope.json", json.dumps(envelope).encode(), "application/json"),
        "body_refs[body]": ("body", body, "application/octet-stream"),
    }
    headers = {
        "X-Phantom-Uid": _DEFAULT_SUB,
        "Authorization": f"Bearer {bearer}",
    }
    if instance_header is not None:
        headers["X-Phantom-Instance"] = instance_header
    # Build the multipart body via httpx's encoder by issuing through a request.
    return await client.post("/v1/send", files=files, headers=headers)


def _assert_degraded_send_500(resp: httpx.Response, *, instance_id: str) -> None:
    """Assert a ``POST /send`` response is the degraded-boot 500 with the exact reason."""
    assert resp.status_code == 500, f"degraded /send must 500, got {resp.status_code}: {resp.text}"
    body = resp.json()
    error = body["error"]
    assert error["code"] == "internal_error", error
    assert error["details"]["reason"] == "storage_unavailable_degraded_boot", error
    assert error["details"]["instance"] == instance_id, error


async def test_single_instance_readonly_datadir_boots_degraded(tmp_path: Path) -> None:
    """A read-only per-instance data_dir boots DEGRADED; /ready, /health, /send all honest.

    Drives the real ``create_app`` lifespan with the single pinned "primary"
    instance whose ``data_root`` (``<data_dir>/primary``) is ``chmod 0o555`` before
    boot. The boot-open recreate raises ``sqlite3.OperationalError`` and the
    guard's substrate probe finds ``data_root`` unwritable -> the instance is
    recorded degraded, no context built. Asserts every read surface + both
    ``POST /send`` routing paths.
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_root = data_dir / "primary"
    inst_root.mkdir()
    # Read-only BEFORE boot: store.start() cannot create uploads.db, the isolate
    # has nothing to rename, and the recreate cannot create it -> the substrate
    # probe classifies the dir as unwritable -> degraded.
    os.chmod(inst_root, _READ_ONLY_MODE)

    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir)
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            # The instance booted DEGRADED: no live context, a typed outcome.
            assert app.state.degraded_boot, "the read-only instance must be degraded"
            assert "primary" in {d.instance_id for d in app.state.degraded_boot}
            assert [inst.cfg.id for inst in app.state.instances] == [], "no live context built"

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                # /ready -> ready:false + a detail naming the instance.
                ready = await client.get("/v1/readyz")
                assert ready.status_code == 200
                ready_body = ready.json()
                assert ready_body["ready"] is False
                assert ready_body["detail"] is not None
                assert "primary" in ready_body["detail"]

                # /health -> status:"ok" (liveness) but storage:"degraded".
                health = await client.get("/v1/healthz")
                assert health.status_code == 200
                health_body = health.json()
                assert health_body["status"] == "ok", "liveness stays ok (the process is up)"
                assert health_body["storage"] == "degraded"
                assert health_body["storage_detail"] is not None
                assert "primary" in health_body["storage_detail"]

                # POST /send via the X-Phantom-Instance header (fires before any
                # body parse) -> 500 with the exact reason.
                header_resp = await client.post(
                    "/v1/send",
                    headers={"X-Phantom-Instance": "primary", "X-Phantom-Uid": _DEFAULT_SUB},
                    content=b"",
                )
                _assert_degraded_send_500(header_resp, instance_id="primary")

                # POST /send via host-prefix routing (a real envelope whose
                # first-step host the degraded "primary" instance owns) -> 500.
                envelope = _envelope_for_host(host="127.0.0.1", port=9, chain_id=uuid4())
                host_resp = await _post_send_multipart(
                    client,
                    envelope=envelope,
                    body=b"degraded-body",
                    bearer="not-a-real-token",
                )
                _assert_degraded_send_500(host_resp, instance_id="primary")
    finally:
        os.chmod(inst_root, _READ_WRITE_MODE)


async def test_multi_instance_one_degraded_one_healthy_isolation(tmp_path: Path) -> None:
    """One degraded + one healthy instance: per-instance isolation holds.

    The sharpest falsifier (the § 4D.2 / 4D-Part-B handoff): the healthy
    instance still serves a 202 while the degraded instance 500s; ``/ready`` is
    false (any degraded -> not ready); ``/health`` names ONLY the degraded id.
    Proves the per-instance map and that the guard does not leak.
    """
    from .helpers.stack import _boot_one_emulator  # in-process upstream for the healthy instance

    # One real emulator for the HEALTHY instance's upstream (loopback 127.0.0.1).
    emulator_server = await _boot_one_emulator()
    healthy_port = int(httpx.URL(emulator_server.url()).port or 0)

    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    healthy_root = data_dir / "healthy"
    degraded_root = data_dir / "degraded"
    healthy_root.mkdir()
    degraded_root.mkdir()
    # Only the degraded instance's data_root is read-only.
    os.chmod(degraded_root, _READ_ONLY_MODE)

    # Two instances: "healthy" owns 127.0.0.1 (the live emulator), "degraded"
    # owns 127.0.0.3 (no emulator needed; it 500s before any upstream call).
    two_instances = {
        "instances": [
            {
                "id": "healthy",
                "host_prefixes": ["127.0.0.1", "localhost"],
                "data_dir": "healthy",
                "routes": [
                    {
                        "name": "healthy-route",
                        "hosts": ["127.0.0.1", "localhost"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            },
            {
                "id": "degraded",
                "host_prefixes": [_DEGRADED_HOST],
                "data_dir": "degraded",
                "routes": [
                    {
                        "name": "degraded-route",
                        "hosts": [_DEGRADED_HOST],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            },
        ]
    }
    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir, config_overrides=two_instances)
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            # Exactly the degraded instance is degraded; the healthy one has a
            # live context.
            degraded_ids = {d.instance_id for d in app.state.degraded_boot}
            assert degraded_ids == {"degraded"}
            live_ids = {inst.cfg.id for inst in app.state.instances}
            assert live_ids == {"healthy"}, f"only the healthy instance has a context: {live_ids}"

            # Mint a bearer the emulator accepts for the healthy 202.
            bearer = _fake_token(emulator_server)

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver", timeout=30.0
            ) as client:
                # The HEALTHY instance still admits (202) - the guard does not
                # leak the degraded fault across instances.
                chain_id = uuid4()
                healthy_envelope = _envelope_for_host(
                    host="127.0.0.1", port=healthy_port, chain_id=chain_id
                )
                healthy_resp = await _post_send_multipart(
                    client,
                    envelope=healthy_envelope,
                    body=b"healthy-instance-body",
                    bearer=bearer,
                )
                assert healthy_resp.status_code == 202, (
                    f"the healthy instance must still 202; got {healthy_resp.status_code}: "
                    f"{healthy_resp.text}"
                )

                # The DEGRADED instance 500s (X-Phantom-Instance header path).
                degraded_resp = await client.post(
                    "/v1/send",
                    headers={"X-Phantom-Instance": "degraded", "X-Phantom-Uid": _DEFAULT_SUB},
                    content=b"",
                )
                _assert_degraded_send_500(degraded_resp, instance_id="degraded")

                # /ready false (any degraded -> not ready).
                ready_body = (await client.get("/v1/readyz")).json()
                assert ready_body["ready"] is False

                # /health names ONLY the degraded id, not the healthy one.
                health_body = (await client.get("/v1/healthz")).json()
                assert health_body["storage"] == "degraded"
                assert "degraded" in health_body["storage_detail"]
                assert "healthy" not in health_body["storage_detail"], (
                    "the storage_detail must name only the degraded instance, not the healthy one"
                )
    finally:
        os.chmod(degraded_root, _READ_WRITE_MODE)
        await emulator_server.stop()


def _fake_token(emulator_server: Any) -> str:
    """Mint a fake bearer JWT the emulator accepts (HS256, shared secret)."""
    from datetime import UTC, datetime, timedelta

    import jwt

    cfg = emulator_server.config
    now = datetime.now(UTC)
    secret = os.environ.get("EMULATOR_SIGNING_KEY", "e2e-test-secret")
    payload = {
        "iss": cfg.auth.issuer,
        "sub": _DEFAULT_SUB,
        "aud": cfg.auth.audience,
        "exp": int((now + timedelta(seconds=3600)).timestamp()),
        "iat": int(now.timestamp()),
        "tid": cfg.auth.tenant_id,
    }
    return jwt.encode(payload, secret, algorithm="HS256")
