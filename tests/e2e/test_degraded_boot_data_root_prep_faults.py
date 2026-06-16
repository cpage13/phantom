"""More ``data_root``-prep substrate faults degrade euid-independently (adversary R5).

Round 4 closed finding M4-A: a non-directory at the per-instance ``data_root``
made ``data_root.mkdir`` raise before the § 4D degrade guard, crash-looping the
whole boot. The fix routed the directory-prep stage through
``_ensure_data_root_writable`` (``app.py``), which PROBES the substrate when
``mkdir`` raises and converts an unwritable substrate into
``_StorageSubstrateUnwritableError`` so the instance boots DEGRADED. The R4 test
(``test_degraded_boot_unwritable_db_path.py``) covers the single-instance
file-at-``data_root`` case. This module re-attacks that fix along the axes R4 did
NOT exercise, all chosen to be euid-INDEPENDENT (a physical filesystem-type
conflict, not a permission bit) so they exercise the degrade path in the exact
root / Balena / ARM environment where the existing
``test_chaos_degraded_boot_readonly_datadir`` SKIPS (a ``chmod 0o555`` dir is
bypassable by root):

* :func:`test_dangling_symlink_at_data_root_degrades` - a DANGLING SYMLINK where
  the instance ``data_root`` must be. ``mkdir(exist_ok=True)`` raises
  ``FileExistsError`` (the symlink path exists) and a probe write through the
  broken link raises ``FileNotFoundError``, so the probe classifies it unwritable
  and the instance degrades. A symlink to a vanished mount target is a plausible
  field condition on a device whose external storage went away.

* :func:`test_file_at_a_parent_component_of_data_root_degrades` - a regular FILE
  at a PARENT component of ``data_root`` (so ``data_root`` would have to live
  UNDER a file). ``mkdir(parents=True)`` raises ``NotADirectoryError`` and the
  probe raises the same, so the instance degrades. This proves the degrade
  decision covers a fault DEEPER than the leaf ``data_root`` (the R4 test put the
  fault at the leaf).

* :func:`test_multi_instance_one_data_root_fault_isolates_euid_independently` -
  the load-bearing isolation falsifier, euid-INDEPENDENT. Two instances: one with
  a file at its ``data_root`` (degrades via the M4-A directory-prep path), one
  with a clean writable ``data_root`` (boots healthy). Asserts the degrade
  isolates to the one instance: the process stays UP, the healthy instance serves
  a real 202, the degraded instance 500s, ``/ready`` is false, and ``/health``
  names ONLY the degraded id. The existing
  ``test_chaos_degraded_boot_readonly_datadir::test_multi_instance_one_degraded_one_healthy_isolation``
  proves this for a read-only ``data_root`` but SKIPS under root; this proves the
  SAME isolation for a fault root cannot mask, so root CI / containers exercise it.

Public e2e-light lane (plan § 5.0): generic driver shapes + the in-process
emulator, no ``PHANTOM_ENABLED``. These drive the real
``create_app`` lifespan via ``app.router.lifespan_context`` (the same path the
unit refusal tests and the existing degraded-boot e2e use) and issue HTTP through
an in-process ASGI transport, with NO root skip.

Falsifier: narrow the M4-A degrade decision back to the DB-open stage (drop
``_ensure_data_root_writable``'s probe-classify) -> any of these data_root-prep
faults crash-loops the lifespan instead of degrading -> entering the lifespan
context raises -> RED. For the multi-instance test, additionally: let the degrade
leak across instances (e.g. fail the whole build loop on the first fault) -> the
healthy instance gets no context and its 202 never fires -> RED.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import httpx
import jwt
import pytest
import yaml
from phantom.app import create_app
from phantom.config.settings import Settings, load_settings

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.payloads import build_create_file_request
from .helpers.stack import PHANTOM_CONFIG_PATH, _boot_one_emulator, _deep_merge_dict

if TYPE_CHECKING:
    from collections.abc import Mapping

pytestmark = [pytest.mark.e2e]

_DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"
# The single pinned instance id in the e2e YAML.
_INSTANCE_ID = "primary"
# A loopback host owned by the degraded instance in the multi-instance test; no
# emulator binds here (the degraded guard 500s before any upstream call).
_DEGRADED_HOST = "127.0.0.4"


def _prod_settings_from_e2e_yaml(
    *, data_dir: Path, config_overrides: Mapping[str, Any] | None = None
) -> Settings:
    """Build production Settings from the pinned e2e YAML + overrides.

    Mirrors ``test_degraded_boot_unwritable_db_path._prod_settings_from_e2e_yaml``:
    the YAML on disk is untouched; the overlay lands on an in-memory copy
    serialized to a temp file so the full ``load_settings`` validator path runs.

    Args:
        data_dir: The top-level storage data dir to point Settings at.
        config_overrides: Optional mapping deep-merged over the base YAML.

    Returns:
        The validated :class:`Settings`.
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


def _fake_token(emulator_server: Any) -> str:
    """Mint a fake bearer JWT the emulator accepts (HS256, shared secret).

    Args:
        emulator_server: The booted emulator whose auth config the claims match.

    Returns:
        A signed HS256 JWT string usable as a ``Bearer`` token.
    """
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


async def _post_send_multipart(
    client: httpx.AsyncClient,
    *,
    envelope: dict[str, Any],
    body: bytes,
    bearer: str,
) -> httpx.Response:
    """POST a multipart chain submission (envelope + one body_ref) to ``/v1/send``.

    Args:
        client: The in-process ASGI HTTP client.
        envelope: The serialized chain envelope.
        body: The single body-ref bytes.
        bearer: The bearer token for the ``Authorization`` header.

    Returns:
        The raw :class:`httpx.Response`.
    """
    files = {
        "envelope": ("envelope.json", json.dumps(envelope).encode(), "application/json"),
        "body_refs[body]": ("body", body, "application/octet-stream"),
    }
    headers = {
        "X-Phantom-Uid": _DEFAULT_SUB,
        "Authorization": f"Bearer {bearer}",
    }
    return await client.post("/v1/send", files=files, headers=headers)


def _envelope_for_host(*, host: str, port: int, chain_id: UUID) -> dict[str, Any]:
    """Build a serialized 2-step envelope whose first-step host is ``host``.

    The first-step URL's hostname is what the no-header dispatcher resolves an
    instance by, so pointing it at ``host`` routes the request to the instance
    that owns that host_prefix.

    Args:
        host: The loopback host the first step targets (the instance owns it).
        port: The emulator port the first step targets.
        chain_id: The chain's local uuid (stamped into the metadata KVS).

    Returns:
        The JSON-serialized envelope dict.
    """
    files_api_base = f"http://{host}:{port}"
    request = build_create_file_request(file_name=f"m4b_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=files_api_base,
        local_uuid=chain_id,
    )
    return envelope.model_dump(mode="json")


def _assert_degraded_send_500(resp: httpx.Response, *, instance_id: str) -> None:
    """Assert a ``POST /send`` response is the degraded-boot 500 with the exact reason.

    Args:
        resp: The ``POST /send`` response.
        instance_id: The configured instance id the request routed to.
    """
    assert resp.status_code == 500, f"degraded /send must 500, got {resp.status_code}: {resp.text}"
    error = resp.json()["error"]
    assert error["code"] == "internal_error", error
    assert error["details"]["reason"] == "storage_unavailable_degraded_boot", error
    assert error["details"]["instance"] == instance_id, error


async def test_dangling_symlink_at_data_root_degrades(tmp_path: Path) -> None:
    """A dangling symlink where the instance data_root must be boots DEGRADED.

    The euid-independent ``data_root``-prep fault: a symlink at ``<data_dir>/primary``
    pointing at a non-existent target (a vanished external mount). ``mkdir`` raises
    ``FileExistsError`` (the link path exists) and the substrate probe write
    through the broken link raises ``FileNotFoundError``, so
    ``_ensure_data_root_writable`` classifies the substrate unwritable and the
    instance degrades. Asserts the lifespan does NOT crash and the instance is
    recorded degraded with no live context.
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_root = data_dir / _INSTANCE_ID
    # The fault: a symlink at data_root pointing nowhere. Not following the link
    # to create the target is what makes this a substrate fault rather than a
    # silent boot into a stray directory.
    inst_root.symlink_to(data_dir / "vanished-external-mount-target")
    assert inst_root.is_symlink() and not inst_root.exists(), (
        "the test fixture must be a DANGLING symlink (link present, target absent)"
    )

    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert app.state.degraded_boot, (
            "a dangling symlink at data_root is an unwritable substrate; the "
            "instance must boot DEGRADED, not crash the lifespan"
        )
        assert _INSTANCE_ID in {d.instance_id for d in app.state.degraded_boot}
        assert [inst.cfg.id for inst in app.state.instances] == [], (
            "no live context may be built for a degraded instance"
        )


async def test_file_at_a_parent_component_of_data_root_degrades(tmp_path: Path) -> None:
    """A file at a PARENT component of data_root boots DEGRADED (deeper non-dir fault).

    The instance ``data_dir`` is set DEEPER than the leaf: a regular file sits at
    ``<data_dir>/primary`` and the instance's ``data_dir`` is ``primary/nested``, so the
    per-instance ``data_root`` (``<data_dir>/primary/nested``) would have to live UNDER
    a file. ``mkdir(parents=True)`` raises ``NotADirectoryError`` and the probe
    raises the same, so ``_ensure_data_root_writable`` degrades the instance. This
    proves the degrade decision covers a substrate fault deeper than the leaf
    ``data_root`` directory itself (the R4 test put the fault at the leaf).
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    # A regular file at the parent component the nested data_root must traverse.
    blocking_file = data_dir / _INSTANCE_ID
    blocking_file.write_text("corrupted-filesystem: a file where a parent directory must be")

    # Point the instance's data_dir at a path UNDER that file. The instances
    # list is replaced wholesale by the deep merge, so the full pinned "primary"
    # instance shape is restated with only data_dir changed to the nested path.
    nested = {
        "instances": [
            {
                "id": _INSTANCE_ID,
                "host_prefixes": ["127.0.0.1", "localhost"],
                "data_dir": f"{_INSTANCE_ID}/nested",
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["127.0.0.1", "localhost"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ]
    }
    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir, config_overrides=nested)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert app.state.degraded_boot, (
            "a file at a parent component of data_root is an unwritable substrate; "
            "the instance must boot DEGRADED, not crash the lifespan"
        )
        assert _INSTANCE_ID in {d.instance_id for d in app.state.degraded_boot}
        assert [inst.cfg.id for inst in app.state.instances] == [], (
            "no live context may be built for a degraded instance"
        )


async def test_multi_instance_one_data_root_fault_isolates_euid_independently(
    tmp_path: Path,
) -> None:
    """One data_root-fault instance degrades; the other boots healthy and serves.

    The euid-INDEPENDENT multi-instance isolation falsifier. One instance has a
    FILE at its ``data_root`` (the M4-A directory-prep fault, which degrades
    regardless of euid); the other has a clean writable ``data_root`` (boots
    healthy). Asserts the degrade isolates to the one instance: the process stays
    UP, the healthy instance admits a real 202, the degraded one 500s, ``/ready``
    is false (any degraded -> not ready), and ``/health`` names ONLY the degraded
    id. The existing read-only-data_root isolation test proves this property too
    but SKIPS under root; this proves it for a fault root cannot mask.
    """
    emulator_server = await _boot_one_emulator()
    healthy_port = int(httpx.URL(emulator_server.url()).port or 0)

    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    healthy_root = data_dir / "healthy"
    healthy_root.mkdir()
    # The degraded instance's data_root is a FILE, not a directory (euid-independent).
    degraded_root = data_dir / "degraded"
    degraded_root.write_text("corrupted-filesystem: a file where the data_root directory must be")

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
            # Exactly the file-at-data_root instance degraded; the healthy one is live.
            degraded_ids = {d.instance_id for d in app.state.degraded_boot}
            assert "degraded" in degraded_ids
            assert "healthy" not in degraded_ids
            live_ids = {inst.cfg.id for inst in app.state.instances}
            assert live_ids == {"healthy"}, (
                f"only the healthy instance may have a live context: {live_ids}"
            )

            bearer = _fake_token(emulator_server)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver", timeout=30.0
            ) as client:
                # The HEALTHY instance still admits (202): the M4-A degrade did not
                # leak across the per-instance boot loop.
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
        await emulator_server.stop()
