"""Substrate-fault boot is survived regardless of euid (adversary, M-4 / seed 7).

The existing read-only-data_dir chaos
(``test_chaos_degraded_boot_readonly_datadir.py``) proves the unwritable-substrate
DEGRADED boot (§ 4D), but it SKIPS under ``os.geteuid() == 0`` because a
``chmod 0o555`` directory is bypassable by root - and Balena / ARM container
deployments plus most CI runners execute as root, so the substrate-fault physics
is never exercised in the exact environment where it happens in the field.

These tests close that gap with substrate faults that do NOT depend on
permission bits, so they drive the production ``create_app`` lifespan's
degrade-or-isolate decision regardless of euid:

* :func:`test_directory_at_db_path_isolates_and_serves_durably` - the
  euid-independent SUBSTRATE fault: a DIRECTORY pre-created at the per-instance
  ``uploads.db`` path. ``aiosqlite.connect`` cannot open a directory as a SQLite
  file, so the boot-time integrity gate's ``PRAGMA integrity_check`` fails, the
  gate QUARANTINES the offending directory to a flat ``uploads.corrupted.<iso>.db``
  sibling (recoverable via ``GET /v1/admin/quarantine``), and the instance boots
  HEALTHY fresh. The CORRECT outcome is durable service, not a crash-loop and not
  a degraded instance: ``/ready`` is ``true``, ``/health`` reports
  ``storage:"ok"``, and a real ``POST /send`` is admitted (202) over the fresh DB.
  This proves the "never crash on a bad on-disk DB substrate" property holds for
  a fault root cannot mask.

* :func:`test_file_at_data_root_must_not_crash_loop_the_boot` - a SHARPER
  euid-independent substrate fault: a FILE pre-created at the per-instance
  ``data_root`` path itself (a plausible SD-card / corrupted-filesystem field
  condition where a file sits where a directory must be). The § 4D contract says
  the service ALWAYS boots and degrades loudly on an unwritable substrate rather
  than crash-looping; this asserts that contract, and the test PASSES. Closed by
  the round-4 M4-A fix: ``_build_instance_context`` prepares the per-instance
  ``data_root`` via ``_ensure_data_root_writable``, which probes the substrate
  when ``mkdir`` raises and converts an unwritable-substrate fault (a
  file-at-``data_root``, a read-only / full parent) into
  ``_StorageSubstrateUnwritableError``, caught by the same widened degrade branch
  the DB-open guard uses, so the instance boots DEGRADED instead of the raw
  ``FileExistsError`` crash-looping the lifespan. See finding M4-A.

Public e2e-light lane (plan § 5.0): generic driver shapes + the in-process
emulator, no ``PHANTOM_ENABLED``. These drive the real lifespan
via ``app.router.lifespan_context`` (the same path the unit refusal tests use)
and issue HTTP through an in-process ASGI transport, exactly like the
read-only-data_dir chaos module, but WITHOUT a root skip.

Falsifier (test 1): drop the integrity gate's quarantine of an unopenable DB
artifact -> the directory crash-loops the boot OR the instance silently serves
without a durable store -> RED. Falsifier (test 2): revert the M4-A fix so the
``data_root.mkdir`` stage is no longer covered by the degrade decision -> a
file-at-``data_root`` raises ``FileExistsError`` from ``mkdir`` and crash-loops
the lifespan instead of booting degraded -> RED.
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


def _prod_settings_from_e2e_yaml(
    *, data_dir: Path, config_overrides: Mapping[str, Any] | None = None
) -> Settings:
    """Build production Settings from the pinned e2e YAML + overrides.

    Mirrors ``test_chaos_degraded_boot_readonly_datadir._prod_settings_from_e2e_yaml``:
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
    request = build_create_file_request(file_name=f"m4_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=files_api_base,
        local_uuid=chain_id,
    )
    return envelope.model_dump(mode="json")


async def test_directory_at_db_path_isolates_and_serves_durably(tmp_path: Path) -> None:
    """A DIRECTORY at the uploads.db path is quarantined; the instance serves durably.

    The euid-independent substrate fault: pre-create a directory at
    ``<data_dir>/primary/uploads.db`` so ``aiosqlite.connect`` cannot open it as a
    SQLite file. The boot-time integrity gate's ``PRAGMA integrity_check`` fails,
    the gate quarantines the directory to a flat ``uploads.corrupted.<iso>.db``
    sibling, and the instance boots HEALTHY fresh. Asserts the lifespan does NOT
    crash, the instance is NOT degraded, ``/ready`` + ``/health`` are honest, the
    offending directory is preserved as a recoverable quarantine artifact, and a
    real ``POST /send`` is admitted over the fresh durable DB.
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_root = data_dir / _INSTANCE_ID
    inst_root.mkdir()
    db_path = inst_root / "uploads.db"
    # The fault: a DIRECTORY where the live SQLite file belongs. Non-empty, so
    # this is unmistakably not a stray empty dir; the quarantine still moves it.
    db_path.mkdir()
    (db_path / "not-a-sqlite-file.bin").write_bytes(b"corrupted substrate artifact")

    emu = await _boot_one_emulator()
    emu_port = int(httpx.URL(emu.url()).port or 0)
    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir)
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            # The instance booted HEALTHY: a live context exists, nothing degraded.
            assert not app.state.degraded_boot, (
                "a directory-at-db-path is an ISOLATE-and-boot-fresh fault on a "
                f"writable substrate, not a degrade: {app.state.degraded_boot!r}"
            )
            live_ids = {inst.cfg.id for inst in app.state.instances}
            assert live_ids == {_INSTANCE_ID}, f"the instance must have a live context: {live_ids}"

            # The offending directory was moved aside to a flat corrupted sibling
            # (recoverable), and the live uploads.db is now a real fresh file.
            assert db_path.is_file(), "the fresh durable uploads.db must be a real file post-boot"
            inst_listing = sorted(p.name for p in inst_root.iterdir())
            corrupted = [name for name in inst_listing if ".corrupted." in name]
            assert corrupted, (
                "the unopenable directory must be preserved as a corrupted-quarantine "
                f"artifact for operator recovery; inst_root={inst_listing!r}"
            )

            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver", timeout=30.0
            ) as client:
                # /ready is true (the instance is healthy, not degraded).
                ready = (await client.get("/v1/readyz")).json()
                assert ready["ready"] is True, f"healthy instance must be ready: {ready!r}"

                # /health reports storage ok (the fresh DB is durable).
                health = (await client.get("/v1/healthz")).json()
                assert health["status"] == "ok"
                assert health["storage"] == "ok", (
                    f"storage must be ok after isolate-and-boot-fresh: {health!r}"
                )

                # A real POST /send is ADMITTED (202) over the fresh durable DB -
                # the substrate fault did not crash-loop and did not leave the
                # instance silently non-durable.
                bearer = _fake_token(emu)
                chain_id = uuid4()
                envelope = _envelope_for_host(host="127.0.0.1", port=emu_port, chain_id=chain_id)
                resp = await _post_send_multipart(
                    client,
                    envelope=envelope,
                    body=b"m4-durable-body",
                    bearer=bearer,
                )
                assert resp.status_code == 202, (
                    "the instance must admit (202) over the fresh durable DB after "
                    f"isolating the directory-at-db-path; got {resp.status_code}: {resp.text}"
                )
    finally:
        await emu.stop()


async def test_file_at_data_root_must_not_crash_loop_the_boot(tmp_path: Path) -> None:
    """A FILE at the per-instance data_root must DEGRADE the boot, not crash it.

    Pre-create a regular file at ``<data_dir>/primary`` (where the instance's
    ``data_root`` directory must be). The § 4D contract is that the service
    ALWAYS boots and degrades loudly on an unwritable substrate. This asserts the
    instance boots DEGRADED (a typed DegradedInstance outcome, no live context),
    exactly like a read-only ``data_root``.

    Closed by the round-4 M4-A fix: ``_build_instance_context`` now prepares the
    per-instance ``data_root`` via ``_ensure_data_root_writable``, which probes the
    substrate when ``mkdir`` raises and converts an unwritable-substrate fault
    (a file-at-``data_root``, a read-only / full parent) into
    ``_StorageSubstrateUnwritableError``, caught by the same widened degrade
    branch the DB-open guard uses, so the instance boots degraded instead of the
    raw ``FileExistsError`` crash-looping the lifespan.
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_root = data_dir / _INSTANCE_ID
    # The fault: a FILE where the instance data_root directory must be.
    inst_root.write_text("corrupted-filesystem: a file where a directory must be")

    settings = _prod_settings_from_e2e_yaml(data_dir=data_dir)
    app = create_app(settings)
    # CORRECT behavior (M4-A fix): the boot survives and degrades the instance
    # loudly. _ensure_data_root_writable probes the substrate when mkdir raises and
    # converts the file-at-data_root fault into _StorageSubstrateUnwritableError,
    # which the widened degrade branch catches, so entering the lifespan succeeds
    # and the instance is recorded as degraded (no live context).
    async with app.router.lifespan_context(app):
        assert app.state.degraded_boot, (
            "a file-at-data_root is an unwritable substrate; the instance must boot "
            "DEGRADED (loud), not crash the lifespan"
        )
        assert _INSTANCE_ID in {d.instance_id for d in app.state.degraded_boot}
        assert [inst.cfg.id for inst in app.state.instances] == [], (
            "no live context may be built for a degraded instance"
        )
