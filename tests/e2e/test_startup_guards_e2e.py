"""End-to-end falsifiers for the five startup guards + instance isolation.

§9.2 of the 2026-05-29 refinement plan: every guard that moved into
``app.py``'s production lifespan gets an END-TO-END falsifier through the
real stack, not only a fast unit test. These drive the production
composition root the way production does:

* **Positive / artifact cases** (umask, cold-backup) boot the full
  in-process stack via :func:`boot_stack` (real uvicorn server + the
  phantom-emulator + the SDK client) and assert observable side effects.
* **Back-up-and-run case** (all_ram-over-populated-bodies) boots the full
  in-process stack twice on one data dir: hybrid persists a body to disk,
  then all_ram boots over the populated tree and (plan § 1 / ADR-025)
  backs the live data up to a ``mode_switch`` quarantine pair and boots
  fresh. The proof is the SDK-served quarantine inventory
  (``GET /v1/admin/quarantine``) reporting the ``mode_switch`` backup, not a
  refusal.
* **Refusal cases** (retention-floor, duplicate instance id, duplicate
  data_dir) build *production* settings from the pinned e2e YAML (the same
  ``load_settings`` path the harness uses) and assert the production
  ``create_app`` lifespan raises the precise :class:`ConfigInvariantError`.
  (Through ``boot_stack`` a refusing lifespan surfaces only as a generic
  ``StackBootError`` after a 15 s uvicorn-startup timeout; driving the
  lifespan context directly asserts the exact guard fired AND is fast.)
* The DB-integrity gate's e2e lives in
  ``tests/e2e/crash_recovery/test_corrupt_db_quarantine_boot.py`` — it
  needs a genuine stop/corrupt/reboot, so it reuses the real-OS
  subprocess seam (§9.2 case 1 "reuse the existing crash_recovery seam").

Falsifier discipline (§9.2): each test goes RED if its guard is removed
from ``app.py`` — a refusal boots, an artifact never appears, the umask
stays at the inherited default.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import yaml  # type: ignore[import-untyped]
from phantom.app import create_app
from phantom.config.settings import load_settings
from phantom.runtime.startup_checks import ConfigInvariantError

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.payloads import build_create_file_request
from .helpers.stack import PHANTOM_CONFIG_PATH, _deep_merge_dict, boot_stack
from .helpers.timing import await_until

if TYPE_CHECKING:
    from collections.abc import Mapping

    from phantom.config.settings import Settings
    from phantom_client import PhantomClient

pytestmark = [pytest.mark.e2e]

_DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"


def _prod_settings_from_e2e_yaml(
    *, data_dir: Path, config_overrides: Mapping[str, Any] | None = None
) -> Settings:
    """Build production Settings from the pinned e2e YAML + overrides.

    Mirrors ``helpers.stack._build_phantom_settings`` minus the live
    bind-host/port wiring (refusal tests never start a server — they
    drive the lifespan context manager directly). The YAML on disk is
    untouched; the overlay lands on an in-memory copy serialized to a
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


# ---------------------------------------------------------------------
# umask (process-wide) — full stack.
# ---------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_umask_owner_only_on_created_files(tmp_path: Path) -> None:
    """A body file created by the live service is owner-only (0o600).

    Boots the full stack, drives one upload to disk, then stats the
    on-disk body file. The umask the lifespan set (0o077) means the file
    is created mode 0o600.

    Falsifier: drop ``apply_umask`` → the inherited default (e.g. 0o022)
    leaves the body world-readable (0o644) → RED.

    The process umask is forced permissive (0o022) BEFORE boot and held
    until AFTER the body is created — the lifespan's ``apply_umask(0o077)``
    runs during ``boot_stack`` startup and overrides it, so the body file
    reflects 0o077 only if the guard is present. (Restoring the umask
    before the upload would let the file be created under the permissive
    default and the test would no longer falsify a removed guard.)
    """
    saved = os.umask(0o022)
    try:
        stack = await boot_stack(
            tmp_path=tmp_path,
            config_overrides={"storage": {"persist_trigger": {"body_size_threshold_bytes": 1}}},
        )
        try:
            emulator = stack.emulator
            pc = stack.phantom_client
            emulator.clear_received()
            emulator.clear_failures()
            # Force the body onto disk: 5xx on the first attempt → persist.
            from phantom_emulator.failure.injection import FailurePolicy, FailureScope

            emulator.inject_failure(
                FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]
            )
            chain_id = uuid4()
            await _submit_one(
                pc,
                chain_id=chain_id,
                emulator_url=stack.emulator_url,
                bearer=stack.fake_security_token(),
            )

            # Wait for the body file to land on disk, then assert its mode.
            body_file = stack.body_path(chain_id, "body")

            async def _body_on_disk() -> bool:
                return body_file.exists()

            await await_until(
                _body_on_disk,
                timeout_seconds=15.0,
                poll_interval_seconds=0.1,
                message=f"body never persisted to {body_file}",
            )
            mode = body_file.stat().st_mode & 0o777
            assert mode == 0o600, f"expected body file mode 0o600 (umask 0o077), got {mode:#o}"
        finally:
            await stack.tear_down()
    finally:
        os.umask(saved)


# ---------------------------------------------------------------------
# Cold-backup (per-instance) — full stack.
# ---------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_cold_backup_artifact_appears(tmp_path: Path) -> None:
    """With ``backup_enabled`` + a short period a backup artifact appears.

    Falsifier: drop the ``ColdBackupScheduler`` spawn from app.py's
    lifespan TaskGroup → no artifact under ``<data>/primary/backups/`` → RED.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {"db_integrity": {"backup_enabled": True, "backup_period_seconds": 1}}
        },
    )
    try:
        backups_root = tmp_path / "primary" / "backups"

        async def _artifact_present() -> bool:
            return backups_root.is_dir() and bool(list(backups_root.glob("uploads.backup.*.db")))

        await await_until(
            _artifact_present,
            timeout_seconds=12.0,
            poll_interval_seconds=0.2,
            message=f"no cold-backup artifact under {backups_root}",
        )
        assert list(backups_root.glob("uploads.backup.*.db"))
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------
# all_ram over populated bodies/ (per-instance).
# ---------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_all_ram_over_populated_bodies_backs_up_and_boots_fresh(tmp_path: Path) -> None:
    """Boot hybrid → land a file-backed body → reboot all_ram → backs up + runs.

    Drives the genuine production sequence end-to-end: a real upload populates
    ``<data>/primary/bodies/`` under hybrid; after teardown, the same data dir is
    booted ``all_ram`` through the full stack. Plan § 1 / ADR-025
    (back-up-and-run, supersedes the prior refuse-to-boot): rather than
    refusing, the service relocates the live DB + body tree to a recoverable
    ``mode_switch`` quarantine pair and boots fresh. The proof is the
    SDK-served quarantine inventory (the exact ``GET /v1/admin/quarantine``
    surface an operator uses) reporting the ``mode_switch`` backup, and the
    live ``bodies/`` tree being empty.

    Falsifier: drop ``check_body_store_mode`` → the all_ram boot leaves the
    intact on-disk body in the live tree, the inventory shows no ``mode_switch``
    backup, and recovery condemns the row to ``corrupted`` → RED.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    # Phase 1 — hybrid boot lands a body_location='file' row on disk.
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"persist_trigger": {"body_size_threshold_bytes": 1}}},
    )
    chain_id = uuid4()
    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        emulator.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]
        )
        await _submit_one(
            stack.phantom_client,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )
        body_file = stack.body_path(chain_id, "body")

        async def _body_on_disk() -> bool:
            return body_file.exists()

        await await_until(
            _body_on_disk,
            timeout_seconds=15.0,
            poll_interval_seconds=0.1,
            message=f"phase-1 body never persisted to {body_file}",
        )
    finally:
        await stack.tear_down()

    assert body_file.exists(), "phase-1 must leave a body on disk for the all_ram guard to catch"
    persisted_bytes = body_file.read_bytes()

    # Phase 2 — all_ram over the same populated data dir backs up and BOOTS.
    # The full stack comes up (no refusal); the SDK quarantine inventory is
    # the operator-facing proof the live data was preserved as a mode_switch
    # backup, and the live bodies/ tree booted fresh.
    stack2 = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"body_store": {"mode": "all_ram"}}},
    )
    try:
        inventory = await stack2.phantom_client.get_quarantine_inventory(instance="primary")
        mode_switch = [e for e in inventory.quarantines if e.reason == "mode_switch"]
        assert mode_switch, (
            "all_ram over a populated dir must back the live data up to a "
            f"mode_switch pair; inventory={inventory.quarantines!r}"
        )
        backup = mode_switch[0]
        assert backup.anomaly is False and backup.backup_id is not None
        assert backup.has_body, "the mode_switch backup must include the relocated body tree"
        assert backup.body_path is not None
        # The pre-existing body bytes survive in the backup body tree (the
        # path is server-local; this test shares the filesystem in-process).
        backup_root = Path(backup.body_path)
        preserved = [
            p for p in backup_root.rglob("*") if p.is_file() and p.read_bytes() == persisted_bytes
        ]
        assert preserved, "the hybrid-persisted body bytes must survive in the mode_switch backup"
        # The live bodies/ tree booted fresh (the relocated body is gone).
        live_bodies = tmp_path / "primary" / "bodies"
        assert not [p for p in live_bodies.rglob("*") if p.is_file()]
    finally:
        await stack2.tear_down()


# ---------------------------------------------------------------------
# Retention-floor (process-wide).
# ---------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_retention_floor_violation_refuses_boot(tmp_path: Path) -> None:
    """A body-outlives-metadata config refuses to boot via the prod root.

    Falsifier: drop ``check_retention_floor`` → boots → RED.
    """
    settings = _prod_settings_from_e2e_yaml(
        data_dir=tmp_path,
        config_overrides={
            "retention": {"succeeded_body_seconds": 600, "succeeded_metadata_seconds": 60}
        },
    )
    app = create_app(settings)
    with pytest.raises(ConfigInvariantError, match=r"succeeded"):
        async with app.router.lifespan_context(app):
            pass  # pragma: no cover — lifespan startup must raise


# ---------------------------------------------------------------------
# Instance isolation (process-wide).
# ---------------------------------------------------------------------


@pytest.mark.e2e
async def test_e2e_duplicate_instance_id_refuses_boot(tmp_path: Path) -> None:
    """A two-instance config with a duplicate id refuses to boot.

    Falsifier: drop ``check_instance_isolation`` → boots with two
    instances sharing an id (the dispatcher silently collapses one) → RED.
    """
    settings = _prod_settings_from_e2e_yaml(
        data_dir=tmp_path,
        config_overrides={
            "instances": [
                {
                    "id": "dup",
                    "host_prefixes": ["emu-a"],
                    "data_dir": "dup-a",
                    "routes": [{"name": "a", "hosts": ["emu-a"], "auth_mode": "none"}],
                },
                {
                    "id": "dup",
                    "host_prefixes": ["emu-b"],
                    "data_dir": "dup-b",
                    "routes": [{"name": "b", "hosts": ["emu-b"], "auth_mode": "none"}],
                },
            ]
        },
    )
    app = create_app(settings)
    with pytest.raises(ConfigInvariantError, match=r"[Dd]uplicate instance id"):
        async with app.router.lifespan_context(app):
            pass  # pragma: no cover — lifespan startup must raise


@pytest.mark.e2e
async def test_e2e_duplicate_data_dir_refuses_boot(tmp_path: Path) -> None:
    """A two-instance config sharing a data_dir refuses to boot.

    Falsifier: drop ``check_instance_isolation`` → boots with two stores
    on one uploads.db (single-writer violation → corruption) → RED.
    """
    settings = _prod_settings_from_e2e_yaml(
        data_dir=tmp_path,
        config_overrides={
            "instances": [
                {
                    "id": "one",
                    "host_prefixes": ["emu-a"],
                    "data_dir": "shared",
                    "routes": [{"name": "a", "hosts": ["emu-a"], "auth_mode": "none"}],
                },
                {
                    "id": "two",
                    "host_prefixes": ["emu-b"],
                    "data_dir": "shared",
                    "routes": [{"name": "b", "hosts": ["emu-b"], "auth_mode": "none"}],
                },
            ]
        },
    )
    app = create_app(settings)
    with pytest.raises(ConfigInvariantError, match=r"share data_dir"):
        async with app.router.lifespan_context(app):
            pass  # pragma: no cover — lifespan startup must raise


async def _submit_one(
    pc: PhantomClient,
    *,
    chain_id: Any,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit one upload-shaped chain through the SDK (single body_ref)."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": b"phantom-e2e-startup-guard-body-" + b"x" * 128},
        uid=_DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
