"""F14: the retention floor is re-checked on every reload, not only at boot.

The floor is the invariant that a body must never outlive the row that
owns it. ``check_retention_floor`` enforced it at exactly one call site,
the lifespan's process-wide guard block, and ``apply_reload`` never ran
it. ``RetentionCfg`` carries no cross-field validator of its own, so
pydantic accepts an inverted pair, and the reaper reads retention from
the live snapshot on every sweep. A SIGHUP or ``POST /v1/admin/reload``
therefore installed a window the boot guard exists to reject, and the
next sweep ran against it.

The cost is asymmetric, which is why the fix has to be at the config
door rather than in the reaper. The metadata pass deletes rows without
touching bodies, so with an inverted window the rows go first. Disk
bytes become janitor orphans and are eventually reclaimed. RAM bytes are
unreclaimable, because ``RamBodyStore.list_orphans`` returns ``[]`` by
design, on the premise that a RAM ``chain_id`` always has a row in
``uploads``, which this path falsifies.

The posture is REJECT THE WHOLE RELOAD, which is distinct from the
restart-required refusals elsewhere in ``apply_reload``. A
restart-required block means the YAML is valid and Phantom simply cannot
apply that part while running: the reload succeeds and warns. A floor
violation means the YAML is invalid, at runtime and after a restart
alike, so it joins the class ADR-013 already defines and rides the one
shared ``RELOAD_FAILURE_ERRORS`` set: 422 ``envelope_invalid`` on the
admin route, log-and-keep-previous on SIGHUP.

``test_reload_unreadable_settings_file.py`` is the template: it drives
one failure class through BOTH consumers.
"""

from __future__ import annotations

import contextlib
import copy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.compression import select_codec
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.models.errors import ErrorEnvelope
from phantom.routes import admin as admin_routes
from phantom.runtime.reload import _sighup_reload, apply_reload
from phantom.runtime.startup_checks import ConfigInvariantError
from phantom.strategies import build_retry_strategy
from phantom.workers.saturation import SaturationGate

# No module-level asyncio mark: ``asyncio_mode = "auto"`` already collects
# the async tests, and the admin-route test is deliberately SYNC because
# ``TestClient`` drives its own event loop.

_INSTANCE_ID = "inst-a"

# The inverted pair. The body window outlives the metadata window, so a
# reaped row would strand its body. Named at the top so the inversion is
# visible without reading a mutate lambda.
_METADATA_SECONDS = 60
_BODY_SECONDS = 600

# A coherent pair for the counter-test: both finite, body <= metadata.
_VALID_METADATA_SECONDS = 900
_VALID_BODY_SECONDS = 120

# The "forever" sentinel. A forever-metadata window over a finite body
# window is legal; a forever-body window under a finite metadata window
# is not.
_FOREVER = -1

# Other knobs the rejected reload also carries, to prove the rejection is
# whole rather than partial. Both differ from any boot value.
_OTHER_MAX_IN_FLIGHT = 7
_OTHER_REAPER_INTERVAL = 41

# The route/host the harness's one instance declares.
_BOOT_HOST = "files.example.com"


def _base_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A probe-reliant one-instance config (the smart-defaults posture)."""
    return {
        "storage": {"data_dir": str(data_dir)},
        "instances": [
            {
                "id": _INSTANCE_ID,
                "host_prefixes": [_BOOT_HOST],
                "data_dir": _INSTANCE_ID,
                "routes": [
                    {
                        "name": "files",
                        "hosts": [_BOOT_HOST],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
    }


class _Producer:
    """One booted reload harness: real Settings, holder, minimal real ctx."""

    def __init__(self, tmp_path: Path) -> None:
        """Boot exactly as production does and assemble the reload surface."""
        self.raw = _base_yaml_payload(tmp_path / "data")
        self.settings_path = tmp_path / "phantom.yaml"
        self.settings_path.write_text(yaml.safe_dump(self.raw))
        boot = Settings.reload_from_yaml(self.settings_path)
        cfg = boot.instances[0]
        self.holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})

        def current_settings_thunk() -> object:
            return self.holder.snapshot_for(_INSTANCE_ID)

        assert boot.saturation.max_in_flight is not None
        assert boot.saturation.max_in_flight_bytes is not None
        assert boot.saturation.max_disk_bytes is not None
        self.ctx = InstanceContext(
            cfg=cfg,
            store=MagicMock(),
            ram_body_store=MagicMock(),
            file_body_store=MagicMock(),
            body_store=MagicMock(),
            persist_controller=None,
            token_cache=MagicMock(),
            minter=None,
            retry_strategy=build_retry_strategy(boot.retry.default_strategy),
            upstream_client=MagicMock(),
            executor=MagicMock(),
            saturation=SaturationGate(
                max_in_flight=boot.saturation.max_in_flight,
                max_in_flight_bytes=boot.saturation.max_in_flight_bytes,
                max_disk_bytes=boot.saturation.max_disk_bytes,
            ),
            codec_factory=lambda: select_codec(self.holder.snapshot_for(_INSTANCE_ID).compression),
            current_settings=current_settings_thunk,  # type: ignore[arg-type]
        )

    def rewrite(self, retention: dict[str, int], **top_level: Any) -> None:
        """Write a new YAML carrying ``retention`` and any extra blocks."""
        raw = copy.deepcopy(self.raw)
        raw.setdefault("retention", {}).update(retention)
        for block, value in top_level.items():
            raw.setdefault(block, {}).update(value)
        self.settings_path.write_text(yaml.safe_dump(raw))

    async def reload(self) -> list[str]:
        """Run the REAL reload path against the current YAML."""
        return await apply_reload(self.holder, self.settings_path, [self.ctx])


def _inverted_succeeded_pair() -> dict[str, int]:
    """The violating retention block: a succeeded body outliving its row."""
    return {
        "succeeded_metadata_seconds": _METADATA_SECONDS,
        "succeeded_body_seconds": _BODY_SECONDS,
    }


async def test_apply_reload_rejects_an_inverted_retention_window(tmp_path: Path) -> None:
    """The defect itself: an inverted window must not reach the live config.

    Objective: ``apply_reload`` must run the same floor check the boot
    path runs, on the freshly loaded Settings, before anything is
    swapped. Success is a ``ConfigInvariantError`` whose message names
    the state and both field names, so the operator can fix the YAML
    without reading the source.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(_inverted_succeeded_pair())

    with pytest.raises(ConfigInvariantError) as excinfo:
        await producer.reload()

    message = str(excinfo.value)
    assert "succeeded_body_seconds" in message
    assert "succeeded_metadata_seconds" in message


async def test_a_rejected_reload_leaves_the_live_snapshot_untouched(tmp_path: Path) -> None:
    """The harm, not just the raise: reject-and-keep-previous, never half-apply.

    Objective: the violating YAML also carries two perfectly legal edits,
    a new ``saturation.max_in_flight`` and a new
    ``retention.reaper_interval_seconds``. A rejected reload must install
    NEITHER: the check runs before the snapshot build, the holder swap
    and the per-instance pushes, so the running config is exactly what it
    was.

    The call is allowed to RETURN via ``contextlib.suppress`` rather than
    asserted with ``pytest.raises``, deliberately. Pre-fix no exception
    exists, so ``pytest.raises`` would die at DID NOT RAISE and never
    reach the assertions that show the harm: the holder carrying the
    inverted window and the gate carrying the new cap.

    Both boot values are read from the live holder and the live gate
    rather than spelled. ``succeeded_body_seconds`` happens to default to
    0 today, so a literal would work while silently coupling this test to
    a shipped default; ``max_in_flight`` is probe-filled from machine
    facts, so no literal is correct on two hosts.
    """
    producer = _Producer(tmp_path)
    boot_body_seconds = producer.holder.snapshot_for(_INSTANCE_ID).retention.succeeded_body_seconds
    boot_max_in_flight = producer.ctx.saturation.max_in_flight
    producer.rewrite(
        {**_inverted_succeeded_pair(), "reaper_interval_seconds": _OTHER_REAPER_INTERVAL},
        saturation={"max_in_flight": _OTHER_MAX_IN_FLIGHT},
    )

    with contextlib.suppress(ConfigInvariantError):
        await producer.reload()

    snapshot = producer.holder.snapshot_for(_INSTANCE_ID)
    assert snapshot.retention.succeeded_body_seconds == boot_body_seconds
    assert snapshot.retention.reaper_interval_seconds != _OTHER_REAPER_INTERVAL
    assert producer.ctx.saturation.max_in_flight == boot_max_in_flight


async def test_sighup_path_swallows_a_floor_violation_and_keeps_running(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The SIGHUP posture, which has no return surface at all.

    Objective: ``_sighup_reload``'s contract is that a bad settings file
    must not crash the process. A floor violation joins the shared
    failure set, so it gets the identical treatment: log through
    ``logger.exception`` and keep the previous snapshot.

    Pre-fix this also returns normally, but for the wrong reason (nothing
    raised), and the snapshot is the inverted one, so the snapshot
    assertion is what fails.
    """
    producer = _Producer(tmp_path)
    boot_body_seconds = producer.holder.snapshot_for(_INSTANCE_ID).retention.succeeded_body_seconds
    producer.rewrite(_inverted_succeeded_pair())

    with caplog.at_level("ERROR", logger="phantom.runtime.reload"):
        await _sighup_reload(producer.holder, producer.settings_path, [producer.ctx])

    assert caplog.records, "a swallowed reload failure must still be logged"
    snapshot = producer.holder.snapshot_for(_INSTANCE_ID)
    assert snapshot.retention.succeeded_body_seconds == boot_body_seconds


def _reload_capable_admin_app(producer: _Producer) -> FastAPI:
    """Mount the admin router wired for reload exactly as production does."""
    app = FastAPI()
    app.include_router(admin_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.state.settings_holder = producer.holder
    app.state.settings_path = producer.settings_path
    app.state.instances = [producer.ctx]
    return app


def test_admin_reload_answers_422_envelope_invalid_for_a_floor_violation(
    tmp_path: Path,
) -> None:
    """The operator-facing contract, in the ADR-017 envelope rather than a 500.

    Objective: the floor violation must reach the operator as the same
    422 ``envelope_invalid`` every other reload validation failure
    produces, because the mechanism is the one shared
    ``RELOAD_FAILURE_ERRORS`` set rather than a second bespoke arm.

    On the PURE pre-fix tree there is no raise at all, so this call
    returns 200 and the assertion fails on the status code. The
    intermediate state, where the check exists but the exception is
    outside the failure set, is a raw 500; naming both keeps a pre-fix
    observation from being misreported.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(_inverted_succeeded_pair())
    client = TestClient(_reload_capable_admin_app(producer), raise_server_exceptions=False)

    response = client.post("/v1/admin/reload")

    assert response.status_code == 422, (
        f"expected the shared reject path's 422; got {response.status_code} body {response.text!r}"
    )
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "envelope_invalid"


async def test_a_valid_retention_reload_still_applies(tmp_path: Path) -> None:
    """Counter-test: the new door must not refuse a legal retention edit.

    Objective: a check at the config door is only correct if it passes
    everything the boot check passes. A coherently shortened pair must
    reload exactly as before, or F14 has turned a fix into an outage.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(
        {
            "succeeded_metadata_seconds": _VALID_METADATA_SECONDS,
            "succeeded_body_seconds": _VALID_BODY_SECONDS,
        }
    )

    reloaded = await producer.reload()

    assert reloaded == [_INSTANCE_ID]
    snapshot = producer.holder.snapshot_for(_INSTANCE_ID)
    assert snapshot.retention.succeeded_metadata_seconds == _VALID_METADATA_SECONDS
    assert snapshot.retention.succeeded_body_seconds == _VALID_BODY_SECONDS


async def test_forever_sentinels_are_still_accepted(tmp_path: Path) -> None:
    """The ``-1`` semantics behave at the reload door exactly as at boot.

    Objective: ``-1`` means forever, and the floor's two arms are not
    symmetric. A forever METADATA window is a forever row, so any finite
    body window under it is fine. A forever BODY window under a finite
    metadata window is the violation in its purest form: the row is
    reaped and the body is kept forever. Success is the first reloading
    cleanly and the second raising.
    """
    producer = _Producer(tmp_path)

    producer.rewrite(
        {
            "succeeded_metadata_seconds": _FOREVER,
            "succeeded_body_seconds": _VALID_BODY_SECONDS,
        }
    )
    await producer.reload()
    assert producer.holder.snapshot_for(_INSTANCE_ID).retention.succeeded_metadata_seconds == (
        _FOREVER
    )

    producer.rewrite(
        {
            "succeeded_metadata_seconds": _VALID_METADATA_SECONDS,
            "succeeded_body_seconds": _FOREVER,
        }
    )
    with pytest.raises(ConfigInvariantError):
        await producer.reload()
