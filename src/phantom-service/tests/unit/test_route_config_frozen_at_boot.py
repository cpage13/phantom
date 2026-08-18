"""D1/F5: the per-instance route block is frozen at boot, for every reader.

``routes``, ``host_prefixes`` and ``data_dir`` are restart-required
(ADR-013). Before F5 ``apply_reload`` repointed ``InstanceContext.cfg`` at
the freshly-loaded block, so admission, both kickers, the dispatcher, the
admin quarantine paths and the two admin status surfaces all followed the
reload while the executor kept the boot table: route config was
split-brain from the first SIGHUP onward.

This module boots the real reload harness (real ``Settings`` from a
probe-reliant YAML, a real ``SettingsHolder``, the REAL ``apply_reload``)
and pins both halves of the fix:

* the freeze itself, observed at the readers the split-brain hurt, and
  as the object identity a future refactor would have to break;
* the operator-facing warn arm, which names the drifted field names on
  every reload and stays quiet when nothing frozen moved.

The two knobs that used to ride the repoint, ``capture_reexecution`` and
``admin_lookup``, must still reload; the first is pinned here at its
consumer, the second by ``tests/e2e/test_hot_reload.py``'s three-leg
binding test.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
import yaml
from phantom.compression import select_codec
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext, instance_storage_paths
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.models.chain import ChainEnvelope, ChainStep
from phantom.models.upload import UploadRow
from phantom.routes.admission import (
    AdmissionInputs,
    _build_row,
    _encode_and_hash_bodies,
    _resolved_route_or_none,
)
from phantom.routing import resolve_route
from phantom.runtime.reload import apply_reload
from phantom.strategies import build_retry_strategy
from phantom.workers._kicker_auth_mode import row_resolved_route
from phantom.workers.saturation import SaturationGate
from pydantic import ValidationError

pytestmark = pytest.mark.asyncio

# The single instance id and host the harness declares. Both are read back
# in assertions, so they are named rather than spelled twice.
_INSTANCE_ID = "inst-a"
_BOOT_HOST = "files.example.com"
_BOOT_URL = f"https://{_BOOT_HOST}/v2/files"

# Values the reload rewrites the frozen blocks to. Each must differ from
# the boot value or the drift arm has nothing to detect.
_OTHER_HOST = "other.example.com"
_OTHER_DATA_DIR = "inst-a-moved"

# A reloadable knob used by the counter-test, which must stay quiet.
_RETENTION_SUCCEEDED_B = 777


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
    """One booted reload harness: real Settings, holder, minimal real ctx.

    Copied from ``test_reload_knob_matrix.py``'s ``_boot_producer`` with
    two adaptations F5 needs: ``boot_cfg`` keeps a reference to the boot
    :class:`InstanceCfg` so the freeze can be asserted as an identity, and
    ``data_root`` keeps the top-level storage root so the ``data_dir``
    reader can be re-derived after a reload.
    """

    def __init__(self, tmp_path: Path) -> None:
        """Boot exactly as production does and assemble the reload surface."""
        self.data_root = tmp_path / "data"
        self.raw = _base_yaml_payload(self.data_root)
        self.settings_path = tmp_path / "phantom.yaml"
        self.settings_path.write_text(yaml.safe_dump(self.raw))
        boot = Settings.reload_from_yaml(self.settings_path)
        cfg = boot.instances[0]
        self.boot_cfg = cfg
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

    def rewrite(self, mutate: Callable[[dict[str, Any]], None]) -> None:
        """Apply ``mutate`` to a deep copy of the raw payload and write it.

        The mutation is surgical on ``raw["instances"][0]`` where the test
        needs it, which is the same reason the e2e helpers avoid a
        list-replacing deep merge.
        """
        raw = copy.deepcopy(self.raw)
        mutate(raw)
        self.settings_path.write_text(yaml.safe_dump(raw))

    async def reload(self) -> list[str]:
        """Run the REAL reload path against the current YAML."""
        return await apply_reload(self.holder, self.settings_path, [self.ctx])


def _envelope() -> ChainEnvelope:
    """A one-step chain aimed at the harness's routed host."""
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[
            ChainStep(  # type: ignore[call-arg]
                name="step",
                method="POST",
                url=_BOOT_URL,
            )
        ],
    )


def _inputs(envelope: ChainEnvelope) -> AdmissionInputs:
    """Admission inputs with NO Authorization header.

    The harness's token cache is a plain ``MagicMock``, and the bearer arm
    of ``_build_row`` awaits ``token_cache.set(...)``; omitting the header
    keeps the builder on the pure path this test cares about.
    """
    return AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs={},
        authorization=None,
        content_encoding=None,
    )


def _flip_auth_mode(raw: dict[str, Any]) -> None:
    """Rewrite the single route's auth_mode to the other kind."""
    raw["instances"][0]["routes"][0].update({"auth_mode": "aws_sigv4"})


async def test_reloaded_auth_mode_does_not_split_the_readers_view(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The F5 defect itself: every ctx reader must keep the executor's route.

    Objective: a reload that flips a route's ``auth_mode`` must not move
    the value the kickers and admission resolve, because the executor was
    constructed with the boot :class:`InstanceCfg` and resolves against it
    forever. Success is both readers still reporting ``phantom_bearer``,
    which is what the executor will inject.

    Pre-fix both read ``aws_sigv4`` while the executor injects a bearer:
    the AuthKicker stops claiming the parked rows, the CredentialKicker
    claims them, the upstream 401s, and the row cycles forever (F6-shaped
    livelock with an operator fix that never lands).
    """
    producer = _Producer(tmp_path)
    producer.rewrite(_flip_auth_mode)
    await producer.reload()

    row = make_upload_row(endpoint=_BOOT_HOST)

    assert row_resolved_route(row, producer.ctx).auth_mode == "phantom_bearer"
    assert resolve_route(row.endpoint, producer.ctx.cfg).auth_mode == "phantom_bearer"


async def test_reloaded_auth_mode_does_not_move_the_f11_cache_gate(tmp_path: Path) -> None:
    """The Phase 2 composition: the F11 bearer-cache gate rides the freeze.

    Objective: § 2.5's ``_resolved_route_or_none`` decides whether an
    inbound ``Authorization`` is cached, and it resolves ``instance_ctx.cfg``.
    After F5 that object is the boot snapshot, so a reloaded ``auth_mode``
    cannot change which routes cache. Asserted at § 2.5's own helper rather
    than at a neighbouring reader, so the composition is proved where it
    lives.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(_flip_auth_mode)
    await producer.reload()

    resolved = _resolved_route_or_none(_BOOT_URL, producer.ctx)
    assert resolved is not None
    assert resolved.auth_mode == "phantom_bearer"


async def test_reload_leaves_the_boot_instance_cfg_object_in_place(tmp_path: Path) -> None:
    """The freeze stated as the object identity it is.

    Objective: a future refactor that reintroduces any rebinding of
    ``InstanceContext.cfg`` fails HERE rather than in a subtle consumer
    three modules away. Success is ``ctx.cfg is boot_cfg`` after a reload
    that really did change the block.
    """
    producer = _Producer(tmp_path)
    boot_cfg = producer.boot_cfg
    producer.rewrite(_flip_auth_mode)
    await producer.reload()

    assert producer.ctx.cfg is boot_cfg


async def test_reloaded_host_prefixes_do_not_move_dispatch(tmp_path: Path) -> None:
    """The dispatcher half of D1.

    Objective: ``InstanceDispatcher.resolve`` matches on
    ``ctx.cfg.host_prefixes``. After F5 a reloaded prefix list does not
    move dispatch, so the instance still accepts the boot host. Pre-fix the
    resolve raises ``NoMatchingInstanceError``, which the send route maps
    to ``421 invalid_target``.
    """
    producer = _Producer(tmp_path)
    dispatcher = InstanceDispatcher([producer.ctx])
    producer.rewrite(lambda raw: raw["instances"][0].update({"host_prefixes": [_OTHER_HOST]}))
    await producer.reload()

    assert dispatcher.resolve(_BOOT_URL, None) is producer.ctx


async def test_reloaded_data_dir_does_not_move_the_instance_storage_paths(
    tmp_path: Path,
) -> None:
    """The data_dir half, which the findings record does not walk.

    Objective: the live store's paths are fixed once at boot, so the admin
    quarantine inventory and restore, which recompute
    ``instance_storage_paths(data_root, ctx.cfg)`` per request, must agree
    with it after a reload. Pre-fix they look for manifests in a directory
    the running instance has never written to.
    """
    producer = _Producer(tmp_path)
    boot_paths = instance_storage_paths(producer.data_root, producer.ctx.cfg)
    producer.rewrite(lambda raw: raw["instances"][0].update({"data_dir": _OTHER_DATA_DIR}))
    await producer.reload()

    assert instance_storage_paths(producer.data_root, producer.ctx.cfg).data_root == (
        boot_paths.data_root
    )


async def test_restart_required_drift_warns_on_every_reload(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The operator-facing arm, including its durability.

    Objective: the drift warning must name the instance and the drifted
    field NAME, must not splice unbounded operator input (a route table, a
    step URL pattern) into the log stream, and must repeat on every reload
    rather than going quiet once the first one has been seen. With
    ``ctx.cfg`` frozen, both restart-required arms compare boot against new
    forever, so an operator who greps the log after the second reload still
    finds the warning.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(lambda raw: raw["instances"][0]["routes"][0].update({"hosts": [_OTHER_HOST]}))

    with caplog.at_level(logging.WARNING, logger="phantom.runtime.reload"):
        await producer.reload()
        first = [r for r in caplog.records if "restart-required" in r.getMessage()]
        caplog.clear()
        await producer.reload()
        second = [r for r in caplog.records if "restart-required" in r.getMessage()]

    assert len(first) == 1
    assert len(second) == 1
    for record in (*first, *second):
        message = record.getMessage()
        assert "routes" in message
        assert _INSTANCE_ID in message
        assert _OTHER_HOST not in message


async def test_unchanged_instance_block_logs_no_restart_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Counter-test: a purely reloadable edit must stay quiet.

    Objective: a warning that fires on every reload regardless of what
    changed is noise an operator learns to ignore. Reloading only
    ``retention.succeeded_metadata_seconds`` must produce no
    restart-required record.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(
        lambda raw: raw.setdefault("retention", {}).update(
            {"succeeded_metadata_seconds": _RETENTION_SUCCEEDED_B}
        )
    )

    with caplog.at_level(logging.WARNING, logger="phantom.runtime.reload"):
        await producer.reload()

    assert [r for r in caplog.records if "restart-required" in r.getMessage()] == []


async def test_all_three_frozen_blocks_are_named_in_one_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The arm reports the whole drift set, not the first field it finds.

    Objective: an operator who edited all three frozen blocks must be told
    about all three, in one record, or they will restart, re-check, and
    discover the second refusal only on the next reload.
    """
    producer = _Producer(tmp_path)

    def _drift_all(raw: dict[str, Any]) -> None:
        raw["instances"][0].update({"host_prefixes": [_OTHER_HOST], "data_dir": _OTHER_DATA_DIR})
        raw["instances"][0]["routes"][0].update({"auth_mode": "aws_sigv4"})

    producer.rewrite(_drift_all)

    with caplog.at_level(logging.WARNING, logger="phantom.runtime.reload"):
        await producer.reload()

    drift = [r for r in caplog.records if "restart-required" in r.getMessage()]
    assert len(drift) == 1
    message = drift[0].getMessage()
    for field in ("routes", "host_prefixes", "data_dir"):
        assert field in message


async def test_instance_cfg_rejects_field_assignment(tmp_path: Path) -> None:
    """The immutability that makes one shared object safe to share.

    Objective: after F5 a single :class:`InstanceCfg` is read by every
    route reader for the whole process lifetime, so an in-place field
    assignment would be a silent global change. ``frozen=True`` turns it
    into a pydantic error at the assignment site.

    This is NOT a restatement of the no-rebinding test.
    ``ctx.cfg = <new object>`` remains legal Python, because
    :class:`InstanceContext` is a mutable dataclass by design; that
    absence is pinned by
    ``test_reload_leaves_the_boot_instance_cfg_object_in_place``. This test
    pins in-place mutation of the shared object instead.
    """
    producer = _Producer(tmp_path)
    with pytest.raises(ValidationError):
        producer.ctx.cfg.routes = []  # type: ignore[misc]


async def test_capture_reexecution_still_reloads_through_the_snapshot(
    tmp_path: Path,
) -> None:
    """D1 must not silently demote a documented reloadable knob.

    Objective: ``capture_reexecution`` is an ADR-013 live-read row that
    used to ride the ``cfg`` repoint. F5 deletes the repoint, so the stamp
    must come from the live snapshot instead. Asserted at the CONSUMER, the
    admitted row, because the matrix already observes the snapshot leg; this
    test exists to prove the consumer moved onto it.
    """
    producer = _Producer(tmp_path)
    producer.rewrite(lambda raw: raw["instances"][0].update({"capture_reexecution": True}))
    await producer.reload()

    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(producer.ctx, {})
    prepared = await _build_row(_inputs(envelope), producer.ctx, encoded)

    assert prepared.row.capture_reexecution_active is True
