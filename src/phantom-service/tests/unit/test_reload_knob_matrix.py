"""The ADR-013 knob table, enforced: every reloadable knob reaches its consumer.

ADR-031 makes live-snapshot read the canonical distribution mechanism,
enumerates the two exceptions (the saturation-gate cap push and the
retry-strategy rebuild), and declares the ADR-013 table the single
contract. This module IS that table's enforcement: for every reloadable
row it boots real Settings from YAML (probe ON, exactly like
production), swaps in a changed value through the REAL ``apply_reload``,
and asserts the consumer-visible truth.

Observation level per mechanism, deliberately:

* live-read rows assert the LIVE SNAPSHOT (or the repointed ``cfg``)
  carries the new value - the per-tick/per-request consumer reads are
  pinned component-by-component by the tests the ADR-013 table cites
  (R6-2 watcher, T1 janitor cadence, hot-reload e2e, ...); this module
  pins the YAML-to-snapshot leg those tests assume.
* push rows assert the pushed artifact (the gate's caps).
* rebuild rows assert the rebuilt artifact (the strategy object; the
  codec factory's product).

``test_matrix_mirrors_the_adr_table`` pins the case set against a
hand-maintained literal mirror of the table's reloadable rows (the ADR
markdown itself is not parsed, so a knob added ONLY to the ADR text
must be brought here by review - C7 enforcement-scope note); a
distribution regression fails its knob's case. This converts the
loop's config-distribution defect class (R5-2, R6-2, R7-1, R8-1, R8-2,
T1) from adversary-priced discovery to CI-priced regression.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml
from phantom.compression import select_codec
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.runtime.reload import apply_reload
from phantom.strategies import build_retry_strategy
from phantom.workers.saturation import SaturationGate

pytestmark = pytest.mark.asyncio

# The single instance id used by every producer.
_INSTANCE_ID = "inst-a"

# Values chosen to differ from every default so a silently-unapplied
# reload can never pass by coincidence.
_RETENTION_SUCCEEDED_B = 777
_REAPER_INTERVAL_B = 33
_LINGER_B = 123
_RAM_POLL_B = 7.5
_RAM_CEILING_B = 31_337
_ORPHAN_SWEEP_B = 99
_AUDIT_PERIOD_B = 44
_PERSIST_THRESHOLD_B = 4_242
_COMPRESSION_LEVEL_B = 5
_SAT_IN_FLIGHT_B = 55
_SAT_BYTES_B = 123_456
_SAT_DISK_B = 999_999
_RETRY_INTERVALS_B = [9, 9]
_LOOKUP_CAPTURE_B = "create_file"
_LOOKUP_PATH_B = "$.id"


@dataclass(frozen=True)
class _Producer:
    """One booted reload harness: real Settings, holder, minimal real ctx."""

    holder: SettingsHolder
    ctx: InstanceContext
    settings_path: Path
    raw: dict[str, Any]


@dataclass(frozen=True)
class _KnobCase:
    """One ADR-013 table row: how to change it, where its truth is read."""

    knob: str
    mutate: Callable[[dict[str, Any]], None]
    observe: Callable[[_Producer], object]
    expected: object


def _base_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A probe-reliant one-instance config (the smart-defaults posture)."""
    return {
        "storage": {"data_dir": str(data_dir)},
        "instances": [
            {
                "id": _INSTANCE_ID,
                "host_prefixes": ["files.example.com"],
                "data_dir": _INSTANCE_ID,
                "routes": [
                    {
                        "name": "files",
                        "hosts": ["files.example.com"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
    }


def _boot_producer(tmp_path: Path) -> _Producer:
    """Boot exactly as production does and assemble the reload surface."""
    raw = _base_yaml_payload(tmp_path / "data")
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(raw))
    boot = Settings.reload_from_yaml(settings_path)
    cfg = boot.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})

    def current_settings_thunk() -> object:
        return holder.snapshot_for(_INSTANCE_ID)

    assert boot.saturation.max_in_flight is not None
    assert boot.saturation.max_in_flight_bytes is not None
    assert boot.saturation.max_disk_bytes is not None
    ctx = InstanceContext(
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
        # Mirrors app.py's factory: the codec comes from the LIVE
        # snapshot's compression block per call (R5-2).
        codec_factory=lambda: select_codec(holder.snapshot_for(_INSTANCE_ID).compression),
        current_settings=current_settings_thunk,  # type: ignore[arg-type]
    )
    return _Producer(holder=holder, ctx=ctx, settings_path=settings_path, raw=raw)


def _snapshot(producer: _Producer) -> Any:
    """The live per-instance snapshot, as every per-use reader sees it."""
    return producer.holder.snapshot_for(_INSTANCE_ID)


_CASES: tuple[_KnobCase, ...] = (
    _KnobCase(
        knob="retention.windows",
        mutate=lambda raw: raw.setdefault("retention", {}).update(
            {"succeeded_metadata_seconds": _RETENTION_SUCCEEDED_B}
        ),
        observe=lambda producer: _snapshot(producer).retention.succeeded_metadata_seconds,
        expected=_RETENTION_SUCCEEDED_B,
    ),
    _KnobCase(
        knob="retention.reaper_interval_seconds",
        mutate=lambda raw: raw.setdefault("retention", {}).update(
            {"reaper_interval_seconds": _REAPER_INTERVAL_B}
        ),
        observe=lambda producer: _snapshot(producer).retention.reaper_interval_seconds,
        expected=_REAPER_INTERVAL_B,
    ),
    _KnobCase(
        knob="body_store.linger_seconds",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("body_store", {})
            .update({"linger_seconds": _LINGER_B})
        ),
        observe=lambda producer: _snapshot(producer).body_store.linger_seconds,
        expected=_LINGER_B,
    ),
    _KnobCase(
        knob="body_store.ram_pressure_poll_seconds",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("body_store", {})
            .update({"ram_pressure_poll_seconds": _RAM_POLL_B})
        ),
        observe=lambda producer: _snapshot(producer).body_store.ram_pressure_poll_seconds,
        expected=_RAM_POLL_B,
    ),
    _KnobCase(
        knob="body_store.ram_ceiling_bytes",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("body_store", {})
            .update({"ram_ceiling_bytes": _RAM_CEILING_B})
        ),
        observe=lambda producer: _snapshot(producer).body_store.ram_ceiling_bytes,
        expected=_RAM_CEILING_B,
    ),
    _KnobCase(
        knob="body_store.body_orphan_sweep_seconds",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("body_store", {})
            .update({"body_orphan_sweep_seconds": _ORPHAN_SWEEP_B})
        ),
        observe=lambda producer: _snapshot(producer).body_store.body_orphan_sweep_seconds,
        expected=_ORPHAN_SWEEP_B,
    ),
    _KnobCase(
        knob="body_store.invariant_audit_period_seconds",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("body_store", {})
            .update({"invariant_audit_period_seconds": _AUDIT_PERIOD_B})
        ),
        observe=lambda producer: _snapshot(producer).body_store.invariant_audit_period_seconds,
        expected=_AUDIT_PERIOD_B,
    ),
    _KnobCase(
        knob="persist_trigger.body_size_threshold_bytes",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("persist_trigger", {})
            .update({"body_size_threshold_bytes": _PERSIST_THRESHOLD_B})
        ),
        observe=lambda producer: _snapshot(producer).persist_trigger.body_size_threshold_bytes,
        expected=_PERSIST_THRESHOLD_B,
    ),
    _KnobCase(
        knob="compression.algorithm",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("compression", {})
            .update({"algorithm": "gzip"})
        ),
        # The consumer truth (R5-2): the codec the NEXT admission gets.
        observe=lambda producer: type(producer.ctx.codec_factory()).__name__,
        expected="GzipCodec",
    ),
    _KnobCase(
        knob="compression.level",
        mutate=lambda raw: (
            raw.setdefault("storage", {})
            .setdefault("compression", {})
            .update({"level": _COMPRESSION_LEVEL_B})
        ),
        observe=lambda producer: _snapshot(producer).compression.level,
        expected=_COMPRESSION_LEVEL_B,
    ),
    _KnobCase(
        knob="instance.capture_reexecution",
        mutate=lambda raw: raw["instances"][0].update({"capture_reexecution": True}),
        observe=lambda producer: _snapshot(producer).capture_reexecution,
        expected=True,
    ),
    _KnobCase(
        knob="instance.admin_lookup",
        mutate=lambda raw: raw["instances"][0].update(
            {"admin_lookup": {"capture_name": _LOOKUP_CAPTURE_B, "json_path": _LOOKUP_PATH_B}}
        ),
        # The lookup route reads the binding per request off the
        # repointed cfg.
        observe=lambda producer: (
            (producer.ctx.cfg.admin_lookup.capture_name, producer.ctx.cfg.admin_lookup.json_path)
            if producer.ctx.cfg.admin_lookup is not None
            else None
        ),
        expected=(_LOOKUP_CAPTURE_B, _LOOKUP_PATH_B),
    ),
    _KnobCase(
        knob="saturation.max_in_flight",
        mutate=lambda raw: raw.setdefault("saturation", {}).update(
            {"max_in_flight": _SAT_IN_FLIGHT_B}
        ),
        observe=lambda producer: producer.ctx.saturation.max_in_flight,
        expected=_SAT_IN_FLIGHT_B,
    ),
    _KnobCase(
        knob="saturation.max_in_flight_bytes",
        mutate=lambda raw: raw.setdefault("saturation", {}).update(
            {"max_in_flight_bytes": _SAT_BYTES_B}
        ),
        observe=lambda producer: producer.ctx.saturation.max_in_flight_bytes,
        expected=_SAT_BYTES_B,
    ),
    _KnobCase(
        knob="saturation.max_disk_bytes",
        mutate=lambda raw: raw.setdefault("saturation", {}).update({"max_disk_bytes": _SAT_DISK_B}),
        observe=lambda producer: producer.ctx.saturation.max_disk_bytes,
        expected=_SAT_DISK_B,
    ),
    _KnobCase(
        knob="retry.default_strategy",
        mutate=lambda raw: raw.setdefault("retry", {}).update(
            {
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": _RETRY_INTERVALS_B,
                }
            }
        ),
        # The rebuilt artifact (the ADR-031 rebuild exception).
        observe=lambda producer: type(producer.ctx.retry_strategy).__name__,
        expected="FixedIntervalsStrategy",
    ),
)


@pytest.mark.parametrize("case", _CASES, ids=lambda c: c.knob)
async def test_reloaded_knob_reaches_its_consumer(case: _KnobCase, tmp_path: Path) -> None:
    """One real reload per knob; the consumer-visible truth must follow.

    Boots the probe-reliant base config (so the case also rides the
    R7-1 re-resolution path), changes exactly this knob in the YAML,
    runs the REAL ``apply_reload``, and asserts at the level the
    ADR-013 table names for the knob's mechanism.
    """
    producer = _boot_producer(tmp_path)
    before = case.observe(producer)
    assert before != case.expected, (
        f"test bug: knob {case.knob} already reads {case.expected!r} before "
        "the reload; pick a value distinct from the boot default"
    )

    raw = copy.deepcopy(producer.raw)
    case.mutate(raw)
    producer.settings_path.write_text(yaml.safe_dump(raw))
    await apply_reload(producer.holder, producer.settings_path, [producer.ctx])

    assert case.observe(producer) == case.expected, (
        f"knob {case.knob} did not reach its consumer after a real reload "
        f"(still {case.observe(producer)!r}): the ADR-013/ADR-031 distribution "
        "contract is broken for this row"
    )


async def test_matrix_mirrors_the_adr_table() -> None:
    """The matrix covers every reloadable row of the ADR-013 table.

    This literal set mirrors the table's reloadable rows (the
    restart-required rows are pinned elsewhere: topology by
    ``test_reload_instance_removed.py``, ad_mint by the reload unit
    tests). Adding a knob to the matrix without this mirror set, or
    removing a case, fails here; the ADR markdown is not parsed, so a
    table edit must be brought to BOTH in the same commit. Async only
    to sit cleanly under the module-wide asyncio pytestmark (the body
    is pure set comparison; the sync form drew a PytestWarning on
    every run).
    """
    expected_rows = {
        "retention.windows",
        "retention.reaper_interval_seconds",
        "body_store.linger_seconds",
        "body_store.ram_pressure_poll_seconds",
        "body_store.ram_ceiling_bytes",
        "body_store.body_orphan_sweep_seconds",
        "body_store.invariant_audit_period_seconds",
        "persist_trigger.body_size_threshold_bytes",
        "compression.algorithm",
        "compression.level",
        "instance.capture_reexecution",
        "instance.admin_lookup",
        "saturation.max_in_flight",
        "saturation.max_in_flight_bytes",
        "saturation.max_disk_bytes",
        "retry.default_strategy",
    }
    assert {case.knob for case in _CASES} == expected_rows
