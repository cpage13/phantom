"""``apply_reload`` rebuilds the per-instance retry strategy (R5-2).

ADR-013 lists retry parameters as hot-reloadable. Before the R5-2 fix,
``InstanceContext.retry_strategy`` was built once at composition and
``apply_reload`` never rebuilt it, so reloaded retry params silently
never applied. This pin drives the real ``apply_reload`` against a
YAML rewrite and asserts the strategy slot is rebuilt from the new
``retry.default_strategy`` block, behaviorally (the next scheduling
decision follows the new parameters) and by type.

Only the pieces ``apply_reload`` actually touches are real (cfg,
holder, saturation gate); the storage components are inert mocks, so
this module starts nothing that needs a teardown.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.runtime.reload import apply_reload
from phantom.strategies import (
    ExponentialBackoffStrategy,
    FixedIntervalsStrategy,
    build_retry_strategy,
)
from phantom.workers.saturation import SaturationGate

# The boot-time fixed-intervals schedule; one entry keeps the
# behavioral assertion unambiguous.
_BOOT_INTERVAL_SECONDS = 11
# The reloaded exponential-backoff base; attempts=0 yields exactly this
# delay because jitter is pinned to zero on both sides.
_RELOADED_BASE_SECONDS = 7.0
# Saturation values pinned in the YAML so the skip-probe reload leaves
# no holes for the gate's update_caps push.
_SATURATION_PINS: dict[str, int] = {
    "max_in_flight": 10,
    "max_in_flight_bytes": 10_000_000,
    "max_disk_bytes": 10_000_000,
    "large_body_threshold_bytes": 1_000_000,
    "max_large_in_flight": 4,
}


def _yaml_payload(retry_strategy: dict[str, Any]) -> dict[str, Any]:
    """One-instance settings payload with ``retry.default_strategy`` set."""
    return {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["files.example.com"],
                "data_dir": "primary",
                "routes": [
                    {
                        "name": "files",
                        "hosts": ["files.example.com"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
        "saturation": dict(_SATURATION_PINS),
        "retry": {"default_strategy": retry_strategy},
    }


@pytest.mark.asyncio
async def test_apply_reload_rebuilds_retry_strategy(tmp_path: Path) -> None:
    """A reloaded ``retry.default_strategy`` reaches the strategy slot."""
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            _yaml_payload(
                {
                    "type": "fixed_intervals",
                    "intervals_seconds": [_BOOT_INTERVAL_SECONDS],
                    "jitter": 0.0,
                }
            )
        )
    )
    boot_settings = Settings.reload_from_yaml(settings_path, skip_probe=True)
    cfg = boot_settings.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot_settings, cfg)})
    ctx = InstanceContext(
        cfg=cfg,
        store=MagicMock(),
        ram_body_store=MagicMock(),
        file_body_store=MagicMock(),
        body_store=MagicMock(),
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=build_retry_strategy(boot_settings.retry.default_strategy),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=_SATURATION_PINS["max_in_flight"],
            max_in_flight_bytes=_SATURATION_PINS["max_in_flight_bytes"],
            max_disk_bytes=_SATURATION_PINS["max_disk_bytes"],
        ),
        codec_factory=MagicMock(),
        current_settings=lambda: holder.snapshot_for(cfg.id),
    )
    assert isinstance(ctx.retry_strategy, FixedIntervalsStrategy)
    boot_delay = ctx.retry_strategy.schedule_next_attempt(
        attempts=0,
        since_received=timedelta(seconds=0),
        last_error=None,
        route_name="files",
    )
    assert boot_delay == timedelta(seconds=_BOOT_INTERVAL_SECONDS)

    settings_path.write_text(
        yaml.safe_dump(
            _yaml_payload(
                {
                    "type": "exponential_backoff",
                    "base_seconds": _RELOADED_BASE_SECONDS,
                    "factor": 2.0,
                    "cap_seconds": 100.0,
                    "jitter": 0.0,
                    "max_attempts": -1,
                    "max_duration_seconds": 86_400,
                }
            )
        )
    )
    reloaded = await apply_reload(holder, settings_path, [ctx])

    assert reloaded == [cfg.id]
    assert isinstance(ctx.retry_strategy, ExponentialBackoffStrategy), (
        "apply_reload must rebuild the retry strategy from the reloaded "
        "retry.default_strategy block (R5-2; ADR-013)"
    )
    reloaded_delay = ctx.retry_strategy.schedule_next_attempt(
        attempts=0,
        since_received=timedelta(seconds=0),
        last_error=None,
        route_name="files",
    )
    assert reloaded_delay == timedelta(seconds=_RELOADED_BASE_SECONDS), (
        "the next scheduling decision must follow the reloaded parameters"
    )
