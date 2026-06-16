"""A topology-ADDING reload must warn like the omission leg does (R9-7).

ADR-013's topology row says instance add/remove/rename is
restart-required, with the implemented warn-and-keep-running posture:
the reload "warns per omitted instance and CARRIES ITS PREVIOUS
SNAPSHOT FORWARD" (R8-1). The ADD direction has no posture at all
(``runtime/reload.py``): ``apply_reload`` builds a snapshot for every
id in the new YAML, installs the added id's snapshot into the holder (a
dead entry no InstanceContext ever reads), and returns
``sorted(snapshots.keys())`` - which the admin route serializes
verbatim as ``{"reloaded_instances": [...]}``. The propagation loop
iterates only LIVE instances, so nothing notices the new id: no
warning, no log line, and the operator's reload response lists the
brand-new instance as reloaded.

Net: an operator who adds an instance to the YAML and hot-reloads
(the natural first try; topology being restart-required is exactly the
kind of fact the warn posture exists to teach) gets a 200 whose
``reloaded_instances`` includes the new id - positive confirmation of
something that did NOT happen. No dispatcher entry exists; ingress for
the new prefix answers 421; nothing anywhere says "restart required".
The omission leg warns; the addition leg is silent. LOW severity:
operator-truthfulness on the contract edge, no data harm (the dead
holder entry is unread), the R8-2/R8-5 contract-edge class.

The test boots real ``Settings``, adds a second instance to the YAML,
runs the REAL ``apply_reload``, and requires a WARNING naming the added
instance - the minimal contract any fix posture satisfies (warn and
keep running, warn and exclude it from the reload report, or refuse).
Scratch confirmed today's behavior: the added id is reported reloaded,
zero warnings fire, and the holder carries a dead snapshot entry for
it.
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from phantom.config.settings import Settings
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.runtime.reload import apply_reload

pytestmark = pytest.mark.asyncio

# The instance booted at startup and the one the operator adds by YAML.
_BOOT_INSTANCE_ID = "inst-a"
_ADDED_INSTANCE_ID = "inst-new"

_R9_7_REASON: str = (
    "R9-7: apply_reload handles a topology-ADDING reload with total silence "
    "- no warning (the omission leg warns per ADR-013's warn-and-keep-running "
    "posture), a dead holder entry for the added id, and the added id "
    "reported in the route's reloaded_instances - so the operator gets "
    "positive confirmation for an instance that does not exist and no signal "
    "that topology changes require a restart"
)


def _base_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A probe-reliant one-instance config (the smart-defaults posture)."""
    return {
        "storage": {"data_dir": str(data_dir)},
        "instances": [
            {
                "id": _BOOT_INSTANCE_ID,
                "host_prefixes": ["files.example.com"],
                "data_dir": _BOOT_INSTANCE_ID,
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


def _added_instance_block() -> dict[str, Any]:
    """The YAML block for the instance the operator adds post-boot."""
    return {
        "id": _ADDED_INSTANCE_ID,
        "host_prefixes": ["other.example.com"],
        "data_dir": _ADDED_INSTANCE_ID,
        "routes": [
            {
                "name": "other",
                "hosts": ["other.example.com"],
                "auth_mode": "phantom_bearer",
            }
        ],
    }


async def test_added_instance_reload_warns_like_the_omission_leg(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Adding an instance by reload must surface a restart-required warning.

    Attack: boot one instance, add a second to the YAML, hot-reload.
    Topology is restart-required and the omitted direction warns per
    instance (R8-1); the added direction must warn too - it is the same
    operator mistake in the other direction, and the warning is the
    only signal that the new instance is NOT live. Today the reload is
    silent and even reports the added id as reloaded.
    """
    raw = _base_yaml_payload(tmp_path / "data")
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(raw))
    boot = Settings.reload_from_yaml(settings_path)
    cfg = boot.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})

    new_raw = copy.deepcopy(raw)
    new_raw["instances"].append(_added_instance_block())
    settings_path.write_text(yaml.safe_dump(new_raw))
    reload_ctx = MagicMock()
    reload_ctx.cfg = cfg
    reload_ctx.token_cache = MagicMock()
    reload_ctx.saturation = MagicMock(update_caps=AsyncMock())

    with caplog.at_level(logging.WARNING, logger="phantom.runtime.reload"):
        await apply_reload(holder, settings_path, [reload_ctx])

    added_warnings = [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and _ADDED_INSTANCE_ID in record.getMessage()
    ]
    assert added_warnings, (
        "a topology-ADDING reload fired no warning naming the added instance "
        f"{_ADDED_INSTANCE_ID!r}: the operator's only signals are a 200 whose "
        "reloaded_instances includes the id (positive confirmation of an "
        "instance that does not exist) and 421s on its host prefix - the "
        "omission leg warns, the addition leg must too"
    )
