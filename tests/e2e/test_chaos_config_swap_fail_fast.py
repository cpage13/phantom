"""Operational-chaos E2E: a stale / invalid config fails boot fast and loud (§ 5.C).

Adversary 2, the config-swap chaos. The regression for the § 4 deployment-config
defect: a configuration carrying a key that no longer exists (a stale field left
over from an older release) or a wrong-typed value MUST stop the boot loudly, not
silently fall back to defaults or boot a half-configured service. Every Phantom
settings model is ``ConfigDict(strict=True, extra="forbid")``, and
``load_settings`` (the validator the real boot path and ``boot_stack`` both run)
wraps a pydantic ``ValidationError`` as ``SettingsError`` (a ``ValueError``).

These drive the SAME in-process boot validator the suite uses
(``boot_stack`` -> ``_build_phantom_settings`` -> ``load_settings``), feeding a
``config_overrides`` overlay that an operator's stale YAML would carry:

* :func:`test_stale_removed_key_fails_boot` - ``storage.default_tier`` was removed
  in Phase 1 (subsumed by ``body_store.mode``); a config still carrying it is the
  canonical stale-deployment shape. ``extra="forbid"`` rejects it -> the boot
  raises before any server is ready.
* :func:`test_wrong_typed_value_fails_boot` - a structurally-valid-but-wrong-typed
  value (a string where an int is required) is the other half of "loud, not
  silent": strict mode refuses the coercion rather than guessing.
* :func:`test_load_settings_is_the_boot_validator` - asserts directly against
  ``load_settings`` (the function ``boot_stack`` calls and ``python -m phantom``
  runs) so the regression is pinned to the production validator, not just the
  test harness.

Public e2e-light lane (§ 5.0): generic config shapes.

Falsifier: relax any settings model to ``extra="ignore"`` (or drop the
``ValidationError`` -> ``SettingsError`` wrap) -> a stale key is silently dropped
and the boot proceeds -> these RED.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml
from phantom.config.settings import SettingsError, load_settings

from .helpers.stack import PHANTOM_CONFIG_PATH, _deep_merge_dict, boot_stack

if TYPE_CHECKING:
    from collections.abc import Mapping

pytestmark = pytest.mark.e2e

# A field removed in Phase 1 (StorageCfg docstring: "removed default_tier,
# subsumed by body_store.mode"). A stale deployment YAML still carrying it is
# the exact § 4 defect this regression guards.
_STALE_REMOVED_KEY_OVERRIDE: dict[str, Any] = {"storage": {"default_tier": "ram"}}
# A wrong-typed value: max_in_flight must be an int, strict mode refuses a string.
_WRONG_TYPED_OVERRIDE: dict[str, Any] = {"saturation": {"max_in_flight": "not-an-int"}}


def _write_merged_config(tmp_path: Path, overrides: Mapping[str, Any]) -> Path:
    """Serialize the pinned e2e YAML + ``overrides`` to a temp file for load_settings.

    The YAML on disk is never touched; the overlay lands on an in-memory copy so
    the full ``load_settings`` validator path runs over a stale-config shape.
    """
    raw = yaml.safe_load(PHANTOM_CONFIG_PATH.read_text())
    assert isinstance(raw, dict)
    _deep_merge_dict(raw, overrides)
    merged = tmp_path / "stale-phantom-config.yml"
    merged.write_text(yaml.safe_dump(raw))
    return merged


async def test_stale_removed_key_fails_boot(tmp_path: Path) -> None:
    """A config carrying the removed ``storage.default_tier`` key fails boot loudly.

    Drives ``boot_stack`` (the same in-process boot the suite uses) with the stale
    overlay; ``extra="forbid"`` makes ``load_settings`` raise ``SettingsError``
    before any server becomes ready - the fail-fast-and-loud contract.
    """
    with pytest.raises(SettingsError) as excinfo:
        await boot_stack(tmp_path=tmp_path, config_overrides=_STALE_REMOVED_KEY_OVERRIDE)
    # The error names the validation failure (loud), not a silent default fallback.
    assert "validate" in str(excinfo.value).lower() or "extra" in str(excinfo.value).lower()


async def test_wrong_typed_value_fails_boot(tmp_path: Path) -> None:
    """A wrong-typed config value fails boot rather than being silently coerced."""
    with pytest.raises(SettingsError):
        await boot_stack(tmp_path=tmp_path, config_overrides=_WRONG_TYPED_OVERRIDE)


def test_load_settings_is_the_boot_validator(tmp_path: Path) -> None:
    """``load_settings`` (the production boot validator) rejects the stale key.

    Pins the regression to the function ``boot_stack`` calls AND that
    ``python -m phantom`` runs at startup, so a future refactor that moves the
    validation cannot silently drop the strict-config guarantee. A clean config
    loads; the stale one raises ``SettingsError``.
    """
    # The pinned YAML loads cleanly (no overlay).
    clean_path = _write_merged_config(tmp_path, {})
    load_settings(clean_path)  # must not raise

    stale_path = _write_merged_config(tmp_path, _STALE_REMOVED_KEY_OVERRIDE)
    with pytest.raises(SettingsError):
        load_settings(stale_path)
