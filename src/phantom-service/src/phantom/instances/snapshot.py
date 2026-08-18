"""Per-instance snapshot of hot-reloadable settings.

Workers consume the current snapshot via
:meth:`InstanceContext.current_settings` on each tick rather than
capturing fields at __init__. This is what makes hot reload safe
(SIGHUP -> atomic swap of the snapshot reference).

Static-at-startup fields (worker count, the per-instance route block,
the instance list) do NOT live in the snapshot; they remain on
InstanceContext directly and are frozen at boot (D1/ADR-013). Every
per-instance knob that DOES reload rides this snapshot, ``admin_lookup``
included. Reload-time pushes that do not flow through the snapshot (the
retry-strategy rebuild, the saturation-gate cap push) live in
:func:`phantom.runtime.reload.apply_reload`; F5 deleted the third one,
the ``ctx.cfg`` repoint.
"""

from __future__ import annotations

from dataclasses import dataclass

from phantom.config.settings import (
    AdminLookupCfg,
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    SaturationCfg,
    Settings,
)


@dataclass(frozen=True)
class InstanceSettingsSnapshot:
    """A frozen view of one instance's hot-reloadable operational config.

    Every field is a settings sub-block that operators may change at
    runtime via SIGHUP or POST /v1/admin/reload, with a live read site
    consuming it. Workers read from this snapshot on each tick; the
    composition root atomically swaps the referenced snapshot under an
    asyncio.Lock when a reload fires.

    Phase 1: removed ``default_tier`` (subsumed by ``body_store.mode``,
    the BodyStore deployment mode is selected at composition time, not
    per-upload). ``body_store`` (the full BodyStoreCfg) is projected
    so workers can read the mode + linger + RAM-ceiling knobs through
    the same hot-reload-friendly snapshot mechanism.

    R5-2: removed the reader-less ``retry`` and AD-mint refresh-timing
    fields. Retry parameters reload via the
    :func:`phantom.runtime.reload.apply_reload` retry-strategy rebuild;
    every ``ad_mint`` knob is restart-required (the minter reads its
    boot-time :class:`AdMintConfig` per cycle; ADR-013).

    F5/D1: ``admin_lookup`` joined the snapshot when the ``ctx.cfg``
    repoint was deleted. It and ``capture_reexecution`` are the two
    per-instance knobs that stay reloadable; the rest of
    :class:`InstanceCfg` (``routes``, ``host_prefixes``, ``data_dir``,
    ``ad_mint``, ``id``) is restart-required and is read straight off the
    frozen boot config.
    """

    persist_trigger: PersistTriggerCfg
    body_store: BodyStoreCfg
    retention: RetentionCfg
    compression: CompressionCfg
    saturation: SaturationCfg
    capture_reexecution: bool
    admin_lookup: AdminLookupCfg | None = None
    """The by-captured-id lookup binding, or ``None`` when unconfigured.

    Defaulted because the config field is itself optional and seven test
    construction sites do not care about it; the one production builder
    always passes it explicitly.
    """


def _build_snapshot(settings: Settings, cfg: InstanceCfg) -> InstanceSettingsSnapshot:
    """Construct an InstanceSettingsSnapshot from the live Settings.

    Maps the hot-reloadable subset of Settings/InstanceCfg into the
    snapshot. Used by the composition root at startup and by the
    admin-reload handler.

    Args:
        settings: The validated top-level Settings (post Family 6's
            probe-driven defaults fill).
        cfg: The InstanceCfg for this specific instance.

    Returns:
        A frozen InstanceSettingsSnapshot.

    Share-by-reference semantics: every instance's snapshot references
    the SAME ``settings.storage.persist_trigger``, ``settings.storage.body_store``,
    ``settings.retention``, ``settings.storage.compression``, and
    ``settings.saturation`` instances (not deep-copied). These are
    immutable Pydantic models, so sharing by reference is safe.
    Per-instance variation comes from ``cfg.capture_reexecution`` and
    ``cfg.admin_lookup``, the two reloadable knobs on
    :class:`InstanceCfg`.
    """
    return InstanceSettingsSnapshot(
        persist_trigger=settings.storage.persist_trigger,
        body_store=settings.storage.body_store,
        retention=settings.retention,
        compression=settings.storage.compression,
        saturation=settings.saturation,
        capture_reexecution=cfg.capture_reexecution,
        admin_lookup=cfg.admin_lookup,
    )


__all__ = ["InstanceSettingsSnapshot", "_build_snapshot"]
