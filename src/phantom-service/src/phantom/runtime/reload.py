"""Hot-reload engine - the cohesive YAML-reload + SIGHUP cluster.

One caller surface, two triggers: the SIGHUP handler installed by
:func:`phantom.app.create_app`'s ``lifespan`` and the
``POST /v1/admin/reload`` admin route. Both go through
:func:`apply_reload`, which re-reads the YAML with the host probe ON
(R7-1: probe-fillable knobs omitted from YAML re-resolve from current
machine facts exactly as at boot, so the smart-defaults deployment
posture survives reload; operator-pinned YAML always wins), builds
fresh per-instance snapshots, swaps them under the
:class:`SettingsHolder` lock, rebuilds each instance's retry strategy,
surfaces AD-mint and per-instance route-block config drift, and pushes
new saturation caps into every gate. Each :class:`InstanceContext.cfg`
is left exactly as booted: the per-instance route block is frozen so
every reader resolves one snapshot (D1/F5). Worker coroutines read the live
snapshot on each tick, so the change propagates without restarting the
pool. Validation (including probe-fill resolution) completes inside
``Settings.reload_from_yaml`` BEFORE any swap, so a rejected reload
leaves the running config untouched per ADR-013.

The SIGHUP path (:func:`sighup_reload`, scheduled by
:func:`make_sighup_handler`) swallows parse/validation failures - a bad
YAML must not crash the process; the admin route instead returns a 422
envelope. :class:`suppress_signal_handler_errors` guards the lifespan's
SIGHUP teardown.

Provenance: extracted from ``app.py`` during the 2026-05-29 refinement
(R-7) so ``app.py`` stays the FastAPI/HTTP + composition surface and the
reload logic lives in one cohesive module.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import yaml  # type: ignore[import-untyped]  # types-PyYAML not in workspace dev deps
from pydantic import ValidationError

from phantom.config.settings import InstanceCfg, Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.runtime.startup_checks import ConfigInvariantError, check_retention_floor
from phantom.strategies import build_retry_strategy

if TYPE_CHECKING:
    from phantom.storage.interface import TokenCache

logger = logging.getLogger(__name__)

# Exception group intentionally bound to a module-level constant. ruff 0.15.x
# strips the parentheses from a parenthesized ``except (A, B):`` under Python
# 3.14 (producing the bare 3.14-only form), so the constant binding is the
# stable, portable, consistent form across interpreters.
RELOAD_FAILURE_ERRORS: Final[tuple[type[BaseException], ...]] = (
    yaml.YAMLError,
    ValidationError,
    ConfigInvariantError,
    OSError,
    UnicodeDecodeError,
)
"""Every reload failure the contract maps to reject-and-keep-previous.

One shared definition for BOTH consumer postures so they cannot drift
(R8-2): the SIGHUP path logs and keeps the previous snapshot; the admin
route answers the 422 envelope. Beyond the parse pair (``yaml.YAMLError``
+ ``ValidationError``), the file READ itself can fail: ``OSError``
(``FileNotFoundError`` when the YAML vanished mid-edit, permission
flips) and ``UnicodeDecodeError`` (a non-UTF-8 byte from a hand edit;
a ``ValueError`` subclass, NOT an ``OSError``). ``ConfigInvariantError``
(F14) covers a CROSS-FIELD config invariant pydantic cannot express,
re-checked between the load and the swap; the retention floor is its one
current member, and it is listed explicitly because it subclasses
``ValueError`` rather than ``ValidationError`` and would otherwise
escape both consumers. All of them strike before any snapshot swap, so
the running config is unaffected.
"""


async def _reload_minter(
    ctx: InstanceContext,
    new_cfg: InstanceCfg,
    token_cache: TokenCache,
) -> None:
    """Surface an AD-mint config change as a WARNING (operator-restart-required).

    H6 audit closure (Phase 2 § 3.2.5) moved AdMinter under the
    lifespan's :class:`asyncio.TaskGroup`, so the run-loop is owned by
    the supervising TaskGroup and cannot be hot-swapped at reload time
    (the group is already running). Changing AD-mint configuration
    (including the None ↔ configured transition) now requires a
    process restart; the YAML reload logs a WARNING surfacing the
    mismatch so the operator knows the new config has not taken effect.

    Every ``ad_mint`` knob requires the restart, refresh timings
    included: the minter reads its boot-time :class:`AdMintConfig` on
    each cycle and nothing projects the refresh-timing fields into the
    hot-reload path (R5-2 doc reconciliation; ADR-013 records the
    restart-required posture).

    Args:
        ctx: The instance context whose minter slot may have drifted.
        new_cfg: The freshly-reloaded :class:`InstanceCfg`.
        token_cache: Token cache (unused - kept for signature stability;
            the lifespan TaskGroup owns minter wiring).
    """
    del token_cache  # No longer used - see docstring.
    if ctx.cfg.ad_mint == new_cfg.ad_mint:
        return
    logger.warning(
        "ad_mint config changed for instance %s; restart required for new minter "
        "(reload-time swap is not supported - Phase 2 H6 moved supervision under "
        "the lifespan TaskGroup)",
        ctx.cfg.id,
    )


def _warn_on_restart_required_drift(live_cfg: InstanceCfg, new_cfg: InstanceCfg) -> None:
    """Warn when a reload changes per-instance config that cannot be applied.

    D1/ADR-013: ``routes``, ``host_prefixes`` and ``data_dir`` are frozen at
    boot. The boot :class:`InstanceCfg` is the ONE snapshot every reader
    resolves against (admission, the dispatcher, both kickers, the executor,
    and the admin quarantine paths), and nothing rebinds it, so a reloaded
    block in this set cannot take effect. This arm is what tells the operator
    that, and it mirrors :func:`_reload_minter`'s ``ad_mint`` posture: warn,
    change nothing, keep running.

    Field NAMES are logged, never their values. A route table is unbounded
    operator input and a step URL pattern is not something a WARNING should
    splice into the log stream.

    Args:
        live_cfg: The instance's frozen boot config.
        new_cfg: The freshly-loaded block for the same instance id.
    """
    drifted: list[str] = []
    if live_cfg.routes != new_cfg.routes:
        drifted.append("routes")
    if live_cfg.host_prefixes != new_cfg.host_prefixes:
        drifted.append("host_prefixes")
    if live_cfg.data_dir != new_cfg.data_dir:
        drifted.append("data_dir")
    if not drifted:
        return
    logger.warning(
        "Reload changed restart-required per-instance config (%s) for instance "
        "%s; the change has NOT been applied and cannot take effect until the "
        "process restarts (ADR-013: routes, host_prefixes and data_dir are "
        "frozen at boot so every reader resolves one snapshot)",
        ", ".join(drifted),
        live_cfg.id,
    )


def make_sighup_handler(
    holder: SettingsHolder,
    settings_path: Path,
    instances: list[InstanceContext],
) -> Callable[[], None]:
    """Build a signal-handler callable that schedules an async reload.

    ``add_signal_handler`` invokes its callback synchronously on the
    event loop thread; the callback cannot ``await``. So the handler
    schedules :func:`apply_reload` as a task on the running loop and
    returns immediately. The handler keeps a strong reference to the
    in-flight reload task so the asyncio runtime cannot GC it mid-run.
    Failures inside the reload are logged but do not crash the process.
    """
    # Strong-reference store keyed by id(task). The done-callback removes
    # finished tasks so the dict stays small (one entry per concurrent
    # in-flight reload, normally zero).
    in_flight: dict[int, asyncio.Task[None]] = {}

    def _handler() -> None:
        """Schedule one SIGHUP reload task and track it until it finishes."""
        loop = asyncio.get_running_loop()
        task = loop.create_task(_sighup_reload(holder, settings_path, instances))
        in_flight[id(task)] = task
        task.add_done_callback(lambda t: in_flight.pop(id(t), None))

    return _handler


async def _sighup_reload(
    holder: SettingsHolder,
    settings_path: Path,
    instances: list[InstanceContext],
) -> None:
    """Run :func:`apply_reload` from the SIGHUP context, swallowing failures.

    Unlike the admin endpoint (which returns a 422 envelope on failure),
    the SIGHUP path has no return surface, so nothing in the shared
    failure set may crash the process: a parse error, a validation error,
    a read failure, or a config-invariant error (the retention floor,
    F14). Log and keep the previous snapshot. The set is the one shared
    ``RELOAD_FAILURE_ERRORS``, so this posture and the route's cannot
    drift.
    """
    try:
        reloaded = await apply_reload(holder, settings_path, instances)
    except RELOAD_FAILURE_ERRORS:
        logger.exception(
            "SIGHUP settings reload failed; keeping previous snapshot",
        )
        return
    logger.info("SIGHUP settings reload installed %d instance(s)", len(reloaded))


class suppress_signal_handler_errors:  # noqa: N801  context-manager naming
    """Suppress benign errors raised by ``loop.remove_signal_handler``.

    The handler may have failed to install (Windows / non-main-thread)
    or the loop may already be closed at teardown. Both paths are
    fine - we don't want lifespan shutdown to mask a real worker error
    with a signal-handler cleanup exception.
    """

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return exc_type is not None and issubclass(
            exc_type, (NotImplementedError, ValueError, RuntimeError)
        )


async def apply_reload(
    holder: SettingsHolder,
    settings_path: Path,
    instances: list[InstanceContext],
) -> list[str]:
    """Reload settings from YAML and swap live state atomically.

    Shared by the SIGHUP handler and ``POST /v1/admin/reload``. Loads a
    fresh :class:`Settings` from ``settings_path`` with the host probe
    ON (R7-1: a probe-reliant YAML, the documented smart-defaults
    posture, previously reloaded with ``skip_probe=True`` and swapped
    ``None`` holes into the live snapshots before validation could
    refuse, half-applying the reload and silently disabling RAM-ceiling
    enforcement), builds new per-instance snapshots, swaps the snapshot
    map under the holder's lock, then propagates the settings change to
    live state that workers don't read from snapshots on each tick:

    * Per-instance :attr:`InstanceContext.cfg` is NOT touched. The route
      block (``routes``, ``host_prefixes``, ``data_dir``) is frozen at
      boot so every reader resolves one snapshot (D1/F5); drift in that
      set is surfaced as a restart-required WARNING via
      :func:`_warn_on_restart_required_drift` and applied nowhere.
    * AD-mint config drift is surfaced as a restart-required WARNING
      via :func:`_reload_minter` (H6: the lifespan TaskGroup owns the
      minter; no reload-time swap).
    * :attr:`InstanceContext.retry_strategy` is rebuilt from the new
      ``retry.default_strategy`` block so reloaded retry parameters
      apply to subsequent scheduling decisions (R5-2; ADR-013).
    * :class:`SaturationGate` caps are pushed via
      :meth:`SaturationGate.update_caps`.

    Args:
        holder: The live :class:`SettingsHolder`.
        settings_path: Path to the YAML config file.
        instances: The live list of :class:`InstanceContext`. Order is
            preserved across reload; instances added or removed by the
            YAML are NOT handled here (the operator must restart the
            process for topology changes).

    Returns:
        Sorted list of instance ids whose snapshots were installed.

    Raises:
        yaml.YAMLError: If the YAML payload is unparseable.
        pydantic.ValidationError: If the config fails validation.
        ConfigInvariantError: If the freshly loaded config violates a
            cross-field invariant pydantic cannot express. Today that is
            the retention floor, re-checked here because the reaper
            live-reads retention per sweep (F14). Like the other two,
            it strikes before any snapshot swap, so the running config
            is unaffected; every member of ``RELOAD_FAILURE_ERRORS``
            shares that guarantee.
    """
    # Probe ON (the classmethod's default): omitted probe-fillable knobs
    # re-resolve from current machine facts, and Pydantic validation
    # completes BEFORE the swap below, so a refused reload never touches
    # the live snapshots (R7-1; ADR-013 atomicity).
    new_settings = Settings.reload_from_yaml(settings_path)
    # The retention floor is a CROSS-FIELD invariant pydantic cannot express,
    # so it rides here rather than in RetentionCfg: bodies must never outlive
    # their row. The reaper reads retention from the live snapshot per sweep,
    # so an inverted window installed by a reload takes effect on the next
    # sweep and strands RAM bodies that RamBodyStore.list_orphans can never
    # reclaim (F14). Boot runs the identical check (app.py); this is the
    # second door, not a second rule.
    check_retention_floor(new_settings)
    # R9-2: body_store.mode is restart-required (the store wiring is
    # composition-time per ADR-025/ADR-013), but _build_snapshot
    # projects the FULL BodyStoreCfg into the live snapshots and
    # admission reads the mode per request. Unguarded, a reloaded mode
    # mints rows whose body_location contradicts the wired stores
    # (boot-hybrid + reload-all_disk births 'file' rows whose bytes
    # live in RAM: invariant #1 broken at insert, the rows quarantined
    # corrupted on the next restart). Preserve the LIVE mode in the
    # reloaded config and WARN, mirroring the ad_mint and topology
    # restart-required postures.
    if instances:
        live_mode = holder.snapshot_for(instances[0].cfg.id).body_store.mode
        if new_settings.storage.body_store.mode != live_mode:
            logger.warning(
                "Reload changed body_store.mode from %s to %s; the deployment "
                "mode is restart-required (ADR-013) - keeping %s live",
                live_mode,
                new_settings.storage.body_store.mode,
                live_mode,
            )
            preserved_body_store = new_settings.storage.body_store.model_copy(
                update={"mode": live_mode}
            )
            preserved_storage = new_settings.storage.model_copy(
                update={"body_store": preserved_body_store}
            )
            new_settings = new_settings.model_copy(update={"storage": preserved_storage})
    snapshots = {cfg.id: _build_snapshot(new_settings, cfg) for cfg in new_settings.instances}
    # R9-7: an instance the new YAML ADDS does not exist in this process
    # (topology is restart-required, same as the omission leg). Warn
    # like the omission leg does, install no dead holder entry, and do
    # not report the id as reloaded - a 200 naming it would be positive
    # confirmation of an instance that is not running.
    live_ids = {ctx.cfg.id for ctx in instances}
    for added_id in sorted(set(snapshots) - live_ids):
        logger.warning(
            "Reload added instance %s; topology changes require a process "
            "restart - the instance is NOT running and was not installed",
            added_id,
        )
        del snapshots[added_id]
    # R8-1: a live instance the new YAML omits keeps its previous
    # snapshot. The holder must never lose a running instance's entry:
    # every per-tick live read (watcher cadence + ceiling, sender linger
    # + retention, reaper interval, admission, observability) resolves
    # holder.snapshot_for(cfg.id), and an evicted entry turns the next
    # read into a KeyError that escapes the worker loop and tears the
    # whole process down through the TaskGroup. ADR-013's posture for
    # topology drift is warn-and-keep-running until restart; the warning
    # below logs per omitted instance.
    for ctx in instances:
        if ctx.cfg.id not in snapshots:
            snapshots[ctx.cfg.id] = holder.snapshot_for(ctx.cfg.id)
    await holder.replace(snapshots)
    # Per-instance live-state propagation. Build a lookup by id so
    # reload-order independence is explicit (the YAML may reorder
    # instances; we still match each context to its block by id).
    new_by_id = {cfg.id: cfg for cfg in new_settings.instances}
    for ctx in instances:
        new_cfg = new_by_id.get(ctx.cfg.id)
        if new_cfg is None:
            # Operator removed this instance from the YAML - keep
            # the live instance running with its previous settings (a
            # topology change requires a process restart).
            logger.warning("Reload omitted instance %s; keeping previous config", ctx.cfg.id)
            continue
        await _reload_minter(ctx, new_cfg, ctx.token_cache)
        _warn_on_restart_required_drift(ctx.cfg, new_cfg)
        # Rebuild the retry strategy from the freshly-loaded block so
        # reloaded retry parameters reach the sender's next scheduling
        # decision (R5-2). Mirrors the saturation cap push below: a
        # reload-time push for state the sender does not re-derive
        # from the snapshot per decision.
        ctx.retry_strategy = build_retry_strategy(new_settings.retry.default_strategy)
        snapshot = holder.snapshot_for(ctx.cfg.id)
        await ctx.saturation.update_caps(snapshot.saturation)
    return sorted(snapshots.keys())
