"""FastAPI factory + composition root + lifespan manager.

The factory wires every Protocol implementation into per-instance
:class:`InstanceContext` bundles, mounts the routers, and runs the
recovery sweep before starting the worker pool.

Hot reload: when ``create_app`` is given a ``settings_path``, the
lifespan installs a SIGHUP handler from the hot-reload engine
(:mod:`phantom.runtime.reload`). Both that handler and the
``POST /v1/admin/reload`` endpoint call
:func:`phantom.runtime.reload.apply_reload`, which parses the YAML,
builds fresh per-instance snapshots, swaps them in the
:class:`SettingsHolder`, restarts AD-mint loops whose config changed,
and pushes new saturation caps into every gate. Worker coroutines read
the live snapshot on each tick so the change propagates without
restarting the pool.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Final, Protocol, assert_never

import aiosqlite
from fastapi import FastAPI
from pydantic import ValidationError

from phantom import __version__
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.probe import probe_machine
from phantom.config.settings import (
    InstanceCfg,
    Settings,
    SigV4CredentialCfg,
    host_is_loopback,
)
from phantom.instances.context import InstanceContext, instance_storage_paths
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import InstanceSettingsSnapshot, _build_snapshot
from phantom.models.admin import ResolvedDefaultsSummary
from phantom.models.credential import (
    HostCredKey,
    ProfileRefCred,
    SigV4StaticCreds,
)
from phantom.observability import configure_logging
from phantom.observability.metrics import MetricsRegistry
from phantom.refresh.ad_client_credentials import AdMinter
from phantom.routes import admin as admin_routes
from phantom.routes import catch_all as catch_all_routes
from phantom.routes import health as health_routes
from phantom.routes import send as send_routes
from phantom.routing import host_key_for, resolve_route
from phantom.runtime.lock_retry import BootOpenLockError, retry_on_transient_lock
from phantom.runtime.reload import (
    make_sighup_handler,
    suppress_signal_handler_errors,
)
from phantom.runtime.startup_checks import (
    DB_QUARANTINE_COUNTER_DESCRIPTION,
    DB_QUARANTINE_COUNTER_NAME,
    EXPECTED_UPLOADS_COLUMNS,
    MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION,
    MODE_SWITCH_BACKUP_COUNTER_NAME,
    SCHEMA_DISCARD_COUNTER_DESCRIPTION,
    SCHEMA_DISCARD_COUNTER_NAME,
    DegradedInstance,
    DegradeReason,
    apply_umask,
    build_body_store,
    check_body_store_mode,
    check_instance_isolation,
    check_retention_floor,
    degrade_action_hint,
    run_integrity_gate,
    run_schema_gate,
)
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.integrity import (
    isolate_db_file,
    quarantine,
    reconcile_interrupted_backup_move,
)
from phantom.storage.interface import BodyStore
from phantom.storage.sqlite_store import SCHEMA_VERSION
from phantom.strategies import build_retry_strategy
from phantom.transport.httpx_client import HttpxUpstreamClient
from phantom.workers.body_orphan_janitor import BodyOrphanJanitor
from phantom.workers.cold_backup import ColdBackupScheduler
from phantom.workers.disk_pressure import DiskPressureProbe
from phantom.workers.invariant_audit import InvariantAuditor
from phantom.workers.kicker import (
    AWS_SIGV4_FLAVOUR,
    PHANTOM_BEARER_FLAVOUR,
    Kicker,
)
from phantom.workers.persist_controller import PersistController
from phantom.workers.ram_pressure import RamPressureWatcher
from phantom.workers.reaper import Reaper
from phantom.workers.recovery import reconcile_saturation, run_recovery
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender
from phantom.workers.vacuum import VacuumScheduler

logger = logging.getLogger(__name__)


# Minimum and maximum bounds on the asyncio default thread pool size.
# The default CPython size is ``min(32, (cpu+4))``; ingress CPU-bound
# fan-out (sha256 + codec + fsync per body_ref) saturates ~12 threads
# under 16-way concurrent admission and inflates the synchronous-return
# latency. We widen the floor and cap at 64 to keep the OS-thread cost
# bounded.
_DEFAULT_EXECUTOR_MIN_WORKERS: int = 32
_DEFAULT_EXECUTOR_MAX_WORKERS: int = 64
# Approximate per-admission thread-pool calls. Used to size the pool
# against the saturation in-flight cap (and instance count for
# multi-instance deployments).
_THREADS_PER_INFLIGHT_REQUEST: int = 4

# Exception group intentionally bound to a module-level constant. ruff 0.15.x
# strips the parentheses from a parenthesized ``except (A, B):`` under Python
# 3.14 (producing the bare 3.14-only form), so the constant binding is the
# stable, portable, consistent form across interpreters.
_SIGHUP_INSTALL_ERRORS: Final[tuple[type[BaseException], ...]] = (
    NotImplementedError,
    ValueError,
)
"""``add_signal_handler`` failures (Windows / non-main-thread) — log and skip."""


def _default_executor_worker_count(settings: Settings) -> int:
    """Compute the asyncio default-executor worker count from settings.

    Pure function — the side-effecting :func:`_resize_default_executor`
    wraps this for testability. Sizes against
    ``saturation.max_in_flight`` x number of instances x
    :data:`_THREADS_PER_INFLIGHT_REQUEST`, then clamps to
    [``_DEFAULT_EXECUTOR_MIN_WORKERS``, ``_DEFAULT_EXECUTOR_MAX_WORKERS``].

    The settings invariants enforced by the Settings model validator
    guarantee ``settings.saturation.max_in_flight`` and
    ``settings.instances`` are non-falsy populated values by the time
    this is called from the lifespan, so no exception handling is
    needed at this layer — any access failure here would be a settings
    invariants bug, not a normal operating condition.
    """
    inflight_cap = settings.saturation.max_in_flight or _DEFAULT_EXECUTOR_MIN_WORKERS
    instance_count = max(1, len(settings.instances))
    desired = inflight_cap * instance_count * _THREADS_PER_INFLIGHT_REQUEST
    return max(_DEFAULT_EXECUTOR_MIN_WORKERS, min(_DEFAULT_EXECUTOR_MAX_WORKERS, desired))


def _resize_default_executor(settings: Settings) -> None:
    """Re-bind the running loop's default executor to a wider pool.

    Per-admission CPU-bound fan-out (sha256 + codec.encode + fsync per
    body_ref) saturates the default ``min(32, cpu+4)`` thread pool
    under concurrent ingress; this widens it. The worker-count
    calculation lives in :func:`_default_executor_worker_count` and is
    unit-tested separately so the formula and clamps stay legible.
    """
    workers = _default_executor_worker_count(settings)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=workers, thread_name_prefix="phantom-pool"),
    )
    logger.info("Sized asyncio default executor to %d workers", workers)


# A boot DB open that cannot be ridden out OR isolated (§ 4D.1). The retry
# already raises a TRANSIENT-lock holder past budget as BootOpenLockError, and
# a permission / I/O / unopenable-WAL fault surfaces as aiosqlite.Error /
# OSError; all three are the UNRECOVERABLE-open class the guard isolates.
_UNRECOVERABLE_OPEN_ERRORS: Final[tuple[type[BaseException], ...]] = (
    aiosqlite.Error,
    OSError,
    BootOpenLockError,
)

# Prefix for the throw-away probe file the substrate-writability check creates
# and immediately unlinks; named so a leaked probe (a crash mid-check) is
# obviously transient scratch in a directory listing.
_SUBSTRATE_PROBE_PREFIX: Final[str] = ".phantom-substrate-probe-"


def _data_root_is_unwritable(data_root: Path) -> bool:
    """Return True iff ``data_root`` cannot be written to right now.

    The authoritative signal for the unwritable-substrate degraded boot (§ 4D.2),
    classified by PROBING the directory rather than by the Python exception type a
    failed DB open happens to raise. A read-only ``data_dir`` makes SQLite raise
    ``sqlite3.OperationalError("unable to open database file")`` (an
    ``aiosqlite.Error``, NOT an ``OSError``), so keying the degrade decision off
    ``OSError`` alone misses the exact real-world fault § 4D exists to catch. An
    actual create-then-unlink is also more truthful than ``os.access(..., W_OK)``,
    which can disagree with reality under root, ACLs, or network filesystems.

    The probe creates a uniquely named temp file in ``data_root`` (so it never
    collides with a real artifact) and unlinks it. Success -> the directory is
    writable (the open failed for some OTHER reason -> isolate, do not degrade);
    an ``OSError`` (``PermissionError``, ``ENOSPC``, a missing directory) ->
    unwritable -> degrade. The probe is best-effort and never raises: any failure
    to even attempt it is itself treated as "unwritable".

    Args:
        data_root: The per-instance data directory whose live ``uploads.db`` /
            ``token_cache.db`` could not be opened or recreated.

    Returns:
        ``True`` if a probe write into ``data_root`` fails (the substrate is
        unwritable -> degrade the instance); ``False`` if the probe write
        succeeds (the substrate is writable -> the open failed for another
        reason, which the caller isolates).
    """
    try:
        fd, probe_path = tempfile.mkstemp(prefix=_SUBSTRATE_PROBE_PREFIX, dir=str(data_root))
    except OSError:
        # Could not even create a probe file (read-only / full / missing dir):
        # the substrate is unwritable.
        return True
    # The create succeeded -> the directory is writable. Clean up the probe;
    # an unlink failure here does not change the writable verdict.
    os.close(fd)
    try:
        os.unlink(probe_path)
    except OSError:
        logger.debug(
            "Could not unlink substrate-probe file %s after a successful write probe",
            probe_path,
            exc_info=True,
        )
    return False


def _ensure_data_root_writable(data_root: Path) -> None:
    """Create the per-instance ``data_root`` directory, degrading if the substrate is bad.

    The § 4D contract is "always boot; degrade loudly on an unwritable substrate,
    never crash-loop." That contract used to span only the DB-OPEN stage, but the
    per-instance boot begins EARLIER, at ``data_root.mkdir``, and a substrate fault
    there (a FILE sitting where the instance ``data_root`` directory must be, a
    read-only or full parent) makes ``mkdir`` raise an ``OSError`` (``FileExistsError``
    / ``NotADirectoryError`` / ``PermissionError`` / ``ENOSPC``). Left unguarded that
    raw error escapes the instance-build loop and CRASH-LOOPS the whole process, the
    exact 4D failure the degrade path exists to prevent (finding M4-A). This helper
    extends the same probe-based degrade decision the DB-open guard uses to cover the
    directory-prep stage, so a bad ``data_root`` degrades ONLY that instance.

    The probe (:func:`_data_root_is_unwritable`), not the ``mkdir`` exception type, is
    the discriminator: a file-at-``data_root`` makes a ``tempfile.mkstemp`` into that
    path raise ``NotADirectoryError`` (an ``OSError``), so the probe classifies it
    unwritable; a read-only / full parent classifies the same way. Only when the probe
    confirms the substrate is genuinely usable (the rare case of a transient ``mkdir``
    failure that nonetheless left a writable directory) is the original error re-raised,
    so a real fault is never silently degraded.

    Args:
        data_root: The per-instance data directory to create.

    Raises:
        _StorageSubstrateUnwritableError: ``data_root`` could not be made into a
            writable directory because the underlying substrate is unwritable; the
            caller degrades the instance (returns a typed
            :class:`DegradedInstance`, builds no context).
    """
    try:
        data_root.mkdir(parents=True, exist_ok=True)
    except OSError as mkdir_error:
        # A non-directory at the path, or an unwritable parent: PROBE to classify.
        # The probe is the signal, not the exception type, but if the probe somehow
        # disagrees (the directory IS usable after all), re-raise the real fault so a
        # genuine, non-substrate failure is never silently turned into a degrade.
        if not _data_root_is_unwritable(data_root):
            raise
        detail = (
            f"the per-instance data directory {data_root} could not be created "
            f"(the storage substrate is unwritable). Directory-prep error: {mkdir_error!r}."
        )
        raise _StorageSubstrateUnwritableError(detail) from mkdir_error
    # mkdir succeeded (or the directory already existed). A successful mkdir on a
    # writable substrate is the common path; the probe is skipped here because the
    # DB-open guard further down already classifies any later substrate fault.


class _Startable(Protocol):
    """A store/cache that opens its connection on ``start()`` / closes on ``stop()``."""

    async def start(self) -> None:
        """Open the connection (and, for the upload store, apply schema)."""
        ...  # pragma: no cover — Protocol stub.

    async def stop(self) -> None:
        """Close the connection (a no-op if it was never opened)."""
        ...  # pragma: no cover — Protocol stub.


async def _started[StartableT: _Startable](store: StartableT) -> StartableT:
    """``start()`` a freshly built store/cache and return the STARTED object.

    The boot-open guard's ``open_fresh`` factories return
    ``_started(SqliteUploadStore(...))`` / ``_started(SqliteTokenCache(...))``,
    so each retry (and the post-isolate reopen) issues a brand-new connection;
    ``start()`` itself returns ``None``, so this adapter returns the object the
    guard hands back as the opened store/cache.

    If ``start()`` raises (a transient lock, a permission / I/O fault), the
    half-opened connection is CLOSED before the error propagates: ``start()``
    assigns ``self._conn`` on its first line, so a later-pragma failure would
    otherwise leak an open fd on a file the guard is about to isolate (rename
    aside) or reopen. Closing keeps each abandoned attempt clean across the
    bounded retry and the post-isolate reopen; the original error is re-raised
    so the guard's classification (transient-lock vs unrecoverable) is unchanged.
    """
    try:
        await store.start()
    except BaseException:
        # Best-effort close of the half-opened connection; never mask the
        # original open error with a teardown error.
        try:
            await store.stop()
        except Exception:
            logger.debug(
                "Ignoring error while closing a store after a failed open",
                exc_info=True,
            )
        raise
    return store


class _StorageSubstrateUnwritableError(RuntimeError):
    """The instance's data_dir is unwritable, so it cannot buffer durably (M-1).

    Raised by :func:`_open_db_with_retry_then_isolate` when the isolate-or-
    recreate of an unrecoverable open ITSELF fails with an ``OSError`` — the
    rename aside or the fresh-DB open hits a read-only / full ``data_dir``. This
    is the one physics boundary "always boot durably" cannot honor: no writable
    disk means no durable buffering. :func:`_build_instance_context` catches it
    and returns a typed :class:`DegradedInstance`
    (``reason=SUBSTRATE_UNWRITABLE``) so the instance boots DEGRADED (no
    context, no dispatcher entry) rather than crash-looping. The read-side
    surfacing (``/ready`` + ``/health`` + the ``POST /send`` 500 guard) reads
    the typed degraded set.

    Attributes:
        detail: Operator-facing fault description (the failing op + the original
            error), carried into :attr:`DegradedInstance.detail`.
    """

    def __init__(self, detail: str) -> None:
        """Store ``detail`` (the degrade detail) and pass it to ``RuntimeError``."""
        super().__init__(detail)
        self.detail = detail


class ConfigCredentialError(RuntimeError):
    """A ``sigv4_credentials`` config entry could not be materialized at boot.

    Raised by :func:`_materialize_config_credentials` when a NAMED environment
    variable a ``sigv4_static`` entry resolves (``access_key_id_env`` /
    ``secret_access_key_env`` / ``session_token_env``) is absent or empty. This
    is fail-fast by design (GLOBAL §1.2(a) B1 / plan Phase 2 TASK 2.4b): a
    config-declared credential whose backing secret env var is missing is an
    operator misconfiguration that must be fixed, not silently skipped — a
    silent skip would strand every ``aws_sigv4`` forward on that host with no
    credential and no signal. Unlike :class:`_StorageSubstrateUnwritableError`
    (a physics boundary that DEGRADES the one instance), this is a config error
    that PROPAGATES out of :func:`_build_instance_context` and crashes boot
    loudly, mirroring ``ad_mint``'s posture that its secret env var must exist.
    """


def _resolve_required_env(var_name: str, *, dest_host: str, field: str) -> str:
    """Resolve a named env var to a non-empty literal or fail fast.

    The B1 rule's boot-time resolution: a ``sigv4_credentials`` entry holds the
    NAME of the env var, never the secret; here (and ONLY here, at boot) the
    name becomes the literal value the store will hold.

    Args:
        var_name: The environment-variable NAME taken from the config entry.
        dest_host: The entry's destination host (for the error message).
        field: The config field the name came from (for the error message).

    Returns:
        The resolved non-empty environment-variable value.

    Raises:
        ConfigCredentialError: When ``var_name`` is absent from ``os.environ``
            or resolves to an empty string.
    """
    value = os.environ.get(var_name)
    if not value:
        raise ConfigCredentialError(
            f"sigv4_credentials entry for dest_host={dest_host!r}: the env var "
            f"{var_name!r} named by {field!r} is "
            f"{'unset' if value is None else 'empty'}. Set it before boot "
            f"(the config route names the env var; the secret literal never "
            f"lives in config)."
        )
    return value


def _config_credential_to_internal(
    cfg: SigV4CredentialCfg,
) -> SigV4StaticCreds | ProfileRefCred:
    """Resolve one config entry's env-var NAMES to a RESOLVED-value credential.

    The config-route analogue of
    :func:`phantom.models.credential.credential_body_to_internal` (which maps
    the admin-push *wire* body, whose values are already resolved literals). The
    forced difference: the config arm carries env-var NAMES, so the
    ``sigv4_static`` arm reads ``os.environ`` here. A ``profile_ref`` arm holds
    no secret and reads no env var (botocore resolves it at sign time). The
    ``sigv4_static`` arm's required fields are guaranteed present by
    :meth:`SigV4CredentialCfg._check_arm_fields`, so the ``assert`` narrows the
    optionals for the type checker rather than guarding a real ``None``.

    Args:
        cfg: One validated :class:`SigV4CredentialCfg` entry.

    Returns:
        The internal frozen credential (resolved literals for the static arm).

    Raises:
        ConfigCredentialError: A named env var (static arm) is unset or empty.
    """
    if cfg.kind == "profile_ref":
        return ProfileRefCred(service=cfg.service, profile=cfg.profile, region=cfg.region)

    # sigv4_static — the validator guarantees these are set.
    assert cfg.access_key_id_env is not None
    assert cfg.secret_access_key_env is not None
    assert cfg.region is not None
    session_token: str | None = None
    if cfg.session_token_env is not None:
        session_token = _resolve_required_env(
            cfg.session_token_env, dest_host=cfg.dest_host, field="session_token_env"
        )
    return SigV4StaticCreds(
        access_key_id=_resolve_required_env(
            cfg.access_key_id_env, dest_host=cfg.dest_host, field="access_key_id_env"
        ),
        secret_access_key=_resolve_required_env(
            cfg.secret_access_key_env,
            dest_host=cfg.dest_host,
            field="secret_access_key_env",
        ),
        region=cfg.region,
        service=cfg.service,
        session_token=session_token,
    )


async def _materialize_config_credentials(
    credentials: list[SigV4CredentialCfg],
    store: SqliteCredentialStore,
    *,
    instance_id: str,
) -> None:
    """Resolve each config-declared credential and write it into ``store``.

    The boot-time config acquisition route (plan Phase 2 TASK 2.4b, Steps C/D).
    For each entry: resolve its env-var NAMES to literals (B1, at boot), build a
    RESOLVED-value :class:`SigV4StaticCreds` / :class:`ProfileRefCred`, and
    ``set`` it under the normalized destination-host key with
    ``source="config"``, the SAME host normalization (``host_key_for``) and the
    SAME store ``set`` the runtime admin push uses, so by lookup time a
    config-declared credential is indistinguishable from an admin-pushed one. An
    empty list is a no-op (the bearer-only default). Runs once per instance
    build, so EVERY instance's store receives the top-level config map.

    Args:
        credentials: The ``settings.sigv4_credentials`` entries (possibly empty).
        store: This instance's already-started credential store.
        instance_id: The owning instance id (for the log line).

    Raises:
        ConfigCredentialError: A static entry's named env var is unset or empty
            (fail-fast; propagates out of :func:`_build_instance_context`).
    """
    for cfg in credentials:
        key = HostCredKey(host_key_for(cfg.dest_host))
        creds = _config_credential_to_internal(cfg)
        await store.set(key, creds, source="config")
        logger.info(
            "Materialized config sigv4 credential for instance=%s dest_host=%s kind=%s",
            instance_id,
            key,
            cfg.kind,
        )


async def _stop_quietly(store: _Startable, *, description: str, instance_id: str) -> None:
    """Best-effort ``stop()`` of an already-opened store on a degrade path.

    When a LATER per-instance boot stage degrades the instance, any store
    opened by an earlier stage must be closed so its file descriptor does
    not leak for the life of the process (the degraded instance never
    reaches ``_stop_instance``). A teardown error must not mask the degrade
    decision, so it is logged and swallowed.

    Args:
        store: The opened store/cache to close.
        description: Short label for the log line.
        instance_id: The degrading instance, for the log line.
    """
    try:
        await store.stop()
    except Exception:
        logger.warning(
            "Ignoring %s close error while degrading instance %r",
            description,
            instance_id,
            exc_info=True,
        )


async def _open_db_with_retry_then_isolate[StoreT](
    *,
    open_fresh: Callable[[], Awaitable[StoreT]],
    isolate: Callable[[], object],
    db_path: Path,
    data_root: Path,
    description: str,
    instance_id: str,
    metrics_registry: MetricsRegistry,
) -> StoreT:
    """Open a boot DB, riding out a transient lock and isolating a hard failure.

    The § 4D.1 boot-open guard, wrapping ``store.start()`` / ``token_cache.start()``.
    The integrity gate (corruption) and the § 4S schema gate already ran, so this
    catches an open-time failure that slips PAST both: a lock held past
    ``busy_timeout``, or a permission / I/O / unopenable-WAL fault the integrity
    probe did not surface.

    Flow:

    1. **Retry a transient lock.** ``open_fresh`` is run through the shared
       :func:`phantom.runtime.lock_retry.retry_on_transient_lock`, so a transient
       ``SQLITE_BUSY`` / ``SQLITE_LOCKED`` holder is ridden out with bounded
       backoff. Each attempt calls ``open_fresh`` again — which builds a FRESH
       store/cache and ``start()``s it, the natural unit of work for an open (a
       half-opened connection from a failed attempt is abandoned, not reused).
    2. **Isolate an unrecoverable failure.** Any other ``aiosqlite.Error`` /
       ``OSError`` at open, OR a transient lock that outlasts the budget
       (:class:`BootOpenLockError`), is unrecoverable: ``isolate`` moves the DB
       aside (reusing the corruption mover), ``db_quarantine_total`` is bumped,
       a WARNING names the EXACT open error, and ``open_fresh`` is retried ONCE
       on the now-empty path to boot fresh. We isolate (not delete) because the
       DB may be a real buffer we merely cannot open right now (§ 4D.0 point 2).
    3. **Degrade on an unwritable substrate.** If ``isolate`` or the post-isolate
       fresh open fails, PROBE ``data_root`` (:func:`_data_root_is_unwritable`):
       when the directory itself is unwritable (read-only / full ``data_dir``),
       raise :class:`_StorageSubstrateUnwritableError` so the caller degrades the
       instance. The probe — not the failed call's Python exception type — is the
       signal: a read-only directory surfaces as ``sqlite3.OperationalError`` (an
       ``aiosqlite.Error``, NOT an ``OSError``) from the recreate ``start()``, so
       keying off ``OSError`` alone would let that exact real-world fault
       crash-loop the boot. When the directory IS writable, the open failed for a
       different reason (a DB unusable past the isolate, a lock past budget on the
       reopen); that original error is re-raised UNCHANGED so a genuine fault is
       never silently degraded — the substrate probe is the only thing that turns
       a failure into a degrade.

    Args:
        open_fresh: Builds a fresh store/cache, ``start()``s it, and returns the
            STARTED object. Re-invoked per retry and once after isolation.
        isolate: Moves the DB (and WAL/SHM) aside. ``quarantine(...)`` for the
            upload store (coupled body tree), :func:`isolate_db_file` for the
            body-less token cache (m-4). Its return (the dest path(s)) is
            discarded — the movers log their own destinations — so the type is
            ``Callable[[], object]``.
        db_path: The DB path, for the WARNING + degraded-fault detail.
        data_root: The per-instance data directory holding ``db_path``, probed for
            writability to classify an isolate-or-recreate failure as the
            unwritable-substrate degrade case vs a genuine fault to re-raise.
        description: Short label for the open, used in the retry/isolate logs.
        instance_id: The instance id, for the logs + degraded detail.
        metrics_registry: Process-wide registry holding ``db_quarantine_total``.

    Returns:
        The successfully started store/cache (fresh, or the original if it
        opened on a later transient-lock attempt).

    Raises:
        _StorageSubstrateUnwritableError: The substrate (``data_root``) is
            unwritable; the caller degrades the instance.
    """
    try:
        return await retry_on_transient_lock(
            open_fresh,
            description=f"{description} open for instance {instance_id!r}",
            on_budget_exhausted=lambda last: BootOpenLockError(
                f"{description} open for instance {instance_id!r} could not "
                f"acquire the DB lock at {db_path} within the boot budget: {last}"
            ),
        )
    except _UNRECOVERABLE_OPEN_ERRORS as open_error:
        # Unrecoverable open: isolate the DB and boot fresh. A FURTHER failure
        # here (the rename aside, OR the recreate start() that itself cannot
        # create the file) is classified by PROBING data_root: an unwritable
        # substrate degrades (do NOT crash); a writable one re-raises the real
        # fault. We catch the same (aiosqlite.Error, OSError) classes the open
        # raises because the recreate runs the very same start() — a read-only
        # dir surfaces there as sqlite3.OperationalError (an aiosqlite.Error,
        # not an OSError), which keying off OSError alone would let escape.
        try:
            isolate()
            db_quarantine_total = metrics_registry.register_counter(
                DB_QUARANTINE_COUNTER_NAME,
                DB_QUARANTINE_COUNTER_DESCRIPTION,
            )
            await db_quarantine_total.inc()
            logger.warning(
                "Unrecoverable %s open for instance %r: isolated %s and booting "
                "fresh. Open error: %r. The isolated database is preserved (not "
                "deleted) and visible via GET /v1/admin/quarantine; it may be a "
                "real buffer that could not be opened (permission, I/O, locked, "
                "or an unopenable WAL).",
                description,
                instance_id,
                db_path,
                open_error,
            )
            return await open_fresh()
        except (aiosqlite.Error, OSError) as isolate_or_recreate_error:
            # Distinguish "the data_dir is unwritable" (degrade) from "this DB is
            # unusable but the dir is writable" (a genuine fault -> re-raise). The
            # probe, not the exception type, is the discriminator.
            if not _data_root_is_unwritable(data_root):
                raise
            detail = (
                f"{description} unavailable: the data directory {data_root} "
                f"is unwritable (could not isolate or recreate the database after "
                f"an unrecoverable open). Open error: {open_error!r}. Isolate/"
                f"recreate error: {isolate_or_recreate_error!r}."
            )
            raise _StorageSubstrateUnwritableError(detail) from isolate_or_recreate_error


# The typed per-instance boot outcome (cycle-7 seam 3): a healthy instance
# yields its wired context; a classified fault yields a DegradedInstance.
# The boot loop folds this union exhaustively (assert_never), so a new
# outcome variant cannot be silently dropped. The ``type`` keyword form is
# fine here (unlike the Literal aliases in phantom.models, nothing
# introspects this union via typing.get_args at runtime).
type BootOutcome = InstanceContext | DegradedInstance


def _degraded(instance_id: str, reason: DegradeReason, detail: str) -> DegradedInstance:
    """Build (and loudly log) one instance's typed DEGRADED outcome.

    The single construction point keeps the degrade log uniform: every
    degrade names the instance, the classified reason, the fault detail,
    and the operator's next action from :func:`degrade_action_hint`.

    Args:
        instance_id: The instance that cannot serve.
        reason: The classified :class:`DegradeReason`.
        detail: Operator-facing fault description (stage + original error).

    Returns:
        The :class:`DegradedInstance` the boot loop folds.
    """
    logger.error(
        "Instance %r booting DEGRADED (reason=%s): %s. Action: %s.",
        instance_id,
        reason.value,
        detail,
        degrade_action_hint(reason),
    )
    return DegradedInstance(instance_id=instance_id, reason=reason, detail=detail)


async def _build_instance_context(
    settings: Settings,
    cfg: InstanceCfg,
    settings_holder: SettingsHolder,
    metrics_registry: MetricsRegistry,
) -> BootOutcome:
    """Wire one per-instance boot outcome per :class:`InstanceCfg` (seam 3).

    Returns the wired :class:`InstanceContext` on a healthy boot, or a typed
    :class:`DegradedInstance` when a CLASSIFIED per-instance fault means the
    instance cannot serve (§ 4D.2 / finding M4-A): each boot stage
    (directory prep, integrity gate, backup reconcile, mode guard, schema
    gate, the two DB opens) maps its fault classes to a
    :class:`DegradeReason` member, so a per-instance storage fault degrades
    ONLY that instance instead of crash-looping the process. There is no
    side-channel map; the boot loop folds the returned union exhaustively
    and the ready/status surfaces read the typed results.

    Two refusals deliberately STAY exceptions (they are not degrades):

    * :class:`IntegrityFailClosedError` - the operator's
      ``db_integrity.fail_open=false`` strict abort (ADR-025's preserved
      corruption hatch) propagates and stops the process.
    * An unclassified error over a probe-confirmed WRITABLE substrate (the
      directory-prep re-raise) - a real, unexplained fault crashes loudly
      rather than hiding behind a degrade.

    Args:
        settings: Validated top-level settings.
        cfg: The instance whose context to build.
        settings_holder: The holder whose live snapshot the instance's
            ``current_settings`` thunk reads on each tick.
        metrics_registry: Process-wide metrics registry (plan § 4.2.2);
            threaded to every store / worker constructed here so emit
            sites resolve to the same surface as the admin endpoints.

    Returns:
        The :class:`BootOutcome`: a wired :class:`InstanceContext`, or the
        instance's :class:`DegradedInstance` (terminal until restart).
    """
    paths = instance_storage_paths(Path(settings.storage.data_dir), cfg)
    data_root = paths.data_root
    db_path = paths.db_path
    bodies_root = paths.bodies_root

    # Per-instance startup guards (plan § 5.2.2 / § 1.2 / § 1.3, findings
    # A-3 + F-2). Ordering is load-bearing: the data dir must exist so the
    # integrity gate can read/quarantine this instance's DB + body tree, then
    # reconciliation finishes any backup/restore move interrupted by a prior
    # crash (so the live tree is fully clean), then the mode guard inspects
    # that clean tree. All MUST run before the SqliteUploadStore opens and
    # before the body store is constructed — opening a corrupt DB or booting
    # all_ram over a populated tree is exactly what they prevent.
    #
    # Seam 3: each stage carries its OWN typed fault mapping below, so the
    # degrade decision is per-stage and exhaustive, not one ambient except.

    # Stage 1 - directory prep. A substrate fault (file-at-data_root,
    # read-only / full parent) degrades (M4-A); an mkdir failure over a
    # probe-confirmed WRITABLE directory re-raises (a real fault, not ours
    # to classify).
    try:
        _ensure_data_root_writable(data_root)
    except _StorageSubstrateUnwritableError as exc:
        return _degraded(cfg.id, DegradeReason.SUBSTRATE_UNWRITABLE, exc.detail)

    # Stage 2 - integrity gate. The fail-closed abort
    # (IntegrityFailClosedError, ADR-025's hatch) deliberately propagates; a
    # filesystem fault in the quarantine backup itself degrades (the corrupt
    # data could not be preserved aside, so booting fresh would risk it).
    try:
        await run_integrity_gate(
            db_path=db_path,
            bodies_root=bodies_root,
            data_root=data_root,
            fail_open=settings.storage.db_integrity.fail_open,
            metrics_registry=metrics_registry,
        )
    except OSError as exc:
        return _degraded(
            cfg.id,
            DegradeReason.QUARANTINE_BACKUP_FAILED,
            f"the corruption quarantine backup failed for {db_path}: {exc!r}",
        )

    # Stage 3 - finish-forward any interrupted mode-switch backup OR restore
    # move (plan § 1.1 / § 1.3). Runs BEFORE check_body_store_mode so an
    # interrupted backup is completed (the leftover body dirs swept out of
    # the live tree) before the mode guard judges whether the tree is
    # populated - without this a crash mid-backup could let the guard see an
    # empty tree and boot all_ram over a healthy DB whose rows point into the
    # backup (the A-3 data loss the guard exists to prevent). A fault here
    # (unreadable marker, manifest load, a move error) degrades: booting over
    # a half-moved tree IS that data loss.
    try:
        reconciled = reconcile_interrupted_backup_move(db_path=db_path, body_store_root=bodies_root)
    except (OSError, ValidationError) as exc:
        return _degraded(
            cfg.id,
            DegradeReason.BACKUP_RECONCILE_FAILED,
            f"finishing the interrupted backup/restore move failed: {exc!r}",
        )
    if reconciled is not None:
        logger.warning(
            "Completed interrupted %s move for instance %r on boot",
            reconciled,
            cfg.id,
        )
        # A "restore" completes a restore, not a backup, so only a "backup"
        # reconciliation bumps the back-up-and-run counter (idempotent fetch).
        if reconciled == "backup":
            mode_switch_backup_total = metrics_registry.register_counter(
                MODE_SWITCH_BACKUP_COUNTER_NAME,
                MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION,
            )
            await mode_switch_backup_total.inc()

    # Stage 4 - back up and run on an unsafe all_ram-over-populated-disk
    # switch (plan § 1.2 / § 1.3). A non-None return means the live DB + body
    # tree were relocated to a recoverable manifested mode_switch backup; log
    # loudly and bump the counter, then boot fresh over the now-empty live
    # tree. A fault in the backup move degrades: booting all_ram anyway would
    # condemn the disk-resident rows (A-3).
    try:
        backup = check_body_store_mode(
            mode=settings.storage.body_store.mode,
            bodies_root=bodies_root,
            db_path=db_path,
        )
    except OSError as exc:
        return _degraded(
            cfg.id,
            DegradeReason.MODE_SWITCH_BACKUP_FAILED,
            f"the all_ram mode-switch backup failed: {exc!r}",
        )
    if backup is not None:
        logger.warning(
            "Unsafe all_ram-over-populated-disk switch for instance %r: backed "
            "up live data and booting fresh (backup_id=%s). DB -> %s, body "
            "store -> %s. Restore via the admin quarantine-restore route "
            "(backup_id) after switching to a disk-backed mode.",
            cfg.id,
            backup.backup_id,
            backup.db_path,
            backup.body_path,
        )
        mode_switch_backup_total = metrics_registry.register_counter(
            MODE_SWITCH_BACKUP_COUNTER_NAME,
            MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION,
        )
        await mode_switch_backup_total.inc()

    # Stage 5 - schema gate (plan § 4S.2 / ADR-025): the LAST per-instance
    # guard before the store opens. A schema mismatch is a DISTINCT failure
    # from corruption (structurally sound bytes, wrong column set), so the
    # integrity gate above does not catch it. This deletes a pre-version /
    # wrong-schema DB (+ its WAL/SHM siblings) before store.start()'s
    # executescript can crash on it (an old DB missing an indexed column dies
    # inside start()), then the instance boots fresh. DELETE, not back up:
    # population of zero - no field DB can hold real undelivered uploads yet
    # (see run_schema_gate's unlink comment). The orphaned body tree is
    # reclaimed by the existing BodyOrphanJanitor in hybrid/all_disk; the
    # gate never touches it. A probe/unlink fault degrades: the gate could
    # not guarantee a safe shape to open.
    try:
        schema_result = await run_schema_gate(db_path=db_path, bodies_root=bodies_root)
    except (aiosqlite.Error, OSError) as exc:
        return _degraded(
            cfg.id,
            DegradeReason.SCHEMA_GATE_FAILED,
            f"the boot-time schema gate failed for {db_path}: {exc!r}",
        )
    if schema_result.discarded_path is not None:
        missing_columns = EXPECTED_UPLOADS_COLUMNS - schema_result.observed_columns
        logger.warning(
            "Pre-version / wrong-schema database for instance %r: deleted and "
            "booting fresh (population of zero - no field data to preserve). "
            "Deleted %s. Observed user_version=%d, expected %d; missing required "
            "columns vs the current schema: %s.",
            cfg.id,
            schema_result.discarded_path,
            schema_result.observed_version,
            SCHEMA_VERSION,
            sorted(missing_columns) or "(none - version mismatch only)",
        )
        schema_discard_total = metrics_registry.register_counter(
            SCHEMA_DISCARD_COUNTER_NAME,
            SCHEMA_DISCARD_COUNTER_DESCRIPTION,
        )
        await schema_discard_total.inc()

    # Stages 6 + 7 - the two SQLite opens. One persistent SQLite, one
    # mode-selected BodyStore binding. The :memory: store is gone - there is
    # no scratch tier anymore; RAM buffering lives entirely in
    # :class:`RamBodyStore` (the body half) while metadata always lands in
    # the single persistent SQLite.
    sqlite_cfg = settings.storage.sqlite
    token_cache_db_path = data_root / "token_cache.db"
    # The aws_sigv4 signer's host-keyed destination-credential store lives in
    # its OWN database file (separate from uploads.db and token_cache.db), so
    # credential writes stay off the hot uploads / token-cache writer locks.
    credential_store_db_path = data_root / "credential_store.db"

    # § 4D.1 - boot-open guard. Both SQLite opens (the upload store and the
    # body-less token cache) run through _open_db_with_retry_then_isolate: a
    # transient lock at open is ridden out with the shared bounded retry; any
    # other unrecoverable open failure (permission / I/O / unopenable WAL, or a
    # lock past budget) isolates the DB and boots fresh; an unwritable data_dir
    # raises _StorageSubstrateUnwritableError, mapped to a typed degrade. A
    # failure PAST every recovery on a writable substrate also degrades (seam
    # 3: DB_UNRECOVERABLE for the uploads DB, STORE_OPEN_FAILED for the token
    # cache) so one instance's dead storage stack cannot crash-loop the
    # process. Each guard arg ``open_fresh`` builds a FRESH store/cache and
    # start()s it, so a retry or a post-isolate reopen never reuses a
    # half-opened connection. The body-less token cache isolates via
    # isolate_db_file (m-4) - the coupled quarantine(...) would move the
    # upload body tree, which is wrong for it.
    def _open_upload_store() -> Awaitable[SqliteUploadStore]:
        fresh = SqliteUploadStore(
            str(db_path),
            sqlite_cfg=sqlite_cfg,
            metrics_registry=metrics_registry,
        )
        return _started(fresh)

    def _open_token_cache() -> Awaitable[SqliteTokenCache]:
        fresh = SqliteTokenCache(str(token_cache_db_path), sqlite_cfg=sqlite_cfg)
        return _started(fresh)

    def _open_credential_store() -> Awaitable[SqliteCredentialStore]:
        fresh = SqliteCredentialStore(str(credential_store_db_path), sqlite_cfg=sqlite_cfg)
        return _started(fresh)

    try:
        store = await _open_db_with_retry_then_isolate(
            open_fresh=_open_upload_store,
            isolate=lambda: quarantine(db_path, bodies_root, reason="corrupted"),
            db_path=db_path,
            data_root=data_root,
            description="upload store",
            instance_id=cfg.id,
            metrics_registry=metrics_registry,
        )
    except _StorageSubstrateUnwritableError as exc:
        return _degraded(cfg.id, DegradeReason.SUBSTRATE_UNWRITABLE, exc.detail)
    except (aiosqlite.Error, OSError, BootOpenLockError) as exc:
        return _degraded(
            cfg.id,
            DegradeReason.DB_UNRECOVERABLE,
            f"the upload store at {db_path} could not be opened, isolated, or recreated: {exc!r}",
        )

    try:
        token_cache = await _open_db_with_retry_then_isolate(
            open_fresh=_open_token_cache,
            isolate=lambda: isolate_db_file(token_cache_db_path),
            db_path=token_cache_db_path,
            data_root=data_root,
            description="token cache",
            instance_id=cfg.id,
            metrics_registry=metrics_registry,
        )
    except _StorageSubstrateUnwritableError as exc:
        await _stop_quietly(store, description="upload store", instance_id=cfg.id)
        return _degraded(cfg.id, DegradeReason.SUBSTRATE_UNWRITABLE, exc.detail)
    except (aiosqlite.Error, OSError, BootOpenLockError) as exc:
        # The upload store opened but the instance still cannot serve: close
        # the open store before degrading so the descriptor never leaks.
        await _stop_quietly(store, description="upload store", instance_id=cfg.id)
        return _degraded(
            cfg.id,
            DegradeReason.STORE_OPEN_FAILED,
            f"the token cache at {token_cache_db_path} could not be opened, "
            f"isolated, or recreated: {exc!r}",
        )

    # The aws_sigv4 credential store — same boot-open guard as the token cache
    # (own DB file, isolated via isolate_db_file rather than the body-coupled
    # quarantine). On failure, close the already-open stores before degrading so
    # no descriptor leaks.
    try:
        credential_store = await _open_db_with_retry_then_isolate(
            open_fresh=_open_credential_store,
            isolate=lambda: isolate_db_file(credential_store_db_path),
            db_path=credential_store_db_path,
            data_root=data_root,
            description="credential store",
            instance_id=cfg.id,
            metrics_registry=metrics_registry,
        )
    except _StorageSubstrateUnwritableError as exc:
        await _stop_quietly(token_cache, description="token cache", instance_id=cfg.id)
        await _stop_quietly(store, description="upload store", instance_id=cfg.id)
        return _degraded(cfg.id, DegradeReason.SUBSTRATE_UNWRITABLE, exc.detail)
    except (aiosqlite.Error, OSError, BootOpenLockError) as exc:
        await _stop_quietly(token_cache, description="token cache", instance_id=cfg.id)
        await _stop_quietly(store, description="upload store", instance_id=cfg.id)
        return _degraded(
            cfg.id,
            DegradeReason.STORE_OPEN_FAILED,
            f"the credential store at {credential_store_db_path} could not be "
            f"opened, isolated, or recreated: {exc!r}",
        )

    # Config acquisition route (plan Phase 2 TASK 2.4b): with the credential
    # store open, materialize the top-level ``sigv4_credentials`` declarations
    # into THIS instance's store — resolve each entry's named env var(s) to
    # literals (the B1 boot-time resolution) and ``set`` under the normalized
    # destination host with ``source="config"``. Empty (the default) is a
    # no-op. A missing/empty named env var raises ConfigCredentialError, which
    # PROPAGATES (a config error the operator must fix — not a per-instance
    # degrade), crashing boot loudly. Runs per instance, so every instance's
    # store receives the top-level map (the admin-push fan-out analogue). On
    # failure, close the three already-open stores before propagating so no
    # descriptor leaks (mirrors the degrade paths' pre-return cleanup).
    try:
        await _materialize_config_credentials(
            settings.sigv4_credentials, credential_store, instance_id=cfg.id
        )
    except ConfigCredentialError:
        await _stop_quietly(credential_store, description="credential store", instance_id=cfg.id)
        await _stop_quietly(token_cache, description="token cache", instance_id=cfg.id)
        await _stop_quietly(store, description="upload store", instance_id=cfg.id)
        raise

    ram_body_store = RamBodyStore()
    file_body_store = FileBodyStore(
        bodies_root,
        shard_prefix_chars=settings.storage.shard_prefix_chars,
    )
    upstream_client = HttpxUpstreamClient(timeout_seconds=settings.upstream.timeout_seconds)

    await ram_body_store.start()
    await file_body_store.start()
    await upstream_client.start()

    # Mode-selected body store + optional PersistController, composed via
    # the single shared decision table (plan § 2.3.10):
    #     hybrid  → HybridBodyStore + PersistController
    #     all_ram → RamBodyStore only; no PersistController target
    #     all_disk → FileBodyStore only; no PersistController source
    # The PersistController, when present, is wired into the sender's
    # retry-linger trigger, the RAM-pressure watcher, and admission's
    # size-aware immediate-persist hook; each call site invokes
    # ``await controller.enqueue(chain_id)`` (idempotent, fire-and-forget).
    # The two halves are kept on the InstanceContext regardless of mode.
    body_store: BodyStore
    persist_controller: PersistController | None
    body_store, persist_controller = await build_body_store(
        mode=settings.storage.body_store.mode,
        ram_body_store=ram_body_store,
        file_body_store=file_body_store,
        store=store,
        metrics_registry=metrics_registry,
    )

    # Per-instance AD-mint construction. When ``cfg.ad_mint`` is set,
    # construct an :class:`AdMinter` that mints AD tokens proactively
    # and writes them to the (endpoint, uid) cache. When ``cfg.ad_mint``
    # is None, the instance relies on inbound-request token injection.
    minter: AdMinter | None
    if cfg.ad_mint is not None:
        minter = AdMinter(config=cfg.ad_mint, token_cache=token_cache)
    else:
        minter = None

    retry_strategy = build_retry_strategy(settings.retry.default_strategy)

    sat_cfg = settings.saturation
    # Every probe-fillable knob is guaranteed non-None post-validator
    # (see Settings._resolve_defaults). The asserts narrow the type
    # for SaturationGate's int-only constructor.
    assert sat_cfg.max_in_flight is not None
    assert sat_cfg.max_in_flight_bytes is not None
    assert sat_cfg.max_disk_bytes is not None
    assert sat_cfg.large_body_threshold_bytes is not None
    assert sat_cfg.max_large_in_flight is not None
    saturation = SaturationGate(
        max_in_flight=sat_cfg.max_in_flight,
        max_in_flight_bytes=sat_cfg.max_in_flight_bytes,
        max_disk_bytes=sat_cfg.max_disk_bytes,
        large_body_threshold_bytes=sat_cfg.large_body_threshold_bytes,
        max_large_in_flight=sat_cfg.max_large_in_flight,
        metrics_registry=metrics_registry,
    )

    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=upstream_client,
        resolve_route=resolve_route,
        clock=default_clock,
        instance=cfg,
        signer_creds=credential_store,
    )

    instance_id = cfg.id

    def current_settings_thunk() -> InstanceSettingsSnapshot:
        return settings_holder.snapshot_for(instance_id)

    def codec_factory() -> BodyCodec:
        # Selects from the LIVE settings snapshot per call so a hot
        # reload of compression.algorithm / compression.level applies
        # to the next admission (ADR-013; round 5 fix R5-2). The
        # one-codec-per-deployment posture is unchanged: the codec
        # still comes from the single storage-level compression block,
        # just the CURRENT one instead of the boot-time capture.
        return select_codec(current_settings_thunk().compression)

    return InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram_body_store,
        file_body_store=file_body_store,
        body_store=body_store,
        persist_controller=persist_controller,
        token_cache=token_cache,
        minter=minter,
        retry_strategy=retry_strategy,
        upstream_client=upstream_client,
        executor=executor,
        saturation=saturation,
        codec_factory=codec_factory,
        current_settings=current_settings_thunk,
        signer_creds=credential_store,
    )


def _build_resolved_defaults_summary(settings: Settings) -> ResolvedDefaultsSummary:
    """Project resolved-settings values into the admin-surface shape.

    Probes the host once more so the admin response can echo the same
    fact set ``compute_defaults`` saw (total RAM, free disk, CPU count).
    The cost is two syscalls per ``/v1/admin/status`` request; cheap
    relative to the response build itself.
    """
    sat = settings.saturation
    storage = settings.storage
    retry = settings.retry
    assert sat.max_in_flight is not None
    assert sat.max_in_flight_bytes is not None
    assert sat.max_disk_bytes is not None
    assert sat.large_body_threshold_bytes is not None
    assert sat.max_large_in_flight is not None
    assert storage.body_store.ram_ceiling_bytes is not None
    assert storage.persist_trigger.body_size_threshold_bytes is not None
    assert retry.worker_count is not None
    facts = probe_machine(settings.storage.data_dir)
    return ResolvedDefaultsSummary(
        max_in_flight=sat.max_in_flight,
        max_in_flight_bytes=sat.max_in_flight_bytes,
        max_disk_bytes=sat.max_disk_bytes,
        ram_ceiling_bytes=storage.body_store.ram_ceiling_bytes,
        large_body_threshold_bytes=sat.large_body_threshold_bytes,
        max_large_in_flight=sat.max_large_in_flight,
        persist_body_size_threshold_bytes=storage.persist_trigger.body_size_threshold_bytes,
        worker_count=retry.worker_count,
        observed_total_ram_bytes=facts.total_ram_bytes,
        observed_free_disk_bytes=facts.free_disk_bytes,
        observed_cpu_count=facts.cpu_count,
    )


async def _stop_instance(ctx: InstanceContext) -> None:
    """Tear down every Protocol-typed dependency for one instance.

    The AdMinter is no longer torn down here — Phase 2 § 3.2.5 (H6
    closure) moved it under the lifespan's :class:`asyncio.TaskGroup`,
    which cancels the run-loop on lifespan exit. Calling minter.stop()
    here would be a double-stop (TaskGroup cancellation already
    happened) and ``stop()`` no longer exists on the minter API.
    """
    await ctx.upstream_client.stop()
    await ctx.token_cache.stop()
    if ctx.signer_creds is not None:
        await ctx.signer_creds.stop()
    await ctx.body_store.stop()
    await ctx.store.stop()


def _warn_if_bound_non_loopback(settings: Settings) -> None:
    """Warn loudly when the single listener is bound beyond loopback.

    The deployment is same-machine-only: Phantom runs on the SAME box as
    its producer and is reached over loopback, so the ONE listener
    (intake + admin + health) defaults to ``127.0.0.1``. The admin surface
    carries NO application-level authentication by design (ADR-004): with
    the loopback default bind, that bind IS the access control. A
    non-loopback ``bind_tcp`` host is therefore an explicit operator opt-in
    to expose the WHOLE surface - including the UNAUTHENTICATED admin API -
    beyond the host. We keep serving (the opt-in is deliberate) but emit a
    prominent warning naming the host, stating the admin endpoints are
    unauthenticated, and instructing the operator to front them with an
    authenticating reverse proxy. The check skips the UDS case (a UDS is a
    filesystem-permissioned local socket, not a network exposure).

    Args:
        settings: The resolved top-level settings.
    """
    if settings.server.bind_uds is not None:
        return
    host, _, _ = settings.server.bind_tcp.partition(":")
    if host_is_loopback(host):
        return
    logger.warning(
        "listener bound to NON-LOOPBACK host %r (%s): the admin API rides this same "
        "listener and is UNAUTHENTICATED by design (ADR-004) - the loopback bind is its "
        "only access control. Exposing it beyond loopback means anyone who can reach %s "
        "can run destructive admin operations (bulk delete, token delete, reload). Front "
        "the port with an authenticating reverse proxy. See ADR-004.",
        host,
        settings.server.bind_tcp,
        settings.server.bind_tcp,
    )


def create_app(
    settings: Settings,
    *,
    settings_path: Path | None = None,
    worker_failure_callback: Callable[[], None] | None = None,
) -> FastAPI:
    """Build the single ASGI application for the given :class:`Settings`.

    The deployment is same-machine-only (Phantom runs on the SAME box as
    its producer and is reached over loopback), so ONE FastAPI app served by ONE
    uvicorn server carries intake (``POST /v1/send``), the admin surface
    (``/v1/admin/*``), and the public liveness/readiness probes
    (``GET /v1/healthz`` / ``GET /v1/readyz``) on ONE port. The admin
    surface is reachable only on the machine because the single listener
    defaults to loopback (``bind_tcp`` default ``127.0.0.1:8080``); that
    loopback bind IS the admin access control (ADR-004). A non-loopback
    ``bind_tcp`` is a deliberate opt-in that emits the unauthenticated-
    exposure warning.

    (History: a two-listener split that bound the admin router on its own
    socket was tried (R12-1) and collapsed here as no-benefit for the
    same-machine deployment - it introduced a startup-ordering bug (R13-1)
    and a bind-collision bug (R13-2), both of which the single listener
    eliminates by construction.)

    Args:
        settings: The loaded + validated top-level settings.
        settings_path: Filesystem path of the YAML config the
            ``settings`` was loaded from. Required for hot reload — both
            SIGHUP and ``POST /v1/admin/reload`` re-read this file. When
            ``None`` (e.g., tests that synthesize a Settings instance),
            hot reload is disabled (the SIGHUP handler is not installed
            and the admin endpoint returns 422).
        worker_failure_callback: Production-server hook invoked when a
            supervised long-lived worker raises an ordinary exception. The
            CLI uses it to stop uvicorn, whose lifespan protocol logs
            post-start failures but does not otherwise terminate its serving
            loop. TaskGroup's direct ``SystemExit``/``KeyboardInterrupt``
            special cases are outside this callback contract.

    Returns:
        The configured :class:`FastAPI` application.
    """
    configure_logging(settings.observability)
    _warn_if_bound_non_loopback(settings)

    # Plan § 4.2 — one process-wide metrics registry. Threaded to every
    # store / worker that emits metrics; exposed via the admin
    # observability endpoints (plan § 4.2.5).
    metrics_registry = MetricsRegistry()

    instances: list[InstanceContext] = []
    # Cycle-7 seam 3 - the typed degraded set. Instances whose per-instance
    # boot returned a DegradedInstance (a classified storage fault): no
    # store/context was built, so they are absent from ``instances`` and the
    # dispatcher. The lifespan's exhaustive BootOutcome fold appends here;
    # the public /v1/readyz + /v1/healthz probes and the POST /v1/send guard
    # read this SAME list through their dependencies. The old
    # ``dict[str, str]`` side-channel is gone.
    degraded_boot: list[DegradedInstance] = []

    settings_holder = SettingsHolder()
    initial_snapshots: dict[str, InstanceSettingsSnapshot] = {
        inst_cfg.id: _build_snapshot(settings, inst_cfg) for inst_cfg in settings.instances
    }

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        # This lifespan is attached to the single ``app`` (assigned below,
        # before this coroutine ever runs); the closure references that
        # named ``app`` to wire its dependency overrides, so ``_app`` itself
        # is intentionally unused.
        #
        # Process-wide startup guards — run ONCE, before any instance
        # context is built (the integrity gate + all_ram guard run
        # per-instance inside _build_instance_context).
        #   * apply_umask: bare-metal owner-only file perms (WS-4 F6).
        #   * check_retention_floor: bodies must not outlive their row.
        #   * check_instance_isolation: unique id / non-nested data_dir /
        #     unique host_prefix so every instance is fully isolated.
        # Each raises before any store/worker exists, so a misconfig
        # crashes startup cleanly.
        apply_umask()
        check_retention_floor(settings)
        check_instance_isolation(settings.instances)
        # Register db_quarantine_total on the process-wide registry so the
        # per-instance integrity gate's bump lands on the same surface the
        # admin observability endpoints expose (the gate also registers
        # idempotently, so this is the canonical declaration site).
        metrics_registry.register_counter(
            DB_QUARANTINE_COUNTER_NAME,
            DB_QUARANTINE_COUNTER_DESCRIPTION,
        )
        # Register mode_switch_backup_total alongside it (plan § 1.3). The
        # per-instance back-up-and-run path and the reconciliation path each
        # fetch this counter idempotently and bump it, so this is the
        # canonical declaration site (same precedent as db_quarantine_total).
        metrics_registry.register_counter(
            MODE_SWITCH_BACKUP_COUNTER_NAME,
            MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION,
        )
        # Register schema_discard_total alongside it (plan § 4S.2). The
        # per-instance schema gate's call site fetches this counter
        # idempotently and bumps it on a discard, so this is the canonical
        # declaration site (same precedent as db_quarantine_total).
        metrics_registry.register_counter(
            SCHEMA_DISCARD_COUNTER_NAME,
            SCHEMA_DISCARD_COUNTER_DESCRIPTION,
        )
        # Install the initial per-instance snapshots before any instance
        # context is built — the InstanceContext.current_settings thunk
        # captures the holder by reference and resolves lazily, so the
        # holder must be populated before the first worker tick.
        await settings_holder.replace(initial_snapshots)
        # Size the default thread pool for ingress concurrency.
        #
        # Every ingress request offloads CPU-bound work to the thread
        # pool: sha256(raw), codec.encode(raw), sha256(encoded), plus
        # fsync(dir) and fsync(file)/replace on the persist path — up
        # to ~6 thread-pool calls per in-flight admission. CPython's
        # default executor (``min(32, cpu+4)``) saturates around 12
        # threads on typical producer hardware; under 16-way concurrent
        # ingress this serializes the multipart receive and inflates
        # the synchronous-return latency. Size to a generous multiple
        # of the in-flight cap, capped at 64 to stay below the OS
        # thread limit.
        _resize_default_executor(settings)
        # Build instances by folding each typed BootOutcome exhaustively
        # (cycle-7 seam 3). A healthy context joins ``instances`` and runs
        # recovery; a DegradedInstance joins the typed degraded set - no
        # store, no recovery, no dispatcher entry, no workers - so the rest
        # of the process boots normally and the fault surfaces on /ready,
        # /health, and the POST /send guard. ``assert_never`` closes the
        # union: a future third outcome variant fails mypy strict here
        # rather than being silently dropped. Two deliberate refusals
        # PROPAGATE out of _build_instance_context by design (they are not
        # outcomes): IntegrityFailClosedError (the operator's fail-closed
        # corruption abort, ADR-025) and an unclassified fault over a
        # writable substrate (a real bug must crash loudly).
        for inst_cfg in settings.instances:
            outcome: BootOutcome = await _build_instance_context(
                settings, inst_cfg, settings_holder, metrics_registry
            )
            match outcome:
                case InstanceContext():
                    instances.append(outcome)
                    # Recovery operates on the single persistent store + the
                    # mode-selected body store (plan § 2.3.15).
                    await run_recovery(outcome.store, outcome.body_store)
                    await reconcile_saturation(outcome.store, outcome.saturation)
                case DegradedInstance():
                    degraded_boot.append(outcome)
                case _:
                    assert_never(outcome)
            # AdMinter is no longer self-spawning. Its run-loop is
            # spawned on the lifespan TaskGroup below (H6 closure —
            # the unsupervised ``minter.start()`` create_task call
            # site is gone).

        dispatcher = InstanceDispatcher(instances)

        # Dependency injection: bind the live dispatcher + the typed
        # degraded set on the single app. Intake, admin, and health all
        # resolve their dependencies from this one app's overrides.
        resolved_defaults_summary = _build_resolved_defaults_summary(settings)
        # Intake (POST /v1/send) + the public health/readiness probes
        # (GET /v1/healthz, /v1/readyz).
        app.dependency_overrides[send_routes.get_dispatcher] = lambda: dispatcher
        app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: (
            settings.storage.max_buffered_bytes
        )
        # Seam 3 - the POST /v1/send degraded-boot guard resolves the
        # CONFIGURED target instance over settings.instances (a degraded
        # instance is absent from the dispatcher) and 500s if that id is in
        # the typed degraded set. Both exist regardless of boot outcome.
        app.dependency_overrides[send_routes.get_instance_cfgs] = lambda: settings.instances
        app.dependency_overrides[send_routes.get_degraded_instances] = lambda: tuple(degraded_boot)
        # The raw-intake catch-all's second destination carrier (Phase 1
        # TASK 1.3). Stringified to the str the DI surface expects (or None
        # when no default upstream is configured).
        app.dependency_overrides[send_routes.get_phantom_default_target] = lambda: (
            str(settings.phantom_default_target)
            if settings.phantom_default_target is not None
            else None
        )
        app.dependency_overrides[health_routes.get_version] = lambda: __version__
        app.dependency_overrides[health_routes.get_dispatcher] = lambda: dispatcher
        # Seam 3 - bind the typed degraded set so /v1/readyz + /v1/healthz
        # report a degraded boot. The lambda snapshots the SAME list the
        # build loop above folded into (app.state.degraded_boot mirrors it),
        # so a probe served after the lifespan ran sees the populated set.
        app.dependency_overrides[health_routes.get_degraded_instances] = lambda: tuple(
            degraded_boot
        )
        # Admin surface (/v1/admin/*) rides the SAME app.
        app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
        app.dependency_overrides[admin_routes.get_version] = lambda: __version__
        app.dependency_overrides[admin_routes.get_resolved_defaults_summary] = lambda: (
            resolved_defaults_summary
        )
        # Plan § 4.2.5 — observability admin endpoints depend on the
        # process-wide MetricsRegistry.
        app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: metrics_registry
        # Plan § 5.2.5 — quarantine-inventory endpoint depends on the
        # resolved storage data_dir for the filesystem walk.
        app.dependency_overrides[admin_routes.get_data_root] = lambda: Path(
            settings.storage.data_dir
        )

        # Spawn workers per instance under one TaskGroup. An unhandled
        # worker exception cancels every sibling and bubbles out as an
        # ExceptionGroup. The production CLI callback below the group then
        # requests uvicorn shutdown; pinned uvicorn drains and then re-raises
        # SIGTERM so the orchestrator restarts. No silent worker death.
        assert settings.retry.worker_count is not None
        stop_event = asyncio.Event()

        # Install the SIGHUP handler before yielding control. The handler
        # (from phantom.runtime.reload) schedules ``apply_reload`` on the
        # running loop; the file lock inside ``SettingsHolder.replace``
        # guarantees concurrent reloads do not interleave.
        sighup_installed = False
        if settings_path is not None:
            loop = asyncio.get_running_loop()
            try:
                loop.add_signal_handler(
                    signal.SIGHUP,
                    make_sighup_handler(settings_holder, settings_path, instances),
                )
                sighup_installed = True
            except _SIGHUP_INSTALL_ERRORS:
                # add_signal_handler is unsupported on Windows event loops
                # and may fail when running outside the main thread (e.g.,
                # in some test harnesses). Hot reload via POST is still
                # available — log and continue.
                logger.warning("SIGHUP handler not installed; admin reload endpoint only")

        try:
            async with asyncio.TaskGroup() as tg:
                for ctx in instances:
                    sender = Sender(
                        instance=ctx,
                        worker_count=settings.retry.worker_count,
                        poll_interval_ms=settings.retry.poll_interval_ms,
                        metrics_registry=metrics_registry,
                    )
                    kicker = Kicker(instance=ctx, flavour=PHANTOM_BEARER_FLAVOUR)
                    # The SAME class in its other flavour (CL2). It reads the
                    # SAME ctx (which carries ``signer_creds``); its oracle
                    # reports itself unconfigured when ``ctx.signer_creds is
                    # None`` (the bearer-only deployment), so it registers no
                    # wake-handler and its rescan returns early. Spawning it on
                    # every instance's TaskGroup is uniform and inert by
                    # default.
                    cred_kicker = Kicker(instance=ctx, flavour=AWS_SIGV4_FLAVOUR)
                    vacuum = VacuumScheduler(
                        instance=ctx, cron_spec=settings.storage.sqlite.vacuum_cron
                    )
                    tg.create_task(sender.run(stop_event), name=f"sender-{ctx.cfg.id}")
                    tg.create_task(kicker.run(stop_event), name=f"auth-kicker-{ctx.cfg.id}")
                    tg.create_task(
                        cred_kicker.run(stop_event), name=f"credential-kicker-{ctx.cfg.id}"
                    )
                    tg.create_task(vacuum.run(stop_event), name=f"vacuum-{ctx.cfg.id}")
                    # Plan § 5.2.6 — optional per-instance cold backup.
                    # Each instance has its own data_root/uploads.db, so
                    # one scheduler per instance when the operator opts in
                    # via db_integrity.backup_enabled. Writes only to
                    # <data_root>/backups/ — the live DB stays single-writer
                    # (plan § 0.5). Driven by stop_event like every other
                    # lifespan worker so the TaskGroup drains cleanly on
                    # shutdown (an unstoppable loop would block teardown).
                    if settings.storage.db_integrity.backup_enabled:
                        backup_paths = instance_storage_paths(
                            Path(settings.storage.data_dir), ctx.cfg
                        )
                        cold_backup = ColdBackupScheduler(
                            db_path=backup_paths.db_path,
                            backup_root=backup_paths.data_root / "backups",
                            settings=settings,
                        )
                        tg.create_task(
                            cold_backup.run(stop_event),
                            name=f"cold-backup-{ctx.cfg.id}",
                        )
                    # H6 audit closure — AdMinter run-loop is now
                    # supervised by the lifespan TaskGroup. An unhandled
                    # exception (AuthUnavailableError with empty backoff,
                    # azure-identity import failure, etc.) propagates as
                    # an ExceptionGroup out of this ``async with`` and
                    # crashes the process visibly; the orchestrator
                    # restarts it. Pre-Phase-2 the minter spawned its
                    # own asyncio.create_task — that task was
                    # unsupervised and a silent exception left the
                    # runtime believing the minter was healthy.
                    if ctx.minter is not None:
                        tg.create_task(
                            ctx.minter.run(stop_event),
                            name=f"ad-minter-{ctx.cfg.id}",
                        )
                    # Mode-gated workers (plan § 2.3.10 + § 2.3.12 / § 2.3.13 /
                    # § 2.3.14). The PersistController only makes sense in
                    # ``hybrid`` mode (RAM source + disk target); the
                    # RamPressureWatcher relies on it. DiskPressureProbe
                    # samples the file body store, so it is meaningful in
                    # ``hybrid`` and ``all_disk``. BodyOrphanJanitor sweeps
                    # disk orphans only — same two modes. ``all_ram`` spawns
                    # none of these.
                    if ctx.persist_controller is not None:
                        # The watcher reads ceiling and cadence from the
                        # instance's live snapshot per tick (R6-2), so it
                        # takes no config values here.
                        ram_watcher = RamPressureWatcher(
                            instance=ctx,
                            persist_controller=ctx.persist_controller,
                            metrics_registry=metrics_registry,
                        )
                        tg.create_task(
                            ctx.persist_controller.run(stop_event),
                            name=f"persist-controller-{ctx.cfg.id}",
                        )
                        tg.create_task(
                            ram_watcher.run(stop_event),
                            name=f"ram-pressure-{ctx.cfg.id}",
                        )
                    mode = settings.storage.body_store.mode
                    if mode in ("hybrid", "all_disk"):
                        disk_probe = DiskPressureProbe(instance=ctx)
                        tg.create_task(
                            disk_probe.run(stop_event),
                            name=f"disk-probe-{ctx.cfg.id}",
                        )
                        janitor = BodyOrphanJanitor(
                            store=ctx.store,
                            body_store=ctx.body_store,
                            # Cadence reads the live snapshot per loop
                            # iteration (T1 / ADR-031).
                            current_settings=ctx.current_settings,
                            metrics_registry=metrics_registry,
                        )
                        tg.create_task(
                            janitor.run(stop_event),
                            name=f"body-orphan-janitor-{ctx.cfg.id}",
                        )

                tg.create_task(
                    Reaper(instances=instances, metrics_registry=metrics_registry).run(stop_event),
                    name="reaper",
                )
                # Plan § 4.2.3 — InvariantAuditor runs in every mode.
                # One audit coroutine per instance — each instance has its
                # own store/body_store pair so the audit is scoped per
                # instance.
                for ctx in instances:
                    auditor = InvariantAuditor(
                        store=ctx.store,  # type: ignore[arg-type]
                        body_store=ctx.body_store,
                        # Cadence reads the live snapshot per loop
                        # iteration (R9-1 / ADR-031).
                        current_settings=ctx.current_settings,
                        metrics_registry=metrics_registry,
                    )
                    tg.create_task(
                        auditor.run(stop_event),
                        name=f"invariant-audit-{ctx.cfg.id}",
                    )

                try:
                    yield
                finally:
                    stop_event.set()
        except BaseExceptionGroup:
            # TaskGroup wraps ordinary child exceptions in a group. Python
            # deliberately re-raises child SystemExit/KeyboardInterrupt
            # directly; do not broaden this to BaseException because normal
            # ASGI lifespan cancellation must not masquerade as a worker crash.
            if worker_failure_callback is not None:
                worker_failure_callback()
            raise
        finally:
            if sighup_installed:
                with suppress_signal_handler_errors():
                    asyncio.get_running_loop().remove_signal_handler(signal.SIGHUP)
            for ctx in instances:
                await _stop_instance(ctx)

    # The single app owns the SOLE lifespan (the one worker TaskGroup,
    # recovery, SIGHUP, the dependency wiring). It serves intake + admin +
    # health on one loopback-bound socket, so the worker pool starts exactly
    # once and admin is reachable only on the machine (the loopback default
    # bind is the admin access control; ADR-004).
    app = FastAPI(title="phantom", version=__version__, lifespan=lifespan)

    # Expose the live settings holder so the hot-reload route handler
    # (Family 5) can swap snapshots without reaching into the lifespan
    # closure. The ``instances`` list and ``settings_path`` mirror the
    # state SIGHUP needs so the admin endpoint can reuse
    # ``phantom.runtime.reload.apply_reload``. POST /v1/admin/reload reads
    # ``request.app.state``; the lifespan + the composition-root tests read
    # ``app.state``. The ``instances`` list is mutated in place by the
    # lifespan.
    app.state.settings_holder = settings_holder
    app.state.settings_path = settings_path
    app.state.instances = instances
    # Seam 3 - the same typed degraded set the lifespan's BootOutcome fold
    # populates (a list of DegradedInstance values; same object the closure
    # mutates), so it is populated by the time the lifespan has built
    # instances. The /v1/readyz + /v1/healthz probes and the POST /v1/send
    # guard read it through their dependencies. Empty in the normal
    # all-instances-healthy case.
    app.state.degraded_boot = degraded_boot
    # Plan § 4.2 — admin observability endpoints (plan § 4.2.5) resolve the
    # process-wide MetricsRegistry from app.state.
    app.state.metrics_registry = metrics_registry

    # One app: producer-facing intake + the public liveness/readiness probes +
    # the admin surface (/v1/admin/*). The whole surface rides one socket
    # bound to loopback by default, so admin is reachable only on the
    # machine (ADR-004 - the loopback bind is the admin access control).
    app.include_router(send_routes.router)
    app.include_router(health_routes.router)
    app.include_router(admin_routes.router)
    # The raw-intake catch-all ``/{phantom_path:path}`` is root-mounted, so it
    # MUST register LAST: FastAPI matches routes in registration order,
    # first-match-wins, and registering before the fixed ``/v1/*`` routers
    # would shadow ``/v1/send``, ``/v1/admin/*`` and the health probes. Mounted
    # last (after admin), the upload verbs only reach it when no fixed route
    # matched; its complementary GET/HEAD/DELETE/OPTIONS arm preserves the
    # service-wide 404 for unknown non-upload requests (Phase 1 TASK 1.1).
    app.include_router(catch_all_routes.router)
    # Every admin typed error (instance_unknown 421, not_found 404,
    # restore_noop 409, lookup_not_configured 400, replay_body_discarded
    # 409, replay_refused_attempting 409, multifile_cursor_conflict 422,
    # key_value_match_invalid 422, bulk_delete_filter_empty 422) rides the
    # ONE shared registration helper so this app factory, the contract
    # admin conftest, and the test fixtures cannot drift apart (round 3
    # defender fix R3-1). Each handler's docstring in routes/admin.py
    # carries the rationale for its envelope code and status.
    admin_routes.register_admin_error_handlers(app)

    # Pre-register dependency overrides so tests can boot the app without
    # the lifespan (FastAPI dependency_overrides survives outside the
    # lifespan). The lifespan rebinds them with the live dispatcher.
    app.dependency_overrides.setdefault(
        send_routes.get_dispatcher,
        lambda: InstanceDispatcher(instances),
    )
    app.dependency_overrides.setdefault(
        send_routes.get_max_buffered_bytes,
        lambda: settings.storage.max_buffered_bytes,
    )
    # Seam 3 - the POST /v1/send degraded-boot guard deps. Bound to the live
    # config + the same typed degraded list the lifespan folds into, so a
    # non-lifespan boot resolves real config (the set is empty until the
    # lifespan runs).
    app.dependency_overrides.setdefault(
        send_routes.get_instance_cfgs,
        lambda: settings.instances,
    )
    app.dependency_overrides.setdefault(
        send_routes.get_degraded_instances,
        lambda: tuple(degraded_boot),
    )
    # The raw-intake catch-all's second destination carrier (Phase 1 TASK
    # 1.3), bound here so a non-lifespan TestClient sees the same wiring.
    app.dependency_overrides.setdefault(
        send_routes.get_phantom_default_target,
        lambda: (
            str(settings.phantom_default_target)
            if settings.phantom_default_target is not None
            else None
        ),
    )
    # The /v1/healthz + /v1/readyz probes resolve their own placeholders
    # (distinct from the admin router's), bound here so a non-lifespan
    # TestClient sees the same wiring.
    app.dependency_overrides.setdefault(
        health_routes.get_version,
        lambda: __version__,
    )
    app.dependency_overrides.setdefault(
        health_routes.get_dispatcher,
        lambda: InstanceDispatcher(instances),
    )
    # Seam 3 - the /v1/readyz + /v1/healthz degraded-boot signal reads the
    # same typed set.
    app.dependency_overrides.setdefault(
        health_routes.get_degraded_instances,
        lambda: tuple(degraded_boot),
    )
    # Admin dep fallbacks for a non-lifespan TestClient (the lifespan
    # rebinds them with the live dispatcher).
    app.dependency_overrides.setdefault(
        admin_routes.get_dispatcher,
        lambda: InstanceDispatcher(instances),
    )
    app.dependency_overrides.setdefault(
        admin_routes.get_version,
        lambda: __version__,
    )
    return app
