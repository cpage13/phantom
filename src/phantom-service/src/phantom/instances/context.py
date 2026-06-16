"""Per-instance bundle of Protocol-typed dependencies (ADR-006)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from phantom.chain.executor import ChainExecutor
from phantom.compression.interface import BodyCodec
from phantom.config.settings import InstanceCfg
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.refresh.ad_client_credentials import AdMinter
from phantom.storage.interface import BodyStore, TokenCache, UploadStore
from phantom.strategies.interface import UploadStrategy
from phantom.transport.interface import UpstreamClient

# Per-instance on-disk artifact basenames under the instance data_root.
# Defined once here so the composition root (``app.py``) and the admin
# quarantine/restore routes derive the same paths from a single source.
_DB_BASENAME = "uploads.db"
_BODIES_BASENAME = "bodies"


@dataclass(frozen=True)
class InstanceStoragePaths:
    """The on-disk paths for one instance, derived from the data roots.

    The composition root builds the live DB + body tree under
    ``<storage.data_dir>/<cfg.data_dir>/``; the admin quarantine inventory
    and restore routes (plan § 1.4 / § 1.5) need the SAME per-instance
    paths to find quarantine artifacts and to target the restore movers.
    Centralizing the layout here keeps the basenames (:data:`uploads.db`,
    :data:`bodies`) in one place instead of duplicated across call sites.

    Attributes:
        data_root: ``<storage.data_dir>/<cfg.data_dir>`` — the instance's
            directory; quarantine artifacts are FLAT siblings here.
        db_path: ``data_root / "uploads.db"`` — the live SQLite DB.
        bodies_root: ``data_root / "bodies"`` — the live body-store root.
    """

    data_root: Path
    db_path: Path
    bodies_root: Path


def instance_storage_paths(data_dir_root: Path, cfg: InstanceCfg) -> InstanceStoragePaths:
    """Derive one instance's storage paths from the top-level data dir + cfg.

    Args:
        data_dir_root: The top-level ``Settings.storage.data_dir``.
        cfg: The instance whose paths to build (uses ``cfg.data_dir``).

    Returns:
        The :class:`InstanceStoragePaths` triple for the instance.
    """
    data_root = data_dir_root / cfg.data_dir
    return InstanceStoragePaths(
        data_root=data_root,
        db_path=data_root / _DB_BASENAME,
        bodies_root=data_root / _BODIES_BASENAME,
    )


# SaturationGate's containing package imports back into phantom.instances
# (via auth_kicker, reaper, sender), so we type-check it without importing
# at runtime to break the cycle. Consumers receive a fully constructed
# instance from the composition root. PersistController is similarly held
# behind TYPE_CHECKING because the workers package imports the runtime
# composition module which imports back into instances.
if TYPE_CHECKING:
    from phantom.storage.file_body_store import FileBodyStore
    from phantom.storage.ram_body_store import RamBodyStore
    from phantom.workers.persist_controller import PersistController
    from phantom.workers.saturation import SaturationGate


@dataclass
class InstanceContext:
    """All Protocol-typed deps for one configured instance.

    The composition root assembles one of these per :class:`InstanceCfg`
    at startup; workers consume their slice from this bundle.

    The bundle is intentionally NOT frozen. The hot-reload path
    (:func:`phantom.app.create_app` / SIGHUP / ``POST /v1/admin/reload``)
    may swap fields that participate in reload — today that is
    :attr:`cfg` (when the instance's ``InstanceCfg`` block changes) and
    :attr:`minter` (when the instance's ``ad_mint`` block changes). All
    other fields are constructed once at startup and not reassigned;
    workers must not mutate them.

    The dual ``memory_store`` / ``disk_store`` pair collapsed into the
    single :attr:`store` (one persistent SQLite). The body halves
    (:attr:`ram_body_store` / :attr:`file_body_store`) survive because
    a small set of workers (the RAM-pressure watcher's RAM-only
    metric, the disk-pressure probe's file-only metric) need the
    specific half rather than the composed :attr:`body_store`. All
    other consumers — admission, admin, sender, body-orphan janitor —
    go through :attr:`body_store` exclusively.
    """

    cfg: InstanceCfg
    store: UploadStore
    """The single persistent SQLite store (post-Phase-1 collapse).

    Owns ``uploads``, ``idempotency_index``, and ``token_cache`` tables.
    Admission writes via :meth:`UploadStore.insert_with_idempotency_claim`
    (atomic upload + idempotency-claim INSERT — H7 closure); workers
    update state machine columns; PersistController flips
    ``body_location='file'``; reaper deletes terminal rows. See plan
    § 0.5 single-writer manifest.
    """
    ram_body_store: RamBodyStore
    """RAM half of the body store. Mode-gated callers only.

    Held here so the PersistController can wire the RAM source and the
    FileBodyStore destination directly without fishing them out of a
    composed :class:`HybridBodyStore`. Most consumers should go through
    :attr:`body_store` instead.
    """
    file_body_store: FileBodyStore
    """File half of the body store. Mode-gated callers only.

    Held here for the same reason as :attr:`ram_body_store` — the
    PersistController and DiskPressureProbe need the half-specific
    surface. All other consumers go through :attr:`body_store`.
    """
    body_store: BodyStore
    """Mode-selected body store binding (plan § 2.3.18).

    :class:`HybridBodyStore` in hybrid mode (routes RAM→disk by
    RAM-presence), :class:`RamBodyStore` in all_ram,
    :class:`FileBodyStore` in all_disk. Admission, admin, sender,
    and the body-orphan janitor consume bodies through this single
    reference; only :class:`HybridBodyStore` itself knows about the
    two halves.
    """
    persist_controller: PersistController | None
    """Set in hybrid mode (plan § 2.3.11) to receive retry-linger
    enqueues from the sender's failure handler and RAM-pressure
    enqueues from :class:`RamPressureWatcher`. ``None`` in all_ram /
    all_disk where no RAM→disk migration target exists.
    """
    token_cache: TokenCache
    # AD-mint engine when ad_client_credentials is configured for this
    # instance; None otherwise (operators inject tokens via the inbound
    # ``Authorization`` header on ingress). The sender invokes
    # ``minter.on_401(...)`` only when this is not None. Reassigned by
    # the hot-reload handler when the instance's ``ad_mint`` block
    # changes (the old minter is stopped, a new one is constructed and
    # started, and this slot is updated atomically from the handler).
    minter: AdMinter | None
    retry_strategy: UploadStrategy
    """The sender's scheduling strategy for retryable failures.

    Built by the composition root from ``retry.default_strategy`` and
    REBUILT by :func:`phantom.runtime.reload.apply_reload` on every hot
    reload, mirroring the ``cfg`` repoint above, so reloaded retry
    parameters reach subsequent scheduling decisions (R5-2; ADR-013).
    """
    upstream_client: UpstreamClient
    executor: ChainExecutor
    saturation: SaturationGate
    codec_factory: Callable[[], BodyCodec]
    """Per-admission codec selector.

    The composition root binds this to select from the LIVE settings
    snapshot on each call, so a hot reload of the compression block
    applies to the next admission (R5-2; ADR-013). Unit tests reassign
    it to inject failing codecs.
    """
    # Thunk returning the live hot-reloadable settings snapshot for this
    # instance. Workers call this on each tick rather than capturing
    # references at __init__, so a hot-reload (SIGHUP / admin reload)
    # propagates without restarting the worker pool. The composition
    # root binds this to ``lambda: settings_holder.snapshot_for(cfg.id)``.
    current_settings: Callable[[], InstanceSettingsSnapshot]
