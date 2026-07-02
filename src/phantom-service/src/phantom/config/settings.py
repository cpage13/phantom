"""Pydantic Settings + YAML loader for Phantom.

The canonical YAML schema lives in plan §7. Loading is a three-step
overlay:

1. YAML file at the given path (``yaml.safe_load``).
2. Environment variables of the form ``PHANTOM_<UPPER>__<UPPER>...``
   (``env_nested_delimiter="__"``).
3. Explicit kwargs passed to :class:`Settings`.

List-typed fields (``instances``) are NOT env-overlay-able — they
live in YAML only.

Smart defaults from the host probe
----------------------------------

At startup the model validator probes machine facts (RAM total, free
disk on the data_dir filesystem, CPU count) via
:func:`phantom.config.probe.probe_machine`, feeds them into
:func:`phantom.config.defaults.compute_defaults`, and fills every
``None``-valued saturation / storage / worker-pool field from the
result. Operators pin individual values in YAML to override the
defaults; pinned values are read literally, not blended.

Fields that take their value from the probe all carry a Pydantic
default of ``None`` so the validator can tell "operator left this
unset" apart from "operator pinned to 0". Reading any such field on
an unresolved ``Settings`` (one whose validator hasn't run) is a
programmer error — the validator guarantees every probe-fillable
field is non-``None`` post-load.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]  # types-PyYAML not in workspace dev deps
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from phantom.config.defaults import compute_defaults
from phantom.config.probe import probe_machine
from phantom.models.credential import SigningService, _coerce_signing_service

logger = logging.getLogger(__name__)


# Set to ``True`` by :meth:`Settings.reload_from_yaml` when the caller opts
# out of the host probe (the SIGHUP / admin-reload path uses this to avoid
# the psutil + shutil syscalls on every reload). The ``_resolve_defaults``
# model validator checks this flag and short-circuits without touching the
# host facts when set. A ``ContextVar`` is process-local and async-safe so
# the flag does not leak across concurrent settings constructions.
_SKIP_PROBE_FLAG: ContextVar[bool] = ContextVar("_SKIP_PROBE_FLAG", default=False)


# Host strings treated as loopback for the single-listener trust boundary
# (ADR-004). The deployment is same-machine-only, so the one listener
# (intake + admin + health) defaults to loopback; the admin API rides that
# listener with no auth, so the loopback bind IS its access control. A
# non-loopback ``bind_tcp`` host is an explicit operator opt-in to expose
# the whole surface (including the UNAUTHENTICATED admin API) beyond the
# host, so the launcher warns when the resolved host is NOT in this set.
# The set is the three canonical loopback spellings: the IPv4 loopback
# address, the IPv6 loopback address, and the conventional hostname that
# resolves to loopback. It is deliberately a NAMED constant (not an inline
# literal) so any consumer of the loopback trust boundary shares ONE
# definition and cannot drift; broader CIDR membership (e.g. 127.0.0.0/8)
# is out of scope - these three exact spellings are what the docs promise.
LOOPBACK_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "::1", "localhost"})


def host_is_loopback(host: str) -> bool:
    """Return whether ``host`` is one of the named loopback spellings.

    Used by the launcher's startup warning so the loopback trust boundary is
    defined in exactly one place (:data:`LOOPBACK_HOSTS`).

    Args:
        host: The configured ``bind_tcp`` host segment.

    Returns:
        ``True`` when ``host`` is loopback (no remote-exposure warning is
        warranted); ``False`` when it is a deliberate non-loopback opt-in.
    """
    return host in LOOPBACK_HOSTS


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class TlsCfg(BaseModel):
    """``server.tls`` block — flip the single listener from HTTP to HTTPS.

    When ``enabled``, the EXISTING uvicorn listener (``__main__.py:89-93``) is
    served with TLS: ``ssl_certfile``/``ssl_keyfile`` are passed straight to
    ``uvicorn.run`` (``uvicorn/main.py:534-536``). Self-signed is fine — Phantom
    does NOT verify its own cert; clients use ``verify=off`` or pin it
    (loopback/sidecar trust model). NOT a second listener; the one socket
    simply speaks HTTPS instead of HTTP.
    """

    model_config = ConfigDict(strict=True, extra="forbid")  # file-wide idiom (settings.py:108)

    enabled: bool = Field(
        False,
        description="Serve the single listener over TLS (HTTPS) instead of plaintext HTTP.",
    )
    cert_path: str | None = Field(
        None,
        description=(
            "PEM certificate path. Operator-supplied (self-signed OK); when None and "
            "``enabled`` is set, Phantom auto-generates one at startup (see "
            ":func:`phantom.runtime.tls_cert.resolve_tls_paths`)."
        ),
    )
    key_path: str | None = Field(
        None,
        description=(
            "PEM private-key path, paired with ``cert_path``. When None and ``enabled`` "
            "is set, Phantom auto-generates the pair at startup."
        ),
    )
    key_password: str | None = Field(
        None,
        description=(
            "Optional passphrase for an encrypted private key (uvicorn ssl_keyfile_password)."
        ),
    )

    @model_validator(mode="after")
    def _reject_half_configured_paths(self) -> TlsCfg:
        """Reject the asymmetric (XOR) cert/key state; the resolver needs both-or-neither.

        Two ``enabled`` configs are valid: BOTH paths set (operator-supplied —
        :func:`phantom.runtime.tls_cert.resolve_tls_paths` validates and uses them
        verbatim) OR BOTH paths None (auto-gen — the resolver mints a self-signed
        pair). Exactly ONE set is a half-configured mistake the resolver has no
        defined branch for, so reject it here at config-load (fails in
        ``--validate``, friendlier than a later ``resolve_tls_paths`` / uvicorn
        ``load_cert_chain`` failure). When ``enabled`` is False the paths are
        inert, so the guard only applies when enabled.
        """
        if self.enabled and (self.cert_path is None) != (self.key_path is None):
            raise ValueError(
                "server.tls: set BOTH cert_path and key_path (operator-supplied), "
                "or NEITHER (auto-gen) — not exactly one"
            )
        return self


class ServerCfg(BaseModel):
    """``server`` block.

    The deployment is same-machine-only: Phantom runs on the SAME box as
    its producer and is reached over loopback, so ONE listener serves intake +
    admin + health on one socket. ``bind_tcp`` (default ``127.0.0.1:8080`` -
    loopback) or ``bind_uds`` configures it. The loopback default bind IS
    the admin access control (ADR-004); an operator who wants network
    reachability sets ``bind_tcp`` explicitly (e.g. ``0.0.0.0:8080``) and
    gets the unauthenticated-exposure warning at startup.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    bind_tcp: str = Field(
        "127.0.0.1:8080",
        description=(
            "TCP listen address as ``host:port`` (loopback by default; the "
            "admin API rides this listener, so a non-loopback host exposes "
            "the unauthenticated admin API - ADR-004)."
        ),
    )
    bind_uds: str | None = Field(
        None,
        description="Optional Unix-domain-socket path (alternative to TCP).",
    )
    tls: TlsCfg = Field(
        default_factory=TlsCfg,  # type: ignore[arg-type]
        description="See :class:`TlsCfg` — flip the single listener to HTTPS (off by default).",
    )


class PersistTriggerCfg(BaseModel):
    """Operator-facing size-based persist threshold (narrow scope post-Phase-1).

    Phase 1 narrowed this class from its pre-refactor shape (which carried
    ``after_attempts`` + ``after_seconds`` knobs) to a single field. The
    persist controller now owns retry-linger detection (time-based) and
    the body store's mode owns deployment-wide RAM/disk policy; the only
    surviving operator knob is the size-aware bypass, which forces
    admission to enqueue against the persist controller (hybrid mode only)
    for bodies above the threshold.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    body_size_threshold_bytes: int | None = Field(
        None,
        ge=0,
        description=(
            "In hybrid mode, admission enqueues against the persist "
            "controller immediately (skipping linger) for bodies whose "
            "stored size (post-codec) is at or above this threshold. "
            "Ignored in all_ram + all_disk modes (no RAM-to-disk migration "
            "in either). None means use the host-probe-derived default; "
            "explicit int overrides; pin to 0 to disable size-aware "
            "persistence entirely."
        ),
    )


class BodyStoreCfg(BaseModel):
    """BodyStore deployment-mode configuration (Phase 1).

    Encapsulates the three production deployment modes (`hybrid`,
    `all_ram`, `all_disk`) plus the RAM-tier ceiling + persist-controller
    cadence knobs. See plan § 0.4 glossary for the per-mode semantics.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    mode: Literal["hybrid", "all_ram", "all_disk"] = Field(
        "hybrid",
        description=(
            "BodyStore deployment mode. All three are first-class "
            "production modes (per strategy §3). hybrid: RAM-first "
            "writes + persist controller migrates on linger or "
            "RAM-pressure (production default). all_ram: RAM-only; "
            "rows + bodies are lost on restart by design (recovery "
            "quarantines). all_disk: every admission writes directly "
            "to disk; no PersistController spawned. See plan § 2.3.10 "
            "per-mode wiring and Phase 6 operator playbook for "
            "mode-selection guidance."
        ),
    )
    ram_ceiling_bytes: int | None = Field(
        None,
        ge=0,
        description=(
            "RamBodyStore size ceiling in bytes. None means probe-fill "
            "at startup — the ``_resolve_defaults`` model validator "
            "sets this to a hardware-aware fraction of available RAM "
            "(preserving the pre-rename ``in_memory_max_bytes`` "
            "probe-fill semantic). Explicit int overrides (e.g., "
            "536_870_912 / 512 MiB). Ignored in all_disk mode (no RAM "
            "to bound)."
        ),
    )
    ram_pressure_poll_seconds: float = Field(
        1.0,
        gt=0.0,
        description=("Cadence at which the ram_pressure watcher polls RamBodyStore.total_bytes()."),
    )
    linger_seconds: int = Field(
        90,
        ge=0,
        description=(
            "Persist controller retry-linger threshold (strategy §2). "
            "A body in RAM with no upstream success past this threshold "
            "migrates to disk."
        ),
    )
    body_orphan_sweep_seconds: int = Field(
        3600,
        ge=1,
        description="Body-orphan janitor sweep cadence (seconds).",
    )
    invariant_audit_period_seconds: int = Field(
        300,
        ge=1,
        description=(
            "InvariantAuditor sweep cadence (seconds). Plan § 4.2.3 — "
            "walks live rows and counters invariant violations against "
            "the seven §0.5 invariants. Low frequency by design; CI "
            "uses the resulting counter to fail the next build on any "
            "non-zero label."
        ),
    )


class CompressionCfg(BaseModel):
    """Body-compression knobs (req §5d).

    Phantom always encodes per the configured ``algorithm``; there is
    no size-based short-circuit and no inbound ``Content-Encoding``
    pass-through. The deployment selects one codec per Phantom process
    via ``algorithm``; the wire-token ``"original"`` requests the
    :class:`PassthroughCodec` (identity codec) explicitly. The
    transparent-proxy invariant is enforced by the sender's decode
    path plus body_hash verification, not by storage-side
    pass-through.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    mode: Literal["always"] = Field(
        "always",
        description=(
            "Encoding policy. Only ``always`` is supported — Phantom "
            "always applies the configured codec."
        ),
    )
    algorithm: Literal["original", "zstd", "gzip"] = Field(
        "zstd",
        description=(
            "Storage codec algorithm. ``original`` selects the "
            ":class:`PassthroughCodec` (identity)."
        ),
    )
    level: int = Field(
        3,
        ge=1,
        description="Codec compression level (zstd 1..22 / gzip 1..9).",
    )


class SqliteCfg(BaseModel):
    """SQLite-specific knobs.

    Phase 1: dropped the configurable ``autovacuum`` knob — ``auto_vacuum``
    is hardcoded to NONE in code per the SD-card-wear hard rule
    (plan § 0.3). The previously free-form ``synchronous`` and
    ``journal_mode`` fields are narrowed to ``Literal`` types so an
    operator typo or unsupported mode is a Pydantic error at startup
    rather than a silent misconfig at runtime.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    vacuum_cron: str = Field(
        "0 3 * * 0",
        description="Cron string for the VACUUM scheduler (Sunday 03:00 default).",
    )
    journal_mode: Literal["WAL"] = Field(
        "WAL",
        description=(
            "SQLite journal mode. Hardcoded to WAL — load-bearing for "
            "the strategy pragma set (plan § 2.3.5) and the recovery "
            "sweep's assumptions."
        ),
    )
    synchronous: Literal["NORMAL", "FULL", "EXTRA"] = Field(
        "NORMAL",
        description=(
            "SQLite synchronous level. Default changed from FULL to "
            "NORMAL (per T1 research): corruption-safe under WAL on "
            "consumer flash; loses at most the last few seconds of "
            "commits on hard power-cut. Rack-server deployments with "
            "battery-backed write cache may pin FULL. Operator playbook "
            "(Phase 6 § 7.2.10) documents the trade-off."
        ),
    )
    journal_size_limit_bytes: int = Field(
        16_777_216,
        ge=0,
        description=(
            "WAL journal size limit in bytes (T2 research recommendation). Default 16 MiB."
        ),
    )
    busy_timeout_ms: int = Field(
        1000,
        ge=0,
        description=(
            "SQLite ``busy_timeout`` in milliseconds — how long SQLite busy-WAITS "
            "in its connection worker thread for a contended write lock before "
            "raising SQLITE_BUSY (finding R9-V6-1). Phantom serializes EVERY "
            "writer through one ``asyncio`` lock on a single connection, so "
            "internal write-vs-write contention is impossible by construction; "
            "this timeout ONLY governs EXTERNAL cross-process contention (a stray "
            "``sqlite3`` admin session, a backup tool, a mis-shared data_dir). "
            "Under such a hold a LARGE value is harmful: each contended writer "
            "monopolizes the single writer slot for the full window, so an "
            "admission burst queues serially behind multiple busy-waits and the "
            "producer's HTTP read times out before admission can return its clean "
            "``503 storage_unavailable``. The 1000 ms default rides out sub-second "
            "external blips while failing FAST under a sustained external hold so "
            "the contended write returns a clean retryable signal quickly. "
            "Boot-time recovery rides out longer locks via its own bounded "
            "retry-with-backoff, independent of this value."
        ),
    )


class DbIntegrityCfg(BaseModel):
    """Phase 4 DB-corruption resilience knobs (plan § 5.2.5).

    Two surfaces:

    * **Cold-backup (plan § 5.2.6)**. Off by default; opt-in for
      belt-and-suspenders deployments. When enabled, the
      :class:`phantom.workers.cold_backup.ColdBackupScheduler` is spawned
      under the composition root's TaskGroup and snapshots the live DB
      to ``<data_dir>/backups/`` on a configurable cadence.
    * **Fail-open vs fail-closed**. Strategy §3 commits to
      ``fail_open=True`` ("service comes up serving empty state after
      quarantine"). Operators willing to abort startup on detected
      corruption can pin ``fail_open=False`` — the composition root then
      logs ERROR and raises after quarantining (the artifacts still get
      preserved for retrieval).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    fail_open: bool = Field(
        default=True,
        description=(
            "Default True per strategy §3 — the service starts with a "
            "fresh empty SQLite after quarantine. Pin False to abort "
            "startup on detected corruption (the quarantine still "
            "fires; the process exits after)."
        ),
    )
    backup_enabled: bool = Field(
        default=False,
        description=(
            "Enable periodic SQLite online-backup snapshots. Off by "
            "default — opt-in for belt-and-suspenders deployments."
        ),
    )
    backup_period_seconds: int = Field(
        default=86_400,  # daily
        ge=1,
        description=(
            "Cadence at which a backup snapshot is taken when "
            "backup_enabled is True. Default 86400 (daily)."
        ),
    )
    backup_rotate_n: int = Field(
        default=3,
        ge=1,
        description=("Number of backup snapshots to keep before rotation. Default 3."),
    )


class StorageCfg(BaseModel):
    """``storage`` block.

    Phase 1: removed ``default_tier`` (subsumed by ``body_store.mode``)
    and ``in_memory_max_bytes`` (renamed to ``body_store.ram_ceiling_bytes``,
    same semantic). The new ``body_store`` nested model owns the
    deployment-mode + RAM-tier ceiling + persist-controller cadence
    knobs.

    Phase 4: added ``db_integrity`` carrying the corruption-resilience
    settings (fail-open default + cold-backup knobs).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    data_dir: str = Field(
        "/var/lib/phantom",
        description="Root of the persisted-tier filesystem layout.",
    )
    max_buffered_bytes: int = Field(
        2_147_483_648,  # 2 GiB
        ge=0,
        description="Per-upload stream-pass-through threshold (req §5).",
    )
    # ``default_factory=<BaseModel subclass>`` is the idiomatic Pydantic way to
    # build a nested-default; without the Pydantic mypy plugin (which is
    # incompatible with mypy 2.0 in this workspace), mypy can't bind the
    # callable's return type against the field's declared type and emits
    # `arg-type`. The runtime behavior is correct.
    body_store: BodyStoreCfg = Field(
        default_factory=BodyStoreCfg,  # type: ignore[arg-type]
        description="See :class:`BodyStoreCfg`.",
    )
    persist_trigger: PersistTriggerCfg = Field(
        default_factory=PersistTriggerCfg,  # type: ignore[arg-type]
        description="See :class:`PersistTriggerCfg`.",
    )
    shard_prefix_chars: int = Field(
        2,
        ge=0,
        description="Number of leading chain_id hex chars used as a directory shard.",
    )
    compression: CompressionCfg = Field(
        default_factory=CompressionCfg,  # type: ignore[arg-type]
        description="See :class:`CompressionCfg`.",
    )
    sqlite: SqliteCfg = Field(
        default_factory=SqliteCfg,  # type: ignore[arg-type]
        description="See :class:`SqliteCfg`.",
    )
    db_integrity: DbIntegrityCfg = Field(
        default_factory=DbIntegrityCfg,
        description="See :class:`DbIntegrityCfg` — Phase 4 corruption-resilience knobs.",
    )


class SaturationCfg(BaseModel):
    """Saturation caps applied at ingress.

    Every numeric cap defaults to ``None`` so the model validator can
    fill it from the host probe (see :func:`Settings._resolve_defaults`).
    Operators pin individual caps in YAML to override the default; the
    override is read literally.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    max_in_flight: int | None = Field(
        None,
        ge=0,
        description=(
            "Row-count cap across all in-flight uploads. Filled from the "
            "host probe when left unset (None). A value of 0 refuses "
            "every admission (no in-flight rows permitted)."
        ),
    )
    max_in_flight_bytes: int | None = Field(
        None,
        ge=0,
        description=(
            "Byte cap across all in-flight uploads. Filled from the host "
            "probe when left unset (None). A value of 0 refuses every "
            "admission — including 0-byte bodies — matching the "
            "zero-semantics of max_in_flight (0 == refuse-all). It is NOT "
            "an 'unlimited' sentinel; leave the field unset (None) to take "
            "the probe-derived default."
        ),
    )
    max_disk_bytes: int | None = Field(
        None,
        ge=0,
        description=(
            "Hard ceiling on persisted-tier disk usage. Filled from the "
            "host probe (80% of free disk at probe time) when left unset "
            "(None)."
        ),
    )
    large_body_threshold_bytes: int | None = Field(
        None,
        ge=0,
        description=(
            "Bodies at or above this size count against the separate "
            "``max_large_in_flight`` cap as well as the regular bytes/count "
            "caps. Lets operators bound how many big bodies are concurrent "
            "without throttling small-body ingress that shares the same "
            "instance. Set to 0 to disable the large-body class entirely. "
            "Filled from the host probe when left unset (None)."
        ),
    )
    max_large_in_flight: int | None = Field(
        None,
        ge=0,
        description=(
            "Max concurrent in-flight bodies of size "
            ">= ``large_body_threshold_bytes``. Set to 0 to refuse all "
            "large bodies (effectively an off-switch for the class). "
            "Filled from the host probe when left unset (None)."
        ),
    )


class RetentionCfg(BaseModel):
    """Per-terminal-state retention windows (req §5b post-ADR-009).

    All seconds; ``-1`` means "never expire automatically" and is the
    ONLY sentinel: every window field is validated ``ge=-1`` (C11,
    round-10 hardening), so an operator typo like ``-5`` is a loud
    validation error instead of silently meaning "forever" (the
    reaper's ``>= 0`` sweep guard treats any negative as never-expire,
    which fails safe but hides the typo).

    Every per-state body / metadata window defaults to a hard-coded
    sensible value — these knobs are tunable but the probe doesn't
    influence them.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    succeeded_metadata_seconds: int = Field(
        180,
        ge=-1,
        description=(
            "Captured values queryable post-delivery (ADR-009). Default 180 s "
            "(3 minutes) so a captured upstream file id stays queryable just "
            "long enough for a caller routine to correlate."
        ),
    )
    succeeded_body_seconds: int = Field(
        0, ge=-1, description="Body released immediately on success."
    )
    failed_metadata_seconds: int = Field(2_592_000, ge=-1, description="30 days.")
    failed_body_seconds: int = Field(
        2_592_000,
        ge=-1,
        description="Body retention after terminal upstream failure (default 30 days).",
    )
    cancelled_metadata_seconds: int = Field(604_800, ge=-1, description="7 days.")
    cancelled_body_seconds: int = Field(
        604_800,
        ge=-1,
        description="Body retention after admin cancellation (default 7 days).",
    )
    corrupted_metadata_seconds: int = Field(
        2_592_000,
        ge=-1,
        description=(
            "Retention for ``corrupted`` rows (body-verification failure). "
            "Default 30 days, matching ``failed``."
        ),
    )
    corrupted_body_seconds: int = Field(
        2_592_000,
        ge=-1,
        description=(
            "Body retention for ``corrupted`` rows. Default 30 days; "
            "operators may want to inspect the bad bytes before sweep."
        ),
    )
    stored_metadata_seconds: int = Field(-1, ge=-1, description="Forever.")
    stored_body_seconds: int = Field(
        15_552_000,
        ge=-1,
        description="Body retention for the ``stored`` terminal state (default 6 months).",
    )
    auth_expired_metadata_seconds: int = Field(-1, ge=-1, description="Forever (holding state).")
    auth_expired_body_seconds: int = Field(
        15_552_000,
        ge=-1,
        description="Body retention while parked in ``auth_expired`` (default 6 months).",
    )
    expired_metadata_seconds: int = Field(
        2_592_000,  # 30 days — mirrors failed_metadata_seconds (a terminal give-up to audit).
        ge=-1,
        description=(
            "Metadata retention after a row gives up at the send-deadline "
            "(``expired``, ADR-032). 30 days (mirrors ``failed_metadata_seconds``)."
        ),
    )
    expired_body_seconds: int = Field(
        0,
        ge=-1,
        description=(
            "Body retention for ``expired`` (0: the body is already discarded at "
            "the transition to ``expired``, so there is nothing to retain)."
        ),
    )
    reaper_interval_seconds: int = Field(
        60,
        ge=1,
        description="Reaper sweep cadence (seconds). Default 60.",
    )
    max_rows: int = Field(
        100_000,
        ge=-1,
        description=(
            "Optional row-count backstop on the ``uploads`` table (V3). The "
            "per-state windows above are purely TIME-based; ``max_in_flight`` "
            "bounds only in-flight rows, so under sustained ingest terminal rows "
            "can grow the table without bound between reaps (notably the "
            "forever-retained ``stored``/``auth_expired`` metadata). When "
            "``max_rows >= 0`` the reaper enforces it as a hard cap AFTER its "
            "time-based passes: if the table exceeds ``max_rows`` it evicts "
            "oldest-DONE-first (by ``updated_at``) until at or below the cap. "
            "Only fully-terminal rows (succeeded/failed/cancelled/stored/"
            "corrupted) are eligible — in-flight (queued/attempting) and "
            "still-deliverable ``auth_expired`` rows are NEVER evicted, so the "
            "backstop cannot drop an undelivered upload (invariant #1 holds). "
            "Default ``100_000``: a safe row backstop sized for the smallest "
            "target producer, so an instance that never sets a value still cannot "
            "grow the table without bound. Set ``-1`` to opt into unbounded "
            "(time-only retention); tune the cap per the SD-card size on the producer."
        ),
    )


class RetryStrategyCfg(BaseModel):
    """Configuration for the default retry strategy."""

    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["exponential_backoff", "fixed_intervals"] = Field(
        "exponential_backoff",
        description="Retry-strategy plugin name.",
    )
    base_seconds: float = Field(5.0, ge=0.0, description="Exponential-backoff base.")
    factor: float = Field(4.0, gt=0.0, description="Exponential-backoff factor.")
    cap_seconds: float = Field(1800.0, ge=0.0, description="Exponential-backoff cap.")
    jitter: float = Field(
        0.2,
        ge=0.0,
        description=(
            "Uniform jitter fraction (e.g. 0.2 → ±20%). Applied by BOTH "
            "``exponential_backoff`` AND ``fixed_intervals`` (V5-C) so a backlog "
            "that fails in the same poll round de-correlates on retry instead of "
            "firing in lockstep (thundering herd) on upstream recovery. ``0.0`` "
            "preserves an exact fixed-interval schedule."
        ),
    )
    max_attempts: int = Field(
        -1,
        description="-1 = unbounded.",
    )
    max_duration_seconds: int = Field(
        86_400,
        ge=0,
        description=(
            "Hard ceiling on retry-attempt span before the row transitions to "
            "``stored``. Default 86 400 s (24 hours)."
        ),
    )
    intervals_seconds: list[int] = Field(
        default_factory=list,
        description="Fixed-intervals schedule (when ``type == 'fixed_intervals'``).",
    )


class RetryCfg(BaseModel):
    """``retry`` block — worker pool size and the default strategy."""

    model_config = ConfigDict(strict=True, extra="forbid")

    worker_count: int | None = Field(
        None,
        ge=1,
        description=(
            "Number of sender worker coroutines. Filled from the host probe "
            "(min(8, max(2, cpu_count))) when left unset (None)."
        ),
    )
    poll_interval_ms: int = Field(
        250,
        ge=10,
        description="Sender-worker poll cadence (milliseconds). Default 250.",
    )
    default_strategy: RetryStrategyCfg = Field(
        default_factory=RetryStrategyCfg,  # type: ignore[arg-type]
        description="The default retry-strategy block applied per upload row.",
    )


class ObservabilityCfg(BaseModel):
    """``observability`` block."""

    model_config = ConfigDict(strict=True, extra="forbid")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        "INFO",
        description="Root logger level.",
    )
    log_to_stdout: bool = Field(
        True,
        description="Stream log records to stdout (default true).",
    )
    log_to_file: str | None = Field(
        None,
        description="Optional path of a secondary log-file sink. None disables file output.",
    )


# ---------------------------------------------------------------------------
# Routes + instances
# ---------------------------------------------------------------------------


class RouteCfg(BaseModel):
    """One route policy within an instance."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., min_length=1, description="Route name (logical handle).")
    hosts: list[str] = Field(
        ...,
        min_length=1,
        description="fnmatch host patterns this route matches.",
    )
    auth_mode: Literal["phantom_bearer", "none", "aws_sigv4"] = Field(
        ...,
        description=(
            "How Phantom authenticates the outbound request on this route. "
            "``phantom_bearer`` injects a cached Authorization bearer; "
            "``aws_sigv4`` re-signs the request with botocore SigV4 using a "
            "host-keyed credential from the CredentialStore; ``none`` forwards "
            "as-is."
        ),
    )
    timeout_seconds: float | None = Field(
        None,
        gt=0.0,
        description=(
            "Optional per-route HTTP request timeout in seconds. None means "
            "use the global default (HttpxUpstreamClient.timeout_seconds, "
            "30 s). Useful when S3 PUTs need a much longer ceiling (e.g. "
            "600 for 10 min on big tars) than fast metadata POSTs. Plumbs "
            "through ResolvedRoute → UpstreamRequest → httpx per-call kwarg."
        ),
    )
    send_deadline_seconds: int | None = Field(
        None,
        ge=1,
        description=(
            "Max wall-clock seconds a buffered upload to this route may keep "
            "trying before Phantom gives up and marks it ``expired`` (terminal, "
            "ADR-032: body released, never re-admitted). Measured from "
            "``received_at`` (admission). None = no deadline (retry per the "
            "strategy forever). Restart-required (routes do not hot-reload)."
        ),
    )


class SigV4CredentialCfg(BaseModel):
    """One config-declared destination credential for the ``aws_sigv4`` signer.

    This is the **config acquisition route** — the boot-time analogue of the
    runtime admin push (``PUT /v1/admin/credentials/{dest_host}``). It is the
    ONE place env-var *names* are accepted (GLOBAL §1.2(a) B1), mirroring
    :class:`~phantom.config.ad_mint.AdMintConfig`'s ``primary_client_secret_env``
    / ``secondary_client_secret_env`` (which likewise hold the *name* of an env
    var, never the secret literal). The secret access key NEVER appears in
    config: only the NAME of the environment variable that holds it. At boot
    (``app.py`` lifespan, ``_build_instance_context``) the named env vars are
    resolved to their literal values and a
    :class:`~phantom.models.credential.SigV4StaticCreds` (or
    :class:`~phantom.models.credential.ProfileRefCred`) of RESOLVED values is
    written into the host-keyed credential store under ``source="config"``. By
    the time the store holds anything, the env-var names are gone (the B1
    invariant: resolved values in the store, names only on this config route).

    Two arms, discriminated by ``kind`` (mirroring the admin push body's
    discriminated union):

    * ``sigv4_static`` — a static key-pair declared by env-var name.
      ``access_key_id_env`` and ``secret_access_key_env`` are REQUIRED;
      ``region`` is REQUIRED; ``session_token_env`` is optional (STS).
    * ``profile_ref`` — a botocore profile / default-chain reference. No env
      vars and no secret are held at rest; the signer resolves credentials at
      sign time. ``profile=None`` marks the default chain.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    dest_host: str = Field(
        ...,
        min_length=1,
        description=(
            "Destination host this credential is keyed on. Normalized through "
            "the same ``_hostname`` helper the executor uses for its "
            "forward-time lookup, so the config key equals the lookup key by "
            "construction."
        ),
    )
    kind: Literal["sigv4_static", "profile_ref"] = Field(
        ...,
        description="The credential arm (NEVER carries a resolved secret).",
    )
    # static arm (kind == "sigv4_static"): env-var NAMES, mirroring
    # ad_mint.primary_client_secret_env (config/ad_mint.py:28).
    access_key_id_env: str | None = Field(
        None,
        min_length=1,
        description="Name of the env var holding the access key id (static arm).",
    )
    secret_access_key_env: str | None = Field(
        None,
        min_length=1,
        description=(
            "Name of the env var holding the secret access key (static arm). "
            "The secret LITERAL never appears in config (ADR-004 / B1)."
        ),
    )
    session_token_env: str | None = Field(
        None,
        min_length=1,
        description=(
            "Optional name of the env var holding the STS session token "
            "(static arm). None means a long-lived key-pair."
        ),
    )
    region: str | None = Field(
        None,
        min_length=1,
        description="AWS region (static arm — required there; ignored for profile_ref).",
    )
    # profile-ref arm (kind == "profile_ref").
    profile: str | None = Field(
        None,
        min_length=1,
        description="AWS profile name (profile_ref arm). None means the default chain.",
    )
    # Scope state for BOTH arms (NOT arm-specific, NOT an env-var-resolved
    # secret): the AWS service the signer signs for, carried onto the
    # materialized credential. Required — strict mode rejects the bare wire
    # string without the before-validator.
    service: SigningService = Field(
        ..., description="AWS service this credential signs for (both arms)."
    )

    @field_validator("service", mode="before")
    @classmethod
    def _validate_service(cls, v: object) -> SigningService:
        """Coerce the wire ``service`` string to :class:`SigningService` (strict mode)."""
        return _coerce_signing_service(v)

    @model_validator(mode="after")
    def _check_arm_fields(self) -> SigV4CredentialCfg:
        """Enforce the per-``kind`` required-field set at settings-load.

        A ``sigv4_static`` arm MUST name both key env vars and a region; a
        ``profile_ref`` arm MUST NOT carry any static field. ``service`` is
        REQUIRED on BOTH arms and is enforced by its non-optional typed field
        (the field validator coerces the wire string; a missing/unknown
        ``service`` already fails at field validation, so it needs no entry in
        the per-arm sets here). Catching the shape error here (loud Pydantic
        ``ValidationError``) keeps the boot materializer's static-arm
        assumptions total — by the time the loop runs, a ``sigv4_static`` entry
        is guaranteed to carry every name it will resolve. The boot loop still
        owns the orthogonal failure (a NAMED env var that is absent or empty at
        runtime).

        Returns:
            ``self`` when the arm's field set is valid.

        Raises:
            ValueError: When a required field for ``kind`` is missing, or a
                field belonging to the other arm is present.
        """
        if self.kind == "sigv4_static":
            missing = [
                name
                for name, value in (
                    ("access_key_id_env", self.access_key_id_env),
                    ("secret_access_key_env", self.secret_access_key_env),
                    ("region", self.region),
                )
                if value is None
            ]
            if missing:
                raise ValueError(
                    f"sigv4_credentials entry for dest_host={self.dest_host!r} has "
                    f"kind='sigv4_static' but is missing required field(s): "
                    f"{', '.join(missing)}."
                )
            if self.profile is not None:
                raise ValueError(
                    f"sigv4_credentials entry for dest_host={self.dest_host!r} has "
                    f"kind='sigv4_static' but also sets 'profile' (a profile_ref "
                    f"field). Pick one arm."
                )
        else:  # kind == "profile_ref"
            stray = [
                name
                for name, value in (
                    ("access_key_id_env", self.access_key_id_env),
                    ("secret_access_key_env", self.secret_access_key_env),
                    ("session_token_env", self.session_token_env),
                    ("region", self.region),
                )
                if value is not None
            ]
            if stray:
                raise ValueError(
                    f"sigv4_credentials entry for dest_host={self.dest_host!r} has "
                    f"kind='profile_ref' but also sets static-arm field(s): "
                    f"{', '.join(stray)}. Pick one arm."
                )
        return self


# AdMintConfig lives in a sibling module to keep this file focused.
# It's imported lazily in :class:`InstanceCfg` to avoid a circular
# import (config/ad_mint.py has no upward deps).
from phantom.config.ad_mint import AdMintConfig  # noqa: E402


class AdminLookupCfg(BaseModel):
    """Per-instance binding for the by-captured-id admin lookup (cycle-7 task 4.3).

    ``GET /v1/admin/uploads/by-captured-id/{captured_id}`` takes NO query
    parameters: where the upstream-assigned identifier lives inside a
    row's captured values is upstream-specific, and Phantom stays
    upstream-ignorant, so the binding is DEPLOYMENT-SUPPLIED through this
    block. An instance without the block answers that endpoint with a
    ``lookup_not_configured`` error envelope (HTTP 400); it never guesses
    a path.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    capture_name: str = Field(
        ...,
        min_length=1,
        description=(
            "The capturing step's key under the row's captured-values "
            "'steps' map (the step name the deployment's envelope uses "
            "for the capture that contains the upstream identifier)."
        ),
    )
    json_path: str = Field(
        ...,
        min_length=1,
        description=(
            "Dotted path under that step's 'values' map down to the "
            "upstream identifier field (capture name first, then keys "
            "inside the captured object)."
        ),
    )


class InstanceCfg(BaseModel):
    """One configured instance (ADR-006)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(..., min_length=1, description="Stable instance id.")
    host_prefixes: list[str] = Field(
        ...,
        min_length=1,
        description=("fnmatch host patterns this instance owns. First-match in declaration order."),
    )
    data_dir: str = Field(
        ...,
        min_length=1,
        description="Subdirectory under storage.data_dir for this instance.",
    )
    capture_reexecution: bool = Field(
        False,
        description="ADR-011 knob — re-execute on capture-TTL expiry.",
    )
    routes: list[RouteCfg] = Field(
        default_factory=list,
        description=(
            "Per-instance routes — declaration order is precedence. Catch-all routes go last."
        ),
    )
    ad_mint: AdMintConfig | None = Field(
        None,
        description=(
            "Per-instance autonomous AD client-credentials mint configuration. "
            "When set, Phantom mints AD tokens proactively via its own app "
            "registration and writes them to the (endpoint, uid) token cache. "
            "When None (default), Phantom waits for the client to push tokens "
            "via the Authorization header on ingress. See ADR-001."
        ),
    )
    admin_lookup: AdminLookupCfg | None = Field(
        None,
        description=(
            "Optional binding for the by-captured-id admin lookup. When "
            "None (default), GET /v1/admin/uploads/by-captured-id on this "
            "instance returns a lookup_not_configured error envelope "
            "(HTTP 400). Deployment-supplied so the service stays "
            "upstream-ignorant."
        ),
    )


# ---------------------------------------------------------------------------
# Top-level Settings
# ---------------------------------------------------------------------------


class SettingsError(ValueError):
    """Raised when settings loading or env-overlay fails."""


class Settings(BaseSettings):
    """Top-level Phantom settings.

    Source precedence (lowest first):

    * YAML file passed to :func:`load_settings`.
    * Environment variables ``PHANTOM_<UPPER>__<UPPER>...``.
    * Explicit kwargs.

    After the model validator runs every probe-fillable knob is
    guaranteed non-``None``. Callers that touch the saturation /
    storage / worker-pool knobs without going through
    :func:`load_settings` (e.g., raw ``Settings()`` construction in
    tests) get the same guarantee because the validator runs as part
    of construction.
    """

    model_config = SettingsConfigDict(
        env_prefix="PHANTOM_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="forbid",
        strict=False,
    )

    server: ServerCfg = Field(
        default_factory=ServerCfg,  # type: ignore[arg-type]
        description="HTTP server bind, ingress, and admin configuration.",
    )
    storage: StorageCfg = Field(
        default_factory=StorageCfg,  # type: ignore[arg-type]
        description="Two-tier storage (RAM + disk), data directory, codec selection.",
    )
    saturation: SaturationCfg = Field(
        default_factory=SaturationCfg,  # type: ignore[arg-type]
        description="In-flight caps applied at ingress (workers/saturation.py).",
    )
    retention: RetentionCfg = Field(
        default_factory=RetentionCfg,  # type: ignore[arg-type]
        description="Per-terminal-state retention windows (reaped by workers/reaper.py).",
    )
    retry: RetryCfg = Field(
        default_factory=RetryCfg,  # type: ignore[arg-type]
        description="Sender worker pool size, poll cadence, and the default retry strategy.",
    )
    observability: ObservabilityCfg = Field(
        default_factory=ObservabilityCfg,  # type: ignore[arg-type]
        description="Logging configuration (no metrics surface beyond structured logs).",
    )

    instances: list[InstanceCfg] = Field(
        default_factory=list,
        description="Per-instance configuration; each instance owns one storage partition.",
    )

    phantom_default_target: HttpUrl | None = Field(
        None,
        description=(
            "Optional single-upstream default destination for the raw-intake "
            "catch-all route (Phase 1). When a stock S3-style request arrives "
            "on ``PUT /{bucket}/{key}`` (no ``?phantom=`` carrier), the "
            "raw->envelope adapter rewrites the synthesized step URL to "
            "``{phantom_default_target}/{bucket}/{key}`` so the destination is "
            "a REAL upstream host, never Phantom's own bind host (which would "
            "loop). ``None`` means no default is configured: a raw request "
            "with no ``?phantom=`` carrier is rejected 421 ``invalid_target`` "
            "before any durable write. Restart-required (not hot-reloaded); "
            "env-overridable as ``PHANTOM_PHANTOM_DEFAULT_TARGET``. The "
            "explicit ``?phantom=<full-url>`` query carrier always wins over "
            "this default (first-hit precedence)."
        ),
    )

    sigv4_credentials: list[SigV4CredentialCfg] = Field(
        default_factory=list,
        description=(
            "Config-declared destination credentials for the ``aws_sigv4`` "
            "signer (the boot-time analogue of the runtime admin push). Each "
            "entry names the env var holding its secret access key (never the "
            "secret literal — GLOBAL §1.2(a) B1 / ADR-004). At boot the named "
            "env vars are resolved to literals and materialized into EVERY "
            "instance's host-keyed credential store under ``source='config'`` "
            "(``app.py`` lifespan). The credentials key on the globally-"
            "meaningful destination host, so this map is top-level rather than "
            "per-instance. A missing named env var is a fail-fast boot error. "
            "Each entry also declares a REQUIRED ``service`` (the AWS service "
            "the signer signs for, e.g. ``s3``); a missing or unknown service "
            "is a loud ``ValidationError`` at settings-load. Empty (the "
            "default) is a no-op: a bearer-only deployment declares nothing "
            "here. Restart-required (not hot-reloaded)."
        ),
    )

    @model_validator(mode="after")
    def _resolve_defaults(self) -> Settings:
        """Fill probe-driven ``None`` fields at startup.

        The validator fills every field that is still ``None`` after
        YAML + env overlay. Operator-supplied (non-``None``) values
        always win; the probe only fills the holes. The check is
        per-field so a partial overlay (e.g. ``saturation.max_in_flight``
        set but ``max_in_flight_bytes`` left ``None``) gets every
        remaining hole filled, not skipped.

        Hot reload path (:meth:`reload_from_yaml`): when called with
        ``skip_probe=True``, the validator's probe is bypassed via a
        process-local flag and any holes in the reloaded Settings stay
        ``None``. The caller is responsible for ensuring those holes are
        not load-bearing for the swap (the only safe use is to reload a
        config whose probe-fillable fields are pinned in YAML).
        """
        # Short-circuit when every probe-fillable field is already
        # populated. Without this guard the probe runs twice per Settings
        # rebuild (pydantic-settings reconstructs after env overlay).
        if (
            self.saturation.max_in_flight is not None
            and self.saturation.max_in_flight_bytes is not None
            and self.saturation.max_disk_bytes is not None
            and self.saturation.large_body_threshold_bytes is not None
            and self.saturation.max_large_in_flight is not None
            and self.storage.body_store.ram_ceiling_bytes is not None
            and self.storage.persist_trigger.body_size_threshold_bytes is not None
            and self.retry.worker_count is not None
        ):
            return self

        # Hot-reload skip-probe flag (set by ``reload_from_yaml``).
        if _SKIP_PROBE_FLAG.get():
            return self

        facts = probe_machine(self.storage.data_dir)
        defaults = compute_defaults(facts)

        # Saturation block.
        if self.saturation.max_in_flight is None:
            self.saturation.max_in_flight = defaults.max_in_flight
        if self.saturation.max_in_flight_bytes is None:
            self.saturation.max_in_flight_bytes = defaults.max_in_flight_bytes
        if self.saturation.max_disk_bytes is None:
            self.saturation.max_disk_bytes = defaults.max_disk_bytes
        if self.saturation.large_body_threshold_bytes is None:
            self.saturation.large_body_threshold_bytes = defaults.large_body_threshold_bytes
        if self.saturation.max_large_in_flight is None:
            self.saturation.max_large_in_flight = defaults.max_large_in_flight

        # Storage block.
        if self.storage.body_store.ram_ceiling_bytes is None:
            # RAM-tier ceiling mirrors in-flight bytes (the cap binds the
            # same physical resource — bytes in the RAM body store).
            # This preserves the pre-rename in_memory_max_bytes
            # probe-fill semantic per plan § 2.3.1.
            self.storage.body_store.ram_ceiling_bytes = defaults.max_in_flight_bytes
        if self.storage.persist_trigger.body_size_threshold_bytes is None:
            self.storage.persist_trigger.body_size_threshold_bytes = (
                defaults.body_size_threshold_bytes
            )

        # Retry block.
        if self.retry.worker_count is None:
            self.retry.worker_count = defaults.worker_count

        return self

    @classmethod
    def reload_from_yaml(cls, path: Path, *, skip_probe: bool = False) -> Settings:
        """Re-read the YAML at ``path`` and produce a new validated Settings.

        Calls the same validator chain as the initial load. The probe
        runs again by default — machine facts may have changed since
        startup (e.g., free disk shrank). Hot reload (SIGHUP and
        ``POST /v1/admin/reload``) uses this default since R7-1: with
        ``skip_probe=True`` a probe-reliant YAML (the documented
        smart-defaults posture) reloaded with ``None`` holes that
        half-applied past validation and silently disabled RAM-ceiling
        enforcement. ``skip_probe=True`` remains a test seam for
        pinning the resolution behavior itself; in that mode the
        ``_resolve_defaults`` model validator short-circuits, any
        probe-fillable field left ``None`` in the YAML stays ``None``,
        and the caller is responsible for pinning the load-bearing
        fields in YAML.

        Args:
            path: Filesystem path to the YAML config.
            skip_probe: When ``True``, bypass the host probe. The new
                Settings is constructed independently — the caller is
                responsible for swapping live snapshots atomically via
                :meth:`SettingsHolder.replace`.

        Returns:
            A fully validated :class:`Settings` instance.

        Raises:
            yaml.YAMLError: If the YAML payload is unparseable.
            pydantic.ValidationError: If the config fails validation.
        """
        raw_text = path.read_text(encoding="utf-8")
        raw = yaml.safe_load(raw_text) or {}
        if not isinstance(raw, dict):
            raise yaml.YAMLError(
                f"Top-level YAML at {path} must be a mapping; got {type(raw).__name__}"
            )
        overlaid = _apply_env_overlay(raw)
        token = _SKIP_PROBE_FLAG.set(skip_probe)
        try:
            return cls(**overlaid)
        finally:
            _SKIP_PROBE_FLAG.reset(token)


def _apply_env_overlay(raw: dict[str, Any]) -> dict[str, Any]:
    """Apply ``PHANTOM_<UPPER>__<UPPER>...`` env vars on top of a YAML mapping.

    Walks ``os.environ`` for keys starting with the prefix and overlays
    nested dict values into ``raw`` (mutating a deep-merged copy).
    """
    import copy
    import os

    prefix = "PHANTOM_"
    sep = "__"
    overlaid = copy.deepcopy(raw)
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        key = env_key[len(prefix) :]
        parts = [p.lower() for p in key.split(sep)]
        if not parts or parts == [""]:
            continue
        cursor: dict[str, Any] = overlaid
        for part in parts[:-1]:
            existing = cursor.get(part)
            if not isinstance(existing, dict):
                existing = {}
                cursor[part] = existing
            cursor = existing
        cursor[parts[-1]] = env_val
        logger.info(
            "Applied env override %s -> %s = %r",
            env_key,
            ".".join(parts),
            env_val,
        )
    return overlaid


def load_settings(path: Path | str) -> Settings:
    """Load Phantom settings from a YAML file with env-var overlay.

    Args:
        path: Path to the YAML file. Missing or empty files are tolerated —
            they produce a no-instance settings object with a startup
            warning.

    Returns:
        A populated :class:`Settings` instance.

    Raises:
        SettingsError: If the YAML is unparseable, the top-level structure
            isn't a mapping, or env-overlay applied to a list field.
    """
    import os

    path = Path(path)
    raw: dict[str, Any] = {}

    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as fh:
                loaded = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise SettingsError(f"Failed to parse YAML at {path}: {exc}") from exc

        if loaded is None:
            logger.warning("phantom.yaml at %s is empty; using defaults only", path)
            raw = {}
        elif not isinstance(loaded, dict):
            raise SettingsError(
                f"Top-level YAML at {path} must be a mapping; got {type(loaded).__name__}",
            )
        else:
            raw = loaded
    else:
        logger.warning("phantom.yaml not found at %s; using defaults only", path)

    # Detect attempts to overlay list-typed fields via env.
    bad_env = "PHANTOM_INSTANCES"
    if bad_env in os.environ:
        raise SettingsError(
            f"{bad_env} is not env-overlay-able; instance lists live in YAML only",
        )

    overlaid = _apply_env_overlay(raw)

    try:
        settings = Settings(**overlaid)
    except Exception as exc:  # validation error
        raise SettingsError(f"Failed to validate settings: {exc}") from exc

    if not settings.instances:
        logger.warning(
            "No instances configured; Phantom will reject all proxy requests with 421",
        )

    logger.info(
        "Resolved defaults: max_in_flight=%s max_in_flight_bytes=%s max_disk_bytes=%s "
        "ram_ceiling_bytes=%s worker_count=%s body_size_threshold_bytes=%s",
        settings.saturation.max_in_flight,
        settings.saturation.max_in_flight_bytes,
        settings.saturation.max_disk_bytes,
        settings.storage.body_store.ram_ceiling_bytes,
        settings.retry.worker_count,
        settings.storage.persist_trigger.body_size_threshold_bytes,
    )

    return settings
