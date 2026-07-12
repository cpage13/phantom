# Phantom: Architecture Intent

> The onboarding map. Read this once to orient. It points at the
> authoritative documents (CONTEXT.md, the ADRs in `docs/adr/`) for the
> details.

---

## 1. What Phantom is

Phantom is a **general-purpose buffering HTTP upload sidecar**. Producers
and other clients POST HTTP uploads to Phantom; Phantom acks fast (202
+ a synthetic `ChainResponse`), persists the body locally, and runs
the actual upstream call in the background with retries and token
refresh. The producer's routine never blocks waiting on cloud
reachability; nothing is lost while Phantom is running.

The service code itself has zero upstream-specific knowledge: the
chain-envelope wire protocol (ADR-009/010) is a generic primitive
describing N HTTP steps with capture and substitution.

Phantom also exposes a **fake-S3 intake** (a catch-all that accepts a
stock S3 SDK's plain `PUT`/`POST`/`PATCH` and synthesizes a one-step
chain), **service-based SigV4 re-signing** (an opt-in per-route
`auth_mode: aws_sigv4` that re-signs the forwarded request for the real
bucket), and an **optional in-code HTTPS listener**. These do not change
the generic framing: SigV4 is opt-in per route, and the service still
holds no upstream-specific business logic.

The consumer-facing layer above the service:

- **`phantom-client`**: the universal Python SDK over Phantom's HTTP
  surface. No upstream-specific knowledge. Two runtime deps: `httpx`,
  `pydantic`. Publishable to public PyPI. An upstream-specific client
  (a thin adapter that maps a particular target API's upload calls onto
  Phantom's wire protocol) builds on top of `phantom-client` for its
  transport.

---

## 2. Why Phantom exists

### The durability problem

Producers in the field upload artifacts (parquet, images, raw captures,
sometimes hundreds of megabytes per run) to an upstream service. The
upstream and its companion S3 (presigned PUTs) can be transiently
unavailable for minutes, sometimes longer. Without a buffer, every
transient cloud failure becomes a producer failure: the producer's
process raises, the data lives only in the process's
in-memory state, and the operator must re-run an expensive workload
because S3 had a bad afternoon. The cost is real and measurable.

Phantom owns the durability layer the producer used to lack: it acks the
upload synchronously, persists the body locally (memory-tier first,
disk-tier after a configurable trigger), and runs the actual cloud
calls in the background with retries. The producer's routine sees
upstream-shaped success immediately, even when the cloud is down: the
upstream client returns a synthetic `FileInformation`
constructed from a Phantom-side UUID, and the real upstream identifier
is captured for later correlation.

### Operational constraints

Phantom runs on producers: heterogeneous embedded boxes with SD
cards, modest RAM, and no operator-in-the-loop most of the time. That
shape drives several decisions visible in the code:

- **No autovacuum on SQLite.** A scheduled VACUUM at 03:00 only fires
  when the in-flight queue is empty. SD-card-death risk beats
  convenience.
- **In-process, asyncio-only, single Docker container.** No
  microservice mesh, no Redis, no Kafka. The unit of failure is one
  producer; Phantom is a sidecar on each. Worker supervision is
  `asyncio.TaskGroup` at the composition root; unhandled worker
  exceptions cascade out of the lifespan and crash the process
  visibly. The orchestrator restarts; persisted rows survive.
- **Loopback admin by default.** Admin APIs don't authenticate; the
  deployment is same-machine-only, so ONE listener serves intake + admin +
  health on one socket bound to `127.0.0.1` by default. The loopback bind IS
  the admin access control: the deployment story is "if you can run an admin
  call, you're already on the host." A non-loopback `bind_tcp` is a
  deliberate opt-in that warns at startup.
- **RAM-first body storage so the happy path stays fast.** In the
  default hybrid mode a body lives in RAM; it migrates to disk only
  after a retry lingers long enough (or immediately on receipt for a
  large body, or under RAM pressure). Healthy uploads stay RAM-only and
  never touch the SD card. Where a body lives is a single
  `body_location` column on the row (ENUM `ram` / `file`), not a
  separate database.
- **Bodies survive process restart.** The `PersistController` is the
  sole writer of the `body_location='ram' -> 'file'` flip, and it
  applies commit-last-column ordering: the body file and its parent
  directory are fsync'd BEFORE the `UPDATE body_location='file'`
  commits, so that one UPDATE is the durability commit point. The
  startup recovery sweep then resolves what the crash left behind:
  a `body_location='file'` row whose body file vanished is quarantined
  into the `corrupted` terminal state, and a `body_location='ram'` row
  (whose RAM bytes are gone after restart) is quarantined the same way.
- **Smart defaults from a host probe.** `config/probe.py` reads
  `psutil.virtual_memory()`, `shutil.disk_usage(data_dir)`, and
  `os.cpu_count()` at startup. `config/defaults.py` derives saturation
  caps, storage caps, and worker counts. Operator-supplied YAML always
  wins.

---

## 3. Runtime topology

Three classes of process matter: the producer process, Phantom, and
in dev/CI, the emulator.

### 3.1 On a producer

```
+----------------------------------- producer box -------------------------------------+
|                                                                                      |
|   +-------------- producer process -----------------+                                |
|   |   producer logic (Python)                      |                                |
|   |     |                                           |                                |
|   |     |  imports the upstream client              |                                |
|   |     v                                           |                                |
|   |   upstream client ----+                         |                                |
|   |     |                 |  (in-process call)      |                                |
|   |     v                 |                         |                                |
|   |   phantom_client      |  HTTP via httpx.AsyncClient                              |
|   |     |                 +---->                    |                                |
|   |     v                                                                            |
|   |   localhost:8080 / docker network "phantom"                                      |
|   +-----------------------+-------------------------+                                |
|                           |                                                          |
|                           v                                                          |
|   +--------------- phantom container ---------------+                                |
|   |  uvicorn.run(app)  (one server, one listener)   |                                |
|   |                                                 |                                |
|   |  Listener :8080 (FastAPI, bind_tcp;             |                                |
|   |   loopback 127.0.0.1 by default):               |                                |
|   |   POST /v1/send              <-- producer       |                                |
|   |   GET  /v1/healthz /v1/readyz <-- probe         |                                |
|   |   POST /v1/admin/reload      <-- on-host        |                                |
|   |   GET|PUT|DELETE /v1/admin/*  <-- on-host        |                                |
|   |   (one socket; loopback bind IS the admin auth) |                                |
|   |                                                 |                                |
|   |  Background coroutines (asyncio.TaskGroup):     |                                |
|   |   - Sender pool   (N workers, probe-derived)    |                                |
|   |   - Reaper                                      |                                |
|   |   - AuthKicker                                  |                                |
|   |   - VacuumScheduler                             |                                |
|   |   - AdMinter.run() loop                         |                                |
|   |       (only when cfg.ad_mint is set;            |                                |
|   |        supervised; Phase 2 H6)                  |                                |
|   |                                                 |                                |
|   |  Storage (per instance):                        |                                |
|   |   - uploads.db (uploads, idempotency_index)   |  body_location ENUM('ram','file')
|   |     + token_cache.db; WAL mode (ADR-030)      |   ram   --> dict[chain_id, bytes]
|   |                                                 |   file  --> data_dir/bodies/    |
|   |                                                 |                                |
|   |  Outbound (httpx.AsyncClient):                  |                                |
|   |   - POST https://files.upstream.example/...  (step 1)                            |
|   |   - PUT  https://<bucket>.s3.amazonaws.com/...  (step 2)                         |
|   |   - POST https://login.microsoftonline.com/...  (AD-mint, optional)              |
|   +-------------------------------------------------+                                |
|                                                                                      |
+--------------------------------------------------------------------------------------+
```

The producer process and the Phantom container share the host.
Communication is HTTP over loopback or a docker-internal network.

### 3.2 In dev / CI

Replace the cloud endpoints with **phantom-emulator** running in its
own container (Docker mode) or as an in-process `uvicorn.Server` task
(in-process mode). The emulator exposes generic upstream-shaped endpoints (`POST
/v1/files/create`, `PUT /v1/files/upload/{token}`, `POST /oauth/token`,
`GET /.well-known/jwks.json`) plus a `/control/*` surface for failure
injection (5xx rate, latency, body cutoff, `expire-all-now`,
pause/resume).

Phantom's outbound httpx client doesn't know it's talking to an
emulator; to Phantom, the emulator IS the upstream. That opaqueness
is the load-bearing test invariant.

### 3.3 Named coroutines inside Phantom-the-service

Every loop below is a coroutine supervised by the composition root
(`app.py`'s FastAPI `lifespan`) via a single `asyncio.TaskGroup`. They
share one event loop. No threading, no multiprocessing. Each row carries
the write-purpose against `uploads` per the single-writer manifest (plan
§ 0.5).

| Coroutine | File | Role | Write-purpose vs. `uploads` | Trigger |
|---|---|---|---|---|
| Ingress handlers (`POST /v1/send`, admin routes) | `routes/send.py`, `routes/admin.py` | FastAPI handlers. Parse, dispatch admission, return 202 (or 200 on idempotency replay) + `X-Phantom-*` headers. | INSERT (via admission): atomic row + idempotency claim in ONE SQLite transaction (H7 closure). | HTTP request arrival. |
| **Sender pool** (N workers; probe-derived min(8, max(2, cpu_count))) | `workers/sender.py` | Each worker polls the single `uploads` table via `claim_due`; calls `ChainExecutor.execute_one_step`; dispatches the executor's discriminated-union result through `_on_*` handlers (ADR-015). | UPDATE state-machine columns (`queued → attempting → terminal`); `attempts`, `next_attempt_at`, `last_error`. Every UPDATE carries `WHERE state='attempting'` (closes M-W4-F7). | Polling tick (`poll_interval_ms`, default 250). |
| **PersistController** | `workers/persist_controller.py` | **Sole** writer of the `body_location='ram' → 'file'` transition. Migrates RAM bodies to disk on retry-linger (default 90 s) or RAM-pressure breach. Commit-last-column ordering: fsync body files + parent dir BEFORE flipping `body_location='file'`. | UPDATE `body_location='file'` (single-writer per § 0.5 + invariant #6). | Linger queue + ram_pressure signal. |
| **RamPressureWatcher** | `workers/ram_pressure.py` | Polls `RamBodyStore.total_bytes()` against `ram_ceiling_bytes`. Signals the PersistController when over ceiling (oldest body migrated first). | None (no DB writes). | Periodic (`ram_pressure_poll_seconds`, default 1.0). |
| **BodyOrphanJanitor** | `workers/body_orphan_janitor.py` | Walks `FileBodyStore.list_orphans()` for files whose `chain_id` is absent from `uploads`; deletes them. Closes C1 + invariant #4. | None (body-store deletes only). | Periodic (`body_orphan_sweep_seconds`, default 3600). |
| **InvariantAuditor** (Phase 3) | `workers/invariant_audit.py` | Periodic row walk. Actively asserts invariants #1 and #3 (see § 5 *Reliability invariants* below). Counters/gauges support the other five. | None (read-only). | Periodic (`invariant_audit_period_seconds`, default 300). |
| **Reaper** | `workers/reaper.py` | Deletes terminal-state rows per the retention YAML. Iterates `succeeded`, `failed`, `cancelled`, `stored`, `corrupted`, `auth_expired`, `expired` on the same sweep. Trims `idempotency_index`. | UPDATE `body_discarded_at`; DELETE terminal rows past retention. | Periodic (`reaper_interval_seconds`, default 60). |
| **AuthKicker** | `workers/auth_kicker.py` | Wakes `auth_expired` rows when a fresh token lands in the cache. Skips body-discarded rows (R6-3); re-admits through the saturation gate, returning the slot on every outcome except a confirmed wake (R9-3 / R10-2). | UPDATE the `auth_expired → queued` wake via the M-W4-F7-guarded `record_attempt_result(expected_state="auth_expired")`; the `CredentialKicker` drives the same guarded transition for `aws_sigv4` rows (doc correction 2026-06-12: this cell previously claimed no `uploads` writes). | `TokenCache.set` fires, plus a 1 s periodic rescan. |
| **CredentialKicker** | `workers/credential_kicker.py` | The SigV4 analogue of the `AuthKicker`: wakes `auth_expired` `aws_sigv4` rows when a fresh destination credential lands in the host-keyed credential store (the `CredentialKicker` class lives HERE, not in the executor). | UPDATE the `auth_expired → queued` wake (same M-W4-F7-guarded transition the `AuthKicker` uses). | A credential push for the row's `dest_host`, plus a periodic rescan. |
| **VacuumScheduler** | `workers/vacuum.py` | Cron-style scheduler for SQLite VACUUM. Only runs when `in_flight == 0`. | None (DDL only). | Cron tick (default Sunday 03:00). |
| **AdMinter.run()** | `refresh/ad_client_credentials.py` | Background loop minting AD tokens before expiry, with `ad_outage_retry_seconds` backoff. Only spawned when an instance's `cfg.ad_mint` is set. Phase 2 H6 closure: supervised by the composition root's TaskGroup (no self-spawned `asyncio.create_task`). | None (touches token_cache only). | Scheduled per the snapshot's AD-mint timings. |
| **DiskPressureProbe** | `workers/disk_pressure.py` | Background probe of `shutil.disk_usage(data_dir).free`; refuses admission via the saturation gate when the threshold is breached. Ported to the composition root TaskGroup in Phase 1. | None (signals saturation gate). | Periodic (every few seconds). |
| **ColdBackupScheduler** (optional, Phase 4) | `workers/cold_backup.py` | Periodic SQLite online-backup snapshots to `<data_dir>/backups/`. Off by default; opt-in via `db_integrity.backup_enabled`. | None (read-only on `uploads`; writes to `backups/`). | Periodic (`backup_period_seconds`, default 86400). |
| **MetricsRegistry** (Phase 3) | `observability/metrics.py` | NOT a coroutine; an in-process registry (`counter` / `gauge`) consulted by every emit site. Surfaced via `GET /v1/admin/observability/*`. | None. | Inline. |
| **Saturation gate** | `workers/saturation.py` | NOT a coroutine; a counter cache consulted synchronously by the ingress handler. Returns a typed `AdmissionResult` discriminated union; the route dispatches via `isinstance`. Hot-reload-aware via `update_caps`. Phase 2 H1 closure: `release()` runs in `try/finally`. | None. | Inline on ingress. |
| **Recovery sweep** | `workers/recovery.py` | `run_recovery` does two things: (1) reset `attempting → queued`; (2) one collect-then-write integrity walk (`body_hashes` ↔ `BodyStore.has_body_ref` per declared ref) that quarantines every non-terminal, non-discarded row with a missing body to `corrupted`. That one walk covers both vanished `body_location='file'` files and `body_location='ram'` rows (the RAM store is empty after restart by design), subject to the invariant #1 carve-out for `body_discarded_at IS NOT NULL`. The `BodyOrphanJanitor` is NOT part of this sweep; the lifespan spawns it separately. | UPDATE boot-time corrections; transition to `corrupted` for integrity-guard hits. | Lifespan startup, before sender pool starts. |
| **Chain executor** | `chain/executor.py` | NOT a coroutine; called synchronously by the sender per step. One call = one step. Owns capture-TTL gate, substitution, idempotency-header injection, auth injection, send, classify response. Auth injection branches on the route's `auth_mode`: `phantom_bearer` injects the cached bearer; `aws_sigv4` re-signs via `sign_sigv4` (`chain/sigv4_signer.py`) from a host-keyed credential, parking the row in `auth_expired` on a `SigV4SigningError`; `none` forwards as-is. Emits DEBUG-level structured logs with `captures` and `sensitive_captures` extras (gated on `isEnabledFor`). | None (sender does the UPDATE). | Sender call. |

### 3.4 Inter-process contracts

| From → To | Protocol | Port / scheme | What's spoken |
|---|---|---|---|
| Producer process → Phantom (proxy ingress) | HTTP/1.1 (httpx via phantom-client) | `http://<phantom>:8080` (TCP) or `unix:/path` (UDS) | `POST /v1/send` with `ChainEnvelope` (JSON or multipart). Headers: `Authorization`, `X-Phantom-Uid` (routing comes from the first step's URL). Responds 202 + `ChainResponse` + `X-Phantom-*` response headers. |
| Stock S3 SDK → Phantom (fake-S3 intake) | HTTP/1.1 (any unmodified S3 client) | `http(s)://<phantom>:8080`, path-style | `PUT`/`POST`/`PATCH /{bucket}/{key}` (the catch-all). Destination is set by `phantom_default_target` or the per-request `?phantom=<url>` carrier. Phantom synthesizes a one-step chain and forwards; the route's `auth_mode` decides re-sign (`aws_sigv4`) vs forward-as-is (`none`). |
| Orchestrator / container probe → Phantom | HTTP/1.1 | `http://<phantom>:8080` (the one listener) | `GET /v1/healthz` (liveness) and `GET /v1/readyz` (readiness) - public `*z` paths on the single listener. |
| Phantom → upstream (e.g., the target API / S3) | HTTP/1.1 (httpx) | `https://files.upstream.example/...`; `https://<bucket>.s3.amazonaws.com/...` | Ordinary HTTP. Phantom isn't aware these are "chain steps" beyond what the envelope declared; it just runs them in order, capturing per ADR-009. **Bytes Phantom forwards are byte-identical to what the agent sent** (the dual-body-hash invariant from ADR-014). For `aws_sigv4` routes Phantom itself SigV4-signs the outbound request with its own host-keyed credential (only the auth headers are constructed; the body bytes are unchanged). |
| Phantom → Azure AD (only when `cfg.ad_mint` is set) | HTTPS (azure-identity or equivalent) | `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token` | OAuth2 client-credentials grant. Phantom's own app-registration credentials, never the producer's. |
| Admin tools / phantom-client admin calls → Phantom admin | HTTP/1.1 | `http://127.0.0.1:8080` (the one listener, loopback by default) or UDS | `GET/POST/PUT/DELETE /v1/admin/*` - no auth; admin rides the single listener, so the loopback default bind IS the access control (ADR-004). `POST /v1/admin/reload` triggers hot reload. |
| Emulator → Phantom (dev/CI control) | n/a | n/a | Emulator never calls Phantom. The flow is one-way: Phantom calls the emulator's upstream endpoints; test runners call the emulator's `/control/*` to inject failures. |

### 3.5 Storage layout

Each Phantom **instance** has its own storage partition rooted under
`<data_dir>/<instance.data_dir>/`:

```
<data_dir>/
  upstream/                             # one subdir per instance
    uploads.db                          # uploads + idempotency_index (serialized writer + read-only reader, ADR-029)
    token_cache.db                      # the persistent token cache, its own DB file (ADR-030)
    credential_store.db                 # host-keyed destination-credential store (aws_sigv4), its own DB file
    bodies/
      ab/                               # 2-char shard prefix (configurable)
        <chain_id>/
          body                          # one file per body_ref name
          body_2                        # if multiple body_refs
    .tmp/                               # FileBodyStore atomic-rename staging (purged at startup)
    uploads.corrupted.<stamp>.db        # flat quarantine backup pair (integrity fail) ...
    bodies.quarantine.<stamp>/          # ... named by its manifest (ADR-026)
    uploads.mode_switch.<stamp>.db      # flat mode-switch backup pair (ADR-025) ...
    bodies.mode_switch.<stamp>/         # ... same manifest scheme
    backup.<backup_id>.manifest.json    # one manifest per backup: identity + declared artifact paths
    backups/                            # optional cold-backup snapshots (when db_integrity.backup_enabled)
```

Phase 1 collapsed the previous two-TIER design (a `:memory:` + disk
pair with identical schemas) into one persistent `uploads.db` carrying
`uploads` (with the `body_location` column) and `idempotency_index`.
The token cache is a deliberate SECOND database file (`token_cache.db`,
its own connection and writer, ADR-030); the `aws_sigv4`
destination-credential store is a THIRD (`credential_store.db`, same
writer-isolation rationale). The schema carries
`body_hashes_json` as a TEXT column (default `'{}'`) so each row
round-trips its `dict[str, BodyHashes]` map.

**Body location is a column, not a database.** The `body_location ENUM('ram', 'file')`
column on each `uploads` row records where its body bytes live; the
`HybridBodyStore` consults this on every read. RAM bodies live in a
`RamBodyStore` (a `dict[ChainId, dict[name, bytes]]`); disk bodies
live under `bodies/<shard>/<chain_id>/` via `FileBodyStore` (atomic
rename through `.tmp/`, fsync on file AND parent dir). The
`PersistController` is the SOLE writer of the `'ram' → 'file'`
transition (invariant #6).

The **token cache** lives in its own `token_cache.db` (ADR-030; the
`token_cache` table also declared in `uploads.db`'s schema sits empty
in production) and survives
process restart; bad tokens stay in the cache with `status="bad"`
rather than being deleted, so the admin API can surface "this is the
only token I have for this slot, and it's bad" (ADR-003).

### 3.6 Composition root supervision

`app.py`'s FastAPI `lifespan` (`phantom.app.create_app`) is the
**composition root**: the single factory that constructs every
long-lived coroutine and supervises them under one `asyncio.TaskGroup`.
It is the one path production runs: `python -m phantom` → `__main__`
→ `create_app(...)` → `uvicorn.run(app)`. (A pre-Phase-1 plan to make
`runtime/composition.py::compose_and_run` the root was abandoned; that
dead, single-instance shim was deleted in the 2026-05-29 refinement and
the boot guards it held were relocated to `runtime/startup_checks.py`;
see ADR-024.) Two structural rules follow:

1. **No unsupervised task spawning.** Every long-lived coroutine is
   spawned via a `TaskGroup` (`tg.create_task(...)`), so a worker
   exception always propagates as an `ExceptionGroup`. The unsupervised
   primitives (`asyncio.create_task(`, `asyncio.ensure_future(`,
   `loop.create_task(`) are forbidden in production outside the
   lifespan's one sanctioned `loop.create_task` (the SIGHUP handler,
   which must schedule its reload as a loop task because the signal
   callback cannot await). A pre-commit grep
   (`forbid-asyncio-create-task-outside-composition`) enforces this
   greppably (invariant #15). The sender's per-instance worker pool
   uses its own nested `asyncio.TaskGroup` inside `Sender.run()`, a
   legitimate structured-concurrency pattern, since that group is
   itself a child of the lifespan group.
2. **Visible crash.** An unhandled worker exception cascades out of
   the TaskGroup. The production CLI's ordinary-worker-failure callback
   requests uvicorn shutdown; pinned uvicorn 0.46 drains and then re-raises
   SIGTERM, terminating `python -m phantom` non-zero (uvicorn otherwise only
   logs a post-start lifespan failure and keeps serving). TaskGroup's direct
   `SystemExit`/`KeyboardInterrupt` special cases are outside this bridge.
   The container orchestrator restarts; persisted rows survive; in-flight RAM
   bodies are lost (and the recovery sweep quarantines stale
   `body_location='ram'` rows by design).

On the next boot, recovery resets `attempting → queued`, completes its body
integrity quarantine pass, and reconstructs the fresh in-memory
`SaturationGate` from every persisted row for which the shared
`row_holds_slot(state, body_discarded_at)` predicate is true. This happens
before workers or ingress open, so recovered backlog cannot bypass the live
row/byte caps.

Before spawning workers the lifespan runs the boot-time guards from
`runtime/startup_checks.py`: process-wide `apply_umask`,
`check_retention_floor`, and `check_instance_isolation` at the top;
then, per instance (after `data_root.mkdir`, before the store opens),
`check_body_store_mode` and the `run_integrity_gate` DB-corruption gate:
`PRAGMA integrity_check` on the SQLite; quarantine + `db_quarantine_total`
bump, then fail-open (or fail-closed via `db_integrity.fail_open=false`)
on detected corruption. `startup_checks.py` also owns the single
`build_body_store` mode-wiring table (`hybrid`/`all_ram`/`all_disk`).

---

## 4. Per-package module breakdown

Three Python packages live under `src/`: `phantom-service`,
`phantom-client`, and `phantom-emulator`. A fourth directory,
`phantom-deploy`, ships the container image plus the reference
`docker-compose.yml`; it is not a Python package and is out of scope
for the producer-side architecture.

### 4.1 `phantom` (the service)

**Role.** The wire-protocol-defining buffering HTTP proxy. Owns every
ADR-010 model, every header, every error code, every state
transition. The other packages target this surface.

**Distribution name.** The PyPI distribution is **`phantom-service`**
(the bare `phantom` was already taken). The Python import name is
unchanged: code continues to read `import phantom` /
`from phantom import …`. The wheel filename is
`phantom_service-<version>-py3-none-any.whl`; the package metadata's
`Name:` is `phantom-service`.

**Module categories:**

- `models/`: Pydantic wire and persistence types (`ChainEnvelope`
  family, `UploadRow` with `body_hashes: dict[str, BodyHashes]`,
  `ErrorBody`, `ChainAdminDetail`, `TokenSlot`).
- `config/`: `Settings` (pydantic-settings with
  `env_nested_delimiter="__"`), `probe.py` (`MachineFacts` + the
  psutil/shutil probe), `defaults.py` (probe → `ResolvedDefaults`),
  `ad_mint.py` (typed `AdMintConfig`). YAML reload via
  `Settings.reload_from_yaml(path, *, skip_probe=False)`.
- `storage/`: `UploadStore` / `BodyStore` / `TokenCache` Protocols
  plus concrete `SqliteUploadStore` (with `vacuum`), `RamBodyStore`,
  `FileBodyStore` (the only place body bytes touch disk, through
  `aiofiles` + `asyncio.to_thread(os.fsync)`), `SqliteTokenCache`.
  `BodyStore` Protocol carries `has_body_ref` for recovery's integrity
  check. `errors.py` defines `StorageCorruptionError` and
  `CodecRoundTripDriftError`.
- `compression/`: `BodyCodec` Protocol +
  `ZstdCodec`/`GzipCodec`/`PassthroughCodec`. `select_codec(cfg) ->
  BodyCodec` returns the configured codec unconditionally
  (always-encode); `CompressionCfg.mode` is the single literal
  `"always"`.
- `chain/`: JSONPath wrapper (`jsonpath-ng`), envelope parser
  (raises typed `ParserError` with ADR-010 error codes), **`ChainExecutor`** (the load-bearing primitive). Discriminated-union result types (`Succeeded`, `Failed5xx`, `FailedAuth`, etc.) are exhaustive by mypy. `sigv4_signer.py` is the `aws_sigv4` arm's primitive: `sign_sigv4` dispatches the botocore signer class from the credential's `SigningService` (the `_SERVICE_SIGNERS` map; S3 → `S3SigV4Auth`, which emits + signs `x-amz-content-sha256`) and raises `SigV4SigningError` on an unknown service rather than a bare `KeyError`.
- `transport/`: `UpstreamClient` Protocol + httpx implementation.
  Only place `httpx.AsyncClient` is constructed.
- `refresh/`: `AdMinter` class (no Protocol; one concrete
  implementation, instantiated from `cfg.ad_mint: AdMintConfig`).
  `InstanceContext.minter: AdMinter | None` carries it. There is no
  generic refresh-strategy abstraction; when an instance has no
  `ad_mint` block, `minter` is `None` and Phantom waits for the
  client to push tokens.
- `strategies/`: `UploadStrategy` Protocol + two retry schedulers
  (`fixed_intervals`, `exponential_backoff`).
- `routing/`: `resolve_route(url, instance_cfg) -> ResolvedRoute`
  function. No Protocol; one concrete implementation. `ResolvedRoute.auth_mode`
  is a 3-valued axis: `phantom_bearer` | `none` | `aws_sigv4`.
- `instances/`: `InstanceContext` dataclass (mutable, so the hot-reload handler can swap `cfg`/`minter`; the docstring is the
  contract that workers only read) + `InstanceDispatcher` (URL-prefix
  → `InstanceContext` lookup) + `InstanceSettingsSnapshot` (the
  per-tick hot-reloadable view) + `SettingsHolder` (the atomic-swap
  registry) + `instances.context.InstanceStoragePaths` (owns the live
  per-instance `uploads.db` / `bodies/` layout; the composition root
  and the quarantine/restore routes derive the same paths from it).
- `workers/`: `Sender`, `Reaper`, `AuthKicker`, `CredentialKicker`
  (`workers/credential_kicker.py`: wakes `auth_expired` `aws_sigv4` rows
  on a fresh credential push; the SigV4 analogue of `AuthKicker`),
  `VacuumScheduler`, `SaturationGate` (returns `AdmissionResult` union),
  `run_recovery` (the boot sweep: reset `attempting → queued`, then one
  collect-then-write integrity walk that quarantines missing-body rows;
  the `BodyOrphanJanitor` is spawned separately by the lifespan).
- `observability/`: `logging.py` (structlog setup with bearer
  redaction filter and `SensitiveCaptureRedactor` filter) and
  `metrics.py` (the in-process `MetricsRegistry` of counters + gauges,
  surfaced via `GET /v1/admin/observability/*`). No OTel tracing.
- `routes/`: `POST /v1/send` handler (under 100 lines; the
  acceptance gate is `scripts/check_post_send_size.py`) + the admin
  router (incl. `PUT /v1/admin/credentials/{dest_host}`, the host-keyed
  credential push) + `admission.py` (the `admit_chain` orchestrator, owning
  body-hash compute, idempotency dedup, persist-trigger decision, and
  two-phase row write) + `catch_all.py` (the fake-S3 / raw-intake catch-all:
  `PUT|POST|PATCH /{phantom_path:path}`, which synthesizes a one-step chain
  for a stock-SDK upload).
- `models/credential.py`: the destination-credential tagged union
  (`CredentialPushBody` = `SigV4StaticCredBody | ProfileRefCredBody`), the
  closed `SigningService` enum (`S3 = "s3"`), the host-keyed `HostCredKey`,
  and the `_coerce_signing_service` `mode="before"` validator that lets
  `service` accept the wire string `"s3"` under `strict=True`.
- `runtime/tls_cert.py`: `resolve_tls_paths`, the optional HTTPS
  listener's cert resolution (auto-generated self-signed for
  `localhost`/`127.0.0.1`, or an operator-supplied PEM pair).
- `app.py`: FastAPI factory + composition root. Lifespan installs
  the SIGHUP handler (when a `settings_path` is configured),
  constructs the `SettingsHolder`, builds per-instance snapshots, and
  spawns workers via `asyncio.TaskGroup`. Unhandled worker exceptions
  cascade out of the `TaskGroup`; the production CLI callback stops uvicorn,
  whose pinned signal path drains and terminates by SIGTERM.

**Public surface.** The HTTP surface is the public surface - all on the one
listener (loopback by default): `POST /v1/send` (ingress), `GET /v1/healthz`
/ `GET /v1/readyz` (liveness/readiness), and `GET|POST|PUT|DELETE
/v1/admin/*` (the loopback default bind is the admin access control). No
Python API is exported for in-process consumers.

### 4.2 `phantom-client` (the SDK)

**Role.** The universal Python SDK over Phantom's HTTP surface. No
upstream-specific knowledge. Publishable to public PyPI (no
corporate-internal deps; runtime deps are exactly `httpx` and `pydantic`).

**Module categories:**

- `models/chain.py`: ADR-010 envelope shapes. **Byte-identical to
  `phantom.models.chain`** (the duplication is mechanized by
  `tests/contract/test_chain_models_alignment.py`).
- `models/status.py`: `UploadRow`, `UploadState`,
  `TERMINAL_STATES` (the `poll_until` default stop-set; seven
  members, every terminal `ChainState`: `succeeded`, `failed`,
  `cancelled`, `stored`, `corrupted`, `auth_expired`, `expired`;
  R6-5 added `corrupted`),
  `SortKey`, `StatsResponse`, `TokenSlot`, health/ready.
- `models/admin.py`: `ExtractFilter`, `DeleteFilter`,
  `KeyValueMatchFilter`, `InstanceSummary`,
  `AdminStatusResponse`, `InstanceStatusResponse`,
  `BulkDeleteResponse`, `UploadBundle`, and **`ChainAdminDetail`**
  (the admin-only chain detail shape returned by
  `GET /v1/admin/chains/{chain_id}`; separate from the wire-facing
  `ChainResponse` returned by `POST /v1/send`).
- `models/envelope.py`: `ResponseHeaders` parsed view +
  `parse_response_headers` stripper.
- `headers.py`: `X-Phantom-*` constants + `build_request_headers`
  helper.
- `errors.py`: typed exception hierarchy (`PhantomClientError` →
  transport-class subtree retry-eligible, HTTP-class subtree not) +
  `EXCEPTION_FOR_CODE` mapping every ADR-010 error code to a class
  (including `storage_corruption` and `codec_round_trip_drift` →
  `PhantomServerError`).
- `config.py`: `Timeouts`, `RetryPolicy`, `SubmitOptions`,
  `ClientConfig`.
- `transport.py`: internal `Transport` (the only place
  `httpx.AsyncClient` is constructed). Selects JSON vs. multipart on
  `bool(body_refs)`. **Every multipart part carries a non-empty
  filename** (load-bearing for the transparent-proxy invariant on
  bodies containing bytes >= 0x80; starlette's MultiPartParser
  UTF-8-decodes filename-less parts).
- `poller.py`: `poll_until` helper with configurable
  `terminal_states` set. Parses admin responses as `ChainAdminDetail`.
- `client.py`: `PhantomClient` async facade. `get_upload` and
  `poll_until` return `ChainAdminDetail`.

**Public surface.** Everything re-exported from
`phantom_client.__init__`: `PhantomClient`, `ClientConfig`, every
ADR-010 model, every error class, `ChainAdminDetail`. `Transport`
is internal-only and never re-exported. `submit_chain` is the only
chain-submission method.

### 4.3 `phantom-emulator` (the upstream emulator)

**Role.** Test infrastructure. Implements generic upstream-shaped
endpoints plus controllable failure injection. Two deployment modes
(wheel for in-process tests; Docker for CI/full-fidelity).

**Module categories:**

- `config.py`: `AppConfig` (Pydantic Settings) + `ServerCfg` /
  `AuthSigningCfg` / `UpstreamCfg`.
- `state.py`: `EmulatorState` (in-process stores: issued tokens,
  accepted bodies, idempotency cache, received log). `AcceptedBody`
  carries `content_encoding: str | None`.
- `auth/jwt_minter.py`, `auth/jwks.py`, `auth/modes.py`: HS256/RS256
  mint, JWKS doc, auth-mode enum + policy.
- `failure/injection.py`, `failure/middleware.py`: `FailurePolicy`
  Pydantic model + seeded RNG + per-scope failure-injection
  middleware.
- `upload/presigned.py`, `upload/correlation.py`: synthetic
  presigned URL minter + `metadata.key_value_store`
  extraction/echo-back.
- `routers/auth.py`, `routers/upstream.py`, `routers/control.py`,
  `routers/s3.py` (the SigV4-validating fake-S3 sink), and
  `routers/raw_sink.py` (the auth-free raw sink): the five router
  surfaces. `ReceivedEntry` (the `/control/received`
  shape) carries `body_hash: str` (SHA-256 hex) and
  `content_encoding: str | None` so transparent-proxy tests can
  assert byte-identity on the emulator side.
- `app.py`, `server.py`, `__main__.py`: FastAPI factory,
  programmatic `Server` class, CLI entry-point.

**Public surface.** `AppConfig`, `start_server`, `Server`,
`FailurePolicy`, `AuthMode`. Publishable to public PyPI.

---

## 5. Invariants

The cross-cutting properties the codebase upholds. Violations are
bugs.

1. **No upload is lost while Phantom is running normally.** Ingress
   acks (202) only after admission's atomic SQLite transaction has
   committed both the row (`body_location='ram'` in hybrid/all_ram
   modes; `body_location='file'` in all_disk mode) and the
   idempotency claim. The H7 closure (Phase 1 § 2.3.17) makes this
   admission INSERT + claim INSERT a single transaction. The
   PersistController applies commit-last-column ordering (body files
   + parent dir fsync'd BEFORE `UPDATE body_location='file'`) so
   crashes between body write and column flip leave a recoverable
   `body_location='ram'` row whose RAM bytes are lost on restart
   (quarantined by the recovery sweep as designed).
2. **Phantom-the-service is opaque at the auth layer (for
   `phantom_bearer`).** On the `phantom_bearer` arm Phantom never parses
   tokens: it treats `Authorization` as an opaque byte string, stores it
   in the `(endpoint, uid)`-keyed cache, and replays it on retry. `uid`
   is caller-supplied (`X-Phantom-Uid` header) and never inspected. The
   `aws_sigv4` arm is the deliberate exception: there Phantom constructs
   a **fresh** SigV4 signature from its own host-keyed credential; it
   never parses or reuses the inbound signature, it re-signs from scratch
   (with a fresh timestamp per attempt). Either way the body bytes
   forwarded upstream stay byte-identical (the transparent-on-the-wire
   invariant is preserved; only the auth headers are constructed).
3. **The `(endpoint, uid)` cache is the single source of truth for
   retry tokens.** The sender always reads the cached value at
   attempt time; it never carries a token from the original request
   through the retry queue.
4. **Bearer values never appear in admin responses.** `TokenSlot`
   has `endpoint, uid, last_updated, status` and nothing else. A
   regression test exercises every admin endpoint and asserts no
   response body contains a substring matching `Bearer [A-Za-z0-9._\-]+`.
5. **Bodies for successful uploads drop immediately on success;
   metadata persists for the configured retention window.**
6. **Bad tokens stay in the cache.** The cache distinguishes `fresh
   | bad | unknown`; bad tokens are not deleted.
7. **The chain envelope's wire schema is owned by phantom.**
   `phantom.models.chain` is the authoritative source.
   `phantom_client.models.chain` duplicates it byte-for-byte; the
   drift-detection contract test enforces. `ChainAdminDetail` is
   admin-only and outside the contract test.
8. **A captured value's TTL bounds the executor's behavior on
   later steps that reference it.** Per ADR-011.
9. **Admin is loopback / UDS only.** The deployment is same-machine-only:
   ONE listener serves intake + admin + health on one socket, and
   `bind_tcp` defaults to `127.0.0.1:8080`. The loopback default bind IS
   the admin access control: nothing is reachable off-box by default, so the
   destructive admin surface is not either. A non-loopback `bind_tcp` is an
   explicit opt-in that warns at startup (admin rides this listener and is
   unauthenticated; front it with an authenticating reverse proxy). See
   ADR-004. (A two-listener split that bound admin on its own socket was
   tried (R12-1) and collapsed as no-benefit for the same-machine
   deployment; it introduced R13-1 + R13-2, both eliminated by the single
   listener.)
10. **The drop-in invariant for an upstream adapter: it overrides
    exactly the upload-path methods, inheriting everything else from the
    upstream client unchanged.** An adapter test enforces this by
    introspecting the class.
11. **Transparent on the wire.** Bytes Phantom forwards to upstream
    are byte-identical to what the agent sent. Dual SHA-256 hashes
    per `body_ref` (`body_hash` of raw bytes; `storage_hash` of
    stored encoded bytes) enforce this. Mismatch transitions the row
    to `corrupted` (terminal). See ADR-014.
12. **Always-encode.** One configured codec per deployment (default
    zstd). No size short-circuit, no `Content-Encoding` pass-through.
    `PassthroughCodec` (`"original"`) is an explicit operator choice.
13. **State transitions are owned by the sender.** `workers/sender.py`'s
    `_on_*` handlers ARE the canonical transition table. The
    executor's discriminated-union result types are exhaustive by
    mypy. See ADR-015.
14. **Operational config is hot-reloadable.** Retention, saturation
    caps, codec choice, persist trigger, capture-re-execution flag,
    retry params, AD-mint refresh timings all reload via SIGHUP or
    `POST /v1/admin/reload`. Atomic snapshot swap via `SettingsHolder`.
    Workers consult `InstanceContext.current_settings()` on each tick.
    See ADR-013.
15. **Composition root owns supervision (no unsupervised spawns).**
    Every long-lived coroutine is spawned by `app.py`'s lifespan under
    one `asyncio.TaskGroup` (`tg.create_task(...)`). The unsupervised
    spawn primitives (`asyncio.create_task(`, `asyncio.ensure_future(`,
    `loop.create_task(` / `get_*_loop().create_task(`) are forbidden in
    production outside the lifespan's one sanctioned `loop.create_task`
    (the SIGHUP handler); TaskGroup-bound spawns are always allowed
    (the sender's nested worker-pool TaskGroup is supervised by
    construction). The `forbid-asyncio-create-task-outside-composition`
    pre-commit grep enforces exactly this predicate (a planted
    `loop.create_task` outside the lifespan fails the hook). An unhandled
    ordinary worker exception invokes the production CLI's fatal-worker
    bridge, which stops uvicorn and terminates through pinned uvicorn's
    SIGTERM path; the container orchestrator restarts;
    persisted rows survive (in-flight RAM bodies do not, and
    the recovery sweep quarantines stale `body_location='ram'` rows by
    design). See § 3.6 and ADR-024.
16. **The saturation ledger balances.** Exactly one gate charge per
    admitted row; release ownership is a pure function of
    `(state, body_discarded_at)` through the single
    `row_holds_slot` predicate (`workers/saturation.py`). The sender
    releases on the terminal transitions it drives and on the
    auth_expired park; `stored` holds until its body discard or its
    removal, whichever first; every row-removal path (admin cancel /
    delete / bulk delete, the reaper's three removal legs) releases on
    accounting captured atomically with the removal; every re-queue of
    a released row (replay, the AuthKicker wake) re-admits through the
    gate. Boot recovery reconstructs those same charges from persisted rows
    before workers start. The gate idles at zero. (R8-4 / R8-6.)
17. **Irreversible cross-worker effects confirm-then-act on live
    state.** A worker acting on another owner's rows re-reads the live
    truth at the decision instant instead of trusting a snapshot: the
    orphan janitor's two-sweep confirmation + live-row re-read (R6-1),
    `mark_persisted`'s H4 guard with the migration undo narrowed to its
    own artifacts (R7-2 / R8-3), the AuthKicker's body-discarded skip
    (R6-3), the InvariantAuditor's live re-read on a body-ref miss
    (R5-1), BOTH body-discard legs' stamp-first order behind the
    in-transaction state+stamp guard with files deleted only after a
    confirmed flip (R9-5 reaper, R10-1 sender), and the live-row
    re-read before every late body delete that follows a row removal
    (R10-D1: admin bulk delete's C1 loop, the reaper's `max_rows`
    eviction; the single-chain delete is exempt by its body-before-row
    order). The step-aside's complement is admission's chain_id
    namespace clear (R11-1): after the live-row refusal and before its
    put, re-admission deletes the reused chain_id's body namespace, so
    a stepped-aside row's leftovers - or any dead chain_id's residue -
    can never poison the new owner's hash-verified namespace union.
18. **Config distribution follows ADR-031.** Live-snapshot read at
    every point of use is canonical; the push/rebuild exceptions and
    the restart-required set are enumerated in the ADR-013 table; the
    knob-matrix contract test enforces the table.

### Reliability invariants: runtime enforcement breakdown

Plan § 0.5 names **seven reliability invariants** every cold reader
of the `uploads` table can rely on. They split into two enforcement
categories; this distinction matters for operators and for Phase 8
deliberate-violation testing:

**Actively asserted by the `InvariantAuditor`'s periodic row walk**
(`workers/invariant_audit.py`, default cadence
`invariant_audit_period_seconds=300`):

| # | Invariant | Audit mechanism |
|---|---|---|
| 1 | `body_location='file'` ⟹ files exist (modulo `body_discarded_at IS NOT NULL` H4 carve-out) | Row walk checks `BodyStore.has_body_ref` for every `body_location='file'` row; misses bump the `missing_body_file` bucket of `invariant_violation_total`. Startup integrity sweep also catches at boot. |
| 3 | `body_hashes` map keys ↔ body-store ref set | Row walk checks `BodyStore.has_body_ref` for each declared `body_hashes` key (individual misses bump the invariant #1 buckets); a row whose declared set is non-empty while NONE of its refs are present bumps the `body_hash_set_mismatch` bucket of `invariant_violation_total`. |

**Counter / gauge monitored**: surfaced via
`GET /v1/admin/observability/*` but NOT asserted in the row walk.
Operators detect violations via the metric values + CI alerting on
non-zero `invariant_violation_total` labels:

| # | Invariant | Monitoring signal |
|---|---|---|
| 2 | Saturation-bytes basis matches `body_size_bytes` (closes H5) | `saturation_balance` gauge (in-flight declared bytes) for operator visibility; the audit row walk does NOT read it and no drift label exists (C10 doc correction 2026-06-12). Structurally enforced by the single function signature `admit(declared_bytes: int)` / `release(actual_bytes: int)` - no code path can pass an encoded size - plus the invariant #16 atomic-accounting discipline, pinned by the per-path unit releases and the saturation idle-balance e2e (`tests/e2e/test_saturation_idle_balance.py`). |
| 4 | Body-file orphans GC'd on a schedule (closes C1) | `orphan_body_count_total` counter (orphans found per janitor sweep). The presence of `BodyOrphanJanitor` IS the structural enforcement; the counter is the visibility. |
| 5 | `record_attempt_result` does not clobber `cancel`/`replay` (closes M-W4-F7) | `record_attempt_result_no_op_total` counter (rowcount=0 events; should be rare and non-zero only under admin cancel/replay racing with sender). Structurally enforced by the `WHERE state='attempting'` predicate on every UPDATE; violation impossible by code shape. |
| 6 | PersistController is the SOLE writer of `body_location='ram' → 'file'` | Two-layer enforcement: (i) code-review checklist; (ii) pre-commit grep (`scripts/check_persist_ordering.py`, run by the persist-ordering hook). No runtime audit label exists for this invariant (C10 doc correction 2026-06-12: the previously cited `body_location_unexpected_writer` label appears nowhere in the code); the write-purpose unit pins (`test_invariants.py`) keep the writer set bounded. |
| 7 | No `attempting` row survives a restart | The `by_state.attempting` count in `GET /v1/admin/stats` (steady state should equal the sender's in-flight count; a spike means a sender died without `record_attempt_result`); no dedicated gauge exists. Structurally enforced by the recovery sweep resetting every `attempting → queued` before opening ingress. |

**Net**: **2 of 7 invariants are actively asserted in the audit
row walk; 5 are counter/gauge monitored.** Operators relying solely
on the InvariantAuditor's `invariant_violation_total` for "all is
well" coverage see only invariants #1 and #3 (the label-value buckets
`missing_body_file`, `missing_body_in_ram`, `body_hash_set_mismatch` -
the audit's complete label set; the counters endpoint surfaces each as a
bucket key in the `invariant_violation_total` map, the `name ->
{label_value: count}` shape documented in § 4.2's observability table,
not as a `{label=...}`-style tag). Phase 8 § 9.2.4 disable-auditor tests
exercise this distinction; deliberate violations of #2, #4, #5, #7
surface through their dedicated counters/gauges (and #6 through its
pre-commit gate and write-purpose unit pins), not the audit's row
walk.

Registered aggregate counters/gauges (full list; Phase 3 onward):

- `invariant_violation_total`: counter labeled by invariant
- `invariant_audit_runs_total`: counter of auditor sweep iterations
- `saturation_balance`: gauge of in-flight declared bytes
- `orphan_body_count_total`: counter of orphans removed per sweep
- `record_attempt_result_no_op_total`: counter
- `body_missing_total`: counter of sender-side `BodyMissingError` events (H8)
- `body_location_distribution`: gauge map (count by `body_location`)
- `persist_total`: counter of PersistController migration outcomes (labels `success` / `failure`)
- `persist_controller_queue_depth`: gauge
- `ram_body_store_bytes`: gauge
- `ram_ceiling_bytes`: gauge (the configured RAM ceiling)
- `ram_pressure_signal_total`: counter
- `reaper_actions_total`: counter (labels `body_discarded` / `row_deleted`)
- `db_quarantine_total`: counter (Phase 4)
- `mode_switch_backup_total`: counter (ADR-025 back-up-and-run)
- `schema_discard_total`: counter (the boot schema gate)

The `ColdBackupScheduler` registers no metrics.

CI rule: a non-zero `invariant_violation_total` sample on any label
fails the next build.

### Deployment-target constraints

Phantom is built for the **producer-side** deployment shape. Operators
deploying outside that shape need to read these constraints first:

- **Pi-class hardware** (Raspberry Pi 4 / 5 class: 2 to 8 GiB RAM,
  16 to 256 GiB SD card, ARM64) is the primary target. The smart
  defaults from `config/probe.py` are calibrated for this class.
- **Abrupt power-loss tolerance.** The default
  `storage.sqlite.synchronous="NORMAL"` (Phase 1 flip from
  `"FULL"`) trades the last few seconds of commits on hard power-
  cut for significantly less SD-card wear. Rack-server deployments
  with battery-backed write cache should pin `synchronous="FULL"`.
- **Flash-wear-aware defaults.** `auto_vacuum` is hardcoded NONE
  (no operator knob; plan § 0.3 hard rule). Cron VACUUM at 03:00
  fires only when `in_flight == 0`. The persist-controller's
  retry-linger (default 90 s) keeps healthy uploads RAM-resident,
  off the SD card entirely.
- **Deployment-mode flexibility.** `body_store.mode` is a
  first-class operator knob: `hybrid` (production default;
  RAM-first with persist-on-linger/pressure), `all_ram` (lossy
  on restart by design), `all_disk` (every admission to disk;
  pays an fsync per ack). The operator playbook walks through the
  selection criteria.
- **Single-container topology.** One Phantom container per producer.
  No microservice mesh, no Redis, no Kafka. Unit of failure is one
  producer.
- **Loopback admin.** Admin port binds `127.0.0.1` by default
  (ADR-004). The deployment story for "operator wants admin access
  from elsewhere" is a reverse proxy with auth in front of the
  loopback bind; that is not Phantom's job.

### New admin endpoints (Phase 3 + Phase 4)

Loopback-only per ADR-004. All return JSON.

| Endpoint | Returns | Purpose |
|---|---|---|
| `GET /v1/admin/observability/counters` | `{ "<counter_name>": { "<label>": <int>, ... } }` | All counters from `MetricsRegistry`. Includes `invariant_violation_total`, `orphan_body_count_total`, `record_attempt_result_no_op_total`, `ram_pressure_signal_total`, `db_quarantine_total`. |
| `GET /v1/admin/observability/gauges` | `GaugesResponse`: `{ "gauges": [ { "name": "...", "description": "...", "values": { "<label>": <float> } }, ... ] }` - a LIST of gauge objects, not a name-to-value map; the empty-string label bucket is the no-label total (C2 doc correction 2026-06-12). | All gauges. Includes `saturation_balance`, `body_location_distribution`, `persist_controller_queue_depth`, `ram_body_store_bytes`, `ram_ceiling_bytes`. |
| `GET /v1/admin/observability/ram_pressure` | `{ "ram_body_store_bytes": <int>, "ram_ceiling_bytes": <int>, "fraction": <float>, "over_ceiling": <bool> }` | Snapshot of the RAM-pressure surface for live dashboards. |
| `GET /v1/admin/quarantine` | `{ "quarantines": [{ "path": "...", "name": "...", "kind": "db" \| "body_store", "bytes": <int>, "reason": "corrupted" \| "mode_switch", "iso": "..." }] }` | Inventory of the FLAT timestamped-sibling backup artifacts in each instance data_root: `uploads.corrupted.<stamp>.db` / `bodies.quarantine.<stamp>/` from a corruption event (Phase 4), and `uploads.mode_switch.<stamp>.db` / `bodies.mode_switch.<stamp>/` from a back-up-and-run mode switch (`<stamp>` = the display iso plus the first 8 hex chars of the `backup_id`, ADR-026). The `reason` field discriminates the two; a `mode_switch` entry is restorable via `POST /v1/admin/quarantine/restore`. Operator surface for "what's been backed up; what do I need to look at?" |

---

## 6. Core flow: the happy path, end-to-end

A producer process wants to upload a file.

```
[Producer process]
    |
    | upstream_client = UpstreamClient(settings, phantom_url="http://phantom:8080")
    | file_info = await upstream_client.in_memory_upload(request, contents)
    v
[upstream client . in_memory_upload]                              <-- upstream client
    |
    | 1. token = self._get_security_token() if configured else None
    | 2. uid = derive_uid(token, fallback=self._uid_fallback)
    | 3. local_uuid = uuid4()
    | 4. request.metadata.key_value_store["phantom_local_uuid"] = str(local_uuid)
    | 5. envelope = build_in_memory_upload_envelope(...)
    |    body_refs = {"body": contents}
    | 6. await self._phantom.submit_chain(envelope, body_refs=body_refs, uid=uid, ...)
    | 7. return FileInformation(id=local_uuid, ...)  <-- synthetic; producer sees this immediately
    v
[phantom_client.client.PhantomClient.submit_chain]                <-- phantom_client
    |
    | 1. select JSON vs. multipart by bool(body_refs)
    | 2. build request headers (Authorization, X-Phantom-Uid,
    |    X-Phantom-Idempotency-Key=<chain_id>)
    | 3. assign non-empty filename to every multipart part (transparent-proxy invariant)
    | 4. POST {phantom_url}/v1/send
    | 5. parse ChainResponse from response body
    v
+----------- network: HTTP POST {phantom}:8080/v1/send -------+
                                |
                                v
[phantom.routes.send.post_send (under 100 lines)]                <-- phantom
    | dispatches to admit_chain in routes/admission.py
    v
[phantom.routes.admission.admit_chain]                            <-- phantom
    |
    | 1. validate step-header names (RFC 7230 token rule)
    | 2. encode bodies via configured codec; compute body_hash (raw) +
    |    storage_hash (encoded) - before the gate so it admits the STORED size
    | 3. saturation gate admits or refuses (typed AdmissionResult; 503 on refusal)
    | 4. row preparation: auth header -> token cache write; ingress dedup key
    |    (X-Phantom-Idempotency-Key, or minted str(chain_id) when absent);
    |    mode-aware body_location
    | 5. persist: chain_id_in_use pre-check -> chain_id namespace clear (R11-1:
    |    a reused chain_id never inherits a prior occupant's body files) ->
    |    body_store.put -> INSERT UploadRow(state="queued", body_hashes={...})
    |    + idempotency claim in ONE atomic transaction (H7); collision ->
    |    200 replay or typed rejection
    | 6. hybrid-mode size-threshold immediate-persist enqueue; commit the slot
    | 7. return 202 + ChainResponse + X-Phantom-* headers
    v
[Sender worker (workers/sender.py) - poll tick]                  <-- phantom
    |
    | 1. claim_due() claims the queued row, transitions to "attempting"
    | 2. load body_refs:
    |    - read stored bytes -> SHA-256 -> compare to storage_hash (mismatch -> corrupted)
    |    - decode via codec  -> SHA-256 -> compare to body_hash    (mismatch -> corrupted)
    | 3. await executor.execute_one_step(row, body_refs)  -- step 0
    v
[chain.executor.ChainExecutor.execute_one_step] for step 0      <-- phantom
    |
    | a. capture-TTL gate
    | b. substitute placeholders
    | c. inject auth (token_cache.get(endpoint, uid))
    | d. inject idempotency header if declared
    | e. send via upstream_client.send(UpstreamRequest(...))
    | f. parse response; jsonpath.extract per ChainCapture (sensitive flag controls log redaction)
    | g. return Succeeded(captured=..., next_step_index=..., chain_done=...)
    v
[Sender worker - dispatches result via _on_succeeded]
    |
    | _on_succeeded writes new_state="queued" (chain not done) or "succeeded" (done).
    | record_attempt_result persists captured_values_json.
    v
... repeats for each step ...
    v
[Phantom-side terminal state]
    UploadRow {
      chain_id = local_uuid,
      state = "succeeded",
      captured_values = { create_file: {upload_url, file_information} },
      body_hashes = { body: BodyHashes(body_hash=..., storage_hash=...) },
      updated_at = now,
    }
    RAM body store drops the body bytes (succeeded_body=0).

[Later - operator queries]
    GET /v1/admin/chains/{chain_id}
    -> ChainAdminDetail(chain_id, state="succeeded",
                        body_location="ram",   # or "file" if PersistController migrated
                        last_step_completed="put_s3",
                        captured=[...], attempts, last_error)
    Available for `succeeded_metadata_seconds` (default 180s).
```

**The stock-SDK fake-S3 path (a parallel walk).** A stock S3 SDK `PUT`s
to Phantom over plain HTTP. The `catch_all.py` intake resolves the
destination (`?phantom=` carrier or `phantom_default_target`), synthesizes
a one-step `ChainEnvelope`, and hands it to the same `admit_chain`; from
there it is an ordinary one-step chain. The re-sign happens at egress, not
at intake: when the sender runs that step, the executor resolves the route
and, for `auth_mode: aws_sigv4`, calls `sign_sigv4` to re-sign the outbound
request for the real bucket before forwarding. The catch-all itself never
signs: it is forward-as-is; signing is the executor's `aws_sigv4` arm.

---

## 7. Failure modes and how they're handled

Each row names the trigger, the detection mechanism (recovery sweep,
invariant audit, runtime exception, etc.), and the recovery contract
(test path that exercises it). The enumeration combines § 5
invariants, strategy § 6 disaster-prevention rows, and the audit
punch-list failure modes.

| Failure | Trigger / detection | Recovery / response | Regression test |
|---|---|---|---|
| Upstream metadata POST returns 5xx | Sender classifies as `Failed5xx`. | State `attempting → queued` with `next_attempt_at` per the retry strategy. | `tests/e2e/regression/test_v5_retry_cadence_and_crash_survival.py`. |
| S3 PUT returns 5xx (or network error) | Same as above on step 2. | Same. The captured presigned URL is still on the row; retry only re-runs step 2 unless the URL expired. | Same. |
| Presigned URL expires (capture TTL elapsed) | Executor's capture-TTL gate. | Per `row.capture_reexecution_active`: `False` → `stored`; `True` → executor rewinds to the producing step and re-executes with the chain's `idempotency_key`. See ADR-011. | `tests/e2e/test_e2e_06_capture_expiry.py`. |
| Upstream returns 401 (token stale) | Sender classifies as `FailedAuth`. | `token_cache.mark_bad(endpoint, uid)`; row → `auth_expired`. `AdMinter` mints fresh when configured; `AuthKicker` wakes matching rows when a fresh token lands. | `tests/e2e/test_e2e_04_auth_kicker.py`, `tests/e2e/test_e2e_36_token_expiry_recovery.py`. |
| `aws_sigv4` credential missing or rejected | The executor's `aws_sigv4` arm raises `SigV4SigningError` (no credential for the host, or its service has no signer). | Sender marks the slot bad; row → `auth_expired` (NOT terminal). On a fresh credential push for that `dest_host`, the `CredentialKicker` wakes the parked rows (the SigV4 analogue of the 401 cycle). Body bytes are never altered. | `tests/e2e/test_e2e_sigv4_resign_round_trip.py` (`test_sigv4_wrong_credential_parks_auth_expired`, `test_sigv4_refresh_loop_wrong_then_correct_credential`). |
| Producer restart | Process loss. | RAM bodies lost; `body_location='file'` rows survive. Admission's atomic transaction (Phase 1 H7) ensures every committed row has an idempotency claim. RAM rows are quarantined by the recovery sweep. | `tests/e2e/crash_recovery/test_crash_recovery_idempotent.py`. |
| Phantom container crashes mid-persist | The PersistController's commit-last-column ordering means: if the crash happens BEFORE the `body_location='file'` UPDATE commits, the row is still `'ram'`; the recovery sweep quarantines (the RAM body is gone after restart). If the crash happens AFTER, the row is `'file'` with a complete on-disk body file (fsync ran before the UPDATE). | Recovery sweep at startup: reset `attempting → queued`; quarantine stale `body_location='ram'` rows; verify `body_location='file'` rows have their body files (missing → `corrupted` subject to the invariant #1 carve-out). Sender only starts after recovery completes. | `tests/e2e/crash_recovery/test_crash_persist_controller.py`. |
| Body file missing at sender attempt time (Phase 2 H8) | `HybridBodyStore.load_body_refs` raises `BodyMissingError`. | Sender's `_drive_one` cascade transitions the row to `corrupted` with `last_error="storage_corruption:bodies_missing"`. No retry. See ADR-014. | `src/phantom-service/tests/unit/test_h8_body_missing_corrupted.py`. |
| Storage corruption (storage_hash mismatch) | Sender's body-load step verifies `storage_hash` before decode. | Row → `corrupted` (terminal). `last_error="storage_corruption:..."`. No retry. | `tests/e2e/test_multipart_corrupted.py`, `tests/e2e/test_files_lost_midway.py`. |
| Codec round-trip drift (body_hash mismatch after decode) | Sender's body-load step verifies `body_hash` after decode. | Row → `corrupted`. `last_error="codec_round_trip_drift:..."`. No retry. | Same. |
| DB corruption detected at startup (Phase 4) | `storage/integrity.py` runs `PRAGMA integrity_check`; non-`ok` result triggers quarantine. | Back up the corrupted SQLite + body store to flat timestamped siblings (`uploads.corrupted.<stamp>.db` + `bodies.quarantine.<stamp>/`, the ADR-026 stamp) in the instance data_root; bump `db_quarantine_total` counter; surface via `GET /v1/admin/quarantine`. With `db_integrity.fail_open=true` (default), serve with empty fresh state. With `false`, abort startup. | `tests/e2e/crash_recovery/test_corrupt_db_quarantine_boot.py`. |
| Body-file orphans (chain_id absent from `uploads`) | `BodyOrphanJanitor` periodic sweep (two-sweep confirmation + live-row re-read, R6-1). | Janitor deletes orphan files; bumps `orphan_body_count_total`. Closes C1 + invariant #4. | Service unit tests: `src/phantom-service/tests/unit/test_body_orphan_janitor.py`, `test_r6_1_janitor_fresh_entry_race.py`, `test_f2_all_ram_orphan_sweep.py` (C1 doc correction 2026-06-12: the previously cited `tests/e2e/regression/test_orphan_*` path never existed). |
| RAM-pressure breach (`ram_body_store_bytes > ram_ceiling_bytes`) | `RamPressureWatcher` periodic poll. | Signals PersistController; oldest body migrated first. Bumps `ram_pressure_signal_total`. Surfaced via `GET /v1/admin/observability/ram_pressure`. | `tests/e2e/stress/test_ram_pressure.py`, `tests/e2e/stress/test_f1_ram_pressure_attempting_filter.py`. |
| Phantom unreachable from producer | phantom-client's httpx raises `httpx.ConnectError` → SDK raises `PhantomConnectError`. | An upstream client can fall back to its direct-to-upstream path on any non-4xx Phantom failure, logging a structured WARNING so the caller still sees upstream success. The fallback is the upstream client's responsibility, not Phantom's. | Owned by the upstream client's own tests. |
| Saturation cap hit | Ingress consults `SaturationGate.admit(declared_bytes)` and dispatches on the typed `AdmissionResult`. | 503 with `error.code="saturation_cap"` and `Retry-After`. phantom-client `Transport` does NOT retry; an upstream client's fallback delegates to its direct-to-upstream path. | `tests/e2e/test_e2e_11_saturation_cap.py`. |
| Disk-pressure breach | `DiskPressureProbe` observes `max_disk_bytes` exceeded; admission refuses via the saturation gate. | 503 with `error.code="disk_pressure"` and `Retry-After`. Same fallback posture as `saturation_cap`. | `tests/e2e/test_chaos_disk_full_503.py`. |
| Content-Length over `max_buffered_bytes` (Phase 2 H2) | Pre-stream Content-Length check OR mid-stream byte counter. | 413 `body_too_large` with detail keys `{ "declared": ..., "limit": ..., "reason": ... }`. | `tests/e2e/test_e2e_10_parser_error.py`, `src/phantom-service/tests/unit/test_send_route.py`. |
| Malformed envelope | Parser raises `ParserError(code, details)` synchronously. | 422 `ErrorEnvelope`; SDK raises `PhantomValidationError`. | Contract tests. |
| Idempotency replay | Admission detects the idempotency key matches an existing row. | 200 with the prior `ChainResponse` body (NOT 202); no re-admission. Phase 1 H7 closure. | `tests/e2e/test_multipart_idempotent_replay.py`. |
| Worker crashes with an ordinary unhandled exception | Composition-root `asyncio.TaskGroup` cancels every sibling and re-raises out of the lifespan; the CLI fatal-worker bridge stops uvicorn. (`SystemExit`/`KeyboardInterrupt` are outside this bridge.) | Pinned uvicorn 0.46 drains and terminates by SIGTERM. Container orchestrator restarts. Persisted rows survive; recovery resets a post-claim `attempting` row and saturation reconciles `1 → 0`. See § 3.6. | `tests/e2e/regression/test_sender_unknown_fault_supervision.py` (real pre-claim/post-claim subprocess faults and unpatched restart); lower-layer body-missing and TaskGroup semantics remain in `test_h10_silent_route_and_supervision.py`. |

---

## 8. Open concerns

Items the code still has to resolve at implementation time. None
block starting work.

1. **Upstream `Idempotency-Key` semantics are unverified.**
   ADR-011 defaults `capture_reexecution: false` for an upstream
   instance pending real verification that the upstream dedups
   `POST /v2/files` on `Idempotency-Key`. The YAML flip from `false`
   to `true` is an operator action gated on the verification result.
2. **Per-step body-size limit on upstream responses.** The default
   "read it all" is acceptable for small upstream response shapes (<8 KiB)
   but adding a soft cap is a future-proofing nicety.
3. **macOS dev experience for body fsync.** `os.fsync` on
   macOS-on-HFS+ does not strictly fsync without `F_FULLFSYNC`. Unit
   tests don't depend on strict fsync; integration tests on Linux
   do.
4. **Hot reload of instance topology.** SIGHUP and the admin reload
   endpoint do NOT add or remove instances; the operator must
   restart for those. A warning logs the omission when the YAML's
   instance list differs from the running set.

---

## 9. Test coverage shape

Four layers, each with a different role:

- **Per-package unit tests** (`src/<pkg>/tests/unit/`; the emulator
  also carries `tests/smoke/`). pytest, fast (< 5s for the emulator,
  longer for phantom). Every module's acceptance criteria are
  testable. mypy `--strict` passes on every package. There are no
  per-package integration suites.
- **Workspace-root integration tests** (`tests/integration/`). pytest.
  A small cross-package wiring suite (the per-mode happy paths through
  the real composition root).
- **Workspace-root contract tests** (`tests/contract/`). ~240 tests,
  including `test_chain_models_alignment.py` (byte-equality between
  `phantom.models.chain` and `phantom_client.models.chain`) and the
  admin-model alignment tests. `ChainAdminDetail` IS pinned strictly
  by the admin-model alignment test
  (`test_admin_models_alignment.py`); only the chain byte-equality
  test excludes it (see § 5 invariant #7).
- **Workspace-root E2E suite** (`tests/e2e/`). pytest. 127 test
  files across the top level plus the `crash_recovery/`, `regression/`,
  `stress/`, `all_ram/`, `ingress_abort/`, and `db_contention/`
  subdirs - 243 test functions, of which 226 run in the default
  lane (the `load` / `perf` / `stress` markers gate out the rest) and
  one is a designed `xfail` (the ADR-011 capture-TTL re-execution
  case). Counts are as of 2026-07-01 and drift as tests land. Boots
  real Phantom + real emulator, drives through the
  test-owned driver, asserts on three surfaces (producer-side return,
  Phantom admin, emulator's received log). Includes transparent-proxy tests, concurrency tests,
  storage-corruption tests, hot-reload tests, persist-on-receipt
  tests, and (under `-m load`) long-running load tests. See
  `tests/e2e/regression/COVERAGE.md` for the authoritative
  failure-mode -> proving-test map.
- **Fake-S3 / SigV4 coverage.** The emulator's SigV4 sink
  (`routers/s3.py`, validates the re-signed signature) and auth-free raw
  sink (`routers/raw_sink.py`) are the e2e oracles: both accept the
  catch-all's full forwarded upload-verb set (`PUT`/`POST`/`PATCH`) and
  record the inbound verb. The keystone
  `tests/e2e/test_e2e_sigv4_resign_round_trip.py` proves stock PUT →
  catch-all → re-sign → SigV4 sink end-to-end over plaintext;
  `test_e2e_https_listener.py` proves the same path over a real TLS
  listener; `test_e2e_raw_intake_forward_as_is.py` covers `auth_mode:
  none`. The TLS unit test (`src/phantom-service/tests/unit/test_tls_listener.py`)
  owns the in-process HTTPS 200, cert generation/rotation, and the
  cert/key XOR validator. (See the engineering test-suite doc for the
  full named breakdown.)

**Falsifiability tooling.** Each script parses source via `ast` and
asserts a structural invariant:

- `scripts/check_post_send_size.py`: asserts `routes/send.py`'s
  `post_send` is ≤ 100 lines.
- `scripts/check_descriptions.py`: every Pydantic field carries a
  non-empty `description=`.
- `scripts/check_persist_ordering.py`: two constraints. (a) Only
  `workers/persist_controller.py` may call `mark_persisted`; (b)
  inside the persist controller the body-store `put` call precedes
  `mark_persisted` (the persist-handoff ordering invariant; § 5
  invariant #6).
- `scripts/check_atomic_admission.py`: admission writes the row +
  idempotency claim in one SQLite transaction (Phase 1 H7).
- `scripts/check_kv_query_uses_json_each.py`: the key-value lookup
  query keys on the bound-parameter `json_each` form (never an
  interpolated quoted JSON path), so the old-SQLite quote-escape
  version skew cannot return. Run per PR.

### CI pipeline (Phase 0 + Phase 5)

Three workflows under `.github/workflows/`:

| File | Trigger | Jobs |
|---|---|---|
| `per_pr.yml` | every PR + push to main | (1) `ruff check` + `ruff format --check`; (2) per-package `mypy --strict` via `scripts/precommit/run_mypy_per_package.sh`; (3) per-package unit tests (+ emulator smoke); (4) workspace integration + contract tests; (5) the e2e-core job, excluding the `load`, `perf`, and `stress` markers; (6) a separate e2e-load job running `-m load`; (7) the falsifiability scripts above. |
| `nightly_stress.yml` | nightly cron + manual dispatch | `pytest tests/e2e/ -m stress`: the high-volume burst tier. |
| `perf.yml` | manual only (`workflow_dispatch`) | `pytest tests/e2e/ -m perf`: latency/throughput budgets on a quiet runner (they false-fail on loaded shared runners). |

**Run cadence.** Unit + integration: continuously during development.
Contract + e2e-core + load: every change via `per_pr.yml` (e2e-load is
its own per-change job). Stress: nightly via `nightly_stress.yml`.
Perf: manual via `perf.yml`.

---

## Pointers

- **Concept glossary:** [CONTEXT.md](../CONTEXT.md).
- **Decisions:** [docs/adr/](adr/). Scan filenames; deep-read the
  ones relevant to the code you're about to touch.
- **Per-package READMEs:** `src/<pkg>/README.md`.
