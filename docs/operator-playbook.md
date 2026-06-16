# Phantom — Operator Playbook

> The operational reference for deploying, configuring, monitoring,
> and recovering Phantom. Read this after `README.md` and
> `docs/architecture-intent.md`.

## Contents

1. [Deployment topology](#1-deployment-topology)
2. [Configuration walkthrough](#2-configuration-walkthrough)
3. [Mode selection guide (`body_store.mode`)](#3-mode-selection-guide-body_storemode)
4. [Observability + alerting](#4-observability--alerting)
5. [Common failure modes + diagnosis](#5-common-failure-modes--diagnosis)
6. [Routine operations](#6-routine-operations)
7. [Migration from pre-release YAML](#7-migration-from-pre-release-yaml)
8. [Trust boundaries](#8-trust-boundaries)

---

## 1. Deployment topology

### The intended shape

- **One Phantom container per producer.** Phantom is a sidecar to
  the producer's process, not a multi-tenant service. Unit of
  failure = one producer.
- **Pi-class hardware.** The smart defaults (saturation caps, RAM
  ceiling, worker count) are calibrated for 2–8 GiB RAM + 16–256 GiB
  SD-card storage. Rack-server deployments work but should tune
  manually.
- **Loopback admin.** The deployment is same-machine-only: ONE listener
  serves intake + admin + health on one socket, bound to `127.0.0.1` by
  default. The loopback default bind IS the admin access control: nothing
  is reachable off-box by default, so the destructive admin endpoints are
  not either (ADR-004). If operator access from elsewhere is needed, put a
  reverse proxy with auth in front of the loopback bind - that is not
  Phantom's job. A non-loopback `bind_tcp` exposes the whole surface,
  including the unauthenticated admin API, and logs a startup warning; do
  it only behind an authenticating proxy.
- **Health probes ride the one listener.** Liveness (`GET /v1/healthz`)
  and readiness (`GET /v1/readyz`) are public `*z` paths on the single
  listener (`:8080` by default), so a container or orchestrator probe on
  the host reaches them.

### Container build + run

Phantom is published to **both** GHCR and Docker Hub on every release
tag — pull from whichever you prefer:

- `ghcr.io/<org>/phantom-service:<tag>`
- `docker.io/<org>/phantom-service:<tag>`

(Per ADR-020.) Both image streams are produced from the same build by
the release workflow and tagged identically; there is no
"primary"/"secondary" — they are equivalent. Operators using existing
Docker Hub credentials can pull from there directly with no GHCR
authentication setup.

Build from source:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f src/phantom-deploy/Dockerfile \
  -t phantom-service:dev \
  .
```

Run (pick whichever registry you prefer):

```bash
docker run \
  -v /var/lib/phantom:/var/lib/phantom \
  -v /etc/phantom/phantom.yaml:/etc/phantom/phantom.yaml \
  -e PHANTOM_SERVER__BIND_TCP=0.0.0.0:8080 \
  -p 127.0.0.1:8080:8080 \
  docker.io/<org>/phantom-service:<tag>
```

The single listener (intake + admin + health) defaults to loopback, which
docker's port-forward cannot reach from inside a container, so the
container listener must bind `0.0.0.0` (the `PHANTOM_SERVER__BIND_TCP`
override). The host-side mapping (`127.0.0.1:8080:8080`) keeps the port
loopback-only on the machine, so the unauthenticated admin surface is not
exposed beyond the host. A bare-metal run uses the config's loopback
default with no override.

The image entrypoint is `python -m phantom -c /etc/phantom/phantom.yaml`.
The container is config-agnostic — every per-deployment knob lives in
the mounted YAML or in `PHANTOM_*` env vars.

### Reference compose

`src/phantom-deploy/docker-compose.yml` carries a single-machine
reference compose for local dev. Production producer deployments use
Balena / Kubernetes / nomad / systemd-podman — whichever fits the
fleet.

---

## 2. Configuration walkthrough

`config/phantom.yaml.example` is the operator reference — every
field is enumerated with inline comments. The big-picture sections:

| YAML section | What it controls |
|---|---|
| `server` | The single listener's bind address (`bind_tcp`/`bind_uds`). Loopback is the default; admin rides this listener. |
| `storage.data_dir` | Root of the per-instance filesystem layout. |
| `storage.body_store` | Deployment mode (`hybrid` / `all_ram` / `all_disk`), RAM ceiling, persist-linger cadence, body-orphan janitor cadence, invariant-audit cadence. **The most important section.** See § 3 below. |
| `storage.persist_trigger` | Size-aware persist bypass (hybrid mode only). |
| `storage.compression` | Always-encode codec choice (`zstd` / `gzip` / `original`). |
| `storage.sqlite` | Pragma-level SQLite knobs (vacuum cron, synchronous level, WAL journal size). |
| `storage.db_integrity` | Phase 4: fail-open default + optional cold-backup. |
| `saturation` | In-flight caps (probe-filled when unset). |
| `retention` | Per-terminal-state metadata + body retention windows (time-based), plus an optional `max_rows` count-cap backstop. See § 2.1. |
| `retry` | Sender worker count + default retry strategy. |
| `observability` | Log level + log sinks. |
| `instances` | Per-upstream-target configuration; each instance optionally has an `ad_mint` block. |

### 2.1 Retention is time-based; `max_rows` is the optional size backstop

Retention's per-state windows are **time-based**: each terminal state has
metadata + body windows (e.g. `failed` = 30 days, `stored`/`auth_expired`
metadata = forever), and the reaper deletes rows once their window
elapses. `saturation.max_in_flight` bounds only *in-flight* rows — a
terminal row has already released its slot — so it does **not** cap the
table. This means under sustained ingest that produces terminal rows
faster than the windows reap them (a flapping upstream generating
`failed` rows, or steady traffic into the forever-retained
`stored`/`auth_expired` states), the `uploads` table (and its SQLite file
on the producer's SD card) would grow between reaps if nothing bounded the row count.

This is bounded two ways:

1. **The `retention.max_rows` row backstop (default `100_000`).** Every
   instance ships row-bounded: when `max_rows >= 0` the reaper enforces it
   as a hard ceiling *after* the time-based passes. If the table exceeds
   `max_rows` it evicts **oldest-DONE-first** (by `updated_at`) until at or
   below the cap. Only fully-terminal rows
   (`succeeded`/`failed`/`cancelled`/`stored`/`corrupted`) are evictable;
   in-flight (`queued`/`attempting`) and still-deliverable `auth_expired`
   rows are **never** evicted, so the backstop cannot drop an undelivered
   upload. If the overage is all ineligible rows, the cap is left unmet
   (durability wins over the count cap) and the shortfall is logged. Raise
   or lower it per the SD-card size on the producer; set `-1` to opt into the
   historical unbounded (time-only) contract.
2. **Size for the forever-retained states.** Independently of the row cap,
   estimate `peak_terminal_rows × avg_row_bytes` for `stored`/`auth_expired`
   over your retention horizon and provision the SD card accordingly.

`max_rows` is a backstop, not a substitute for the time windows — set it
generously above steady-state so it only fires under pathological growth.
It is hot-reloadable (lands on the next reaper sweep).

### 2.2 Durability knob: `storage.sqlite.synchronous`

`uploads.db` runs WAL with `synchronous: NORMAL` by default:
corruption-safe on consumer flash, with a bounded exposure window (the
last few seconds of commits) on a hard power cut. For power-loss-strict
producers (switched power, no UPS, where losing even seconds of accepted
rows is unacceptable) set:

```yaml
storage:
  sqlite:
    synchronous: "FULL"
```

FULL fsyncs the database per commit, so a hard power cut can no longer
lose recently-committed rows; the cost is fsync latency on every
admission and more SD wear (`EXTRA` exists for the strictest posture).
FULL alone protects row metadata, not RAM-resident bodies: power-loss-
strict producers pair it with `body_store.mode: all_disk` (§ 3), which
makes the body bytes themselves durable before the 202. The token cache
always runs FULL on its own database file (ADR-030); this knob governs
`uploads.db` only.

### Validating a config without binding

```bash
uv run python -c "
import yaml
from phantom.config.settings import Settings
cfg = yaml.safe_load(open('/etc/phantom/phantom.yaml'))
Settings.model_validate(cfg)
print('OK')
"
```

### Hot reload

```bash
kill -HUP <pid>
# or
curl -X POST http://127.0.0.1:8080/v1/admin/reload
```

Reloadable: retention, saturation caps, codec choice (applies to
admissions after the reload), persist trigger size threshold,
capture re-execution, retry params, the admin lookup binding, and
body-store tuning (linger, RAM ceiling, RAM-pressure poll).
**Restart-required:** worker count, the instance list
(adding/removing an instance), `body_store.mode`, and every
`ad_mint` knob including the refresh timings (the reload logs a
WARNING when `ad_mint` changes). See ADR-013.

---

## 3. Mode selection guide (`body_store.mode`)

Phantom ships three first-class deployment modes. The choice is per-
deployment, not per-upload. All three are tested and supported.

### Questions to answer

1. **How much RAM does the producer have?**
   - <2 GiB → consider `all_disk`.
   - 2–8 GiB (typical Pi-class producer) → `hybrid` is the default.
   - >8 GiB and you want max throughput → `hybrid` still wins
     unless body sizes are unpredictably large.

2. **How much disk does the producer have, and is it SD-card flash?**
   - SD card → keep healthy uploads in RAM (default: `hybrid`).
     The `linger_seconds=90` default keeps successful uploads off
     the SD entirely.
   - eMMC / spinning rust / SSD → `all_disk` is fine if you want
     bullet-proof durability and don't mind the per-admission
     fsync cost.

3. **How much data-loss-on-restart can you tolerate?**
   - Zero → `all_disk`. Every admission is on disk before the 202;
     restarts lose nothing.
   - "Last few seconds" — `hybrid` (default). Successful uploads
     drop body bytes immediately; pending-retry uploads migrate to
     disk after `linger_seconds=90`; in-flight RAM bodies at
     restart are lost (and the recovery sweep quarantines the
     stale rows).
   - "I'm running tests; data loss is fine" → `all_ram`. RAM only;
     restart wipes everything. Use only for E2E tests.

4. **How predictable are body sizes?**
   - Mostly small (<100 MiB) → `hybrid` with the default
     `body_size_threshold_bytes` is fine.
   - Occasional huge bodies → `hybrid` with a small
     `body_size_threshold_bytes` (e.g. 50 MiB) so the giants land
     on disk immediately without holding RAM hostage.

### The three modes

#### `hybrid` (production default)

```yaml
storage:
  body_store:
    mode: "hybrid"
    # linger_seconds: 90        # default
    # ram_ceiling_bytes: null   # probe-filled
```

- Admission writes the body to `RamBodyStore`.
- `PersistController` migrates RAM bodies to disk on **linger**
  (default 90 s without reaching a terminal state) OR **RAM
  pressure** (`ram_body_store_bytes > ram_ceiling_bytes`).
- Successful uploads drop body bytes immediately; metadata
  persists for `succeeded_metadata_seconds` (default 180 s).
- Trade-off: best happy-path performance; small risk of RAM-body
  loss on restart for in-flight uploads (offset by the recovery
  sweep + idempotent retry from the producer).

#### `all_ram`

```yaml
storage:
  body_store:
    mode: "all_ram"
```

- Every body stays in RAM until terminal-or-restart. **Restart
  wipes every in-flight body** (the recovery sweep quarantines
  any stale `body_location='file'` rows that somehow exist).
- No PersistController spawned.
- Trade-off: zero disk wear; zero durability across restarts.
  Useful for ephemeral test deployments where the producer will
  re-submit on any failure.

> **Destructive mode-flip guard (back-up-and-run).** `all_ram` wires a
> RAM-only body store with no disk awareness. Switching **into** `all_ram`
> on a data dir that previously ran `hybrid` or `all_disk` and still holds
> body files on disk would, left unguarded, be a data-loss + disk-leak
> operation: recovery would condemn every `body_location='file'` row to
> `corrupted` (its bytes are intact on disk, but `all_ram` has no disk-aware
> store to find them), and no orphan janitor runs to sweep the stranded
> files. Phantom does **not** fail closed on this (ADR-025): instead it
> **backs up and runs**. At startup it relocates the live database and the
> per-instance `bodies/` tree to a recoverable `mode_switch` backup pair
> (`uploads.mode_switch.<stamp>.db` and `bodies.mode_switch.<stamp>/` beside
> them, where `<stamp>` is the display iso plus the first 8 hex chars of the
> backup's `backup_id`, and a `backup.<backup_id>.manifest.json` names the
> pair), bumps the `mode_switch_backup_total` counter, logs a loud WARNING,
> and boots fresh over the now-empty live tree. No data is lost (the bytes
> sit in the backup, neither corrupted nor leaked) and the service stays up.
> The backup is listed by `GET /v1/admin/quarantine` with `reason=mode_switch`
> and moved back into the live tree by `POST /v1/admin/quarantine/restore`.
> To return to a durable posture: set `hybrid` or `all_disk`, restart, then
> restore (see the matrix below and § 5 *Retrievability*). If the buffered
> uploads are disposable, simply delete the `mode_switch` backup artifacts.

#### `all_disk`

```yaml
storage:
  body_store:
    mode: "all_disk"
```

- Every admission writes the body directly to `FileBodyStore`
  (atomic rename + fsync of file AND parent dir) BEFORE the 202.
- No PersistController spawned; no RAM body store.
- Trade-off: bullet-proof durability; pays an fsync cost per
  admission. Pick for producers where zero data loss > admission
  latency.

### Switching modes (the mode-switch matrix)

You change a mode by editing `storage.body_store.mode` and **restarting**
on the same `data_dir` (it is not a hot-reload knob: worker wiring differs
per mode). What happens to data already on disk depends only on the mode you
are switching **into**:

| Switch (from → to) | What Phantom does on the next boot | Buffered data |
| --- | --- | --- |
| `hybrid` → `all_disk` | Nothing special. `FileBodyStore` adopts the existing `bodies/` tree; recovery re-queues in-flight rows. | Preserved, keeps delivering. |
| `all_disk` → `hybrid` | Nothing special. RAM store for new admissions; the existing disk bodies are served + drained normally. | Preserved, keeps delivering. |
| `hybrid` ↔ `hybrid`, `all_disk` ↔ `all_disk` | No-op restart. | Preserved. |
| any disk-backed → `all_ram`, **empty** `bodies/` | Boots `all_ram` directly. | None on disk to worry about. |
| any disk-backed → `all_ram`, **populated** `bodies/` | **Back-up-and-run** (see the guard callout under `all_ram`): the live DB + `bodies/` are moved to a `mode_switch` backup pair and the service boots fresh. `mode_switch_backup_total` bumps. | Preserved in the backup (NOT lost), restorable. |
| `all_ram` → `hybrid` / `all_disk` | Nothing special (the `all_ram` live tree has no disk bodies to guard). | RAM bodies were already gone on the `all_ram` restart by design; metadata rows survive. |

**The only switch that sets data aside is the unsafe one:** selecting
`all_ram` over a populated disk tree. Every other switch is a plain restart
that preserves and keeps delivering buffered work. Switching *out of*
`all_ram` is always safe.

**Recovering a `mode_switch` backup.** After an unsafe switch backed your
data up, the durable-recovery workflow is:

1. Confirm the backup: `GET /v1/admin/quarantine` lists ONE entry per
   backup (manifest-driven) with `reason=mode_switch`, its `backup_id`
   (the restore handle), an `iso_display` stamp (display and sort only),
   and `has_db` / `has_body` flags reporting what is on disk right now.
   An artifact with no manifest surfaces as a flagged anomaly entry
   (`backup_id` null); anomalies are never restorable.
2. Set `body_store.mode` back to `hybrid` or `all_disk` (a disk-backed mode
   that can actually serve restored bodies) and restart.
3. Restore it:
   `POST /v1/admin/quarantine/restore?backup_id=<backup_id>&instance=<id>`
   (or the typed client's `restore_quarantine_backup(backup_id=...)`). The
   restore first backs up the current live tree (clobber-safe), then moves
   the chosen backup into the live position; a backup whose DB half is
   missing is refused 409 up front, before any live data is displaced. It
   is **restart-required**: the response says `restart_required=true`
   because the running store still holds the old database file descriptor.
4. Restart once more so the freshly-restored database is served. The
   buffered rows are live again and resume delivering.

The backup move is crash-safe end-to-end: an in-progress marker means a
power-loss mid-backup (or mid-restore) is finished forward on the next boot,
so you never end up with a half-moved tree. See § 5 *Retrievability after a
switch, rerun, kill, or reload* for the full durability picture.

---

## 4. Observability + alerting

### Structured logs

Phantom emits JSON-line logs at the configured `observability.log_level`.
Bearer tokens are redacted by the `bearer_redaction` filter; sensitive
captured values (marked `sensitive=True` per ADR-009/010) are
redacted by the `SensitiveCaptureRedactor` filter.

Key log events to alert on:

| Event | Severity | What it means |
|---|---|---|
| `db_quarantine` ERROR | ERROR | SQLite corruption detected at startup; the live DB and body tree moved to a flat backup pair in the instance data root (`uploads.corrupted.<stamp>.db` + `bodies.quarantine.<stamp>/`, named by a `backup.<backup_id>.manifest.json`). Service is serving with fresh empty state if `fail_open=true`. |
| `invariant_violation` ERROR | ERROR | InvariantAuditor row walk found a violation. Inspect the `label` field. |
| `body_orphan_swept` INFO | INFO (high rate = WARNING) | BodyOrphanJanitor deleted orphan file(s). Normal at low rate; high rate indicates a bug. |
| `ram_pressure_signal` INFO | INFO (high rate = WARNING) | RamPressureWatcher signaled the PersistController. High rate means the RAM ceiling is too tight. |
| Worker crash + `TaskGroup` cancellation | ERROR | Composition root's TaskGroup is exiting the lifespan; the container should restart. |

### Counters and gauges

Loopback-only via `GET /v1/admin/observability/*`. Scrape into your
metrics system of choice (Prometheus / Vector / OTel collector
running on the host).

| Counter | Meaning | Alert if |
|---|---|---|
| `invariant_violation_total` (any label-value bucket) | Phase 3 audit found a violation. | **non-zero on any sample** - CI rule. |
| `orphan_body_count_total` | Orphan files removed per janitor sweep. | High rate sustained. |
| `record_attempt_result_no_op_total` | Sender UPDATE matched 0 rows (admin cancel/replay raced with sender). | High rate = bug. |
| `ram_pressure_signal_total` | RAM-pressure event fired. | High rate = ceiling too tight. |
| `db_quarantine_total` | DB-corruption quarantine fired at startup. | non-zero → operator investigates. |
| `cold_backup_total` / `cold_backup_failed_total` | Cold-backup outcomes. | failed rate > 0. |

| Gauge | Meaning | Alert if |
|---|---|---|
| `saturation_balance` | In-flight declared bytes. | Climbing to the cap. |
| `attempting_row_count` | Current `attempting` rows. | Spike + sender heartbeat gone → sender died mid-flight. |
| `body_location_distribution{value=ram\|file}` | Count by `body_location`. | `ram` count growing unbounded → linger/ceiling not migrating fast enough. |
| `persist_controller_queue_depth` | Current enqueued migrations. | Growing unbounded = controller falling behind. |
| `ram_body_store_bytes` | RAM-body-store bytes. | Approaching `ram_ceiling_bytes`. |

### RAM-pressure dashboard

`GET /v1/admin/observability/ram_pressure` returns:

```json
{
  "ram_body_store_bytes": 412057600,
  "ram_ceiling_bytes": 536870912,
  "fraction": 0.767,
  "over_ceiling": false
}
```

For live RAM-pressure visualization without scraping.

### Reading the parked backlog

"Parked" work is the operator-owned non-success backlog: rows that have
stopped advancing on their own and are waiting for an operator decision
or an external event. Two states make it up:

- **`stored`** (terminal): the retry budget was exhausted but the body is
  still recoverable on the producer (you can inspect or re-drive it).
- **`auth_expired`** (non-terminal): the cached token 401'd without a
  successful refresh, so the row is waiting for a fresh token (the
  `AuthKicker` re-queues it the moment one lands).

You can read the whole parked picture in a single call:

```bash
curl -s http://127.0.0.1:8080/v1/admin/stats
```

| Field | What it tells you |
|---|---|
| `by_state.stored.count` / `by_state.stored.bytes` | How many stored rows and how many body bytes they hold (the recoverable backlog footprint). |
| `auth.auth_expired_count` | How many rows are stuck waiting for a token. |
| `parked_total` | The single-number backlog: `stored.count + auth_expired_count`. |

Alert on `parked_total` climbing: a rising `auth_expired_count` means a
credential slot needs attention (see § 5 *Auth failures*); a rising
`by_state.stored` means uploads are exhausting their retries (a sick
upstream, or retry params too tight). `parked_total` pairs with the
`max_rows` backstop (§ 2.1): the parked states are the forever-retained
ones that drive table growth, so watch both together.

### Group and identifier queries

Three loopback reads (all accept `?instance=<id>` to scope one
instance; without it they fan out across every instance). Typed client:
`get_group_status`, `find_by_captured_id`, `find_by_local_uuid`, plus
`poll_group_until_finished`.

```bash
# Everything about one group in a single call: per-state counts,
# all_finished, first_received_at / last_sent_at, receipt-ordered members.
curl -s http://127.0.0.1:8080/v1/admin/groups/<group_id>

# Find uploads by the upstream-assigned identifier captured at delivery.
# Requires the per-instance admin_lookup config binding (capture_name +
# json_path); an unconfigured instance answers 400 lookup_not_configured.
curl -s http://127.0.0.1:8080/v1/admin/uploads/by-captured-id/<id>

# Find uploads by the producer-side correlation uuid (submitted as the
# phantom_local_uuid metadata key). No config needed.
curl -s http://127.0.0.1:8080/v1/admin/uploads/by-local-uuid/<uuid>
```

- `all_finished` is true iff no member is `queued` / `attempting`.
  `auth_expired` and `corrupted` count as finished (neither progresses
  without intervention); a token push that revives an `auth_expired`
  member honestly flips the flag back while it re-attempts.
- The group rollup 404s only when NO row carries the id. A `chain_id`
  resolves to its singleton group (every upload is a group of one).
- A lookup miss is HTTP 200 with `found=false` (the question is a
  membership test, not a resource fetch); multiple matches are returned
  honestly, sorted by receipt time.

---

## 5. Common failure modes + diagnosis

### DB quarantine

**Symptom.** Startup log carries an ERROR with quarantine
destination. `db_quarantine_total` counter incremented.
`GET /v1/admin/quarantine` returns one or more entries.

**Cause.** `PRAGMA integrity_check` returned non-`ok` at startup —
SQLite corruption from a hard power-cut + SD-card-wear, or actual
hardware failure.

**Procedure.**

1. Stop the container (`docker stop`).
2. **Preserve the backup artifacts**: copy the quarantine pair and its
   manifest out of `<data_dir>/<instance.data_dir>/`
   (`uploads.corrupted.<stamp>.db`, `bodies.quarantine.<stamp>/`, and
   `backup.<backup_id>.manifest.json`) off the device for forensics.
3. With `db_integrity.fail_open=true` (default), the service
   already restarted with fresh empty state. Verify by checking
   the post-quarantine log + `GET /v1/admin/status`.
4. The producer's pending uploads in the quarantined DB are lost.
   Recovery options:
   - If the producer still has the bodies in its in-memory state,
     re-submit.
   - If the producer already discarded them, the upload is gone (the
     producing work must be re-run).
5. Restart the container.

If `db_integrity.fail_open=false`, the container won't start until
the operator either restores from backup or accepts the fresh
empty state by deleting the corrupted DB.

**Power loss DURING quarantine is self-healing.** Quarantine renames the
body-store root first and the corrupt DB last. The corrupt DB is the
re-trigger: if the producer loses power mid-quarantine, the corrupt DB is left
in place, and the next boot's integrity gate simply re-quarantines it and
continues — in **every** body-store mode, including `all_ram`. No manual
clearing of the per-instance `bodies/` tree is needed after a mid-quarantine
power loss. (The mode-switch backup move is likewise crash-safe via its own
in-progress marker — see § 3 *Switching modes* — so neither a mid-quarantine
nor a mid-mode-switch power loss leaves a wedged half-state.)

### RAM pressure

**Symptom.** `ram_body_store_bytes` gauge near `ram_ceiling_bytes`.
`ram_pressure_signal_total` high rate. PersistController queue depth
climbing.

**Cause.** The `ram_ceiling_bytes` is too tight for the
upload volume, OR the upstream is too slow and bodies linger.

**Ceiling enforcement.** `ram_ceiling_bytes` is an *enforced* bound, not
a best-effort gauge. When the RAM total breaches the ceiling, the
`RamPressureWatcher` migrates oldest-first RAM bodies to disk. A body
whose row is mid-attempt is skipped only while the attempt is *fresh*
(started within ~2× `ram_pressure_poll_seconds`); an attempt stalled
longer — e.g. a slow or unreachable upstream holding the sender pool —
is migrated anyway. This is the F-P8-A fix: previously a slow upstream
that pinned every oldest RAM row in `attempting` let RAM climb past the
ceiling unbounded (OOM risk on a small producer). Migrating a stalled body is
safe — it is fsynced to disk before its RAM copy is dropped, and the
sender re-reads from disk on its next attempt.

**Procedure.**

1. Check upstream health — if upstream is healthy, raise
   `body_store.ram_ceiling_bytes` (hot-reloadable).
2. If upstream is unhealthy, the bodies are migrating to disk per
   design — verify `body_location_distribution{value=file}` is
   climbing and disk usage is acceptable. RAM stays bounded by the
   ceiling even while the upstream is stalled.
3. If the disk is also filling up, consider raising
   `saturation.max_in_flight_bytes` and `max_disk_bytes` to
   match the new traffic shape OR lowering them to back-pressure
   the producer.

### Body-too-large rejections (413)

**Symptom.** Producer logs HTTP 413 with `error.code="body_too_large"`.

**Cause.** Body size exceeds `storage.max_buffered_bytes` (default
2 GiB).

**Procedure.**

- If the body is legitimate and the deployment can afford the
  RAM/disk, raise `storage.max_buffered_bytes` (hot-reloadable).
- If the body is a bug (the producer generating runaway artifacts),
  fix the producer.

### Auth failures (401)

**Symptom.** Producer logs HTTP 401 with `error.code="auth_token_missing"`,
OR rows accumulate in `auth_expired` state.

**Cause path 1: missing token at admission.** Producer didn't send
`Authorization` header on a `phantom_bearer` route and no cached
token exists for the `(endpoint, uid)` slot. Have the producer push a
token via the `Authorization` header on any subsequent request, or
via `PUT /v1/admin/tokens/{endpoint}/{uid}` on the admin API.

**Cause path 2: cached token rejected.** Upstream returned 401 to
Phantom; sender marked the token `bad` and parked the row in
`auth_expired`. When a fresh token lands, the `AuthKicker` wakes
all matching rows. Diagnose via:

```bash
curl 'http://127.0.0.1:8080/v1/admin/tokens?endpoint=<endpoint>'
# lists TokenSlot entries { status: "bad" | "fresh" | "unknown", ... };
# bearer values are never returned. There is no per-slot GET: the read
# surface is this list. Writes: PUT /v1/admin/tokens/{endpoint}/{uid};
# removal: DELETE /v1/admin/tokens/{endpoint}/{uid}.
```

For instances with `ad_mint` configured, the minter mints fresh
tokens automatically; the operator surface for ad-mint failures
is the log + the `auth_expired` row count.

### Idempotency-key conflicts (422) and chain_id reuse (409)

**Symptom.** Producer logs HTTP 422 with
`error.code="idempotency_key_conflict"`, OR HTTP 409 with
`error.code="chain_id_in_use"`.

**The idempotency contract.** An `X-Phantom-Idempotency-Key` MUST be a
function of the request body **and the destination** — the operation a
key names is "deliver THESE bytes to THIS destination". Reusing one key
for a *different* body, OR for the same body but a *different* destination
(the resolved per-step `(method, URL)` of the chain), is a client bug:
the first submission is already buffered, and the second would be
silently dropped behind a 200 replay (delivered to the original place).
Phantom rejects both with `idempotency_key_conflict` (422) instead of
acknowledging a submission it will never deliver as sent — the producer learns
immediately that its key reuse dropped data. The same key with the *same*
body *and same destination* is a legitimate replay and still returns the
original `ChainResponse` (HTTP 200). Note the idempotency identity covers
body + destination only: two submissions that differ only in a step-body
field (e.g. an embedded `file_name` or `phantom_local_uuid`) but share
key + body + destination still dedup. Derive the key from a hash of the
body (or a body-unique id) so retries dedup and distinct uploads never
collide.

**chain_id reuse.** The envelope `chain_id` is the row primary key. A
re-POST of a `chain_id` already held by a live row returns
`chain_id_in_use` (409). Client-supplied UUID4 collisions are
astronomically unlikely, so this almost always means the producer reused an
id — mint a fresh `chain_id` per submission. (Earlier builds let this
escape as a naked HTTP 500; it is now a deterministic 409 with the
ADR-017 error envelope.)

**Procedure.** Both are caller-side contract violations — fix the producer's
key/chain_id derivation. No operator action on Phantom is required.

### Retrievability after a switch, rerun, kill, or reload

The recurring operator question is "if I restart, redeploy, kill, or
hot-reload Phantom, what happens to work already accepted?" The answer in
normal operation is: **undelivered work survives, is re-queued, and stays
visible.** This is reliability invariant #1: no accepted upload is lost
while Phantom is running normally.

- **The SQLite metadata record always survives** every restart, in
  **every** body-store mode, including `all_ram`. The row, its state, its
  captured values, and its `last_error` are durable. So after any of
  these events the row is still queryable via
  `GET /v1/admin/chains/{chain_id}` and counted in
  `GET /v1/admin/stats`.
- **A clean restart or process kill** re-queues in-flight rows: the
  recovery sweep resets `attempting` rows back to `queued` so the sender
  picks them up again. A `queued`/`attempting` row is never dropped by the
  `max_rows` backstop (§ 2.1).
- **A hot reload (SIGHUP / `POST /v1/admin/reload`)** swaps config
  atomically and never drops rows; only the restart-required knobs in
  § 2.1 *Hot reload* (worker count, the instance list,
  `body_store.mode`, `ad_mint`) need a full restart.
- **A recoverable mode switch** (selecting `all_ram` on a data dir that
  still holds disk bodies from a prior disk-backed run) does not destroy
  that data: Phantom relocates the live DB and body tree to a set-aside
  **mode-switch backup** and boots fresh (the back-up-and-run guard,
  § 1 / ADR-025). The backup is reachable via
  `GET /v1/admin/quarantine` (it appears with `reason=mode_switch`) and
  is moved back into the live tree by the one-call, restart-required
  `POST /v1/admin/quarantine/restore`. To resume service in a durable
  posture, set `hybrid` or `all_disk` and restart, then restore.

**The one caveat: `all_ram` bodies across a restart.** In `all_ram`
(see § 3) a body lives only in RAM until its row reaches a terminal
state. A body that was never migrated to disk is **gone on any restart by
design**; that is the explicit `all_ram` trade-off (zero disk wear, zero
cross-restart body durability). The row itself still survives and stays
recoverable: Phantom marks it `corrupted` with reason
`ram_body_lost_on_restart`, so `GET /v1/admin/chains/{chain_id}` shows
both that the row exists and *why* its body is unavailable. The metadata
is retrievable; the bytes are not. Run a disk-backed mode if
cross-restart body durability matters.

---

## 6. Routine operations

### Vacuum

Cron-scheduled SQLite VACUUM via `VacuumScheduler`. Default
`storage.sqlite.vacuum_cron: "0 3 * * 0"` (Sunday 03:00). Fires
only when `in_flight == 0`. No operator action needed in normal
operation. To force a vacuum, restart at an idle moment.

### Cold backup

Opt-in via `storage.db_integrity.backup_enabled: true`. Snapshots
the live SQLite to `<data_dir>/backups/` on `backup_period_seconds`
cadence (default daily), keeping the last `backup_rotate_n` (default
3). Use for deployments where DB-corruption recovery time matters
more than disk space.

To restore from backup (with the service stopped):

```bash
cp <data_dir>/backups/<timestamp>/uploads.db <data_dir>/<instance>/uploads.db
```

### Producer deployment via Balena (or any fleet manager)

The container is config-agnostic. Per-deployment customization is
in the mounted YAML (or `PHANTOM_*` env vars). The fleet manager
handles image distribution + restart-on-config-change.

---

## 7. Migration from pre-release YAML

The Phase 1 refactor renamed and removed several keys. An
unmigrated YAML will fail Pydantic validation at startup. Apply
this migration table BEFORE bringing up the new service:

| Old YAML key | New YAML key | Notes |
|---|---|---|
| `storage.in_memory_max_bytes` | `storage.body_store.ram_ceiling_bytes` | Same semantic; renamed. Probe-filled when unset. |
| `storage.default_tier: memory` | `storage.body_store.mode: hybrid` | The new `hybrid` mode is the production default. |
| `storage.default_tier: persisted` | `storage.body_store.mode: all_disk` | If you wanted every admission on disk. |
| `storage.persist_trigger.body_size_threshold_bytes` | `storage.persist_trigger.body_size_threshold_bytes` | Key + semantic preserved. Hybrid-mode behavior is more focused now (see operator notes). |
| `storage.persist_trigger.after_attempts` | **REMOVE the key** | Use `body_store.mode: all_disk` if you want every body on disk; `body_size_threshold_bytes` for size-based triggers. |
| `storage.persist_trigger.after_seconds` | **REMOVE the key** | The PersistController's `linger_seconds` covers it. |
| `storage.sqlite.autovacuum` | **REMOVE the key** | `auto_vacuum` is hardcoded to NONE in code (SD-card-wear rule). If you had `true`, that was wrong; the corrected behavior is NONE. |
| `storage.sqlite.synchronous: FULL` | `storage.sqlite.synchronous: NORMAL` | **Default changed.** Pi-class deployments should accept the new default (WAL-corruption-safe + better SD wear). Rack-server deployments with battery-backed write cache may prefer explicit `"FULL"`. |
| `device_profile` block | (none) | Removed in Phase 1. Probe runs unconditionally; pin individual fields in YAML to override. |
| `routes[*].strategy` | (none) | Removed in Phase 0. Retry strategy is per-instance, not per-route. |
| `instances[*].routes[*].presigned_ttl_seconds_override` | (none) | Removed in Phase 0 (dead code). |

**Container image:** The production image is at
`ghcr.io/<org>/phantom-service:<tag>` per ADR-020.

---

## 8. Trust boundaries

Per WS-4 Finding 6 — the operator-facing trust model:

- **Inside the container** — Phantom trusts its own filesystem,
  its own SQLite, its own RAM. `os.umask(0o077)` is set at
  startup so any new files/dirs are owner-only.
- **Loopback admin** — Phantom trusts any caller that can reach
  the loopback bind. The admin surface rides the single listener,
  which defaults to `127.0.0.1`, so the loopback default bind IS the
  admin access control; admin endpoints don't authenticate (ADR-004).
  If the host is shared, restrict access via firewall rules + a reverse
  proxy with auth (out of scope for Phantom). A non-loopback `bind_tcp`
  is a deliberate opt-in and warns.
- **Ingress** — Phantom does NOT authenticate inbound traffic
  beyond the route's `auth_mode`. The producer's network reachability
  to Phantom IS the auth boundary.
- **Bare-metal vs containerized.** On bare metal, the operator
  is responsible for the umask + filesystem permissions. In a
  container, the umask runs in the container's filesystem
  namespace (the image's user); volume mounts inherit the host's
  filesystem permissions.
- **Quarantine backups** - the flat backup pairs and manifests in each
  instance data root (`uploads.corrupted.<stamp>.db`,
  `bodies.quarantine.<stamp>/`, `uploads.mode_switch.<stamp>.db`,
  `bodies.mode_switch.<stamp>/`, `backup.<backup_id>.manifest.json`)
  carry potentially-corrupted DBs + bodies. Treat with the same
  permissions discipline as the live data dir.

---

## See also

- [`config/phantom.yaml.example`](../config/phantom.yaml.example) —
  the operator-facing config reference.
- [`docs/architecture-intent.md`](architecture-intent.md) — runtime
  topology, invariants, failure modes.
- [`CONTEXT.md`](../CONTEXT.md) — domain glossary.
- [`docs/adr/017-error-code-matrix.md`](adr/017-error-code-matrix.md)
  — every HTTP status + reason code Phantom emits.
- [`docs/adr/020-container-image-as-deployment-artifact.md`](adr/020-container-image-as-deployment-artifact.md)
  — container deployment shape.
