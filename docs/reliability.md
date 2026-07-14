# Phantom: Reliability and Robustness

Status: current-authority. Authored 2026-05-29 from the pre-push
adversary/defender hardening cycle. Maintain this file when a new
failure mode is hardened or a proving test is added or moved.

Phantom's single job is **durability**: an acknowledged upload is never
silently lost, and the service survives crash, power loss, storage
faults, and contention on Pi-class field hardware, resuming on restart.
This document is the project-facing record of *what Phantom is hardened
against* and *how to reason about each failure*. It is the prose
companion to the end-to-end suite: the tests prove the behavior; this
document indexes and explains it.

It has two halves:

- **[Part 1. Operator-facing](#part-1-operator-facing-what-phantom-survives)**:
  what Phantom survives, in plain language. Each failure mode names the
  trigger, what an operator observes, and which `body_store.mode` (if
  any) changes the picture.
- **[Part 2. Contributor-facing](#part-2-contributor-facing-failure-mode--proving-test-map)**:
  a failure-mode → proving-test map. Each row links a failure mode to
  the exact test path that proves the contract, so a reader can run the
  proof.

Authority order is unchanged: the ADRs
([docs/adr/](adr/)) are the settled decisions;
[docs/architecture-intent.md](architecture-intent.md) is the onboarding
map; [CONTEXT.md](../CONTEXT.md) is the glossary. This document
aggregates the durability story those sources establish and adds the
test index. Where a behavior turns on a settled decision, the relevant
ADR is cited inline.

---

## Background: the storage model in one paragraph

Each instance owns THREE SQLite databases (all WAL journal, all under
the `auto_vacuum=NONE` hard rule for flash wear): `uploads.db`
(operator-tunable `synchronous`, default NORMAL) carrying `uploads` and
`idempotency_index`; `token_cache.db` (`synchronous=FULL`) carrying the
persistent token cache, its own file so token writes never contend with
the hot uploads writer (ADR-030); and `credential_store.db`
(`synchronous=FULL`) carrying the host-keyed Destination credential
store for `aws_sigv4` routes, its own file for the same
writer-contention reason. Body bytes live in RAM, on disk, or both,
selected per deployment by `body_store.mode`:

- **`all_disk`**: every body is written to disk (atomic rename plus
  fsync of the file *and* its parent directory) before it is considered
  durable.
- **`all_ram`**: bodies live only in a RAM dictionary; there is no disk
  body store and no body janitor. A crash loses every not-yet-delivered
  body by design.
- **`hybrid`** (the field default): bodies start in RAM for low-latency
  acknowledgement, and the `PersistController` migrates a body to disk
  on retry-linger (default 90 s) or under RAM-ceiling pressure. The flip
  of `body_location` from `'ram'` to `'file'` is the single durability
  commit point, and everything that makes the file durable runs *before*
  that flip commits (the commit-last-column ordering invariant).

Integrity is enforced by two SHA-256 hashes per body (`body_hash` of the
raw submitted bytes, `storage_hash` of the stored encoded bytes; see
[ADR-014](adr/014-dual-body-hash.md)). Any mismatch sends the row to the
terminal `corrupted` state rather than forwarding bad bytes.

---

# Part 1. Operator-facing: what Phantom survives

Each subsection states the trigger, what you observe, and the
mode-specific notes. The recurring principle: **a fault is either
ridden out, surfaced as a clean retryable signal, or quarantined to a
terminal state: never a silent loss and never a wedged service.**

## 1.1 Crash and power loss (recovery sweep)

**Trigger.** The process dies abruptly: power loss, OOM-kill, container
SIGKILL, or an ordinary unhandled worker exception that cascades out of the
composition root's `asyncio.TaskGroup`. For the worker case, the production
CLI's fatal-worker bridge stops uvicorn; pinned uvicorn 0.46 drains and
re-raises SIGTERM, producing signal termination. Uvicorn's default post-start
lifespan handling would only log the failure. TaskGroup's direct
`SystemExit`/`KeyboardInterrupt` special cases are outside this bridge.

A deliberate consequence: a row that deterministically raises an unknown
fault crash-loops the service, because each restart's recovery requeues the
row and a worker reclaims it. There is no poison-row quarantine, and
`attempts` does not increment on a crashed attempt, so no counter demotes
such a row. The design prefers a loud restart loop over a silently degraded
process; breaking the loop means fixing the fault or removing the row (admin
cancel/replay can take a row out of circulation).

**What happens.** The orchestrator restarts the container. Before the
sender pool opens, a boot-time **recovery sweep** runs:

1. Every `attempting` row is reset to `queued` (no in-flight row
   survives a restart; there is no live attempt behind it any more).
2. `body_location='file'` rows whose body files vanished are quarantined
   to `corrupted`, *except* a delivered row that already discarded its
   body (the `body_discarded_at` carve-out), and *except* any row
   already in a terminal state (see below).
3. `body_location='ram'` rows are quarantined: the RAM store is empty
   after a restart by design, so the body is genuinely gone.
4. After those state corrections, the fresh in-memory saturation gate is
   reconstructed from every persisted row that still holds a slot. A paused
   upstream therefore exposes recovered backlog as live `in_flight` capacity
   before delivery resumes; terminal release returns it to zero.

**What you observe.** The service comes back up healthy. Rows that were
durably on disk resume retrying with their last-known token. Rows whose
bodies were RAM-only at the crash instant appear in the admin surface as
`corrupted` (queryable via `GET /v1/admin/chains/{chain_id}`). They are
not lost without trace.

**Hardened details worth knowing:**

- **Hot-WAL cursor drain.** A high-volume crash can leave an
  un-checkpointed WAL with thousands of dirty pages. Recovery walks rows
  with a read cursor and, in the same pass, needs to *write* quarantine
  transitions. Issuing a write while that read cursor is still open over
  a hot WAL can deadlock into `database is locked`. Recovery avoids this
  by **collecting** every quarantine target during the cursor walk and
  writing them only *after* the cursor closes. As a second layer,
  `start()` runs `PRAGMA wal_checkpoint(TRUNCATE)` before recovery, so
  the WAL is cold by the time recovery (and steady-state traffic) runs.
- **Recovery rides out a transient lock at boot.** If a sibling process
  holds a write lock when recovery tries to write, recovery does not
  abort startup. Each recovery write is wrapped in a bounded
  retry-with-backoff (budget ~120 s) keyed on the transient-lock
  classifier (see [§1.2](#12-external-db-lock--sqlite_busy-contention)
  and [ADR-023](adr/023-transient-lock-classifier.md)). A lock held
  past the budget surfaces as a clean `RecoveryLockError`, not a raw
  `OperationalError` that wedges the service and strands the backlog.
- **Terminal rows are never re-quarantined.** A finished row (succeeded,
  failed, stored, cancelled, corrupted, expired) whose body is gone is
  *expected*: bodies are deleted on success and on terminal cleanup, and
  an `expired` row released its body when its send-deadline elapsed
  (ADR-032). Recovery skips
  terminal-state rows during the quarantine walk, and the underlying
  `mark_corrupted` write itself refuses to overwrite a terminal row.
  This protects the success record: a delivered upload's missing body is
  never mistaken for corruption. See
  [ADR-022](adr/022-recovery-terminal-state-skip.md).
- **Crash-during-recovery is safe.** A second power loss while recovery
  is mid-sweep is a no-op-safe restart: recovery is idempotent and
  re-runnable.

**Mode notes.** In `all_disk`, only torn or absent body files quarantine
(rare; requires a crash inside the write/rename window). In `hybrid`,
RAM-resident bodies that had not yet migrated quarantine; migrated bodies
survive. In `all_ram`, every not-yet-delivered body is lost on crash and
cleanly quarantined. This is the documented trade-off of running
`all_ram`, and the recovery path handles it without wedging.

## 1.2 External DB-lock / `SQLITE_BUSY` contention

**Trigger.** A separate process on the box (a backup tool, an admin
script, a sibling reading the database) holds a write lock on
`uploads.db` longer than Phantom's `busy_timeout`. (Phantom cannot
contend with *itself*: every writer serializes through one `asyncio`
lock on a single connection, so write-vs-write contention is impossible
intra-process. The timeout therefore only ever applies to *external*
contention.)

**What happens at admission.** A contended `OperationalError` whose
message carries a known `SQLITE_BUSY` / `SQLITE_LOCKED` fragment is
classified as transient (the single shared
`is_transient_lock_error` gate) and mapped to a clean **`503
storage_unavailable`** with a `Retry-After` header. A *non*-lock
`OperationalError` (malformed schema, a type error) is re-raised as an
`internal_error`. The classifier is deliberately narrow so a real bug
is never masked behind an infinite retry.

**What happens at boot.** Recovery rides out the lock with its own
bounded retry-with-backoff (see [§1.1](#11-crash-and-power-loss-recovery-sweep)),
independent of `busy_timeout`.

**What happens in the sender.** A classified lock before claim waits one poll
and retries. The same classified `OperationalError` after a row was claimed
also waits one poll and lets the worker continue polling instead of killing the
process. The row remains durably `attempting`; this handling does not silently
requeue it or promise immediate delivery, and startup recovery can reclaim it.
The parked row keeps the saturation slot it was admitted with: no in-process
path requeues it, so that slot stays held until a restart (boot reconstruction
re-charges it, delivery then releases it) or an operator admin cancel/replay.
Non-transient and unknown exceptions still escape fatal supervision.

**What you observe.** Under a transient external hold, the producer's POST
gets a retryable 503 and tries again; nothing is lost and nothing
crashes. The `busy_timeout` is **1000 ms** (lowered from 5000 ms) so a
contended writer fails fast rather than monopolizing the single writer
slot for five seconds. A long timeout amplifies an admission burst into
serialized busy-waits and HTTP read timeouts on the producer. One second
rides out sub-second blips and fails fast under a sustained hold. See
[ADR-021](adr/021-busy-timeout-tuning.md).

**Mode notes.** Mode-independent: all three modes share the one SQLite
metadata database (`uploads.db`).

## 1.3 RAM-lost bodies in `all_ram` / `hybrid`

**Trigger.** The process dies while a body is RAM-resident and has not
yet been migrated to disk (`hybrid`), or at all (`all_ram`).

**What happens.** The body is genuinely gone: RAM does not survive a
restart. The recovery sweep transitions the row to the terminal
`corrupted` state. It is **never** silently dropped or left in a
limbo state that a later sweep might mistake for deliverable.

**What you observe.** The row is visible as `corrupted` in the admin API
with a descriptive `last_error`. The operator can see exactly which
uploads were lost to the crash window and act on them.

**Mode notes.** In `hybrid`, the RAM-resident window is bounded by the
persist-linger timer and the RAM-pressure watcher, so durable
acknowledgement is the common case and this loss window is small. In
`all_ram`, the window is the whole lifetime of an undelivered row.
`all_ram` trades durability for zero disk I/O and is an explicit
operator choice. A mode-flip guard at the composition root covers
booting `all_ram` over a data directory that still holds body files
from a prior disk-backed mode. Unguarded, that boot would condemn those
rows to `corrupted` and leak the files. Per ADR-025 the guard never
refuses to boot. It relocates the live DB and body tree to a
recoverable `mode_switch` backup, logs a WARNING, bumps
`mode_switch_backup_total`, and boots fresh over the now-empty live
tree. The restore workflow lives in the operator playbook
([§ 3 *Switching modes*](operator-playbook.md#switching-modes-the-mode-switch-matrix)).

## 1.4 A missing or vanished body directory

**Trigger.** A body file that a row needs is absent at send time: an
external `rm`, a janitor race, a vanished mount, or a directory entry
that did not survive a crash.

**What happens.** The body-store load raises `BodyMissingError`; the
sender transitions the row to `corrupted` with
`last_error="storage_corruption:body_missing_in_sender:<missing body_refs>"`
and does not retry. The worker does **not** crash: a missing body is a
row-level terminal outcome, not a process-level fault. (A body found
missing by the boot-time recovery sweep of §1.1 instead quarantines
with reason `ram_body_lost_on_restart` or
`file_body_missing_on_recovery`.)

**What you observe.** The affected row is `corrupted`; the service and
every other in-flight upload are unaffected.

**Mode notes.** Applies to any mode with a disk body store (`all_disk`,
`hybrid`).

## 1.5 Truncated or client-aborted uploads

**Trigger.** The client connection aborts mid-body: a dropped LTE link,
a carrier-grade NAT reset, a client crash while a large POST is
streaming in.

**What happens.** The ingress path buffers the **entire** body before it
admits the chain: the saturation gate and the body-store write both run
*after* the full read. So a mid-upload disconnect lands *before* any
saturation slot or body is taken: there is nothing to leak.

**What you observe.** No `202` was ever sent, so nothing was promised. No
saturation slot is consumed, no `.tmp` body file is orphaned, and no
phantom row is created. A subsequent clean upload admits normally. Under
a burst of aborts, the in-flight slot count returns to zero and the gate
never exhausts.

**Mode notes.** Mode-independent (the buffering happens before storage).

## 1.6 Idempotency and duplicate-chain races

**Trigger.** The same logical upload arrives twice: a client retry after
an ambiguous outcome, a duplicate submission of an in-flight `chain_id`,
or a reused idempotency key.

**What happens.** Admission is **exactly-once** by construction: the
upload row INSERT and the idempotency-claim INSERT happen in **one atomic
SQLite transaction** ([ADR-019](adr/019-atomic-transaction-idempotency.md)).
Conflicts return typed error codes rather than naked 500s
([ADR-017](adr/017-error-code-matrix.md)):

- **Replay of a completed idempotency key** → `200` with the prior
  `ChainResponse` (not a re-admission).
- **Idempotency key reused with a *different* body** → `422
  idempotency_key_conflict`.
- **Idempotency key reused with the same body but a *different*
  destination** → `422` (identity is body *and* destination).
- **Duplicate of a live `chain_id`** → `409 chain_id_in_use`. Critically,
  the rejection does **not** touch the original chain's body: the
  `chain_id` is pre-checked *before* the body-store write, so a duplicate
  submission cannot clobber or delete the bytes of the upload already in
  flight under that key.
- **Duplicate multipart `body_refs[name]` or `envelope` part** → `422`
  (`body_ref_duplicate` / `envelope_duplicate`), never a silent
  last-wins.

**What you observe.** Duplicate work is deduplicated; conflicting work is
rejected with a specific, actionable code; the original upload is always
preserved.

**Mode notes.** Mode-independent.

## 1.7 RAM-ceiling and unbounded-table bounds

**Trigger.** Sustained ingest outruns delivery, or months of
forever-retained rows accumulate on a small SD card.

**What happens.**

- **RAM ceiling.** The `RamPressureWatcher` polls RAM-body bytes against
  `body_store.ram_ceiling_bytes` and signals the `PersistController` to
  migrate the oldest body to disk first. The ceiling is an **enforced**
  bound, not a best-effort gauge: a body whose attempt has stalled longer
  than ~2× the poll interval (slow or unreachable upstream) is migrated
  anyway, so a slow upstream cannot pin the oldest RAM rows and let RAM
  grow unbounded. In `all_ram`, the operative bound is instead
  `saturation.max_in_flight_bytes`, which refuses admission with a clean
  `503 saturation_cap` before RAM is exhausted.
- **Saturation cap.** Before RAM or disk pressure, the `SaturationGate`
  refuses admission past its row and byte caps with a `503 saturation_cap`
  (or `503 disk_pressure`) and a `Retry-After`. The producer gets clean
  backpressure rather than unbounded buffering.
- **Row-count backstop.** A `retention.max_rows` cap lets the reaper
  evict the oldest *terminal* rows once the table exceeds the limit
  (default `100_000`; set `-1` to opt into unbounded, time-only
  retention). In-flight and `auth_expired` rows are never evicted by
  the cap. This is the backstop against an `uploads` table growing
  without bound between time-based reaps.

**What you observe.** Under pressure, the producer sees retryable 503s;
RAM and the table stay bounded; nothing accepted is lost.

**Mode notes.** RAM-ceiling enforcement is most relevant in `hybrid`; the
`max_in_flight_bytes` bound governs `all_ram`; the `max_rows` backstop is
mode-independent.

## 1.8 Retry cadence and thundering herd

**Trigger.** A large backlog of rows with near-identical `next_attempt_at`
all become due at once when an upstream recovers (a Pi fleet
reconnecting after an outage).

**What happens.** Both shipped retry strategies apply **jitter** to
de-correlate retries: `fixed_intervals` now jitters in parity with
`exponential_backoff`, so a backlog does not fire in lockstep.

**What you observe.** Retries spread out in time rather than arriving as a
synchronized burst that could re-overwhelm a just-recovered upstream.

**Mode notes.** Mode-independent.

> **Known limitation.** Jitter de-correlates retry *timing*, but the
> simultaneous-fire *count* is still bounded only by the sender worker
> count, so a large single-box backlog still bursts somewhat on recovery.
> A sender-wide outbound rate semaphore is the fleet-friendly follow-up;
> it is not yet shipped.

## 1.9 Missing or rejected SigV4 credential (the `aws_sigv4` park)

**Trigger.** A route configured `auth_mode: aws_sigv4` is sent before a
destination credential is provisioned for its host, or the credential is
present but wrong (the signer cannot resolve it).

**What happens.** The re-sign raises a SigV4 signing error, which the sender
catches and parks the row in `auth_expired`: exactly like a bearer 401, and
the same non-terminal, non-evicted state (§1.7: `auth_expired` rows are never
reaped by the row-count cap). The credential store is persistent (ADR-003), so
the park survives a restart and resumes against the last known credential. When
an operator pushes a fresh credential for that host, the `CredentialKicker`
wakes every parked row on it back to `queued` (the SigV4 analogue of the auth
kicker). This is not a new failure class: `auth_expired` already covers the
`aws_sigv4` park (ADR-032).

**Re-signing does not alter body bytes.** The signer reconstructs only the auth
headers (`Authorization`, `X-Amz-Date`, the signed `x-amz-content-sha256`, and a
session-token header when present); the body bytes forwarded upstream stay
byte-identical to what the producer sent. The dual-hash / transparent-on-the-wire
guarantees are unchanged; only auth headers differ.

**What you observe.** Rows sit visibly in `auth_expired` until the credential is
corrected; no retry budget is burned while parked.

---

# Part 2. Contributor-facing: failure-mode → proving-test map

This is the index from a failure mode to the exact test that proves the
contract. Run any row directly, e.g.:

```
uv run pytest tests/e2e/crash_recovery/test_crash_sigkill_recovery_no_database_locked.py
```

The end-to-end (`tests/e2e/`) tests boot a real Phantom subprocess plus
the upstream emulator and drive field-realistic faults (real
`os.kill(SIGKILL)`, external lock holders, mid-body aborts). The unit
tests (`src/phantom-service/tests/unit/`) pin component-level invariants. Both
layers run in the standard `pytest` battery; the end-to-end crash and
contention tests are the highest-value durability coverage.

## 2.1 Crash, power loss, and recovery

| Failure mode | Proving test | What it proves |
|---|---|---|
| SIGKILL over a hot WAL → recovery must not deadlock `database is locked` | [`tests/e2e/crash_recovery/test_crash_sigkill_recovery_no_database_locked.py`](../tests/e2e/crash_recovery/test_crash_sigkill_recovery_no_database_locked.py) (`[hybrid, all_disk]`) | A real `os.kill(SIGKILL)` after ≥25 rows are on disk; restart recovers healthy and survivors persist. The cursor-drain fix. |
| SIGKILL during the recovery sweep itself (compound failure) | [`src/phantom-service/tests/unit/test_r73_recovery_idempotent_resumable.py`](../src/phantom-service/tests/unit/test_r73_recovery_idempotent_resumable.py) | Recovery is idempotent and re-runnable; a second crash mid-recovery does not wedge. |
| Recovery transitions and terminal-state skip | [`src/phantom-service/tests/unit/test_recovery.py`](../src/phantom-service/tests/unit/test_recovery.py) | A delivered `succeeded` row with a missing body is **not** flipped to `corrupted`; terminal rows are skipped (ADR-022). |
| Crash mid-persist (commit-last-column ordering) | [`tests/e2e/crash_recovery/test_crash_persist_controller.py`](../tests/e2e/crash_recovery/test_crash_persist_controller.py) | A crash before the `body_location='file'` flip leaves the row recoverable; after, the on-disk body is complete. |
| Atomic admission survives a crash mid-transaction | [`tests/e2e/crash_recovery/test_crash_admission_atomic.py`](../tests/e2e/crash_recovery/test_crash_admission_atomic.py) | Row + idempotency claim are all-or-nothing on restart (ADR-019). |
| General crash/restart recovery | [`tests/e2e/crash_recovery/test_crash_recovery_idempotent.py`](../tests/e2e/crash_recovery/test_crash_recovery_idempotent.py), [`tests/e2e/test_e2e_09_shutdown_restart.py`](../tests/e2e/test_e2e_09_shutdown_restart.py), [`tests/e2e/test_e2e_24_sigkill_idempotency.py`](../tests/e2e/test_e2e_24_sigkill_idempotency.py) | Persisted rows survive a restart; exactly-once holds across a kill. |
| `all_ram` high-volume SIGKILL → clean quarantine, no wedge | [`tests/e2e/all_ram/test_r9_pm_allram_crash_recovery_clean.py`](../tests/e2e/all_ram/test_r9_pm_allram_crash_recovery_clean.py) | RAM-lost rows quarantine to `corrupted`; the service stays healthy. |

## 2.2 External DB-lock / `SQLITE_BUSY` contention

| Failure mode | Proving test | What it proves |
|---|---|---|
| External lock held past `busy_timeout` at admission → clean `503 storage_unavailable` | [`tests/e2e/regression/test_r9_v6_external_lock_admission_clean_503.py`](../tests/e2e/regression/test_r9_v6_external_lock_admission_clean_503.py) | A sibling holding `BEGIN IMMEDIATE` past the timeout yields a retryable 503, not a naked 5xx (ADR-021). |
| External lock at recovery boot → bounded retry, never a wedged startup | [`tests/e2e/crash_recovery/test_r9_v6_recovery_boot_survives_external_lock.py`](../tests/e2e/crash_recovery/test_r9_v6_recovery_boot_survives_external_lock.py) | Recovery rides out a long external lock via its own retry-with-backoff and comes up healthy (ADR-022, ADR-023). |
| External lock causes no process death, corruption, or WAL blow-up | [`tests/e2e/db_contention/test_external_lock_no_corruption_no_wal_blowup.py`](../tests/e2e/db_contention/test_external_lock_no_corruption_no_wal_blowup.py) | Classified post-claim contention does not kill sender supervision; durable rows survive, integrity remains `ok`, and the WAL remains reclaimable. |
| `busy_timeout` value and the transient-lock classifier | [`src/phantom-service/tests/unit/test_sqlite_store.py`](../src/phantom-service/tests/unit/test_sqlite_store.py), [`src/phantom-service/tests/unit/test_token_cache.py`](../src/phantom-service/tests/unit/test_token_cache.py) | The pragma is applied at the lowered value; `is_transient_lock_error` classifies lock fragments and rejects non-lock `OperationalError` (ADR-023). |
| `all_ram` under external lock | [`tests/e2e/all_ram/test_r9_pm_allram_ram_ceiling_clean.py`](../tests/e2e/all_ram/test_r9_pm_allram_ram_ceiling_clean.py) | The lock fixes are mode-independent (shared SQLite). |

## 2.3 Body integrity and storage faults

| Failure mode | Proving test | What it proves |
|---|---|---|
| At-rest / in-transit body corruption (dual-hash) | [`tests/e2e/test_multipart_corrupted.py`](../tests/e2e/test_multipart_corrupted.py) | A storage-hash or body-hash mismatch sends the row to `corrupted`; bad bytes are never forwarded (ADR-014). |
| Body file vanishes before send (`BodyMissingError`) | [`tests/e2e/test_files_lost_midway.py`](../tests/e2e/test_files_lost_midway.py) | A missing body is a row-level `corrupted` outcome, not a worker crash. |
| `all_ram` orphan body left by a prior disk-backed mode | [`src/phantom-service/tests/unit/test_f2_all_ram_orphan_sweep.py`](../src/phantom-service/tests/unit/test_f2_all_ram_orphan_sweep.py) | The mode-flip guard and orphan handling for a populated body-store root. |
| Naive-local timestamps labelled UTC | [`src/phantom-service/tests/unit/test_a2_utc_timestamps_regression.py`](../src/phantom-service/tests/unit/test_a2_utc_timestamps_regression.py) | Cold-backup and quarantine timestamps are truthfully UTC under a non-UTC `TZ`. |
| SIGKILL mid integrity-quarantine (crash-atomic) | [`src/phantom-service/tests/unit/test_r36_sigkill_mid_quarantine_allram.py`](../src/phantom-service/tests/unit/test_r36_sigkill_mid_quarantine_allram.py) | A crash between the quarantine moves still recovers on next boot in every mode. |

## 2.4 Truncated / client-aborted uploads

| Failure mode | Proving test | What it proves |
|---|---|---|
| Client aborts mid-body → no saturation-slot leak | [`tests/e2e/ingress_abort/test_r9_cr_mid_body_abort_no_slot_leak.py`](../tests/e2e/ingress_abort/test_r9_cr_mid_body_abort_no_slot_leak.py) | A mid-body disconnect lands before `gate.admit`/`body_store.put`; no slot, no `.tmp` orphan, no row. |
| Burst of mid-body aborts → no slot exhaustion | [`tests/e2e/ingress_abort/test_r9_cr_abort_burst_no_slot_exhaustion.py`](../tests/e2e/ingress_abort/test_r9_cr_abort_burst_no_slot_exhaustion.py) | The in-flight count returns to zero under concurrent aborts vs. the cap. |
| Oversize body vs. cap / declared length | [`src/phantom-service/tests/unit/test_send_route.py`](../src/phantom-service/tests/unit/test_send_route.py) (the H2 Content-Length-precheck + streaming-cap tests) | A Content-Length precheck and stream cap reject with `413 body_too_large`; no unbounded buffering. |

## 2.5 Idempotency and duplicate-chain races

| Failure mode | Proving test | What it proves |
|---|---|---|
| Duplicate `chain_id` PK collision → `409`, original body preserved | [`tests/e2e/regression/test_d1_duplicate_chain_id.py`](../tests/e2e/regression/test_d1_duplicate_chain_id.py), [`tests/e2e/regression/test_r74b_duplicate_chainid_preserves_inflight.py`](../tests/e2e/regression/test_r74b_duplicate_chainid_preserves_inflight.py) | A re-submit of a live `chain_id` returns `409 chain_id_in_use` without clobbering the in-flight upload's body. |
| Idempotency key reused with a different body → `422` | [`tests/e2e/regression/test_g1_idempotency_key_reuse_different_body.py`](../tests/e2e/regression/test_g1_idempotency_key_reuse_different_body.py), [`src/phantom-service/tests/unit/test_r32_resolve_idempotent_row_no_naked_500.py`](../src/phantom-service/tests/unit/test_r32_resolve_idempotent_row_no_naked_500.py) | A conflicting reuse rejects cleanly; an orphaned claim re-resolves to `202`, never a naked 500. |
| Idempotency key reused with a different destination → `422` | [`src/phantom-service/tests/unit/test_r33_idempotency_envelope_divergence.py`](../src/phantom-service/tests/unit/test_r33_idempotency_envelope_divergence.py) | Identity is body *and* destination. |
| Duplicate multipart `body_refs` / `envelope` part → `422` | [`tests/e2e/regression/test_e1_duplicate_multipart_body_ref.py`](../tests/e2e/regression/test_e1_duplicate_multipart_body_ref.py), [`src/phantom-service/tests/unit/test_parser.py`](../src/phantom-service/tests/unit/test_parser.py) | A duplicate part rejects, never a silent last-wins. |
| Idempotency replay returns the prior response | [`tests/e2e/test_multipart_idempotent_replay.py`](../tests/e2e/test_multipart_idempotent_replay.py) | A replay returns `200` with the prior `ChainResponse` (ADR-019). |
| Ambiguous outcome (ack lost after upstream store) → exactly-once | [`tests/e2e/regression/test_r75a_ambiguous_outcome_exactly_once.py`](../tests/e2e/regression/test_r75a_ambiguous_outcome_exactly_once.py) | A re-send after a lost ack does not double-deliver. |
| 2xx missing a declared capture → retry, not a wedge | [`tests/e2e/regression/test_r75_2xx_missing_capture_no_wedge.py`](../tests/e2e/regression/test_r75_2xx_missing_capture_no_wedge.py) | A success response lacking a required capture is treated as retryable, not a wedged multi-step chain. |

## 2.6 RAM-ceiling and unbounded-table bounds

| Failure mode | Proving test | What it proves |
|---|---|---|
| RAM ceiling enforced when oldest rows are all `attempting` | [`tests/e2e/stress/test_f1_ram_pressure_attempting_filter.py`](../tests/e2e/stress/test_f1_ram_pressure_attempting_filter.py) | A stalled attempt is migrated to disk anyway; RAM stays bounded under a slow upstream. |
| Saturation bytes-cap zero-semantics | [`src/phantom-service/tests/unit/test_a1_saturation_zero_bytes_cap.py`](../src/phantom-service/tests/unit/test_a1_saturation_zero_bytes_cap.py) | A zero byte-cap refuses all, consistent with the row cap. |
| In-flight byte counter does not leak under compression | [`src/phantom-service/tests/unit/test_r38_in_flight_bytes_no_leak_under_compression.py`](../src/phantom-service/tests/unit/test_r38_in_flight_bytes_no_leak_under_compression.py) | Admit-unit ≡ release-unit ≡ `body_size_bytes`; the gate never wedges over time. |
| Saturation slot released exactly once on conflict paths | [`src/phantom-service/tests/unit/test_r31_no_double_release_on_conflict.py`](../src/phantom-service/tests/unit/test_r31_no_double_release_on_conflict.py) | A conflict-reject never double-releases a slot and steals from live rows. |
| Unbounded table growth → `max_rows` backstop | [`src/phantom-service/tests/unit/test_v3_retention_table_growth_and_cleanup.py`](../src/phantom-service/tests/unit/test_v3_retention_table_growth_and_cleanup.py) | The reaper evicts oldest terminal rows over the cap; in-flight/`auth_expired` never evicted. |
| Mode-flip guard over persisted rows | [`src/phantom-service/tests/unit/test_a3_mode_flip_with_persisted_rows.py`](../src/phantom-service/tests/unit/test_a3_mode_flip_with_persisted_rows.py) | Booting `all_ram` over a populated disk-backed root backs up and runs (ADR-025): the live DB and body tree move to a recoverable `mode_switch` backup, `mode_switch_backup_total` bumps, and the service boots fresh with no silent data loss. |

## 2.7 Retry cadence and thundering herd

| Failure mode | Proving test | What it proves |
|---|---|---|
| Retry cadence is jittered and survives a crash | [`tests/e2e/regression/test_v5_retry_cadence_and_crash_survival.py`](../tests/e2e/regression/test_v5_retry_cadence_and_crash_survival.py) | A backlog retries with spread, not in lockstep, and the schedule survives a restart. |
| Jitter parity across both retry strategies | [`src/phantom-service/tests/unit/test_strategies.py`](../src/phantom-service/tests/unit/test_strategies.py) | `fixed_intervals` jitters in parity with `exponential_backoff`. |

---

## Coverage and gaps

The end-to-end regression coverage matrix lives at
[`tests/e2e/regression/COVERAGE.md`](../tests/e2e/regression/COVERAGE.md).
The hardening cycle's coverage is heavily weighted toward crash/recovery,
content integrity, contention, and concurrency. Fault families that
remain lighter on direct coverage (tracked as future work)
include fsync-EIO and ENOSPC fault injection on the storage path,
filesystem-level rename/directory-durability injection, and several
network-realism vectors (DNS failure, TLS/cert expiry, distinct
connect-vs-read timeouts). These are documented exposures, not
regressions; the durability invariants above hold against the modeled
faults that have proving tests.

## Pointers

- **Decisions:** [docs/adr/](adr/), in particular
  [ADR-014](adr/014-dual-body-hash.md) (dual body hash),
  [ADR-017](adr/017-error-code-matrix.md) (error-code matrix),
  [ADR-019](adr/019-atomic-transaction-idempotency.md) (atomic
  admission), [ADR-021](adr/021-busy-timeout-tuning.md) (busy-timeout
  tuning), [ADR-022](adr/022-recovery-terminal-state-skip.md) (recovery
  terminal-state skip), [ADR-023](adr/023-transient-lock-classifier.md)
  (transient-lock classifier).
- **Failure-mode overview in the architecture map:**
  [docs/architecture-intent.md §5](architecture-intent.md) (invariants)
  and §7 (failure modes).
- **Operator diagnosis and tuning:**
  [docs/operator-playbook.md](operator-playbook.md).
- **Glossary:** [CONTEXT.md](../CONTEXT.md).
