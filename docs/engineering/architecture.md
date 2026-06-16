# Phantom: Architecture and Operational Framework

This document explains what Phantom is, how a request moves through it, how it
stores data, and how it keeps that storage correct under contention. It is the
first of three engineering deep dives:

- **Architecture and Operational Framework** (this document)
- [The End-to-End Test Suite](test-suite.md)
- [Reliability, Error Handling, and Security](reliability-and-security.md)

If you only read one section, read [Database retry mechanisms](#database-retry-mechanisms).
That is where most of the engineering effort went, and it is the reason Phantom
can run on flaky consumer hardware without corrupting its own state.

## What Phantom is

Phantom is a store-and-forward HTTP upload proxy. A data-producing process (a
producer, a field sensor, a long-running job) POSTs its upload to Phantom
instead of sending it to the cloud directly. Phantom acknowledges in
milliseconds with HTTP 202 and a tracking ID, writes the body to durable local
storage before it acknowledges, and then performs the real upstream calls in
the background with retries, backoff, and token refresh.

The producer gets a fast, always-available upload endpoint. Phantom owns the
unreliable part: the network, the expiring credentials, the upstream that is
sometimes down. The forwarding guarantee is at-least-once and idempotency-keyed,
and the local buffer survives process restarts and power loss.

Three properties hold at runtime:

1. **Transparent on the wire.** Whatever bytes the producer sends are the exact
   bytes that reach the upstream. Two SHA-256 hashes per stored body (one over
   the raw bytes, one over the encoded-at-rest bytes) enforce that byte-identity
   end to end, so a silent corruption is caught rather than delivered.
2. **Always-encode.** Every body is stored under one configured codec per
   deployment (the default is zstd). There is a single encode path, not a
   sometimes-on optimization, so the storage format is uniform and testable.
3. **Crash-safe.** Every long-lived task runs under one supervising task group.
   If any worker raises an unhandled exception the process exits loudly so the
   orchestrator restarts it into recovery. Nothing dies silently.

Phantom buffers POST and upload traffic. Read-side proxying (GET) is not in
scope.

## The pieces

Phantom is a small set of packages. Most deployments only touch two of them.

| Package | What it is | Who uses it |
|---|---|---|
| `phantom` | The service. A FastAPI process that accepts uploads, persists them, and forwards them upstream with retries. Runs as a container. | Operators |
| `phantom-client` | A typed Python SDK over the service's HTTP surface. Its only runtime dependencies are httpx and pydantic. | Anyone writing producer code |
| `phantom-emulator` | A stand-in upstream that mimics a typical cloud upload API (object-create, presigned object-upload, and an OAuth2 token mint) plus a control surface for injecting failures. It lets the full stack run with no internet. | Anyone writing tests |
| `phantom-deploy` | The deployment artifact: a multi-architecture container image and a reference compose file. The image itself is the artifact, not a Python package. | Operators |

The service is deliberately generic. At the HTTP layer it is opaque: it knows
about uploads, headers, codecs, retries, and error codes, and nothing about any
particular upstream product. It owns the wire protocol, every data model, and
every error code.

## The request lifecycle

A POST flows through the service in six stages.

1. **Ingress.** The ingress endpoint (`POST /v1/send`) hands off to the admission
   path. Ingress, admin, and health all ride the one listener (see
   [The admin surface](#the-admin-surface)).
2. **Admission.** The admission path parses and validates the upload envelope,
   its declared body references, and the size caps. An instance dispatcher
   resolves the target hostname to the correct instance (an unmatched target is
   rejected). A saturation gate then either admits the request or refuses it with
   a typed result (a 503 plus a `Retry-After` header on refusal). On admission,
   the body is encoded, both hashes are computed (raw and encoded), the body is
   written through the body store, and finally the metadata row and the
   idempotency claim are written in one atomic database transaction. The
   response is HTTP 202 for a fresh admission or HTTP 200 for an idempotency
   replay. The row begins life in the `queued` state.
3. **Persistence.** Body bytes go to the mode-selected body store. The metadata
   row and the idempotency claim go to the single per-instance database, together,
   in one transaction.
4. **Forwarding.** The sender loop atomically claims a due `queued` row and moves
   it to `attempting`. It loads the body references, verifies the encoded hash
   before decoding and the raw hash after decoding (a mismatch on either drives
   the row to the terminal `corrupted` state), executes the next step of the
   upload chain, classifies the result, and records the outcome. The sender owns
   every state transition. There is no separate state-machine module to drift out
   of sync.
5. **Upstream.** The upstream transport performs the real HTTP call. Auth is
   injected from a token cache keyed by (endpoint, uid) at the moment of the
   attempt, so a token refreshed mid-retry is the one actually used. Template
   placeholders are substituted, and any values the chain needs to carry forward
   are captured from the response.
6. **Completion.** A delivered chain reaches the terminal `succeeded` state and
   its body is dropped. Captured values stay queryable through the admin API for
   the configured retention window.

When a retryable failure happens with budget remaining, the sender asks the
configured retry strategy for the next delay and re-queues the row with a
`next_attempt_at` timestamp. When the budget is exhausted, the row moves to
`stored` rather than being lost, and its body stays recoverable through the
admin export. A 401 moves the row to `auth_expired`, where it waits for a fresh
token rather than burning retry budget.

### The two idempotency identifiers

Phantom carries two distinct idempotency identifiers, and they are easy to
conflate because both default to the same value. They sit at opposite ends of
the request lifecycle and protect different parties.

1. **The inbound dedup key (`chain_id_at_ingress`).** The caller may set the
   `X-Phantom-Idempotency-Key` HTTP header on `POST /v1/send`. Phantom stores
   that value on the upload row as `chain_id_at_ingress` and writes it into the
   idempotency index as a UNIQUE claim in the same atomic admission transaction.
   This is what stops Phantom from buffering the same upload twice: a resend with
   the same key finds the existing claim and replays the original 202 (returned
   as HTTP 200) instead of admitting a duplicate. When the caller omits the
   header, Phantom mints `str(chain_id)` so the dedup claim is never skipped.
2. **The outbound forwarded value (`envelope.idempotency_key`).** This is a
   field inside the upload envelope, not a header at ingress. At send time the
   executor emits it to the upstream as the HTTP header named by the step's
   `idempotency_header` (for example `Idempotency-Key`), and that value is stable
   across every retry of the chain. This is what stops the upstream from
   recording the same upload twice when Phantom retries. When the caller omits
   it, it too defaults to `str(chain_id)`.

The load-bearing point that the recurring confusion misses: the envelope is a
**stored, replayable plan**, not a payload. It lives in the database as the
upload's plan, and the executor walks it step by step. The bytes Phantom sends
upstream are the **original buffered upload**, byte for byte, never the envelope
JSON. The idempotency value rides as an HTTP **header**, never inside the JSON
body. Custom step headers the caller declared are forwarded too; headers in
Phantom's reserved `X-Phantom-*` namespace are stripped, preserving the
transparent-proxy invariant.

```
  caller
    | POST /v1/send
    |   header: X-Phantom-Idempotency-Key: <k>   (optional; minted str(chain_id) if absent)
    v
  Phantom admission
    - stored as row.chain_id_at_ingress  ->  idempotency_index claim  (admission dedup)
    - envelope persisted as the upload PLAN (ChainEnvelope JSON in the DB)
    |
    v
  executor (per step)
    - outbound HTTP <step.method> <url>
        header[step.idempotency_header] = envelope.idempotency_key  (minted str(chain_id) if absent)
        custom step.headers forwarded; X-Phantom-* stripped
        body = the ORIGINAL buffered upload bytes  (NOT the envelope JSON)
    v
  upstream
```

## The admin surface

Admin endpoints (status, listing, export, replay, cancel, token push,
observability, quarantine inventory) ride the SAME single listener as ingress
and health. The deployment is same-machine-only, so `phantom.app.create_app`
returns ONE `FastAPI` app and `phantom.__main__` runs ONE `uvicorn` server
bound to `bind_tcp` (default `127.0.0.1:8080` - loopback) else `bind_uds`; the
one app carries the sole lifespan, so the worker pool starts exactly once. The
loopback default bind is the authentication boundary: there is no separate admin
password (an optional configured secret is recorded as future work in ADR-004),
because nothing is reachable off-host by default unless an operator deliberately
fronts it with a reverse proxy (a non-loopback `bind_tcp` opt-in warns at
startup). Bearer-token values are never returned by any admin response, and the
bulk-destructive endpoints refuse a call with an empty filter so a
fat-fingered request cannot wipe everything. Liveness (`/v1/healthz`) and
readiness (`/v1/readyz`) ride the same single listener.

(A two-listener split that bound the admin router on its own socket was tried
(R12-1) and collapsed as no-benefit for the same-machine deployment; it
introduced two bugs - a startup-ordering window (R13-1) and a bind-collision
alias gap (R13-2) - both eliminated by the single listener.)

## Storage architecture

Each instance owns exactly one persistent SQLite database in WAL mode, at a path
under its data directory. That database holds the upload rows, the idempotency
index, and the token cache. Body bytes live separately, in the body store.

### SQLite configuration

The store sets and then reads back each pragma at startup, and refuses to start
if a pragma did not take effect:

| Pragma | Value | Notes |
|---|---|---|
| `journal_mode` | `WAL` | Write-ahead logging. Fixed. |
| `synchronous` | `NORMAL` | Configurable. Chosen over `FULL` after testing on consumer flash storage. |
| `journal_size_limit` | 16 MiB | Configurable. Bounds WAL growth. |
| `foreign_keys` | `ON` | |
| `auto_vacuum` | `NONE` | Hard-coded and never configurable. Autovacuum's write amplification shortens the life of SD cards and similar flash media. |
| `busy_timeout` | 1000 ms | Configurable. See [Database retry mechanisms](#database-retry-mechanisms). |

At startup, before recovery runs, the store also issues a
`wal_checkpoint(TRUNCATE)` to drain a WAL that may be hot from an abrupt kill.

### Upload rows and states

Each upload is one row. Its `state` is one of `queued`, `attempting`,
`succeeded`, `failed`, `cancelled`, `stored`, `corrupted`, or `auth_expired`.
The terminal states are `succeeded`, `failed`, `stored`, `cancelled`, and
`corrupted`. Note that `auth_expired` is deliberately not terminal: such a row is
still deliverable once a working token arrives.

A `body_location` column records whether a body currently lives in RAM or on
disk. This column is the single durability commit point, and only one component
is ever allowed to change it (see the next section). Alongside it the row carries
the two hashes per body reference, the attempt count, the next-attempt timestamp,
the last error, any captured values, the current step index, and the idempotency
key.

### The body store and deployment modes

The production-default body store is hybrid. Writes go to RAM first. Reads check
RAM and fall through to disk on a miss, so the read path is authoritative on RAM
presence and never has to consult the database column to find the bytes. The
RAM-bytes total is the figure used for memory-pressure and saturation decisions.

There are three first-class modes, selected in configuration:

| Mode | Where bodies live | Trade-off |
|---|---|---|
| `hybrid` (default) | RAM first, spilled to disk on memory pressure or retry-linger | Lowest steady-state latency with a disk safety net |
| `all_disk` | Every body written to disk immediately | Maximum durability across a power cut |
| `all_ram` | Bodies stay in RAM, never written to disk | Lowest latency for workloads where re-running on loss is acceptable |

### Single-writer-per-purpose

Every table-mutating purpose has exactly one owner. Admission is the only writer
of the row-plus-claim insert. The sender is the only writer of state transitions.
The persist controller is the only component that flips `body_location` from RAM
to disk. The reaper is the only one that deletes rows for retention. The boot
recovery sweep is the only caller that marks a row `corrupted`. The pressure
watchers, the orphan janitor, and the invariant auditor write nothing to the
upload table at all. This discipline is what makes the concurrency tractable:
for any given change to a row, there is exactly one place to look.

## Database retry mechanisms

This is the core of Phantom's durability story. SQLite is a single-file
database, and the failure that ruins single-file databases in the field is lock
contention turning into corruption or into a wedged process. Phantom handles
this in layers.

### Layer 1: write serialization makes internal contention impossible

Every coroutine in the process shares one database connection, and every write
path goes through one async lock. The consequence is strong: there is never more
than one Phantom write in flight at the SQLite level, so Phantom can never
contend with itself. Reads are exempt and run concurrently. This single fact
removes the entire category of self-inflicted lock errors and means the
lock-handling below only ever has to deal with contention from outside the
process.

### Layer 2: a bounded busy_timeout for external contention

The `busy_timeout` is 1000 ms (configurable). Because Phantom never contends
with itself, this timeout only ever fires when something external holds the
database lock: a stray interactive SQLite session, a backup or snapshot tool, or
a misconfiguration that points two instances at the same file. The value was
deliberately lowered from a longer setting, because a long timeout makes an
external hold worse: each blocked writer monopolizes the single writer slot for
the full window, so an incoming burst queues serially and the producer's HTTP
call times out before Phantom can return a clean refusal. A short timeout lets
Phantom fail fast and cleanly with a 503 that tells the producer to retry.

### Layer 3: one shared classifier for "transient lock" versus "real fault"

A single function decides whether an error is a transient lock to be ridden out
or a genuine fault to be surfaced. It returns true only for the specific SQLite
operational errors whose messages indicate lock contention ("database is
locked", "database is busy", "database table is locked"). It is deliberately
narrow: a schema or type error is left unclassified so that a real bug can never
be hidden behind a retry loop. Both the admission path and the boot recovery
path call this one function, so the two cannot drift apart on what counts as
retryable.

### Layer 4: the admission write is one atomic transaction

Admission writes the upload row and the idempotency claim inside one explicit
transaction with an explicit begin, commit, and rollback. It returns a typed
outcome: inserted (HTTP 202), an idempotency collision (a replay returns HTTP 200,
a true conflict returns HTTP 422), or a chain-id collision (HTTP 409, which
replaces what would otherwise have been an opaque 500). A stale idempotency claim
whose row no longer exists is cleaned up inside the same transaction. If anything
raises, the transaction rolls back and leaves no half-written state.

### Layer 5: rollback-and-continue keeps the shared connection healthy

Because all writers share one connection, a write that fails partway through
could leave an open transaction that wedges every subsequent writer with a
"cannot start a transaction within a transaction" error until the process
restarts. To prevent that, every non-admission write path is wrapped so that any
exception triggers a rollback before the error propagates. This clears the open
transaction and keeps the connection usable. The posture is rollback-and-continue
rather than panic-and-exit, because testing showed that a failed commit leaves no
durable half-commit and the database still passes its integrity check, so there
is no reason to abort in-flight deliveries. State-transition updates are also
guarded with a `WHERE` clause on the expected current state, so a concurrent
admin cancel or replay cannot be silently clobbered: the update simply affects
zero rows and the caller notices.

### Layer 6: admission turns a transient lock into a clean 503

When the admission insert hits a transient lock, the request becomes an HTTP 503
with a `Retry-After` header, not an error. A genuine non-lock error is re-raised
and surfaces as an internal error instead. A storage write failure from the body
store (a full disk, an I/O error) maps the same way, to a clean 503. The producer
is told to back off and try again, and nothing was half-committed.

### Layer 7: boot recovery rides out long external locks

Steady-state traffic relies on the short `busy_timeout`, but boot recovery is
different: if an external process holds the lock when Phantom restarts, recovery's
first write would otherwise crash startup and strand the entire buffered backlog.
So recovery wraps its writes in a bounded retry-with-backoff: exponential backoff
starting at 0.5 s, capped at 5 s per wait, with a total budget of 120 s. It
retries only when the shared classifier says the error is a transient lock. If
the budget is exhausted it raises a clean, named startup error that the
supervisor restarts into, rather than a raw traceback or a hung boot. This is safe
precisely because recovery's writes are idempotent: re-running them loses nothing.

### Layer 8: avoiding the cursor-versus-checkpoint hazard

There is one more lock source that the `busy_timeout` does not cover, because it
is same-connection rather than cross-process. Over a WAL that grew hot from an
abrupt kill, a write issued while a read cursor is still open can trigger an
in-line checkpoint that collides with that cursor. Two defenses handle it. First,
the startup checkpoint-and-truncate runs before any cursor is opened, so steady
traffic sees a cold WAL. Second, the recovery sweep collects everything it needs
to change while walking its read cursor, and only issues the writes after the
cursor has been fully drained. Anything that still slips through is caught by the
same transient-lock classifier and ridden out by the recovery backoff above.

## Background workers

All long-lived tasks run under one supervising task group, so an unhandled
failure in any of them takes the process down loudly into a restart.

- **Recovery.** Runs once at boot, before any other worker starts, so the rest of
  the system sees an already-corrected population. It resets every `attempting`
  row back to `queued` (an attempt that was in flight at the crash must be
  retried), then walks the rows and quarantines any whose declared body has gone
  missing. It skips terminal rows so a delivered upload whose body was already
  dropped is never wrongly marked corrupted.
- **Sender.** The load-bearing loop. It claims due rows, drives each upload step,
  owns all state transitions, schedules retries, and hands bodies to the persist
  controller when a retry lingers long enough to be worth spilling to disk.
- **Reaper.** The retention sweep. It deletes terminal rows once they pass their
  per-state retention window and enforces a hard maximum row count. Default
  cadence is 60 s.
- **RAM-pressure watcher.** Samples RAM body bytes against the ceiling and, when
  over, hands the oldest bodies to the persist controller to spill to disk. A
  stalled in-flight attempt is migrated anyway, so a slow upstream cannot pin RAM
  indefinitely. Default poll is 1 s.
- **Disk-pressure probe.** Samples on-disk body usage out of band and feeds it to
  the saturation gate, so the gate's admission check stays free of disk I/O. Over
  the disk ceiling, new admissions are refused with a clean 503. Default cadence
  is 30 s.
- **Vacuum scheduler.** Runs a SQLite `VACUUM` on a cron schedule, and only when
  the in-flight queue is empty, to reclaim space without the write amplification
  that autovacuum (locked off) would cause. Default schedule is weekly.
- **Invariant auditor.** Walks the rows on a low frequency and asserts the
  invariants that are checkable by a row walk (a row claiming an on-disk body must
  have that file; the recorded hash set must match the body store). It increments
  a violation counter that the build treats as a failure, and it writes nothing.
  Default period is 300 s.
- **Cold-backup scheduler.** Off by default. When enabled, it uses SQLite's
  online-backup API to write a consistent snapshot and rotates a fixed number of
  copies.
- **Storage integrity gate.** Runs at boot, per instance, before the store opens.
  It runs an integrity check and, on failure, moves the corrupt database and its
  body tree aside to flat timestamped backup siblings in the instance data root
  (a `uploads.corrupted.<stamp>.db` file plus a `bodies.quarantine.<stamp>/`
  directory, not nested under a shared quarantine folder), then either proceeds
  with fresh empty state or aborts, depending on configuration.
- **Body-orphan janitor.** Periodically deletes body files whose upload row no
  longer exists. Default sweep is hourly. It writes nothing to the upload table.
- **Persist controller.** The only component that moves a body from RAM to disk
  and the only writer of the `body_location` flip. It writes and fsyncs the file
  (and its parent directory) before flipping the column and before dropping the
  RAM copy, so a crash in the middle leaves a row that recovery can still resolve.
- **Auth kicker.** Wakes `auth_expired` rows back to `queued` when a fresh token
  lands in the cache for their (endpoint, uid) slot.

Process-wide startup guards run in the same boot path and refuse to start the
service in an unsafe state. They are covered in
[Reliability, Error Handling, and Security](reliability-and-security.md).
