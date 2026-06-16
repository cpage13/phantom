# Phantom: Reliability, Error Handling, and Security

This document explains how Phantom handles things going wrong, why it is safe to
run on hardware you do not fully trust, and what guarantees it actually makes. It
is the third of three engineering deep dives:

- [Architecture and Operational Framework](architecture.md)
- [The End-to-End Test Suite](test-suite.md)
- **Reliability, Error Handling, and Security** (this document)

The short version: Phantom is built to survive crashes, power loss, disk and
memory pressure, upstream outages, expired credentials, and database lock
contention without losing data and without sending duplicates. The rest of this
document is the detail behind that claim. Every behavior described here has a
corresponding test in [the suite](test-suite.md).

## The error-handling model

### A small, purpose-built exception set

Phantom defines a handful of typed exceptions rather than leaning on generic
ones, so that each failure has a name and a single place it is raised:

- A missing-body error, raised when a row's declared body is absent from the body
  store at send time.
- A storage-corruption error, raised when the bytes on disk do not match the hash
  recorded for them, before any decode is attempted.
- A codec round-trip error, raised when storage verified clean but the decoded
  bytes do not match the original hash, which would indicate a codec bug rather
  than a storage fault.
- A recovery-lock error, raised when boot recovery cannot acquire the database
  write lock within its budget. This is a clean operator-facing startup failure,
  not a raw traceback.
- A config-invariant error, raised when a load-bearing configuration invariant
  fails at startup.
- An auth-unavailable error, raised when no configured credential can mint a
  token.
- An admission error carrying a typed error code, which is mapped to an HTTP
  status and a structured error envelope on the wire.

### The error-code matrix

Every code Phantom emits maps to one HTTP status and one structured response
shape. The wire envelope is strict (unknown fields are rejected). The table below
is the contract a client can rely on.

| HTTP | Code | Fires when | What the client should do |
|---|---|---|---|
| 401 | `auth_token_missing` | The request has no authorization and there is no cached token for the slot | Refresh credentials and resend |
| 404 | `not_found` | An admin lookup references a chain, instance, or token that does not exist | Do not retry |
| 413 | `body_too_large` | The content-length precheck, or a mid-stream cap, is exceeded | Split the upload or raise the configured cap |
| 421 | `invalid_target` | The target header is missing or malformed | Fix the target |
| 421 | `instance_unknown` | The target hostname matched no configured instance | Add the host prefix to an instance |
| 422 | envelope and body-reference validation codes | The upload envelope or a multipart part failed validation | Fix the caller; this is a request bug |
| 422 | `idempotency_key_conflict` | An idempotency key was reused with a different body | Use a body-derived key |
| 409 | `chain_id_in_use` | The chain ID is already in use by a live row | Mint a fresh chain ID |
| 502 | `upstream_unreachable` | The upstream could not be reached. Surfaced on admin lookups only, never on the upload path | Admin-only signal |
| 503 | `saturation_cap` | The in-flight gate refused admission | Back off and retry per `Retry-After`. Phantom is the retry engine, so the client should not itself retry aggressively |
| 503 | `disk_pressure` | The disk-usage ceiling was crossed (a proactive limit) | Back off; the operator frees disk |
| 503 | `storage_unavailable` | A storage write fault (a full disk or an I/O error), or a transient database lock during the admission insert | Back off and retry per `Retry-After`. Durability holds: nothing was committed |
| 500 | `internal_error` | An unexpected exception | Inspect the logs; this is a service bug |
| 200 | `idempotency_replay` | The idempotency key matched a prior accepted upload | Informational, not an error. The prior response is returned and nothing re-runs |

Two corruption codes (a storage-corruption code and a codec round-trip code) are
never returned over the wire. They are recorded on the row and visible only
through the admin detail view, because by the time they fire the upload has
already been accepted and the producer has moved on. Database quarantine is
likewise deliberately not a wire code: it happens at boot before any request is
served, and surfaces through an error log, a counter, and an admin quarantine
inventory.

### The discipline behind it

A few rules are enforced mechanically rather than by convention. There are no
bare exception catches (a linter rule plus a pre-commit check forbid the
silent-swallow patterns). Every external, I/O, and database call is wrapped with
context. Background workers that loop catch broadly and log and continue, so a
transient blip cannot kill a loop, while the admission slot-release path catches
even cancellation so a cancelled request cannot leak a saturation slot.

## Fallback procedures, by failure mode

This section is the heart of the document. For each way an operation can go
wrong, here is exactly what Phantom does.

### The upstream is down or a forward fails

The sender classifies every result. A retryable failure with budget remaining is
re-queued with a computed delay. The default backoff is exponential, the delay
being the smaller of a cap and a base multiplied by a growth factor raised to the
attempt count, with jitter applied to avoid a thundering herd of synchronized
retries. A fixed-interval strategy is also available. When the retry budget is
exhausted, the row moves to `stored` rather than being lost, and its body stays
recoverable through the admin export. Throughout every retry, the body stays in
the store and the row stays in the database. Nothing is dropped until a terminal
state is reached. This is the core store-and-forward promise.

### The database is locked or busy

This is covered in depth in
[the architecture document](architecture.md#database-retry-mechanisms). In
summary: because all writers serialize internally, a lock only ever comes from
outside the process. Steady-state traffic uses a short busy timeout and turns a
transient lock into a clean 503 with a `Retry-After`. Boot recovery, which cannot
afford to crash and strand the backlog, rides out a much longer external lock
with a bounded retry-with-backoff (starting at half a second, capped at five
seconds per wait, with a total budget of two minutes) and fails cleanly if that
budget is exhausted. A genuine non-lock database error is never retried; it is
surfaced.

### A body is missing at send time

If a body the row declares is not in the store when the sender goes to read it,
the body store raises a missing-key error, the sender converts it to the typed
missing-body error, and the row moves to the terminal `corrupted` state. It is
never delivered as a silent empty payload, which would be data loss disguised as
success.

### The database or a body is corrupted

There are four corruption-detection paths, and all of them end in the `corrupted`
state, never a retry. The sender verifies the encoded hash before decoding and
the raw hash after decoding, so on-disk mutation and codec drift are both caught.
At boot, an integrity check runs against the database before the store opens. On
failure, the body tree is moved aside first and the corrupt database last (so a
crash in the middle leaves the corrupt database in place to be re-detected on the
next boot), to flat timestamped backup siblings in the instance data root. These
are renames, never deletes: the operator decides what to remove. The quarantine inventory is
available through the admin API, and the admin export can stream out every
buffered body for recovery.

### The process crashes or is killed mid-operation

Recovery runs at boot, before any worker starts, so the rest of the system sees
an already-corrected population. It resets every in-flight row back to queued (an
attempt whose writes never landed must re-attempt) and quarantines any row whose
declared body has gone missing. To avoid colliding with a hot write-ahead log, it
collects what it needs to change while walking its read cursor and only issues the
writes after the cursor drains. No double send is possible: admission writes the
row and the idempotency claim in one atomic transaction, a replay of a matching
key returns the existing row, and a re-submit of an already-live chain is checked
before the body is even written, so a normal client retry never clobbers the
original. Recovery skips terminal rows, so a delivered upload whose body was
already dropped on success is never wrongly re-marked corrupted. Because every
recovery write is idempotent, re-running recovery is always safe.

### The disk fills or memory runs high

Memory pressure is handled by a watcher that samples RAM usage against a ceiling
and hands the oldest bodies to the persist controller to spill to disk; a stalled
in-flight attempt is migrated anyway so a slow upstream cannot pin memory forever.
The spill writes and fsyncs the disk copy before dropping the RAM copy, so an
in-flight reader always finds a durable copy. Disk pressure is handled
proactively by an out-of-band probe that feeds usage to the admission gate, which
then refuses new work with a clean 503 before the disk is full. A disk that fills
between probe samples is handled reactively: the write fault becomes a clean 503,
and because the write failed no row was committed. The in-flight gate also caps
the count and bytes of concurrent uploads, with a separate class for large bodies.
On boot, the staging directory is purged of partial files left by a crash
mid-write, and every body write is atomic (write to a temporary file, fsync,
rename, fsync the parent directory).

### The service is asked to start in a bad state

Five guards run at startup and refuse to start the service rather than run
unsafely:

1. A umask guard sets owner-only permissions process-wide, so every file created
   afterward is private.
2. A retention-floor guard refuses a configuration where a body would be retained
   longer than its metadata row, which would orphan the body.
3. An instance-isolation guard refuses duplicate instance IDs, colliding or nested
   data directories (two stores on one database file is a corruption risk), and
   duplicate routing prefixes.
4. A body-store-mode guard refuses to start in all-ram mode over a populated disk
   body directory, which would both condemn intact on-disk rows and leak their
   files.
5. The database integrity gate quarantines a corrupt database before the store
   opens.

In addition, the store reads back its pragmas after setting them and refuses to
start if any did not take effect. A misconfiguration crashes startup cleanly,
before any store or worker exists, and the orchestrator surfaces it.

## Security posture

- **Instance isolation.** The isolation guard provably separates each instance's
  identity, storage, and routing, or refuses to start.
- **Loopback-only admin with no separate auth.** The deployment is
  same-machine-only: ONE listener serves intake + admin + health on one socket,
  bound to `127.0.0.1` by default. The loopback default bind is the
  authentication boundary - nothing is reachable off-box by default, so the
  destructive admin endpoints are not either. Bearer-token values are never
  returned by any admin response, and bulk-destructive endpoints reject a call
  with an empty filter. Exposing the surface to a wider network is an operator
  decision to be made with a fronting reverse proxy (a non-loopback `bind_tcp`
  opt-in warns at startup that the admin API rides this listener and is
  unauthenticated), and is outside Phantom's scope. An optional configured admin
  secret (and HTTPS for off-box) is recorded as future work (ADR-004).
  Liveness/readiness probes (`/v1/healthz`, `/v1/readyz`) ride the same single
  listener.
- **Owner-only files.** A process-wide umask of owner-only is applied first thing
  at startup, so every file Phantom then creates (the database, the write-ahead
  log, buffered bodies, the token cache) is readable and writable only by its
  owner.
- **No secrets in URLs.** Authentication rides the authorization header. The only
  Phantom-specific wire headers identify the caller and the upstream target. The
  token cache is keyed by (endpoint, uid); Phantom does not parse or interpret the
  uid.
- **Secret and log redaction.** A logging filter scrubs bearer tokens from every
  log record. Values a chain marks sensitive (for example a presigned upload URL)
  are redacted before formatting, and that redaction path is gated behind debug
  logging so production logging pays no cost for it. The bearer filter runs first,
  so a token embedded inside a captured value is still scrubbed.
- **Transparent header handling.** The upstream transport forwards request headers
  faithfully, and the dual-hash invariant guarantees the body bytes forwarded
  upstream are byte-identical to what the producer sent. This is verified end to
  end in the suite, not merely asserted.
- **Token lifecycle.** The token cache is persistent and survives a restart, so a
  buffered upload keeps retrying with the last known token. A token that returns a
  401 is not silently evicted; the row enters the visible `auth_expired` state so
  an operator can see which credential needs attention. An optional OAuth2
  client-credentials minter can refresh tokens ahead of expiry and on demand; it
  reads its client secret from an environment variable, never inline, and it is
  supervised so a silent mint failure crashes the process loudly rather than
  looking healthy.
- **Upstream TLS verification is on.** The upstream client verifies certificates,
  and there is no configuration knob to turn that off.

## Robustness guarantees

- **At-least-once, idempotent delivery.** A buffered upload is never dropped:
  retryable failures cycle between queued and attempting until success or the
  `stored` state, and the row and body persist across every retry. Idempotency
  (the atomic row-plus-claim insert, the replay returning the existing row, and
  the body-keyed chain-id precheck) guarantees no double send.
- **Commit-last persist ordering.** The persist controller is the sole writer of
  the durability flip from RAM to disk. It fsyncs the file and its parent directory
  before flipping the column and before dropping the RAM copy, so an in-flight
  reader always finds a durable copy and a crash mid-flip leaves a recoverable row.
  This ordering is enforced mechanically by pre-commit checks.
- **Durability across restart.** One persistent database per instance in
  write-ahead-logging mode, with autovacuum locked off to protect flash media and
  a hot-WAL checkpoint on start, plus a recovery sweep that corrects only the rows
  that need it and a token cache that reloads so retries resume with a token.
- **Continuous invariant checking.** An auditor walks the rows on a low frequency
  and asserts the invariants checkable that way (a row claiming an on-disk body
  must have the file; the recorded hash set must match the body store). It
  increments a violation counter that the build treats as a failure. Other
  invariants are enforced structurally or watched with counters and gauges.
- **No silent worker death.** Every long-lived task runs under one supervising
  task group, so an unhandled exception in any worker propagates out, the process
  exits, the orchestrator restarts it, and the persisted rows survive. A
  pre-commit check forbids spawning unsupervised tasks, so this property cannot be
  quietly broken.
