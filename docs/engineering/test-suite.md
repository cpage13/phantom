# Phantom: The End-to-End Test Suite

This document is a tour of the end-to-end test suite. It is the second of three
engineering deep dives:

- [Architecture and Operational Framework](architecture.md)
- **The End-to-End Test Suite** (this document)
- [Reliability, Error Handling, and Security](reliability-and-security.md)

Unit tests check pieces in isolation. The end-to-end suite is where Phantom is
made to prove its reliability claims against the real three-package stack: the
service, the client SDK, and the emulator standing in for the upstream, all
wired together over real sockets. Most of the tests below do not just check a
happy path. They inject failures (upstream outages, expired tokens, killed
processes, corrupted files, held database locks, mid-upload disconnects) and
assert that Phantom behaves correctly anyway. That is the point of the suite: to
show the durability is real, not asserted.

For the two-step emulator protocol, exact cardinality comes from its typed,
append-only upstream event oracle: every successful metadata-create response
(including a cached idempotency hit) and every accepted body PUT is a distinct
event. The older token-keyed accepted-body map remains useful as a latest-value
view but is not an exact-once oracle because a repeated PUT overwrites its key.

## How the suite is tiered

Tests carry markers that sort them into lanes, so the fast majority can run on
every change while the long and resource-heavy ones run on their own schedule.

| Marker | Meaning | Where it runs |
|---|---|---|
| `e2e` | Exercises the full stack. Applied automatically to everything in the suite. | The default lane |
| `load` | Long-running, minutes of wall-clock (for example a sustained 15-minute soak). | Per-change CI and a dedicated load lane |
| `perf` | Asserts a latency or throughput budget. Excluded from the default lane because a busy host produces false failures. | A dedicated, quiet perf lane |
| `stress` | High-volume bursts. | A nightly schedule |

The default local selection runs everything except the `load`, `perf`, and
`stress` lanes. Continuous integration runs that same default selection on each
change, plus a separate per-change job for the `load` lane. The `perf` lane is
manual only (`workflow_dispatch`, on a quiet runner). The `stress` lane runs
nightly. Those three workflows are the whole CI set; there is no full-sweep
lane.

By the numbers, the suite is 127 end-to-end test files (the top level
plus the `crash_recovery/`, `regression/`, `stress/`, `all_ram/`,
`ingress_abort/`, and `db_contention/` subdirs) and 243 test functions:
226 in the default lane, 2 in `load`, 9 in `perf`, and 6 in `stress`,
with one designed `xfail` (counts as of 2026-07-01). Counts drift as tests land - see
`tests/e2e/regression/COVERAGE.md` for the authoritative failure-mode map.
Performance budgets throughout are named constants in the tests, not bare
numbers, so a budget is always self-documenting.

## Functional tests

These cover the normal feature surface and its first-order failure handling. Each
entry notes what it proves, the database effect it checks, and any performance
budget it asserts.

- **Happy path.** A two-step upload chain (a metadata POST, then a presigned
  object-store PUT) runs to completion. Asserts the fast synthetic acknowledgment,
  the chain reaching `succeeded`, the upstream receiving the body, and the
  tracking ID round-tripping. Database: a terminal `succeeded` row.
- **Metadata step down.** The first step returns 5xx, is retried, and recovers
  once the failure clears. Asserts at least two attempts during the outage, then
  `succeeded`. Performance: the synthetic acknowledgment stays under budget even
  while the upstream is failing.
- **Object step down.** The second step returns 503 repeatedly, then recovers.
  Asserts that the first step is not re-executed on recovery (the upstream sees
  exactly one metadata pair). Performance: acknowledgment under budget.
- **Auth refresh.** A 401 parks the row in `auth_expired`. An external token push
  lands a fresh bearer, and the auth kicker wakes the row to `succeeded`.
  Database: `auth_expired` then `succeeded`.
- **Capture expiry.** With re-execution off (the default), an expired capture
  window moves the chain to `stored` with its body still recoverable. With
  re-execution on, Phantom re-runs the first step under an idempotency key and the
  upstream returns the same object identity, reaching `succeeded`.
- **Shutdown and restart.** A row is in `attempting` at shutdown; on restart,
  recovery resets it to `queued` and the next attempt completes. Database: the
  RAM-to-disk persist is verified and the body survives the restart.
- **Unknown sender fault and restart.** A test-owned child launcher stops the
  real sender immediately before claim and immediately after durable claim,
  then raises an unknown exception. The production process must exit non-zero;
  the stopped database must show `queued` or `attempting` respectively; an
  unpatched restart must reconstruct saturation at one while upstream is
  paused, then deliver exactly one metadata-create/PUT sequence and reconcile
  saturation to zero. Under pinned uvicorn 0.46, the fatal bridge's supported
  status is signal termination by SIGTERM after a graceful drain.
- **Parser errors.** A malformed or oversized envelope produces a structured
  error envelope on the wire, which the client surfaces as a typed validation
  error carrying the error code and details.
- **Saturation cap.** With a low in-flight limit, the request past the limit is
  refused at ingress with a saturation code; once one in-flight upload finishes,
  the gate reopens. Database: no row is admitted on the overflow.
- **Concurrent throughput.** Many uploads run at once with distinct bodies; the
  atomic claim prevents any row being claimed twice. Runs in the `stress` lane.
- **Storage encoding.** A round trip under the encode-at-rest codec, with and
  without a content-encoding header, confirms the wire body is byte-identical.
- **Stored admin flow.** A chain that reaches `stored` is then cancelled, and
  separately replayed, through the admin API. Database: `stored`, then
  `cancelled` or `succeeded`.
- **Multi-row auth refresh.** Several rows sharing one token slot all park on a
  401; a single token push wakes all of them, and each succeeds exactly once.
- **Reaper retention.** Per-state retention windows are honored: the reaper
  deletes each terminal row at its configured boundary.
- **Bulk export.** Three uploads in three different terminal states are streamed
  out through the admin export, and the manifest plus body files are inspected.
- **Identity-scheme coexistence.** The same endpoint serves two different
  bearer-token identity schemes; each gets its own token-cache slot carrying its
  own bearer.
- **Multi-instance dispatch.** Two upstreams and two instances confirm dispatch
  correctness and that admin queries scope to one instance. Database: per-instance
  row isolation.
- **Sustained soak (load).** Ten requests per second for fifteen minutes. Asserts
  in-flight stays under the cap, the reaper keeps the success backlog flat, and
  RAM bytes never cross the ceiling.
- **Kill-and-recover idempotency (load).** A burst is put in flight, the process
  is genuinely killed with SIGKILL mid-write, and restarted on the same data
  directory. Recovery cleans the partial persists and every surviving chain
  reaches a terminal state exactly once, with no double send.
- **Concurrent gather, all succeed and one fails.** Mirrors a workload that fires
  several uploads at once. The first variant asserts independent chains with no
  cross-contamination; the second sends one of the concurrent calls a 401 and
  asserts per-chain failure isolation. Performance budgeted.
- **Large sequential routine.** The largest sequential workload (dozens of
  uploads, several megabytes) runs within a wall-clock budget. Performance lane.
- **Large-body tier migration.** A body larger than the RAM ceiling is forced
  through a RAM-to-disk migration, including a multi-megabyte case. Performance
  lane. Database: the persist transition is verified.
- **Process-pool fan-out.** A dozen concurrent clients share a single token slot
  and all reach terminal. Performance lane.
- **One refresh wakes many.** A single cache write wakes every parked row.
  Performance budgeted.
- **Token-cache and auth-kicker lifecycle.** Rows expire to `auth_expired` and
  resume after a token is pushed, exercising the wait strategy and the cache
  directly.
- **Hostname dispatch.** With no routing header present, requests are dispatched
  to the correct instance purely by the target URL's hostname; corrupting one
  instance's database quarantines only that instance. The pair is never skipped:
  when `127.0.0.2` is not bindable, fixture setup fails loudly with a message
  naming `scripts/dev/ensure_loopback_alias.sh`.
- **Synthetic workload burst.** A realistic mixed-size upload burst (small and
  large objects, a config blob, an HTML payload) runs with synthetic bytes; a
  drift checker keeps the synthetic shape honest over time.
- **Files lost midway.** An upstream truncation mid-transfer is retried to success
  with no data loss; a body file deleted from disk after persistence drives the
  chain to `corrupted`.
- **Hot reload.** A signal and an admin reload change retention and saturation
  settings at runtime; in-flight rows keep the settings they were admitted under;
  the swap is atomic under a burst.
- **Multipart.** A multi-file upload delivers every file byte-identical; a
  corrupted constituent file drives the chain to `corrupted` rather than
  delivering tampered data; replaying the same multipart chain yields one row and
  no duplicate upstream traffic.
- **Startup guards.** Six tests through a real boot confirm owner-only file
  permissions, the cold-backup artifact appearing, and the guards that refuse to
  boot in an unsafe state.
- **Transparent proxy.** Ten tests confirm byte-identity across codecs (pass
  through, zstd, gzip), across JSON and multipart shapes, for a pre-compressed
  body, after a 5xx retry, for a 50 MiB body, and after a persist transition.
- **Concurrency under load.** Five tests confirm byte-identity under a
  fifty-way burst, during encode, under a saturation refusal, in a mid-encode
  collision, and with two senders against one upstream.

### Fake-S3 routing and SigV4 re-signing

This feature lets a stock S3 SDK point at Phantom over plain HTTP while Phantom
re-signs each upload with AWS SigV4 for the real bucket.

- **SigV4 re-sign keystone (the proof the feature works end-to-end).**
  [`tests/e2e/test_e2e_sigv4_resign_round_trip.py`](../../tests/e2e/test_e2e_sigv4_resign_round_trip.py)
  drives a stock PUT through the catch-all, Phantom re-signs it with `S3SigV4Auth`,
  and the emulator's SigV4 sink validates it, asserting byte-identity and the
  signed `x-amz-content-sha256`. Four legs: the happy round trip
  (`test_sigv4_resign_round_trip_keystone`), a wrong credential parking the row in
  `auth_expired` (`test_sigv4_wrong_credential_parks_auth_expired`), the
  `CredentialKicker` refresh loop where a corrected credential push wakes the
  parked row to success (`test_sigv4_refresh_loop_wrong_then_correct_credential`),
  and a directly-corrupted signature rejected `403`
  (`test_sigv4_corrupted_signature_direct_put_rejected_403`). A parametrized leg
  (`test_sigv4_resign_per_verb_round_trip`) repeats the round trip for each
  forwarded verb and asserts the stored `S3Object.method`.
- **Forward-as-is (`auth_mode: none`).**
  [`tests/e2e/test_e2e_raw_intake_forward_as_is.py`](../../tests/e2e/test_e2e_raw_intake_forward_as_is.py)
  forwards a bare upload (e.g. a presigned URL; its own signature is the auth)
  unchanged to the emulator's auth-free `/raw` sink; a parametrized leg asserts
  the stored `RawBody.method` for each verb.
- **Destination via the `?phantom=` carrier.**
  [`tests/e2e/test_e2e_raw_intake_phantom_carrier.py`](../../tests/e2e/test_e2e_raw_intake_phantom_carrier.py)
  proves the query-carrier leg of destination resolution: the carrier alone
  names the destination with no `phantom_default_target` configured, and the
  carrier wins when both are set (two tests, default lane).
- **HTTPS over a real TLS listener.**
  [`tests/e2e/test_e2e_https_listener.py`](../../tests/e2e/test_e2e_https_listener.py)
  runs the same re-sign-and-forward path with `server.tls.enabled`, proving the
  upload lands byte-identically over HTTPS. This is distinct from the keystone
  (which proves re-signing over **plaintext**) and from the TLS unit test (which
  owns cert generation, rotation, and the XOR cert/key validator); the three are
  not interchangeable.
- **Config-provisioned credential.**
  [`tests/e2e/test_e2e_config_sigv4_credential.py`](../../tests/e2e/test_e2e_config_sigv4_credential.py)
  boots with a `sigv4_credentials` config block (env-var names) and no admin push,
  then drives a successful re-sign, closing the config-to-store boot path e2e.
- **Emulator sinks accept all forwarded upload verbs.** The sinks now accept
  `PUT`, `POST`, and `PATCH` (the catch-all's full forwarded set), not PUT-only.
  [`src/phantom-emulator/tests/unit/test_s3_router.py`](../../src/phantom-emulator/tests/unit/test_s3_router.py)
  covers the SigV4 sink's POST/PATCH legs (happy `200` + `method` recorded,
  `403` bad signature, `400` missing `x-amz-content-sha256`, `413` over cap) plus
  a verb-set invariant test pinning the sink's `UPLOAD_METHODS` to the catch-all's
  source of truth;
  [`src/phantom-emulator/tests/unit/test_raw_sink.py`](../../src/phantom-emulator/tests/unit/test_raw_sink.py)
  covers the raw sink's POST/PATCH legs and the load-bearing router-registration
  order.
- **Signer unit coverage.**
  [`src/phantom-service/tests/unit/test_sigv4_executor.py`](../../src/phantom-service/tests/unit/test_sigv4_executor.py)
  proves the `profile_ref` signing arm (botocore session faked at the module seam,
  zero real AWS/SSO I/O): it signs correctly, applies the three-tier region
  fallback (`us-east-1` default; session config wins), and raises a SigV4 signing
  error on an empty credential chain. The TLS unit test
  ([`src/phantom-service/tests/unit/test_tls_listener.py`](../../src/phantom-service/tests/unit/test_tls_listener.py))
  serves a real in-process HTTPS `200` (`test_single_listener_serves_https_200`)
  with a plaintext-to-TLS-port negative control, and owns cert generation,
  rotation-near-expiry, and the half-configured (cert-only / key-only) rejection.
- **Client credential surface.** The `push_credential` test in
  [`src/phantom-client/tests/unit/test_client.py`](../../src/phantom-client/tests/unit/test_client.py)
  asserts both credential arms issue `PUT /v1/admin/credentials/{dest_host}` with
  the serialized body, and pins the contract that the strict client model rejects a
  raw `service` string (callers must pass a `SigningService` member).

## Aggressor tests

The aggressor tests are written in an adversarial spirit: each one tries to break
a specific property under an awkward input or an awkward race. They are a good
place to see what Phantom guarantees.

- **Admin envelope byte-fidelity.** The metadata and per-step headers stored on a
  row come back byte-identical through the admin detail view.
- **Admin completeness.** The admin chain view returns the full metadata,
  headers, and captures; the listing surfaces metadata.
- **Body round-trip.** A body retrieved through the admin body endpoint is
  byte-equal to what was sent.
- **Gzip codec.** A gzip codec is honored end to end across body sizes.
- **Cross-instance isolation.** Two instances with separate data directories keep
  their bodies isolated on disk; a body fetched through one instance is absent
  from the other.
- **Empty-body POST and PUT.** A zero-length body is accepted and persisted end to
  end, in the production empty-body shape.
- **Process-pool burst.** A sixteen-way process-pool burst (the upper bound of
  host cores) runs to completion. Performance budgeted.
- **Idempotency dedup.** Re-sending under the same idempotency key returns the
  existing row as a replay, with no new row and no duplicate upstream call.
- **List pagination.** Fifty uploads are paginated with a limit and a cursor; the
  collected IDs are a complete superset with no duplicates.
- **Mixed-encoding chain.** Under a gzip codec, a pre-compressed body is stored
  as-is (no double compression) while a raw body is encoded.
- **Multi-item pattern.** A workload of several dozen sequential uploads across a
  shared token slot all deliver.
- **Token rotation.** A token pushed during a retry is used for the next attempt
  rather than the bytes cached at submit time; auth-kicker recovery via an admin
  push is also covered.
- **Transparent-proxy headers.** Custom upstream metadata headers are preserved,
  Phantom's own control headers never leak upstream, header casing is preserved,
  and the authorization header is substituted with the cached token.
- **Control-header strip edge cases.** Five tests confirm the control-header strip
  is prefix-scoped and case-insensitive and is not fooled by embedded values,
  repeated mixed-case names, an extended namespace, a substring that is not a
  prefix, or whitespace-padded names.

## Reliability tests

These directories hold the tests that target crash safety, resource exhaustion,
database contention, and concurrency races directly.

### Crash recovery

- A corrupt database is quarantined on a real subprocess boot.
- A crash mid-admission leaves neither an upload row nor an idempotency claim
  (the admission transaction is atomic).
- A crash at each of three labeled windows of a RAM-to-disk migration is
  resolved correctly on restart, confirming the commit-last ordering and orphan
  cleanup.
- Running recovery twice produces the same end state (recovery is idempotent).
- A genuine SIGKILL under load, in both hybrid and all-disk modes, restarts
  healthy with no "database is locked" error.
- A recovery boot survives an external write-lock held well past the busy timeout,
  riding it out with the bounded backoff and still coming up healthy with every
  seeded row intact.

### RAM-only mode

- A SIGKILL in all-ram mode loses the backlog by design, but loses it cleanly: the
  restart comes up healthy with a consistent, empty database.
- All-ram mode has no disk fallback, so admitted bytes are bounded only by the
  in-flight byte limit; admission refuses gracefully with a 503 rather than
  running the host out of memory.

### Database contention

- A sustained external write-lock during continuous write churn produces no
  corruption and no unbounded WAL growth.

### Ingress abort

- A client that declares a large content length, sends a fraction, and drops the
  connection leaks no saturation slot and no orphan body.
- A burst of overlapping mid-body aborts under a constrained in-flight limit does
  not exhaust the slot pool.

### Regression

These pin specific bugs so they cannot return.

- A duplicate chain ID returns a structured 409 rather than a naked 500.
- A duplicate multipart part name returns a structured 4xx rather than silently
  overwriting.
- An idempotency key reused with a different body is not silently dropped behind a
  success-shaped response.
- A missing body raises the missing-body error rather than silently routing an
  empty payload, and a worker exception cancels its siblings.
- A duplicate submit of an in-flight chain is rejected without destroying the
  original, which still delivers.
- A success response that is missing a required capture becomes a retryable
  condition and the chain reaches a clean terminal state with no wedge and no slot
  leak.
- An ambiguous outcome (the object was stored but the acknowledgment was lost to a
  reset or truncation) is delivered exactly once.
- An admission burst under a held external lock returns only successes or clean
  503s, and recovers after the lock releases.
- The retry cadence reaches a terminal state on budget exhaustion, holds its
  backoff rather than hammering, and survives a SIGKILL mid-schedule.

### Stress

- A burst of one thousand uploads succeeds, and is safe to replay.
- Many concurrent submits under one idempotency key result in exactly one insert
  winning.
- Driving RAM above the ceiling makes the watcher migrate the oldest bodies to
  disk and unblocks admission.
- Sustained RAM pressure on in-flight rows does not pin RAM above the ceiling
  indefinitely.

## What the suite proves, grouped by property

- **Crash recovery.** At every labeled crash position, restart-time recovery
  converges to exactly one correct state, with no orphaned body and no half-written
  row.
- **Kill idempotency.** An abrupt kill mid-write loses nothing durable in the
  hybrid and all-disk modes, and loses the backlog cleanly in all-ram mode by
  design. The kills are real SIGKILLs of the running subprocess, not simulated.
- **No silent data loss.** A body that vanishes or is corrupted surfaces as the
  `corrupted` state, never as a silent empty delivery, and an upstream-signaled
  mid-transfer truncation retries to success.
- **Multipart atomicity.** Multi-file uploads succeed or fail as a unit, each part
  byte-identical; duplicate part names are rejected; replays do not duplicate.
- **Transparent proxying.** The upstream receives byte-identical bodies and
  headers, with the documented exception of the auth headers: substituted with
  the cached bearer (`phantom_bearer`) or reconstructed by the SigV4 re-sign
  (`aws_sigv4`), the body bytes unchanged either way. Phantom's own control
  headers are stripped.
- **Admin completeness.** Every stored chain is fully reconstructable through the
  admin API, pagination surfaces every row exactly once, and the export recovers
  every buffered body.
- **Isolation.** With no routing header, requests dispatch to the correct instance
  by hostname, and each instance's storage is fully isolated.
