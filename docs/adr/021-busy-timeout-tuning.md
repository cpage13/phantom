# 021. SQLite busy_timeout 5000 → 1000 ms

Status: Accepted
Date: 2026-05-29

## Context

Every Phantom writer to `uploads.db` serializes through ONE `asyncio`
lock (`_write_lock`) on a single shared `aiosqlite` connection. Two
writers in the same process can therefore never contend for the SQLite
write lock — the application-level lock already serializes them. The
SQLite `busy_timeout` pragma only ever fires for **external**
contention: a separate process on the box (a backup tool, an admin
script, a sibling connection) holding a write lock on `uploads.db` past
the timeout window.

The pragma was set to `5000` ms. A hardening-cycle finding (the external
cross-process lock-contention probe) showed that a *long* timeout is
actively harmful under external contention. When a sibling holds
`BEGIN IMMEDIATE` longer than a few hundred milliseconds, each contended
Phantom writer monopolizes the single writer slot for the full timeout
window. An admission burst then queues serially behind multiple
five-second busy-waits, and the producer's HTTP read times out before
Phantom can return the clean retryable `503 storage_unavailable` that
the contention path is designed to produce (see ADR-023). The long
timeout converts a transient external blip into a wave of client-visible
HTTP timeouts.

Durability is not at stake either way: a write that loses the contention
commits no row, and the data layer never corrupts under an external lock
(independently confirmed). The only question is the *latency posture*
under contention.

## Decision

Lower the SQLite `busy_timeout` from `5000` ms to **`1000` ms**. The
value is the module constant `SQLITE_BUSY_TIMEOUT_MS` in
`storage/sqlite_store.py`, applied via `PRAGMA busy_timeout=` when the
connection opens. The token-cache connection imports and shares the
same constant, so the two connections stay in lockstep.

One second is long enough to ride out sub-second contention blips
(the common case — a quick sibling read or a short backup transaction)
and short enough to fail fast under a *sustained* hold, returning a
clean retryable signal quickly instead of blocking the single writer
slot. Boot-time recovery rides out far longer external locks via its
**own** bounded retry-with-backoff (ADR-022), independent of this value,
so lowering `busy_timeout` does not weaken startup resilience.

### Recorded direction: promote to a typed `SqliteCfg.busy_timeout_ms`

The timeout currently lives as a bare module constant. The recorded
forward direction is to promote it to a typed `SqliteCfg.busy_timeout_ms`
field (alongside the existing `synchronous`, `journal_mode`, and
`journal_size_limit_bytes` knobs), so a deployment with an unusual
contention profile — e.g. a rack server with a battery-backed write
cache and a known long-running sibling — can tune it without a code
change, the same way `synchronous` is already tunable. Until that field
lands, `1000` ms is the single binding for every deployment. The default,
once the field exists, stays `1000` ms.

## Consequences

- **Faster clean failure under sustained external contention.** A
  contended admission returns `503 storage_unavailable` + `Retry-After`
  within ~1 s rather than ~5 s, staying under typical producer HTTP read
  timeouts.
- **No durability change.** A failed contended write commits nothing;
  the data layer is unaffected.
- **No startup-resilience change.** Recovery's own retry loop, not
  `busy_timeout`, governs how long boot rides out an external lock.
- **Two pinned unit tests updated in lockstep** to assert the lowered
  value on both the upload-store and token-cache connections.
- **Behavior change beyond a literal bug fix.** This is a tuning
  decision, not a defect repair — recorded here so the rationale is
  durable and the value is not silently raised again.

## Cross-references

- `phantom.storage.sqlite_store` — `SQLITE_BUSY_TIMEOUT_MS` and the
  `PRAGMA busy_timeout` application site.
- `phantom.storage.token_cache` — imports and shares the constant.
- `phantom.config.settings.SqliteCfg` — the typed config block the
  timeout is slated to join.
- ADR-022 — recovery's independent bounded retry over external locks.
- ADR-023 — the transient-lock classifier that turns a contended
  admission into a clean `503 storage_unavailable`.
- ADR-017 — the `storage_unavailable` error-code row.
