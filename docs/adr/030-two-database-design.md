# 030. Two databases per instance, on purpose

Status: Accepted
Date: 2026-06-10

## Context

Documentation claimed one persistent SQLite per instance, but
production wires two files: `uploads.db` (uploads + idempotency index)
and `token_cache.db` (the persistent token cache, ADR-003). The
mismatch read like drift, and `schema.sql`'s `token_cache` table
(empty in production, since the live cache applies its own DDL to its
own file) looked like dead code. The cycle-7 review asked: is the
second database deliberate, and if so, why?

## Decision

Two databases per instance is the settled design, now documented
instead of accidental-looking:

- SQLite serializes writers PER DATABASE FILE. A separate
  `token_cache.db` with its own connection and write lock keeps token
  reads and writes entirely off the hot uploads writer, so a token
  refresh burst never contends with admission or sender commits.
- A token is shared across many uploads; its lifecycle (refresh,
  mark-bad, delete) is independent of any one row, so it does not
  belong in the uploads transaction domain.
- Durability is tuned per database: both run WAL; the uploads store's
  `synchronous` is operator-parameterized (default NORMAL, FULL the
  documented recommendation where hard power cuts are expected); the
  token cache pins `synchronous=FULL` (token writes are rare, so the
  per-commit fsync cost is negligible there).

The empty `token_cache` table in `uploads.db` and the duplicate DDL in
`storage/token_cache.py` are deliberate and stay: the cache owns its
copy so it can boot against its own file, and the schema.sql copy keeps
the full declared shape in one reviewable place. Both sites carry
comments naming the duplication; nothing is consolidated.

## Consequences

- Token traffic and upload traffic cannot lock-contend by
  construction.
- Two files to back up, quarantine, and reason about per instance; the
  integrity machinery already treats the token-cache isolate as a
  first-class manifested backup (ADR-026).
- The duplicate DDL is a known, commented invariant: a change to the
  `token_cache` shape must land in both `schema.sql` and
  `SqliteTokenCache.start()`.

## Cross-references

- `phantom.storage.token_cache` - the cache's own file, connection,
  lock, and DDL.
- `phantom/storage/schema.sql` - the deliberately-kept duplicate
  table.
- `phantom.app` - wires `<instance data_root>/token_cache.db`.
- ADR-003 - the persistent token cache this houses.
- ADR-029 - the uploads-side read/write split this complements.
- `CONTEXT.md` "Two SQLite databases per instance (deliberate)".
