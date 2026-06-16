# 029. A dedicated read-only connection for admin and SDK reads

Status: Accepted
Date: 2026-06-10

## Context

Every read and write on an instance's `uploads.db` shared ONE aiosqlite
connection behind one `asyncio.Lock`. Status polling therefore queued
behind admission and sender writes (and could queue them behind a slow
read), and a long-lived read cursor on the shared connection could
collide with the writer's checkpoint. With cycle-7 adding a group
rollup, two identifier lookups, and SDK pollers, read traffic grows and
must not contend with the hot write path.

## Decision

`SqliteUploadStore` opens a SECOND aiosqlite connection in `start()`
via `file:<path>?mode=ro` (uri=True), after the schema/stamp/checkpoint
block, with the writer's `busy_timeout` and a split-brain probe (the
reader's `user_version` must equal `SCHEMA_VERSION`). All read-only
store methods route through it; every write keeps the existing single
serialized writer connection and its one lock, untouched. WAL (already
the journal mode) is what makes the reader safe beside the writer; no
write-path semantics change.

The store hides the split: routes and the SDK see `UploadStore`
methods, never connections. SELECTs that are transactionally coupled to
writes (inside `bulk_delete`, eviction, the idempotency claim) stay on
the writer by design. Error classification on the read path consults
the one shared transient-lock classifier (ADR-023); defining a second
classifier is forbidden, and read-method errors propagate raw with no
in-store retry. Lifecycle: the reader opens only after the instance's
integrity gate and mode guard have finished moving files, and during a
staged quarantine restore it keeps its old descriptor until the
required restart, exactly like the writer. A `:memory:` store (unit
test convenience) gets no reader; file-backed stores always split.

Known semantics, documented at `_read_connection`: overlapping reads on
the one reader connection share its read transaction, so an overlapped
read can serve a snapshot as of the oldest still-active read's start. A
quiescent read always sees the latest commit; staleness is bounded by
read duration. Reads are always consistent committed snapshots.

## Consequences

- Client status polling cannot queue behind, or be queued behind,
  admission and sender writes; a write completes promptly while a read
  cursor is open mid-iteration.
- SQLite itself enforces the split (`attempt to write a readonly
  database`), so a write can never creep onto the read path.
- One more file descriptor and connection per instance; reads stay
  consistent snapshots with read-duration-bounded staleness under
  overlap.

## Cross-references

- `phantom.storage.sqlite_store._read_connection` - the routing point
  and the snapshot-semantics docstring.
- ADR-023 - the single transient-lock classifier the read path
  consults.
- ADR-021 - the `busy_timeout` the reader shares.
- ADR-030 - the companion per-database writer split (the token cache's
  own file).
