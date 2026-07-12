# 023. The transient-lock classifier as the single shared gate

Status: Accepted
Date: 2026-05-29

## Context

Three paths need to distinguish a *transient* SQLite lock/contention error
— one that will clear if you wait or retry — from a *genuine*
`OperationalError` that signals a real bug (a malformed schema, a missing
table, a type error):

- **Admission.** An external lock held past `busy_timeout` (ADR-021), or
  a same-connection cursor-vs-checkpoint collision, raises
  `sqlite3.OperationalError`. Admission must surface a transient one as a
  clean retryable `503 storage_unavailable` (ADR-017) and re-raise a
  genuine one as `internal_error` — never the reverse.
- **Recovery boot.** A write issued during the recovery sweep can hit the
  same transient lock. Recovery must ride it out with a bounded
  retry-with-backoff (ADR-022) and surface a genuine fault as a real
  error, rather than crashing startup over a transient lock or looping
  forever on a real one.
- **Sender storage boundaries.** Both `claim_due` and the post-claim
  `_drive_one` storage transitions can hit the same cross-process contention.
  The sender waits one poll and continues after a classified transient lock.
  A post-claim row is already durably `attempting`; the catch does not requeue
  or promise delivery, and startup recovery can reclaim it. A schema-class or
  unknown fault must escape TaskGroup supervision and trigger the production
  fatal-worker path.

If these three paths classified errors differently — or each rolled its own
substring check — they could drift: one path could treat a real
`no such table` as retryable and mask a bug behind an infinite retry,
while the other crashed correctly. The classification rule is
load-bearing and must be defined **once**.

## Decision

Introduce a single shared classifier,
`is_transient_lock_error(exc: BaseException) -> bool`, in
`storage/sqlite_store.py` (re-exported from `storage/__init__.py`). It is
the one definition of "this error is a ride-it-out lock contention, not a
permanent fault," and admission, recovery, and both sender storage boundaries
consult it.

### Deliberately narrow

The classifier returns `True` only for a `sqlite3.OperationalError`
whose lower-cased message carries one of a small set of known
`SQLITE_BUSY` / `SQLITE_LOCKED` fragments:

- `database is locked` — the cross-process write-lock timeout
  (`SQLITE_BUSY`).
- `database is busy` — an alternate `SQLITE_BUSY` phrasing across SQLite
  versions.
- `database table is locked` — table-level contention (`SQLITE_LOCKED`).

Matching on the fragment (not an exact string) keeps the classifier
robust across SQLite versions. Everything else returns `False`:

- A genuine `OperationalError` (malformed schema, `no such table`, a
  type error) carries none of these fragments and is correctly left
  un-classified — it must surface as a real fault, not a retry.
- Non-`OperationalError` exceptions (`IntegrityError`, `OSError`) are out
  of scope and return `False`, so their existing dedicated handling
  (chain-id collision → `409`, storage `OSError` → `503`) is untouched.

The parameter is typed `BaseException` so a call site can pass a caught
error without a prior `isinstance` narrowing; the classifier does the
`isinstance` check itself.

## Consequences

- **One classification rule, three consumers.** Admission, recovery, and
  sender storage handling cannot disagree about whether a given error is
  transient.
- **A real bug is never masked.** The narrow fragment set means a
  schema/type `OperationalError` is surfaced, not retried into a
  misleading `503` or an infinite loop.
- **Existing non-lock handling is preserved.** `IntegrityError` and
  `OSError` paths are explicitly out of scope.
- **The transient-contention class is fully handled.** A cross-process
  `SQLITE_BUSY` that outlasts `busy_timeout` and a same-connection
  cursor-vs-checkpoint `SQLITE_LOCKED` are both classified transient and
  routed to their respective clean responses.

## Cross-references

- `phantom.storage.sqlite_store.is_transient_lock_error` — the
  definition.
- `phantom.storage.__init__` — the re-export.
- `phantom.routes.admission` — the admission consumer (→ `503
  storage_unavailable`).
- `phantom.workers.recovery` — the recovery consumer (→ bounded
  retry-with-backoff).
- `phantom.workers.sender` — the pre-/post-claim consumer (→ wait and continue
  only for classified contention; other faults escape supervision).
- ADR-021 — the `busy_timeout` value the cross-process case outlasts.
- ADR-022 — recovery's bounded retry that the classifier gates.
- ADR-017 — the `storage_unavailable` / `internal_error` rows.
