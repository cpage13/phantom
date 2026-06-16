# 028. Grouping schema: group_id, multifile_id, send_order, sent_at

Status: Accepted
Date: 2026-06-10

## Context

The old `batch_id` column carried two unrelated jobs at once: a
uniformity handle (admission defaulted it to `chain_id`, so every row
had one) and a multi-file association. Query grouping, multi-file
membership, and delivery time were not separately answerable, and
`order_in_batch` implied an ordering contract the sender never had.

## Decision

One clean-break schema revision (`schema.sql`) splits the jobs:

- `group_id TEXT NOT NULL` - the query-grouping axis. Admission stores
  the `X-Phantom-Group-Id` header value, else `chain_id`; the response
  echo `X-Phantom-Group-Id` is always present.
- `multifile_id TEXT` nullable - the old `batch_id` renamed to its one
  remaining job, multi-file association. NULL means standalone.
- `send_order INTEGER NOT NULL DEFAULT 0` - the old `order_in_batch`
  renamed. A RECORDED position for display, NEVER enforced at delivery:
  no claim gate, no release endpoint, no cross-row delivery coupling
  (the enforcement blueprint stays parked out of v1).
- `sent_at TEXT` nullable - ISO-8601 UTC, stamped once on confirmed
  delivery by the sender's chain-done success branch only (ADR-015),
  guarded write-once (`sent_at IS NULL`), never moved, survives
  operator replay.

The NOT NULL vs nullable asymmetry is deliberate: `group_id` is the
universal query handle (every upload is a group of one; the uniformity
job the old `batch_id` carried moves here, and the echo is always
present), while `multifile_id` marks an exceptional structure (NULL
answers "is this row part of a multi-file set" crisply, and SQL NULL
never equals NULL, so standalone rows can never correlate
accidentally).

Indexes: `idx_uploads_group_id(group_id)` is a full index (NOT NULL
leaves no rows to exclude); `idx_uploads_batch` is replaced by
`idx_uploads_multifile(multifile_id, send_order)`; `sent_at` gets NO
index (surfaced and aggregated, never range-queried).

Version-gate posture: `SCHEMA_VERSION` bumped 1 to 2 with NO migration
registered. The existing gate (`run_schema_gate`) routes any DB whose
version stamp or uploads column set does not match to
discard-and-boot-fresh, which IS the clean break (safe: the deployed
population is zero). The empty `SCHEMA_MIGRATIONS` registry and the
`SchemaMigration` shape stay as the reserved seam for the day a
populated deployment needs a real migration.

## Consequences

- Group queries, multi-file membership, and delivery time are three
  separately answerable questions with honest types.
- Standalone rows can never be accidentally swept into a multi-file
  query; a group is addressable for every upload with zero caller
  effort.
- Operators can read confirmed delivery time directly (`sent_at`)
  instead of inferring it from `updated_at` on a succeeded row.
- Recorded-not-enforced ordering keeps delivery independent per row; a
  stalled member cannot starve its set.

## Cross-references

- `phantom/storage/schema.sql` and `phantom.storage.sqlite_store`
  (`SCHEMA_VERSION`); `phantom.runtime.startup_checks`
  (`run_schema_gate`, `EXPECTED_UPLOADS_COLUMNS` derived from
  schema.sql at import).
- `phantom.routes.admission` - the header-else-default stores.
- ADR-015 - the sender ownership the `sent_at` stamp rides.
- ADR-012 - the SDK mirrors every model change in the same phase.
