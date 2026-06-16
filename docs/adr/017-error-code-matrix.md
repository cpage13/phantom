# 017. Error-code matrix

Status: Accepted
Date: 2026-05-27

## Context

Phantom emits HTTP status codes plus an ADR-010 `ErrorEnvelope` body
on every error response. The complete vocabulary was scattered across
plan §5.6, the `phantom.models.errors` module, the
`phantom_client.errors` exception hierarchy, and ad-hoc README
sections — WS-1 D13. A reader/integrator had no single page to
answer *"what does Phantom return when X happens, and what should
my code do about it?"*.

This ADR is that single page. It is authoritative: the source-code
`STATUS_FOR_CODE` map and `EXCEPTION_FOR_CODE` map must match this
table. The Phase 5 admin contract tests (§ 6.2.7) assert that
each documented `(condition, code)` pair fires the documented
status; future changes update this ADR in lockstep with the maps.

## Decision

Every HTTP status code Phantom returns is documented below with the
condition that fires it, the ADR-010 `error.code`, the response body
shape, the typed `phantom-client` exception class, and the expected
client action.

### Error envelope shape

Every Phantom error response (2xx admin replies excepted) carries a
JSON body of shape:

```json
{
  "error": {
    "code": "<stable_string>",
    "message": "<human readable>",
    "instance_id": "<InstanceCfg.id or 'unrouted'>",
    "request_id": "<per-request correlation id>",
    "details": { "<code-specific>": "..." }
  }
}
```

Source of truth for the envelope: `phantom.models.errors.ErrorEnvelope`.

### Status × code table

| HTTP status | `error.code` | Condition | `details` payload | `phantom-client` class | Client action |
|---:|---|---|---|---|---|
| 400 | `header_invalid` | An optional grouping/ordering request header on `POST /v1/send` was present but failed to parse: `X-Phantom-Group-Id` / `X-Phantom-Multifile-Id` must be UUIDs, `X-Phantom-Order` a non-negative integer. Rejected loudly so the producer learns its grouping intent was dropped instead of the upload being silently filed under the chain_id defaults (cycle-7 task 2.2). | `{ "header": "<name>", "value": "<as supplied>" }` | `PhantomBadRequestError` | Producer fixes the header value. the upstream client fallback propagates (4xx). |
| 400 | `lookup_not_configured` | `GET /v1/admin/uploads/by-captured-id/{captured_id}` targeted an instance (directly or via fan-out) whose configuration carries no `admin_lookup` binding (`capture_name` + `json_path`). Where the upstream identifier lives inside the captured values is deployment knowledge; Phantom never guesses, and silently skipping an unconfigured instance would lie about it (cycle-7 task 4.3). | `{ "unconfigured_instances": [...] }` | `PhantomBadRequestError` | The operator adds the `admin_lookup` block to the instance config, or scopes the query with `?instance=` to a configured instance. Loopback admin endpoint; not an ingress path. |
| 401 | `auth_token_missing` | Inbound `POST /v1/send` is missing `Authorization` for a route whose `auth_mode` is `phantom_bearer` and no cached token exists. | `{}` | `PhantomUnauthorizedError` | The upstream client fallback: 4xx propagates to caller. Client refreshes credentials and retries. |
| 404 | `not_found` | Admin lookup for a chain/instance/token slot that does not exist (`GET /v1/admin/chains/{chain_id}` etc.). | `{}` | `PhantomNotFoundError` | Caller treats as "no such resource"; no retry. |
| 413 | `body_too_large` | `POST /v1/send` Content-Length precheck OR mid-stream cap breach. Triggered by `Settings.storage.max_buffered_bytes`. Closes the H2 audit. | `{ "limit_bytes": <int>, "actual_bytes": <int> }` | `PhantomPayloadTooLargeError` | Operator splits the upload or raises `max_buffered_bytes`. the upstream client fallback propagates (4xx). |
| 421 | `invalid_target` | The first chain step's URL hostname matched no configured `InstanceCfg.host_prefixes` (routing is by the first step's URL; there is no target header). | `{}` | `PhantomBadRequestError` | Producer fixes the step URL OR the operator adds the host prefix. the upstream client fallback propagates. |
| 421 | `instance_unknown` | The `X-Phantom-Instance` override header named an instance id that is not configured. | `{}` | `PhantomBadRequestError` | Producer fixes the override OR the operator configures the instance. the upstream client fallback propagates. |
| 422 | `envelope_invalid` | Chain envelope failed Pydantic validation (missing required fields, wrong types, etc.). | `{ "validation_errors": [...] }` | `PhantomValidationError` | Bug in the caller's envelope construction; surface to operator. the upstream client fallback propagates. |
| 422 | `body_ref_missing` | Envelope referenced a `body_ref` name not provided in the multipart submission. | `{ "missing_refs": [...] }` | `PhantomValidationError` | Bug in the caller; the upstream client fallback propagates. |
| 422 | `body_ref_orphan` | A multipart `body_refs[<name>]` part was submitted but no chain step references it. | `{ "orphan_refs": [...] }` | `PhantomValidationError` | Bug in the caller; the upstream client fallback propagates. |
| 422 | `body_ref_duplicate` | A multipart submission carried two parts with the SAME `body_refs[<name>]`. Rejected for parity with the other `body_ref_*` codes (finding E-1) — silently last-wins dropped one body. | `{ "name": "<ref name>" }` | `PhantomValidationError` | Bug in the caller (sent the same part twice); the upstream client fallback propagates. |
| 422 | `envelope_duplicate` | A multipart submission carried two parts named `envelope`. Rejected for parity with `body_ref_duplicate` and the duplicate-step-name check (finding R3-9, the E-1 sibling) — silently last-wins dropped one envelope, and the envelope carries the chain_id / destination / step chain, so the producer could not tell which chain was admitted. | `{}` | `PhantomValidationError` | Bug in the caller (sent the envelope part twice); the upstream client fallback propagates. |
| 422 | `template_unresolved` | A chain step references a captured value (`{{step.name}}`) that no earlier step produces. | `{ "template": "<string>", "missing_capture": "..." }` | `PhantomValidationError` | Bug in envelope construction; the upstream client fallback propagates. |
| 422 | `idempotency_key_conflict` | `POST /v1/send` reused an `X-Phantom-Idempotency-Key` whose prior claim was made with a DIFFERENT body (body-hash divergence). An idempotency key MUST be a function of the body; reuse with different bytes would otherwise silently drop the second body behind a 200 replay (finding G-1). | `{ "idempotency_key": "<key>" }` | `PhantomUnprocessableError` | Bug in the caller's key derivation. The producer must use a body-derived key; the upstream client fallback propagates (4xx). |
| 422 | `multifile_cursor_conflict` | `GET /v1/admin/chains` combined `?multifile_id=` with `?cursor=`. The multifile listing is one-shot by design (ordered by `send_order`, never paginated, `next_cursor` always null), so a cursor cannot apply to it (promoted from FastAPI's raw `{"detail": ...}` body to the canonical envelope by the round-2 defender fix, R2-2). | `{ "multifile_id": "<uuid>", "cursor": "<token>" }` | `PhantomUnprocessableError` | The caller drops the cursor (multifile results arrive whole) or paginates without the multifile filter. Loopback admin endpoint; not an ingress path. |
| 422 | `key_value_match_invalid` | `GET /v1/admin/chains` carried a `?key_value_match=` value that does not parse as `key:value` with non-empty key and value (promoted to the canonical envelope by the round-2 defender fix, R2-2). The SDK always encodes the colon, so this surfaces to raw-wire callers. | `{ "key_value_match": "<as supplied>" }` | `PhantomValidationError` | The caller fixes the query value. Loopback admin endpoint; not an ingress path. |
| 422 | `bulk_delete_filter_empty` | `DELETE /v1/admin/chains` carried an all-None filter body. An empty filter would mean "delete every row", which the bulk surface refuses by design (ADR-004); promoted to the canonical envelope by the round-2 defender fix, R2-2. The SDK pre-flights empty filters (`EmptyFilterError`), so this surfaces to raw-wire callers. | `{}` | `PhantomUnprocessableError` | The caller sets at least one of `state` / `route` / `since` / `instance`, or deletes individual chains by id. Loopback admin endpoint; not an ingress path. |
| 422 | `request_invalid` | A typed path or query parameter failed FastAPI request validation (malformed UUID in `/groups/{group_id}` or `/uploads/by-local-uuid/{local_uuid}`, malformed or missing `backup_id` on `/quarantine/restore`, and any future typed parameter). Promoted from FastAPI's raw `{"detail": [...]}` body to the canonical envelope by the round-6 defender fix R6-4 via one shared `RequestValidationError` handler. | `{ "errors": [{"loc": [...], "msg": "...", "type": "..."}] }` | `PhantomValidationError` | Bug in the caller's identifier construction; the typed SDK coerces UUIDs up front, so this surfaces to raw-wire callers. Loopback admin surface (the ingress route parses its envelope manually and emits the `envelope_invalid` family). |
| 409 | `chain_id_in_use` | `POST /v1/send` carried an `envelope.chain_id` (the row primary key) already used by a live row. Distinct from idempotency replay: a re-POST of the same chain_id under a fresh idempotency key would otherwise escape as a naked HTTP 500 (finding D-1). | `{ "chain_id": "<uuid>" }` | `PhantomConflictError` | The producer must mint a fresh `chain_id`. UUID4 collisions are astronomically rare, so this almost always means a client bug reusing an id. the upstream client fallback propagates (4xx). |
| 409 | `restore_noop` | `POST /v1/admin/quarantine/restore` did not land the chosen `mode_switch` backup's DB half in the live tree: refused UP FRONT when the manifest's DB artifact is missing on disk (nothing displaced), or the moves ran and the DB still did not land (the artifact vanished mid-restore). Returning a success-shaped response would silently strand the buffered uploads (extreme-hardening H-1 / L-2). | `{ "backup_id": "<uuid>", "instance_id": "<id>", "interim_backup_db": "<path or null>", "interim_backup_body": "<path or null>", "cause": "<one line>" }` | `PhantomConflictError` | The operator re-checks `GET /v1/admin/quarantine` and retries with a valid `backup_id`. Loopback admin endpoint; not an ingress path. |
| 409 | `replay_body_discarded` | `POST /v1/admin/chains/{chain_id}/replay` named a row whose `body_discarded_at` is stamped: the body was already discarded per the row's own accounting (the sender's immediate leg at `succeeded_body_seconds == 0`, or the reaper's scheduled leg), so a re-queue could only land the row in `corrupted` on the sender's next claim. Refused up front; the row is left exactly as it was, `sent_at` preserved (cycle-7 phase 7 pre-round defender fix). | `{ "chain_id": "<uuid>", "body_discarded_at": "<iso timestamp>" }` | `PhantomConflictError` | The operator re-submits the upload through `POST /v1/send` if it must run again. Loopback admin endpoint; not an ingress path. |
| 409 | `replay_refused_attempting` | `POST /v1/admin/chains/{chain_id}/replay` named a row currently in `attempting`: a sender is actively driving it, and a re-queue would clobber the in-flight attempt (M-W4-F7 audit closure; promoted from FastAPI's raw `{"detail": ...}` body to the canonical envelope by the round-1 defender fix, R1-1). Refused up front; the row is left exactly as it was. | `{ "chain_id": "<uuid>" }` | `PhantomConflictError` | The operator waits for the attempt to settle (or cancels the chain first), then retries the replay. Loopback admin endpoint; not an ingress path. |
| 502 | `upstream_unreachable` | Phantom completed the chain attempt but the upstream HTTP call could not be made (connect refused / DNS failure). Surfaced via admin lookups, not POST `/v1/send`. | `{ "upstream_host": "..." }` | `PhantomServerError` | Surfaces on admin lookups only; the upstream client fallback applies to POST `/v1/send` failures, not admin. |
| 503 | `saturation_cap` | `SaturationGate.admit(declared_bytes)` returned `AdmissionRefusedSaturation` — the in-flight row/byte cap is full. Response carries `Retry-After`. | `{ "cap": "max_in_flight" \| "max_in_flight_bytes" \| "max_large_in_flight", "current": <int>, "limit": <int> }` | `PhantomUnavailableError` | The upstream client fallback swallows: delegates to its direct-to-upstream path. SDK does NOT retry; Phantom IS the retry engine. |
| 503 | `disk_pressure` | `DiskPressureProbe` observed `max_disk_bytes` exceeded; admission rejects to protect the disk. Response carries `Retry-After`. | `{ "max_disk_bytes": <int>, "current_disk_bytes": <int> }` | `PhantomUnavailableError` | the upstream client fallback swallows. Operator should free disk or raise `max_disk_bytes`. |
| 503 | `storage_unavailable` | A storage-layer write FAULT (an `OSError` — fsync EIO or ENOSPC) struck while admission was durably buffering the upload body: from `body_store.put`, or from the R11-1 chain_id namespace clear (`body_store.delete`) that precedes it (R11-1 doc update 2026-06-12). REACTIVE per-write failure, distinct from the PROACTIVE `disk_pressure` ceiling: a burst fills the real disk between probe ticks, or a flaky SD card returns EIO. Response carries `Retry-After`. Durability holds — the failed clear/put commits no row, so a retry-or-not loses nothing (findings R7-1-A/B, R7-2-A; the R7 durability result). | `{ "reason": "body_store_write_failed" }` | `PhantomUnavailableError` | the upstream client fallback swallows (503), but the retryable shape means the producer should retry per `Retry-After` rather than abandon buffering. Operator frees disk / replaces a failing SD card. |
| 500 | `internal_error` | Catch-all for unexpected exceptions reaching the FastAPI error handler. | `{ "exception_class": "..." }` (DEBUG mode only; production omits) | `PhantomServerError` | Bug in Phantom; the upstream client fallback swallows. Operator inspects logs. |

### Replay (idempotency hit)

`POST /v1/send` carrying an `X-Phantom-Idempotency-Key` that
matches a prior accepted chain returns **HTTP 200** (not 202) with
the previously-issued `ChainResponse` body and `error.code =
"idempotency_replay"` (informational, not an error in the
operational sense). The chain row is **NOT** re-admitted; the row
returned IS the one the prior POST created.

This is the H7 closure — admission's atomic transaction at
`routes/admission.py` writes the chain row + the idempotency claim
in one SQLite transaction; replay returns the existing row without
re-running anything.

### Row-level corruption codes (not HTTP responses)

Some `error.code` values surface **only on admin lookups** for rows
that hit the corrupted terminal state during sender body
verification — they are never returned as HTTP responses to a `POST
/v1/send` request:

| `error.code` | Trigger | Where surfaced |
|---|---|---|
| `storage_corruption` | Sender's body-load step found a `storage_hash` mismatch before decode, OR a body file was missing post-startup (ADR-014 + Phase 2 H8). Row transitions to `corrupted`. | `ChainAdminDetail.last_error` returned by `GET /v1/admin/chains/{chain_id}`. |
| `codec_round_trip_drift` | Sender's body-load step decoded successfully but the post-decode `body_hash` did not match the recorded raw-hash. Row transitions to `corrupted`. | Same as above. |

Both map to `PhantomServerError` on the client side so a caller
dispatching on `EXCEPTION_FOR_CODE` gets a sensible
"server-side-problem" exception class. The `STATUS_FOR_CODE` map
records `500` defensively for completeness — the codes never
actually emit over the wire.

### DB quarantine — NOT in the matrix

Phase 4's DB-quarantine path fires **at startup, before workers
spawn**. After quarantining, the service serves with empty state.
There is no `503 db_quarantined` response code post-quarantine.
The operator surface for quarantine is:

- ERROR-level log at startup recording the corruption + the backup
  destination: flat timestamped siblings in the instance data_root
  (`uploads.corrupted.<iso>.db` + `bodies.quarantine.<iso>/`).
- `db_quarantine_total` counter at `GET /v1/admin/observability/counters`.
- `GET /v1/admin/quarantine` returning the quarantine inventory.

See `docs/operator-playbook.md` "DB quarantine workflow" for the
recovery procedure.

## Consequences

- Source-of-truth: this table. The `phantom.models.errors.STATUS_FOR_CODE`
  map, the `phantom_client.errors.EXCEPTION_FOR_CODE` map, and the
  Phase 5 admin contract tests must agree with this table.
- Adding a new code requires: ADR-017 row update + `STATUS_FOR_CODE`
  entry + `EXCEPTION_FOR_CODE` entry + a contract test that asserts
  the code fires when expected.
- Removing a code requires: deletion from all three (no parallel
  schema — § 0.3 of the plan).

## Cross-references

- `src/phantom-service/src/phantom/models/errors.py` — `ErrorCode` literal,
  `STATUS_FOR_CODE` map, `ErrorEnvelope` Pydantic shape.
- `src/phantom-client/src/phantom_client/errors.py` —
  `EXCEPTION_FOR_CODE` map and the typed exception hierarchy.
- `src/phantom-service/src/phantom/routes/admission.py` — admission-time
  codes (`auth_token_missing`, `body_too_large`, `body_ref_*`,
  `template_unresolved`, `invalid_target`, `instance_unknown`,
  `envelope_invalid`, `saturation_cap`, `disk_pressure`,
  `storage_unavailable`, `idempotency_key_conflict`, `chain_id_in_use`).
- `src/phantom-service/src/phantom/routes/send.py`: the route-level
  `header_invalid` emit (grouping/ordering header parse, cycle-7).
- `src/phantom-service/src/phantom/chain/parser.py` — multipart-parser codes
  (`body_ref_missing`, `body_ref_orphan`, `body_ref_duplicate`,
  `envelope_invalid`, `envelope_duplicate`).
- `src/phantom-service/src/phantom/workers/sender.py` —
  `storage_corruption` / `codec_round_trip_drift` row-level emit.
- `docs/adr/010-request-chain-envelope-schema.md` — the
  `ErrorEnvelope` wire schema.
- `docs/adr/014-dual-body-hash.md` — the body-verification path
  that emits the corruption codes.
- `docs/architecture-intent.md` §7 (failure modes) — operator-facing
  description of each code.
