# 019. Atomic admission transaction + idempotency claim

Status: Accepted
Date: 2026-05-27

## Context

Pre-Phase-1 admission (`phantom.routes.admission.admit_chain`):

1. Compute body hashes; assemble `UploadRow`.
2. **INSERT row** into `uploads`.
3. **INSERT idempotency claim** into `idempotency_index`.
4. On idempotency-collision (claim INSERT raises): catch, delete
   the orphan row from step 2, attempt to read the existing row,
   return 200.

The window between step 2 and step 3 was an exposed race
(WS-3 H7): a worker that observed the row before step 3 committed
could begin processing it; the idempotency-collision rollback in
step 4 would then delete the row out from under the worker,
producing an inconsistent state (sender holding a `attempting`
state for a row that no longer exists in `uploads`). The plan §
2.3.17 H7 mitigation was to make the two INSERTs atomic.

The pre-Phase-1 code carried a defense-in-depth comment:
*"this rollback path is unreachable under WAL-with-NORMAL but kept
in case the pragmas change."* That assumption was load-bearing
but undocumented. ADR-019 records the structural closure that
makes the comment unnecessary.

## Decision

`routes/admission.admit_chain` writes **both** the row and the
idempotency claim in **one atomic SQLite transaction** via
`SqliteUploadStore.insert_with_idempotency_claim(row, claim_value)`.
The store method opens an explicit `BEGIN IMMEDIATE` /
`COMMIT` / `ROLLBACK` block.

### Order inversion

The claim INSERT runs **first** inside the transaction; the row
INSERT follows. If the claim INSERT raises `sqlite3.IntegrityError`
(UNIQUE constraint violation on `idempotency_index.key`), the
transaction is `ROLLBACK`-ed and admission returns the existing
row's `AdmissionOutcome(row, status_code=200)`. No orphan row to
delete; the transaction never committed one.

### Body-store cleanup on collision

In all-disk mode (and in hybrid mode when the body crossed the
size threshold and admission wrote directly to disk), the body
file is written BEFORE the admission transaction opens. On
collision, the transaction rollback does not touch the body file;
the routes/admission code path explicitly calls
`body_store.delete_for_chain(chain_id)` to clean up the orphan
bytes. The BodyOrphanJanitor would catch any miss, but the cleanup
runs synchronously to keep the chain immediately consistent.

### The "defense in depth" comment is removed

With the atomic transaction in place, the pre-Phase-1 orphan-row
cleanup branch becomes unreachable by code shape, not by pragma
contingency. The inline comment is deleted in Phase 1 § 2.3.17;
the no-parallel-schema rule (§ 0.3) forbids preserving the dead
branch "just in case."

### Replay semantics

A successful idempotency-replay returns HTTP **200** (not 202)
with the previously-issued `ChainResponse` body and
`error.code = "idempotency_replay"` (informational; see ADR-017).
The chain row IS the one the prior POST created; no re-admission.

## Consequences

- **Race closed.** No window where a worker can observe an
  uncommitted-claim row.
- **Single INSERT path.** `SqliteUploadStore` exposes ONE method
  for admission writes: `insert_with_idempotency_claim`. No
  separate `insert` + `claim_idempotency` pair. Plan § 0.5
  single-writer manifest records admission's write-purpose as
  "INSERT row + INSERT idempotency claim, in ONE transaction."
- **Falsifiability.** `scripts/check_atomic_admission.py` parses
  `routes/admission.py` via `ast` and asserts the call goes
  through `insert_with_idempotency_claim`; non-atomic alternatives
  fail the check. Runs in the Phase 0 CI per_pr battery.
- **No defense-in-depth comment.** The closure is structural.
- **The H7 audit finding is closed.**

## Cross-references

- `phantom.routes.admission.admit_chain` — the call site.
- `phantom.storage.sqlite_store.insert_with_idempotency_claim` —
  the atomic implementation.
- `scripts/check_atomic_admission.py` — the falsifiability check.
- ADR-017 — idempotency-replay 200 row in the error-code matrix.
- Plan § 0.5 single-writer manifest — admission row.
- Plan § 2.3.17 — the Phase 1 task that landed this.

## Update - 2026-06-12 (R7-4b collision-kind cleanup + R11-1 namespace clear)

Two later findings reshaped the "Body-store cleanup on collision"
paragraph above; the decision (one atomic transaction) is unchanged.

- **Cleanup is collision-kind-keyed (R7-4b), not unconditional.** The
  rollback delete - `BodyStore.delete(chain_id)`, the method's actual
  name - runs ONLY for `IDEMPOTENCY_COLLISION` (the duplicate's body
  sits at its own non-colliding chain_id key). The
  `CHAIN_ID_COLLISION` arm never deletes: the body at the shared key
  belongs to the winning live row. A live-chain_id duplicate is
  rejected by a pre-check BEFORE any body write, so the common retry
  path has nothing to clean up at all.
- **The put is preceded by the R11-1 chain_id namespace clear.** After
  the live-row pre-check and before `body_store.put`, admission
  deletes the chain_id's body namespace so a reused chain_id (legal
  the instant the prior row is removed) never inherits a prior
  occupant's body files. The put-before-transaction ordering this ADR
  records is unchanged; the clear simply joins the pre-transaction
  body stage. See `routes/admission._persist_row_and_claim`.
