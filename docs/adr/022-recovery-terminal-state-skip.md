# 022. Recovery skips terminal rows + the `mark_corrupted` terminal guard

Status: Accepted
Date: 2026-05-29

## Context

The boot-time recovery sweep (`workers/recovery.py`) walks every row and,
for `body_location='file'` rows whose body files are absent, quarantines
the row to `corrupted`. The original walk applied this body-existence
check to rows in **any** state.

A hardening-cycle finding exposed a data-loss bug hidden by the project's
prior test shape. A *delivered* row (`succeeded`) deletes its body on
success. On the next restart, recovery's body-existence walk found the
delivered row's body gone and called `mark_corrupted` — which had no
state guard — flipping `succeeded` → `corrupted` and **destroying the
success record**. The bug was invisible because every prior crash test
restarted only with in-flight rows; a restart with delivered-then-reaped
rows is what surfaced it. The same latent bug existed at a second site:
the periodic `InvariantAuditor` row walk raised spurious
`missing_body` / `body_hash_set_mismatch` signals on every bodyless
`succeeded` row.

The root misconception is that a missing body always means corruption.
For a *terminal* row, a missing body is **expected**: bodies are deleted
on success and during terminal cleanup. Treating that absence as
corruption is wrong, and it overwrites a finished, correct outcome.

## Decision

Recovery's quarantine contract changes in two layers.

### Recovery skips terminal-state rows

The recovery quarantine walk `continue`s past any row whose state is in
`TERMINAL_STATES` (`succeeded`, `failed`, `stored`, `cancelled`,
`corrupted`). A finished row's missing body is expected and must not be
re-examined. `auth_expired` rows are **not** terminal — they are still
deliverable once a fresh token arrives — so they remain subject to the
body-existence check.

### The `mark_corrupted` terminal guard

`SqliteUploadStore.mark_corrupted` gains `AND state NOT IN
(TERMINAL_STATES)` on its UPDATE. A quarantine attempt against a row that
is already terminal becomes a safe no-op (rowcount 0) rather than a
state overwrite. This is defense-in-depth: recovery is the sole caller of
`mark_corrupted`, and the sender's own ADR-014 corruption path uses
`record_attempt_result` (unaffected by this guard), so the guard cannot
mask a legitimate in-flight corruption transition.

### The second site is fixed identically

The `InvariantAuditor._sweep_once` walk gains the same terminal carve-out,
so it no longer raises spurious corruption signals on bodyless terminal
rows.

### What is deliberately NOT done

A `body_discarded_at` stamp is **not** added to the success-delete path.
Overloading the reaper's existing body-retention marker for this purpose
risks side effects; the terminal-state skip is the actual guarantee, and
the existing `body_discarded_at IS NOT NULL` carve-out (invariant #1)
remains scoped to its original H4 purpose.

## Consequences

- **Recovery no longer destroys delivered records.** A `succeeded` row
  with a reaped body survives a restart intact — this changes recovery's
  contract: recovery now corrects only **non-terminal** rows.
- **The bug class is closed at both sites.** Recovery and the invariant
  auditor share the terminal carve-out.
- **`mark_corrupted` is state-guarded.** Quarantine of a terminal row is
  a structural no-op, not a contingent one.
- **A pre-existing counter-test that encoded the bug was corrected.** A
  body-retention contract test had asserted `succeeded` + missing-body →
  `corrupted`; its carrier state was re-pointed to a deliverable state so
  it still guards real corruption without asserting the defect. The test
  was corrected, not weakened.

## Cross-references

- `phantom.workers.recovery.run_recovery` — the terminal-skip in the
  quarantine walk.
- `phantom.storage.sqlite_store.mark_corrupted` — the terminal-guard
  UPDATE.
- `phantom.workers.invariant_audit` — the second site's identical
  carve-out.
- `phantom.storage.interface.TERMINAL_STATES` — the state set both
  layers consult.
- ADR-014 — the dual-body-hash corruption path the sender owns (uses
  `record_attempt_result`, not `mark_corrupted`).
- ADR-021, ADR-023 — recovery's bounded retry and the shared lock
  classifier, which also touch `run_recovery`.
- `docs/architecture-intent.md §5` — invariant #1 and the
  `body_discarded_at` carve-out.
