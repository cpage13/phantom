# 036. Gate-owned slot settlement

Status: Accepted
Date: 2026-08-18

## Context

The saturation ledger was mutated from **twenty-six hand-written call sites
across eight modules** (twenty-three across seven after the two kicker modules
merged). Each one answered the same question in its own words:

> Did this row's `row_holds_slot(state, body_discarded_at)` value CHANGE, and
> if so in which direction?

That question has one input the caller does not own: the row's state and stamp
**as the write saw them**. Every defect in this area was a caller answering it
from a snapshot the write had already invalidated, and six prior review
findings each fixed one site's copy of the answer: R8-4 (removal accounting
captured atomically with the DELETE), R8-6 (replay must re-admit a released
row), R9-3 (the kicker's rowcount-0 leg must return its admit), R9-4 (replay's
re-admit decision must use in-transaction state), R9-5 (the reaper's
confirm-then-act discard), R10-2 (the kicker's exception leg must return its
admit). Six fixes, six sites, one rule that lived nowhere.

The sites were also doing **two structurally different things**, which a single
transition-shaped rule cannot cover:

- **Transitions.** A write moves a row across the predicate, and the ledger
  follows.
- **Speculative reservations.** A charge is taken BEFORE a write, because the
  write must be refusable, and it comes back if the write does not happen or
  turns out not to have been needed. There is no write, no outcome and no
  crossing on those legs, so a `landed`-gated rule does nothing there and leaks
  on every one of them.

## Decision

**The saturation ledger is settled by the gate from store outcomes, and a
caller may not compute a slot transition.**

**Two layers, both public.** The PRIMITIVE layer (`admit`, `release`,
`reconcile_admit`, `set_disk_usage_bytes`, `update_caps`, the properties) owns
the ledger arithmetic: the caps, the byte total, the R9-6 large-class
charge-time pairing, the gauge. The SETTLEMENT layer (`settle`, `unwind`) owns
the two decisions. The primitives are not a compatibility shim: they are a
separately tested surface driven directly by the gate's own unit tests, which
is where the cap arithmetic, the zero-cap semantics (A-1), the large-class
pairing and the hot-reload interaction are exercised without inventing a row
transition to carry each case.

**The crossing rule has exactly one application site.** `SlotDelta._crossing`
applies `row_holds_slot` to both sides of one write. Five adapter classmethods
feed it, one per store outcome type: `from_attempt`, `from_discard`,
`from_removal`, `from_replay`, `from_cancel`. A caller never constructs a
`SlotDelta` directly; if callers could build deltas by hand, the twenty-three
derivations would survive under a new type and nothing would be discharged.

**No adapter may take a predicate input from a field the store computed
outside the write's own transaction.** After-states come from the write's own
literal; before-states and before-stamps come from the write's own
in-transaction pre-image. `replay` and `cancel` both read their returned row
AFTER the transaction commits, so `outcome.row` is used for the release BASIS
and for the caller's return value, and never as a predicate input.

**The store outcomes carry the pre-image.** `record_attempt_result` returns
`AttemptWriteOutcome` instead of a bare `int`, keeping `rowcount` as a field so
its documented "0 or 1" contract is unchanged and adding `landed` as a derived
property (two fields carrying one truth can disagree; a property cannot).
`DiscardOutcome` gains `previous_state` and `discarded_at`, both from data the
store already held. `CancelOutcome` gains `previous_body_discarded_at`, one
extra column on a SELECT that already ran inside the transaction.

**Reservations are first-class.** `SaturationGate.admit` mints a
`SlotReservation` carried on `AdmissionGranted`, and mints it nowhere else. A
reservation is consumed either by the CREATION of the row it was taken for, or
by a write whose crossing is a charge and which is handed the reservation
through `settle(..., consumes=)`. Everything else is an `unwind`. The rule is
total over three crossings times two reservation states, and the arm that no
site reaches today (a holder that reserved AND whose write dropped the row out
of the in-flight set) is implemented anyway, so the rule is a rule rather than
a case analysis over current callers.

**The release basis is a caller input, deliberately.** Every adapter requires a
`size_bytes` keyword and none derives it from the outcome. The bases genuinely
diverge on the tree: the four sender sites release `row.body_size_bytes`, a
caller-held snapshot, while `workers/_expire.py` releases an in-transaction
size, and `tests/unit/test_reaper_body_discard_stale_snapshot.py` documents the
harm the difference once caused. Unifying them is a real behaviour change with
its own witness and its own ledger assertions, and it is out of scope. This
paragraph is where a future reader finds out that the divergence is **known
rather than accidental**, and the required keyword is what keeps the
unification from arriving as a tidy.

**Boot reconstruction is a SEED, not a transition,** and keeps its own verb.
`reconcile_saturation` walks the recovered rows and charges for rows that
already exist: no write, no CAS, no outcome, no crossing. The ROW's status does
not change; the LEDGER's knowledge of it does. Expressing that as a landed
transition would be a fabrication about the row, so `reconcile_admit` survives
with exactly one caller. Its second former meaning, replay's R9-4 repair, IS a
real transition that merely needs to bypass the caps, and it moved to
`settle`'s charge arm. Splitting those two meanings is part of the deepening:
one verb serving a seed and a transition is why the replay reconcile was
written as a hand-rolled four-arm `if`.

**Two `row_holds_slot` consultations survive outside the gate,** and neither is
an exception. Boot reconstruction's and replay's pre-check both ask a
CURRENT-STATE question about a row the caller is not transitioning, which is
the one question the predicate stays public for.

## The one pre-authorised behaviour delta

`workers/_expire.py` released its slot only when its body DISCARD flipped. The
discard's guard is `state = 'expired' AND body_discarded_at IS NULL`, so it
misses for exactly two reasons: the row was already stamped (only the reaper's
body pass can stamp an `expired` row, and `expired_body_seconds` defaults to 0,
so it is live on the same tick), or the row was removed (admin `delete_upload`,
admin `bulk_delete_uploads`, the reaper's metadata pass,
`evict_terminal_over_limit`). On all five actors **nobody released**: each
remover decides on `row_holds_slot("expired", ...)`, which is False. The slot
leaked for the process lifetime.

The crossing is the CAS-guarded STATE write, so exactly one writer lands it and
exactly one release follows whoever wins the discard. The release basis moves
with it, from the discard outcome's size to the state write's, and that is part
of the same delta rather than a second one: **whenever the discard FLIPS the two
sizes are provably equal**, because `discard_body_and_zero_accounting` is the
only writer of `body_size_bytes` after admission and its guard proves it had not
yet run when the state write read its pre-image. So the amount changes on no
interleaving where the old code released at all. Choosing the old basis would
return the row's COUNT and strand its BYTES.

`tests/unit/test_expire_releases_when_the_discard_did_not_flip.py` is the
witness. **Q31's residual stands and this ADR does not claim otherwise:** the
structural claims prove the `flipped` gate is gone and the witness proves the
release fires and returns both counters, but the witness substitutes the
race's OUTCOME rather than racing, so neither proves the ledger under every
real interleaving.

A second, smaller difference travels with `from_cancel`, which takes its
before-stamp from the write's own transaction where the route previously read
it off a row fetched after the commit. The two forms differ on one arm only, a
`stored` row whose stamp is NULL at transaction time and SET before the
post-commit read, where the old code declined to release and nothing else
released either. Its only reachable opener needs `cancelled_body_seconds` set
to 0 against a default of 604800, so on default configuration the window cannot
open. It is a HALF closure: the count returns and the bytes stay stranded,
because the racing discard zeroed `body_size_bytes` in the same UPDATE and
cancel's basis is frozen at `outcome.row.body_size_bytes`.

## Consequences

- The twenty-three derivations become nineteen call sites: three `admit`
  (unchanged), one `reconcile_admit` (boot only), ten `settle`, five `unwind`,
  and zero hand-written `release` calls outside the gate.
- Replay's four-arm hand-rolled reconcile collapses to one `settle` call, and
  its reservation leak closes as a consequence of the rule rather than as a
  special case.
- The sender's eight attempt-write sites share one private
  `_settle_transition`. The WRITE stays at each call site, because
  `tests/unit/test_transition_table.py` scans the sender's AST for
  `new_state=` literals; the helper folds the DECISION, not the write.
- `record_attempt_result` costs one extra primary-key SELECT inside the
  transaction that already holds the write lock. This is the pattern `replay`,
  `cancel` and `bulk_delete` already use.
- **Reversal cost.** The primitives stay public, so reverting the settlement
  layer is deleting two methods and restoring the call sites. What is
  hard to reverse is the CONTRACT: the rule above is what stops the next site
  from being written the old way, which is the whole reason six findings were
  needed to fix six copies of one decision.

## Cross-references

- ADR-032 (`expired`), whose writer this changes.
- ADR-015 (state transitions owned by the sender), which the sender's single
  transition writer sharpens.
- ADR-014 (the corrupted path), whose outcomes travel the same way.
- `docs/architecture-intent.md` invariant 16.
