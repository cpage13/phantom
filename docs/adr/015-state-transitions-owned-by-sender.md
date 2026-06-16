# 015. State transitions owned by sender (Path A)

Upload-row state transitions are owned by `workers.sender.Sender`'s `_on_*` handlers (`_on_succeeded`, `_on_failed_5xx`, `_on_failed_auth`, `_on_failed_transport`, `_on_corrupted`, `_on_stored`, `_on_cancelled`). The executor returns a discriminated-union result type (`Succeeded`, `Failed5xx`, `FailedAuth`, `FailedTransport`, `CaptureExpired`, etc.). The sender's `isinstance` dispatch on that union ARE the canonical state-transition table; mypy enforces exhaustiveness.

There is no separate state-machine module. `phantom/state/` does not exist. `InvalidTransition` is not a runtime concept.

The path-not-taken (Path B) was to keep a pure transition function (`state.transition(current_state, event) -> new_state`) that workers call. The sender would feed the executor's result into the transition function and apply the returned state. This is the conventional design and the one earlier drafts of the code carried.

Path A was chosen over Path B because of the codebase's specific shape:

1. **The seven states are stable.** `queued`, `attempting`, `succeeded`, `failed`, `stored`, `cancelled`, `auth_expired`, `corrupted` — these were settled before any code landed and have not shifted. A transition function earns its keep when the state machine is in flux; when it's stable, the function is a pass-through.
2. **Five non-overlapping callers each own their slice of the table.** Sender owns the `attempting → {succeeded, queued, failed, stored, auth_expired, corrupted}` transitions. Auth-kicker owns `auth_expired → queued`. Admin cancel owns `* → cancelled`. Recovery owns `attempting → queued` and the corruption-quarantine path to `corrupted`. Reaper owns the deletion of terminal-state rows (which is a removal, not a transition). No two callers share a transition; there is no "wrong caller raised the transition" failure mode for a function to catch.
3. **mypy-checked exhaustive dispatch is stronger than runtime InvalidTransition raises.** The discriminated union forces the sender's `match`/`isinstance` cascade to handle every variant or fail typecheck. A runtime `InvalidTransition` raise catches the same class of bug at runtime, after the row has been claimed. For this codebase's shape — mypy strict mandatory, every PR — the type system catches the bug earlier.
4. **The deletion test.** A pure transition function whose body is a 5-arm dispatch with no shared logic between arms is a pass-through. Inlining the dispatch at each caller eliminates the indirection without losing any property the function provided.

If a future change introduces a third actor that needs to drive state — e.g., a separate "stuck-row reaper" that promotes rows past a deadline regardless of sender activity — the transition table can be reintroduced with the actual second caller in hand. The current codebase is a one-true-caller-per-arm shape, and the function would be premature abstraction.

The acceptance test is `tests/unit/test_transition_table.py`, which scans `workers/sender.py` for every `new_state=` keyword literal in the `_on_*` handlers and asserts the set is a subset of `ChainState`. A second test asserts the excluded states (`attempting`, `cancelled`) ARE written somewhere in the codebase (`storage.claim_due` and the admin cancel handler respectively), so the exclusion is intentional and not silent dead code.

Status: Accepted
Date: 2026-05-14
