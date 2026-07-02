# 032. The `expired` terminal state

Status: Accepted
Date: 2026-06-24

## Context

Phantom's reuse-the-loop design parks an upload that cannot currently be
delivered and waits for the world to change. A `phantom_bearer` row whose
upstream returns 401/403 parks in `auth_expired` and waits for a fresh token;
an `aws_sigv4` row whose signing credentials are missing or rejected parks in
`auth_expired` and waits for an operator credential push (Phase 2). A
forward-as-is row that captured a normal SigV4 request can also retry against
an upstream that will never accept it again (the captured signature has aged
out of the upstream's clock-skew window).

In every one of these cases the row can park **forever**. The only mover of a
parked `auth_expired` row is a kicker sweep, which either wakes the row (the
cred/token slot went fresh) or leaves it parked (the slot is still bad or
absent). A producer that never re-pushes the credential — or a captured
request that is permanently doomed — leaves the row parked indefinitely,
holding its buffer slot and its saturation accounting. There was no backstop
that bounds the buffer/retry window: nothing said "this upload has been trying,
or waiting to try, for too long; give up and free the space."

The two cheaper reuses were both rejected:

- **Reuse `stored`.** Retry-exhaustion already lands in `stored`, which
  deliberately **retains the body for replay** and **deliberately does NOT
  release the saturation slot** ("the body still occupies space until export
  or replay resolves the row"). That is "parked, replayable," the opposite of
  what a deadline give-up wants, which is "dead, stop, free the space."
- **Reuse `auth_expired`.** That state is reactive to an upstream 401/403 and
  is **re-admitted** by the kicker on the next token/credential refresh.
  A deadline give-up is the opposite: it must never be re-admitted.

A new terminal `ChainState` is a hard-to-reverse decision on two axes: it is
part of the **SDK wire contract** (the client `ChainState` and
`TERMINAL_STATES` ship to consumers per ADR-012, and a consumer may `switch`
on the member, so removing it later is a breaking change), and it carries
**retention semantics** (a per-state `RetentionCfg` pair, a reaper sweep row,
and a startup-check pair). That is exactly the kind of decision an ADR records.

## Decision

Add a ninth `ChainState` member, **`expired`**, that is terminal:
dead / do-not-retry / release-the-body / never-re-admitted.

### Semantics

`expired` means the per-route **send-deadline** (`send_deadline_seconds`,
measured from the row's `received_at` ingress timestamp) elapsed. The request
is dead. It is **not retried**, the **body is released** (unlike `stored`), the
**saturation slot is released**, and the row is **never re-admitted** (unlike
`auth_expired`). Therefore `expired` IS a member of the service
`TERMINAL_STATES` frozenset (`phantom.storage.interface`), where `auth_expired`
deliberately is not — that frozenset drives `list_non_terminal`
(`WHERE state NOT IN (TERMINAL_STATES)`), so adding `expired` there is exactly
what makes every kicker sweep skip `expired` rows for free.

`expired` is distinct from both of its neighbours:

| state          | body      | saturation slot | re-admitted? | terminal? |
| -------------- | --------- | --------------- | ------------ | --------- |
| `stored`       | retained  | held            | replay only  | yes       |
| `auth_expired` | retained  | released        | yes (kicker) | no        |
| `expired`      | discarded | released        | never        | yes       |

### The wire-contract obligation (ADR-012)

The `ChainState` literal and the `TERMINAL_STATES` set are duplicated
service↔client (ADR-012). The `get_args` set-equality contract test
(`tests/contract/test_chain_models_alignment.py`) guards alignment, so both
copies of the enum move together; the client `TERMINAL_STATES` gains `expired`
alongside the service one.

### The retention obligation

Every terminal state structurally requires the per-state retention triad:
a `RetentionCfg` `<state>_metadata_seconds` / `<state>_body_seconds` pair, a
reaper retention-table row, and a startup-check retention pair. For `expired`:

- **Body** — the body is **discarded at the transition** (mirrors `succeeded`,
  whose body is also discarded promptly), so by the time the reaper sees an
  `expired` row the body is already gone. `expired_body_seconds` is therefore
  near-vestigial but must still exist for triad completeness; it defaults to
  **0** (nothing to retain). A non-zero value would be dead config.
- **Metadata** — **retained** so an operator can see which uploads gave up
  (the `last_error` carries `send_deadline:Ns`), then reaper-swept. Mirrors
  `failed`, the closest semantic sibling (a terminal give-up worth auditing):
  `expired_metadata_seconds` defaults to **30 days** (`2_592_000`, the same
  default as `failed_metadata_seconds`).

Both the reaper table and the startup-check pair enumerate a per-state
metadata/body retention, but in **opposite field orders** — the reaper tuple
is `(state, metadata, body)`, the startup-check pair is `(state, body,
metadata)`. The `expired` entry honours each site's own order.

### Why this is not `stored` + `last_error`

The cheaper reuse conflates "retry budget exhausted" (`stored`, body retained
for replay, slot held) with "send-window elapsed" (`expired`, body released,
slot released, never re-admitted). Clean terminal semantics were chosen over
the cost of the wide blast radius the new member ripples through (the two enum
copies, the two `TERMINAL_STATES` sets, the retention triad, the shared writer,
and the documentation-as-test guards). This ADR is the record of that
tradeoff. Reversal cost: removing `expired` later is a breaking SDK change.

### The single writer spans two callers (ADR-015 note)

Under ADR-015 the sender owns state transitions and writes each
`new_state="…"` literal inside `workers/sender.py`. `expired` is the one
exception: it is fired from two subsystems — the executor-driven sender
give-up path and the kicker parked-row sweeps — and both must apply identical
body-discard + saturation-release + CAS semantics. So the `new_state="expired"`
write is centralised in ONE shared leaf module, `workers/_expire.py`'s
`expire_row`, that both the sender and the kickers delegate to. This preserves
the ADR-015 one-writer-per-effect discipline (exactly one
`new_state="expired"` call site) while spanning the two callers. The
transition-table documentation-test
(`tests/unit/test_transition_table.py`) is widened to scan `_expire.py` as well
as `sender.py`, so the writer is still forced to exist; `expired` is **not**
added to that test's `_SENDER_EXCLUDES` (it is genuinely sender-owned via the
delegated writer). ADR-015 anticipated this: it foresaw "a separate stuck-row
reaper that promotes rows past a deadline" as the future second actor that
would justify a shared transition path.

### Known v1 `StateBreakdown` undercount (recorded, not a bug)

The `/v1/admin/stats` curated `StateBreakdown` (`phantom.models.admin`) has
explicit per-state tier fields for only `queued`, `attempting`, `auth_expired`,
`stored`, `succeeded_recent`, and `failed_recent`. It deliberately OMITS an
`expired` tier for v1. Its consumer reads by attribute name and skips unknowns
(`getattr(by_state, row.state, None)`), so an `expired` row is silently dropped
from that tiered view. This is an intentional cosmetic undercount, NOT a lost
count: the raw `counts_by_state` histogram (`routes/admin.py`, which
auto-derives from `get_args(ChainState)`) DOES count `expired` rows, so the
per-state total is never lost — only the curated tiered breakdown omits it.
Adding a tiered `expired` bucket (plus its SDK mirror) is a clean,
non-breaking follow-on if operators want it.

## Consequences

- **The reuse-the-loop park is now bounded.** A row parked in `auth_expired`
  awaiting a credential or token re-push that never comes is given up at the
  send-deadline; a forward-as-is row that retries a doomed captured request
  past the window is given up at the send-deadline. Both free the body and the
  saturation slot.
- **`expired` rows never re-admit.** Because `expired` is in `TERMINAL_STATES`,
  every kicker sweep skips it for free, and replay refuses it (the body is
  released, so the replay body-discard precheck rejects it up front); `expired`
  is deliberately left OUT of the replay-eligible `IN`-set.
- **The buffer self-heals under a stuck producer.** An `expired` row's body and
  slot are released immediately; its metadata is reaper-swept after 30 days.
- **Removing `expired` later is a breaking SDK change.** The member ships in the
  client `ChainState` and `TERMINAL_STATES`; a consumer may switch on it.

## Cross-references

- `phantom.models.chain.ChainState` / `phantom_client.models.chain.ChainState`
  — the duplicated enum (both gain `expired`).
- `phantom.storage.interface.TERMINAL_STATES` /
  `phantom_client.models.status.TERMINAL_STATES` — the terminal sets `expired`
  joins (and `auth_expired` deliberately does not, service-side).
- `phantom.workers._expire.expire_row` — the single shared writer of
  `new_state="expired"` (body discard + saturation release + caller-correct
  CAS guard).
- `phantom.config.settings.RetentionCfg` — the `expired_metadata_seconds` (30
  days) / `expired_body_seconds` (0) pair; `phantom.workers.reaper` and
  `phantom.runtime.startup_checks` — the reaper table row and startup-check
  pair (opposite field orders).
- ADR-012 — the duplicated chain/admin schemas and the alignment contract test.
- ADR-015 — state transitions owned by the sender; the one-writer-per-effect
  discipline this ADR preserves while spanning two callers.
- ADR-011 — the capture-TTL re-execution check the executor send-deadline gate
  sits beside.
