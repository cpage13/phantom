# 027. Typed BootOutcome for instance boot

Status: Accepted
Date: 2026-06-10

## Context

Per-instance boot (integrity gate, backup reconcile, mode guard, schema
gate, store open) used to report a degraded instance through a
side-channel: the builder returned `None` and wrote a reason string into
a `degraded_instances` dict that routes read back out. Side-channel
state carrying control decisions between functions was a prior-cycle
failure mode: the next unhandled fault site shows up as a runtime
surprise, and nothing forces a new failure class to be handled at all.

## Decision

Instance boot returns a typed union:
`BootOutcome = InstanceContext | DegradedInstance` (PEP 695 alias in
`app.py`). `DegradedInstance` is a frozen dataclass (instance id, a
`DegradeReason` enum member, fault detail) in
`runtime/startup_checks.py`; each member of `DegradeReason` maps to an
actual fault site in the boot ladder. The lifespan folds outcomes with
an exhaustive `match` closed by `assert_never`, and
`degrade_action_hint` gives every reason an operator action string
through a second exhaustive match. The `degraded_instances` dict is
deleted; the typed `app.state.degraded_boot` list replaces it, and the
readiness/liveness probes (`/v1/readyz`, `/v1/healthz` - public since
R12-1; originally `/v1/admin/ready`, `/v1/admin/health`) and the POST
/v1/send guard surface the instance id, reason, fault, and action hint
from it.

Adding a `DegradeReason` member without handling it is now a mypy
strict failure at the `assert_never` sites, not a runtime crash loop.
Degrade remains terminal until restart.

Two faults deliberately do NOT degrade: the integrity fail-closed abort
(`db_integrity.fail_open=false` stays a process abort, the operator
hatch ADR-025 preserves) and an unclassified error over a
probe-confirmed writable substrate (a real unexplained fault must crash
loudly, not hide in a degrade).

## Consequences

- The compiler, not production, finds the next unhandled boot fault.
- Degrade reporting is structured: admin surfaces carry typed reason
  values and action hints instead of ad hoc strings.
- The boot loop's ownership is unchanged; only its return type changed.

## Cross-references

- `phantom.runtime.startup_checks` - `DegradeReason`,
  `DegradedInstance`, `degrade_action_hint`.
- `phantom.app` - the `BootOutcome` alias and the exhaustive fold in
  the lifespan.
- ADR-015 - the precedent: typed discriminated unions with
  mypy-enforced exhaustive dispatch instead of runtime checks.
- ADR-024 - the lifespan composition root that owns the fold.
- ADR-025 - the preserved fail-closed abort hatch.
