# 031. Config-distribution architecture: live-read is canonical

## Context

Hot reload (ADR-013) turns every operational knob into a small
distribution problem with four obligations: the new value must reach
its consumer; validation must complete before any swap; the snapshot
registry must cover every live instance; and the failure contract must
cover every failure. Five defects across the phase-7.3 iteration loop
came from meeting these obligations per-knob, ad hoc: R5-2 (codec and
retry values swapped into the snapshot but consumers held boot-time
copies), R6-2 (the RAM ceiling captured at watcher construction), R7-1
(swap before validation half-applied a probe-reliant YAML and silently
disabled RAM-ceiling enforcement), R8-1 (a topology-shrinking reload
evicted a live instance's snapshot and crashed the process), R8-2
(file-read failures outside the handled set). A sixth instance, the
orphan janitor's construction-pinned cadence contradicting its own
documented contract, was found by inspection during the 2026-06-11
architecture meta-analysis.

## Decision

1. **Live-read is the canonical mechanism.** A consumer reads the live
   `InstanceSettingsSnapshot` (via `InstanceContext.current_settings()`)
   at every point of use. A worker MUST NOT cache a snapshot-derived
   value across ticks or loop iterations; a constructor MUST NOT take a
   config value a snapshot can carry.
2. **Exceptions are enumerated and justified, never implied.** Two
   exist: the saturation-gate cap push (`update_caps`), because
   `admit()` is the synchronous ingress hot path and must not pay a
   snapshot dereference and recompute per request; and the
   retry-strategy rebuild, because strategy construction is allocation,
   not a read. A new exception requires amending this ADR.
3. **Restart-required is an explicit, enumerated set** (worker count,
   instance topology, AD-mint, `body_store.mode` and storage paths,
   the `db_integrity` block, and the SQLite vacuum/pragma schedule),
   not a default for whatever was inconvenient. Topology drift at
   reload is warn-and-keep-running: the omitted instance's previous
   snapshot is carried forward so its per-use reads keep working
   (R8-1).
4. **The contract is ONE table**, in ADR-013: knob, consumer and read
   point, mechanism, pinning test. Docstrings point at the table; they
   do not restate it.
5. **The table is enforced by the knob-matrix contract test**
   (`src/phantom-service/tests/unit/test_reload_knob_matrix.py`): for
   every reloadable knob it performs a real `apply_reload` with a
   changed value and asserts the consumer-visible truth (the live
   snapshot for live-read knobs; the pushed or rebuilt artifact for the
   exceptions). Enforcement scope (clarified 2026-06-12): the mirror
   test compares the case set against a hand-maintained literal set in
   the test module, not against this ADR's markdown - a knob added to
   the matrix without the mirror set (or removed) fails the test; a
   knob added ONLY to the ADR text must be brought to both by review.

## Consequences

The config-distribution defect class moves from adversary-priced
discovery to CI-priced regression. Reload-time work stays minimal (two
pushes plus the cfg repoint); everything else is read-side and
therefore correct under the atomic snapshot swap by construction.
Validation and resolution complete inside `Settings.reload_from_yaml`
before any swap (R7-1), and every failure in the shared
`RELOAD_FAILURE_ERRORS` set rejects-and-keeps-previous on both trigger
paths (R8-2).

Status: Accepted
Date: 2026-06-11
Amended: 2026-06-12 (round-10 doc pass) - decision 3's restart set now
names the `db_integrity` block and the SQLite vacuum/pragma schedule
(C3, with matching ADR-013 rows); decision 5 states the mirror test's
real enforcement scope (C7). No decision is reversed.
