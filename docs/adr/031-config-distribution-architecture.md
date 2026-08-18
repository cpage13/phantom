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
   the `db_integrity` block, the SQLite vacuum/pragma schedule, and the
   per-instance route block: `routes`, `host_prefixes` and the
   per-instance `data_dir`), not a default for whatever was
   inconvenient. Topology drift at reload is warn-and-keep-running: the
   omitted instance's previous snapshot is carried forward so its
   per-use reads keep working (R8-1). The route block is the same
   posture made mechanical (D1/F5): the boot `InstanceCfg` is frozen and
   is the ONE object every route reader resolves, so a reloaded block
   cannot take effect and the reload warns naming the drifted fields.
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
   The matrix now covers BOTH halves of the table (2026-08-14): the
   reloadable rows assert that the consumer-visible value MOVED, and
   the restart-required D1 rows assert that it did NOT, each guarded
   against a vacuous mutation. The two halves carry separate mirror
   sets. What the matrix deliberately does not assert is the
   restart-required WARNING itself: the matrix is the distribution
   contract, the warning is operator-facing behaviour, and keeping them
   apart means a change to the log wording breaks one focused test
   rather than the contract suite.

## Consequences

The config-distribution defect class moves from adversary-priced
discovery to CI-priced regression. Reload-time work stays minimal: the
two enumerated pushes, plus two warn-only arms that install nothing.
The `cfg` repoint used to be a third mechanism here, named in this
sentence and in ADR-013's opening paragraph but absent from decision 2's
enumeration; F5 resolved that contradiction by DELETING the mechanism
rather than documenting it, which is one fewer way for config to reach a
consumer. Everything else is read-side and therefore correct under the
atomic snapshot swap by construction.
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
Amended: 2026-08-14 (review 08-12 remediation, Phase 3) - Consequences
no longer counts the `cfg` repoint as a third mechanism, because F5
deleted it; decision 3's restart set names the per-instance route block
(`routes`, `host_prefixes`, `data_dir`) and its freeze-and-warn
enforcement; decision 5 records that the matrix now covers the
restart-required half with its own mirror set, and what it deliberately
leaves to a focused test. Decision 2's "Two exist" needed no edit and
is now true everywhere rather than only in that sentence. No decision is
reversed.
