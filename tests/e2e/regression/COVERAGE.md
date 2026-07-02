# Regression coverage matrix (plan § 6.2 / peer-review § C-2)

Every Critical + High audit ID must have AT LEAST ONE regression
test in this directory or a documented path to an equivalent
regression elsewhere in the suite. The Phase 8 § 9.2.7 phantom-
revert sweep reads this matrix as the authoritative coverage
inventory.

| ID | Severity | Closure | Regression test path |
|---|---|---|---|
| C1 | Critical | Orphan body GC (Phase 2 § 3.2.1) | `src/phantom-service/tests/unit/test_admin_bulk_delete_c1.py` (Phase 2 unit). E2E body-lost-then-cleaned coverage falls out of the body-orphan janitor unit tests in `src/phantom-service/tests/unit/test_body_orphan_janitor.py`. |
| C2 | Critical | Saturation try/finally (Phase 2 § 3.2.2 / H1) | Release-on-failure tests in `src/phantom-service/tests/unit/test_admission.py` (saturation_refused, idempotency collision, codec failure, body-store failure, namespace-clear failure) + `test_auth_kicker_noop_releases_admitted_slot.py` + `test_auth_kicker_releases_on_store_exception.py` (same directory). |
| H1 | High | Saturation try/finally (Phase 2 § 3.2.2) | Same as C2: H1 and C2 collapse into one fix; the C2 row's tests are the regression. |
| H2 | High | Content-Length precheck (Phase 2 § 3.2.3) | Content-length precheck tests in `src/phantom-service/tests/unit/test_send_route.py` (Phase 2). |
| H3 | High | TOCTOU race; closed structurally + H9 regression | `tests/e2e/regression/test_h9_h3_toctou.py` (H9 is the regression for H3 closure; see plan § 0.2 line 88). |
| H4 | High | Body-retention contract + recovery carve-out (Phase 2 § 3.2.4) | `src/phantom-service/tests/unit/test_h4_body_retention_contract.py` + `src/phantom-service/tests/unit/test_recovery.py::test_recovery_h4_carveout_discarded_body_not_quarantined` (Phase 2). |
| H6 | High | AdMinter into composition-root TaskGroup (Phase 2 § 3.2.5) | `src/phantom-service/tests/unit/test_h6_ad_minter_supervised.py` (Phase 2). The H10(b) test below covers the cancellation contract end-to-end. |
| H7 | High | Atomic admission (Phase 1 § 2.3.17) | Phase 1 Slice 1.F unit tests + `tests/e2e/crash_recovery/test_crash_admission_atomic.py` (Phase 5) + `scripts/check_atomic_admission.py` (static check, Phase 5 § 6.2.8). |
| H8 | High | BodyMissingError → corrupted (Phase 2 § 3.2.6) | `src/phantom-service/tests/unit/test_h8_body_missing_corrupted.py` (Phase 2) + `tests/e2e/test_files_lost_midway.py::test_manual_body_file_lost_post_persist` (Phase 5) + `tests/e2e/regression/test_h10_silent_route_and_supervision.py::test_h10a_missing_body_raises_body_missing_error` (Phase 5). |
| H9 | High | TOCTOU regression for H3 (Phase 5 § 6.2.9) | `tests/e2e/regression/test_h9_h3_toctou.py` (two tests: cancel-during-attempt + replay-during-attempt). |
| H10 | High | Runtime silent-route + TaskGroup-cancellation regression (Phase 5 § 6.2.9) | `tests/e2e/regression/test_h10_silent_route_and_supervision.py` (two tests: H10(a) silent-route closure + H10(b) TaskGroup-cancellation). |
| H13/D3 | High | architecture-intent supervision text (Phase 2 § 3.2.7) | Documentation update: no test surface; covered by the falsifiability-descriptions CI job which lints docstring/description drift. |
| D1 | Med-High | Re-submitting a live `chain_id` under a fresh idempotency key returns a structured 409 `chain_id_in_use` (ADR-017), never a naked 500 (Defender R2: typed `InsertClaimOutcome`, `CHAIN_ID_COLLISION` arm). | `tests/e2e/regression/test_d1_duplicate_chain_id.py`. |
| E1 | Low | Duplicate multipart `body_refs[<name>]` parts are rejected with a structured 422 `body_ref_duplicate` instead of a silent last-wins overwrite (Defender R2 parser guard). | `tests/e2e/regression/test_e1_duplicate_multipart_body_ref.py`; parser-level unit coverage in `src/phantom-service/tests/unit/test_parser.py`. |
| G1 | Low-Med | Reusing an `X-Phantom-Idempotency-Key` with a DIFFERENT body is rejected 422 `idempotency_key_conflict` via the body-hash divergence check; an identical body still replays at 200 (Defender R2). | `tests/e2e/regression/test_g1_idempotency_key_reuse_different_body.py`. |
| V1 | High | Recovery cursor-drain over a SIGKILL-hot WAL (Defender R6): `run_recovery` collects quarantine targets, drains the `iter_rows` cursor, THEN writes; store also checkpoint-truncates a hot WAL at `start()`. | `tests/e2e/crash_recovery/test_crash_sigkill_recovery_no_database_locked.py` (real `os.kill(SIGKILL)`, `[hybrid]` param = RAM-lost quarantine path). |
| V2 | High | Same fix as V1: the recovery lock hit every mode; the all_disk file-missing quarantine path goes through the same `mark_corrupted` write. | Same file, `[all_disk]` param (file-missing quarantine path). |
| V5 | Low-Med | Retry-cadence pins (aggressor Round 5, adopted exploratory): the retry budget reaches a terminal state, the backoff interval is held (no hammering at the poll rate), and the retry schedule (`next_attempt_at` + attempts) survives SIGKILL. The V5-C jitter fix is asserted at unit level in `src/phantom-service/tests/unit/test_strategies.py`. | `tests/e2e/regression/test_v5_retry_cadence_and_crash_survival.py` (subprocess harness). |
| R7-4b | High | A duplicate submit of an in-flight chain_id no longer destroys the original (Defender R8): admission pre-checks the chain_id PK BEFORE `body_store.put`, so a duplicate of a live chain_id is rejected 409 without touching the original's body; the CHAIN_ID_COLLISION arm no longer deletes the shared body. | `tests/e2e/regression/test_r74b_duplicate_chainid_preserves_inflight.py` (original survives + delivers `succeeded`). |
| R7-5-A | Holds (no defect) | Ambiguous outcome: the upstream stored the body but the ack back to Phantom was reset/truncated. Delivery stays exactly-once via at-least-once retry + dedup; no double-delivery on the terminal upload step. Adopted exploratory pin (Defender R8); the property held on current code. | `tests/e2e/regression/test_r75a_ambiguous_outcome_exactly_once.py`. |
| R7-5-B | Medium | A 2xx response missing a required capture no longer wedges the chain + leaks a slot (Defender R8): the executor validates declared-and-downstream-referenced captures before advancing; a missing one is a retryable `CaptureIncomplete`. | `tests/e2e/regression/test_r75_2xx_missing_capture_no_wedge.py` (chain reaches a clean terminal). |
| R7-1-D / R7-2-B | Medium-High | A SQLITE_IOERR / SQLITE_FULL commit failure no longer leaks an open transaction / wedges the shared connection (Defender R8): `_write_txn` rolls back on any failure across every writer + the token cache. | `src/phantom-service/tests/unit/test_r71_r72_storage_fault_failclosed.py` (open-txn parametrized: ioerr + full). |
| R7-1-A/B / R7-2-A | Low-Med | A storage-layer OSError (fsync EIO / ENOSPC) during admission body buffering no longer escapes as a naked 500 (Defender R8): mapped to the new ADR-017 `storage_unavailable` 503. | `src/phantom-service/tests/unit/test_admission.py::test_admit_chain_releases_slot_on_body_store_failure` (typed code + slot released) + the `storage_unavailable` contract tests. |
| R9-V6-2 | High | An external DB lock held past `busy_timeout` during recovery boot no longer crashes startup / wedges the service (Defender R9): `run_recovery` rides out a transient lock via a bounded retry-with-backoff on `is_transient_lock_error` (recovery is idempotent; R7-3); a persistent lock past the budget surfaces a clean `RecoveryLockError`, never a raw traceback. | `tests/e2e/crash_recovery/test_r9_v6_recovery_boot_survives_external_lock.py` (seeded data_dir + external lock held 45 s during a fresh boot → service still becomes healthy + all seeded rows survive). |
| R9-V6-1 | Medium | An external DB lock held past `busy_timeout` during admission no longer surfaces as a naked 5xx / `PhantomTimeoutError` (Defender R9): admission maps the transient-lock `OperationalError` to the ADR-017 `storage_unavailable` 503, and `SQLITE_BUSY_TIMEOUT_MS` lowered 5000→1000 so a contended writer fails fast (a clean retryable) instead of monopolizing the single `_write_lock` slot and timing out the client. | `tests/e2e/regression/test_r9_v6_external_lock_admission_clean_503.py` (8-deep admission burst under a held external lock → every response is a success or a clean 503; service recovers after release). |

## How Phase 8 deliberate-violation tests use this matrix

Phase 8 § 9.2.4 will revert each fix and assert the regression
test trips. The matrix tells the Phase 8 author which test file
to expect a green→red transition in. If a fix is reverted and
the listed test still passes, the regression test is insufficient
and must be hardened.

## Adding new entries

When a new Critical or High audit ID lands, add a row above with:

- The ID + severity classification.
- The closure phase + task ID.
- The path to the regression test (must exist; "to be added" is
  not acceptable; author the test first).
