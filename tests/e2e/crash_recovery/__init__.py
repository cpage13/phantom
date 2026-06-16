"""Crash-recovery E2E suite (plan § 6.2.2 / WS-4 crash-window matrix).

Each test in this directory exercises ONE position from the WS-4
crash-window matrix:

1. Drive a partial sequence of the persist-controller / admission /
   recovery code paths up to the labelled checkpoint.
2. Halt at the checkpoint, simulating a process crash.
3. Reboot the same on-disk layout (fresh SqliteUploadStore, fresh
   body-store instances against the same data_dir).
4. Run :func:`phantom.workers.recovery.run_recovery` (the canonical
   startup recovery sweep).
5. Assert the post-recovery state matches what the WS-4 matrix
   predicts for that crash position.

The tests use storage-component-level integration (not full uvicorn
spawn) because the in-process E2E harness cannot SIGKILL the test
runner without killing pytest itself; the wire surface is irrelevant
to what the WS-4 matrix tests — its concern is the on-disk
representation between persist-controller steps. The persist
controller, recovery sweep, and storage components are exercised
end-to-end against real on-disk SQLite + body files.

WS-4 crash-window positions (per plan § 6.2.2):

* ``test_crash_between_get_all_and_file_put`` — Step 1 done, Step 2
  not yet started; row at ``body_location='ram'`` + RAM lost on
  restart → recovery quarantines as ``corrupted``.
* ``test_crash_between_file_put_and_mark_persisted`` — Step 2 done,
  Step 3 not yet committed; row at ``body_location='ram'`` + body
  files on disk → recovery quarantines (RAM lost) and orphan body
  files are the janitor's responsibility (verified independently).
* ``test_crash_between_mark_persisted_and_ram_delete`` — Step 3
  committed, Step 4 not yet run; row at ``body_location='file'`` +
  body files on disk → recovery preserves the row at ``queued`` (the
  Phase 1 commit-last-column ordering invariant).
* ``test_crash_during_admission_atomic_transaction`` — admission's
  atomic INSERT pair rolled back; neither the upload row nor the
  idempotency claim exists post-restart.
* ``test_crash_during_recovery_sweep_itself`` — recovery is
  idempotent; running it a second time after a partial first pass
  reaches the same end state.
"""
