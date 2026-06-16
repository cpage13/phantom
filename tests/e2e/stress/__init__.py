"""Stress E2E suite (plan § 6.2.3).

Tests in this directory are marked ``@pytest.mark.stress`` and run
only in the nightly-stress workflow (Phase 0 § 1.1.10). Per-PR CI
excludes them via the pyproject ``addopts`` filter.

Subsumes the deleted ``test_e2e_21_burst_1k.py`` (replaced by
:func:`test_burst_1k.test_burst_1k_succeeds`) and the deleted-flaky
``test_e2e_22_burst_1k_flaky.py`` (the flakiness was a naked-sleep
issue that the Phase 5 de-flake pass cured; the load coverage moves
here).

Coverage matrix (plan § 6.2.3):

* burst — concurrent POST of 1000 small bodies; every chain reaches
  ``succeeded`` end-to-end + idempotency holds for replays.
* ram_pressure — admit bodies up to the configured ceiling; verify
  the RAM-pressure watcher signals + the persist controller migrates
  + admission unblocks within a bounded time.
* idempotency_race — concurrent claims under the same idempotency
  key race; exactly one wins; the losers see the H7 collision
  response without leaking partial state.
"""
