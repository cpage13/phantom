"""all_ram storage-mode E2E suite (aggressor R9-PM).

``all_ram`` keeps every body in RAM ONLY -- never persisted to disk. It is the
operator's speed-over-durability tradeoff, and it has two properties worth
pinning that hybrid/all_disk do not exercise the same way:

* Crash recovery: a SIGKILL loses every un-delivered body BY DESIGN, but the
  loss must be CLEAN -- the service comes back healthy, RAM-lost rows are
  terminal-quarantined (never left "deliverable" pointing at a vanished body),
  and no truncated/garbage body is ever forwarded upstream.
* RAM ceiling: all_ram has NO disk fallback, so the only admitted-byte bound is
  the saturation ``max_in_flight_bytes`` gate. Filling RAM to the bound must
  refuse cleanly (503 saturation_cap), never OOM / grow unbounded / crash, and
  must recover once pressure clears.

These are GREEN regression PINS (aggressor R9, per-mode set: scenarios PM1
crash-recovery and PM2 RAM-ceiling) -- the highest-value all_ram coverage gaps.
"""
