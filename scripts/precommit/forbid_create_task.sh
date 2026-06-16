#!/usr/bin/env bash
# Pre-commit hook: forbid UNSUPERVISED task spawning outside the
# composition root.
#
# Invariant (architecture-intent.md invariant #15 / § 3.6): every
# long-lived coroutine is spawned under an `asyncio.TaskGroup`, so an
# unhandled worker exception cascades out of the group and crashes the
# process visibly (the orchestrator restarts; persisted rows survive).
# The ONE composition root is `phantom.app.create_app`'s lifespan.
#
# What counts as a violation (the UNSUPERVISED spawn primitives — they
# schedule a coroutine on the running loop with NO supervising
# TaskGroup, so a silent exception leaves the runtime believing the
# worker is healthy):
#
#   - `asyncio.create_task(`
#   - `asyncio.ensure_future(`
#   - `loop.create_task(`  /  `get_running_loop().create_task(`
#     /  `get_event_loop().create_task(`
#
# What is NOT a violation (deliberately): `<taskgroup>.create_task(`
# (e.g. `tg.create_task(...)`). A TaskGroup-bound spawn IS supervised by
# definition — membership in a `async with asyncio.TaskGroup()` block
# guarantees exceptions propagate as an ExceptionGroup. The sender's
# per-instance worker pool (`workers/sender.py` — `tg.create_task(...)`
# inside its own TaskGroup, itself spawned by the lifespan) is a
# legitimate NESTED structured-concurrency pattern, not an escape.
#
# Allowlist (the only sanctioned unsupervised-primitive sites):
#   - src/phantom-service/src/phantom/runtime/reload.py — the SIGHUP handler's
#     single `loop.create_task(_sighup_reload(...))`. add_signal_handler's
#     callback runs synchronously on the loop thread and cannot await,
#     so it MUST schedule the reload as a loop task; the handler keeps a
#     strong reference + a done-callback so it is not GC'd or silently
#     dropped (reload.py `make_sighup_handler`). The lifespan
#     (app.py `create_app`) installs this handler — it remains the one
#     composition root; the reload engine is just its cohesive module.
#   - src/phantom-emulator/src/phantom_emulator/server.py — the
#     emulator's internal test-only server task spawn. Permanent.
#   - All tests/ paths (and src/<pkg>/tests/*).
#
# Acceptance (R-4): a planted `loop.create_task(...)` (or
# `asyncio.create_task(` / `asyncio.ensure_future(`) in any production
# module OUTSIDE this allowlist makes the hook exit non-zero; removing
# the plant makes it pass.

set -euo pipefail

# The unsupervised-spawn patterns. `tg.create_task(`/TaskGroup methods
# are intentionally NOT matched — only loop-level / module-level spawns.
PATTERN='asyncio\.create_task\(|asyncio\.ensure_future\(|(^|[^.[:alnum:]_])loop\.create_task\(|get_running_loop\(\)\.create_task\(|get_event_loop\(\)\.create_task\('

# Allowlisted sites (sanctioned unsupervised-primitive call sites or
# non-production trees). Matched against the `path:lineno:` grep prefix.
ALLOWLIST='(/tests/|phantom-emulator/src/phantom_emulator/server\.py:|src/phantom-service/src/phantom/runtime/reload\.py:[0-9]+:[[:space:]]*task = loop\.create_task\(_sighup_reload)'

hits=$(grep -rEn "$PATTERN" --include='*.py' src/ \
  | grep -v -E "$ALLOWLIST" \
  || true)

if [ -n "$hits" ]; then
  echo "Unsupervised task spawn outside the composition root (invariant #15):"
  echo "$hits"
  echo
  echo "Spawn long-lived coroutines via the lifespan's asyncio.TaskGroup"
  echo "(tg.create_task(...)) so an unhandled exception crashes the process"
  echo "visibly. See docs/architecture-intent.md § 3.6 / invariant #15."
  exit 1
fi
exit 0
