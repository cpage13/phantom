#!/usr/bin/env bash
# Pre-commit hook: forbid `await` inside `async with X_lock:` block (best-effort).
#
# Static analysis is imperfect; the integration test in Phase 3 closes residual
# gaps. 50-line window per plan note 1.1.6.A.

set -euo pipefail

hits=$(grep -rEn 'async with [a-zA-Z_]+_lock:' --include='*.py' -A 50 src/ \
  | grep -E '^[^:]+[:-]\s*await ' \
  || true)

if [ -n "$hits" ]; then
  echo "Possible await inside async with X_lock (review for false positives):"
  echo "$hits"
  exit 1
fi
exit 0
