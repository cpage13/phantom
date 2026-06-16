#!/usr/bin/env bash
# Pre-commit hook: forbid bare numeric literals in production code
# (informational — Phase 0 emits findings but exits success).
#
# Best-effort regex. Phase 6 documents the named-constant convention and
# may convert this to a hard gate. Per plan note 1.1.6.B, this hook exits
# success either way — output is informational.

set -euo pipefail

hits=$(grep -rEn '^[[:space:]]+[a-zA-Z_][a-zA-Z0-9_]*\s*[+\-*/]?=\s*[0-9]+(\.[0-9]+)?([eE][+\-]?[0-9]+)?\s*$' \
  --include='*.py' src/*/src/ 2>/dev/null \
  | grep -vE '(test_|conftest|^[^:]+:\s*[A-Z_]+\s*=)' \
  | head -30 || true)

if [ -n "$hits" ]; then
  echo "Informational — bare numeric literals (review for named-constant opportunities):"
  echo "$hits"
fi
exit 0
