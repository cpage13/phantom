#!/usr/bin/env bash
# Pre-commit hook: forbid naked asyncio.sleep in tests/e2e/.
#
# Phase 5 deflakes the 22 existing offenders; until then, the
# `# pre-commit-allow: sleep` annotation is the documented escape hatch.

set -euo pipefail

hits=$(grep -rEn 'asyncio\.sleep\(' tests/e2e/ \
  | grep -v '# pre-commit-allow: sleep' \
  || true)

if [ -n "$hits" ]; then
  echo "Naked asyncio.sleep() in tests/e2e/ (use event-based wait or annotate # pre-commit-allow: sleep):"
  echo "$hits"
  exit 1
fi
exit 0
