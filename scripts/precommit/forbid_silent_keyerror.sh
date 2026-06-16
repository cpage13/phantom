#!/usr/bin/env bash
# Pre-commit hook: forbid silent KeyError suppression in storage routes.
#
# H8 closure (Phase 2): BodyMissingError routes to corrupted. Phase 0 form
# is broad (storage/ subtree); Phase 2 tightens to the specific paths once
# the BodyMissingError shape lands.

set -euo pipefail

hits=$(grep -rEn 'except KeyError:\s*$' --include='*.py' src/phantom-service/src/phantom/storage/ || true)

if [ -n "$hits" ]; then
  echo "Silent KeyError suppression in storage route (H8 closure):"
  echo "$hits"
  exit 1
fi
exit 0
