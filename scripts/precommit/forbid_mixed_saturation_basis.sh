#!/usr/bin/env bash
# Pre-commit hook: forbid passing a raw encoded-bytes blob to
# saturation.admit/release (best-effort).
#
# The gate accounts a byte COUNT, not a bytes object, and the admit and
# release sides must use the SAME byte basis. Finding R3-8 settled that
# basis as the STORED (encoded/buffered) size — i.e. the row's
# ``body_size_bytes`` (InvariantAuditor invariant #2): admission encodes
# first and admits ``stored_body_size``; the sender and the auth-kicker
# admit/release the same ``body_size_bytes``. This grep catches the
# length-vs-bytes confusion (a variable literally named ``encoded`` passed
# where a count belongs); the unit-symmetry itself is verified by
# ``test_r38_in_flight_bytes_no_leak_under_compression`` + invariant #2.

set -euo pipefail

hits=$(grep -rEn 'saturation\.(admit|release)\(.*encoded' --include='*.py' src/ || true)

if [ -n "$hits" ]; then
  echo "Saturation accounting receiving a raw encoded-bytes blob (must be a byte COUNT):"
  echo "$hits"
  exit 1
fi
exit 0
