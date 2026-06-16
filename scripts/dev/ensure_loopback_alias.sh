#!/usr/bin/env bash
# Ensure the second loopback address used by the multi-instance e2e tests
# is bindable on this machine.
#
# Why: the no-header hostname-dispatch e2e
# (tests/e2e/test_e2e_multi_instance_hostname_dispatch.py) binds two
# emulators to two DISTINCT loopback IPs. Linux binds all of 127.0.0.0/8
# natively, so this script is a no-op there. macOS only exposes 127.0.0.1
# on lo0 until an alias is added, so on darwin this script adds the alias
# (an operation that needs sudo, typically once per boot).
#
# Idempotent: safe to run repeatedly; exits 0 immediately when the address
# is already bindable. Only darwin is ever modified.
#
# Usage: bash scripts/dev/ensure_loopback_alias.sh

set -euo pipefail

# The one extra loopback address the multi-instance e2e suite needs
# (tests/e2e/test_e2e_multi_instance_hostname_dispatch.py STAGING_HOST).
SECOND_LOOPBACK_ADDR="127.0.0.2"

# Bindability probe, not an ifconfig parse: what the tests actually need is
# a successful bind(2), so probe exactly that.
can_bind() {
    python3 - "$SECOND_LOOPBACK_ADDR" <<'PY'
import socket
import sys

addr = sys.argv[1]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind((addr, 0))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

if can_bind; then
    echo "ensure_loopback_alias: ${SECOND_LOOPBACK_ADDR} is already bindable; nothing to do."
    exit 0
fi

if [ "$(uname -s)" != "Darwin" ]; then
    # Linux (and anything else) binds 127.0.0.0/8 natively; if the probe
    # failed here something unrelated is wrong and aliasing lo0 is not the
    # fix, so do not touch the system.
    echo "ensure_loopback_alias: ${SECOND_LOOPBACK_ADDR} not bindable and this is not darwin;" >&2
    echo "ensure_loopback_alias: refusing to modify a non-darwin host (no-op)." >&2
    exit 1
fi

echo "ensure_loopback_alias: adding lo0 alias ${SECOND_LOOPBACK_ADDR} (sudo may prompt)..."
sudo /sbin/ifconfig lo0 alias "$SECOND_LOOPBACK_ADDR" up

if can_bind; then
    echo "ensure_loopback_alias: ${SECOND_LOOPBACK_ADDR} is now bindable."
    echo "ensure_loopback_alias: note: the alias does not survive a reboot; rerun after rebooting."
    exit 0
fi

echo "ensure_loopback_alias: alias command ran but ${SECOND_LOOPBACK_ADDR} is still not bindable." >&2
exit 1
