"""Ingress connection-reset / mid-body-abort E2E suite (aggressor R9-CR).

Each test here drives the client->phantom direction: a client opens a raw
socket, sends a well-formed request line + headers with a large
``Content-Length``, transmits only a fraction of the declared body, then drops
the connection while Phantom is still RECEIVING (in ``request.stream()``).

These are GREEN regression PINS, not break reproductions. They lock the
"leak-proof-by-construction" ingress ordering invariant: Phantom buffers the
WHOLE body into RAM (``_parse_body``) BEFORE it ever calls ``gate.admit`` or
``body_store.put``. A mid-body abort therefore lands BEFORE any saturation slot
is claimed or any body file is staged, so it can leak neither. A future refactor
that claims the slot first (e.g. streaming admission that reserves capacity at
header time) would make an aborted upload leak a slot and turn these pins RED —
which is exactly why they exist.

Aggressor R9, vector CR (connection reset): scenarios CR1 (single mid-body
abort) and CR3 (concurrent abort burst).
"""
