"""Cycle-7 admin routes must ride the canonical envelope on malformed input (R6-4).

ADR-017 states the contract without exception: "Every Phantom error
response (2xx admin replies excepted) carries a JSON body" of the
ADR-010 ``ErrorEnvelope`` shape. ``test_admin_4xx_envelope.py``'s own
docstring asserts the round-1 defender "promoted the last KNOWN escapee
(``replay_refused_attempting``) onto that envelope" and the round-2
defender closed the three bare-``HTTPException`` sites - implying the
admin surface is now envelope-clean.

It is not. The escapee CLASS the prior rounds missed is FastAPI's own
request-validation 422: a malformed path/query value never reaches a
handler, so none of the typed admin error handlers fire, and FastAPI
emits its default ``{"detail": [...]}`` body - NOT an ``ErrorEnvelope``.
The cycle-7 admin routes added this cycle inherit it:

* ``GET /v1/admin/groups/{group_id}`` (group rollup) on a non-UUID id.
* ``GET /v1/admin/uploads/by-local-uuid/{local_uuid}`` on a non-UUID id.
* ``POST /v1/admin/quarantine/restore?backup_id=<non-uuid>`` and the
  same route with ``backup_id`` omitted (a required query param).

These are raw-wire-only escapes (the typed SDK coerces ``group_id`` /
``local_uuid`` / ``backup_id`` to ``UUID`` before sending, so it cannot
produce one), which puts them in exactly the severity class the round-2
defender nonetheless fixed for ``key_value_match_invalid`` /
``bulk_delete_filter_empty`` - raw-wire callers (curl, a non-SDK
operator script) still get a body their tooling cannot dispatch on,
breaking the ADR-017 promise the contract suite claims to uphold.

Why it matters: an operator or downstream tool that dispatches on
``error.code`` (the documented contract) gets ``{"detail": ...}`` with no
``error`` object on these cycle-7 routes, so it falls through its
error-handling and surfaces a generic failure instead of the precise
"you sent a malformed identifier" signal the envelope carries everywhere
else. The fix is one shared ``RequestValidationError`` handler registered
in ``register_admin_error_handlers`` that maps FastAPI's validation
failure onto the canonical 422 ``ErrorEnvelope`` (which also closes the
identical escape on the phase-1 UUID routes, whose mechanism predates
cycle-7).

The R6-4 fix landed exactly that handler, emitting the new
``request_invalid`` code (ADR-017 row added in lockstep); each case now
asserts the envelope decodes and carries that code.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.errors import ErrorEnvelope
from phantom.routes import admin as admin_routes

# FastAPI's request-validation status. The escape is about the BODY
# SHAPE (raw detail vs canonical envelope), not the status code; the
# canonical envelope rides the same 422.
_UNPROCESSABLE = 422

# A value that fails UUID coercion on every UUID-typed path/query param.
_NOT_A_UUID = "not-a-uuid"

# (method, url) pairs over the cycle-7 admin routes added this cycle.
# Before R6-4 every one answered FastAPI's raw {"detail": ...} on
# malformed input instead of the canonical ErrorEnvelope.
_CYCLE7_MALFORMED_PROBES: tuple[tuple[str, str], ...] = (
    ("GET", f"/v1/admin/groups/{_NOT_A_UUID}"),
    ("GET", f"/v1/admin/uploads/by-local-uuid/{_NOT_A_UUID}"),
    ("POST", f"/v1/admin/quarantine/restore?backup_id={_NOT_A_UUID}"),
    ("POST", "/v1/admin/quarantine/restore"),
)


@pytest.mark.parametrize(("method", "url"), _CYCLE7_MALFORMED_PROBES)
def test_cycle7_route_malformed_input_rides_the_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
    tmp_path: Path,
    method: str,
    url: str,
) -> None:
    """A malformed identifier on a cycle-7 admin route must answer in-envelope.

    Pins the ADR-017 contract: the error body decodes as an
    ``ErrorEnvelope`` carrying the dispatchable ``request_invalid``
    code. Before R6-4 each route answered FastAPI's raw
    ``{"detail": [...]}`` (no ``error`` object); the shared
    ``RequestValidationError`` handler in
    ``register_admin_error_handlers`` closes the whole class.
    """
    app, _ctx = admin_app
    # The quarantine routes resolve the data root via a dependency the
    # minimal admin_app fixture does not override; point it at tmp_path so
    # these probes exercise request validation, not a missing dependency.
    app.dependency_overrides[admin_routes.get_data_root] = lambda: tmp_path

    response = TestClient(app, raise_server_exceptions=False).request(method, url)

    assert response.status_code == _UNPROCESSABLE, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "request_invalid", (
        "the error body must be a canonical ErrorEnvelope carrying the "
        f"request_invalid code per ADR-017 (R6-4); got {response.text!r}"
    )
    assert envelope.error.message
