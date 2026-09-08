"""Every admin error response carries the canonical envelope, without exception.

Objective: pin that an unhandled exception on the admin surface is a typed
``internal_error`` envelope rather than a bare 500 with a non-JSON body.

THE DEFECT THIS CLOSES. ADR-017 states that every error response carries an
``ErrorEnvelope`` and documents a 500 ``internal_error`` row, but only the
enumerated spec types and ``RequestValidationError`` were registered as
handlers. Anything else reached starlette's default and returned
``500 Internal Server Error`` with a plain-text body.

That is not a cosmetic gap. The SDK decodes admin failures through
``EXCEPTION_FOR_CODE``, so a bare 500 did not surface as a typed exception at
all: the caller got a decode failure instead, precisely when it most needed to
know what had gone wrong.

The escapes were reachable, not theoretical: a malformed pagination cursor on
the listing route, a body read on a chain whose retention had already discarded
its bytes, a bulk delete filtered only by instance, and a body-store failure
during an idempotency-collision rollback.

Registering the fallback makes the envelope contract structurally true rather
than maintained by hand. It guarantees the SHAPE; several of those sites still
deserve a better ANSWER than 500, which is tracked separately.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.models.errors import STATUS_FOR_CODE
from phantom.routes.admin import register_admin_error_handlers, router


@pytest.fixture
def client() -> TestClient:
    """An app carrying the admin router and its registered handlers."""
    app = FastAPI()
    app.include_router(router, prefix="/v1/admin")
    register_admin_error_handlers(app)

    @app.get("/v1/admin/_boom")
    async def _boom() -> None:
        """Raise the shape an unguarded store call raises."""
        raise KeyError("no body refs for chain_id=...")

    # raise_server_exceptions=False makes the client return the handler's
    # response instead of re-raising, which is what a real HTTP caller sees.
    return TestClient(app, raise_server_exceptions=False)


def test_an_unhandled_exception_returns_the_canonical_envelope(
    client: TestClient,
) -> None:
    """Objective: the response is JSON in the documented envelope shape.

    Expected: 500 with an ``error`` object carrying ``internal_error``. Before
    the fallback this was a plain-text body the SDK's decoder could not parse,
    so the caller saw a decode failure rather than a typed error.
    """
    response = client.get("/v1/admin/_boom")

    assert response.status_code == STATUS_FOR_CODE["internal_error"] == 500
    body = response.json()
    assert "error" in body, "the response is not in the canonical envelope shape"
    assert body["error"]["code"] == "internal_error"


def test_the_envelope_names_the_exception_class_but_not_its_message(
    client: TestClient,
) -> None:
    """Objective: enough to triage, nothing that can leak.

    Expected: the class name is present and the exception's own text is not. An
    unhandled error's message is uncontrolled and routinely carries a path, a
    query string or a credential, so it must not be echoed to the caller. The
    full traceback goes to the log instead.
    """
    response = client.get("/v1/admin/_boom")

    body = response.json()
    assert body["error"]["details"]["exception_class"] == "KeyError"
    rendered = response.text
    assert "no body refs" not in rendered, (
        "the exception's own message was echoed to the caller; an unhandled "
        "error's text is uncontrolled and can carry a path or a credential"
    )


@pytest.mark.parametrize(
    ("label", "exc"),
    [
        ("base64-decode", ValueError("Invalid base64-encoded string")),
        ("json-decode", ValueError("Expecting value: line 1 column 1")),
        ("missing-field", ValueError("cursor payload missing required field")),
        ("absent-body", KeyError("No body refs for chain_id=...")),
        ("empty-filter", ValueError("Bulk delete requires at least one filter field")),
        ("storage-io", OSError("Input/output error")),
    ],
)
def test_every_reachable_escape_class_returns_the_envelope(label: str, exc: Exception) -> None:
    """Objective: the classes that actually escaped are all covered.

    Expected: each returns the canonical envelope rather than a plain-text
    body. These are the exception types the review found reaching starlette's
    default from real admin routes: three ways a malformed pagination cursor
    can fail to decode, the absent-body read on a chain whose retention already
    discarded its bytes, the instance-only bulk delete hitting the store's
    empty-filter guard, and a body-store failure during an
    idempotency-collision rollback.

    Parametrising by exception CLASS rather than by route keeps this test about
    the handler's totality, which is what the fallback guarantees. Whether each
    site deserves a better answer than 500 is a separate question, tracked
    separately.
    """
    app = FastAPI()
    register_admin_error_handlers(app)

    @app.get("/raise")
    async def _raise() -> None:
        raise exc

    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/raise")

    assert response.status_code == 500, f"{label} did not reach the fallback"
    assert response.headers["content-type"].startswith("application/json"), (
        f"{label} produced a non-JSON body the SDK cannot decode"
    )
    body = response.json()
    assert body["error"]["code"] == "internal_error", f"{label} escaped the envelope"
    assert body["error"]["details"]["exception_class"] == type(exc).__name__
