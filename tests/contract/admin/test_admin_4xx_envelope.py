"""Canonical-envelope contract for the remaining admin 4xx refusals.

ADR-017 states the envelope contract plainly: "Every Phantom error
response (2xx admin replies excepted) carries a JSON body" of the
ADR-010 ``ErrorEnvelope`` shape, and the round-1 defender fix (R1-1)
promoted the last KNOWN escapee (``replay_refused_attempting``) onto
that envelope. Round 2 adversary finding R2-2 found three admin 4xx
sites still answering with FastAPI's raw ``{"detail": ...}`` body via
bare ``HTTPException`` raises in ``phantom.routes.admin``; the round 2
defender fix promoted all three onto typed errors with app-registered
handlers:

* ``GET /v1/admin/chains`` combining ``multifile_id`` with ``cursor``
  (422 ``multifile_cursor_conflict``). SDK-REACHABLE:
  ``PhantomClient.list_uploads`` forwards both parameters, so an SDK
  caller paginating a multifile listing now gets the typed
  ``PhantomUnprocessableError`` instead of ``PhantomEnvelopeError``.
* ``GET /v1/admin/chains`` with a colon-less ``key_value_match``
  (422 ``key_value_match_invalid``). Raw-wire callers only (the SDK
  always encodes the colon).
* ``DELETE /v1/admin/chains`` with an all-None filter body
  (422 ``bulk_delete_filter_empty``). Raw-wire callers only (the SDK
  pre-flights empty filters).

Each test asserts the documented contract: the body decodes as the
canonical ``ErrorEnvelope`` and carries the documented ``error.code``
and ``details`` payload (ADR-017 rows).
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.errors import ErrorEnvelope

# HTTP status the three refusals use; the R2-2 finding was about the
# BODY shape, not the status code.
_UNPROCESSABLE = 422


def test_multifile_cursor_combination_422_rides_the_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """Combining ``multifile_id`` with ``cursor`` must refuse in-envelope.

    The refusal itself is correct and pinned elsewhere
    (``test_list_chains_multifile_filter_rejects_cursor``); this test
    pins the BODY SHAPE the SDK dispatches on.
    """
    app, _ctx = admin_app
    multifile_id = uuid4()
    response = TestClient(app).get(
        f"/v1/admin/chains?multifile_id={multifile_id}&cursor=opaque-cursor"
    )
    assert response.status_code == _UNPROCESSABLE, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.message
    assert envelope.error.code == "multifile_cursor_conflict"
    assert envelope.error.details == {
        "multifile_id": str(multifile_id),
        "cursor": "opaque-cursor",
    }


def test_malformed_key_value_match_422_rides_the_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """A colon-less ``key_value_match`` must refuse in-envelope."""
    app, _ctx = admin_app
    response = TestClient(app).get("/v1/admin/chains?key_value_match=no-colon-here")
    assert response.status_code == _UNPROCESSABLE, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.message
    assert envelope.error.code == "key_value_match_invalid"
    assert envelope.error.details == {"key_value_match": "no-colon-here"}


def test_bulk_delete_empty_filter_422_rides_the_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """An all-None bulk-delete filter must refuse in-envelope."""
    app, _ctx = admin_app
    response = TestClient(app).request("DELETE", "/v1/admin/chains", json={})
    assert response.status_code == _UNPROCESSABLE, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.message
    assert envelope.error.code == "bulk_delete_filter_empty"


# Round 3 adversary hardening: every strict-parse refusal of
# ``_parse_key_value_match`` rides the same canonical envelope WITH its
# named reason in the message. Covers the round 2 wire tightening
# ('k:' / ':v' / ':' now refuse where they previously matched silently
# or queried a meaningless pair) and the quoted-key form's strictness
# legs (undefined escape, missing closing quote, missing post-quote
# colon, empty quoted key), none of which were pinned anywhere.
# (raw wire string, fragment of the named reason the message carries)
_KV_MATCH_REFUSALS: tuple[tuple[str, str], ...] = (
    ("k:", "the value must be non-empty"),
    (":v", "the key must be non-empty"),
    (":", "the key must be non-empty"),
    ('"":v', "the key must be non-empty"),
    ('"k":', "the value must be non-empty"),
    ('"k"v', "expected ':' immediately after the quoted key"),
    ('"k', "missing its closing quote"),
    ('"', "missing its closing quote"),
    ('"k\\x":v', "bad escape in the quoted key"),
    ('"k\\', "bad escape in the quoted key"),
)


@pytest.mark.parametrize(("raw", "reason_fragment"), _KV_MATCH_REFUSALS)
def test_every_key_value_match_strict_parse_refusal_rides_the_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
    raw: str,
    reason_fragment: str,
) -> None:
    """Each strict-parse refusal answers the canonical 422 envelope.

    Pins the round 2 defender's single-reading ruling end to end on the
    contract app: empty key or value refuses under BOTH wire forms (the
    deliberate tightening Defender 2 logged for round 3), and every
    malformed quoted-key form refuses with its named reason in the
    message, the documented ``details`` payload, and the
    ``key_value_match_invalid`` code the SDK dispatches on. A regression
    that silently matched (the pre-tightening behavior) or escaped the
    envelope (the R2-2 class) fails loudly here.
    """
    app, _ctx = admin_app
    response = TestClient(app).get("/v1/admin/chains", params={"key_value_match": raw})
    assert response.status_code == _UNPROCESSABLE, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "key_value_match_invalid"
    assert reason_fragment in envelope.error.message
    assert envelope.error.details == {"key_value_match": raw}
