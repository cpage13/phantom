"""Contract tests for the /v1/admin/tokens-family endpoints.

Covers:

- ``GET /v1/admin/tokens`` — list cached slots (no bearer values).
- ``PUT /v1/admin/tokens/{endpoint}/{uid}`` — push one bearer.
- ``DELETE /v1/admin/tokens/{endpoint}/{uid}`` - invalidate one slot
  (mark it ``bad`` and preserve it, per ADR-003).
- ``DELETE /v1/admin/tokens`` - invalidate every slot (mark all ``bad``).

The push-endpoint and push-global variants exercise the same fan-out
loop as push-one + list_slots; this module asserts the contract
shape for the core three operations + the global delete.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext


async def test_list_tokens_empty_returns_empty(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /tokens`` on a fresh cache returns ``{"tokens": []}``."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/tokens")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "tokens" in body
    assert body["tokens"] == []


async def test_put_token_one_then_list_returns_slot(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """PUT one bearer; subsequent GET /tokens shows the slot (no bearer leaked)."""
    app, _ctx = admin_app
    client = TestClient(app)
    endpoint = "files.example.com"
    uid = "test-uid"
    response = client.put(
        f"/v1/admin/tokens/{endpoint}/{uid}",
        json={"token": "Bearer test-bearer-do-not-leak"},
    )
    assert response.status_code == 204, response.text

    list_response = client.get("/v1/admin/tokens")
    assert list_response.status_code == 200
    body = list_response.json()
    assert len(body["tokens"]) == 1
    slot = body["tokens"][0]
    assert slot["endpoint"] == endpoint
    assert slot["uid"] == uid
    # The bearer value MUST NOT appear in the list response (ADR-004).
    slot_str = str(slot)
    assert "test-bearer-do-not-leak" not in slot_str


async def test_delete_token_one_marks_slot_bad(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """DELETE one slot marks it ``bad`` and PRESERVES it (ADR-003 / R-EX3)."""
    app, _ctx = admin_app
    client = TestClient(app)
    endpoint = "files.example.com"
    uid = "test-uid"
    client.put(
        f"/v1/admin/tokens/{endpoint}/{uid}",
        json={"token": "Bearer x"},
    )
    response = client.delete(f"/v1/admin/tokens/{endpoint}/{uid}")
    assert response.status_code == 204

    list_response = client.get("/v1/admin/tokens")
    assert list_response.status_code == 200
    slots = list_response.json()["tokens"]
    # ADR-003: the slot is not deleted - it persists with status='bad'.
    assert len(slots) == 1, f"slot vanished after invalidate; ADR-003 preserves it: {slots}"
    assert slots[0]["endpoint"] == endpoint
    assert slots[0]["uid"] == uid
    assert slots[0]["status"] == "bad"


async def test_delete_all_tokens_marks_every_slot_bad(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """DELETE /tokens (no path args) marks every slot ``bad`` and preserves them."""
    app, _ctx = admin_app
    client = TestClient(app)
    # Seed two slots.
    client.put("/v1/admin/tokens/ep1.example.com/uid-a", json={"token": "Bearer a"})
    client.put("/v1/admin/tokens/ep2.example.com/uid-b", json={"token": "Bearer b"})

    response = client.delete("/v1/admin/tokens")
    assert response.status_code == 204

    list_response = client.get("/v1/admin/tokens")
    slots = list_response.json()["tokens"]
    # ADR-003: both slots persist, each flipped to status='bad'.
    assert len(slots) == 2, f"slots vanished after invalidate-all; ADR-003 preserves them: {slots}"
    assert {s["status"] for s in slots} == {"bad"}


async def test_put_token_missing_body_returns_422(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """PUT a token without a body is rejected with 422 (Pydantic validation)."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.put("/v1/admin/tokens/ep/uid")
    assert response.status_code == 422
