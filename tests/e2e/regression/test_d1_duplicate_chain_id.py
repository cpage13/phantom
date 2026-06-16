"""Regression test for aggressor finding D-1 (adopted Round 2).

Asserts that re-submitting the SAME envelope ``chain_id`` under a
FRESH ``X-Phantom-Idempotency-Key`` returns a structured ADR-017
ErrorEnvelope, never a naked FastAPI 500. Round 1 found the
``uploads.chain_id`` PRIMARY KEY collision escaped admission as a bare
``Internal Server Error`` 500.

Defender Round 2 fix: ``insert_with_idempotency_claim`` returns a typed
``InsertClaimOutcome``; admission maps the ``CHAIN_ID_COLLISION`` arm to
a deterministic 409 ``chain_id_in_use`` (registered in ADR-017,
``phantom.models.errors``, and ``phantom_client.errors`` as
``PhantomConflictError``). The no-idempotency-key plain-insert path is
guarded the same way via ``is_chain_id_collision``.
"""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import E2EStack


@pytest.mark.asyncio
async def test_duplicate_envelope_chain_id_returns_structured_4xx(
    stack: E2EStack,
) -> None:
    """A second submit with the same envelope.chain_id returns ADR-017 4xx."""
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    request = build_create_file_request(file_name="d1-regression")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    envelope_json = envelope.model_dump_json()

    body_bytes = b"dup-chain-id-test"
    headers_a = {
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Uid": "00000000-0000-0000-0000-000000000001",
        "X-Phantom-Idempotency-Key": "idem-A",
    }
    headers_b = {**headers_a, "X-Phantom-Idempotency-Key": "idem-B"}

    async with httpx.AsyncClient(timeout=10.0) as http:
        files_1 = {
            "envelope": ("envelope.json", envelope_json, "application/json"),
            "body_refs[body]": ("body", body_bytes, "application/octet-stream"),
        }
        first = await http.post(f"{stack.phantom_url}/v1/send", headers=headers_a, files=files_1)
        assert first.status_code == 202, first.text

        files_2 = {
            "envelope": ("envelope.json", envelope_json, "application/json"),
            "body_refs[body]": ("body", body_bytes, "application/octet-stream"),
        }
        second = await http.post(f"{stack.phantom_url}/v1/send", headers=headers_b, files=files_2)

    # Acceptance criteria.
    assert second.status_code != 500, (
        f"second submit should be a deterministic 4xx, got 500 with body "
        f"{second.text!r}. The collision must surface via ADR-017's "
        f"ErrorEnvelope shape with a meaningful error.code."
    )
    assert second.status_code in (409, 422), (
        f"second submit returned {second.status_code}; expected 409 or 422 "
        f"with a duplicate_chain_id-class error code."
    )
    # Body must be JSON of the ADR-017 ErrorEnvelope shape.
    try:
        envelope_body = second.json()
    except json.JSONDecodeError as exc:
        raise AssertionError(f"second submit response is not JSON: {second.text!r}") from exc
    assert "error" in envelope_body, (
        f"response body lacks 'error' key (not ADR-017 shape): {envelope_body!r}"
    )
    err = envelope_body["error"]
    assert err.get("code"), f"response error.code is empty (not ADR-017 shape): {err!r}"
    # The code must NOT be "internal_error" — that's the catch-all.
    assert err["code"] != "internal_error", (
        "response code is 'internal_error' — the bug is unfixed. The "
        "duplicate-chain situation should be a specific, named code."
    )
