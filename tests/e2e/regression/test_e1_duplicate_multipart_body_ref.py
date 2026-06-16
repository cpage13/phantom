"""Regression test for aggressor finding E-1 (adopted Round 2).

Asserts the multipart parser rejects requests carrying two parts
named ``body_refs[<same-name>]`` with a structured 4xx ADR-017 error
envelope. Round 1 found the parser silently overwrote the first part's
bytes with the second's (last-wins) with no signal to the producer.

Defender Round 2 fix: ``chain/parser.py`` guards ``ref_name in
body_refs`` before assignment and raises ``ParserError(
"body_ref_duplicate", ...)``. The ``body_ref_duplicate`` code (422) is
registered in ADR-017, ``phantom.models.errors`` (ErrorCode +
STATUS_FOR_CODE), and ``phantom_client.errors.EXCEPTION_FOR_CODE``
(PhantomValidationError). Parser-level unit coverage lives in
``src/phantom-service/tests/unit/test_parser.py``.
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
async def test_duplicate_body_ref_part_returns_structured_4xx(
    stack: E2EStack,
) -> None:
    """Two multipart parts with the same body_refs[name] is rejected.

    Construct a raw multipart with two ``body_refs[body]`` parts and
    submit. Assert the response is a 4xx with the ADR-017 ErrorEnvelope
    shape and a recognizable error code.
    """
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    request = build_create_file_request(file_name="e1-regression")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    envelope_json = envelope.model_dump_json()

    boundary = "----PhantomRegressionE1Boundary"
    crlf = b"\r\n"
    parts = [
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="envelope"',
        b"Content-Type: application/json",
        b"",
        envelope_json.encode(),
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="body_refs[body]"; filename="b1"',
        b"Content-Type: application/octet-stream",
        b"",
    ]
    body = crlf.join(parts) + crlf + b"first-body" + crlf
    parts2 = [
        f"--{boundary}".encode(),
        b'Content-Disposition: form-data; name="body_refs[body]"; filename="b2"',
        b"Content-Type: application/octet-stream",
        b"",
    ]
    body += crlf.join(parts2) + crlf + b"second-body" + crlf
    body += f"--{boundary}--\r\n".encode()

    headers = {
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Uid": "00000000-0000-0000-0000-000000000001",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.post(
            f"{stack.phantom_url}/v1/send",
            headers=headers,
            content=body,
        )

    # Acceptance: a 4xx with the ADR-017 ErrorEnvelope shape and a
    # body_ref-class error.code (the exact code is the fix author's
    # choice — body_ref_duplicate is the most expressive).
    assert 400 <= response.status_code < 500, (
        f"expected 4xx for duplicate body_refs[body]; got {response.status_code} "
        f"with body {response.text!r}"
    )
    try:
        body_json = response.json()
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"duplicate body_refs[body] response is not ADR-017 ErrorEnvelope "
            f"JSON: {response.text!r}"
        ) from exc
    assert "error" in body_json, body_json
    code = body_json["error"].get("code", "")
    assert "body_ref" in code or "duplicate" in code, (
        f"response error.code {code!r} doesn't describe the duplicate-body-ref "
        "situation; expected something like 'body_ref_duplicate'."
    )
