"""The grouping-header matrix over the wire: valid, absent, malformed.

Cycle-7 plan 06_09 task 7.1(f). The route-level matrix is pinned at the
unit tier (``test_send_grouping_headers.py``); this module drives the
same matrix through a REAL ``POST /v1/send`` over HTTP with raw
multipart requests (no SDK header builder, so a malformed value can
actually reach the wire) and asserts:

* valid values for ``X-Phantom-Group-Id`` / ``X-Phantom-Multifile-Id``
  / ``X-Phantom-Order`` (together and each alone) persist onto the row
  and the 202 always echoes the effective ``X-Phantom-Group-Id``;
* absent headers admit byte-identically to the defaults: ``group_id``
  = chain_id (echoed), ``multifile_id`` null, ``send_order`` 0;
* malformed values 400 with the canonical ``header_invalid`` envelope
  naming the offending header and value, admit NOTHING (the chain id
  stays unused), and the same chain id resubmits cleanly afterward.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from ._driver import build_in_memory_upload_envelope
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack

pytestmark = pytest.mark.e2e

# The three optional grouping headers under test (request side).
HEADER_GROUP_ID: str = "X-Phantom-Group-Id"
HEADER_MULTIFILE_ID: str = "X-Phantom-Multifile-Id"
HEADER_ORDER: str = "X-Phantom-Order"

# The 202 echo header (always present; group_id is NOT NULL on the row).
HEADER_GROUP_ECHO: str = "X-Phantom-Group-Id"

# Body bytes for every probe submission (content irrelevant here).
PROBE_BODY: bytes = b"ingress-header-matrix-probe-body"

# HTTP budget for one loopback request (generous deadlock backstop).
REQUEST_TIMEOUT_SECONDS: float = 10.0

_UID: str = "00000000-0000-0000-0000-000000000001"


@dataclass(frozen=True)
class _ValidCase:
    """One valid-or-absent matrix leg.

    ``None`` means the header is ABSENT from the request; the expected
    row values then follow the admission defaults.
    """

    name: str
    group_id: UUID | None
    multifile_id: UUID | None
    order: int | None


@dataclass(frozen=True)
class _MalformedCase:
    """One malformed matrix leg: ``value`` rides under ``header``."""

    name: str
    header: str
    value: str


def _matrix_headers(
    *,
    bearer: str,
    chain_id: UUID,
    extra: dict[str, str],
) -> dict[str, str]:
    """Assemble the raw request headers for one matrix submission."""
    return {
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Uid": _UID,
        "X-Phantom-Idempotency-Key": str(chain_id),
        **extra,
    }


def _multipart_for(envelope_json: str) -> list[tuple[str, tuple[str, bytes, str]]]:
    """Build the documented multipart shape for one submission.

    Mirrors the SDK transport: an ``envelope`` part carrying the JSON
    envelope plus one ``body_refs[body]`` part with the body bytes.
    """
    return [
        ("envelope", ("envelope.json", envelope_json.encode("utf-8"), "application/json")),
        ("body_refs[body]", ("body", PROBE_BODY, "application/octet-stream")),
    ]


def _fresh_envelope_json(stack: E2EStack, *, file_name: str) -> tuple[UUID, str]:
    """Mint one submission's chain id + serialized envelope."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=file_name)
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    return chain_id, envelope.model_dump_json(by_alias=True)


async def test_valid_and_absent_header_combinations_persist_and_echo(stack: E2EStack) -> None:
    """Valid values persist onto the row; absent values admit the defaults."""
    shared_group = uuid4()
    shared_multifile = uuid4()
    cases: tuple[_ValidCase, ...] = (
        _ValidCase("all_three", shared_group, shared_multifile, 2),
        _ValidCase("group_only", uuid4(), None, None),
        _ValidCase("multifile_only", None, uuid4(), None),
        _ValidCase("order_only", None, None, 7),
        _ValidCase("all_absent", None, None, None),
    )
    bearer = stack.fake_security_token()
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as raw:
        for case in cases:
            chain_id, envelope_json = _fresh_envelope_json(
                stack, file_name=f"matrix-{case.name}.bin"
            )
            extra: dict[str, str] = {}
            if case.group_id is not None:
                extra[HEADER_GROUP_ID] = str(case.group_id)
            if case.multifile_id is not None:
                extra[HEADER_MULTIFILE_ID] = str(case.multifile_id)
            if case.order is not None:
                extra[HEADER_ORDER] = str(case.order)

            response = await raw.post(
                f"{stack.phantom_url}/v1/send",
                headers=_matrix_headers(bearer=bearer, chain_id=chain_id, extra=extra),
                files=_multipart_for(envelope_json),
            )
            assert response.status_code == httpx.codes.ACCEPTED, (
                f"case {case.name}: expected 202, got {response.status_code}: {response.text}"
            )

            # The echo is ALWAYS present: the supplied group id, else the
            # chain_id default.
            expected_group = case.group_id if case.group_id is not None else chain_id
            assert response.headers.get(HEADER_GROUP_ECHO) == str(expected_group), (
                f"case {case.name}: the 202 must echo the effective group id"
            )

            # The row carries the persisted values / defaults.
            detail = await stack.phantom_client.get_upload(chain_id)
            assert detail.group_id == expected_group, f"case {case.name}: group_id"
            assert detail.multifile_id == case.multifile_id, f"case {case.name}: multifile_id"
            expected_order = case.order if case.order is not None else 0
            assert detail.send_order == expected_order, f"case {case.name}: send_order"


async def test_malformed_header_values_400_and_admit_nothing(stack: E2EStack) -> None:
    """Each malformed value 400s with the header_invalid envelope; no row lands."""
    cases: tuple[_MalformedCase, ...] = (
        _MalformedCase("group_not_uuid", HEADER_GROUP_ID, "not-a-uuid"),
        _MalformedCase("multifile_not_uuid", HEADER_MULTIFILE_ID, "1234-nope"),
        _MalformedCase("order_not_int", HEADER_ORDER, "three"),
        _MalformedCase("order_negative", HEADER_ORDER, "-1"),
    )
    bearer = stack.fake_security_token()
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as raw:
        for case in cases:
            chain_id, envelope_json = _fresh_envelope_json(
                stack, file_name=f"matrix-{case.name}.bin"
            )
            response = await raw.post(
                f"{stack.phantom_url}/v1/send",
                headers=_matrix_headers(
                    bearer=bearer, chain_id=chain_id, extra={case.header: case.value}
                ),
                files=_multipart_for(envelope_json),
            )
            assert response.status_code == httpx.codes.BAD_REQUEST, (
                f"case {case.name}: expected 400, got {response.status_code}: {response.text}"
            )
            payload: dict[str, Any] = response.json()
            error = payload["error"]
            assert error["code"] == "header_invalid", f"case {case.name}: envelope code"
            assert error["details"]["header"] == case.header, f"case {case.name}: details.header"
            assert error["details"]["value"] == case.value, f"case {case.name}: details.value"
            assert case.header in error["message"], (
                f"case {case.name}: the message names the offending header"
            )

            # Nothing was admitted: the chain id resolves to no row.
            miss = await raw.get(f"{stack.phantom_url}/v1/admin/chains/{chain_id}")
            assert miss.status_code == httpx.codes.NOT_FOUND, (
                f"case {case.name}: a 400-rejected submission must admit nothing"
            )

        # The strongest no-partial-state proof: a chain id whose first
        # submission was 400-rejected resubmits CLEANLY with valid
        # headers (no chain_id_in_use conflict, no idempotency replay).
        chain_id, envelope_json = _fresh_envelope_json(stack, file_name="matrix-reuse.bin")
        rejected = await raw.post(
            f"{stack.phantom_url}/v1/send",
            headers=_matrix_headers(
                bearer=bearer, chain_id=chain_id, extra={HEADER_GROUP_ID: "broken"}
            ),
            files=_multipart_for(envelope_json),
        )
        assert rejected.status_code == httpx.codes.BAD_REQUEST
        retry_group = uuid4()
        accepted = await raw.post(
            f"{stack.phantom_url}/v1/send",
            headers=_matrix_headers(
                bearer=bearer, chain_id=chain_id, extra={HEADER_GROUP_ID: str(retry_group)}
            ),
            files=_multipart_for(envelope_json),
        )
        assert accepted.status_code == httpx.codes.ACCEPTED, (
            "a chain id rejected for a malformed header must remain usable: "
            f"{accepted.status_code}: {accepted.text}"
        )
        body = json.loads(accepted.text)
        assert body["chain_id"] == str(chain_id)
        detail = await stack.phantom_client.get_upload(chain_id)
        assert detail.group_id == retry_group
