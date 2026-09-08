"""Regression tests for the ``POST /v1/send`` untrusted-boundary findings.

``routes/send.py`` parses producer-controlled input on an unauthenticated
port, so every refusal it can make has to be reachable before the bytes
that would make it expensive are read. Four defects broke that:

* **Multipart had no size cap at all.** The route relied on starlette's
  ``max_part_size``, which ``MultiPartParser.on_part_data`` checks ONLY
  inside ``if self._current_part.file is None``. A part whose
  Content-Disposition carries a filename (phantom-client's transport gives
  EVERY part one) takes the else branch and is appended to a
  ``SpooledTemporaryFile`` unchecked. With ``Transfer-Encoding: chunked``
  there was no ``Content-Length`` for the precheck to read either, so a
  producer could spool without limit and then have the whole spool pulled
  into one ``bytes`` object by ``await value.read()``.
* **The JSON envelope was capped 2000x too high.** It was read under the
  per-upload body cap (2 GiB by default) where the multipart path applies
  the 1 MiB ``ENVELOPE_MAX_BYTES`` to the same envelope.
* **Every form-parser error was reported 413 ``body_too_large``.**
  ``Request._get_form`` converts ANY ``MultiPartException`` to
  ``HTTPException(400)`` when ``"app"`` is in scope, which FastAPI always
  sets, and the parser raises it from five sites of which only one is a
  size overrun. A missing boundary came back as 413 with a
  ``multipart_part_too_large`` reason: status, message and details all
  disagreeing, and a client that shrinks its payload on 413 retrying
  smaller forever.
* **A guaranteed-rejection header parse ran after the body was
  buffered.** ``_parse_grouping_headers`` depends on nothing the body
  provides, so a request already destined for 400 was first allowed to
  stream up to the cap.

Each test names the shape it drives and the observable that separates
fixed from broken.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.parser import ENVELOPE_MAX_BYTES
from phantom.config.settings import SaturationCfg
from phantom.routes import send as send_routes

from .test_send_route import _build_app

# A deliberately tiny per-upload cap. Every size probe below is sized
# against it, so the suite trips the caps on kilobytes rather than
# gigabytes and stays fast.
_TINY_CAP_BYTES = 1_000

# Payload slab handed to the streaming probes. Small enough that the cap
# is crossed several slabs in, so "the server stopped early" is a real
# observation rather than an artifact of one giant chunk.
_SLAB_BYTES = 256

# Total slabs a streaming probe offers. 800 * 256 B = 200 KiB, which is
# 200x the cap: pre-fix the multipart path buffered every one of them.
_SLAB_COUNT = 800

# Fixed multipart boundary for the hand-built wire bodies. The tests build
# the multipart framing themselves because the cases under test (chunked
# framing, a missing boundary parameter) cannot be expressed through
# httpx's own ``files=`` builder.
_BOUNDARY = "phantomtestboundary"

_TARGET_HOST = "files.example.com"

# Row-count caps for the one test that raises the fixture's saturation
# gate out of the way; the gate is not the subject there.
_ROOMY_MAX_IN_FLIGHT = 100


async def _build_capped_app(tmp_path: Path, cap: int) -> FastAPI:
    """Build the ingress app with ``max_buffered_bytes`` pinned to ``cap``."""
    app, _ = await _build_app(tmp_path)
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: cap
    return app


def _body_ref_envelope() -> dict[str, Any]:
    """A one-step envelope declaring a single ``body`` body_ref."""
    return {
        "chain_id": str(uuid4()),
        "idempotency_key": "k",
        "steps": [
            {
                "name": "put_s3",
                "method": "PUT",
                "url": f"https://{_TARGET_HOST}/v2/files",
                "body": {
                    "kind": "body_ref",
                    "name": "body",
                    "content_type": "application/octet-stream",
                },
            }
        ],
    }


def _multipart_preamble() -> bytes:
    """The envelope part plus the opening framing of the file part.

    The file part declares a ``filename``, which is exactly what puts
    starlette on the unchecked ``SpooledTemporaryFile`` branch and is what
    phantom-client's transport sends on every part.
    """
    envelope = json.dumps(_body_ref_envelope())
    return (
        f"--{_BOUNDARY}\r\n"
        'Content-Disposition: form-data; name="envelope"\r\n'
        "Content-Type: application/json\r\n"
        "\r\n"
        f"{envelope}\r\n"
        f"--{_BOUNDARY}\r\n"
        'Content-Disposition: form-data; name="body_refs[body]"; filename="body.bin"\r\n'
        "Content-Type: application/octet-stream\r\n"
        "\r\n"
    ).encode()


class _SlabCounter:
    """Counts how many payload slabs the server actually pulled.

    The whole point of a mid-stream cap is that the producer never gets to
    deliver the rest, so the count is the load-bearing observation: an
    uncapped path drains all :data:`_SLAB_COUNT` of them.
    """

    def __init__(self) -> None:
        self.slabs_sent = 0

    async def stream(self) -> AsyncIterator[bytes]:
        """Yield the hand-built multipart body one slab at a time."""
        yield _multipart_preamble()
        for _ in range(_SLAB_COUNT):
            self.slabs_sent += 1
            yield b"x" * _SLAB_BYTES
        yield f"\r\n--{_BOUNDARY}--\r\n".encode()


async def _post_streamed(app: FastAPI, counter: _SlabCounter) -> httpx.Response:
    """POST the counter's stream to ``/v1/send`` without a Content-Length.

    ``httpx.ASGITransport`` pulls the request body lazily, one ``receive``
    at a time, so the app decides how much of it is ever produced. An
    async-iterator body also means httpx sends ``Transfer-Encoding:
    chunked`` with no ``Content-Length``, which is the shape that slips
    past the precheck.
    """
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.post(
            "/v1/send",
            content=counter.stream(),
            headers={
                "X-Phantom-Uid": "u",
                "Content-Type": f"multipart/form-data; boundary={_BOUNDARY}",
            },
        )


# ---------------------------------------------------------------------------
# Finding 1 - the multipart path had no size cap at all.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multipart_file_part_is_capped_mid_stream(tmp_path: Path) -> None:
    """A chunked multipart file part over the cap is refused mid-stream.

    Objective: prove the cap is enforced DURING the read, so unbounded
    producer bytes reach neither the temp filesystem nor RAM. Success is
    413 ``body_too_large`` with ``reason="streaming_cap"`` AND an
    ``observed`` count at the cap rather than at the payload size, which
    is only possible if the read aborted instead of completing.

    Pre-fix this returned 422 ``body_ref_missing`` only AFTER starlette
    had spooled the entire 200 KiB part and ``await value.read()`` had
    pulled it into one ``bytes`` object.
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    counter = _SlabCounter()

    response = await _post_streamed(app, counter)

    assert response.status_code == 413, response.text
    error = response.json()["error"]
    assert error["code"] == "body_too_large"
    assert error["details"]["reason"] == "streaming_cap"
    assert error["details"]["limit"] == _TINY_CAP_BYTES
    # The refusal fired at the cap, not after the payload was consumed.
    assert error["details"]["observed"] <= _TINY_CAP_BYTES + _SLAB_BYTES


@pytest.mark.asyncio
async def test_multipart_refusal_leaves_the_payload_unsent(tmp_path: Path) -> None:
    """The producer never gets to deliver the body it was refused for.

    Objective: the sibling half of the cap contract, measured on the
    PRODUCER side. Success is a slab count a small multiple of the cap,
    proving the server stopped reading; an uncapped path drains all 800
    slabs (200 KiB) before answering.
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    counter = _SlabCounter()

    response = await _post_streamed(app, counter)

    assert response.status_code == 413, f"{counter.slabs_sent=} {response.text}"
    max_slabs = _TINY_CAP_BYTES // _SLAB_BYTES + 2
    assert counter.slabs_sent <= max_slabs, (
        f"the server drained {counter.slabs_sent} slabs of {_SLAB_COUNT}; "
        f"a mid-stream cap must stop within {max_slabs}"
    )


@pytest.mark.asyncio
async def test_multipart_under_the_cap_still_admits(tmp_path: Path) -> None:
    """Counter-test: a file part that fits is admitted exactly as before.

    Objective: the cap must refuse only what breaches it. Success is a 202
    for a body well under a cap that comfortably covers the multipart
    framing too.
    """
    app, ctx = await _build_app(tmp_path)
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: _TINY_CAP_BYTES
    envelope = _body_ref_envelope()
    files = {
        "envelope": (None, json.dumps(envelope), "application/json"),
        "body_refs[body]": ("body.bin", b"y" * 64, "application/octet-stream"),
    }
    client = TestClient(app)

    response = client.post("/v1/send", files=files, headers={"X-Phantom-Uid": "u"})

    assert response.status_code == 202, response.text
    from uuid import UUID

    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None


# ---------------------------------------------------------------------------
# Finding 2 - the JSON envelope was read under the body cap, not the
# envelope cap.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_json_envelope_is_capped_at_the_envelope_limit(tmp_path: Path) -> None:
    """A JSON envelope over 1 MiB is refused even under a 2 GiB body cap.

    Objective: the envelope cap the multipart path already applies to its
    ``envelope`` part must apply to the same envelope on the JSON path.
    Success is 413 ``body_too_large`` reporting ``ENVELOPE_MAX_BYTES`` as
    the limit while the configured per-upload cap is 2 GiB.

    Pre-fix this envelope was read under the body cap, so a 2 GiB JSON
    submission was buffered in full; and because an envelope declares no
    body_refs, admission charges the saturation gate ZERO bytes for it, so
    the byte cap could not bound how many were in parse at once either.
    """
    production_default_cap = 2_147_483_648
    app = await _build_capped_app(tmp_path, production_default_cap)
    oversized = b"x" * (ENVELOPE_MAX_BYTES + 1)
    client = TestClient(app)

    response = client.post(
        "/v1/send",
        content=oversized,
        headers={"X-Phantom-Uid": "u", "Content-Type": "application/json"},
    )

    assert response.status_code == 413, response.text
    error = response.json()["error"]
    assert error["code"] == "body_too_large"
    assert error["details"]["limit"] == ENVELOPE_MAX_BYTES
    assert error["details"]["limit"] < production_default_cap
    # The streaming read is what refuses it: the Content-Length precheck
    # keeps the per-upload cap, so it lets this declaration through and the
    # envelope ceiling stops the read 2 GiB short of where it used to.
    assert error["details"]["reason"] == "streaming_cap"


@pytest.mark.asyncio
async def test_envelope_cap_never_exceeds_a_smaller_configured_cap(tmp_path: Path) -> None:
    """An operator cap below 1 MiB still wins on the JSON path.

    Objective: the envelope ceiling is a ``min``, not a replacement, so an
    operator who pins a small ``max_buffered_bytes`` stays in charge.
    Success is the refusal reporting the configured cap, not 1 MiB.
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    client = TestClient(app)

    response = client.post(
        "/v1/send",
        content=b"x" * (_TINY_CAP_BYTES + 1),
        headers={"X-Phantom-Uid": "u", "Content-Type": "application/json"},
    )

    assert response.status_code == 413, response.text
    assert response.json()["error"]["details"]["limit"] == _TINY_CAP_BYTES


@pytest.mark.asyncio
async def test_multipart_body_ref_still_gets_the_full_upload_cap(tmp_path: Path) -> None:
    """Counter-test: the envelope ceiling does not shrink the upload cap.

    Objective: the 1 MiB ceiling belongs to the ENVELOPE, not to the
    payload, so a multipart submission keeps the whole configured
    per-upload cap. Success is the multipart ceiling tracking
    ``max_buffered_bytes`` while the JSON one clamps to
    ``ENVELOPE_MAX_BYTES``, asserted on the one function both the
    Content-Length precheck and the streaming backstop consult.
    """
    generous_cap = 8 * ENVELOPE_MAX_BYTES
    multipart_type = f"multipart/form-data; boundary={_BOUNDARY}"

    assert send_routes._body_read_cap(multipart_type, generous_cap) == generous_cap
    assert send_routes._body_read_cap("application/json", generous_cap) == ENVELOPE_MAX_BYTES
    # And the operator's smaller cap still wins on both paths.
    assert send_routes._body_read_cap(multipart_type, _TINY_CAP_BYTES) == _TINY_CAP_BYTES
    assert send_routes._body_read_cap("application/json", _TINY_CAP_BYTES) == _TINY_CAP_BYTES


@pytest.mark.asyncio
async def test_multipart_body_ref_over_the_envelope_cap_admits(tmp_path: Path) -> None:
    """The end-to-end half: a body_ref larger than 1 MiB still admits.

    Objective: pin the ceiling claim on the wire, not only on the helper.
    The fixture's saturation byte cap is raised first, because that gate
    (not the read cap) is what would otherwise refuse a multi-MiB body in
    this fixture. Success is a 202 for a body_ref twice the envelope cap.
    """
    app, ctx = await _build_app(tmp_path)
    generous_cap = 8 * ENVELOPE_MAX_BYTES
    app.dependency_overrides[send_routes.get_max_buffered_bytes] = lambda: generous_cap
    await ctx.saturation.update_caps(
        SaturationCfg(
            max_in_flight=_ROOMY_MAX_IN_FLIGHT,
            max_in_flight_bytes=generous_cap,
            max_disk_bytes=generous_cap,
            large_body_threshold_bytes=0,
            max_large_in_flight=_ROOMY_MAX_IN_FLIGHT,
        )
    )
    files = {
        "envelope": (None, json.dumps(_body_ref_envelope()), "application/json"),
        "body_refs[body]": (
            "body.bin",
            b"z" * (2 * ENVELOPE_MAX_BYTES),
            "application/octet-stream",
        ),
    }
    client = TestClient(app)

    response = client.post("/v1/send", files=files, headers={"X-Phantom-Uid": "u"})

    assert response.status_code == 202, response.text


# ---------------------------------------------------------------------------
# Finding 3 - malformed multipart was reported as a size refusal.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_multipart_boundary_is_a_malformed_body_not_a_413(
    tmp_path: Path,
) -> None:
    """A multipart Content-Type with no boundary is 422, not 413.

    Objective: the five parser refusals are not one refusal. Only a size
    overrun is a size overrun; a missing boundary parameter is a malformed
    body and must not tell a size-shrinking client to retry smaller.
    Success is 422 ``envelope_invalid`` with a ``multipart_malformed``
    reason and no size ``limit`` in the details.

    Pre-fix this was 413 ``body_too_large`` carrying
    ``{"reason": "multipart_part_too_large", "limit": 2147483648}`` beside
    the message "Missing boundary in multipart."
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    client = TestClient(app)

    response = client.post(
        "/v1/send",
        content=b"whatever",
        # multipart/form-data with the boundary parameter omitted.
        headers={"X-Phantom-Uid": "u", "Content-Type": "multipart/form-data"},
    )

    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "envelope_invalid"
    assert error["details"] == {"reason": "multipart_malformed"}


@pytest.mark.asyncio
async def test_part_without_a_name_is_a_malformed_body_not_a_413(tmp_path: Path) -> None:
    """A part whose Content-Disposition omits ``name`` is 422, not 413.

    Objective: the second of the four non-size parser refusals, driven on
    the wire. Success is the same malformed-body mapping as the missing
    boundary; the payload is a handful of bytes, so a 413 would be
    self-evidently wrong.
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    body = (
        f"--{_BOUNDARY}\r\nContent-Disposition: form-data\r\n\r\ntiny\r\n--{_BOUNDARY}--\r\n"
    ).encode()
    client = TestClient(app)

    response = client.post(
        "/v1/send",
        content=body,
        headers={
            "X-Phantom-Uid": "u",
            "Content-Type": f"multipart/form-data; boundary={_BOUNDARY}",
        },
    )

    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "envelope_invalid"
    assert len(body) < _TINY_CAP_BYTES  # a size refusal here would be a lie


# ---------------------------------------------------------------------------
# Finding 4 - a guaranteed-rejection header parse ran after the body read.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bad_grouping_header_is_refused_before_the_body_is_read(
    tmp_path: Path,
) -> None:
    """A malformed grouping header rejects without draining the body.

    Objective: ``_parse_grouping_headers`` reads only ``request.headers``
    and can only accept or reject, so a request already destined for 400
    must not first be allowed to stream up to ``max_buffered_bytes`` into
    the process - the same argument the explicit-route degraded guard is
    already ordered on. Success is 400 ``header_invalid`` with ZERO
    payload slabs pulled from the producer.

    Pre-fix the header parse ran after ``_parse_and_resolve`` had already
    streamed and materialised the whole body, which made it the cheapest
    amplifier for the two size findings above.
    """
    app = await _build_capped_app(tmp_path, _TINY_CAP_BYTES)
    counter = _SlabCounter()
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/v1/send",
            content=counter.stream(),
            headers={
                "X-Phantom-Uid": "u",
                "X-Phantom-Group-Id": "not-a-uuid",
                "Content-Type": f"multipart/form-data; boundary={_BOUNDARY}",
            },
        )

    assert response.status_code == 400, response.text
    error = response.json()["error"]
    assert error["code"] == "header_invalid"
    assert error["details"]["header"] == "X-Phantom-Group-Id"
    assert counter.slabs_sent == 0, (
        f"a header-only rejection read {counter.slabs_sent} payload slabs; it must read none"
    )


@pytest.mark.asyncio
async def test_valid_grouping_headers_still_reach_admission(tmp_path: Path) -> None:
    """Counter-test: the reorder changes ordering only, never outcomes.

    Objective: moving the parse earlier must not change what a WELL-FORMED
    grouping header does. Success is a 202 whose row carries the supplied
    group id.
    """
    app, ctx = await _build_app(tmp_path)
    group_id = uuid4()
    client = TestClient(app)

    response = client.post(
        "/v1/send",
        json={
            "chain_id": str(uuid4()),
            "idempotency_key": "k",
            "steps": [
                {
                    "name": "create_file",
                    "method": "POST",
                    "url": f"https://{_TARGET_HOST}/v2/files",
                }
            ],
        },
        headers={"X-Phantom-Uid": "u", "X-Phantom-Group-Id": str(group_id)},
    )

    assert response.status_code == 202, response.text
    from uuid import UUID

    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    assert row.group_id == group_id
