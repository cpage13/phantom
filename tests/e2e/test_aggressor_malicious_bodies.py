"""Aggressor (Part 5.B) - malicious request bodies over POST /v1/send.

Drives the public ingress surface with hostile body shapes and asserts
each is handled cleanly: the right status code, no crash, no data loss,
and (crucially) no unbounded buffering - an oversized payload is refused
before its bytes are read, never spiked into RAM. After each refusal the
admin interface stays truthful (no phantom row was created for a body
that never made it past the size gate).

The byte-level shapes (lying Content-Length, truncated mid-body,
slowloris-style trickle, conflicting transfer headers, bizarre
multipart) cannot be expressed through the SDK, which always builds a
well-framed request; they are sent with a hand-built ``httpx`` client
against ``stack.phantom_url``.

Verified caps (read against the live route, not assumed):

* ``_check_content_length`` rejects a declared ``Content-Length`` STRICTLY
  GREATER than ``max_buffered_bytes`` with 413 ``body_too_large`` before
  any body bytes are read. A declared length EQUAL to the cap passes the
  precheck.
* ``_read_body_capped`` aborts a chunked / no-Content-Length stream the
  moment cumulative bytes exceed the cap (413 ``body_too_large``,
  ``reason=streaming_cap``) - the backstop a lying / absent
  Content-Length cannot evade.
* A malformed ``Content-Length`` header falls through to the streaming
  cap by design.

Test-tree boundary (§ 5.0): public e2e-light lane, generic shapes and
raw HTTP only.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# A small per-upload cap so the size-gate cases trip on tiny synthetic
# payloads (keeping the suite fast). Mirrors test_e2e_10's low-cap setup.
LOW_CAP_BYTES: int = 4 * 1024  # 4 KiB

pytestmark = pytest.mark.e2e


def _single_step_json_envelope(*, emulator_url: str, chain_id: UUID) -> dict[str, Any]:
    """A one-step JSON-body envelope as a raw dict the emulator tolerates.

    Returns a ``dict[str, Any]`` because the value is a free-form JSON
    envelope built by hand (the permitted dynamic-JSON-boundary ``Any``
    per CONTEXT.md); tests control the exact wire bytes here.
    """
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": f"{emulator_url}/v2/files",
                "headers": {"Content-Type": "application/json"},
                "body": {
                    "kind": "json",
                    "value": {
                        "domain": "generic",
                        "laneBaseName": "history_parquet_data",
                        "fileName": f"body-{chain_id.hex[:8]}",
                        "metadata": {"keyValueStore": {"uploader_id": "12345"}},
                    },
                },
                "capture": [],
                "idempotency_header": None,
            }
        ],
        "default_target": None,
    }


# httpx's accepted per-part shape: (filename, content, content_type).
_MultipartFiles = dict[str, tuple[str | None, str | bytes, str]]


def _body_ref_envelope(*, emulator_url: str, chain_id: UUID) -> dict[str, Any]:
    """A one-step PUT envelope carrying a single ``body`` body_ref.

    Used for the multipart cases; the emulator's upload route returns
    403 for the fake token, but admission (the surface under test) runs
    regardless. Returns ``dict[str, Any]`` (free-form JSON boundary).
    """
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "put_s3",
                "method": "PUT",
                "url": f"{emulator_url}/v1/files/upload/tok",
                "headers": {"Content-Type": "application/octet-stream"},
                "body": {
                    "kind": "body_ref",
                    "name": "body",
                    "content_type": "application/octet-stream",
                },
                "capture": [],
                "idempotency_header": None,
            }
        ],
        "default_target": None,
    }


def _multipart_files(*, envelope: dict[str, Any], body: bytes) -> _MultipartFiles:
    """Build the multipart ``files`` mapping (envelope part + one body part)."""
    return {
        "envelope": (None, json.dumps(envelope), "application/json"),
        "body_refs[body]": ("body", body, "application/octet-stream"),
    }


def _base_headers(*, bearer: str) -> dict[str, str]:
    """The standard ingress headers shared by the raw-HTTP probes."""
    return {
        "Content-Type": "application/json",
        "X-Phantom-Uid": DEFAULT_SUB,
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Idempotency-Key": str(uuid4()),
    }


async def _backlog_count(stack: E2EStack) -> int:
    """Total rows the admin list surface reports (admin-truthfulness probe)."""
    rows, _ = await stack.phantom_client.list_uploads(limit=500)
    return len(rows)


async def test_zero_byte_body_is_rejected_cleanly_not_crashed(tmp_path: Path) -> None:
    """A zero-byte POST body is a clean envelope-invalid (422), not a crash.

    An empty body cannot parse as a ChainEnvelope, so the JSON parser
    rejects it with ``envelope_invalid`` (422). The load-bearing
    invariant: the process stays up and the admin surface is reachable
    afterwards (no row created, health still ok).
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{stack.phantom_url}/v1/send",
                content=b"",
                headers=_base_headers(bearer=bearer),
            )
        assert resp.status_code == 422, (
            f"zero-byte body should be a clean 422 envelope_invalid; got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "envelope_invalid"

        # Admin stays truthful + reachable: no row, process alive.
        assert await _backlog_count(stack) == 0, "a zero-byte reject must not create a row"
        health = await stack.phantom_client.get_health()
        assert health.status == "ok"
    finally:
        await stack.tear_down()


def _padded_json_envelope_of_size(
    *, emulator_url: str, chain_id: UUID, target_total_bytes: int
) -> bytes:
    """Serialize a one-step JSON envelope padded to EXACTLY ``target_total_bytes``.

    The cap is checked against the whole request body, so to probe the
    ``max_buffered_bytes`` boundary byte-accurately we grow the
    ``fileName`` field until the serialized JSON length equals the
    target. ASCII padding means one pad char is one byte, so the length
    converges in a single correction step.
    """
    base = _single_step_json_envelope(emulator_url=emulator_url, chain_id=chain_id)
    body_value: dict[str, Any] = base["steps"][0]["body"]["value"]
    raw = json.dumps(base).encode("utf-8")
    deficit = target_total_bytes - len(raw)
    if deficit < 0:
        raise ValueError(
            f"base envelope ({len(raw)} bytes) already exceeds target {target_total_bytes}"
        )
    body_value["fileName"] = str(body_value["fileName"]) + ("p" * deficit)
    raw = json.dumps(base).encode("utf-8")
    assert len(raw) == target_total_bytes, f"padding failed: {len(raw)} != {target_total_bytes}"
    return raw


async def test_body_at_cap_admitted_one_over_rejected(tmp_path: Path) -> None:
    """A request body exactly at the cap is admitted; one byte over is 413.

    Probes the ``max_buffered_bytes`` boundary byte-accurately on the
    JSON path (where the request body IS the envelope). The
    ``Content-Length`` precheck uses ``declared <= cap``, so a body of
    exactly ``cap`` bytes passes and is admitted (202); a body of
    ``cap + 1`` bytes is refused with ``body_too_large`` (413) BEFORE any
    body bytes are buffered. No oversized body is ever durably stored.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"max_buffered_bytes": LOW_CAP_BYTES}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        at_cap_id = uuid4()
        over_id = uuid4()
        body_at = _padded_json_envelope_of_size(
            emulator_url=stack.emulator_url, chain_id=at_cap_id, target_total_bytes=LOW_CAP_BYTES
        )
        body_over = _padded_json_envelope_of_size(
            emulator_url=stack.emulator_url,
            chain_id=over_id,
            target_total_bytes=LOW_CAP_BYTES + 1,
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            headers_at = _base_headers(bearer=bearer)
            headers_at["X-Phantom-Idempotency-Key"] = str(at_cap_id)
            r_at = await client.post(
                f"{stack.phantom_url}/v1/send", content=body_at, headers=headers_at
            )
            assert r_at.status_code == 202, (
                f"a body exactly at the cap should be admitted; got {r_at.status_code}: {r_at.text}"
            )

            headers_over = _base_headers(bearer=bearer)
            headers_over["X-Phantom-Idempotency-Key"] = str(over_id)
            r_over = await client.post(
                f"{stack.phantom_url}/v1/send", content=body_over, headers=headers_over
            )
            assert r_over.status_code == 413, (
                f"a body one byte over the cap should be 413 body_too_large; got "
                f"{r_over.status_code}: {r_over.text}"
            )
            assert r_over.json()["error"]["code"] == "body_too_large"

        # The over-cap chain must NOT have a row (refused before durable write).
        rows, _ = await stack.phantom_client.list_uploads(limit=500)
        ids = {r.chain_id for r in rows}
        assert over_id not in ids, "an over-cap body must not create a durable row"
        assert at_cap_id in ids, "the at-cap body should have been admitted as a row"
    finally:
        await stack.tear_down()


async def test_content_length_over_cap_rejected_by_precheck(tmp_path: Path) -> None:
    """An over-cap Content-Length is refused by the precheck, not the stream cap.

    A declared ``Content-Length`` greater than the cap is rejected with
    413 ``body_too_large`` and ``reason=content_length_precheck`` - the
    precheck fires on the header before ``_read_body_capped`` accumulates
    the body, so a multi-GB declared POST cannot spike RAM. We send a
    genuinely over-cap body (httpx sets the honest, over-cap
    Content-Length) and pin the ``content_length_precheck`` reason, which
    is what distinguishes this header-only gate from the streaming-cap
    backstop exercised by the chunked case below.

    (A client that under-declares Content-Length to smuggle extra bytes
    cannot get them past the ASGI server, which reads exactly the
    declared length; the streaming-cap test covers the no/absent
    Content-Length evasion path.)
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"max_buffered_bytes": LOW_CAP_BYTES}},
    )
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()
        headers = _base_headers(bearer=bearer)
        # A genuinely over-cap body; httpx sets a matching over-cap
        # Content-Length, which the precheck rejects on the header alone.
        oversized = b"x" * (LOW_CAP_BYTES * 4)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{stack.phantom_url}/v1/send",
                content=oversized,
                headers=headers,
            )
        assert resp.status_code == 413, (
            f"an over-cap declared Content-Length should be 413; got "
            f"{resp.status_code}: {resp.text}"
        )
        body = resp.json()["error"]
        assert body["code"] == "body_too_large"
        assert body["details"].get("reason") == "content_length_precheck", (
            f"expected the precheck (header-only gate) to fire; details={body['details']}"
        )
        assert await _backlog_count(stack) == 0
    finally:
        await stack.tear_down()


async def test_chunked_over_cap_aborts_midstream(tmp_path: Path) -> None:
    """A chunked body with no Content-Length is aborted by the streaming cap.

    A lying or absent Content-Length cannot evade the cap: the streaming
    backstop aborts the moment cumulative bytes exceed the cap, returning
    413 ``body_too_large`` (``reason=streaming_cap``). We stream a chunked
    body that overshoots the cap and assert the clean refusal, proving no
    unbounded buffering.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"max_buffered_bytes": LOW_CAP_BYTES}},
    )
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()

        async def _overshooting_chunks() -> AsyncIterator[bytes]:
            # Yield more than the cap in several chunks; no Content-Length
            # is set when the body is an async generator (chunked TE).
            for _ in range((LOW_CAP_BYTES // 512) + 4):
                yield b"x" * 512

        headers = _base_headers(bearer=bearer)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(
                "POST",
                f"{stack.phantom_url}/v1/send",
                content=_overshooting_chunks(),
                headers=headers,
            )
        assert resp.status_code == 413, (
            f"an over-cap chunked body should be aborted with 413; got "
            f"{resp.status_code}: {resp.text}"
        )
        body = resp.json()["error"]
        assert body["code"] == "body_too_large"
        assert body["details"].get("reason") == "streaming_cap", (
            f"expected the streaming cap to fire; details={body['details']}"
        )
        assert await _backlog_count(stack) == 0
    finally:
        await stack.tear_down()


async def test_truncated_body_midstream_does_not_crash(tmp_path: Path) -> None:
    """A client that aborts mid-body leaves no row and the service alive.

    The client streams a partial body then raises, severing the
    connection before the full envelope arrives. Phantom must not crash
    or leak a half-row; a follow-up health check confirms the process is
    still serving and no phantom row was created.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        full = json.dumps(
            _single_step_json_envelope(emulator_url=stack.emulator_url, chain_id=chain_id)
        ).encode("utf-8")

        class _TruncateError(Exception):
            """Raised mid-stream to sever the upload connection."""

        async def _half_then_die() -> AsyncIterator[bytes]:
            yield full[: len(full) // 2]
            raise _TruncateError

        headers = _base_headers(bearer=bearer)
        headers["X-Phantom-Idempotency-Key"] = str(chain_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            # The aborted send raises locally; the server must survive it.
            with pytest.raises((_TruncateError, httpx.HTTPError)):
                await client.request(
                    "POST",
                    f"{stack.phantom_url}/v1/send",
                    content=_half_then_die(),
                    headers=headers,
                )

        # The service is still up and the truncated chain produced no row.
        health = await stack.phantom_client.get_health()
        assert health.status == "ok", "service must survive a mid-body client abort"
        rows, _ = await stack.phantom_client.list_uploads(limit=500)
        assert chain_id not in {r.chain_id for r in rows}, (
            "a truncated upload must not leave a durable row"
        )
    finally:
        await stack.tear_down()


async def test_zstd_compressible_bomb_is_handled_with_small_stored_size(tmp_path: Path) -> None:
    """A highly-compressible (all-zeros) body is admitted with a tiny stored size.

    The always-encode codec is zstd; a body of all-zeros compresses to
    almost nothing. A 256 KiB all-zeros body (well under a 1 MiB cap)
    must admit cleanly (202) and the codec path must not blow up; the
    stored (compressed) size reported by admin must be a tiny fraction of
    the raw 256 KiB, confirming the codec ran and the row is buffered
    with a real, bounded stored size, not an unbounded one. (The cap
    gates RAW request bytes, independent of compression, so this is the
    codec-robustness probe, not a cap-evasion one.)
    """
    bomb_cap_bytes = 1024 * 1024  # 1 MiB cap, comfortably above the body
    raw_body_bytes = 256 * 1024  # 256 KiB of zeros
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"max_buffered_bytes": bomb_cap_bytes}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        # All-zeros body: maximally compressible (the zstd "bomb" shape).
        zeros = b"\x00" * raw_body_bytes
        files = _multipart_files(
            envelope=_body_ref_envelope(emulator_url=stack.emulator_url, chain_id=chain_id),
            body=zeros,
        )
        headers = _base_headers(bearer=bearer)
        del headers["Content-Type"]
        headers["X-Phantom-Idempotency-Key"] = str(chain_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{stack.phantom_url}/v1/send", files=files, headers=headers)
        assert resp.status_code == 202, (
            f"a compressible cap-sized body should admit cleanly; got "
            f"{resp.status_code}: {resp.text}"
        )
        # The row exists (the codec ran). The aggregate stored bytes,
        # summed across body locations, are a tiny fraction of the raw
        # 256 KiB - confirming zstd compressed the all-zeros body and the
        # buffer is bounded, not blown up. (ChainAdminDetail does not
        # surface a per-row stored size, so the stats body_location
        # breakdown is the observable stored-bytes signal.)
        detail = await stack.phantom_client.get_upload(chain_id)
        assert detail.chain_id == chain_id
        stats = await stack.phantom_client.get_stats(instance="primary")
        total_stored = sum(tier.bytes for tier in stats.body_location.values())
        assert 0 <= total_stored < raw_body_bytes // 10, (
            f"all-zeros body should compress to far below 10% of raw; "
            f"raw={raw_body_bytes} total_stored={total_stored}"
        )
    finally:
        await stack.tear_down()


async def test_conflicting_transfer_headers_handled_cleanly(tmp_path: Path) -> None:
    """Conflicting Content-Length + Transfer-Encoding does not crash the server.

    RFC 9112 forbids sending both ``Content-Length`` and
    ``Transfer-Encoding: chunked``; an attacker sends both to attempt
    request smuggling. Whatever the ASGI server decides (reject as a
    protocol error, or honor TE and ignore CL), Phantom must not crash:
    the request resolves to SOME definite HTTP status and the admin
    surface stays reachable afterward.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        body = json.dumps(
            _single_step_json_envelope(emulator_url=stack.emulator_url, chain_id=chain_id)
        ).encode("utf-8")
        headers = _base_headers(bearer=bearer)
        headers["X-Phantom-Idempotency-Key"] = str(chain_id)
        # Send BOTH a (wrong) Content-Length and Transfer-Encoding.
        headers["Content-Length"] = str(len(body) + 999)
        headers["Transfer-Encoding"] = "chunked"
        status_seen: int | None = None
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                resp = await client.request(
                    "POST",
                    f"{stack.phantom_url}/v1/send",
                    content=body,
                    headers=headers,
                )
                status_seen = resp.status_code
            except httpx.HTTPError:
                # A client/transport-level rejection of the malformed
                # framing is an acceptable outcome (the request never
                # reached application logic); the server-survival check
                # below is the load-bearing assertion.
                status_seen = None

        # Whatever happened on the wire, the process is still serving.
        health = await stack.phantom_client.get_health()
        assert health.status == "ok", (
            f"service must survive conflicting transfer headers (status_seen={status_seen})"
        )
    finally:
        await stack.tear_down()


async def test_bizarre_multipart_missing_envelope_part_rejected(tmp_path: Path) -> None:
    """Multipart with body parts but NO ``envelope`` part is a clean 422.

    A multipart submission that omits the required ``envelope`` part (or
    names every part nonsensically) cannot be parsed; the parser rejects
    with ``envelope_invalid`` (422) rather than crashing. The process
    stays up and no row is created.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        bearer = stack.fake_security_token()
        # Parts present, but none named "envelope" -> missing envelope.
        files: _MultipartFiles = {
            "not_an_envelope": (None, "garbage", "text/plain"),
            "body_refs[orphan]": ("orphan", b"orphan-bytes", "application/octet-stream"),
        }
        headers = _base_headers(bearer=bearer)
        del headers["Content-Type"]
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{stack.phantom_url}/v1/send", files=files, headers=headers)
        assert resp.status_code == 422, (
            f"multipart missing the envelope part should be 422 envelope_invalid; got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "envelope_invalid"
        assert await _backlog_count(stack) == 0
        health = await stack.phantom_client.get_health()
        assert health.status == "ok"
    finally:
        await stack.tear_down()


async def test_slowloris_upstream_trickle_does_not_lose_the_upload(tmp_path: Path) -> None:
    """A slow-trickling upstream does not lose the buffered upload.

    The emulator trickles its upload-step response at a few bytes/sec
    (``slow_trickle_bytes_per_sec``), a slowloris-shaped upstream. Phantom
    buffered the body durably at admission, so even if the trickled
    attempt is slow or times out, the chain is retried, not dropped: it
    reaches a terminal state (succeeded once the trickle completes, or
    parks for retry) and the admin surface keeps reporting the row. The
    load-bearing invariant is Invariant 1 - an undelivered upload is never
    lost - under a deliberately sluggish upstream.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 100,
                "default_strategy": {"type": "fixed_intervals", "intervals_seconds": [0, 1, 1, 2]},
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        # Trickle the upload step's response very slowly.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                slow_trickle_bytes_per_sec=8,
            )
        )

        chain_id = uuid4()
        req = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
        req.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
        envelope, _ = build_in_memory_upload_envelope(
            request=req,
            files_api_base=stack.emulator_url,
            local_uuid=chain_id,
        )
        resp = await pc.submit_chain(
            envelope,
            body_refs={"body": b"phantom-slowloris-upstream-body"},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {bearer}",
        )
        assert resp.chain_id == chain_id

        # The buffered upload is durable: it is still tracked by admin and
        # eventually reaches a terminal state (the small body's trickle
        # completes well inside the budget). Either way it is never lost.
        detail = await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=30.0
        )
        assert detail.state == "succeeded"
    finally:
        await stack.tear_down()
