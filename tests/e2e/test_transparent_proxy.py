"""E2E — Transparent-proxy invariant.

Phantom forwards body bytes byte-identical to what the agent submitted.
Each test computes a SHA-256 of the body it intends to submit, drives
the upload through Phantom, polls until the chain reaches ``succeeded``,
then asserts the matching :class:`ReceivedEntry.body_hash` recorded by
the emulator equals the agent's pre-submit hash.

The matrix covers:
- The three codecs (passthrough/zstd/gzip) — same body, different
  storage encoding; the wire-side hash must always match.
- The two submission shapes (JSON inline base64; multipart body_refs).
- Pre-compressed bodies (client sets ``Content-Encoding: gzip``).
- A 5xx retry cycle — the same bytes go upstream twice with identical
  hashes.
- A large body (50 MiB) that forces chunked codec I/O.
- A persist transition (memory → disk) — bytes survive the round-trip.
- A no-``Content-Encoding`` baseline — the emulator records the header
  as absent rather than spuriously injecting a value.
"""

from __future__ import annotations

import gzip
import hashlib
import secrets
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client.models.chain import (
    ChainBodyBytes,
    ChainEnvelope,
    ChainStep,
)
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import (
    assert_chain_reaches_state,
    assert_emulator_received,
    assert_row_body_location,
)
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# Per-test body sizes. 100 KiB is the plan's default sweet spot for
# the codec-matrix tests — large enough to round-trip through a non-
# trivial encoder, small enough to keep the suite fast.
SMALL_BODY_BYTES: int = 100 * 1024

# 50 MiB stresses the chunked I/O paths in both the codec and the
# file body store. Random bytes so codecs can't dedup-cheat.
LARGE_BODY_BYTES: int = 50 * 1024 * 1024

# Default fake-token sub used by every helper that submits a chain.
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"


pytestmark = pytest.mark.e2e


def _sha256(data: bytes) -> str:
    """Return the SHA-256 hex of ``data``."""
    return hashlib.sha256(data).hexdigest()


async def _submit_upload_chain(
    stack: E2EStack,
    *,
    body: bytes,
    chain_id: UUID | None = None,
) -> UUID:
    """Submit an upload-shaped chain via the driver's envelope builder.

    Returns the ``chain_id`` for downstream assertions.
    """
    chain_id = chain_id or uuid4()
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {stack.fake_security_token()}",
    )
    return chain_id


async def _assert_round_trip_hash(
    stack: E2EStack,
    chain_id: UUID,
    body: bytes,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    """Assert chain succeeds AND emulator received body_hash equals SHA-256(body)."""
    await assert_chain_reaches_state(
        stack.phantom_client,
        chain_id,
        state="succeeded",
        timeout_seconds=timeout_seconds,
    )
    received = await assert_emulator_received(
        stack.emulator,
        phantom_local_uuid=str(chain_id),
        body_size=len(body),
        timeout_seconds=timeout_seconds,
    )
    expected_hash = _sha256(body)
    assert received.body_hash == expected_hash, (
        f"emulator body_hash={received.body_hash!r} != agent hash {expected_hash!r}"
    )


# ---------------------------------------------------------------------------
# Codec matrix — passthrough / zstd / gzip.
# ---------------------------------------------------------------------------


async def test_byte_identity_passthrough_codec(tmp_path: Path) -> None:
    """Passthrough codec preserves byte-identity end-to-end."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                "compression": {"mode": "always", "algorithm": "original"},
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


async def test_byte_identity_zstd_codec(tmp_path: Path) -> None:
    """Zstd codec (suite default) preserves byte-identity end-to-end."""
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


async def test_byte_identity_gzip_codec(tmp_path: Path) -> None:
    """Gzip codec preserves byte-identity end-to-end."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                "compression": {"mode": "always", "algorithm": "gzip"},
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Submission-shape matrix — JSON (inline base64) vs. multipart.
# ---------------------------------------------------------------------------


async def test_byte_identity_json_shape(tmp_path: Path) -> None:
    """JSON envelope shape (inline ``ChainBodyBytes``) preserves bytes.

    The JSON shape is selected when ``body_refs`` is empty / None; the
    body rides inline as base64 inside the envelope. Phantom decodes
    the base64 once and treats the resulting bytes identically to a
    multipart body_ref.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = uuid4()
        envelope = _build_inline_chain(
            stack=stack,
            body=body,
            chain_id=chain_id,
        )
        # body_refs=None forces the JSON shape on submit_chain.
        await stack.phantom_client.submit_chain(
            envelope,
            body_refs=None,
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {stack.fake_security_token()}",
        )
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


async def test_byte_identity_multipart_shape(tmp_path: Path) -> None:
    """Multipart envelope shape (body_refs) preserves bytes.

    Same body, same chain shape — multipart selected by passing a
    non-empty ``body_refs`` mapping. The assertion is identical to
    the JSON case; this test pins the parity invariant.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Header preservation — Content-Encoding pass-through (and absence).
# ---------------------------------------------------------------------------


async def test_byte_identity_pre_compressed_body(tmp_path: Path) -> None:
    """Client gzips before submit; emulator receives byte-identical gzip.

    The body bytes carried over the wire to upstream are EXACTLY the
    bytes the client submitted (already-gzipped). The PUT step
    declares ``Content-Encoding: gzip`` so the emulator captures the
    header for later assertion.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                "compression": {"mode": "always", "algorithm": "original"},
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        raw = secrets.token_bytes(SMALL_BODY_BYTES)
        gzipped = gzip.compress(raw)
        chain_id = uuid4()
        envelope = _build_two_step_envelope_with_put_headers(
            stack=stack,
            chain_id=chain_id,
            extra_put_headers={"Content-Encoding": "gzip"},
        )
        await stack.phantom_client.submit_chain(
            envelope,
            body_refs={"body": gzipped},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {stack.fake_security_token()}",
        )
        await _assert_round_trip_hash(stack, chain_id, gzipped)
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(gzipped),
        )
        assert received.content_encoding == "gzip", (
            f"expected emulator to record Content-Encoding=gzip; got {received.content_encoding!r}"
        )
    finally:
        await stack.tear_down()


async def test_content_encoding_header_preserved_when_unset(
    tmp_path: Path,
) -> None:
    """No Content-Encoding from client → emulator records absent header.

    The complement of the gzip test: when the chain envelope does not
    declare ``Content-Encoding`` on the PUT step, the emulator's
    ``ReceivedEntry.content_encoding`` is ``None``. This catches
    accidental injection of an encoding header somewhere in the
    pipeline.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        await _assert_round_trip_hash(stack, chain_id, body)
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(body),
        )
        assert received.content_encoding is None, (
            f"expected no Content-Encoding header; got {received.content_encoding!r}"
        )
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Retry — identical bytes on every attempt.
# ---------------------------------------------------------------------------


async def test_byte_identity_after_5xx_retry(tmp_path: Path) -> None:
    """5xx on first attempt → retry succeeds; identical hash post-retry.

    Install a 100% 5xx policy, submit, wait until at least one attempt
    has fired, then clear the policy and let the chain succeed. The
    emulator does not store the rejected body bytes (the 5xx returns
    before the body-accept path runs), so we verify the eventually-
    accepted body has the same hash as the one the agent submitted.
    Retries that mutate the body would break the assertion.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = uuid4()

        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        await _submit_upload_chain(stack, body=body, chain_id=chain_id)

        # Wait until at least one attempt has fired — confirms the
        # sender hit the failing emulator and is in a retry loop.
        async def _at_least_one_attempt() -> bool:
            detail = await stack.phantom_client.get_upload(chain_id)
            return detail.attempts >= 1 or detail.last_error is not None

        await await_until(
            _at_least_one_attempt,
            timeout_seconds=10.0,
            message="no attempt fired during the 5xx window",
        )

        stack.emulator.clear_failures()
        await _assert_round_trip_hash(stack, chain_id, body)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Scale — 50 MiB random body.
# ---------------------------------------------------------------------------


async def test_byte_identity_50_mib(tmp_path: Path) -> None:
    """50 MiB random body round-trips byte-identical through zstd."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            # Large body needs a generous saturation budget. The
            # default budgets are probe-derived and may be tighter
            # than 50 MiB on small test hosts; pin a roomy ceiling.
            "saturation": {
                "max_in_flight": 8,
                "max_in_flight_bytes": 256 * 1024 * 1024,
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(LARGE_BODY_BYTES)
        chain_id = await _submit_upload_chain(stack, body=body)
        # Wider budget for the 50 MiB round-trip — codec encode + disk
        # I/O + network simulation on top of the default 30 s.
        await _assert_round_trip_hash(stack, chain_id, body, timeout_seconds=120.0)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Persist transition — memory tier → disk tier mid-flight.
# ---------------------------------------------------------------------------


async def test_byte_identity_after_persist_transition(tmp_path: Path) -> None:
    """Body survives a RAM→file migration with byte-identity intact.

    Force the migration by setting the size-threshold knob so the body
    enqueues against the PersistController immediately at admission
    (plan § 2.3.17 hybrid-mode size-threshold path). After the
    PersistController completes the migration the row's
    ``body_location`` flips to ``'file'``; we clear the emulator's
    induced failure and assert the round-trip hash still matches the
    original bytes (the disk load path runs the codec round-trip).

    Phase 1 Slice 1.F migration: the pre-Phase-1 trigger was
    ``persist_trigger.after_attempts: 1`` (deleted with the knob);
    the new equivalent is a small ``body_size_threshold_bytes`` that
    fires the persist-controller enqueue at admission time.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # body_size_threshold_bytes=1 forces every body >= 1 byte
                # to immediate-enqueue via the persist controller in
                # hybrid mode. Equivalent to the old ``after_attempts: 1``
                # in spirit (any non-trivial body lands on disk fast).
                "persist_trigger": {
                    "body_size_threshold_bytes": 1,
                },
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        body = secrets.token_bytes(SMALL_BODY_BYTES)
        chain_id = uuid4()

        # 100% 5xx, scoped globally so the driver's `/v2/files`
        # alias also fires. One failed attempt triggers persist_now.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        await _submit_upload_chain(stack, body=body, chain_id=chain_id)

        # Confirm the row landed on disk (body_location='file') before
        # clearing the failure so we know we actually exercised the
        # disk load path on the next attempt. Phase 1 Slice 1.E
        # renamed the helper + replaced ``tier='persisted'`` with
        # ``body_location='file'``.
        await assert_row_body_location(
            stack,
            chain_id,
            body_location="file",
            timeout_seconds=20.0,
        )

        stack.emulator.clear_failures()
        await _assert_round_trip_hash(stack, chain_id, body, timeout_seconds=30.0)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Helpers — direct chain envelope construction for non-default shapes.
# ---------------------------------------------------------------------------


def _build_inline_chain(
    *,
    stack: E2EStack,
    body: bytes,
    chain_id: UUID,
) -> ChainEnvelope:
    """Build a two-step chain whose PUT body rides inline as base64.

    Used by :func:`test_byte_identity_json_shape` — the JSON
    submission shape requires every body to ride inside the envelope
    rather than as a multipart part.
    """
    import base64

    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    # Replace the PUT step's body_ref with an inline base64 body.
    put_step = envelope.steps[-1]
    inline_body = ChainBodyBytes(
        kind="bytes",
        value_b64=base64.b64encode(body).decode("ascii"),
        content_type="application/octet-stream",
    )
    new_put_step = ChainStep(
        name=put_step.name,
        method=put_step.method,
        url=put_step.url,
        headers=put_step.headers,
        body=inline_body,
        capture=put_step.capture,
        idempotency_header=put_step.idempotency_header,
    )
    return ChainEnvelope(
        chain_id=envelope.chain_id,
        idempotency_key=envelope.idempotency_key,
        steps=[envelope.steps[0], new_put_step],
        default_target=envelope.default_target,
    )


def _build_two_step_envelope_with_put_headers(
    *,
    stack: E2EStack,
    chain_id: UUID,
    extra_put_headers: dict[str, str],
) -> ChainEnvelope:
    """Clone the two-step upload envelope and inject extra headers on the PUT step."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    put_step = envelope.steps[-1]
    merged_headers = {**put_step.headers, **extra_put_headers}
    new_put_step = ChainStep(
        name=put_step.name,
        method=put_step.method,
        url=put_step.url,
        headers=merged_headers,
        body=put_step.body,
        capture=put_step.capture,
        idempotency_header=put_step.idempotency_header,
    )
    return ChainEnvelope(
        chain_id=envelope.chain_id,
        idempotency_key=envelope.idempotency_key,
        steps=[envelope.steps[0], new_put_step],
        default_target=envelope.default_target,
    )
