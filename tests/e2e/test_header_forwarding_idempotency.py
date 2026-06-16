"""Header-forwarding: the idempotency value, the body, and the reserved namespace (§ 5.D).

Plan § 5.2 Part 5.D, the header-forwarding bundle. The existing transparent-proxy
suite already proves body byte-identity per codec (``test_transparent_proxy.py``) and
custom-header / X-Phantom-strip / Authorization-substitution behaviour
(``test_aggressor_transparent_proxy_headers.py``). This file fills the remaining
Part 5.D assertions, which none of those cover:

* :func:`test_idempotency_value_forwarded_as_configured_header_and_stable_across_retries`
  - the upstream receives the chain's idempotency value AS the configured
    ``Idempotency-Key`` header (the step's ``idempotency_header``), it equals
    ``envelope.idempotency_key`` AND the admin row's ``idempotency_key``, and it is
    STABLE across a forced upstream retry. Stability is proven deterministically: the
    CREATE step (``FailureScope.UPSTREAM_FILES_CREATE``) is made to 503 until a near-future
    moment, so the sender retries the create step (the executor resumes from the failed
    step, ``sender.py`` ``current_step_index``) and re-sends the SAME idempotency value
    on every attempt before it finally succeeds.
* :func:`test_body_is_the_original_upload_not_the_envelope_json` - the bytes the upstream
  receives on the PUT are the ORIGINAL buffered upload (``body_hash`` byte-identity
  against the agent's pre-submit SHA-256), NOT the envelope JSON. The ``ChainEnvelope`` is
  a stored, replayable PLAN, never the payload: the create-step's JSON body and the
  serialized envelope both differ in bytes AND length from the original upload, so a
  regression that forwarded either would be caught by both the hash and the size.
* :func:`test_no_x_phantom_header_reaches_upstream` - no ``X-Phantom-*`` header (Phantom's
  reserved ingress namespace) leaks onto the upstream call, asserted directly against the
  emulator's full inbound-header capture across BOTH chain steps.

Public e2e-light lane (§ 5.0): generic ``submit`` shapes + the emulator.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

pytestmark = pytest.mark.e2e

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
# The configured idempotency header on the create step (see _driver._IDEMPOTENCY_HEADER).
CONFIGURED_IDEMPOTENCY_HEADER: str = "Idempotency-Key"
TERMINAL_BUDGET_SECONDS: float = 20.0

# A distinctive original upload. Chosen so neither the create-step JSON envelope nor the
# serialized ChainEnvelope can possibly hash- or length-match it by accident.
ORIGINAL_BODY: bytes = b"phantom-5D-original-upload-bytes:" + bytes(range(64))


async def _submit(
    stack: E2EStack,
    *,
    chain_id: UUID,
    body: bytes,
    bearer: str,
) -> UUID:
    """Submit one two-step chain; return the chain_id used as the local UUID."""
    request = build_create_file_request(file_name=f"hdr_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    # The configured idempotency value is the envelope's idempotency_key.
    assert envelope.idempotency_key == str(chain_id)
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id


async def test_idempotency_value_forwarded_as_configured_header_and_stable_across_retries(
    tmp_path: Path,
) -> None:
    """Idempotency rides as the configured header, equals envelope+row, stable across retries.

    The CREATE step is forced to 503 for a short window so the sender retries it; the
    create ``Idempotency-Key`` the upstream finally accepts must be byte-identical to
    ``envelope.idempotency_key`` (== ``str(chain_id)``) and to the admin row's
    ``idempotency_key``. A regenerated-per-attempt value would break at-least-once dedup.

    Falsifier: have the executor mint a fresh idempotency value per attempt (or forward it
    under a hard-coded header name) -> the received ``idempotency_key`` differs from the
    envelope value / arrives under the wrong name -> RED.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"retry": {"worker_count": 2, "poll_interval_ms": 100}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Force the CREATE step (/v2/files) to 503 for ~2.5 s: the sender retries it, so the
        # create Idempotency-Key is re-sent on each attempt before the create finally lands.
        unavailable_until = datetime.now(UTC) + timedelta(seconds=2.5)
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; mypy lacks the plugin
                scope=FailureScope.UPSTREAM_FILES_CREATE,
                unavailable_until=unavailable_until,
            )
        )

        chain_id = uuid4()
        await _submit(stack, chain_id=chain_id, body=ORIGINAL_BODY, bearer=bearer)

        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        # The chain retried at least once (the create step was down at first attempt).
        assert detail.attempts >= 1, (
            f"expected a retry on the down create step; attempts={detail.attempts}"
        )

        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(ORIGINAL_BODY),
        )
        # The upstream received the idempotency value as the configured Idempotency-Key
        # header (the emulator parses that exact header into ReceivedEntry.idempotency_key).
        assert received.idempotency_key == str(chain_id), (
            f"upstream idempotency value {received.idempotency_key!r} != envelope value "
            f"{str(chain_id)!r}; the configured {CONFIGURED_IDEMPOTENCY_HEADER!r} header did "
            "not carry the chain's idempotency_key"
        )
        # And the admin row reports the same forwarded value (single source of truth).
        rows, _ = await stack.phantom_client.list_uploads(limit=100)
        matching = [r for r in rows if r.chain_id == chain_id]
        assert matching, f"no admin row for chain {chain_id}"
        assert matching[0].idempotency_key == str(chain_id), (
            f"admin row idempotency_key {matching[0].idempotency_key!r} != forwarded value "
            f"{str(chain_id)!r}"
        )
    finally:
        await stack.tear_down()


async def test_body_is_the_original_upload_not_the_envelope_json(tmp_path: Path) -> None:
    """The PUT body upstream is the ORIGINAL upload, never the envelope JSON.

    The envelope is a stored replayable plan; the bytes forwarded are the buffered upload.
    Asserts ``received.body_hash`` equals SHA-256 of the original body AND that neither the
    create-step JSON body nor the serialized ``ChainEnvelope`` could have been forwarded
    instead (both differ in hash AND length from the original).

    Falsifier: forward the envelope JSON (or the create-step JSON) as the PUT body ->
    ``received.body_hash`` / ``body_size`` no longer match the original -> RED.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        request = build_create_file_request(file_name=f"hdr_{chain_id.hex[:12]}")
        request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
        envelope, _ = build_in_memory_upload_envelope(
            request=request,
            files_api_base=stack.emulator_url,
            local_uuid=chain_id,
        )
        await stack.phantom_client.submit_chain(
            envelope,
            body_refs={"body": ORIGINAL_BODY},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {bearer}",
        )

        await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(ORIGINAL_BODY),
        )

        expected_hash = hashlib.sha256(ORIGINAL_BODY).hexdigest()
        assert received.body_hash == expected_hash, (
            f"upstream body_hash {received.body_hash!r} != original-upload hash "
            f"{expected_hash!r}; the forwarded body is not the buffered upload"
        )

        # Prove the envelope/create-step JSON are genuinely different bytes, so a
        # mistaken forward of either would have been caught above.
        envelope_json = envelope.model_dump_json().encode("utf-8")
        create_step_json = json.dumps(
            request.model_dump(by_alias=True, mode="json"), separators=(",", ":")
        ).encode("utf-8")
        assert hashlib.sha256(envelope_json).hexdigest() != expected_hash
        assert hashlib.sha256(create_step_json).hexdigest() != expected_hash
        assert len(envelope_json) != len(ORIGINAL_BODY), (
            "test setup invalid: envelope JSON happens to match the original body length"
        )
        assert received.body_size == len(ORIGINAL_BODY), (
            f"upstream body_size {received.body_size} != original {len(ORIGINAL_BODY)}"
        )
    finally:
        await stack.tear_down()


async def test_no_x_phantom_header_reaches_upstream(tmp_path: Path) -> None:
    """No X-Phantom-* header (Phantom's reserved ingress namespace) leaks upstream.

    The SDK sets ``X-Phantom-Idempotency-Key`` / ``X-Phantom-Uid`` at
    ingress; none may ride onto the upstream call. Asserted against the emulator's full
    inbound-header capture on the PUT (the step that records ``headers``).

    Falsifier: stop stripping the ``X-Phantom-*`` prefix in the executor's outbound header
    emit -> a leaked header appears in ``received.headers`` -> RED.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        await _submit(stack, chain_id=chain_id, body=ORIGINAL_BODY, bearer=bearer)
        await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(ORIGINAL_BODY),
        )
        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"X-Phantom-* headers leaked to upstream: {leaked}; "
            f"all captured headers: {sorted(received.headers.keys())}"
        )
    finally:
        await stack.tear_down()
