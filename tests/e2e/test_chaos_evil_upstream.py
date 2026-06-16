"""Operational-chaos E2E: an evil upstream is ridden out, byte-identity intact (§ 5.C).

Adversary 2, the hostile-upstream chaos. Phantom buffers and retries against an
upstream that misbehaves; the transparent-proxy invariant (the body delivered is
SHA-256-identical to the buffered upload) must survive every retry. This file
covers the evil-upstream angles the emulator can express that are NOT already
proven elsewhere:

* :func:`test_slow_upstream_eventually_delivers_byte_identical` - a high-latency
  upstream (``latency_ms``) does not lose or corrupt the upload: the chain rides
  out the slow PUT and delivers, and the emulator's accepted body hashes equal the
  submitted bytes.
* :func:`test_flapping_upstream_retries_through_and_delivers` - an intermittent
  failure (partial ``error_rate_5xx``) is ridden out by the retry loop; after the
  flap clears the chain reaches ``succeeded`` exactly once, byte-identical.

COVERED-BY-EXISTING (deliberately NOT duplicated here, logged in execution § 5.C):
* 500-FOREVER -> ``stored``: the persistent-5xx-plus-capture-expiry path (ADR-011,
  ``capture_reexecution=false``) is proven by
  ``tests/e2e/test_e2e_15_stored_admin_flow.py`` (a short capture TTL + a 5xx PUT
  routes the row to ``stored``). A bare persistent 5xx without capture expiry stays
  in the retry loop (``queued`` / ``attempting``); the row only LEAVES that loop via
  capture expiry, which 15 already covers.
* 5xx-then-recovery (one transient outage window): proven by
  ``tests/e2e/test_e2e_03_s3_down.py`` (step-2 503 until cleared, then one success,
  step 1 not re-executed).

DROPPED per F-12 (no emulator knob): 429 + Retry-After (the ``FailurePolicy`` has
no 429 / Retry-After field), TLS / DNS failure, and clock skew. These are
upstream-network failure modes the retry path already treats as generic transient
failures, covered by the 503 / slow / timeout knobs above.

Public e2e-light lane (§ 5.0): generic ``submit`` shapes + the emulator.

Falsifier: break the retry loop (or the storage/forward hash check) -> a slow or
flapping upstream drops the upload or the delivered body hash diverges -> RED.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import ChainResponse, PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

pytestmark = pytest.mark.e2e

_DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"
# Body big enough that a truncation/corruption would change the hash, small enough
# to stay fast. Random so a codec cannot dedup-cheat the byte-identity check.
_BODY_BYTES = 64 * 1024
# A slow PUT: long enough to prove buffering rides it out, short enough for the
# suite budget. The synthetic 202 returns immediately regardless.
_SLOW_LATENCY_MS = 1_500
# Flap probability: ~half the PUTs 503 until cleared, so the retry loop must
# survive several intermittent failures before the clear.
_FLAP_5XX_RATE = 0.5
# Generous terminal budget covering the slow PUT + a couple of retry intervals.
_TERMINAL_BUDGET_SECONDS = 30.0
# Window during which the flap is left active before clearing it.
_FLAP_WINDOW_SECONDS = 2.5


def _sha256(data: bytes) -> str:
    """Return the SHA-256 hex digest of ``data``."""
    return hashlib.sha256(data).hexdigest()


async def _submit(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID | None = None,
) -> tuple[UUID, ChainResponse]:
    """Submit one upload-shaped two-step chain; return ``(chain_id, response)``."""
    chain_id = chain_id or uuid4()
    request = build_create_file_request(file_name=f"evil_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    response = await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=_DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id, response


async def test_slow_upstream_eventually_delivers_byte_identical(tmp_path: Path) -> None:
    """A slow (high-latency) upstream is ridden out; the delivered body is identical."""
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # Throttle the PUT step with a large pre-handler sleep.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=_SLOW_LATENCY_MS,
            ),
        )

        body = secrets.token_bytes(_BODY_BYTES)
        chain_id, _ = await _submit(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
            body=body,
        )

        # Despite the slow PUT, the chain delivers and the emulator's accepted
        # body is byte-identical to what was buffered.
        delivered = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )
        assert delivered.state == "succeeded"
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(body),
        )
        assert received.body_hash == _sha256(body), (
            "delivered body hash diverged from the buffered upload (transparent-proxy "
            "invariant broken under a slow upstream)"
        )
    finally:
        await stack.tear_down()


async def test_flapping_upstream_retries_through_and_delivers(tmp_path: Path) -> None:
    """Intermittent 5xx is ridden out by the retry loop; one byte-identical delivery.

    Installs a partial ``error_rate_5xx`` so the PUT fails on some attempts, leaves
    it active briefly, then clears it. The retry loop must survive the flap and
    deliver exactly one byte-identical body (no duplicate successful PUT, no loss).
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # Deterministic flap: seed the emulator RNG so the coin-flips repeat.
        stack.emulator.set_seed(1234)
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                error_rate_5xx=_FLAP_5XX_RATE,
            ),
        )

        body = secrets.token_bytes(_BODY_BYTES)
        chain_id, _ = await _submit(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
            body=body,
        )

        # Let the flap fail a few attempts, then clear it so a later retry lands.
        await asyncio.sleep(_FLAP_WINDOW_SECONDS)  # pre-commit-allow: sleep
        stack.emulator.clear_failures()

        delivered = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )
        assert delivered.state == "succeeded"

        # Exactly one SUCCESSFUL PUT body recorded for this chain, byte-identical.
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(body),
        )
        assert received.body_hash == _sha256(body), (
            "delivered body hash diverged after a flapping upstream"
        )
        successful = [
            e
            for e in stack.emulator.received()
            if e.metadata_kvs.get("phantom_local_uuid") == str(chain_id)
        ]
        assert len(successful) == 1, (
            f"expected exactly one accepted body after the flap; got {len(successful)}"
        )
    finally:
        await stack.tear_down()
