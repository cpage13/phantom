"""Aggressor — transparent-proxy header invariants.

The sharpened transparent-proxy requirement: the upstream sees what
the producer would have sent if Phantom didn't exist. Bytes AND headers
are byte-identical, with one documented exception (Authorization is
substituted with the cached token).

The existing `test_transparent_proxy.py` covers body byte-equality
across codecs, JSON/multipart shapes, retries, and persist transitions.
It does NOT cover:

- Arbitrary custom header round-tripping (X-Custom-* / X-Request-Id).
- Phantom-header isolation (X-Phantom-* MUST NOT leak to upstream).
- User-Agent preservation (upstream should NOT see httpx/<version>).
- Authorization substitution (the documented mutation point).
- Header case preservation (values are case-sensitive even when names
  are not).

This file fills those gaps using the emulator's `/v1/files/upload/...`
PUT endpoint which records `x-amz-meta-*` headers in its received-log.
For non-meta headers we use the create-file endpoint's metadata KVS
echo to pin custom values.

A representative header set is small: Authorization, Content-Type,
Accept, Accept-Encoding, plus httpx defaults. But producers and
upstream-adjacent tooling DO inject X-Request-Id / X-Correlation-Id
opportunistically, and the transparent-proxy invariant must cover
those — otherwise Phantom turns into a header rewriter.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainEnvelope, ChainStep

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
BODY: bytes = b"phantom-aggressor-headers-body"
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


def _build_envelope_with_put_headers(
    *,
    chain_id: UUID,
    emulator_url: str,
    extra_put_headers: dict[str, str],
) -> ChainEnvelope:
    """Build a two-step envelope with custom headers injected on the PUT.

    Returns the envelope; caller adds body_refs and submits.
    """
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    put_step = envelope.steps[-1]
    merged_headers = {**put_step.headers, **extra_put_headers}
    new_put = ChainStep(
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
        steps=[envelope.steps[0], new_put],
        default_target=envelope.default_target,
    )


async def _submit_envelope(
    pc: PhantomClient,
    envelope: ChainEnvelope,
    *,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit an envelope with the standard body_ref."""
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_aggressor_xamzmeta_custom_headers_preserved(tmp_path: Path) -> None:
    """Custom `x-amz-meta-*` headers on the PUT step are byte-identical upstream.

    The emulator's PUT path captures every `x-amz-meta-*` header in
    ``ReceivedEntry.x_amz_meta_headers``. We add several custom keys
    on the PUT step and assert they survive byte-equal.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        custom_headers = {
            "x-amz-meta-ref-id": "1d2e3f4a-5b6c-7d8e-9f01-234567890abc",
            "x-amz-meta-uploader-id": "12345",
            "x-amz-meta-label": "alpha",
            "x-amz-meta-tracker": "source-supplied-tracker-value",
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=custom_headers,
        )
        await _submit_envelope(
            pc,
            envelope,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )

        # Wait for chain to succeed.
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )
        # Each custom header must surface in the emulator's x-amz-meta map.
        for key, value in custom_headers.items():
            recv_value = received.x_amz_meta_headers.get(key.lower())
            assert recv_value == value, (
                f"header {key!r} round-trip failed: sent={value!r}, received={recv_value!r}"
            )
    finally:
        await stack.tear_down()


async def test_aggressor_xphantom_headers_isolated_from_upstream(
    tmp_path: Path,
) -> None:
    """X-Phantom-* headers MUST NOT leak through to the upstream PUT.

    The producer (this test) accidentally adds X-Phantom-Idempotency-Key
    on the PUT step's headers. Phantom MUST strip any X-Phantom-* header
    from outbound to upstream — these are Phantom's internal namespace.

    Round-2 (post-defender): the emulator now captures the FULL inbound
    header set, so absence of ``x-phantom-*`` headers is asserted
    directly at the emulator boundary rather than inferred from the
    narrower ``x-amz-meta-*`` slice.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        custom_headers = {
            "X-Phantom-Idempotency-Key": "bogus-idempotency-key-injection",
            "X-Phantom-Uid": "bogus-uid-override",
            "X-Phantom-Probe": "bogus-reserved-prefix-value",
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=custom_headers,
        )
        await _submit_envelope(
            pc,
            envelope,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )

        # The chain must still succeed — Phantom should accept the
        # envelope and (per the transparent-proxy invariant) forward
        # only the non-X-Phantom headers.
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )
        # Round-2 strict assertion: full-header capture proves no
        # x-phantom-* header reached the upstream PUT.
        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"X-Phantom-* headers leaked through to upstream PUT: {leaked}. "
            f"All captured headers: {sorted(received.headers.keys())}"
        )
        # And the older x-amz-meta scan still holds: no x-phantom-*
        # leaked under the x-amz-meta-* namespace either.
        for recv_key, recv_value in received.x_amz_meta_headers.items():
            if recv_key == "x-amz-meta-phantom-local-uuid":
                continue
            assert recv_value not in custom_headers.values(), (
                f"X-Phantom-* header VALUE leaked through as x-amz-meta header: "
                f"{recv_key}={recv_value!r}"
            )
            assert "x-phantom" not in recv_key.lower(), (
                f"X-Phantom-* header NAME leaked through as x-amz-meta header: {recv_key}"
            )
    finally:
        await stack.tear_down()


async def test_aggressor_x_amz_meta_case_preservation(tmp_path: Path) -> None:
    """`x-amz-meta-*` header VALUES are case-preserved.

    HTTP header NAMES are case-insensitive, but VALUES must round-trip
    byte-equal (case included). Submit a header with mixed-case value
    and assert the emulator's received value matches case-exactly.

    Round-2 (post-defender): also assert ``User-Agent`` and a custom
    ``x-source-correlation-id`` survive byte-equal via the new full-header
    capture. The transparent-proxy invariant covers arbitrary header
    names, not just ``x-amz-meta-*``.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        custom_value = "MixedCase-VALUE-with-Special-Chars_AND_digits-12345"
        custom_user_agent = "client-agent/2.7.18 (source-01)"
        custom_corr_id = "corr-2026-05-15-AB-cd"
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers={
                "x-amz-meta-case-test": custom_value,
                "User-Agent": custom_user_agent,
                "x-source-correlation-id": custom_corr_id,
            },
        )
        await _submit_envelope(
            pc,
            envelope,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )
        recv_value = received.x_amz_meta_headers.get("x-amz-meta-case-test")
        assert recv_value == custom_value, (
            f"x-amz-meta-* header value case not preserved: "
            f"sent={custom_value!r}, received={recv_value!r}"
        )
        # Round-2 strict: arbitrary non-meta headers also round-trip
        # via the new full-header capture.
        assert received.headers.get("user-agent") == custom_user_agent, (
            f"User-Agent not preserved: "
            f"sent={custom_user_agent!r}, received={received.headers.get('user-agent')!r}"
        )
        assert received.headers.get("x-source-correlation-id") == custom_corr_id, (
            f"x-source-correlation-id not preserved: "
            f"sent={custom_corr_id!r}, "
            f"received={received.headers.get('x-source-correlation-id')!r}"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_authorization_substitution_uses_cached_token(
    tmp_path: Path,
) -> None:
    """Authorization is substituted with the cached token, not the source-supplied one.

    This is the one documented exception to the transparent-proxy
    invariant. The producer submits with `Bearer T1`; Phantom caches T1
    under (endpoint, uid). The operator then pushes T2 via admin.
    A subsequent successful upload should use T2 — not T1 — because
    Phantom owns the freshest token in the slot.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 100,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        t1 = stack.fake_security_token(extra_claims={"token_version": "T1"})
        t2 = stack.fake_security_token(extra_claims={"token_version": "T2"})
        assert t1 != t2

        # Submit chain A with T1. Wait for succeeded. Slot now has T1.
        chain_a = uuid4()
        request_a = build_create_file_request(file_name=f"e2e_{chain_a.hex[:12]}")
        request_a.metadata.key_value_store["phantom_local_uuid"] = str(chain_a)
        envelope_a, _ = build_in_memory_upload_envelope(
            request=request_a,
            files_api_base=stack.emulator_url,
            local_uuid=chain_a,
        )
        await pc.submit_chain(
            envelope_a,
            body_refs={"body": BODY},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {t1}",
        )
        await assert_chain_reaches_state(
            pc,
            chain_a,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )

        # Push T2 to overwrite the cached T1.
        emulator_host = urlparse(stack.emulator_url).hostname or ""
        await pc.push_token(
            endpoint=emulator_host,
            uid=DEFAULT_SUB,
            token=t2,
        )

        # Clear emulator log so we observe ONLY chain B's PUT.
        stack.emulator.clear_received()

        # Submit chain B with NO auth_token (Phantom should pull from
        # the cache slot which now holds T2). The submission goes
        # through; the upstream's Authorization MUST be T2.
        chain_b = uuid4()
        request_b = build_create_file_request(file_name=f"e2e_{chain_b.hex[:12]}")
        request_b.metadata.key_value_store["phantom_local_uuid"] = str(chain_b)
        envelope_b, _ = build_in_memory_upload_envelope(
            request=request_b,
            files_api_base=stack.emulator_url,
            local_uuid=chain_b,
        )
        await pc.submit_chain(
            envelope_b,
            body_refs={"body": BODY},
            uid=DEFAULT_SUB,
            auth_token=None,  # rely on cache slot (T2)
        )
        # Chain should succeed using the cached T2.
        detail_b = await assert_chain_reaches_state(
            pc,
            chain_b,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        assert detail_b.state == "succeeded"

        # Round-2 strict assertion: the emulator's full-header capture
        # proves the Authorization byte-equals the cached T2 token, not
        # the originally-submitted T1. The slot stores the raw bearer
        # value as pushed via admin (NO ``Bearer `` prefix); the
        # executor sets ``Authorization = slot.bearer`` directly.
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_b),
            body_size=len(BODY),
        )
        observed_auth = received.headers.get("authorization")
        assert observed_auth == t2, (
            f"Authorization not substituted with cached T2: "
            f"observed={observed_auth!r}, expected={t2!r}"
        )
        assert observed_auth != t1, (
            "Authorization should NOT carry the originally-submitted T1 — "
            "the cache slot substitution invariant is violated"
        )

        slots = await pc.list_tokens(endpoint=emulator_host)
        matching = [s for s in slots if s.uid == DEFAULT_SUB]
        assert matching, f"no slot for uid={DEFAULT_SUB}"
        assert matching[0].status in {"fresh", "unknown"}, (
            f"after successful chain B, slot status should be fresh/unknown; "
            f"got {matching[0].status!r}"
        )
    finally:
        await stack.tear_down()
