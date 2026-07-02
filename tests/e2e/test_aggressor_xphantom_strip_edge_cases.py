"""Aggressor — X-Phantom-* strip edge cases beyond simple case-mixing.

The defender's round-2 fix added a case-insensitive ``startswith("x-phantom-")``
strip in ``src/phantom-service/src/phantom/chain/executor.py:188-195``. The existing
``test_aggressor_xphantom_headers_isolated_from_upstream`` test pins the
"three title-case source-supplied headers get stripped" base case. These
sub-tests cover edge variants the base case doesn't:

1. **Embedded-Bearer values.** A producer pastes a bearer token into the
   VALUE of an ``X-Phantom-Idempotency-Key`` header. The strip MUST
   fire on the NAME, not the value — Phantom does not inspect header
   values for bearer-looking strings. The whole header (regardless of
   what the value looks like) must NOT reach upstream.

2. **Mixed-case repeated declarations.** A buggy producer sends THREE
   variants of ``X-Phantom-Uid`` on one PUT step:
   - ``X-Phantom-Uid`` (title-case)
   - ``X-PHANTOM-UID`` (all-caps)
   - ``x-phantom-uid`` (lowercase)
   Python dicts merge by case-sensitive keys, so in the
   ``ChainStep.headers`` dict these are THREE distinct entries.
   Each entry must be stripped. Upstream must see NO ``x-phantom-uid``
   header (or any prefix variant).

3. **Extended ``X-Phantom-*`` namespace.** A header named
   ``X-Phantom-Foo-Bar`` (something Phantom doesn't currently use)
   must still be stripped — the contract is the entire reserved
   prefix, not a hardcoded list of known names. This guards against
   the future-expansion case where Phantom adds a new control
   header and a stale producer leaks the old one.

4. **Substring-but-not-prefix.** A header named
   ``X-Custom-Phantom-Trace`` (substring match somewhere in the
   middle) must SURVIVE — the strip is on the prefix, not on any
   occurrence of "x-phantom-". This is the negative case.

5. **Whitespace-padded names.** A header named ``"  X-Phantom-Probe  "``
   (literal leading/trailing whitespace on the name). HTTP technically
   forbids this but producers can be sloppy. The current strip uses
   ``name.lower().startswith(...)``, which returns False for
   space-prefixed names. The behavior to observe: either Phantom
   rejects the envelope at admission (model validation), accepts but
   strips, or accepts and leaks. The test records the observed
   behavior and fails on leak.

These sub-tests use the new full-header capture on the emulator's
``ReceivedEntry.headers`` to assert ground truth directly.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainEnvelope, ChainStep

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
BODY: bytes = b"phantom-aggressor-xphantom-strip-edge-body"
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


def _build_envelope_with_put_headers(
    *,
    chain_id: UUID,
    emulator_url: str,
    extra_put_headers: dict[str, str],
) -> ChainEnvelope:
    """Build a two-step envelope with custom headers injected on the PUT step.

    Mirrors the helper from ``test_aggressor_transparent_proxy_headers``.
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


async def _submit(
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


async def test_aggressor_xphantom_strip_embedded_bearer_value(tmp_path: Path) -> None:
    """X-Phantom-* strip fires on NAME, ignoring the value's content.

    The producer accidentally pastes a bearer string into the value of
    ``X-Phantom-Idempotency-Key``. The strip must remove the entire
    header by name — Phantom does NOT inspect the value for
    bearer-looking content.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        source_supplied_value = "Bearer eyJhbGciOiJIUzI1NiJ9.source-paste-mistake.x"
        extra_headers = {
            # The whole point: a "bearer-shaped" value should NOT
            # protect the header from being stripped. The strip is
            # name-based.
            "X-Phantom-Idempotency-Key": source_supplied_value,
            # And a control header — a legitimate Bearer in Authorization
            # is handled by the substitution path; this should round-trip
            # via the cached token, not this header value.
            "X-Phantom-Uid": "Bearer also-bearer-but-still-stripped",
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=extra_headers,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )

        # Strip invariant: NO x-phantom-* header reaches upstream,
        # regardless of value content.
        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"X-Phantom-* headers leaked through to upstream PUT: {leaked}. "
            f"The strip is name-based; value content (Bearer-shaped) "
            f"should not affect it. Captured: {sorted(received.headers.keys())}"
        )

        # And the source-supplied bearer-shaped values must NOT appear in
        # the emulator's Authorization header (Phantom substitutes
        # Authorization from the token cache, not from source-supplied
        # X-Phantom-* values).
        observed_auth = received.headers.get("authorization", "")
        assert source_supplied_value not in observed_auth, (
            f"X-Phantom-Idempotency-Key value (Bearer-shaped) leaked "
            f"into Authorization: observed={observed_auth!r}, "
            f"source-supplied={source_supplied_value!r}"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_xphantom_strip_mixed_case_repeated(tmp_path: Path) -> None:
    """All three case variants of X-Phantom-Uid are stripped.

    A buggy producer sends three case-variants of the same header. In
    ``ChainStep.headers`` (Python dict, case-sensitive keys), these are
    three distinct entries. The strip must catch all three regardless
    of casing.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        # Three case variants — all should be stripped.
        # Python dicts preserve insertion order; the dict here holds
        # all three (distinct keys despite same lowercased form).
        extra_headers = {
            "X-Phantom-Uid": "title-case-value",
            "X-PHANTOM-UID": "all-caps-value",
            "x-phantom-uid": "lowercase-value",
        }
        # Pre-condition for the test: the dict really has three entries.
        assert len(extra_headers) == 3, "test setup error: case-variant keys collapsed to one entry"
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=extra_headers,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )

        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"X-Phantom-* mixed-case variants leaked to upstream: {leaked}. "
            f"Strip must be case-insensitive on the name's prefix. "
            f"Captured: {sorted(received.headers.keys())}"
        )
        # No leaked VALUE either.
        for variant_value in extra_headers.values():
            for recv_value in received.headers.values():
                assert variant_value not in recv_value, (
                    f"source-supplied X-Phantom-Uid value {variant_value!r} "
                    f"leaked into upstream header: {recv_value!r}"
                )
    finally:
        await stack.tear_down()


async def test_aggressor_xphantom_strip_extended_namespace(tmp_path: Path) -> None:
    """A future ``X-Phantom-Foo-Bar`` header is stripped.

    Pins the contract: the strip fires on the entire ``x-phantom-``
    prefix, not on a hardcoded list of known control header names.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        extra_headers = {
            # Hypothetical future control header.
            "X-Phantom-Foo-Bar": "should-not-leak",
            # An extra-deep name — strip is by prefix only.
            "x-phantom-experimental-route-policy": "alpha",
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=extra_headers,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )

        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"Extended-namespace X-Phantom-* headers leaked: {leaked}. "
            f"Strip must cover the whole `x-phantom-` prefix, not a "
            f"hardcoded list of known control headers. "
            f"Captured: {sorted(received.headers.keys())}"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_xphantom_strip_substring_not_prefix(tmp_path: Path) -> None:
    """Non-prefix ``x-phantom-`` substring matches MUST survive.

    Negative case: a header whose NAME contains ``x-phantom-`` somewhere
    in the middle (``X-Custom-Phantom-Trace``) is NOT a reserved header
    and must round-trip to upstream. The strip is on the prefix only.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        # Substring match somewhere other than at the start. Strip
        # must NOT fire on these.
        keep_value_1 = "source-supplied-custom-trace-value"
        keep_value_2 = "source-supplied-phantom-debug-id"
        extra_headers = {
            "X-Custom-Phantom-Trace": keep_value_1,
            # Underscores are uncommon in HTTP header names but legal
            # at the wire level.
            "X-Source-Phantom-Debug": keep_value_2,
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=extra_headers,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        received = await assert_emulator_received(
            stack.emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY),
        )

        # Each header MUST round-trip to upstream (HTTP header names
        # are case-insensitive; the emulator records lowercased keys).
        assert received.headers.get("x-custom-phantom-trace") == keep_value_1, (
            f"X-Custom-Phantom-Trace was incorrectly stripped or mutated: "
            f"got {received.headers.get('x-custom-phantom-trace')!r}, "
            f"expected {keep_value_1!r}. The strip must fire on prefix "
            f"only, not on `x-phantom-` substring matches."
        )
        assert received.headers.get("x-source-phantom-debug") == keep_value_2, (
            f"X-Source-Phantom-Debug was incorrectly stripped or mutated: "
            f"got {received.headers.get('x-source-phantom-debug')!r}, "
            f"expected {keep_value_2!r}."
        )

        # And no x-phantom-* should appear since none were sent with
        # that prefix.
        leaked = [k for k in received.headers if k.startswith("x-phantom-")]
        assert not leaked, (
            f"Unexpected x-phantom-* headers in upstream PUT: {leaked}. "
            f"None were sent with that prefix; substring matches "
            f"should not produce them."
        )
    finally:
        await stack.tear_down()


async def test_aggressor_xphantom_strip_whitespace_padded_name(tmp_path: Path) -> None:
    """Whitespace-padded ``X-Phantom-*`` header name MUST not stall the chain.

    The current strip uses ``name.lower().startswith("x-phantom-")``,
    which returns False for a name starting with whitespace. The header
    survives the strip; httpx then refuses to send the request because
    the header name is illegal per RFC 7230. Phantom retries the same
    malformed request indefinitely (or until the retry strategy gives
    up — whichever budget comes first).

    Observed failure mode (round-3 round of testing): the chain
    *parks in retry* forever, and operators have no easy escape — the
    producer sent something Phantom can never deliver.

    Three plausible defender fixes (all out of scope for this test):

    1. **Normalize at strip time** — strip whitespace from the header
       name before the prefix check; the malformed-name header is then
       dropped along with all other ``X-Phantom-*`` cases.
    2. **Reject at admission** — Pydantic-validate every step's
       ``headers`` dict for RFC-compliant header names (no whitespace,
       no control chars). Surface a 422 with a clear message.
    3. **Reject at executor** — same as #1 but specifically for
       any header whose name fails ``name.strip() == name``;
       log + skip with a metric so operators can see the count.

    The test pins one load-bearing invariant: a chain submitted with
    a whitespace-padded X-Phantom-* header must NOT consume retry
    budget forever. Either:

    - Admission rejects the envelope (4xx at submit time), OR
    - The chain reaches a TERMINAL state (succeeded after strip, OR
      failed after the retry strategy gives up) within
      ``TERMINAL_BUDGET_SECONDS``.

    Today (round-3-start) this test FAILS because the chain stalls
    in retry loop, never reaching a terminal state. ``poller``
    eventually raises ``PollDeadlineExceeded``.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                # Cap retries so the chain reaches `failed` quickly
                # rather than retrying indefinitely. The bug under
                # observation is "the chain never terminates" — by
                # bounding attempts we make the failure-mode signal
                # cleanly: success (strip fired) vs failed (httpx
                # rejected each attempt) vs stuck (the bug).
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": [0, 1, 1],
                    "max_attempts": 3,
                },
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        sentinel_value = "whitespace-padded-source-target-value"
        # Whitespace-padded name. Pydantic dict[str, str] accepts any
        # string keys including those with leading/trailing whitespace.
        extra_headers = {
            "  X-Phantom-Probe  ": sentinel_value,
        }
        envelope = _build_envelope_with_put_headers(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            extra_put_headers=extra_headers,
        )

        # Submission may succeed (Phantom 202s and parks the chain)
        # or fail with a 4xx (admission rejected).
        admission_rejected = False
        try:
            await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)
        except Exception:
            admission_rejected = True

        if admission_rejected:
            # Admission rejected the envelope — that's an acceptable
            # defensive behavior. Test passes.
            return

        # The chain MUST reach a terminal state within budget.
        # Terminal states: succeeded (strip fired correctly), failed
        # (retries exhausted), auth_expired (irrelevant here).
        # If the chain never progresses past `queued`, the bug is
        # surfaced.

        # Per `phantom.storage.interface.TERMINAL_STATES`: succeeded,
        # failed, stored, cancelled, corrupted, expired. `auth_expired`
        # is NOT terminal (auth-kicker re-queues it). For this test, ANY
        # of these terminal landings is acceptable — the bug we pin is
        # "chain stuck retrying forever", not "chain failed gracefully".
        async def _reached_terminal() -> bool:
            row = await pc.get_upload(chain_id)
            return row.state in {
                "succeeded",
                "failed",
                "stored",
                "cancelled",
                "corrupted",
                "expired",
            }

        try:
            await await_until(
                _reached_terminal,
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
                message=(
                    f"chain {chain_id} never reached a terminal state "
                    f"within {TERMINAL_BUDGET_SECONDS}s — Phantom is "
                    f"stuck retrying a malformed-header request. "
                    f"Either: (a) the X-Phantom-* strip should "
                    f"normalize whitespace before the prefix check, "
                    f"OR (b) admission should reject the envelope at "
                    f"submit time with a 422."
                ),
            )
        except AssertionError as exc:
            # Make the bug signal explicit in the test name.
            row = await pc.get_upload(chain_id)
            assert False, (  # noqa: B011
                f"BUG: chain {chain_id} stuck in state={row.state!r} "
                f"after {row.attempts} attempts. Phantom does not "
                f"terminate when a source-supplied X-Phantom-* header "
                f"with whitespace-padded name is unacceptable to httpx. "
                f"Original deadline-exceeded: {exc}"
            )

        # If the chain succeeded, verify no leak. If it failed,
        # there's no upstream emulator entry to check.
        row = await pc.get_upload(chain_id)
        if row.state == "succeeded":
            received = await assert_emulator_received(
                stack.emulator,
                phantom_local_uuid=str(chain_id),
                body_size=len(BODY),
            )
            for value in received.headers.values():
                assert value != sentinel_value, (
                    f"BUG: whitespace-padded X-Phantom-Probe header "
                    f"value {sentinel_value!r} LEAKED to upstream as "
                    f"{value!r}."
                )
    finally:
        await stack.tear_down()
