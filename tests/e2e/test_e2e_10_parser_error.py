"""E2E-10 — Parser-error propagation (cross-protocol error path).

Verifies the end-to-end shape of Phantom's parser-rejection path:
the service emits an :class:`phantom.models.errors.ErrorEnvelope`,
phantom-client parses the body and raises the typed
:class:`PhantomValidationError`, with ``error_code`` and ``details``
preserved. The test bypasses the driver (which only builds
correctly-shaped envelopes) and constructs the malformed payload at
the phantom-client layer directly — that's the cross-protocol
contract: the SDK is the layer that crafts envelopes.

Sub-case A (``body_ref_missing``) exercises the parser's
declared-vs-provided cross-check. Sub-case B
(``body_ref_too_large``) overrides ``storage.max_buffered_bytes``
to a low cap so a small synthetic body trips the per-upload limit
without needing a multi-GiB payload.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from phantom_client import (
    ChainBodyRef,
    ChainEnvelope,
    ChainStep,
    PhantomClient,
)
from phantom_client.errors import PhantomPayloadTooLargeError, PhantomValidationError

from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, boot_stack

# Body-ref name declared by the envelope but deliberately not
# provided in the submit_chain call's body_refs dict. The parser
# should reject with ``error.code == "body_ref_missing"`` and
# surface this name in ``details["missing"]``.
DECLARED_BUT_MISSING_REF: str = "intentionally_missing_body"

# A different body_ref name provided in submit_chain's body_refs
# dict. Without at least one entry in body_refs, the SDK encodes
# the request as JSON (not multipart), bypassing the parser's
# cross-check entirely — the body_ref_missing path lives in the
# multipart parser only. Providing this orphan name forces multipart
# encoding so the declared-missing path engages.
ORPHAN_REF_NAME: str = "unrelated_provided_body"

# Tiny placeholder bytes for the orphan ref.
ORPHAN_REF_BYTES: bytes = b"unrelated"

# The envelope target URL. We point at the live emulator just so the
# envelope passes basic URL validation; nothing should ever reach the
# emulator because the parser rejects synchronously.
TEST_PATH: str = "/v2/files"


pytestmark = pytest.mark.e2e


async def test_e2e_10_body_ref_missing_raises_validation_error(
    stack: E2EStack,
    phantom_client: PhantomClient,
) -> None:
    """submit_chain with a missing body_ref raises validation (sub-case A).

    Phantom's parser cross-checks every ``ChainBodyRef.name`` in the
    envelope against the names provided in the multipart
    ``body_refs[<name>]`` parts. A declared ref without a matching
    part is a ``body_ref_missing`` error (HTTP 422). The SDK
    translates that into :class:`PhantomValidationError`; the test
    asserts on the typed exception class, the error code, and the
    presence of the missing name in ``details``.
    """
    chain_id = uuid4()
    step = ChainStep(
        name="single_step",
        method="POST",
        url=stack.emulator_url.rstrip("/") + TEST_PATH,
        headers={"Content-Type": "application/octet-stream"},
        body=ChainBodyRef(
            kind="body_ref",
            name=DECLARED_BUT_MISSING_REF,
            content_type="application/octet-stream",
        ),
        capture=[],
        idempotency_header=None,
    )
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[step],
        default_target=None,
    )

    # Pass a non-empty body_refs map with an UNRELATED ref name. The
    # SDK encodes the request as multipart (any non-empty dict
    # triggers the multipart path), and the parser's cross-check
    # fires on the mismatch between declared and provided names.
    with pytest.raises(PhantomValidationError) as exc_info:
        await phantom_client.submit_chain(
            envelope,
            body_refs={ORPHAN_REF_NAME: ORPHAN_REF_BYTES},
            uid=DEFAULT_FAKE_SUB,
            auth_token=f"Bearer {stack.fake_security_token()}",
        )

    err = exc_info.value
    assert err.status_code == 422, f"expected HTTP 422; got {err.status_code}"
    assert err.error_code == "body_ref_missing", (
        f"expected error_code='body_ref_missing'; got {err.error_code!r}"
    )
    # The parser surfaces the missing ref names in details.missing.
    missing = err.details.get("missing")
    assert isinstance(missing, list), f"expected details.missing to be a list; got {missing!r}"
    assert DECLARED_BUT_MISSING_REF in missing, (
        f"expected {DECLARED_BUT_MISSING_REF!r} in details.missing; got {missing!r}"
    )


# Body-ref cap for sub-case B. Set well below the synthetic body so the
# parser trips the per-upload limit on a small payload (keeping the suite
# fast). The route reads this value via the get_max_buffered_bytes
# dependency, threading it into starlette's multipart max_part_size and
# the post-parse buffered-bytes check.
_BODY_REF_TOO_LARGE_CAP_BYTES: int = 4 * 1024  # 4 KiB
# Synthetic body exceeding the cap. 64 KiB is well over the 4 KiB cap and
# fits comfortably under starlette's defaults so the failure point is
# unambiguously Phantom's buffered-bytes check.
_OVERSIZED_BODY_BYTES: int = 64 * 1024  # 64 KiB


@pytest_asyncio.fixture
async def low_cap_stack() -> AsyncIterator[E2EStack]:
    """Boot a stack with a small ``max_buffered_bytes`` cap.

    Yields a stack where ``storage.max_buffered_bytes`` is overridden
    to :data:`_BODY_REF_TOO_LARGE_CAP_BYTES` so sub-case B can trip the
    cap with a small synthetic payload.
    """
    s = await boot_stack(
        config_overrides={
            "storage": {"max_buffered_bytes": _BODY_REF_TOO_LARGE_CAP_BYTES},
        },
    )
    try:
        yield s
    finally:
        await s.tear_down()


async def test_e2e_10_body_ref_too_large_raises_validation_error(
    low_cap_stack: E2EStack,
) -> None:
    """submit_chain with an oversized body_ref raises body_too_large (sub-case B).

    Phase 2 § 3.2.3 (H2 audit closure) changed the oversized-body
    contract: instead of routing to ``body_ref_missing`` (HTTP 422)
    after the body landed in RAM, the service now precheck'es
    ``Content-Length`` and rejects with ``body_too_large`` (HTTP 413)
    before any body bytes are read. The SDK exposes
    :class:`PhantomPayloadTooLargeError` for that path.
    """
    chain_id = uuid4()
    ref_name = "oversized_body"
    step = ChainStep(
        name="single_step",
        method="POST",
        url=low_cap_stack.emulator_url.rstrip("/") + TEST_PATH,
        headers={"Content-Type": "application/octet-stream"},
        body=ChainBodyRef(
            kind="body_ref",
            name=ref_name,
            content_type="application/octet-stream",
        ),
        capture=[],
        idempotency_header=None,
    )
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[step],
        default_target=None,
    )
    with pytest.raises(PhantomPayloadTooLargeError) as exc_info:
        await low_cap_stack.phantom_client.submit_chain(
            envelope,
            body_refs={ref_name: b"x" * _OVERSIZED_BODY_BYTES},
            uid=DEFAULT_FAKE_SUB,
            auth_token=f"Bearer {low_cap_stack.fake_security_token()}",
        )

    err = exc_info.value
    # H2 closure: 413 with ``body_too_large`` — Content-Length precheck
    # fires before body bytes are read.
    assert err.status_code == 413, f"expected HTTP 413; got {err.status_code}"
    assert err.error_code == "body_too_large", (
        f"expected error_code='body_too_large'; got {err.error_code!r}"
    )
    # The PhantomValidationError import is retained — sub-case A still
    # uses it for the body_ref_missing path.
    assert PhantomValidationError is not None
