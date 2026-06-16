"""Lower-tier admin read + token surfaces over the wire (TEST-4/5/7).

These admin verbs were pinned at the contract/unit tier but had no e2e.
Adding the missing full-stack coverage surfaced a coherent cluster of
SDK-vs-server WIRE mismatches in client-facing methods that no test ever
exercised end to end: the contract tests hit the raw routes with
``TestClient.get(...)`` and never called the SDK methods against a real
server, so the SDK response models had drifted from the server shapes
undetected. Three of the four cases below (R-EX2 / R-EX3 / R-EX4) were
real wire-contract defects, now FIXED; all four drive the SDK against a
real listener so any future SDK<->server drift fails here.

* TEST-4 - ``fetch_bundle`` -> R-EX2 (fixed). The server route
  ``GET /chains/{id}/bundle`` returns
  ``{"metadata": ..., "body_refs": {name: <hex>}}``. The SDK
  ``UploadBundle`` model used to require ``{"metadata": ..., "body":
  <bytes>}`` and could not parse the response; it now mirrors the
  server's ``body_refs`` name -> bytes map (decoding the wire hex), the
  richer shape that keeps every named ref distinct.

* TEST-5 - ``invalidate_token`` -> R-EX3 (fixed). The SDK docstring
  promises "Mark the (endpoint, uid) slot as bad (status=bad)" and cites
  ADR-003 ("bad tokens are preserved, not deleted"). The server route
  ``DELETE /tokens/{endpoint}/{uid}`` used to HARD-DELETE the slot so it
  vanished from ``list_tokens``; it now honors ADR-003 - marks the slot
  ``bad`` and PRESERVES it (``token_cache.mark_bad``), matching the SDK
  contract.

* TEST-7 - split:
  - ``get_instance_status`` -> PASSES. Real new coverage: a single
    instance's status returns its id + readiness over the wire.
  - ``list_instances`` -> R-EX4 (fixed). The server route used to return
    a bare JSON array while the SDK ``_InstanceListResponse`` expects an
    envelope ``{"instances": [...]}``, so the SDK could not parse the
    array. The route now returns the envelope (an ``InstanceListResponse``
    response model), matching the SDK and the ``/chains`` + ``/tokens``
    list convention.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# Distinctive body so the bundle round-trip assertion is meaningful.
BUNDLE_BODY_BYTES: bytes = b"phantom-admin-bundle-e2e-distinct-body-bytes"

# The suite's default instance id (see tests/e2e/phantom-config.yml).
DEFAULT_INSTANCE_ID: str = "primary"

# Shared sub for the seeded uploads.
SHARED_SUB: str = "00000000-0000-0000-0000-000000000457"

# A bearer to push then invalidate. Any non-empty string works - the
# slot status, never the value, is what the admin surface reports.
PUSHED_BEARER: str = "e2e-token-to-invalidate"

# Budget for a seeded upload to park in auth_expired (body retained).
PARK_BUDGET_SECONDS: float = 20.0


async def _seed_parked_upload(stack: E2EStack, *, file_name: str) -> UUID:
    """Submit one real upload and park it in ``auth_expired`` (body on disk)."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=file_name)
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={BODY_REF_NAME: BUNDLE_BODY_BYTES},
        uid=SHARED_SUB,
        auth_token=f"Bearer {stack.fake_security_token(sub=SHARED_SUB)}",
    )
    await assert_chain_reaches_state(
        stack.phantom_client,
        chain_id,
        state="auth_expired",
        timeout_seconds=PARK_BUDGET_SECONDS,
    )
    return chain_id


async def test_fetch_bundle_returns_metadata_and_body() -> None:
    """fetch_bundle returns the row metadata plus the submitted body bytes (TEST-4).

    Wire-contract regression line for R-EX2 (fixed): the SDK
    ``UploadBundle`` now mirrors the server's ``body_refs`` name -> bytes
    map (decoding the wire hex), so a real round-trip yields the exact
    submitted body under its declared ref name.
    """
    stack = await boot_stack(
        config_overrides={
            # Passthrough codec so the stored body bytes equal the
            # submitted bytes for a verbatim comparison.
            "storage": {"compression": {"mode": "always", "algorithm": "original"}},
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        # 401 so the upload parks in auth_expired with its body retained.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            ),
        )

        chain_id = await _seed_parked_upload(stack, file_name="bundle-roundtrip")
        emulator.clear_failures()

        bundle = await pc.fetch_bundle(chain_id)
        assert bundle.metadata.chain_id == chain_id, (
            f"bundle metadata chain_id {bundle.metadata.chain_id} != requested {chain_id}"
        )
        assert bundle.body_refs.get(BODY_REF_NAME) == BUNDLE_BODY_BYTES, (
            "bundle body_refs do not carry the submitted body under its ref name "
            f"(got {sorted(bundle.body_refs)} with "
            f"{len(bundle.body_refs.get(BODY_REF_NAME, b''))} bytes, "
            f"expected key {BODY_REF_NAME!r} with {len(BUNDLE_BODY_BYTES)} bytes)"
        )
    finally:
        await stack.tear_down()


async def test_invalidate_token_marks_slot_bad() -> None:
    """invalidate_token transitions a pushed slot to status='bad' (TEST-5).

    Wire-contract regression line for R-EX3 (fixed): the server route now
    honors ADR-003 - it marks the slot ``bad`` and PRESERVES it rather
    than hard-deleting, so ``list_tokens`` still surfaces the slot with
    ``status='bad'`` and the SDK ``invalidate_token`` contract holds.
    """
    stack = await boot_stack()
    try:
        pc = stack.phantom_client
        endpoint = urlparse(stack.emulator_url).hostname or ""
        assert endpoint, "could not derive the emulator endpoint hostname"

        # Push a bearer into the (endpoint, uid) slot, then read it back -
        # a freshly pushed slot is fresh/unknown, never bad yet.
        await pc.push_token(endpoint=endpoint, uid=SHARED_SUB, token=PUSHED_BEARER)
        before = [s for s in await pc.list_tokens(endpoint=endpoint) if s.uid == SHARED_SUB]
        assert before, f"no token slot for uid={SHARED_SUB} after push"
        assert before[0].status in {"fresh", "unknown"}, (
            f"freshly pushed slot status={before[0].status!r}, expected fresh/unknown"
        )

        # Invalidate it (the inverse of push) - the slot must persist as
        # bad, not vanish (ADR-003 keeps bad tokens).
        await pc.invalidate_token(endpoint=endpoint, uid=SHARED_SUB)
        after = [s for s in await pc.list_tokens(endpoint=endpoint) if s.uid == SHARED_SUB]
        assert after, (
            f"token slot for uid={SHARED_SUB} vanished after invalidate; "
            "ADR-003 keeps bad tokens (status=bad), it does not delete them"
        )
        assert after[0].status == "bad", (
            f"slot status after invalidate={after[0].status!r}, expected 'bad'"
        )
    finally:
        await stack.tear_down()


async def test_get_instance_status_reports_id_and_ready() -> None:
    """get_instance_status returns one instance's id + readiness (TEST-7, passing)."""
    stack = await boot_stack()
    try:
        status = await stack.phantom_client.get_instance_status(DEFAULT_INSTANCE_ID)
        assert status.id == DEFAULT_INSTANCE_ID, (
            f"instance status id {status.id!r} != requested {DEFAULT_INSTANCE_ID!r}"
        )
        assert status.ready is True, (
            "the instance status should report ready on a freshly booted, healthy stack"
        )
    finally:
        await stack.tear_down()


async def test_list_instances_surfaces_configured_set() -> None:
    """list_instances surfaces the configured instance set over the wire (TEST-7).

    Wire-contract regression line for R-EX4 (fixed): the server route now
    returns an ``{"instances": [...]}`` envelope matching the SDK
    ``list_instances`` model, so the SDK parses the configured set.
    """
    stack = await boot_stack()
    try:
        instances = await stack.phantom_client.list_instances()
        ids = {summary.id for summary in instances}
        assert DEFAULT_INSTANCE_ID in ids, (
            f"list_instances did not surface the configured instance "
            f"{DEFAULT_INSTANCE_ID!r}; got {ids}"
        )
    finally:
        await stack.tear_down()
