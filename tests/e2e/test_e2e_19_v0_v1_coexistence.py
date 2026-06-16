"""E2E-19 — v0 vs v1 token coexistence (same endpoint, two slots).

Submits two uploads against the same upstream endpoint but with two
different bearer tokens — v0-shape JWT (``sub`` is a numeric id) and
v1-shape JWT (``sub`` is an Azure AD app object id) per ADR-001. The
token cache should produce two distinct ``(endpoint, uid)`` slots,
each carrying its own bearer.

See ADR-002 (cache key) for the v0/v1 framing.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

# v0 sub identity — numeric id as JWT ``sub``.
V0_SUB: str = "operator-12345"

# v1 sub identity — service-principal UUID as JWT ``sub``.
V1_SUB: str = "00000000-0000-0000-0000-000000000019"

# Distinct body bytes per upload — diagnostic correlation only.
V0_BODY: bytes = b"phantom-e2e-coexistence-v0-body"
V1_BODY: bytes = b"phantom-e2e-coexistence-v1-body"


@pytest.mark.e2e
async def test_e2e_19_v0_v1_coexistence() -> None:
    """Two bearers against the same endpoint produce two cache slots."""
    stack = await boot_stack()
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # Mint two distinct bearers — same endpoint, different sub.
        v0_bearer = stack.fake_security_token(sub=V0_SUB)
        v1_bearer = stack.fake_security_token(sub=V1_SUB)

        # Submit one envelope per identity.
        chain_v0 = uuid4()
        await _submit_via_phantom_client(
            pc,
            chain_id=chain_v0,
            body=V0_BODY,
            uid=V0_SUB,
            bearer=v0_bearer,
            emulator_url=stack.emulator_url,
        )
        chain_v1 = uuid4()
        await _submit_via_phantom_client(
            pc,
            chain_id=chain_v1,
            body=V1_BODY,
            uid=V1_SUB,
            bearer=v1_bearer,
            emulator_url=stack.emulator_url,
        )

        # Both reach succeeded.
        await assert_chain_reaches_state(
            pc,
            chain_v0,
            state="succeeded",
            timeout_seconds=10.0,
        )
        await assert_chain_reaches_state(
            pc,
            chain_v1,
            state="succeeded",
            timeout_seconds=10.0,
        )

        # Two distinct cache slots, both on the same endpoint.
        slots = await pc.list_tokens()
        by_uid = {s.uid: s for s in slots}
        assert V0_SUB in by_uid, f"v0 slot missing; slots={[s.uid for s in slots]}"
        assert V1_SUB in by_uid, f"v1 slot missing; slots={[s.uid for s in slots]}"
        # Same endpoint, different uid.
        assert by_uid[V0_SUB].endpoint == by_uid[V1_SUB].endpoint, (
            f"v0 endpoint={by_uid[V0_SUB].endpoint!r} differs from "
            f"v1 endpoint={by_uid[V1_SUB].endpoint!r}"
        )
        # Slots independent — both fresh after happy round-trips.
        assert by_uid[V0_SUB].status == "fresh"
        assert by_uid[V1_SUB].status == "fresh"

        # Emulator received both uploads, one per phantom_local_uuid.
        received = emulator.received()
        seen_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in received}
        expected = {str(chain_v0), str(chain_v1)}
        assert seen_uuids == expected, (
            f"emulator received mismatch: expected={expected}, got={seen_uuids}"
        )
    finally:
        await stack.tear_down()


async def _submit_via_phantom_client(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    uid: str,
    bearer: str,
    emulator_url: str,
) -> None:
    """Submit a happy two-step envelope keyed by the supplied uid."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=uid,
        auth_token=f"Bearer {bearer}",
    )
