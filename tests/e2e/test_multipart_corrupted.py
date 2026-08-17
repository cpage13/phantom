"""Multipart missing-body E2E (plan § 6.2.6, extended by F2).

Two tests, same 3-file multipart chain in all_disk mode, differing in HOW MUCH
of the body is lost and therefore in WHICH mechanism catches it:

* ``test_multipart_corrupted_one_file_transitions_to_corrupted`` deletes the
  WHOLE chain directory. The body store's own traversal cannot list it and
  raises ``KeyError``, which the sender re-raises as ``BodyMissingError``.
* ``test_multipart_single_missing_ref_transitions_to_corrupted`` deletes exactly
  ONE ref's file, leaving the directory and the other two files intact. The body
  store returns a short dict without raising (it lists what it holds and never
  sees the row's declared refs), so the SENDER's declared-versus-returned
  completeness check catches it (F2) and raises the same ``BodyMissingError``.

Neither path is the dual-hash check: a missing file never reaches a hash
comparison. Both land the row in ``corrupted`` with the same ``last_error``
prefix, which is the body-as-atomic-unit invariant of ADR-014 on the failure
leg: if any constituent file is absent, the chain MUST surface ``corrupted``
rather than deliver a truncated body.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import boot_stack
from .helpers.timing import await_until
from .test_multipart_happy_path import _build_multipart_envelope

logger = logging.getLogger(__name__)

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Multipart with 3 files.
MULTIPART_FILE_COUNT: int = 3
PER_FILE_BODY_BYTES: int = 4096

# Wait budgets.
PER_CHAIN_BUDGET_SECONDS: float = 60.0

# The single ref the partial-loss test deletes: the LAST one. Its POST/PUT pair
# is the last of the six steps, so the injected upstream hold guarantees those
# steps never ran before the file was removed.
DELETED_REF_NAME: str = f"body_{MULTIPART_FILE_COUNT - 1}"

pytestmark = [pytest.mark.e2e]


async def test_multipart_corrupted_one_file_transitions_to_corrupted() -> None:
    """Delete the WHOLE chain directory; the chain surfaces corrupted.

    Boot in all_disk mode so bodies land on disk; submit multipart; delete the
    whole-chain body directory; clear the upstream failure; assert the chain
    reaches ``corrupted``. This is the H8 path: the body store's traversal
    cannot list the directory, raises ``KeyError``, and the sender re-raises
    ``BodyMissingError``. The partial-loss sibling below exercises the other
    mechanism.
    """
    stack = await boot_stack(
        config_overrides={
            "storage": {"body_store": {"mode": "all_disk"}},
        },
    )
    try:
        emulator = stack.emulator
        emulator.clear_received()
        # Hold the upstream so the body isn't consumed before we
        # vandalize it.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=10_000,
            ),
        )
        bearer = stack.fake_security_token()

        chain_id: UUID = uuid4()
        envelope, names = _build_multipart_envelope(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            body_count=MULTIPART_FILE_COUNT,
        )
        bodies: dict[str, bytes] = {n: secrets.token_bytes(PER_FILE_BODY_BYTES) for n in names}

        await PhantomClient.submit_chain.__call__(
            stack.phantom_client,
            envelope,
            body_refs=bodies,
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {bearer}",
        )

        # Wait for the body to be persisted on disk (all_disk mode
        # writes at admission).
        instance = stack.get_instance("primary")

        async def _on_disk() -> bool:
            row = await instance.store.get(chain_id)
            return row is not None and row.body_location == "file"

        await await_until(
            _on_disk,
            timeout_seconds=10.0,
            poll_interval_seconds=0.1,
            message="row never reached body_location='file' in all_disk mode",
        )

        # Vandalize: delete the WHOLE chain directory. ``BodyStore.delete`` is
        # whole-chain by contract, so every ref goes at once; the body store's
        # own traversal then raises KeyError. The single-ref case, which the
        # store cannot detect and the sender's completeness check must, is the
        # sibling test below.
        await instance.body_store.delete(chain_id)

        # Clear the upstream failure so the sender retries.
        emulator.clear_failures()

        # The chain must reach 'corrupted'.
        await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="corrupted",
            timeout_seconds=PER_CHAIN_BUDGET_SECONDS,
        )

        detail = await stack.phantom_client.get_upload(chain_id)
        assert detail.state == "corrupted"
        last_error = detail.last_error or ""
        assert "body" in last_error.lower() or "missing" in last_error.lower(), (
            f"expected last_error to mention missing body; got {last_error!r}"
        )
    finally:
        await stack.tear_down()


async def test_multipart_single_missing_ref_transitions_to_corrupted() -> None:
    """Delete exactly ONE ref's file; the chain still surfaces corrupted (F2).

    Objective: prove the partial-loss case end to end, which the whole-directory
    test above cannot cover. The body store returns the two surviving refs
    without raising, so only the sender's declared-versus-returned check stands
    between a partial body and a truncated delivery stamped ``succeeded``.

    Success: the chain reaches ``corrupted``, its ``last_error`` names the
    deleted ref, and the emulator never accepted that ref's bytes.

    **Deleting the LAST ref is load-bearing, not stylistic.**
    ``_build_multipart_envelope`` emits a metadata POST plus a body PUT per
    file, so a three-file chain is a SIX-step chain, and the injected latency
    stalls the PUT steps but not the metadata POSTs. Deleting the last ref
    guarantees its steps provably never ran during the hold. For the same
    reason the emulator assertion is scoped to that ref's hash rather than
    asserting the emulator received nothing at all: the earlier pairs may
    legitimately have completed during the ten second hold.
    """
    stack = await boot_stack(
        config_overrides={
            "storage": {"body_store": {"mode": "all_disk"}},
        },
    )
    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=10_000,
            ),
        )
        bearer = stack.fake_security_token()

        chain_id: UUID = uuid4()
        envelope, names = _build_multipart_envelope(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            body_count=MULTIPART_FILE_COUNT,
        )
        bodies: dict[str, bytes] = {n: secrets.token_bytes(PER_FILE_BODY_BYTES) for n in names}

        await PhantomClient.submit_chain.__call__(
            stack.phantom_client,
            envelope,
            body_refs=bodies,
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {bearer}",
        )

        instance = stack.get_instance("primary")

        async def _on_disk() -> bool:
            row = await instance.store.get(chain_id)
            return row is not None and row.body_location == "file"

        await await_until(
            _on_disk,
            timeout_seconds=10.0,
            poll_interval_seconds=0.1,
            message="row never reached body_location='file' in all_disk mode",
        )

        # Delete exactly one ref's file, leaving the chain directory and the
        # other two files intact. ``path_for`` is the public, documented hook
        # for this kind of test manipulation.
        instance.file_body_store.path_for(chain_id, DELETED_REF_NAME).unlink()

        emulator.clear_failures()

        await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="corrupted",
            timeout_seconds=PER_CHAIN_BUDGET_SECONDS,
        )

        detail = await stack.phantom_client.get_upload(chain_id)
        assert detail.state == "corrupted"
        last_error = detail.last_error or ""
        assert DELETED_REF_NAME in last_error, (
            f"last_error must name the deleted ref {DELETED_REF_NAME!r}; got {last_error!r}"
        )

        deleted_body_hash = hashlib.sha256(bodies[DELETED_REF_NAME]).hexdigest()
        accepted_hashes = {entry.body_hash for entry in emulator.received()}
        assert deleted_body_hash not in accepted_hashes, (
            "the deleted ref's bytes must never have been accepted upstream; the emulator "
            f"holds a body with hash {deleted_body_hash}"
        )
    finally:
        await stack.tear_down()
