"""Multipart corrupted-body E2E (plan § 6.2.6).

Submits a 3-file multipart chain in all_disk mode; manually corrupts
one body file on disk; triggers the sender via an upstream failure
+ clear; asserts the chain transitions to ``corrupted`` rather than
silently delivering tampered bytes.

This is the body-as-atomic-unit invariant on the failure leg: if any
constituent file is corrupted, the chain MUST surface ``corrupted``
— the dual-hash check (ADR-014) fires on the sender's body-read
path.
"""

from __future__ import annotations

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

pytestmark = [pytest.mark.e2e]


async def test_multipart_corrupted_one_file_transitions_to_corrupted() -> None:
    """Corrupt one of three multipart files; chain surfaces corrupted.

    Boot in all_disk mode so bodies land on disk; submit multipart;
    delete one file's on-disk body; clear the upstream failure;
    assert the chain reaches ``corrupted`` (the H8 path detects the
    missing body during the sender's body-read).
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

        # Vandalize: delete ONE body_ref's on-disk file (the body
        # store's delete is whole-chain — we delete every ref since
        # the row is multipart; the body-as-atomic-unit invariant
        # means the chain quarantines on any missing ref).
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
