"""Aggressor — ``completion_post`` empty-body POST shape.

A representative empty-body completion POST: the URL carries all the
payload (identifiers in the path) and the body is empty
(``Content-Length: 0``). A neutral example URL shape is
``/api/v1/sequence/{source_id}/{label}/complete``.

The existing ``test_aggressor_empty_body_post`` covers the general
empty-body admission contract. This test sharpens the contract for the
``completion_post`` shape specifically:

1. **Persistence shape**: under ``default_tier: persisted`` +
   ``persist_trigger.after_attempts: 0`` (a representative deployment
   config), the row persists IMMEDIATELY on receipt. For an
   empty-body POST, ``body_size_bytes == 0`` and there should be
   NO on-disk body file (or an empty one — exact behavior is
   implementation-defined, but the load-bearing invariant is "no
   non-zero body bytes are stored").

2. **Admin GET surfaces ``has_body=False``**: per the round-2 admin
   envelope projection (``_extract_step_projections`` in
   ``src/phantom-service/src/phantom/routes/admin.py:600-639``),
   ``has_body = step.get("body") is not None``. For a step with
   ``body=None``, the admin detail's step entry must report
   ``has_body=False``. This pins the projection contract for the
   bodyless case.

3. **Method round-trip and URL byte-fidelity**: a step's path
   round-trips byte-equal even for the URL-carrying-payload pattern
   (``/api/v1/sequence/{id}/{label}/complete``).

4. **Sender forwards with no body bytes**: the emulator's create-file
   endpoint accepts empty JSON body (it tolerates with
   ``body_json = json.loads(body_raw) if body_raw else {}``).
   We use the create-file route as our completion-post proxy because
   the emulator has no dedicated completion endpoint;
   structurally the chain step's behavior is identical
   (POST + ``Content-Length: 0`` + URL-carries-payload).

If Phantom rejects empty-body POSTs at admission, persists ghost-bytes
on disk, or reports ``has_body=True`` for a bodyless step, this test
catches it.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainEnvelope, ChainStep

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
TERMINAL_BUDGET_SECONDS: float = 15.0
# Synthetic source id + label for the URL.
SERIAL_NUMBER: str = "SRC-2026-001"
LABEL_NAME: str = "sequential_batch"

pytestmark = pytest.mark.e2e


async def _submit_mark_complete(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
) -> str:
    """Submit a completion_post-shaped empty-body POST chain.

    Returns the URL submitted, so the test can assert URL byte-fidelity
    against the admin projection.
    """
    # A representative completion URL is /api/v1/sequence/{id}/{label}/complete.
    # The emulator doesn't have that exact route, but it does have
    # POST /v1/files/create which tolerates an empty body. We use that
    # endpoint with a query-string-carrying-payload shape that mirrors
    # a completion post's "the URL is the payload" semantics.
    submitted_url = f"{emulator_url}/v1/files/create?serial={SERIAL_NUMBER}&label={LABEL_NAME}"
    step = ChainStep(
        name="completion_post",
        method="POST",
        url=submitted_url,
        # A completion post sends just Content-Type from httpx
        # defaults; we add only that here. No Authorization-shaped
        # content; Phantom injects Authorization from the cache.
        headers={"Content-Type": "application/json"},
        body=None,  # Empty body — Content-Length: 0.
        capture=[],
        idempotency_header=None,
    )
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[step],
        default_target=None,
    )
    await pc.submit_chain(
        envelope,
        body_refs=None,
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return submitted_url


async def test_aggressor_completion_post_empty_post_persists_no_body(
    tmp_path: Path,
) -> None:
    """An empty-body POST persists with zero-byte body and ``has_body=False``.

    Representative shape: ``completion_post``. Loaded with
    ``default_tier: persisted`` + ``after_attempts: 0`` so the row
    immediately writes to disk.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # Phase 1: all-disk mode replaces ``default_tier:
                # persisted`` + ``after_attempts: 0``.
                "body_store": {"mode": "all_disk"},
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
            "retention": {
                # Keep the row long enough to inspect after success.
                "succeeded_metadata_seconds": 300,
                "succeeded_body_seconds": 60,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        submitted_url = await _submit_mark_complete(
            pc,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            chain_id=chain_id,
        )

        # 1. Chain reaches succeeded — Phantom accepts the empty body
        #    and the emulator returns 200.
        detail = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"
        assert detail.last_step_completed == "completion_post"
        # No retries needed for a clean POST.
        assert detail.attempts == 1, (
            f"empty-body POST should succeed on attempt 1; attempts={detail.attempts}"
        )

        # 2. Persistence shape: under body_store.mode='all_disk', the
        #    row is on disk. Phase 1 renamed ``tier='persisted'`` →
        #    ``body_location='file'`` and dropped ``committed``.
        assert detail.body_location == "file", (
            f"chain landed in body_location={detail.body_location!r}; "
            f"expected 'file' under body_store.mode='all_disk'."
        )

        # 3. Admin envelope: has_body=False for the completion_post
        #    step. Per the round-2 projection contract.
        assert len(detail.steps) == 1, (
            f"expected 1 step in admin detail, got {len(detail.steps)}: "
            f"{[s.name for s in detail.steps]}"
        )
        admin_step = detail.steps[0]
        assert admin_step.name == "completion_post", (
            f"step name byte-fidelity: got {admin_step.name!r}, expected 'completion_post'"
        )
        assert admin_step.method == "POST", (
            f"step method byte-fidelity: got {admin_step.method!r}, expected POST"
        )
        assert admin_step.url == submitted_url, (
            f"step url byte-fidelity broken: got {admin_step.url!r}, expected {submitted_url!r}"
        )
        assert admin_step.has_body is False, (
            f"BUG: empty-body POST has has_body={admin_step.has_body!r} "
            f"in admin detail; expected False because step.body=None. "
            f"The admin projection's `has_body = step.body is not None` "
            f"contract is broken."
        )

        # 4. Direct storage check: no body file on disk for the
        #    bodyless chain. (For body_refs={"body": <bytes>}, the
        #    file would exist at <data_dir>/<instance>/bodies/<shard>/
        #    <chain_id>/body.) For an empty-body chain, no file_ref
        #    is created so the body directory either doesn't exist
        #    or is empty.
        instance_cfg = next(i for i in stack.settings.instances if i.id == "primary")
        shard = chain_id.hex[:2]
        body_dir = stack.data_dir / instance_cfg.data_dir / "bodies" / shard / str(chain_id)
        if body_dir.exists():
            files_in_body_dir = list(body_dir.iterdir())
            assert not files_in_body_dir, (
                f"BUG: empty-body POST left files on disk at {body_dir}: "
                f"{[p.name for p in files_in_body_dir]}. Expected no "
                f"body files because step.body=None and body_refs=None."
            )

        # 5. Underlying row-level body_size_bytes check.
        #    The client UploadRow doesn't surface body_size_bytes, so
        #    we read the underlying row directly via the instance. The
        #    Phase 1 single-store collapse replaced ``disk_store`` with
        #    the single ``store``.
        instance = stack.get_instance(instance_id="primary")
        row = await instance.store.get(chain_id)
        assert row is not None, f"BUG: empty-body POST row not found via store.get({chain_id})"
        assert row.body_size_bytes == 0, (
            f"BUG: empty-body POST persisted with "
            f"body_size_bytes={row.body_size_bytes}; expected 0. "
            f"Phantom is storing ghost bytes for an empty-body chain."
        )
    finally:
        await stack.tear_down()


async def test_aggressor_completion_post_empty_post_admission_succeeds(
    tmp_path: Path,
) -> None:
    """Empty-body POST is admitted (returns 202), not rejected as 4xx.

    A simpler sanity check on the admission contract, before the
    fancy persistence + has_body assertions. If admission rejects
    bodyless chains, this fails fast and surfaces a clear signal.
    """
    # Memory-tier (default) — no extra persistence overhead.
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()

        # Submission MUST NOT raise. If admission rejects empty-body
        # POSTs with a 4xx, submit_chain raises a transport error
        # carrying that status. We catch and re-raise with context.
        try:
            await _submit_mark_complete(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                chain_id=chain_id,
            )
        except Exception as exc:
            raise AssertionError(
                f"BUG: admission rejected empty-body POST chain: {exc!r}. "
                f"a completion POST is a representative empty-body pattern; "
                f"admission must accept bodyless POSTs."
            ) from exc

        # And the chain must reach succeeded (the emulator tolerates
        # the empty body on /v1/files/create).
        detail = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )
        # Sanity: at least one attempt fired against upstream.
        assert detail.attempts >= 1, (
            f"empty-body POST: chain reached succeeded but attempts={detail.attempts}; "
            f"expected at least 1 fired attempt."
        )
    finally:
        await stack.tear_down()
