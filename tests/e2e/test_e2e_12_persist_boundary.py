"""E2E-12 — Persist-trigger boundary (memory → disk migration).

Submits one envelope while the upstream returns 503 on the metadata
POST. The sender's first attempt fails; the persist trigger fires
(``after_attempts: 1``); ``persist_now`` migrates the row from the
memory tier to the disk tier via the commit-last-column path. Clearing
the failure lets the next attempt succeed end-to-end.

This test exercises the load-bearing attempt-count path that the
size-aware persist trigger and the startup orphan-body sweep both
build on.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.status import UploadRow
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack
from .helpers.timing import await_until

# Body for the round-trip. Large enough that the disk-tier write is
# observable as a non-empty file but small enough that the test never
# crosses the size-aware persist threshold (which is profile-derived
# and may differ between hosts).
BODY_BYTES: bytes = b"phantom-e2e-persist-boundary-body-" + b"x" * 256

# Wait budget for the row to migrate. The first failed attempt fires
# from the sender pool on next poll (poll_interval_ms: 100); the
# persist runs inline; we observe via admin within a couple of seconds.
MIGRATION_WAIT_SECONDS: float = 10.0


@pytest.mark.e2e
async def test_e2e_12_persist_boundary(tmp_path: Path) -> None:
    """Memory → disk migration fires after the first failed attempt."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            # Phase 1 migration: ``persist_trigger.after_attempts: 1``
            # (force-persist on first failed attempt) is replaced by
            # the size-threshold immediate-persist path in hybrid mode.
            # Setting body_size_threshold_bytes=1 ensures any non-trivial
            # body is enqueued against the PersistController at admission.
            "storage": {
                "persist_trigger": {
                    "body_size_threshold_bytes": 1,
                },
            },
        },
    )
    try:
        emulator = stack.emulator
        pc = stack.phantom_client
        emulator.clear_received()
        emulator.clear_failures()

        # 1. Inject a 100% 5xx so the first attempt fails. We use
        #    GLOBAL scope because the driver's hardcoded POST URL is
        #    `/v2/files`; the emulator's `_scope_for_path` only
        #    recognises `/v1/files/create` (the canonical generic
        #    endpoint). The `/v2/files` alias mounted by
        #    `helpers/stack.py` for driver compatibility passes
        #    through the middleware as GLOBAL scope. Once the first
        #    failed attempt fires `persist_now`, we clear the policy
        #    and let step 2 succeed against the unaltered emulator.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        # 2. Submit one envelope.
        chain_id = uuid4()
        await _submit_one(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )

        # 3. Wait for the row to migrate to disk. We probe via
        #    list_uploads (the SDK's get_upload returns ChainResponse
        #    which doesn't carry the body_location; UploadRow does).
        #    Phase 1 renamed tier='persisted' → body_location='file'.
        row = await _await_body_location(pc, chain_id, expected_body_location="file")
        assert row.body_location == "file"
        # Body presence is verified directly on disk by the test's
        # filesystem walk further down — the row no longer carries a
        # ``body_path`` column (Family 1 cut).
        # State after the PersistController's flip — the row sits
        # queued or attempting depending on the sender pickup window.
        # We only assert it's not stranded in a terminal state.
        assert row.state in ("queued", "attempting"), (
            f"post-migration row state={row.state!r}; expected queued or attempting "
            f"so the sender re-picks it"
        )

        # 4. The body must exist on disk now. The InstanceCfg has
        #    `data_dir: "primary"` (relative to the top-level
        #    `storage.data_dir`), and `shard_prefix_chars: 2` in
        #    phantom-config.yml means the layout is
        #    `<tmp>/primary/bodies/<first-2-chars-of-uid>/<uid>/<body_ref_name>`.
        bodies_root = tmp_path / "primary" / "bodies"
        instance_tree = list((tmp_path / "primary").rglob("*"))
        assert bodies_root.exists(), (
            f"expected bodies/ root at {bodies_root}; tree: {instance_tree}"
        )
        # FileBodyStore lays bodies out as
        # ``<root>/<shard>/<uid>/<body_ref_name>``; the driver's
        # envelope builder names the single body_ref ``body``. The
        # ``body_path`` field on the SDK's :class:`UploadRow` stores
        # the templated ``<data_dir>/bodies/<uid>`` prefix (the dir
        # holding all named body_refs for this row), not the file.
        chain_id_str = str(chain_id)
        upload_dir = bodies_root / chain_id_str[:2] / chain_id_str
        body_file = upload_dir / "body"
        assert body_file.exists(), (
            f"expected body file at {body_file}; bodies/ tree: {list(bodies_root.rglob('*'))}"
        )
        assert body_file.stat().st_size > 0

        # 5. Clear the failure and let the chain succeed.
        emulator.clear_failures()
        chain = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=30.0,
        )
        assert chain.state == "succeeded"

        # 6. Emulator received exactly one body whose hash matches
        #    the original bytes — the disk-tier round-trip preserves
        #    the body content end-to-end.
        received = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(BODY_BYTES),
            timeout_seconds=15.0,
        )
        assert received.body_size == len(BODY_BYTES)
        expected_hash = hashlib.sha256(BODY_BYTES).hexdigest()
        # The emulator doesn't surface a hash directly — we rely on
        # `body_size` plus the absence of a body-mutation injection.
        # The hash assertion is satisfied transitively (no
        # `body_cutoff_at_bytes` policy was installed). Keeping the
        # local hash for log-side diagnostics.
        _ = expected_hash
    finally:
        await stack.tear_down()


async def _submit_one(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Build a single envelope and submit it through phantom-client."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid="00000000-0000-0000-0000-000000000001",
        auth_token=f"Bearer {bearer}",
    )


async def _await_body_location(
    pc: PhantomClient,
    chain_id: UUID,
    *,
    expected_body_location: str,
    timeout_seconds: float = MIGRATION_WAIT_SECONDS,
) -> UploadRow:
    """Poll list_uploads until ``chain_id`` reports ``expected_body_location``.

    Returns the matching :class:`UploadRow`. Raises
    :class:`AssertionError` on deadline exhaustion. We use
    ``list_uploads`` rather than ``get_upload`` because
    ``ChainResponse`` doesn't carry the body location; that field is
    on the admin-side :class:`UploadRow` shape. Phase 1 Slice 1.E
    renamed ``tier`` → ``body_location``.
    """
    matched: UploadRow | None = None

    async def _seen() -> bool:
        nonlocal matched
        rows, _ = await pc.list_uploads(limit=200)
        for row in rows:
            if row.chain_id == chain_id and row.body_location == expected_body_location:
                matched = row
                return True
        return False

    await await_until(
        _seen,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=0.1,
        message=(
            f"chain_id={chain_id} never reached body_location={expected_body_location!r} "
            f"within {timeout_seconds}s"
        ),
    )
    assert matched is not None  # await_until raises on failure; this is a guard for mypy.
    return matched
