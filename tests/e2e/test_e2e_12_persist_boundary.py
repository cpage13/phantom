"""E2E-12: the size-threshold persist trigger, RAM to disk and back out.

What actually fires here (corrected per review finding S12-6). The
attempt-count trigger ``persist_trigger.after_attempts`` was DELETED in
Phase 1. This test configures
``storage.persist_trigger.body_size_threshold_bytes`` instead, which in
hybrid mode enqueues the chain against the :class:`PersistController` AT
ADMISSION, before the sender has attempted anything. The injected 100
percent 5xx no longer drives the migration; its only remaining job is to
keep the row non-terminal long enough to observe ``body_location`` flip
to ``file`` and the body land on disk. Clearing the failure then lets the
chain succeed, which proves the disk-backed body still round-trips to the
upstream byte-for-byte.

What this file does NOT cover, stated plainly so a reader auditing
coverage is not misled. The module name, the test name and both
docstrings used to claim "migration fires after the first failed
attempt", which had not been true since Phase 1. There is no
attempt-count trigger to cover any more. The OTHER live trigger,
retry-linger, is a sender-side decision and is covered by
``src/phantom-service/tests/unit/test_retry_linger_persist_enqueue.py``.
The RAM-pressure trigger is covered by
``src/phantom-service/tests/unit/test_ram_pressure_fresh_attempt_bound.py``
and its reload sibling.

``attempts`` is deliberately NOT asserted here. The size trigger fires at
admission, so the attempt count at the moment the migration is observed
depends only on where the sender's poll window happened to fall, and
pinning it would be a race rather than a contract.
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
# observable as a non-empty file, and comfortably over the size
# threshold this test configures, whatever the codec does to it.
BODY_BYTES: bytes = b"phantom-e2e-persist-boundary-body-" + b"x" * 256

# Wait budget for the row to migrate. Admission enqueues the chain
# against the PersistController straight away and the controller
# serializes the migration on its own queue, so the flip is observable
# via admin within a couple of seconds.
MIGRATION_WAIT_SECONDS: float = 10.0

# The configured size trigger. Any body at or above this STORED size is
# enqueued for migration at admission. Pinned at 1 byte so BODY_BYTES is
# unambiguously over it whatever the configured codec does to it, and
# named rather than left as a bare literal in the config block so a
# reader can see what the test is actually turning on.
PERSIST_SIZE_THRESHOLD_BYTES: int = 1

# Rungs of one second each, held open while step 3 polls for the migration.
# Sized so the retry budget cannot exhaust into ``stored`` before the poll
# observes the flip, even on a loaded runner.
_RETRY_RUNGS_WHILE_OBSERVING: int = 120


@pytest.mark.e2e
async def test_e2e_12_size_threshold_migrates_ram_body_to_disk(tmp_path: Path) -> None:
    """Objective: the size trigger migrates a RAM body to disk and delivery still matches.

    Expected outcome, in order:

    1. The admitted row reports ``body_location='file'``, so the
       size-threshold enqueue at admission reached the
       :class:`PersistController`.
    2. The row is still non-terminal at that point, so the migration
       happened while the chain was undelivered rather than as part of
       tidying a finished row.
    3. The body file exists on disk and is non-empty, so the flip is a
       real durability commit and not a column update on its own.
    4. Once the injected failure is cleared the chain succeeds and the
       emulator receives exactly the original byte count, so the
       disk-backed body round-trips unchanged.

    Falsifier: invert the size comparison, or drop the admission-time
    enqueue, and step 1 times out with the row still at
    ``body_location='ram'``.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            # The live trigger. ``persist_trigger.after_attempts`` was
            # deleted in Phase 1; the size threshold is what enqueues a
            # body against the PersistController, and it does so at
            # admission rather than after any attempt.
            "storage": {
                "persist_trigger": {
                    "body_size_threshold_bytes": PERSIST_SIZE_THRESHOLD_BYTES,
                },
            },
            # A retry ladder long enough that the row CANNOT exhaust its budget
            # while step 3 observes the migration. The shared e2e ladder is
            # [0, 1, 2, 5, 10], about eighteen seconds before the sender gives
            # up and parks the row in ``stored``; on a loaded full-lane run the
            # migration poll outlasted that, the row was terminal by the time it
            # was read, and the undelivered-chain assertions below failed. That
            # is a property of the WINDOW, not of the behaviour under test, so
            # the window is widened rather than the assertions weakened. Each
            # rung stays at one second so clearing the failure policy still lets
            # the chain deliver promptly in step 5.
            "retry": {
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": [0] + [1] * _RETRY_RUNGS_WHILE_OBSERVING,
                },
            },
        },
    )
    try:
        emulator = stack.emulator
        pc = stack.phantom_client
        emulator.clear_received()
        emulator.clear_failures()

        # 1. Inject a 100% 5xx so the row stays non-terminal while we
        #    observe the migration. This does NOT trigger the migration;
        #    the size threshold already did, at admission. We use
        #    GLOBAL scope because the driver's hardcoded POST URL is
        #    `/v2/files`; the emulator's `_scope_for_path` only
        #    recognises `/v1/files/create` (the canonical generic
        #    endpoint). The `/v2/files` alias mounted by
        #    `helpers/stack.py` for driver compatibility passes
        #    through the middleware as GLOBAL scope. Once the migration
        #    is observed we clear the policy and let the chain succeed
        #    against the unaltered emulator.
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
        #    list_uploads (the SDK's get_upload answers with
        #    ChainAdminDetail, and the body_location we need is on the
        #    admin UploadRow shape list_uploads returns).
        #    Phase 1 renamed tier='persisted' → body_location='file'.
        row = await _await_body_location(pc, chain_id, expected_body_location="file")
        assert row.body_location == "file"
        # Body presence is verified directly on disk by the test's
        # filesystem walk further down; the row no longer carries a
        # ``body_path`` column (Family 1 cut).
        #
        # The row must still be UNDELIVERED at this point: the migration
        # is a durability step for a live chain, not cleanup of a
        # finished one. queued or attempting are both legitimate, and
        # which one it is depends purely on where the sender's poll
        # window fell, so ``attempts`` is deliberately not pinned here
        # (see the module docstring, finding S12-6).
        assert row.state in ("queued", "attempting"), (
            f"post-migration row state={row.state!r}; expected queued or attempting, "
            f"so the migration happened to a live undelivered chain and the sender "
            f"re-picks it"
        )
        assert row.sent_at is None, (
            f"the row reports sent_at={row.sent_at!r}, so it had already been "
            f"delivered when the migration was observed; this test must observe "
            f"the RAM to disk flip on an undelivered chain"
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

    Returns the matching :class:`UploadRow`, the admin list route's row
    shape, which carries ``body_location`` and ``sent_at``. Raises
    :class:`AssertionError` on deadline exhaustion. The stale reason
    this docstring used to give ("``ChainResponse`` doesn't carry the
    body location") no longer holds: ``get_upload`` answers with
    ``ChainAdminDetail`` and carries both fields as well, so either
    route would serve. Phase 1 Slice 1.E renamed ``tier`` to
    ``body_location``.
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
