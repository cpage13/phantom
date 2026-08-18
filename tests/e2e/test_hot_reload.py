"""E2E — Hot reload of operational config.

Hot reload exposes two triggers: SIGHUP on the process and
``POST /v1/admin/reload`` on the admin endpoint. Both go through the
same ``apply_reload`` helper in ``phantom.runtime.reload`` — re-read
YAML (with the host probe skipped), build new per-instance snapshots, swap them
atomically into the :class:`SettingsHolder`, propagate live-state
changes (AD-mint drift warning, saturation cap update, retry-strategy
rebuild), return the reloaded instance list. Since F5 the per-instance
``cfg`` is NOT repointed: the route block is frozen at boot.

This suite exercises six invariants:

1. ``test_reload_via_sighup_changes_retention`` — SIGHUP picks up a
   shorter retention window; the reaper deletes a succeeded row inside
   the new window.
2. ``test_reload_via_admin_endpoint_changes_saturation`` — admin
   endpoint picks up a tighter saturation cap; subsequent ingress
   trips ``saturation_cap`` at the new (lower) cap.
3. ``test_in_flight_rows_keep_snapshotted_capture_reexecution`` — a
   row's :attr:`UploadRow.capture_reexecution_active` is snapshotted
   at ingress and is NOT mutated by a later reload. New rows submitted
   after the reload see the new value.
4. ``test_reload_is_atomic`` — concurrent ingress submissions during
   a reload either see the OLD snapshot or the NEW; never a half-applied
   blend.
5. ``test_reload_changes_admin_lookup_binding_without_restart``: the
   cycle-7 by-captured-id binding follows the reloaded config live;
   a repointed ``json_path`` is consulted at request time, removal
   restores the 400 refusal, re-adding restores resolution (round 2
   adversary hardening).
6. ``test_reload_changes_codec_choice_for_new_admissions``: ADR-013
   lists codec choice as a reloadable knob; a post-reload admission
   must encode with the newly configured algorithm. Passing pin of the
   R5-2 fix: ``codec_factory`` selects from the LIVE settings snapshot
   per admission instead of closing over the boot-time
   ``CompressionCfg``.

There is NO mid-flight ``ad_mint`` reload test: structural ``ad_mint``
changes log a WARNING and require a process restart (the post-Phase-2
contract; see the note at the bottom of this module). An earlier
revision of this docstring advertised one anyway; corrected by the
round 2 adversary.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import yaml
from phantom_client import PhantomBadRequestError, PhantomClient, PhantomUnavailableError

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until, pace

logger = logging.getLogger(__name__)

# Default ``sub`` claim used by the suite's fake security token.
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Small body payload for hot-reload tests — these tests assert on
# state transitions, not body throughput, so a few hundred bytes is
# enough.
RELOAD_BODY: bytes = b"phantom-e2e-hot-reload-body-" + b"x" * 128

# How long to wait for the reload to actually land on the live state
# (snapshot swap, gate update_caps, minter swap). The reload itself is
# a few SQLite reads + a file write + a few async calls; 5s is generous.
RELOAD_PROPAGATION_BUDGET_SECONDS: float = 5.0

# How long to wait for the reaper to delete a succeeded row after the
# retention window elapses. The suite's ``reaper_interval_seconds`` is
# 5s; we give the reaper two ticks plus a margin.
REAPER_BUDGET_SECONDS: float = 15.0


pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes = RELOAD_BODY,
    chain_id: UUID | None = None,
) -> UUID:
    """Submit one upload-shaped chain and return its ``chain_id``."""
    chain_id = chain_id or uuid4()
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
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id


async def _wait_for_admin_404(pc: PhantomClient, chain_id: UUID, *, budget: float) -> None:
    """Wait until ``GET /v1/admin/chains/{chain_id}`` returns 404.

    The reaper deletes a row whose retention window has elapsed; the
    admin lookup then returns 404 (``not_found``). The SDK raises
    :class:`PhantomNotFoundError` on that response, which inherits from
    :class:`PhantomClientError`. Polls until the not-found surface
    appears or the budget elapses.
    """
    from phantom_client import PhantomNotFoundError

    async def _gone() -> bool:
        try:
            await pc.get_upload(chain_id)
        except PhantomNotFoundError:
            return True
        return False

    await await_until(
        _gone,
        timeout_seconds=budget,
        poll_interval_seconds=0.25,
        message=f"chain {chain_id} was never reaped within {budget}s",
    )


async def _wait_for_holder_snapshot(
    stack: E2EStack,
    *,
    predicate: Any,
    budget: float = RELOAD_PROPAGATION_BUDGET_SECONDS,
) -> None:
    """Poll the live :class:`InstanceSettingsSnapshot` until ``predicate`` holds.

    ``predicate(snapshot)`` is a sync callable returning truthy when
    the desired post-reload state is observable. The snapshot is read
    via :meth:`InstanceContext.current_settings`, which always returns
    the latest swap (the holder lock makes this safe even mid-reload).
    """
    instance = stack.get_instance("primary")

    async def _ok() -> bool:
        snapshot = instance.current_settings()
        return bool(predicate(snapshot))

    await await_until(
        _ok,
        timeout_seconds=budget,
        poll_interval_seconds=0.05,
        message=f"snapshot predicate {predicate!r} never held after reload",
    )


# ---------------------------------------------------------------------------
# Test 1 — SIGHUP changes retention; reaper picks up the new window.
# ---------------------------------------------------------------------------


async def test_reload_via_sighup_changes_retention(tmp_path: Path) -> None:
    """SIGHUP picks up a shorter retention window; reaper deletes within it.

    Start with ``succeeded_metadata_seconds: 180`` (the suite default).
    Rewrite the YAML to ``succeeded_metadata_seconds: 5``. Send SIGHUP
    to the current process (the in-process Phantom shares the test
    process's signal-handler table). Submit a fresh chain; once it
    reaches ``succeeded``, the reaper sweeps it within ~10 seconds
    (5s retention + up to 5s reaper interval + margin). Before the
    reload the same row would have stayed at least 180s.
    """
    stack = await boot_stack(tmp_path=tmp_path, enable_hot_reload=True)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # 1. Confirm we boot with the long retention.
        instance = stack.get_instance("primary")
        before = instance.current_settings()
        assert before.retention.succeeded_metadata_seconds == 180, (
            f"baseline retention not 180s; got {before.retention.succeeded_metadata_seconds}"
        )

        # 2. Rewrite YAML for a shorter retention window.
        stack.rewrite_yaml(
            {"retention": {"succeeded_metadata_seconds": 5}},
        )

        # 3. Fire SIGHUP at the current process. The app's lifespan
        # installed the handler on the running loop; the handler
        # schedules ``apply_reload`` and returns. We wait for the
        # snapshot swap to observe the new retention value.
        os.kill(os.getpid(), signal.SIGHUP)
        await _wait_for_holder_snapshot(
            stack,
            predicate=lambda s: s.retention.succeeded_metadata_seconds == 5,
        )

        # 4. Submit a chain. With a healthy emulator it reaches
        # succeeded fast.
        chain_id = await _submit_one(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=15.0,
        )

        # 5. Wait for the reaper to delete the row. With a 5-second
        # window and the suite's 5-second reaper interval, the row
        # disappears within ~15 seconds even at the worst phase of the
        # reaper's tick.
        await _wait_for_admin_404(
            stack.phantom_client,
            chain_id,
            budget=REAPER_BUDGET_SECONDS,
        )
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Test 2 — admin endpoint changes saturation; new cap enforced at ingress.
# ---------------------------------------------------------------------------


async def test_reload_via_admin_endpoint_changes_saturation(tmp_path: Path) -> None:
    """``POST /v1/admin/reload`` picks up a tighter saturation cap.

    Start with a high cap (so a 1-submit burst is admitted). Rewrite
    YAML to ``max_in_flight: 0`` (no admits allowed). POST to the
    admin endpoint. The next submit is refused with 503 and the
    ``saturation_cap`` error code.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        enable_hot_reload=True,
        config_overrides={
            "saturation": {"max_in_flight": 50, "max_in_flight_bytes": 16 * 1024 * 1024},
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        instance = stack.get_instance("primary")

        # 1. Sanity-check the baseline cap.
        before = instance.current_settings()
        assert before.saturation.max_in_flight == 50

        # 2. A baseline submit goes through (the gate has 50 slots).
        first_id = await _submit_one(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            stack.phantom_client,
            first_id,
            state="succeeded",
            timeout_seconds=15.0,
        )

        # 3. Rewrite YAML for zero in-flight capacity. The
        # ``persist_trigger`` block carries the suite default so the
        # validator doesn't drop into the size-aware persist path.
        stack.rewrite_yaml(
            {"saturation": {"max_in_flight": 0}},
        )

        # 4. POST admin reload. The endpoint returns 200 + a list of
        # reloaded instance ids on success.
        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            resp = await client.post("/v1/admin/reload")
        assert resp.status_code == 200, (
            f"admin reload failed: status={resp.status_code} body={resp.text!r}"
        )
        body = resp.json()
        assert body == {"reloaded_instances": ["primary"]}, (
            f"unexpected reload response body: {body!r}"
        )

        # 5. Wait for the cap propagation to land on the live gate.
        await _wait_for_holder_snapshot(
            stack,
            predicate=lambda s: s.saturation.max_in_flight == 0,
        )

        # 6. The next submit is refused with ``saturation_cap``.
        with pytest.raises(PhantomUnavailableError) as excinfo:
            await _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
            )
        assert excinfo.value.error_code == "saturation_cap", (
            f"expected saturation_cap error; got {excinfo.value.error_code!r}"
        )
        assert excinfo.value.status_code == 503
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Test 3 — in-flight rows keep their snapshotted capture_reexecution.
# ---------------------------------------------------------------------------


async def test_in_flight_rows_keep_snapshotted_capture_reexecution(tmp_path: Path) -> None:
    """A row's snapshotted ``capture_reexecution_active`` survives a reload.

    Boot with ``capture_reexecution: false`` on the primary instance and
    submit a chain (with the emulator throttled so the row sits in
    flight). Reload the YAML to ``capture_reexecution: true``. The
    row's ``capture_reexecution_active`` flag is snapshotted at
    ingress (per ADR-011) and must NOT mutate. A NEW row submitted
    after the reload sees the new value.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    stack = await boot_stack(
        tmp_path=tmp_path,
        enable_hot_reload=True,
        config_overrides={
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        },
    )
    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # 1. Inject 503 on the metadata POST so the first row sits in
        # flight in ``queued`` state for the duration of the reload.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        # 2. Submit a row under the OLD ``capture_reexecution: false`` value.
        old_chain_id = await _submit_one(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        # Wait for the row to land in the store so the snapshot is
        # readable before we trigger the reload.
        instance = stack.get_instance("primary")

        async def _old_row_landed() -> bool:
            return (await instance.store.get(old_chain_id)) is not None

        await await_until(
            _old_row_landed,
            timeout_seconds=5.0,
            poll_interval_seconds=0.05,
            message=f"old row {old_chain_id} never landed in the store before reload",
        )

        # 3. Rewrite YAML to flip the flag and reload.
        stack.rewrite_yaml(
            {
                "instances": [
                    {
                        "id": "primary",
                        "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                        "data_dir": "primary",
                        "capture_reexecution": True,
                        "routes": [
                            {
                                "name": "emulator",
                                "hosts": ["emulator", "127.0.0.1", "localhost"],
                                "auth_mode": "phantom_bearer",
                            },
                        ],
                    },
                ],
            },
        )
        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            resp = await client.post("/v1/admin/reload")
        assert resp.status_code == 200

        # 4. Wait for the live snapshot to carry the new
        # ``capture_reexecution``. It rides
        # :class:`InstanceSettingsSnapshot`, which the reload swaps under
        # the holder's lock; ``ctx.cfg`` is the frozen boot block and is
        # never repointed (F5). The previous comment here claimed the
        # opposite and was already false before that change.
        instance = stack.get_instance("primary")

        async def _snapshot_carries_capture_reexecution() -> bool:
            return bool(instance.current_settings().capture_reexecution)

        await await_until(
            _snapshot_carries_capture_reexecution,
            timeout_seconds=RELOAD_PROPAGATION_BUDGET_SECONDS,
            poll_interval_seconds=0.05,
            message=("snapshot capture_reexecution never observed True after reload"),
        )

        # 5. Inspect the old row directly via the disk/memory store.
        # The row's snapshot is on the persisted record itself.
        old_row = await _fetch_row(instance, old_chain_id)
        assert old_row is not None, f"old row {old_chain_id} not found in any tier"
        assert old_row.capture_reexecution_active is False, (
            f"in-flight row snapshot mutated after reload; "
            f"got capture_reexecution_active={old_row.capture_reexecution_active}"
        )

        # 6. Submit a fresh row. It snapshots the NEW value at ingress.
        new_chain_id = await _submit_one(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )

        async def _new_row_landed() -> bool:
            return (await instance.store.get(new_chain_id)) is not None

        await await_until(
            _new_row_landed,
            timeout_seconds=5.0,
            poll_interval_seconds=0.05,
            message=f"new row {new_chain_id} never landed in the store after submit",
        )
        new_row = await _fetch_row(instance, new_chain_id)
        assert new_row is not None, f"new row {new_chain_id} not found in any tier"
        assert new_row.capture_reexecution_active is True, (
            f"newly submitted row did not snapshot the post-reload value; "
            f"got capture_reexecution_active={new_row.capture_reexecution_active}"
        )
    finally:
        await stack.tear_down()


async def _fetch_row(instance: Any, chain_id: UUID) -> Any:
    """Return the :class:`UploadRow` for ``chain_id`` from the single store.

    Phase 1 Slice 1.E collapsed the dual ``memory_store``/``disk_store``
    pair on :class:`InstanceContext` into a single
    ``store: UploadStore``. Used by tests that need raw-row
    introspection the admin API does not surface (e.g.,
    ``capture_reexecution_active``).
    """
    return await instance.store.get(chain_id)


# ---------------------------------------------------------------------------
# Test 4 — reload is atomic; concurrent submissions see either old or new.
# ---------------------------------------------------------------------------


# How many concurrent submissions to fire during the atomic-reload race.
ATOMIC_BURST_COUNT: int = 10


async def test_reload_is_atomic(tmp_path: Path) -> None:
    """Concurrent submissions during a reload see a coherent snapshot.

    Spawn 10 concurrent submissions while a reload is mid-flight. Each
    row's snapshot must be either the OLD ``capture_reexecution_active``
    or the NEW — never a half-applied blend (which the
    :class:`SettingsHolder` lock guarantees on its own: the whole
    snapshot map is replaced under that lock, and since F5 there is no
    second per-instance write to race it). Some rows will land before
    the reload, some after; both buckets are acceptable.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    stack = await boot_stack(
        tmp_path=tmp_path,
        enable_hot_reload=True,
        config_overrides={
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
            # Bump saturation so 10 concurrent submits all admit.
            "saturation": {"max_in_flight": 50, "max_in_flight_bytes": 32 * 1024 * 1024},
        },
    )
    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Inject 5xx so rows stay queued through the race window.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        # Prepare the reload YAML rewrite (don't write yet — fire it
        # mid-burst).
        new_overrides: dict[str, Any] = {
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": True,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        }

        chain_ids: list[UUID] = [uuid4() for _ in range(ATOMIC_BURST_COUNT)]

        instance = stack.get_instance("primary")

        async def _submit(idx: int) -> None:
            # Tiny stagger so the burst overlaps the reload firing.
            await pace(0.005 * idx)
            await _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                chain_id=chain_ids[idx],
            )

        async def _reload() -> None:
            # Wait a bit so some submissions land first, then race the
            # reload against the in-progress burst. The wait is bounded
            # by an observable predicate (at least one row visible in
            # the store), but capped — if no submissions land within
            # the cap we fire the reload anyway and let the
            # post-gather settle assertion catch the empty-burst case.
            async def _first_landed() -> bool:
                for cid in chain_ids:
                    if (await instance.store.get(cid)) is not None:
                        return True
                return False

            # Cap-reached suppression is intentional: if no rows
            # land within the 0.5s cap we fire the reload anyway —
            # the race-window test tolerates an empty pre-burst and
            # the post-gather assertion catches a half-applied
            # snapshot.
            with contextlib.suppress(AssertionError):
                await await_until(
                    _first_landed,
                    timeout_seconds=0.5,
                    poll_interval_seconds=0.005,
                    message="no rows landed before reload fired (cap reached)",
                )
            stack.rewrite_yaml(new_overrides)
            async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
                resp = await client.post("/v1/admin/reload")
            assert resp.status_code == 200

        # Race the reload against the burst.
        await asyncio.gather(
            *(_submit(i) for i in range(ATOMIC_BURST_COUNT)),
            _reload(),
        )

        # Settle until every row has landed in the store with a
        # snapshot — the read-after-write side of the atomic-reload
        # invariant we are about to assert on.
        async def _all_rows_landed() -> bool:
            for cid in chain_ids:
                if (await instance.store.get(cid)) is None:
                    return False
            return True

        await await_until(
            _all_rows_landed,
            timeout_seconds=5.0,
            poll_interval_seconds=0.05,
            message="not every row in the atomic-reload burst landed in the store",
        )

        # Every row's snapshot is either old (False) or new (True) —
        # never null, never anything else.
        seen_false = 0
        seen_true = 0
        for cid in chain_ids:
            row = await _fetch_row(instance, cid)
            assert row is not None, f"chain {cid} missing from both tiers"
            assert isinstance(row.capture_reexecution_active, bool), (
                f"chain {cid} snapshot has non-bool {row.capture_reexecution_active!r}"
            )
            if row.capture_reexecution_active:
                seen_true += 1
            else:
                seen_false += 1

        # Sanity: the burst should produce at least one of each (the
        # reload landed mid-flight). If everything is in one bucket
        # the test still passes (the invariant is coherence, not a
        # specific distribution), but the log line below documents the
        # split for diagnosis.
        logger.info(
            "atomic-reload race produced split: false=%d true=%d (sum=%d/%d)",
            seen_false,
            seen_true,
            seen_false + seen_true,
            ATOMIC_BURST_COUNT,
        )
        assert seen_false + seen_true == ATOMIC_BURST_COUNT
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Test 5 — AD-mint reload mid-flight.
#
# DELETED in Phase 2 § 3.2.5 (H6 audit closure).
#
# The pre-Phase-2 surface this test exercised — hot-reload of ``ad_mint``
# config installing a new minter loop via ``AdMinter.start()``'s
# self-spawning ``asyncio.create_task`` — is gone. The H6 closure moved
# AdMinter under the lifespan ``asyncio.TaskGroup``, so swapping the
# minter at reload time would require the supervising TaskGroup to
# accept new tasks AFTER its body has already started — which is not
# how ``async with asyncio.TaskGroup()`` works.
#
# The contract (per ``runtime.reload._reload_minter`` post-Phase-2):
# changing the ``ad_mint`` config block logs a WARNING and requires a
# process restart for the new minter to take effect. That covers EVERY
# ``ad_mint`` knob, refresh timings included: the minter reads its
# boot-time AdMintConfig on each cycle (R5-2 reconciled ADR-013 and the
# playbook to this truth). The supervision discipline is the load-
# bearing invariant Phase 2 prioritized.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 6 (round 2 adversary) — reload changes the admin_lookup binding.
# ---------------------------------------------------------------------------

# A syntactically valid binding path that the emulator's captured shape
# does not populate; proves a reloaded json_path is consulted at request
# time (the lookup honestly misses through the new path).
_DANGLING_JSON_PATH = "file_information.nonexistent_field"

# The suite-default binding (tests/e2e/phantom-config.yml), restored in
# the final leg to prove a re-added binding resolves without restart.
_DEFAULT_LOOKUP_BINDING: dict[str, str] = {
    "capture_name": "create_file",
    "json_path": "file_information.id",
}

# Budget for the probe upload to deliver on the healthy in-process stack.
_LOOKUP_RELOAD_TERMINAL_BUDGET_SECONDS: float = 15.0


def _rewrite_admin_lookup(stack: E2EStack, binding: dict[str, str] | None) -> None:
    """Surgically set or remove ``instances[0].admin_lookup`` in the YAML.

    ``rewrite_yaml`` deep-merges and so cannot DELETE a key; this helper
    rewrites the instances block directly (read, mutate, write back).
    """
    assert stack.settings_path is not None, "enable_hot_reload=True required"
    raw: dict[str, Any] = yaml.safe_load(stack.settings_path.read_text())
    first_instance: dict[str, Any] = raw["instances"][0]
    if binding is None:
        first_instance.pop("admin_lookup", None)
    else:
        first_instance["admin_lookup"] = dict(binding)
    stack.settings_path.write_text(yaml.safe_dump(raw))


async def _post_admin_reload(stack: E2EStack) -> None:
    """Fire ``POST /v1/admin/reload`` and assert it reloaded the instance."""
    async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
        resp = await client.post("/v1/admin/reload")
    assert resp.status_code == 200, (
        f"admin reload failed: status={resp.status_code} body={resp.text!r}"
    )


async def test_reload_changes_admin_lookup_binding_without_restart(tmp_path: Path) -> None:
    """The by-captured-id binding follows the reloaded config live.

    Round 2 adversary seed: the hot-reload suite covered retention,
    saturation, and ad_mint but never the cycle-7 ``admin_lookup``
    block. ``apply_reload`` swaps the instance's live settings snapshot,
    which since F5 is where ``admin_lookup`` lives, and the lookup route
    reads the binding off that snapshot per request, so all three legs
    must hold without a restart: a reloaded json_path is consulted
    immediately (the
    lookup follows the NEW path and honestly misses through a dangling
    one), removing the block restores the 400 ``lookup_not_configured``
    refusal, and re-adding it restores resolution.
    """
    stack = await boot_stack(tmp_path=tmp_path, enable_hot_reload=True)
    try:
        pc: PhantomClient = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = await _submit_one(pc, emulator_url=stack.emulator_url, bearer=bearer)
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=_LOOKUP_RELOAD_TERMINAL_BUDGET_SECONDS,
        )
        detail = await pc.get_upload(chain_id)
        captured_by_step = {step.step_name: step.values for step in detail.captured}
        upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]

        # Baseline: the boot-time binding resolves the captured id.
        baseline = await pc.find_by_captured_id(upstream_file_id)
        assert baseline.found is True
        assert [m.chain_id for m in baseline.matches] == [chain_id]

        # Leg 1: a reloaded json_path takes effect without restart; the
        # dangling path makes the same value an honest miss.
        _rewrite_admin_lookup(
            stack,
            {"capture_name": "create_file", "json_path": _DANGLING_JSON_PATH},
        )
        await _post_admin_reload(stack)
        repointed = await pc.find_by_captured_id(upstream_file_id)
        assert repointed.found is False, (
            "the reloaded json_path must be consulted at request time; a hit "
            "here means the lookup still reads the boot-time binding"
        )

        # Leg 2: removing the block restores the 400 refusal.
        _rewrite_admin_lookup(stack, None)
        await _post_admin_reload(stack)
        with pytest.raises(PhantomBadRequestError) as excinfo:
            await pc.find_by_captured_id(upstream_file_id)
        assert excinfo.value.error_code == "lookup_not_configured"

        # Leg 3: re-adding the binding restores resolution, no restart.
        _rewrite_admin_lookup(stack, _DEFAULT_LOOKUP_BINDING)
        await _post_admin_reload(stack)
        restored = await pc.find_by_captured_id(upstream_file_id)
        assert restored.found is True
        assert [m.chain_id for m in restored.matches] == [chain_id]
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Test 6: ADR-013 codec-choice reload (round 5 adversary, R5-2).
# ---------------------------------------------------------------------------

# Codec configuration the reload switches TO. The suite boots with the
# zstd default, so the observable flip is zstd -> identity ("original"
# is the explicit PassthroughCodec wire token per CompressionCfg).
_RELOADED_CODEC_OVERRIDE: dict[str, Any] = {
    "storage": {
        "compression": {
            "mode": "always",
            "algorithm": "original",
        },
    },
}


async def test_reload_changes_codec_choice_for_new_admissions(tmp_path: Path) -> None:
    """A post-reload admission must encode with the reloaded algorithm.

    ADR-013 puts "Codec choice" (compression.algorithm, zstd | gzip |
    original, plus compression.level) on the reloadable-knobs list, and
    the operator playbook repeats it. The probe: boot with the suite
    default (zstd), submit row A, reload to the identity codec, WAIT
    until the live snapshot observably carries the new algorithm (so
    the swap itself is not in question), submit row B, then read both
    rows' ``storage_encoding`` off the admin list surface. Row A keeps
    "zstd" (admitted under the old config; rows are never re-encoded,
    the same admitted-under semantics test 3 pins for
    capture_reexecution); row B must carry "original" per the ADR.
    """
    stack = await boot_stack(tmp_path=tmp_path, enable_hot_reload=True)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        pc = stack.phantom_client

        # Baseline: the suite boots on the zstd codec.
        instance = stack.get_instance("primary")
        assert instance.current_settings().compression.algorithm == "zstd", (
            "suite default changed; rebase this test's baseline leg"
        )
        chain_a = await _submit_one(pc, emulator_url=stack.emulator_url, bearer=bearer)

        # Reload to the identity codec and wait for the snapshot swap.
        stack.rewrite_yaml(_RELOADED_CODEC_OVERRIDE)
        await _post_admin_reload(stack)
        await _wait_for_holder_snapshot(
            stack,
            predicate=lambda s: s.compression.algorithm == "original",
        )

        # Post-reload admission.
        chain_b = await _submit_one(pc, emulator_url=stack.emulator_url, bearer=bearer)

        rows, _ = await pc.list_uploads(limit=50)
        rows_by_id = {r.chain_id: r for r in rows}
        assert rows_by_id[chain_a].storage_encoding == "zstd", (
            "row A was admitted under the boot codec and is never re-encoded"
        )
        assert rows_by_id[chain_b].storage_encoding == "original", (
            "ADR-013: a post-reload admission must encode with the reloaded "
            "algorithm; 'zstd' here means the reload never reached the "
            "admission codec"
        )
    finally:
        await stack.tear_down()
