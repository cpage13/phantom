"""Multi-instance robustness: one upstream host down while the others stay healthy.

The owner's round-6 checkpoint asked, after the normal multi-host path and
throughput, for the EDGES: one host down while others stay healthy, admin
reporting correct per-instance truth, and mixed host behaviors. The existing
multi-instance e2e all run with EVERY upstream healthy; none drives a topology
where one instance's upstream is failing while its siblings deliver normally.
This file closes that gap.

Mixed host behaviors, all in one stack (three instances, three distinct
emulators, header-routed exactly as ``test_multi_instance_throughput`` and the
isolation aggressor):

- instance ``alpha`` -> a HEALTHY upstream (delivers normally),
- instance ``bravo`` -> a HEALTHY upstream (delivers normally),
- instance ``charlie`` -> a DOWN upstream (1:1 5xx; the chain buffers,
  undelivered, retrying).

The robustness contract Phantom must honor: one bad upstream is ISOLATED to its
own instance. The healthy instances reach ``succeeded`` and their bodies land
byte-identically on their own emulators; the down instance's chain stays
non-terminal (queued/attempting, never ``succeeded``, never lost) and never
bleeds to a healthy emulator; and admin reports the truth PER INSTANCE
(``list_uploads?instance=charlie`` shows the stuck row with attempts > 0 while
the healthy scopes show succeeded). Then the down host RECOVERS (failures
cleared) and charlie's buffered upload drains to ``succeeded`` and delivers
byte-identically. A buffered upload is parked through an upstream outage, never
dropped, exactly the durability Phantom exists to provide, proven under a
multi-instance mixed-health topology.

Public e2e-light lane: in-process stack, generic chain shapes,
no ``PHANTOM_ENABLED``.

Falsifier: if a bad upstream is NOT isolated (e.g. charlie's failures stall or
crash a healthy instance), alpha/bravo miss ``succeeded`` -> RED. If the stuck
upload is silently dropped or marked ``succeeded`` while undelivered, the
post-recovery delivery assertion (the emulator never receives it) -> RED. If
charlie's chain bleeds to a healthy emulator, the cross-talk assertion -> RED.

Cycle-7 extension (plan 06_09 task 7.2): the NEW read surfaces must report
the mixed-health truth too. While charlie is down: its singleton group rollup
reads all_finished=false with no sent_at; the by-local-uuid lookup finds the
stuck row (attempted, undelivered, captured_file_id honestly None because the
create step never succeeded); a healthy chain's rollup reads finished and its
captured id resolves through the by-captured-id binding. After recovery: the
down chain's rollup flips finished with the stamp, and its freshly captured
id resolves.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import ChainAdminDetail, PhantomClient, SubmitOptions
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

pytestmark = pytest.mark.e2e

# Three instances: two healthy upstreams, one down. Index 2 (charlie) is the
# DOWN instance; indices 0/1 (alpha/bravo) stay healthy.
_INSTANCE_IDS: tuple[str, ...] = ("alpha", "bravo", "charlie")
_DOWN_INDEX: int = 2

_BODY_BYTES: int = 2 * 1024
_SHARED_SUB: str = "00000000-0000-0000-0000-0000000000d0"

# A 1:1 5xx rate keeps charlie's admitted upload buffered (undelivered) for the
# whole window; clearing it lets the next retry complete.
_BLOCK_UPSTREAM = FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]

# A long, non-exhausting retry cadence so charlie's row stays RETRYABLE
# (queued/attempting) across the outage window rather than exhausting to
# ``stored``. Mirrors test_mode_switch_restore_then_delivers's durable cadence.
_RETRY_BUDGET_ATTEMPTS: int = 600
_DURABLE_RETRY: dict[str, object] = {
    "retry": {
        "worker_count": 4,
        "poll_interval_ms": 50,
        "default_strategy": {
            "type": "fixed_intervals",
            "intervals_seconds": [1] * _RETRY_BUDGET_ATTEMPTS,
        },
    },
    "retention": {"reaper_interval_seconds": 3600},
}

_HEALTHY_BUDGET_SECONDS: float = 30.0
_DELIVERY_BUDGET_SECONDS: float = 45.0
_STUCK_PROOF_SECONDS: float = 6.0
_POLL_SECONDS: float = 0.2


def _overrides() -> dict[str, object]:
    """``config_overrides``: durable retry + three isolated all_disk instances.

    Cycle-7: every instance carries the ``admin_lookup`` binding so the
    by-captured-id surface is exercisable in the mixed-health topology.
    """
    return {
        **_DURABLE_RETRY,
        "storage": {"body_store": {"mode": "all_disk"}},
        "instances": [
            {
                "id": instance_id,
                "host_prefixes": [f"emulator-{instance_id}"],
                "data_dir": instance_id,
                "capture_reexecution": False,
                "admin_lookup": {
                    "capture_name": "create_file",
                    "json_path": "file_information.id",
                },
                "routes": [
                    {
                        "name": f"{instance_id}_route",
                        "hosts": [f"emulator-{instance_id}", "127.0.0.1", "localhost"],
                        "auth_mode": "none",
                    },
                ],
            }
            for instance_id in _INSTANCE_IDS
        ],
    }


async def _captured_file_id_of(pc: PhantomClient, chain_id: UUID) -> str:
    """Read the upstream-assigned file id off a delivered chain's captures."""
    detail = await pc.get_upload(chain_id)
    captured_by_step = {step.step_name: step.values for step in detail.captured}
    upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]
    assert isinstance(upstream_file_id, str)
    return upstream_file_id


async def _submit(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
    instance_id: str,
) -> None:
    """Submit one two-step upload routed to ``instance_id`` via the header."""
    request = build_create_file_request(file_name=f"down_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=_SHARED_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(instance_id=instance_id),  # type: ignore[call-arg]
    )


async def _await_attempted(pc: PhantomClient, chain_id: UUID) -> ChainAdminDetail:
    """Poll until the chain has been attempted at least once (attempts > 0).

    Proves the row is actively being worked (and 5xx-bounced), not merely
    sitting un-dispatched, before we assert it stays non-terminal.
    """
    deadline = time.monotonic() + _HEALTHY_BUDGET_SECONDS
    detail = await pc.get_upload(chain_id)
    while time.monotonic() < deadline:
        detail = await pc.get_upload(chain_id)
        if detail.attempts > 0:
            return detail
        await asyncio.sleep(_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"down-instance chain {chain_id} was never attempted within "
        f"{_HEALTHY_BUDGET_SECONDS}s (attempts={detail.attempts}, state={detail.state})"
    )


async def test_one_upstream_down_isolates_to_its_instance_then_recovers(tmp_path: Path) -> None:
    """A down upstream is isolated to its instance; healthy siblings deliver; then it recovers.

    Mixed-health multi-instance topology: alpha/bravo deliver byte-identically
    while charlie's upstream 5xxes; charlie's upload stays buffered (never
    succeeded, never lost) and admin reports that truth per instance; then
    charlie's upstream recovers and the buffered upload drains and delivers.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=len(_INSTANCE_IDS) - 1,
        config_overrides=_overrides(),
    )
    try:
        pc = stack.phantom_client
        emulators: list[EmulatorControl] = [stack.emulator, *stack.extra_emulators]
        emulator_urls: list[str] = [stack.emulator_url, *stack.extra_emulator_urls]
        assert len(emulators) == len(_INSTANCE_IDS), (
            f"harness gap: asked for {len(_INSTANCE_IDS)} emulators, got {len(emulators)}"
        )

        for emu in emulators:
            emu.clear_received()
            emu.clear_failures()
            emu.set_auth_mode(AuthMode.NONE)
        bearer = stack.fake_security_token(sub=_SHARED_SUB)

        # Take charlie's upstream DOWN (1:1 5xx); alpha/bravo stay healthy.
        down_emu = emulators[_DOWN_INDEX]
        down_emu.inject_failure(_BLOCK_UPSTREAM)

        # Submit one upload to each instance.
        bodies: dict[str, bytes] = {}
        chains: dict[str, UUID] = {}
        for idx, instance_id in enumerate(_INSTANCE_IDS):
            body = secrets.token_bytes(_BODY_BYTES)
            chain_id = uuid4()
            bodies[instance_id] = body
            chains[instance_id] = chain_id
            await _submit(
                pc,
                chain_id=chain_id,
                body=body,
                emulator_url=emulator_urls[idx],
                bearer=bearer,
                instance_id=instance_id,
            )

        healthy_ids = [iid for i, iid in enumerate(_INSTANCE_IDS) if i != _DOWN_INDEX]
        down_id = _INSTANCE_IDS[_DOWN_INDEX]

        # 1. The HEALTHY instances deliver, byte-identically, despite the sibling
        #    outage (isolation: one bad upstream does not stall the others).
        for instance_id in healthy_ids:
            await assert_chain_reaches_state(
                pc, chains[instance_id], state="succeeded", timeout_seconds=_HEALTHY_BUDGET_SECONDS
            )
        for idx, instance_id in enumerate(_INSTANCE_IDS):
            if instance_id in healthy_ids:
                await assert_emulator_received(
                    emulators[idx],
                    phantom_local_uuid=str(chains[instance_id]),
                    body_size=len(bodies[instance_id]),
                )

        # 2. The DOWN instance's chain is actively retrying (attempts > 0) and
        #    has NOT reached succeeded: buffered, undelivered, not lost.
        down_detail = await _await_attempted(pc, chains[down_id])
        assert down_detail.state != "succeeded", (
            f"down instance {down_id} chain must not be succeeded while its upstream 5xxes; "
            f"state={down_detail.state}"
        )
        # Hold briefly and re-confirm it STAYS non-terminal (the 5xx keeps it
        # buffered; it must not silently flip to a terminal state).
        await asyncio.sleep(_STUCK_PROOF_SECONDS)  # pre-commit-allow: sleep
        held = await pc.get_upload(chains[down_id])
        assert held.state in {"queued", "attempting"}, (
            f"down instance {down_id} chain must remain retryable (queued/attempting) under a "
            f"sustained 5xx, got state={held.state} (attempts={held.attempts})"
        )
        assert held.attempts >= down_detail.attempts, (
            "the down chain should keep accruing attempts while the upstream 5xxes"
        )

        # 2b. Cycle-7 read surfaces report the mixed-health truth. The
        #     down chain's singleton group (group_id defaults to chain_id)
        #     is honestly unfinished with no stamp; the lookup finds the
        #     stuck row with captured_file_id None (its create step never
        #     succeeded, so nothing was captured).
        down_rollup = await pc.get_group_status(chains[down_id])
        assert down_rollup.all_finished is False, (
            "a buffered, retrying chain must keep its group unfinished"
        )
        assert down_rollup.total == 1
        assert down_rollup.last_sent_at is None
        assert down_rollup.members[0].sent_at is None
        by_uuid = await pc.find_by_local_uuid(chains[down_id])
        assert by_uuid.found is True
        stuck = by_uuid.matches[0]
        assert stuck.instance_id == down_id
        assert stuck.state in {"queued", "attempting"}
        assert stuck.sent_at is None
        assert stuck.attempts > 0, "the stuck row is being worked, not parked"
        assert stuck.captured_file_id is None, (
            "no create step succeeded on the down instance; a captured id here is a lie"
        )
        # A healthy sibling's surfaces read settled at the same moment.
        healthy_probe = healthy_ids[0]
        healthy_rollup = await pc.get_group_status(chains[healthy_probe])
        assert healthy_rollup.all_finished is True
        assert healthy_rollup.members[0].sent_at is not None
        healthy_captured = await _captured_file_id_of(pc, chains[healthy_probe])
        scoped = await pc.find_by_captured_id(healthy_captured, instance=healthy_probe)
        assert scoped.found is True
        assert [m.chain_id for m in scoped.matches] == [chains[healthy_probe]]
        fanout = await pc.find_by_captured_id(healthy_captured)
        assert [m.chain_id for m in fanout.matches] == [chains[healthy_probe]], (
            "the fan-out lookup must answer correctly even while a sibling upstream is down"
        )

        # 3. ADMIN per-instance truth: the down emulator never received the body
        #    (nothing delivered), and no healthy emulator received charlie's
        #    chain (no cross-talk under the mixed-health topology).
        down_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in down_emu.received()}
        assert str(chains[down_id]) not in down_uuids, (
            f"the down upstream must not have accepted {down_id}'s body (it 5xxed everything)"
        )
        for idx, instance_id in enumerate(_INSTANCE_IDS):
            if instance_id == down_id:
                continue
            sib_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in emulators[idx].received()}
            assert str(chains[down_id]) not in sib_uuids, (
                f"down instance {down_id}'s chain leaked to healthy instance {instance_id}'s "
                f"upstream (cross-talk under mixed health)"
            )

        # 4. Admin scoping still tells the truth per instance: each healthy scope
        #    shows its own succeeded row; the down scope shows its stuck row.
        for instance_id in healthy_ids:
            rows, _ = await pc.list_uploads(instance=instance_id)
            assert chains[instance_id] in {r.chain_id for r in rows}, (
                f"healthy instance {instance_id} scope is missing its own row"
            )
        down_rows, _ = await pc.list_uploads(instance=down_id)
        down_row_ids = {r.chain_id for r in down_rows}
        assert chains[down_id] in down_row_ids, (
            f"down instance {down_id} scope must still list its buffered row"
        )
        for instance_id in healthy_ids:
            assert chains[instance_id] not in down_row_ids, (
                f"healthy instance {instance_id}'s row leaked into the down scope {down_id}"
            )

        # 5. RECOVERY: clear charlie's upstream failure; the buffered upload
        #    drains to succeeded and delivers byte-identically. Parked through
        #    the outage, never dropped.
        down_emu.clear_failures()
        await assert_chain_reaches_state(
            pc, chains[down_id], state="succeeded", timeout_seconds=_DELIVERY_BUDGET_SECONDS
        )
        recv = await assert_emulator_received(
            down_emu,
            phantom_local_uuid=str(chains[down_id]),
            body_size=len(bodies[down_id]),
        )
        expected_hash = hashlib.sha256(bodies[down_id]).hexdigest()
        assert recv.body_hash == expected_hash, (
            f"recovered upload for {down_id} did not arrive byte-identically: "
            f"emulator hash {recv.body_hash[:12]}... != expected {expected_hash[:12]}..."
        )

        # 6. Cycle-7 read surfaces flip with the recovery: the rollup
        #    reads finished with the fresh stamp, and the id captured
        #    during recovery resolves through the binding.
        recovered_rollup = await pc.get_group_status(chains[down_id])
        assert recovered_rollup.all_finished is True
        assert recovered_rollup.counts_by_state["succeeded"] == 1
        assert recovered_rollup.last_sent_at is not None
        recovered_captured = await _captured_file_id_of(pc, chains[down_id])
        recovered_lookup = await pc.find_by_captured_id(recovered_captured, instance=down_id)
        assert recovered_lookup.found is True
        assert [m.chain_id for m in recovered_lookup.matches] == [chains[down_id]]
        assert recovered_lookup.matches[0].sent_at is not None
    finally:
        await stack.tear_down()
