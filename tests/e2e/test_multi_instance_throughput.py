"""Multi-instance + multi-emulator throughput: the NORMAL happy path under load.

The owner's round-6 checkpoint asked convergence to prove multi-instance is
GENUINELY supported (several Phantom instances forwarding to several distinct
upstream emulators), with the normal multi-host happy path and sustained
throughput correctness covered FIRST, not only the weird edges. The existing
multi-instance e2e files cover dispatch correctness and per-instance isolation
for ONE upload each (``test_e2e_20_multi_instance_dispatch``,
``test_e2e_multi_instance_hostname_dispatch``,
``test_aggressor_cross_instance_isolation``); none drives a SUSTAINED concurrent
burst across several instances and asserts every upload delivers byte-identically
to its own upstream with no cross-talk. This file closes that throughput gap.

Harness shape (confirmed by inspecting ``helpers/stack.boot_stack``): the
in-process stack stands up N emulators via ``extra_emulators=N-1``, each on its
own ephemeral loopback port, and a Phantom configured with N instances. Routing
uses the ``X-Phantom-Instance`` header (``SubmitOptions(instance_id=...)``)
because the in-process harness binds every emulator on ``127.0.0.1`` differing
only by port, and the dispatcher matches hostname IGNORING port (the genuine
no-header hostname path needs distinct loopback IPs, which only bind on Linux;
that path is covered by ``test_e2e_multi_instance_hostname_dispatch`` on Linux
CI). Header routing is the established multi-instance-under-one-loopback pattern
(E2E-20, the isolation aggressor) and is orthogonal to throughput: it proves N
instances each running a full real upload pipeline to N real upstreams
concurrently.

This is a CORRECTNESS-under-load test, deliberately NOT marked ``perf``: the
``perf`` marker is for tight synthetic-latency budgets that a loaded host makes
flaky. Here there is no wall-clock budget; the assertion is that under a
sustained concurrent burst spread across every instance, EVERY upload reaches
``succeeded`` and lands byte-identically (SHA-256) on its OWN upstream, and no
upload bleeds to a sibling upstream. It runs in the default e2e-light lane.

Falsifier: a dispatch/throughput regression that drops, misroutes, or corrupts
an upload under concurrency makes some chain miss ``succeeded`` within budget, or
makes an emulator receive a body whose hash is not the one routed to it, or makes
a sibling emulator receive a foreign chain -> RED.

Cycle-7 extension (plan 06_09 task 7.2): the burst now also exercises the NEW
read surfaces under load. Each instance's uploads share a per-instance query
group; while the burst delivers, a prober loop hammers the group rollup, the
by-local-uuid lookup, and the quarantine inventory (truthfully empty) and
collects any fault; after the burst, the rollup / both identifier lookups /
the inventory must report the settled multi-instance truth (every instance's
binding resolves its OWN chains, fan-out included).
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import secrets
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.auth.modes import AuthMode

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

pytestmark = pytest.mark.e2e

# Several instances (3) against several emulators (3). "Several" per the owner's
# brief; 3 is enough to prove N-to-N without making the in-process burst slow.
_INSTANCE_IDS: tuple[str, ...] = ("alpha", "bravo", "charlie")

# Uploads PER instance in the sustained burst. 8 x 3 instances = 24 concurrent
# real upload pipelines (create-file POST + body PUT each), enough to exercise
# sustained multi-instance throughput while staying fast in the default lane.
_UPLOADS_PER_INSTANCE: int = 8

# Body size: large enough that a misrouted/truncated body is unmistakable on the
# SHA-256, small enough to keep the in-process burst quick.
_BODY_BYTES: int = 2 * 1024

_SHARED_SUB: str = "00000000-0000-0000-0000-0000000000c0"

# Generous budget: 24 uploads through the in-process stack settle well under
# this; the value is a deadlock/regression backstop, not a latency assertion.
_TERMINAL_BUDGET_SECONDS: float = 45.0

# Seeded RNG for the mid-burst read prober so the op mix is reproducible.
_PROBE_RNG_SEED: int = 11


@dataclass(frozen=True)
class _SubmittedUpload:
    """One submitted upload's identity, for post-burst delivery verification.

    Attributes:
        chain_id: The chain's id (also stamped as ``phantom_local_uuid``).
        instance_id: The instance the upload was routed to.
        body_hash: SHA-256 hex of the exact body bytes submitted, asserted
            against the emulator's accepted-body hash for byte-identity.
    """

    chain_id: UUID
    instance_id: str
    body_hash: str


def _multi_instance_overrides() -> dict[str, object]:
    """``config_overrides`` declaring N fully-isolated instances.

    Each instance gets its own ``data_dir`` and a route with ``auth_mode: none``
    (each in-process emulator has its own ephemeral issuer, so a single JWT
    cannot satisfy them all; dropping upstream auth keeps the test focused on
    throughput/dispatch, matching E2E-20 and the isolation aggressor). The
    ``host_prefixes`` are placeholder per-instance names; routing is forced by
    the ``X-Phantom-Instance`` header, so they only need to be distinct.

    Cycle-7: every instance carries the ``admin_lookup`` binding so the
    by-captured-id lookup (scoped AND fan-out) is exercisable under the
    multi-instance topology.
    """
    return {
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


async def _submit_to_instance(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
    instance_id: str,
    group_id: UUID | None = None,
) -> None:
    """Submit one two-step upload routed to ``instance_id`` via the header.

    ``group_id`` (cycle-7) optionally tags the upload into a query group
    so the rollup surface has real multi-row groups to aggregate.
    """
    request = build_create_file_request(file_name=f"thru_{chain_id.hex[:12]}")
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
        options=SubmitOptions(instance_id=instance_id, group_id=group_id),  # type: ignore[call-arg]
    )


async def test_sustained_multi_instance_burst_delivers_byte_identically(tmp_path: Path) -> None:
    """N instances each forward a sustained burst to their OWN upstream, correctly.

    The normal multi-host happy path under load: every upload across every
    instance reaches ``succeeded`` and lands byte-identically (SHA-256) on the
    emulator that owns its instance, with zero cross-talk between upstreams.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=len(_INSTANCE_IDS) - 1,  # primary + extras = N emulators
        config_overrides=_multi_instance_overrides(),
    )
    try:
        pc = stack.phantom_client

        # Pair each instance id with its emulator (primary first, then extras in
        # order). The instance list and the emulator list are both index-stable.
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

        # Build the full burst: _UPLOADS_PER_INSTANCE distinct bodies per
        # instance, each with a unique chain_id and a recorded SHA-256,
        # all of an instance's uploads sharing its query group (cycle-7).
        groups: dict[str, UUID] = {instance_id: uuid4() for instance_id in _INSTANCE_IDS}
        submitted: list[_SubmittedUpload] = []
        submit_coros = []
        for idx, instance_id in enumerate(_INSTANCE_IDS):
            emu_url = emulator_urls[idx]
            for _ in range(_UPLOADS_PER_INSTANCE):
                chain_id = uuid4()
                body = secrets.token_bytes(_BODY_BYTES)
                submitted.append(
                    _SubmittedUpload(
                        chain_id=chain_id,
                        instance_id=instance_id,
                        body_hash=hashlib.sha256(body).hexdigest(),
                    )
                )
                submit_coros.append(
                    _submit_to_instance(
                        pc,
                        chain_id=chain_id,
                        body=body,
                        emulator_url=emu_url,
                        bearer=bearer,
                        instance_id=instance_id,
                        group_id=groups[instance_id],
                    )
                )

        # Fire the whole burst CONCURRENTLY (sustained multi-instance load):
        # admission must atomically claim every chain across all instances.
        await asyncio.gather(*submit_coros)

        # Every upload must reach succeeded (delivered to its upstream)
        # while a prober loop (cycle-7 task 7.2) hammers the new read
        # surfaces against the live burst and collects any fault.
        read_faults: list[str] = []
        burst_settled = asyncio.Event()

        async def _probe_read_surfaces_until_settled() -> None:
            """Hammer rollup/lookup/inventory while deliveries churn."""
            rng = random.Random(_PROBE_RNG_SEED)
            by_instance = {
                instance_id: [u for u in submitted if u.instance_id == instance_id]
                for instance_id in _INSTANCE_IDS
            }
            while not burst_settled.is_set():
                instance_id = _INSTANCE_IDS[rng.randrange(len(_INSTANCE_IDS))]
                mine = by_instance[instance_id]
                try:
                    rollup = await pc.get_group_status(groups[instance_id], instance=instance_id)
                    assert rollup.total == len(mine), (
                        f"{instance_id} rollup total {rollup.total} != {len(mine)}"
                    )
                    assert sum(rollup.counts_by_state.values()) == rollup.total
                    target = rng.choice(mine)
                    found = await pc.find_by_local_uuid(target.chain_id)
                    assert found.found is True, f"admitted {target.chain_id} must be findable"
                    assert found.matches[0].instance_id == instance_id
                    inventory = await pc.get_quarantine_inventory(instance=instance_id)
                    assert inventory.quarantines == [], "a clean burst quarantines nothing"
                except AssertionError as exc:
                    read_faults.append(str(exc))
                except Exception as exc:  # probe collector; re-surfaced below
                    read_faults.append(f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(0)  # pre-commit-allow: sleep (zero-second yield)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(_probe_read_surfaces_until_settled())
            try:
                await asyncio.gather(
                    *(
                        assert_chain_reaches_state(
                            pc,
                            u.chain_id,
                            state="succeeded",
                            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
                        )
                        for u in submitted
                    )
                )
            finally:
                burst_settled.set()
        assert not read_faults, (
            f"{len(read_faults)} read-surface fault(s) under the burst: {read_faults[:10]}"
        )

        # Per-emulator byte-identity + no cross-talk: each emulator received
        # EXACTLY the chains routed to its instance, each with the right hash,
        # and no foreign chain.
        for idx, instance_id in enumerate(_INSTANCE_IDS):
            emu = emulators[idx]
            expected = {u.chain_id: u.body_hash for u in submitted if u.instance_id == instance_id}
            received = emu.received()
            received_by_uuid = {e.metadata_kvs["phantom_local_uuid"]: e.body_hash for e in received}

            # Every expected chain arrived, byte-identically (SHA-256 match).
            for chain_id, body_hash in expected.items():
                got_hash = received_by_uuid.get(str(chain_id))
                assert got_hash is not None, (
                    f"instance {instance_id}: chain {chain_id} never reached its emulator "
                    f"(received {len(received)} entries)"
                )
                assert got_hash == body_hash, (
                    f"instance {instance_id}: chain {chain_id} body corrupted in flight; "
                    f"expected sha256={body_hash[:12]}..., emulator accepted {got_hash[:12]}..."
                )

            # No foreign chain: every uuid this emulator accepted belongs to
            # THIS instance (the other instances' chains must not bleed here).
            foreign = received_by_uuid.keys() - {str(c) for c in expected}
            assert not foreign, (
                f"instance {instance_id}'s emulator received foreign chains {foreign} "
                f"under concurrent multi-instance load (cross-talk)"
            )

        # Admin truth: each instance's scope lists its own rows and only those.
        for instance_id in _INSTANCE_IDS:
            rows, _ = await pc.list_uploads(instance=instance_id)
            scope_ids = {r.chain_id for r in rows}
            mine = {u.chain_id for u in submitted if u.instance_id == instance_id}
            assert mine <= scope_ids, (
                f"instance {instance_id} scope missing its own rows: {mine - scope_ids}"
            )
            others = {u.chain_id for u in submitted if u.instance_id != instance_id}
            leaked = others & scope_ids
            assert not leaked, f"instance {instance_id} scope leaked sibling rows: {leaked}"

        # Cycle-7 settled truth on the new read surfaces, per instance.
        for instance_id in _INSTANCE_IDS:
            mine = {u.chain_id for u in submitted if u.instance_id == instance_id}

            # The group rollup (unscoped: the fan-out finds the group on
            # exactly one instance) reports the finished burst.
            rollup = await pc.get_group_status(groups[instance_id])
            assert rollup.total == _UPLOADS_PER_INSTANCE
            assert rollup.all_finished is True
            assert rollup.counts_by_state["succeeded"] == _UPLOADS_PER_INSTANCE
            assert rollup.last_sent_at is not None
            assert {m.chain_id for m in rollup.members} == mine
            for member in rollup.members:
                assert member.sent_at is not None, "delivered members carry sent_at"

            # by-captured-id resolves a delivered chain back through its
            # OWN instance's binding, scoped and fan-out (emulator file
            # ids are uuid4, so cross-emulator collisions cannot occur).
            probe = next(u for u in submitted if u.instance_id == instance_id)
            detail = await pc.get_upload(probe.chain_id)
            captured_by_step = {step.step_name: step.values for step in detail.captured}
            upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]
            assert isinstance(upstream_file_id, str)
            scoped = await pc.find_by_captured_id(upstream_file_id, instance=instance_id)
            assert scoped.found is True
            assert [m.chain_id for m in scoped.matches] == [probe.chain_id]
            fanout = await pc.find_by_captured_id(upstream_file_id)
            assert [m.chain_id for m in fanout.matches] == [probe.chain_id]
            assert fanout.matches[0].instance_id == instance_id
    finally:
        await stack.tear_down()
