"""Cycle-7 phase 5 acceptance: the four new SDK methods over the live stack.

Drives the real Phantom + emulator stack purely through the public SDK
surface (plan 06_09 task 5.1, acceptance leg "SDK method tests against
the emulator"):

1. ``submit_chain`` with ``SubmitOptions(group_id=..., multifile_id=...,
   order=...)``: the renamed grouping headers ride the wire end to end.
2. ``poll_group_until_finished``: the group is held un-finished by an
   injected upstream 5xx, the poll observes ``all_finished=False``
   mid-flight, the failure is lifted, and the poll exits on the flip.
3. ``get_group_status``: rollup fields (total, histogram, member
   ``send_order`` / ``multifile_id`` / ``sent_at``, ``last_sent_at``),
   the singleton default group, and the 404 miss.
4. ``find_by_local_uuid`` / ``find_by_captured_id``: hit and
   found=false legs; the captured-id binding comes from the stack's
   per-instance ``admin_lookup`` config (capture_name ``create_file``,
   json_path ``file_information.id``).

The full group lifecycle matrix (corrupted-member F1 end to end,
unconfigured-400, multi-instance straddling) is phase 7 scope; this
module pins the SDK plumbing for each method against the live wire.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_client.errors import PhantomNotFoundError
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver, build_in_memory_upload_envelope
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import DEFAULT_BODY, build_create_file_request
from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, EmulatorControl

# Upper bound on one upload reaching `succeeded` on the healthy path
# (mirrors the happy-path module's helper-default budget).
TERMINAL_STATE_BUDGET_SECONDS: float = 10.0

# Deadline for poll_group_until_finished AFTER the failure is lifted.
# Phantom's e2e retry strategy is fixed_intervals [0, 1, 2, 5, 10], so
# a member that failed twice while held down retries within ~2 s of the
# clear; 30 s gives the two-member group a generous cushion.
GROUP_POLL_BUDGET_SECONDS: float = 30.0

# How long the poll runs against the still-failing upstream before the
# test lifts the failure: longer than the poller's 0.5 s initial delay,
# so at least one all_finished=False rollup is observed mid-poll.
HELD_DOWN_OBSERVATION_SECONDS: float = 1.5

# 5xx error rate for the hold-down policy. 1.0 = every request fails.
FORCE_5XX_RATE: float = 1.0

# The eight canonical ChainState values (the rollup histogram always
# carries all of them, zero counts included).
ALL_STATES: frozenset[str] = frozenset(
    {
        "queued",
        "attempting",
        "succeeded",
        "failed",
        "auth_expired",
        "stored",
        "cancelled",
        "corrupted",
    }
)

pytestmark = pytest.mark.e2e


async def test_sdk_group_submit_poll_and_lookups(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Submit a group through the SDK, flip all_finished mid-poll, look both ids up."""
    emulator.clear_received()
    emulator.clear_failures()
    # Hold the upstream down so the group stays un-finished while the
    # poll starts (the mid-poll flip is the acceptance leg).
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.GLOBAL,
            error_rate_5xx=FORCE_5XX_RATE,
        )
    )

    group_id = uuid4()
    multifile_id = uuid4()
    try:
        first = await driver.in_memory_upload(
            build_create_file_request(file_name="group-member-0.bin"),
            DEFAULT_BODY,
            options=SubmitOptions(  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
                group_id=group_id, multifile_id=multifile_id, order=0
            ),
        )
        second = await driver.in_memory_upload(
            build_create_file_request(file_name="group-member-1.bin"),
            DEFAULT_BODY,
            options=SubmitOptions(  # type: ignore[call-arg]
                group_id=group_id, multifile_id=multifile_id, order=1
            ),
        )

        # get_group_status while held down: both members present, group
        # honestly NOT finished, full eight-state histogram.
        rollup = await phantom_client.get_group_status(group_id)
        assert rollup.group_id == group_id
        assert rollup.total == 2
        assert rollup.all_finished is False
        assert set(rollup.counts_by_state) == ALL_STATES
        assert {m.chain_id for m in rollup.members} == {first.id, second.id}
        assert rollup.last_sent_at is None, "nothing delivered while the upstream 5xxes"

        # poll_group_until_finished with the flip mid-poll: start the
        # poll against the failing upstream, observe it holding, then
        # lift the failure and let the poll exit on all_finished.
        deadline = datetime.now(tz=UTC) + timedelta(seconds=GROUP_POLL_BUDGET_SECONDS)
        async with asyncio.TaskGroup() as tg:
            poll = tg.create_task(
                phantom_client.poll_group_until_finished(group_id, deadline=deadline)
            )
            # Deliberate fixed window: the poll must be OBSERVED still
            # looping against the failing upstream before the flip; an
            # event-based wait cannot express "has not finished yet".
            await asyncio.sleep(HELD_DOWN_OBSERVATION_SECONDS)  # pre-commit-allow: sleep
            assert not poll.done(), (
                "the group poll must still be looping while every upstream call 5xxes"
            )
            emulator.clear_failures()
        final = poll.result()

        assert final.all_finished is True
        assert final.total == 2
        assert final.counts_by_state["succeeded"] == 2
        assert final.last_sent_at is not None
        members_by_id = {m.chain_id: m for m in final.members}
        assert members_by_id[first.id].send_order == 0
        assert members_by_id[second.id].send_order == 1
        for member in final.members:
            assert member.multifile_id == multifile_id
            assert member.sent_at is not None, "delivered members carry the sent_at stamp"
    finally:
        emulator.clear_failures()

    # find_by_local_uuid: hit leg (the driver stuffed phantom_local_uuid
    # into the metadata KVS; the service pins that exact envelope path).
    by_uuid = await phantom_client.find_by_local_uuid(first.id)
    assert by_uuid.kind == "local_uuid"
    assert by_uuid.found is True
    assert [m.chain_id for m in by_uuid.matches] == [first.id]
    hit = by_uuid.matches[0]
    assert hit.local_uuid == first.id
    assert hit.instance_id == "primary"
    assert hit.state == "succeeded"
    assert hit.multifile_id == multifile_id

    # find_by_local_uuid: miss is a 200 found=false answer, not an error.
    miss = await phantom_client.find_by_local_uuid(uuid4())
    assert miss.found is False
    assert miss.matches == []

    # find_by_captured_id: pull the upstream-assigned id off the captured
    # values, then resolve the upload back through the configured binding.
    detail = await phantom_client.get_upload(first.id)
    captured_by_step = {step.step_name: step.values for step in detail.captured}
    upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]
    assert isinstance(upstream_file_id, str)

    by_captured = await phantom_client.find_by_captured_id(upstream_file_id)
    assert by_captured.kind == "captured_file_id"
    assert by_captured.value == upstream_file_id
    assert by_captured.found is True
    assert [m.chain_id for m in by_captured.matches] == [first.id]
    assert by_captured.matches[0].captured_file_id == upstream_file_id

    # find_by_captured_id: miss leg.
    captured_miss = await phantom_client.find_by_captured_id("no-such-upstream-id")
    assert captured_miss.found is False


async def test_sdk_singleton_group_and_unknown_group_404(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """An optionless upload is a group of one; an unknown group 404s."""
    emulator.clear_received()
    emulator.clear_failures()

    result = await driver.in_memory_upload(
        build_create_file_request(file_name="singleton.bin"), DEFAULT_BODY
    )
    await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )

    # group_id defaulted to chain_id at admission: the chain_id resolves
    # to its self-evident singleton group.
    rollup = await phantom_client.get_group_status(result.id)
    assert rollup.total == 1
    assert rollup.all_finished is True
    assert rollup.members[0].chain_id == result.id
    assert rollup.members[0].multifile_id is None, "optionless submit stays standalone"
    assert rollup.members[0].send_order == 0

    # The rollup is the one lookup that 404s on a miss.
    with pytest.raises(PhantomNotFoundError):
        await phantom_client.get_group_status(uuid4())


# Round 3 adversary hardening (R2-3 judged in substance): the SDK
# quoted-key auto-encoding, the server's strict single-reading parser,
# and the store's quoted JSON1 path labels compose over a REAL daemon
# and real URL encoding, which the route-tier TestClient pins cannot
# prove. KVS pairs whose keys carry every path-special character class.
_SPECIAL_KVS_PAIRS: tuple[tuple[str, str], ...] = (
    ("source:left", "cal-7"),
    ("telemetry.v2", "on"),
    ('"odd', "yes"),
    ("back\\slash", "bs-1"),
)


async def test_sdk_find_by_metadata_special_keys_round_trip_the_wire(
    stack: E2EStack,
    phantom_client: PhantomClient,
) -> None:
    """Special-character KVS keys are addressable end to end via the SDK.

    One single-step chain (create_file only, so no KVS-derived
    ``x-amz-meta-*`` header rides a hostile name) carries four
    path-special KVS keys through real ingress; after delivery,
    ``find_by_metadata`` resolves each pair exactly through the live
    daemon. The pre-fix first-colon reading of the colon-bearing key
    stays an honest miss, and a wrong-value probe stays empty, pinning
    the no-union single-reading rule over the wire.
    """
    request = build_create_file_request(
        file_name="special-keys.bin",
        extra_metadata=dict(_SPECIAL_KVS_PAIRS),
    )
    local_uuid = uuid4()
    request.metadata.key_value_store["phantom_local_uuid"] = str(local_uuid)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=local_uuid,
    )
    # create_file only: the put_s3 step would translate each KVS key
    # into an x-amz-meta-* header NAME, where ':' and '"' are illegal.
    envelope = envelope.model_copy(update={"steps": [envelope.steps[0]]})
    response = await phantom_client.submit_chain(
        envelope,
        uid=DEFAULT_FAKE_SUB,
        auth_token=f"Bearer {stack.fake_security_token()}",
    )
    await assert_chain_reaches_state(
        phantom_client,
        response.chain_id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )

    for key, value in _SPECIAL_KVS_PAIRS:
        rows = await phantom_client.find_by_metadata(key=key, value=value)
        assert [row.chain_id for row in rows] == [response.chain_id], (
            f"KVS key {key!r} did not round-trip the live wire"
        )

    # Single-reading honesty on the live wire: the pre-fix reading of
    # the colon-bearing pair (key 'source', value 'left:cal-7') is an
    # exact miss, never a wrong-key hit; a wrong value is a miss too.
    assert await phantom_client.find_by_metadata(key="source", value="left:cal-7") == []
    assert await phantom_client.find_by_metadata(key="source:left", value="wrong") == []
