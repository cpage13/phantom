"""Body-store MODE matrix vs the cycle-7 read surfaces (round 5).

Round 3 adjudicated the construction: the uploads DB (where group_id /
multifile_id / send_order / sent_at live) has zero body-mode awareness,
so the cycle-7 read surfaces are mode-independent BY CONSTRUCTION. This
module EXECUTES that claim over live daemons, one per non-default mode
(``all_ram`` and ``all_disk``; the suite default ``hybrid`` carries the
rest of the e2e suite). The mode-switch backup + restore legs of the
matrix are already pinned by test_mode_switch_back_up_and_run.py and
test_mode_switch_restore_then_delivers.py (parametrized across modes).

Per mode, over the wire:

* a two-member group parks buffered under an upstream 5xx wall; the
  rollup answers total=2, all_finished=False, the full eight-state
  histogram, null last_sent_at, and both members' send_order;
* ``find_by_metadata`` (key_value_match) and ``find_by_local_uuid``
  resolve the buffered members exactly;
* after the heal both members deliver: sent_at lands on every member,
  the rollup flips all_finished, and the emulator received each RAW
  body exactly once (the mode's body store fed the wire faithfully).
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import build_in_memory_upload_envelope
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

pytestmark = pytest.mark.e2e

# Every request 5xxes while the group must stay buffered.
_FORCE_5XX_RATE = 1.0
# Upper bound for one member reaching succeeded after the heal.
_TERMINAL_BUDGET_SECONDS = 15.0
# The KVS pair the metadata lookup resolves.
_KVS_KEY = "bench"
_KVS_VALUE = "left-rail"
# The e2e stack's fixed credential-cache axis value.
_UID = "00000000-0000-0000-0000-000000000001"
# The eight canonical ChainState values (the rollup histogram always
# carries all of them, zero counts included).
_ALL_STATES: frozenset[str] = frozenset(
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


def _body_for(member_index: int) -> bytes:
    """A distinct body per group member."""
    return (b"phantom-mode-matrix-payload-" + str(member_index).encode() + b"-") * 64


async def _submit_grouped(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
    options: SubmitOptions,
) -> None:
    """Submit one grouped member carrying the KVS pair + local uuid."""
    request = build_create_file_request(file_name=f"r5_mode_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    request.metadata.key_value_store[_KVS_KEY] = _KVS_VALUE
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=_UID,
        auth_token=f"Bearer {bearer}",
        options=options,
    )


@pytest.mark.parametrize("mode", ["all_ram", "all_disk"])
async def test_mode_matrix_rollup_lookups_sent_at(mode: str) -> None:
    """The cycle-7 read surfaces hold verbatim on an all_ram / all_disk daemon."""
    stack = await boot_stack(
        config_overrides={"storage": {"body_store": {"mode": mode}}},
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=_FORCE_5XX_RATE,
            )
        )

        group_id = uuid4()
        multifile_id = uuid4()
        member_ids = [uuid4(), uuid4()]
        bodies = {member_ids[i]: _body_for(i) for i in range(len(member_ids))}
        for order, chain_id in enumerate(member_ids):
            await _submit_grouped(
                pc,
                chain_id=chain_id,
                body=bodies[chain_id],
                emulator_url=stack.emulator_url,
                bearer=bearer,
                options=SubmitOptions(  # type: ignore[call-arg]
                    group_id=group_id, multifile_id=multifile_id, order=order
                ),
            )

        # Rollup truth while parked buffered in this mode's body store.
        rollup = await pc.get_group_status(group_id)
        assert rollup.group_id == group_id
        assert rollup.total == len(member_ids)
        assert rollup.all_finished is False
        assert set(rollup.counts_by_state) == _ALL_STATES
        assert rollup.last_sent_at is None
        members_by_id = {m.chain_id: m for m in rollup.members}
        assert set(members_by_id) == set(member_ids)
        for order, chain_id in enumerate(member_ids):
            member = members_by_id[chain_id]
            assert member.send_order == order
            assert member.multifile_id == multifile_id
            assert member.sent_at is None

        # Both identifier lookups resolve the buffered members exactly.
        by_kvs = await pc.find_by_metadata(key=_KVS_KEY, value=_KVS_VALUE)
        assert sorted(r.chain_id for r in by_kvs) == sorted(member_ids)
        by_uuid = await pc.find_by_local_uuid(member_ids[0])
        assert by_uuid.found is True
        assert [m.chain_id for m in by_uuid.matches] == [member_ids[0]]

        # Heal; the mode's body store feeds the wire; sent_at lands.
        emulator.clear_failures()
        for chain_id in member_ids:
            await assert_chain_reaches_state(
                pc, chain_id, state="succeeded", timeout_seconds=_TERMINAL_BUDGET_SECONDS
            )
        rollup = await pc.get_group_status(group_id)
        assert rollup.all_finished is True
        assert rollup.last_sent_at is not None
        for member in rollup.members:
            assert member.sent_at is not None
        for chain_id in member_ids:
            received = await assert_emulator_received(
                emulator,
                phantom_local_uuid=str(chain_id),
                body_size=len(bodies[chain_id]),
            )
            assert received.body_size == len(bodies[chain_id])
    finally:
        await stack.tear_down()
