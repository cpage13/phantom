"""Saturation refusals during a group burst leave NO phantom group members.

Round 4 adversary hardening (iteration loop, task 7.3). A burst of
grouped submissions hits the in-flight row cap mid-group. The contract
under attack:

* A refused submission (503 ``saturation_cap``, canonical envelope)
  admits NOTHING: no row, no idempotency claim, no group membership.
  The group rollup and ``list_by_group_id`` count only admitted rows.
* A live cap raise (``SaturationGate.update_caps``, the hot-reload
  push) opens admission immediately; a refused chain_id then resubmits
  CLEANLY into the same group (no ``chain_id_in_use``, no idempotency
  ghost from the refused attempt).
* ``update_caps`` racing a concurrent admit/release storm never breaks
  the accounting symmetry: counters equal grants minus releases at
  every settle point and drain back to zero.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from phantom.config.settings import SaturationCfg
from phantom.workers.saturation import AdmissionGranted, SaturationGate

from tests.unit.test_send_route import _build_app, _valid_envelope

# The send-route fixture builds its gate with max_in_flight=2: the
# third and fourth submissions of the burst refuse on the ROW cap.
_FIXTURE_ROW_CAP = 2
# Burst size: two admitted + two refused gives both refusal evidence
# and a survivor group to roll up.
_BURST_SIZE = 4
# The raised row cap for the mid-burst reload leg.
_RAISED_ROW_CAP = 4
# Wide byte/disk caps so only the row axis is load-bearing here.
_WIDE_BYTES_CAP = 1_000_000
# Storm sizing for the cap-flip gather: enough concurrency to
# interleave with two cap flips, small enough to stay instant.
_STORM_ADMITS = 50
_STORM_ROW_CAP = 10
_STORM_RAISED_CAP = 25
_STORM_LOWERED_CAP = 5
# Every storm admission accounts one byte: byte and row axes then
# mirror each other, so one equality check pins both.
_STORM_BODY_BYTES = 1


def _wide_caps(max_in_flight: int) -> SaturationCfg:
    """A SaturationCfg with every probe-fillable field explicit.

    ``update_caps`` requires all five fields non-None (the Settings
    validator fills them in production; tests fill them here).
    """
    return SaturationCfg(
        max_in_flight=max_in_flight,
        max_in_flight_bytes=_WIDE_BYTES_CAP,
        max_disk_bytes=_WIDE_BYTES_CAP,
        large_body_threshold_bytes=0,
        max_large_in_flight=0,
    )


def _post_grouped(
    client: TestClient,
    envelope: dict[str, Any],
    group_id: UUID,
    multifile_id: UUID,
    send_order: int,
) -> Any:
    """POST one grouped submission with the full cycle-7 header trio."""
    return client.post(
        "/v1/send",
        json=envelope,
        headers={
            "X-Phantom-Uid": "user-1",
            "X-Phantom-Group-Id": str(group_id),
            "X-Phantom-Multifile-Id": str(multifile_id),
            "X-Phantom-Order": str(send_order),
        },
    )


@pytest.mark.asyncio
async def test_saturation_refusals_leave_no_phantom_group_members(tmp_path: Path) -> None:
    """Refused grouped submissions admit nothing; the rollup never counts them."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    group_id = uuid4()
    multifile_id = uuid4()
    envelopes = [_valid_envelope() for _ in range(_BURST_SIZE)]

    statuses: list[int] = []
    refused_bodies: list[dict[str, Any]] = []
    for order, envelope in enumerate(envelopes):
        response = _post_grouped(client, envelope, group_id, multifile_id, order)
        statuses.append(response.status_code)
        if response.status_code != 202:
            refused_bodies.append(response.json())

    assert statuses == [202, 202, 503, 503], statuses

    # Refusals ride the canonical envelope and tell the producer when
    # to come back.
    for body in refused_bodies:
        assert body["error"]["code"] == "saturation_cap"
        assert body["error"]["instance_id"] == "primary"

    admitted_ids = [UUID(envelopes[i]["chain_id"]) for i in range(_FIXTURE_ROW_CAP)]
    refused_ids = [UUID(envelopes[i]["chain_id"]) for i in range(_FIXTURE_ROW_CAP, _BURST_SIZE)]

    # The store holds EXACTLY the admitted members under the group id,
    # with the supplied grouping persisted.
    members = await ctx.store.list_by_group_id(group_id)
    assert sorted(row.chain_id for row in members) == sorted(admitted_ids)
    for row in members:
        assert row.group_id == group_id
        assert row.multifile_id == multifile_id
        assert row.sent_at is None

    # Refused chain ids admitted NOTHING: no row in the store, 404 on
    # the admin detail surface.
    for refused in refused_ids:
        assert await ctx.store.get(refused) is None
        detail = client.get(f"/v1/admin/chains/{refused}")
        assert detail.status_code == 404
        assert detail.json()["error"]["code"] == "not_found"

    # The rollup counts only admitted members.
    rollup = client.get(f"/v1/admin/groups/{group_id}")
    assert rollup.status_code == 200
    rollup_body = rollup.json()
    assert rollup_body["total"] == _FIXTURE_ROW_CAP
    assert rollup_body["all_finished"] is False

    # Gate accounting matches the admitted set exactly.
    assert ctx.saturation.in_flight == _FIXTURE_ROW_CAP
    assert ctx.saturation.in_flight_bytes == sum(row.body_size_bytes for row in members)


@pytest.mark.asyncio
async def test_cap_raise_mid_burst_admits_refused_resubmission(tmp_path: Path) -> None:
    """update_caps opens admission; a refused chain_id resubmits cleanly."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    group_id = uuid4()
    multifile_id = uuid4()
    envelopes = [_valid_envelope() for _ in range(_BURST_SIZE)]
    for order, envelope in enumerate(envelopes):
        _post_grouped(client, envelope, group_id, multifile_id, order)
    refused_envelope = envelopes[_BURST_SIZE - 1]
    refused_id = UUID(refused_envelope["chain_id"])
    assert await ctx.store.get(refused_id) is None

    # The hot-reload push: raise the row cap on the LIVE gate.
    await ctx.saturation.update_caps(_wide_caps(_RAISED_ROW_CAP))

    # The refused submission retries with the SAME chain_id and group:
    # a clean fresh admission (the refusal left no idempotency ghost,
    # no chain_id_in_use).
    retry = _post_grouped(client, refused_envelope, group_id, multifile_id, _BURST_SIZE - 1)
    assert retry.status_code == 202, retry.text

    members = await ctx.store.list_by_group_id(group_id)
    assert len(members) == _FIXTURE_ROW_CAP + 1
    fresh = await ctx.store.get(refused_id)
    assert fresh is not None
    assert fresh.group_id == group_id
    assert fresh.send_order == _BURST_SIZE - 1
    assert ctx.saturation.in_flight == _FIXTURE_ROW_CAP + 1

    rollup = client.get(f"/v1/admin/groups/{group_id}")
    assert rollup.json()["total"] == _FIXTURE_ROW_CAP + 1


@pytest.mark.asyncio
async def test_update_caps_mid_storm_keeps_accounting_exact() -> None:
    """Cap flips racing an admit storm never corrupt the counters."""
    gate = SaturationGate(
        max_in_flight=_STORM_ROW_CAP,
        max_in_flight_bytes=_WIDE_BYTES_CAP,
        max_disk_bytes=_WIDE_BYTES_CAP,
    )
    granted_count = 0

    async def _admit_one() -> None:
        nonlocal granted_count
        result = await gate.admit(_STORM_BODY_BYTES)
        # No lock needed: the check + increment form one synchronous
        # section of a single event-loop task (no await between them),
        # so concurrent _admit_one tasks cannot interleave here.
        if isinstance(result, AdmissionGranted):
            granted_count += 1

    async def _flip_caps() -> None:
        await gate.update_caps(_wide_caps(_STORM_RAISED_CAP))
        # Cooperative yield (0 s) so admits interleave between the two
        # flips; not a wait.
        await asyncio.sleep(0)
        await gate.update_caps(_wide_caps(_STORM_LOWERED_CAP))

    await asyncio.gather(*[_admit_one() for _ in range(_STORM_ADMITS)], _flip_caps())

    # Exact symmetry: counters equal grants (every grant is one row and
    # one byte; refusals account nothing).
    assert gate.in_flight == granted_count
    assert gate.in_flight_bytes == granted_count * _STORM_BODY_BYTES

    # Drain: release every grant; the gate returns to zero and admits
    # afresh under the final (lowered) cap.
    for _ in range(granted_count):
        await gate.release(_STORM_BODY_BYTES)
    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0
    fresh = await gate.admit(_STORM_BODY_BYTES)
    assert isinstance(fresh, AdmissionGranted)
    await gate.release(_STORM_BODY_BYTES)
