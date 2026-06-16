"""Header matrix tests for the cycle-7 grouping/ordering ingress reads.

Plan section 3, tasks 2.2 + 2.3 acceptance: for each of the three new
``POST /v1/send`` request headers (``X-Phantom-Group-Id``,
``X-Phantom-Multifile-Id``, ``X-Phantom-Order``) the matrix covers
present-valid, absent, and malformed, asserting the stored row values
or the 400 ``header_invalid`` ErrorEnvelope. The headerless leg also
proves the phase-1 transitional hardcodes are gone: ``multifile_id`` is
NULL (standalone) instead of the old ``chain_id`` copy.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from .test_send_route import _build_app, _valid_envelope


def _post(client: TestClient, body: dict, headers: dict[str, str]) -> object:
    """POST the envelope with X-Phantom-Uid plus the given extra headers."""
    return client.post("/v1/send", json=body, headers={"X-Phantom-Uid": "user-1", **headers})


@pytest.mark.asyncio
async def test_all_headers_absent_stores_defaults(tmp_path: Path) -> None:
    """No grouping headers: group_id=chain_id, multifile_id NULL, send_order 0.

    This is the deleted-hardcodes proof (task 2.3): phase 1 stored
    multifile_id=chain_id transitionally; the final semantics are NULL.
    """
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {})
    assert response.status_code == 202
    # The 202 always carries the X-Phantom-Group-Id echo (task 2.4):
    # without a supplied group it equals chain_id.
    assert response.headers["X-Phantom-Group-Id"] == body["chain_id"]
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.group_id == UUID(body["chain_id"])
    assert row.multifile_id is None
    assert row.send_order == 0


@pytest.mark.asyncio
async def test_group_id_present_valid(tmp_path: Path) -> None:
    """A valid X-Phantom-Group-Id is stored verbatim; other defaults hold."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    group_id = uuid4()
    response = _post(client, body, {"X-Phantom-Group-Id": str(group_id)})
    assert response.status_code == 202
    # The echo carries the SUPPLIED group on the 202 (task 2.4).
    assert response.headers["X-Phantom-Group-Id"] == str(group_id)
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.group_id == group_id
    assert row.multifile_id is None
    assert row.send_order == 0


@pytest.mark.asyncio
async def test_idempotency_replay_echoes_original_group(tmp_path: Path) -> None:
    """The 200 replay response carries the ORIGINAL row's group echo."""
    app, _ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    group_id = uuid4()
    first = _post(
        client,
        body,
        {"X-Phantom-Group-Id": str(group_id), "X-Phantom-Idempotency-Key": "k-echo"},
    )
    assert first.status_code == 202

    body_two = _valid_envelope()
    replay = _post(client, body_two, {"X-Phantom-Idempotency-Key": "k-echo"})
    assert replay.status_code == 200
    assert replay.headers["X-Phantom-Group-Id"] == str(group_id)


@pytest.mark.asyncio
async def test_group_id_malformed_is_400(tmp_path: Path) -> None:
    """A malformed X-Phantom-Group-Id is a 400 header_invalid; no row lands."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Group-Id": "not-a-uuid"})
    assert response.status_code == 400
    parsed = response.json()
    assert parsed["error"]["code"] == "header_invalid"
    assert parsed["error"]["details"]["header"] == "X-Phantom-Group-Id"
    assert parsed["error"]["details"]["value"] == "not-a-uuid"
    assert await ctx.store.get(UUID(body["chain_id"])) is None


@pytest.mark.asyncio
async def test_group_id_empty_string_is_400(tmp_path: Path) -> None:
    """An empty header value counts as present-but-malformed (400)."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Group-Id": ""})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "header_invalid"
    assert await ctx.store.get(UUID(body["chain_id"])) is None


@pytest.mark.asyncio
async def test_multifile_id_present_valid(tmp_path: Path) -> None:
    """A valid X-Phantom-Multifile-Id is stored; group_id stays chain_id."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    multifile_id = uuid4()
    response = _post(client, body, {"X-Phantom-Multifile-Id": str(multifile_id)})
    assert response.status_code == 202
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.multifile_id == multifile_id
    assert row.group_id == UUID(body["chain_id"])
    assert row.send_order == 0


@pytest.mark.asyncio
async def test_multifile_id_malformed_is_400(tmp_path: Path) -> None:
    """A malformed X-Phantom-Multifile-Id is a 400 header_invalid."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Multifile-Id": "12345"})
    assert response.status_code == 400
    parsed = response.json()
    assert parsed["error"]["code"] == "header_invalid"
    assert parsed["error"]["details"]["header"] == "X-Phantom-Multifile-Id"
    assert await ctx.store.get(UUID(body["chain_id"])) is None


@pytest.mark.asyncio
async def test_order_present_valid(tmp_path: Path) -> None:
    """A valid X-Phantom-Order is stored as send_order."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Order": "3"})
    assert response.status_code == 202
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.send_order == 3


@pytest.mark.asyncio
async def test_order_zero_valid(tmp_path: Path) -> None:
    """X-Phantom-Order accepts 0 (the boundary of the int >= 0 contract)."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Order": "0"})
    assert response.status_code == 202
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.send_order == 0


@pytest.mark.asyncio
async def test_order_non_integer_is_400(tmp_path: Path) -> None:
    """A non-integer X-Phantom-Order is a 400 header_invalid."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Order": "first"})
    assert response.status_code == 400
    parsed = response.json()
    assert parsed["error"]["code"] == "header_invalid"
    assert parsed["error"]["details"]["header"] == "X-Phantom-Order"
    assert await ctx.store.get(UUID(body["chain_id"])) is None


@pytest.mark.asyncio
async def test_order_negative_is_400(tmp_path: Path) -> None:
    """A negative X-Phantom-Order violates the int >= 0 contract (400)."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Order": "-1"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "header_invalid"
    assert await ctx.store.get(UUID(body["chain_id"])) is None


@pytest.mark.asyncio
async def test_all_three_headers_stored_together(tmp_path: Path) -> None:
    """A multi-file member submission stores all three values."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    group_id = uuid4()
    multifile_id = uuid4()
    response = _post(
        client,
        body,
        {
            "X-Phantom-Group-Id": str(group_id),
            "X-Phantom-Multifile-Id": str(multifile_id),
            "X-Phantom-Order": "2",
        },
    )
    assert response.status_code == 202
    row = await ctx.store.get(UUID(body["chain_id"]))
    assert row is not None
    assert row.group_id == group_id
    assert row.multifile_id == multifile_id
    assert row.send_order == 2


@pytest.mark.asyncio
async def test_malformed_header_does_not_leak_saturation_slot(tmp_path: Path) -> None:
    """The 400 rejection happens BEFORE admission; the gate stays untouched."""
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    response = _post(client, body, {"X-Phantom-Group-Id": "broken"})
    assert response.status_code == 400
    assert ctx.saturation.in_flight == 0
    assert ctx.saturation.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_idempotency_replay_ignores_conflicting_grouping_headers(
    tmp_path: Path,
) -> None:
    """Round 1 adversary (suite hardening): first-write-wins for grouping.

    The seed attack: replay the same idempotency key with the same body
    and destination but DIFFERENT ``X-Phantom-Group-Id`` and
    ``X-Phantom-Multifile-Id`` headers. The grouping/ordering headers are
    not part of the idempotency identity (only the body, finding G-1, and
    the destination, finding R3-3, are), so the second submission is a
    genuine replay: it returns the ORIGINAL row with status 200 and never
    mutates the stored grouping. Pinning this proves an idempotency replay
    cannot retroactively move a row into a different query group out from
    under a concurrent rollup poll: the first submission's group wins, the
    echo is honest, and the stored row is byte-identical on the grouping
    axis.
    """
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    original_group = uuid4()
    original_multifile = uuid4()
    first_body = _valid_envelope()
    first = _post(
        client,
        first_body,
        {
            "X-Phantom-Idempotency-Key": "k-grouping-conflict",
            "X-Phantom-Group-Id": str(original_group),
            "X-Phantom-Multifile-Id": str(original_multifile),
            "X-Phantom-Order": "1",
        },
    )
    assert first.status_code == 202
    assert first.headers["X-Phantom-Group-Id"] == str(original_group)
    original_chain_id = UUID(first_body["chain_id"])

    # A DIFFERENT chain_id, same destination + body, same key, but a
    # conflicting group / multifile / order. This is a genuine replay.
    replay_group = uuid4()
    replay_multifile = uuid4()
    replay = _post(
        client,
        _valid_envelope(),
        {
            "X-Phantom-Idempotency-Key": "k-grouping-conflict",
            "X-Phantom-Group-Id": str(replay_group),
            "X-Phantom-Multifile-Id": str(replay_multifile),
            "X-Phantom-Order": "9",
        },
    )
    assert replay.status_code == 200
    # The echo carries the ORIGINAL group, not the replay's group.
    assert replay.headers["X-Phantom-Group-Id"] == str(original_group)

    # The stored row's grouping axis is untouched by the replay attempt.
    row = await ctx.store.get(original_chain_id)
    assert row is not None
    assert row.group_id == original_group
    assert row.multifile_id == original_multifile
    assert row.send_order == 1
    # The replay's chain_id never became a row (idempotency dedup held).
    assert await ctx.store.list_all_chain_ids() == [original_chain_id]


@pytest.mark.asyncio
async def test_group_id_whitespace_padded_is_400(tmp_path: Path) -> None:
    """Round 1 adversary (suite hardening): padded UUID is malformed.

    ``X-Phantom-Group-Id`` is load-bearing identity (it keys the rollup),
    so its parse must be strict. A UUID wrapped in surrounding whitespace
    is NOT canonicalized into a valid id; it is a 400 ``header_invalid``
    and admits nothing, exactly like the empty-string and non-UUID legs.
    This closes the whitespace corner of the malformed-400 contract for
    the identity headers (the existing legs cover ``""`` and ``not-a-uuid``
    but not a padded-but-otherwise-valid value).
    """
    app, ctx = await _build_app(tmp_path)
    client = TestClient(app)
    body = _valid_envelope()
    padded = f"  {uuid4()}  "
    response = _post(client, body, {"X-Phantom-Group-Id": padded})
    assert response.status_code == 400
    parsed = response.json()
    assert parsed["error"]["code"] == "header_invalid"
    assert parsed["error"]["details"]["header"] == "X-Phantom-Group-Id"
    assert await ctx.store.get(UUID(body["chain_id"])) is None
