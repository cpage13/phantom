"""Count-cap eviction self-heals the idempotency claim; the default cap is active.

Adversary round 1, M-3 (sharpens seed finding 10). Two real-deployment properties
the existing retention coverage does not assert end to end:

1. **The count-cap eviction does not strand a "ghost" idempotency claim.** When
   the reaper's ``max_rows`` backstop evicts a terminal row, that row's inbound
   idempotency claim must not silently dedupe a later resend that reuses the same
   ``X-Phantom-Idempotency-Key``. If it did, a long-running producer that churns
   terminal rows past the cap would, after an eviction, answer a genuine resend of
   the evicted chain with a SUCCESS-shaped 200 replay of a row that no longer
   exists - silently dropping the new upload. The defense is two-layer: the reaper
   trims the index (``cleanup_idempotency_index``) AND a new admission self-heals
   by orphan-deleting a claim whose ``chain_id`` is gone from ``uploads``
   (``insert_with_idempotency_claim``). This test proves the END-TO-END effect:
   pre-eviction a reused key replays (200); POST-eviction the same key is admitted
   anew (202), never a ghost replay.

2. **The new default cap (``retention.max_rows = 100_000``) is the ACTIVE enforced
   value through the loaded production ``Settings``** - not the historical
   unbounded ``-1``. A unit canary pins the field default; this guards the
   loaded-config reality in the e2e lane against an accidental flip slipping past.

Public e2e-light lane (plan § 5.0): generic single-step JSON envelopes over raw
HTTP, no ``PHANTOM_ENABLED``.

Falsifier: drop the reaper's ``cleanup_idempotency_index`` AND the admission
orphan-delete -> the post-eviction resend replays the dead row (200) instead of
admitting (202) -> RED. Or flip the default cap back to ``-1`` -> the default-cap
assertion -> RED.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from phantom.config.settings import RetentionCfg

from tests.e2e.helpers.stack import E2EStack, boot_stack

pytestmark = pytest.mark.e2e

_DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
# A small body the emulator tolerates inline; identical bytes across resends so a
# reused key is a clean REPLAY (same body), never a body-conflict (422).
_SHARED_KEY: str = "evicted-chain-shared-idempotency-key-001"

_EVICTION_BUDGET_SECONDS: float = 20.0
_EVICTION_POLL_SECONDS: float = 0.3


def _single_step_json_envelope(
    *, emulator_url: str, chain_id: UUID, marker: str
) -> dict[str, object]:
    """Build a one-step JSON-body envelope as a raw dict (no SDK).

    Mirrors ``test_aggressor_no_key_dedup._single_step_json_envelope`` so the test
    controls the exact wire bytes and the ``X-Phantom-Idempotency-Key`` header.
    """
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": f"{emulator_url}/v2/files",
                "headers": {"Content-Type": "application/json"},
                "body": {
                    "kind": "json",
                    "value": {
                        "domain": "generic",
                        "laneBaseName": "history_parquet_data",
                        "fileName": f"cap-{marker}",
                        "metadata": {"keyValueStore": {"uploader_id": "12345"}},
                    },
                },
                "capture": [],
                "idempotency_header": None,
            }
        ],
        "default_target": None,
    }


async def _raw_send(
    client: httpx.AsyncClient,
    *,
    phantom_url: str,
    envelope: dict[str, object],
    bearer: str,
    idempotency_key: str,
) -> httpx.Response:
    """POST a JSON envelope to ``/v1/send`` with a verbatim idempotency header."""
    headers = {
        "Content-Type": "application/json",
        "X-Phantom-Uid": _DEFAULT_SUB,
        "Authorization": f"Bearer {bearer}",
        "X-Phantom-Idempotency-Key": idempotency_key,
    }
    return await client.post(
        f"{phantom_url}/v1/send",
        content=json.dumps(envelope).encode("utf-8"),
        headers=headers,
    )


async def _await_chain_terminal(stack: E2EStack, chain_id: UUID) -> None:
    """Poll until ``chain_id`` reaches ``succeeded`` (so it is count-cap evictable)."""
    await stack.phantom_client.poll_until(
        chain_id,
        terminal_states=frozenset({"succeeded"}),
        deadline=datetime.now(UTC) + timedelta(seconds=_EVICTION_BUDGET_SECONDS),
    )


async def _await_chain_evicted(stack: E2EStack, chain_id: UUID) -> None:
    """Poll the admin list surface until ``chain_id`` is gone (count-cap evicted)."""
    deadline = time.monotonic() + _EVICTION_BUDGET_SECONDS
    present = True
    while time.monotonic() < deadline:
        rows, _ = await stack.phantom_client.list_uploads(limit=500)
        present = chain_id in {r.chain_id for r in rows}
        if not present:
            return
        await asyncio.sleep(_EVICTION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"chain {chain_id} was never evicted by the count cap within {_EVICTION_BUDGET_SECONDS}s"
    )


def _default_cap_is_active() -> None:
    """Assert the loaded ``RetentionCfg`` default cap is the new bounded value."""
    cap = RetentionCfg().max_rows
    assert cap == 100_000, (
        f"the default retention.max_rows backstop changed to {cap!r}; the documented "
        "default is 100_000 (row-bounded out of the box). A flip back to -1 (unbounded) "
        "is a deliberate decision and must update this guard + the operator playbook."
    )


def test_default_max_rows_is_the_active_bounded_value() -> None:
    """The new default cap (100_000) is the loaded production value, not -1.

    Falsifier: revert ``settings.py`` ``max_rows`` to ``-1`` -> RED. Guards the
    e2e lane against the default silently reverting to unbounded.
    """
    _default_cap_is_active()


async def test_count_cap_eviction_does_not_ghost_replay_an_evicted_chain(
    tmp_path: Path,
) -> None:
    """After the count-cap evicts a chain, a resend of its key admits anew (no ghost replay).

    Drives the full sequence over the real HTTP ingress: submit a chain with a
    known inbound idempotency key (claim written), confirm a same-key same-body
    resend REPLAYS (200) while the chain is live, then push the table over a low
    ``max_rows`` so the reaper evicts that chain, and finally resend the same key:
    it must be ADMITTED anew (202), never a 200 replay of the evicted row.
    """
    # A low cap so a handful of seeded terminal rows trips the backstop; a short
    # reaper interval so the eviction lands promptly.
    cap = 3
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retention": {"max_rows": cap, "reaper_interval_seconds": 1},
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # 1. Submit one chain with the shared inbound key; it succeeds and writes
        #    a real idempotency claim keyed by the inbound key. The envelope body
        #    is held IDENTICAL across all three submissions (only chain_id varies)
        #    so a reused key is a clean REPLAY (same body + same destination),
        #    never a body/destination conflict (422).
        victim_id = uuid4()
        shared_marker = "evicted-chain-body"
        envelope = _single_step_json_envelope(
            emulator_url=stack.emulator_url, chain_id=victim_id, marker=shared_marker
        )
        async with httpx.AsyncClient(timeout=30.0) as client:
            first = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=envelope,
                bearer=bearer,
                idempotency_key=_SHARED_KEY,
            )
            assert first.status_code == 202, (
                f"first submit should be admitted (202); got {first.status_code}: {first.text}"
            )

            # 2. While the chain is LIVE, a same-key same-body resend (new chain_id)
            #    REPLAYS: 200, not a second admission. This proves the claim exists.
            replay_envelope = _single_step_json_envelope(
                emulator_url=stack.emulator_url, chain_id=uuid4(), marker=shared_marker
            )
            replay = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=replay_envelope,
                bearer=bearer,
                idempotency_key=_SHARED_KEY,
            )
            assert replay.status_code == 200, (
                "while the chain is live, a same-key same-body resend must REPLAY (200); "
                f"got {replay.status_code}: {replay.text}"
            )

            # 3. Let the victim reach a TERMINAL state (only terminal rows are
            #    count-cap evictable), then push the table over the cap. Seed NEWER
            #    terminal rows directly so the victim is the OLDEST evictable row
            #    (oldest-DONE-first); the count cap then drops it.
            await _await_chain_terminal(stack, victim_id)
            instance = stack.get_instance("primary")
            store = instance.store
            base = datetime.now(UTC) + timedelta(seconds=10)  # newer than the victim
            for i in range(cap + 2):
                await store.insert(
                    _seed_terminal_row(uuid4(), updated_at=base + timedelta(seconds=i))
                )

            # 4. The reaper's count-cap backstop evicts the OLDEST terminal rows
            #    (the victim among them) until the table is at or below the cap.
            await _await_chain_evicted(stack, victim_id)

            # 5. THE CRUX: resend the SAME key (new chain_id, same body) AFTER the
            #    victim was evicted. The orphaned claim must NOT ghost-replay a dead
            #    row: the submission is ADMITTED anew (202).
            post_evict_id = uuid4()
            post_evict_envelope = _single_step_json_envelope(
                emulator_url=stack.emulator_url, chain_id=post_evict_id, marker=shared_marker
            )
            post_evict = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=post_evict_envelope,
                bearer=bearer,
                idempotency_key=_SHARED_KEY,
            )
            assert post_evict.status_code == 202, (
                "after the count cap evicted the original chain, a resend of its inbound "
                "idempotency key must be ADMITTED anew (202), not silently deduped as a "
                f"200 replay of the evicted row; got {post_evict.status_code}: {post_evict.text}"
            )
            assert UUID(post_evict.json()["chain_id"]) == post_evict_id, (
                "the post-eviction resend must admit the NEW chain, not echo the evicted one"
            )
    finally:
        await stack.tear_down()


def _seed_terminal_row(chain_id: UUID, *, updated_at: datetime) -> object:
    """Build a minimal terminal (``failed``) ``UploadRow`` with a controllable age.

    Mirrors the seed shape used by ``test_retention_boot_sweep_and_bound``; the
    ``updated_at`` drives the count cap's oldest-first eviction ordering.
    """
    from phantom.models.upload import UploadRow

    digest = "0" * 64
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "emulator",
            "state": "failed",
            "body_location": "ram",
            "received_at": updated_at,
            "updated_at": updated_at,
            "endpoint": "e",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": f"seed-{chain_id}",
            "chain_id_at_ingress": str(chain_id),
            "capture_reexecution_active": False,
            "body_hashes": {"body": {"body_hash": digest, "storage_hash": digest}},
            "body_size_bytes": 8,
        },
    )
