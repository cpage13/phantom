"""E2E — Concurrency invariants.

The transparent-proxy invariant must hold under simultaneous load.
This suite drives multi-submit bursts at the Phantom ingress and
asserts every body delivered upstream is hash-identical to the one
the agent submitted — even when the saturation gate refuses some,
when encoding is in flight on a separate large body, when the worker
pool has multiple senders, and when the memory→disk persist transition
fires under burst.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import ChainResponse, PhantomClient, PhantomUnavailableError

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import (
    assert_chain_reaches_state,
)
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

# Default sub claim used by the suite's fake security token.
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Small per-upload body size for burst tests — 10 KiB keeps the
# saturation-bytes cap from dominating row-count behavior.
SMALL_BODY_BYTES: int = 10 * 1024

# 50 MiB stresses the encode path. Random bytes so codecs can't
# dedup-cheat.
LARGE_BODY_BYTES: int = 50 * 1024 * 1024

# Burst sizes per test. Each test pins its own count so the saturation
# math is auditable from the test source.
BURST_50_SMALL: int = 50
BURST_100_TIGHT: int = 100
BURST_20_WORKERS: int = 20


pytestmark = pytest.mark.e2e


def _sha256(data: bytes) -> str:
    """Return the SHA-256 hex of ``data``."""
    return hashlib.sha256(data).hexdigest()


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID | None = None,
) -> tuple[UUID, ChainResponse]:
    """Submit one upload-shaped chain and return ``(chain_id, response)``."""
    chain_id = chain_id or uuid4()
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    response = await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id, response


async def test_burst_50_concurrent_small_bodies(tmp_path: Path) -> None:
    """50 concurrent 10 KiB submits — all 202; every emulator hash matches."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "saturation": {"max_in_flight": 100, "max_in_flight_bytes": 64 * 1024 * 1024},
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        bodies: list[bytes] = [secrets.token_bytes(SMALL_BODY_BYTES) for _ in range(BURST_50_SMALL)]
        chain_ids: list[UUID] = [uuid4() for _ in bodies]

        async def _one(idx: int) -> None:
            await _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=bodies[idx],
                chain_id=chain_ids[idx],
            )

        await asyncio.gather(*(_one(i) for i in range(BURST_50_SMALL)))

        # Wait for every chain to reach succeeded.
        await asyncio.gather(
            *(
                assert_chain_reaches_state(
                    stack.phantom_client,
                    cid,
                    state="succeeded",
                    timeout_seconds=60.0,
                )
                for cid in chain_ids
            )
        )

        # Hash check on the emulator's received log. Build a map of
        # phantom_local_uuid → body_hash and verify every chain matches.
        received_by_uuid: dict[str, str] = {
            entry.metadata_kvs.get("phantom_local_uuid", ""): entry.body_hash
            for entry in stack.emulator.received()
        }
        for cid, body in zip(chain_ids, bodies, strict=True):
            expected = _sha256(body)
            actual = received_by_uuid.get(str(cid))
            assert actual == expected, (
                f"chain_id={cid}: emulator body_hash={actual!r} != agent hash={expected!r}"
            )
    finally:
        await stack.tear_down()


async def test_burst_during_encode_activity(tmp_path: Path) -> None:
    """Mid-encode of a 50 MiB body, fire 10 small submits; no corruption."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "saturation": {
                "max_in_flight": 50,
                "max_in_flight_bytes": 256 * 1024 * 1024,
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        large_body = secrets.token_bytes(LARGE_BODY_BYTES)
        small_bodies = [secrets.token_bytes(SMALL_BODY_BYTES) for _ in range(10)]
        large_chain_id = uuid4()
        small_chain_ids = [uuid4() for _ in small_bodies]

        # Kick off the large encode first.
        large_task = asyncio.create_task(
            _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=large_body,
                chain_id=large_chain_id,
            )
        )
        # Then fire 10 small submits in parallel; they should be
        # admitted in parallel with the large encode.
        small_tasks = [
            asyncio.create_task(
                _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=small_bodies[i],
                    chain_id=small_chain_ids[i],
                )
            )
            for i in range(10)
        ]
        await asyncio.gather(large_task, *small_tasks)

        # Every chain must succeed.
        await asyncio.gather(
            assert_chain_reaches_state(
                stack.phantom_client,
                large_chain_id,
                state="succeeded",
                timeout_seconds=180.0,
            ),
            *(
                assert_chain_reaches_state(
                    stack.phantom_client,
                    cid,
                    state="succeeded",
                    timeout_seconds=60.0,
                )
                for cid in small_chain_ids
            ),
        )

        received_by_uuid: dict[str, str] = {
            entry.metadata_kvs.get("phantom_local_uuid", ""): entry.body_hash
            for entry in stack.emulator.received()
        }
        # Large body matches.
        assert received_by_uuid.get(str(large_chain_id)) == _sha256(large_body), (
            f"large body hash mismatch for chain_id={large_chain_id}"
        )
        # Every small body matches.
        for cid, body in zip(small_chain_ids, small_bodies, strict=True):
            expected = _sha256(body)
            actual = received_by_uuid.get(str(cid))
            assert actual == expected, (
                f"small chain_id={cid}: emulator body_hash={actual!r} != agent hash={expected!r}"
            )
    finally:
        await stack.tear_down()


async def test_saturation_refusal_under_burst(tmp_path: Path) -> None:
    """Tight max_in_flight + 100 burst → some 503 with saturation_cap + Retry-After.

    Throttles the PUT step heavily so admitted rows occupy slots
    long enough that subsequent submits trip the gate. We assert at
    least one 503 with the canonical ``saturation_cap`` code AND a
    ``Retry-After`` header on that 503.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"saturation": {"max_in_flight": 5}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # Pause the emulator's PUT step long enough that the first
        # 5 admitted rows occupy their slot through the next bursts.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=4_000,
            ),
        )

        bearer = stack.fake_security_token()
        bodies = [secrets.token_bytes(SMALL_BODY_BYTES) for _ in range(BURST_100_TIGHT)]
        chain_ids = [uuid4() for _ in bodies]

        async def _one(idx: int) -> str | None:
            try:
                await _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=bodies[idx],
                    chain_id=chain_ids[idx],
                )
            except PhantomUnavailableError as exc:
                assert exc.error_code == "saturation_cap", (
                    f"expected saturation_cap, got {exc.error_code!r}"
                )
                assert exc.status_code == 503
                retry_after = exc.response_headers.get("retry-after") or exc.response_headers.get(
                    "Retry-After"
                )
                assert retry_after is not None, (
                    f"503 saturation_cap response missing Retry-After header; "
                    f"headers={dict(exc.response_headers)!r}"
                )
                return "refused"
            return "admitted"

        results = await asyncio.gather(*(_one(i) for i in range(BURST_100_TIGHT)))
        refused = sum(1 for r in results if r == "refused")
        admitted = sum(1 for r in results if r == "admitted")
        assert refused > 0, f"expected at least some 503s under tight cap; got admitted={admitted}"
        assert admitted > 0, f"expected at least some admissions; got refused={refused}"
    finally:
        await stack.tear_down()


async def test_mid_encode_collision(tmp_path: Path) -> None:
    """Two large bodies near-simultaneously; both complete with correct hashes."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "saturation": {
                "max_in_flight": 10,
                "max_in_flight_bytes": 256 * 1024 * 1024,
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        body_a = secrets.token_bytes(LARGE_BODY_BYTES)
        body_b = secrets.token_bytes(LARGE_BODY_BYTES)
        chain_a = uuid4()
        chain_b = uuid4()

        await asyncio.gather(
            _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=body_a,
                chain_id=chain_a,
            ),
            _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=body_b,
                chain_id=chain_b,
            ),
        )

        await asyncio.gather(
            assert_chain_reaches_state(
                stack.phantom_client,
                chain_a,
                state="succeeded",
                timeout_seconds=180.0,
            ),
            assert_chain_reaches_state(
                stack.phantom_client,
                chain_b,
                state="succeeded",
                timeout_seconds=180.0,
            ),
        )

        received_by_uuid: dict[str, str] = {
            entry.metadata_kvs.get("phantom_local_uuid", ""): entry.body_hash
            for entry in stack.emulator.received()
        }
        assert received_by_uuid.get(str(chain_a)) == _sha256(body_a)
        assert received_by_uuid.get(str(chain_b)) == _sha256(body_b)
    finally:
        await stack.tear_down()


async def test_two_senders_one_upstream(tmp_path: Path) -> None:
    """worker_count=2 + 20 bodies → all delivered; emulator receives all 20."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 100,
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": [0, 1, 2, 5, 10],
                },
            },
            "saturation": {"max_in_flight": 50, "max_in_flight_bytes": 64 * 1024 * 1024},
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        bodies = [secrets.token_bytes(SMALL_BODY_BYTES) for _ in range(BURST_20_WORKERS)]
        chain_ids = [uuid4() for _ in bodies]

        await asyncio.gather(
            *(
                _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=bodies[i],
                    chain_id=chain_ids[i],
                )
                for i in range(BURST_20_WORKERS)
            )
        )

        await asyncio.gather(
            *(
                assert_chain_reaches_state(
                    stack.phantom_client,
                    cid,
                    state="succeeded",
                    timeout_seconds=60.0,
                )
                for cid in chain_ids
            )
        )

        received_uuids = {
            entry.metadata_kvs.get("phantom_local_uuid", "") for entry in stack.emulator.received()
        }
        for cid in chain_ids:
            assert str(cid) in received_uuids, f"emulator did not record upload for chain_id={cid}"
    finally:
        await stack.tear_down()


# test_concurrent_persist_now: DELETED in Phase 1 Slice 1.F.
# The test exercised the now-removed persist_trigger.after_attempts=0
# admission knob and the deleted persist_now function (replaced by
# PersistController per plan § 2.3.11). The replacement coverage is:
#   - Slice 1.C: tests/unit/test_persist_controller.py — controller
#     idempotency + commit-last-column ordering.
#   - Slice 1.F: tests/integration/test_per_mode_wiring.py — per-mode
#     happy paths including all_disk admission writing directly to
#     file with no PersistController involvement.
#   - Slice 1.F: tests/unit/test_sqlite_store.py — concurrent
#     idempotency-claim race + serialization invariant.
# See test_migration_ledger_05_25.md (Phase 1 Slice 1.F section) for
# the deletion-to-replacement traceability.
