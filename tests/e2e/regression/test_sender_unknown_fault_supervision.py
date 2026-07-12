"""Production-process sender unknown-fault supervision and recovery E2Es.

These scenarios replace the old generic TaskGroup demonstration with the real
CLI, composition-root lifespan, Sender, SQLite store, recovery sweep, SDK, and
upstream emulator. A test-only alternative child launcher injects a one-shot
unknown fault at an exact pre-claim or post-claim boundary. Bounded loopback
IPC provides reached/release/released ordering, so no timing sleep decides
where the fault lands and peer loss cannot hang the lane.
"""

from __future__ import annotations

import asyncio
import hashlib
import signal
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from phantom.models.upload import BodyLocation, UploadState
from phantom_client import PhantomClient
from phantom_client.models.admin import ChainAdminDetail
from phantom_emulator.state import BodyPutEvent, MetadataCreateEvent, UpstreamEvent

from tests.e2e._harness.sender_fault_ipc import (
    FAULT_MESSAGE_PREFIX,
    FAULT_PHASE_ENV,
    IPC_ENDPOINT_ENV,
    SenderFaultIpcServer,
    SenderFaultPhase,
)
from tests.e2e._harness.subprocess_harness import (
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    db_path_for,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_FAULT_EXIT_TIMEOUT_SECONDS = 30.0
_DELIVERY_TIMEOUT_SECONDS = 30.0
_DELIVERY_POLL_SECONDS = 0.1
_BODY = b"sender-fault-e2e-body-sentinel"
_BODY_HASH = hashlib.sha256(_BODY).hexdigest()


@dataclass(frozen=True)
class StoppedRow:
    """Durable row fields that discriminate the two sender fault points."""

    chain_id: UUID
    state: UploadState
    body_location: BodyLocation
    attempts: int
    received_at: datetime
    updated_at: datetime
    current_step_index: int
    last_error: str | None


def _child_argv(config_path: Path) -> tuple[str, ...]:
    """Build argv for the test-only launcher around the production CLI."""
    return (
        sys.executable,
        "-m",
        "tests.e2e._harness.sender_fault_launcher",
        "-c",
        str(config_path),
    )


def _read_stopped_row(data_dir: Path, chain_id: UUID) -> StoppedRow:
    """Read one upload through a SQLite ``mode=ro`` connection."""
    database = db_path_for(data_dir)
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        row = connection.execute(
            """
            SELECT chain_id, state, body_location, attempts, received_at,
                   updated_at, current_step_index, last_error
              FROM uploads
             WHERE chain_id = ?
            """,
            (str(chain_id),),
        ).fetchone()
    if row is None:
        raise AssertionError(f"durable row {chain_id} was not present in stopped database")
    return StoppedRow(
        chain_id=UUID(str(row[0])),
        state=cast(UploadState, str(row[1])),
        body_location=cast(BodyLocation, str(row[2])),
        attempts=int(row[3]),
        received_at=datetime.fromisoformat(str(row[4])),
        updated_at=datetime.fromisoformat(str(row[5])),
        current_step_index=int(row[6]),
        last_error=None if row[7] is None else str(row[7]),
    )


async def _await_succeeded(client: PhantomClient, chain_id: UUID) -> ChainAdminDetail:
    """Poll the public SDK until ``chain_id`` reaches terminal success."""
    deadline = asyncio.get_running_loop().time() + _DELIVERY_TIMEOUT_SECONDS
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = await client.get_upload(chain_id)
        if last.state == "succeeded":
            return last
        await asyncio.sleep(_DELIVERY_POLL_SECONDS)  # pre-commit-allow: bounded status poll
    raise AssertionError(
        f"chain {chain_id} did not recover to succeeded; "
        f"last state={getattr(last, 'state', None)!r}"
    )


async def _live_in_flight(phantom_url: str) -> int:
    """Return the production saturation gate's live in-flight row count."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(f"{phantom_url}/v1/admin/status")
        response.raise_for_status()
    payload = response.json()
    return sum(int(instance["in_flight"]) for instance in payload["instances"])


async def _await_in_flight(phantom_url: str, expected: int) -> None:
    """Poll the live saturation gate until it reaches ``expected``."""
    deadline = asyncio.get_running_loop().time() + _DELIVERY_TIMEOUT_SECONDS
    observed = -1
    while asyncio.get_running_loop().time() < deadline:
        observed = await _live_in_flight(phantom_url)
        if observed == expected:
            return
        await asyncio.sleep(_DELIVERY_POLL_SECONDS)  # pre-commit-allow: bounded status poll
    raise AssertionError(f"live in_flight stayed at {observed}; expected {expected}")


def _chain_events(emulator: EmulatorHandle, chain_id: UUID) -> list[UpstreamEvent]:
    """Return append-only upstream events correlated to ``chain_id``."""
    return [event for event in emulator.upstream_events() if event.chain_id == chain_id]


def _assert_exact_two_step_delivery(emulator: EmulatorHandle, chain_id: UUID) -> None:
    """Require exactly one metadata create followed by exactly one accepted PUT."""
    events = _chain_events(emulator, chain_id)
    assert len(events) == 2, f"expected create+PUT exactly once, got {events!r}"
    create, put = events
    assert isinstance(create, MetadataCreateEvent)
    assert isinstance(put, BodyPutEvent)
    assert create.occurred_at <= put.occurred_at
    assert create.idempotency_key is not None
    assert put.idempotency_key == create.idempotency_key
    assert put.file_id == create.file_id
    assert put.upload_token == create.upload_token
    assert put.upload_url == create.upload_url
    assert put.body_hash == _BODY_HASH
    assert put.body_size == len(_BODY)


def _assert_no_sentinels(log_text: str, *, bearer: str) -> None:
    """Require a child log to exclude the scenario's bearer and body markers."""
    assert bearer not in log_text
    assert _BODY.decode() not in log_text


async def _assert_health_lost(url: str) -> None:
    """Prove the listener disappeared with the supervised child process."""
    async with httpx.AsyncClient(timeout=2.0) as client:
        with pytest.raises(httpx.TransportError):
            await client.get(f"{url}/v1/healthz")


async def _run_fault_scenario(tmp_path: Path, *, phase: SenderFaultPhase) -> None:
    """Drive one exact sender fault through stopped-state and clean recovery."""
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    ipc = await SenderFaultIpcServer.start()
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides={
            "storage": {"body_store": {"mode": "all_disk"}},
            "retry": {"worker_count": 1, "poll_interval_ms": 20},
        },
    )
    faulted = PhantomSubprocess.make(
        config_path,
        port,
        argv=_child_argv(config_path),
        env_overrides={
            FAULT_PHASE_ENV: phase.value,
            IPC_ENDPOINT_ENV: ipc.endpoint,
        },
    )
    recovered: PhantomSubprocess | None = None
    bearer = fake_security_token(emulator)
    chain_id = uuid4()

    try:
        await faulted.start()
        if phase is SenderFaultPhase.PRE_CLAIM:
            reached = await ipc.wait_reached()
            assert reached.phase is SenderFaultPhase.PRE_CLAIM
            assert reached.chain_id is None
            async with PhantomClient(faulted.url) as client:
                await submit_one(
                    client,
                    emulator_url=emulator.url,
                    bearer=bearer,
                    body=_BODY,
                    chain_id=chain_id,
                    file_prefix="sender-preclaim",
                )
            reached_row = await asyncio.to_thread(_read_stopped_row, data_dir, chain_id)
            assert reached_row.state == "queued"
            assert reached_row.updated_at == reached_row.received_at
        else:
            async with PhantomClient(faulted.url) as client:
                await submit_one(
                    client,
                    emulator_url=emulator.url,
                    bearer=bearer,
                    body=_BODY,
                    chain_id=chain_id,
                    file_prefix="sender-postclaim",
                )
            reached = await ipc.wait_reached()
            assert reached.phase is SenderFaultPhase.POST_CLAIM
            assert reached.chain_id == chain_id
            reached_row = await asyncio.to_thread(_read_stopped_row, data_dir, chain_id)
            assert reached_row.state == "attempting"
            assert reached_row.updated_at > reached_row.received_at

        assert reached_row.body_location == "file"
        assert reached_row.attempts == 0
        assert reached_row.current_step_index == 0
        assert reached_row.last_error is None
        assert _chain_events(emulator, chain_id) == []

        await ipc.release()
        returncode = await faulted.wait_for_expected_exit(
            timeout_seconds=_FAULT_EXIT_TIMEOUT_SECONDS
        )
        assert returncode == -signal.SIGTERM
        await _assert_health_lost(faulted.url)

        stopped_row = await asyncio.to_thread(_read_stopped_row, data_dir, chain_id)
        assert stopped_row == reached_row

        fault_log = faulted.read_full_log()
        marker = f"{FAULT_MESSAGE_PREFIX}:{phase.value}"
        assert fault_log.count(marker) == 1, (
            f"expected exactly one injected-fault record for {phase.value}; log:\n{fault_log}"
        )
        assert "supervised worker failed; stopping Phantom process" in fault_log
        _assert_no_sentinels(fault_log, bearer=bearer)

        emulator.pause()
        recovered = PhantomSubprocess.make(config_path, port)
        await recovered.start()
        await _await_in_flight(recovered.url, 1)
        assert _chain_events(emulator, chain_id) == []
        emulator.resume()
        async with PhantomClient(recovered.url) as client:
            detail = await _await_succeeded(client, chain_id)
        assert detail.state == "succeeded"
        assert detail.attempts == 1
        _assert_exact_two_step_delivery(emulator, chain_id)
        await _await_in_flight(recovered.url, 0)
        _assert_no_sentinels(recovered.read_full_log(), bearer=bearer)
    finally:
        faulted.terminate()
        if recovered is not None:
            recovered.terminate()
        await ipc.close()
        await emulator.stop()


async def test_unknown_pre_claim_fault_crashes_and_clean_restart_delivers_once(
    tmp_path: Path,
) -> None:
    """Before claim: process exits, row stays queued, restart delivers once."""
    await _run_fault_scenario(tmp_path, phase=SenderFaultPhase.PRE_CLAIM)


async def test_unknown_post_claim_fault_crashes_and_recovery_delivers_once(
    tmp_path: Path,
) -> None:
    """After claim: process exits with attempting row; recovery delivers once."""
    await _run_fault_scenario(tmp_path, phase=SenderFaultPhase.POST_CLAIM)


async def test_alternative_launcher_without_fault_delivers_and_exits_cleanly(
    tmp_path: Path,
) -> None:
    """Negative control: the launcher alone neither crashes nor duplicates."""
    emulator = await boot_emulator()
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=tmp_path / "data",
        bind_port=port,
        config_overrides={
            "storage": {"body_store": {"mode": "all_disk"}},
            "retry": {"worker_count": 1, "poll_interval_ms": 20},
        },
    )
    child = PhantomSubprocess.make(
        config_path,
        port,
        argv=_child_argv(config_path),
        env_overrides={FAULT_PHASE_ENV: SenderFaultPhase.CONTROL.value},
    )
    chain_id = uuid4()
    bearer = fake_security_token(emulator)
    try:
        await child.start()
        async with PhantomClient(child.url) as client:
            await submit_one(
                client,
                emulator_url=emulator.url,
                bearer=bearer,
                body=_BODY,
                chain_id=chain_id,
                file_prefix="sender-control",
            )
            detail = await _await_succeeded(client, chain_id)
        assert detail.state == "succeeded"
        _assert_exact_two_step_delivery(emulator, chain_id)
        await _await_in_flight(child.url, 0)
        child.terminate()
        control_log = child.read_full_log()
        assert child.returncode == -signal.SIGTERM
        assert "Application shutdown complete." in control_log
        assert "supervised worker failed; stopping Phantom process" not in control_log
        assert FAULT_MESSAGE_PREFIX not in control_log
        _assert_no_sentinels(control_log, bearer=bearer)
    finally:
        child.terminate()
        await emulator.stop()
