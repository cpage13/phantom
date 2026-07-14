"""Production log no-leak guard (audit T11 / G12).

Runs the real ``python -m phantom`` child at ``observability.log_level:
DEBUG`` so the executor actually emits its capture record ("chain step
captured values") and passes capture extras through the redaction filters,
then proves the COMPLETE child log text never contains the bearer, the
sensitive captured upload URL, its upload token, or the body bytes.

Deliberately limited proof strength (the audit's decision gate): the
production formatter renders no capture extras, so this guard asserts only
zero sentinel occurrences in production text output. It does NOT assert
``<redacted>`` and does NOT claim the redactor mutation loop executed; that
mechanism remains unit-proven in the service package.
"""

from __future__ import annotations

import asyncio
import signal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.state import BodyPutEvent, MetadataCreateEvent

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_DELIVERY_TIMEOUT_SECONDS = 30.0
_DELIVERY_POLL_SECONDS = 0.1
_BODY = b"no-leak-guard-body-sentinel"
# The executor's gated DEBUG record (chain/executor.py); its presence proves
# the capture-extras path actually executed in this child.
_CAPTURE_DEBUG_MARKER = "chain step captured values"


async def _await_succeeded(client: PhantomClient, chain_id: UUID) -> None:
    """Poll the public SDK until ``chain_id`` reaches terminal success."""
    deadline = asyncio.get_running_loop().time() + _DELIVERY_TIMEOUT_SECONDS
    last_state: str | None = None
    while asyncio.get_running_loop().time() < deadline:
        detail = await client.get_upload(chain_id)
        last_state = detail.state
        if last_state == "succeeded":
            return
        await asyncio.sleep(_DELIVERY_POLL_SECONDS)  # pre-commit-allow: sleep; bounded status poll
    raise AssertionError(f"chain {chain_id} did not reach succeeded; last state={last_state!r}")


async def test_debug_production_log_contains_no_bearer_or_sensitive_capture(
    tmp_path: Path,
) -> None:
    """Complete DEBUG-level child log holds zero secret sentinels."""
    emulator = await boot_emulator()
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=tmp_path / "data",
        bind_port=port,
        config_overrides={
            "observability": {"log_level": "DEBUG"},
            "retry": {"worker_count": 1, "poll_interval_ms": 20},
        },
    )
    child = PhantomSubprocess.make(config_path, port)
    bearer = fake_security_token(emulator)
    chain_id = uuid4()
    try:
        await child.start()
        async with PhantomClient(child.url) as client:
            await submit_one(
                client,
                emulator_url=emulator.url,
                bearer=bearer,
                body=_BODY,
                chain_id=chain_id,
                file_prefix="no-leak",
            )
            await _await_succeeded(client, chain_id)

        # Independent upstream observables: exactly one create and one
        # accepted PUT for this chain. The sensitive capture values (the
        # presigned URL and its token) come from the emulator oracle, never
        # from the log under test.
        events = [e for e in emulator.upstream_events() if e.chain_id == chain_id]
        assert len(events) == 2, f"expected create+PUT exactly once, got {len(events)} events"
        create, put = events
        assert isinstance(create, MetadataCreateEvent)
        assert isinstance(put, BodyPutEvent)
        sensitive_url = create.upload_url
        sensitive_token = create.upload_token

        # Clean SIGTERM drain so every handler flushes before the scan.
        child.terminate()
        assert child.returncode == -signal.SIGTERM
        log_text = child.read_full_log()
        assert "Application shutdown complete." in log_text

        # Discriminating preconditions: the child really ran at DEBUG and
        # the executor's capture record was emitted on this exact chain.
        assert " DEBUG " in log_text, "child did not run at DEBUG level"
        assert _CAPTURE_DEBUG_MARKER in log_text, "executor capture record was never emitted"

        # The guard. Assertion messages are value-suppressed by design.
        assert bearer not in log_text, "bearer sentinel leaked into child log (value suppressed)"
        assert sensitive_url not in log_text, (
            "sensitive captured upload URL leaked into child log (value suppressed)"
        )
        assert sensitive_token not in log_text, (
            "upload token leaked into child log (value suppressed)"
        )
        assert _BODY.decode() not in log_text, "body bytes leaked into child log (value suppressed)"
    finally:
        child.terminate()
        await emulator.stop()
