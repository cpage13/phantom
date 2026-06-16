"""The admin surface reports reality after each scenario, end to end (§ 5.D).

Plan § 5.2 Part 5.D, the admin-truth bundle. After a scenario drives the service into a
given state, the loopback admin surface (``/status``, ``/stats``, ``/quarantine``,
``/ready``, ``/health``) must report that reality - not a stale or default snapshot. This
module ties the admin surface to the states the other Part 5.* tests produce; the
quarantine-``reason`` truthfulness after corruption / a mode switch is already proven by
the § 5.A / § 5.C boot chaos tests (``test_startup_guards_e2e``,
``test_mode_switch_back_up_and_run``, ``test_chaos_unreadable_db_boot``), so it is asserted
here only in its NEGATIVE form (a clean deployment shows an empty inventory).

* :func:`test_admin_surface_reports_reality_for_stored_and_parked` - drive a chain to the
  now-populated ``stored`` terminal state (capture-TTL expiry + ``capture_reexecution:
  false``, the § 4D/E2E-15 recipe), then assert ``/stats`` surfaces ``by_state.stored`` and
  the § 3 ``parked_total``, ``/status`` surfaces ``ready`` / ``total_backlog`` /
  ``disk_usage_bytes``, and ``/ready`` / ``/health`` report a healthy, non-degraded service.

* :func:`test_admin_write_path_token_push_resumes_and_stats_track_it` - the admin WRITE path
  with truthful stats around it: a 401 parks a row (``auth.auth_expired_count == 1``,
  ``parked_total >= 1``), an admin ``PUT /v1/admin/tokens/{endpoint}/{uid}`` lands a fresh
  bearer, the row resumes to ``succeeded``, and the stats surface tracks the count back to 0.

* :func:`test_malformed_admin_requests_rejected_cleanly` - an unknown chain_id (404
  ``not_found``), an unknown ``?instance=`` (421 ``instance_unknown``), and a bogus restore
  ``backup_id`` (404) are each rejected with the typed SDK exception, and the admin surface
  stays responsive afterwards (health still ``ok``).

Public e2e-light lane (§ 5.0): generic ``submit`` shapes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomBadRequestError, PhantomNotFoundError
from phantom_client.models.chain import ChainCapture, ChainEnvelope
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = pytest.mark.e2e

_BODY: bytes = b"phantom-5D-admin-truth-body"
_TERMINAL_BUDGET_SECONDS: float = 20.0
_AUTH_EXPIRED_BUDGET_SECONDS: float = 8.0
_POST_PUSH_BUDGET_SECONDS: float = 20.0
_CAPTURE_TTL_SECONDS: int = 1


def _with_short_capture_ttl(envelope: ChainEnvelope, *, ttl_seconds: int) -> ChainEnvelope:
    """Return a copy of ``envelope`` with every capture's TTL clamped (E2E-15 recipe)."""
    new_steps = []
    for step in envelope.steps:
        new_captures: list[ChainCapture] = []
        for cap in step.capture:
            data: dict[str, Any] = cap.model_dump(by_alias=True)
            data["ttl_seconds"] = ttl_seconds
            new_captures.append(ChainCapture.model_validate(data))
        new_steps.append(step.model_copy(update={"capture": new_captures}))
    return envelope.model_copy(update={"steps": new_steps})


async def _submit(
    stack: E2EStack,
    *,
    chain_id: UUID,
    body: bytes,
    short_ttl: bool = False,
) -> None:
    """Submit one two-step chain (optionally with a short capture TTL)."""
    request = build_create_file_request(file_name=f"admin_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    if short_ttl:
        envelope = _with_short_capture_ttl(envelope, ttl_seconds=_CAPTURE_TTL_SECONDS)
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_FAKE_SUB,
        auth_token=f"Bearer {stack.fake_security_token()}",
    )


async def test_admin_surface_reports_reality_for_stored_and_parked(tmp_path: Path) -> None:
    """After a chain parks in ``stored``, /stats /status /ready /health all report reality.

    Falsifier: have ``/stats`` keep ``by_state.stored`` structurally zero (the pre-§3 bug)
    or omit ``parked_total`` -> the stored/parked assertions fail -> RED.
    """
    # capture_reexecution: false (ADR-011 default) so the TTL-expired chain stores rather
    # than re-executing; stored metadata is forever-retained so the row stays observable.
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        }
                    ],
                }
            ]
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # Fail step 2 so the retry crosses the 1 s capture TTL -> stored.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; mypy lacks the plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                error_rate_5xx=1.0,
            )
        )

        chain_id = uuid4()
        await _submit(stack, chain_id=chain_id, body=_BODY, short_ttl=True)
        await assert_chain_reaches_state(
            pc, chain_id, state="stored", timeout_seconds=_TERMINAL_BUDGET_SECONDS
        )

        # /stats reflects the populated stored state + the § 3 parked_total.
        stats = await pc.get_stats(instance="primary")
        assert stats.by_state.stored.count >= 1, (
            f"admin stats must surface the stored row; by_state.stored={stats.by_state.stored!r}"
        )
        assert stats.parked_total >= 1, (
            f"parked_total must count the stored row; got {stats.parked_total}"
        )

        # /status reports the aggregate reality (ready, a backlog field, disk usage).
        status = await pc.get_admin_status()
        assert status.ready is True, f"a healthy stack must report ready; {status!r}"
        assert status.total_backlog >= 0
        assert status.disk_usage_bytes >= 0

        # /ready and /health report a healthy, non-degraded service.
        ready = await pc.get_ready()
        assert ready.ready is True, f"not ready: {ready!r}"
        health = await pc.get_health()
        assert health.status == "ok"
        assert health.storage == "ok", f"writable stack must report storage ok; {health!r}"

        # NEGATIVE quarantine truthfulness: a clean deployment shows no quarantine artifacts.
        inv = await pc.get_quarantine_inventory(instance="primary")
        assert inv.quarantines == [], (
            f"a clean deployment must report an empty quarantine inventory; got {inv.quarantines!r}"
        )
    finally:
        await stack.tear_down()


async def test_admin_write_path_token_push_resumes_and_stats_track_it(tmp_path: Path) -> None:
    """A token push resumes a parked row; the stats surface tracks the count to 0.

    Falsifier: drop the auth-kicker wake on cache-set so the pushed token never re-queues
    the row -> the chain never leaves auth_expired and auth_expired_count stays 1 -> RED.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"retry": {"worker_count": 2, "poll_interval_ms": 100}},
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        # Upstream 401s from the first call: the row parks in auth_expired.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; mypy lacks the plugin
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            )
        )
        chain_id = uuid4()
        await _submit(stack, chain_id=chain_id, body=_BODY)

        async def _parked() -> bool:
            snap = await pc.get_upload(chain_id)
            return snap.state == "auth_expired"

        await await_until(
            _parked,
            timeout_seconds=_AUTH_EXPIRED_BUDGET_SECONDS,
            message=f"chain {chain_id} did not park in auth_expired",
        )

        # Admin truth WHILE parked: the stats surface counts it.
        parked_stats = await pc.get_stats(instance="primary")
        parked_count = parked_stats.auth.auth_expired_count
        assert parked_count == 1, (
            f"auth.auth_expired_count must be 1 while parked; got {parked_count}"
        )
        assert parked_stats.parked_total >= 1
        assert parked_stats.by_state.auth_expired.count == 1

        # The admin WRITE path: clear the upstream fault, push a fresh bearer.
        rows, _ = await pc.list_uploads(limit=50)
        row = next(r for r in rows if r.chain_id == chain_id)
        stack.emulator.clear_failures()
        await pc.push_token(endpoint=row.endpoint, uid=row.uid, token=stack.fake_security_token())

        # The row resumes to succeeded and the stats count drops back to 0.
        await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=_POST_PUSH_BUDGET_SECONDS
        )

        async def _count_cleared() -> bool:
            s = await pc.get_stats(instance="primary")
            return s.auth.auth_expired_count == 0

        await await_until(
            _count_cleared,
            timeout_seconds=_POST_PUSH_BUDGET_SECONDS,
            message="auth_expired_count did not return to 0 after the push + resume",
        )
        # Admin stays responsive throughout.
        assert (await pc.get_health()).status == "ok"
    finally:
        await stack.tear_down()


async def test_malformed_admin_requests_rejected_cleanly(tmp_path: Path) -> None:
    """Malformed admin requests are rejected with typed errors; admin stays responsive.

    Falsifier: return a 200/empty body for an unknown chain or instance (silent) -> the
    expected typed exception is not raised -> RED.
    """
    stack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client

        # Unknown chain_id -> 404 not_found.
        with pytest.raises(PhantomNotFoundError):
            await pc.get_upload(uuid4())

        # Unknown ?instance= -> 421 instance_unknown (a bad-request-family typed error).
        with pytest.raises(PhantomBadRequestError):
            await pc.get_stats(instance="no-such-instance")

        # A bogus restore backup_id -> 404 (no such mode_switch backup).
        with pytest.raises(PhantomNotFoundError):
            await pc.restore_quarantine_backup(backup_id=uuid4(), instance="primary")

        # Admin stayed responsive: health + a clean stats read still work.
        assert (await pc.get_health()).status == "ok"
        stats = await pc.get_stats(instance="primary")
        assert stats.parked_total >= 0
    finally:
        await stack.tear_down()
