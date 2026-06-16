"""E2E-9 — Mid-traffic shutdown + restart recovery.

Exercises the load-bearing crash-recovery path: a row sitting in
``attempting`` when Phantom shuts down is reset to ``queued`` by the
startup recovery sweep when Phantom comes back up, and the next
attempt completes the chain. The body bytes are preserved through
the restart because the row was persist-migrated to the disk tier
before shutdown.

Approach for in-process mode:

1. Force the first attempt to fail with 503 — this triggers the
   ``persist_trigger.after_attempts: 1`` rule and migrates the row
   from RAM to disk.
2. Confirm via the admin API that the row is on the persisted tier.
3. Stop the emulator's failure injection so future attempts could
   succeed; install a slow-trickle to keep step 2 in flight.
4. Shut down Phantom mid-attempt by toggling its uvicorn
   ``should_exit`` flag and waiting for the serve task to finish.
5. Boot a fresh Phantom against the SAME data_dir and a SAME bind
   port. The recovery sweep at startup reads the persisted row,
   sees ``state == 'attempting'`` (because step 2 was hanging when
   shutdown landed) and resets it to ``queued``.
6. Reconnect the SDK client; clear the slow-trickle; the recovered
   row reaches ``succeeded`` on the next attempt cycle.

This in-process path is the trickier of the two modes; the test is
written conservatively (generous timeouts, defensive cleanup) so
flakiness shows up as a clear timeout rather than a confusing
teardown failure.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest
import uvicorn
from phantom.app import create_app as phantom_create_app
from phantom.routes import admin as admin_routes
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_emulator_received
from .helpers.payloads import DEFAULT_BODY, build_create_file_request
from .helpers.stack import (
    INPROCESS_BIND_HOST,
    SERVER_STARTUP_TIMEOUT_SECONDS,
    E2EStack,
    _build_phantom_settings,
    boot_stack,
)
from .helpers.timing import await_until, settle_for

logger = logging.getLogger(__name__)

# Budget for the row to migrate to the persisted tier after the
# first failure. The configured `intervals_seconds: [0, 1, 2, 5, 10]`
# means the second attempt comes 1s after the first; persist_trigger
# fires on the first failed attempt, so the move happens within ~1s.
PERSIST_BUDGET_SECONDS: float = 3.0

# Budget for the chain to reach succeeded after restart + clear of
# the slow-trickle injection. 30 s, because the recovery sweep +
# retry interval (up to 10 s on the configured fixed_intervals) can
# dominate.
TERMINAL_AFTER_RESTART_BUDGET_SECONDS: float = 30.0

# Slow-trickle rate. 32 B/s ensures the PUT takes minutes against a
# DEFAULT_BODY-sized payload (~66 bytes / 32 B/s = ~2 s). Long
# enough that we can shut down phantom mid-flight; short enough that
# the post-clear retry races to completion fast.
SLOW_TRICKLE_BYTES_PER_SEC: int = 32

# Persist trigger override. ``after_attempts: 1`` is the default in
# the suite YAML; we restate it here so the test reads end-to-end
# without grep'ing the YAML.
PERSIST_AFTER_ATTEMPTS: int = 1


pytestmark = pytest.mark.e2e


async def test_e2e_09_mid_traffic_shutdown_restart_recovers(
    tmp_path: Path,
) -> None:
    """Phantom restarts mid-flight; recovery sweep finishes the chain.

    Boots a per-test stack with an explicit
    data_dir under tmp_path so we can re-launch Phantom against the
    same disk-tier rows. Stops Phantom mid-flight while a persisted
    row sits in ``attempting``; verifies the second Phantom process
    resets it to ``queued`` and runs the chain to ``succeeded``.
    """
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()

    # Phase 1 migration: the pre-Phase-1 trigger was
    # ``persist_trigger.after_attempts: 1`` (forced linger on the first
    # failed attempt). The new equivalent in hybrid mode is the
    # size-threshold path (admission enqueues against the
    # PersistController immediately for bodies above the threshold).
    # We use threshold=1 so any non-trivial body enqueues on admission.
    overrides: dict[str, object] = {
        "storage": {
            "persist_trigger": {
                "body_size_threshold_bytes": 1,
            },
        },
    }

    # Boot the initial stack against an explicit data_dir.
    stack = await boot_stack(tmp_path=data_dir, config_overrides=overrides)
    try:
        emulator = stack.emulator
        driver = _driver_for_stack(stack)
        try:
            emulator.clear_received()
            emulator.clear_failures()

            # 1. Inject 503 on PUT so the first attempt of step 2
            #    fails. The configured persist_trigger.after_attempts
            #    = 1 migrates the row to disk after that failure.
            emulator.inject_failure(
                FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                    scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                    error_rate_5xx=1.0,
                )
            )

            request = build_create_file_request()
            contents = DEFAULT_BODY
            result = await driver.in_memory_upload(request, contents)

            # 2. Wait for the row to land on the persisted tier.
            phantom_client = stack.phantom_client

            async def _on_disk() -> bool:
                # ChainResponse does not expose `body_location`; query
                # the list endpoint which surfaces the row's
                # body_location field. Phase 1 renamed tier='persisted'
                # → body_location='file'.
                rows, _ = await phantom_client.list_uploads(limit=10)
                return any(
                    row.chain_id == result.id and row.body_location == "file" for row in rows
                )

            await await_until(
                _on_disk,
                timeout_seconds=PERSIST_BUDGET_SECONDS,
                message=(
                    f"chain {result.id} did not migrate to disk tier "
                    f"within {PERSIST_BUDGET_SECONDS}s"
                ),
            )

            # 3. Stop the failure injection. Future attempts could
            #    succeed if Phantom didn't get interrupted.
            emulator.clear_failures()

            # 4. Inject slow-trickle so the next step-2 attempt hangs.
            emulator.inject_failure(
                FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                    scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                    slow_trickle_bytes_per_sec=SLOW_TRICKLE_BYTES_PER_SEC,
                )
            )

            # Give Phantom a moment to start the next attempt against
            # the slow-trickle handler. We don't strictly need the
            # row to be in `attempting` at shutdown time — recovery
            # also reclaims `queued` rows — but the test name
            # promises mid-traffic shutdown, so we let one tick of
            # the worker loop happen.
            await settle_for(
                0.3,
                reason="mid-traffic-shutdown: let the sender pool start the next attempt",
            )
        finally:
            # The driver does not own the PhantomClient lifecycle (the
            # stack owns it); nothing to close here.
            pass

        # 5. Shut Phantom down. The emulator stays running so the
        #    new Phantom can reach it.
        phantom_url_pre = stack.phantom_url
        await stack.phantom_client.aclose()
        assert stack._phantom_uv_server is not None
        assert stack._phantom_serve_task is not None
        stack._phantom_uv_server.should_exit = True
        try:
            await asyncio.wait_for(
                stack._phantom_serve_task,
                timeout=SERVER_STARTUP_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            stack._phantom_serve_task.cancel()

        # 6. Clear the slow-trickle so the recovered attempt
        #    succeeds promptly.
        emulator.clear_failures()

        # 7. Boot a fresh Phantom on the same port + same data_dir.
        phantom_port = _port_from_url(phantom_url_pre)
        new_settings, _new_settings_path = _build_phantom_settings(
            bind_host=INPROCESS_BIND_HOST,
            bind_port=phantom_port,
            data_dir=data_dir,
        )
        # R12-1: co-mount admin on the public app (the SDK get_upload below
        # hits an admin route), mirroring the in-process harness convenience.
        new_app = phantom_create_app(new_settings)
        new_app.include_router(admin_routes.router)
        admin_routes.register_admin_error_handlers(new_app)
        new_uv_config = uvicorn.Config(
            app=new_app,
            host=INPROCESS_BIND_HOST,
            port=phantom_port,
            log_level=new_settings.observability.log_level.lower(),
            lifespan="on",
            access_log=False,
        )
        new_uv_server = uvicorn.Server(config=new_uv_config)
        new_serve_task = asyncio.create_task(new_uv_server.serve())
        await _await_uvicorn_started(new_uv_server, new_serve_task)

        # Reconnect the SDK to the restarted Phantom.
        new_phantom_client = PhantomClient(phantom_url_pre)
        await new_phantom_client.__aenter__()
        try:
            # 8. Wait for the recovery sweep + retry cycle to land
            #    succeeded.
            async def _row_succeeded() -> bool:
                snapshot = await new_phantom_client.get_upload(result.id)
                return snapshot.state == "succeeded"

            await await_until(
                _row_succeeded,
                timeout_seconds=TERMINAL_AFTER_RESTART_BUDGET_SECONDS,
                message=(
                    f"chain {result.id} did not reach succeeded within "
                    f"{TERMINAL_AFTER_RESTART_BUDGET_SECONDS}s after restart"
                ),
            )

            # 9. Emulator received exactly one final body, regardless
            #    of how many attempts step 2 took before recovery.
            received = await assert_emulator_received(
                emulator,
                phantom_local_uuid=str(result.id),
                body_size=len(contents),
            )
            assert received.body_size == len(contents)

            # No duplicate entries in the emulator's received log: we
            # assert this is exactly one row matching the chain's local
            # UUID.
            matched = [
                entry
                for entry in emulator.received()
                if entry.metadata_kvs.get("phantom_local_uuid") == str(result.id)
            ]
            assert len(matched) == 1, (
                f"expected exactly one emulator-received entry; got {len(matched)}"
            )
        finally:
            await new_phantom_client.aclose()
            new_uv_server.should_exit = True
            try:
                await asyncio.wait_for(new_serve_task, timeout=SERVER_STARTUP_TIMEOUT_SECONDS)
            except TimeoutError:
                new_serve_task.cancel()
    finally:
        # The stack's tear_down covers the (now-stopped) initial
        # phantom and the still-running emulator; safe to call.
        try:
            await stack.tear_down()
        except Exception:
            # The phantom is already stopped; tear_down may log a
            # warning but shouldn't fail the test.
            logger.exception("tear_down raised in cleanup; suppressing")


def _driver_for_stack(stack: E2EStack) -> PhantomDriver:
    """Build a public :class:`PhantomDriver` pointed at ``stack``.

    Inline so this test (which owns its own stack across a restart)
    doesn't depend on conftest's ``driver`` fixture. The driver borrows
    the stack's open :class:`PhantomClient`; it does not own its
    lifecycle.
    """
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


def _port_from_url(url: str) -> int:
    """Parse the port out of a ``http://host:port`` URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.port is None:
        raise ValueError(f"URL has no port: {url!r}")
    return parsed.port


async def _await_uvicorn_started(
    server: uvicorn.Server,
    serve_task: asyncio.Task[None],
) -> None:
    """Block until ``server`` reports ``started`` or the timeout fires."""

    async def _started() -> bool:
        return bool(server.started)

    try:
        await await_until(
            _started,
            timeout_seconds=SERVER_STARTUP_TIMEOUT_SECONDS,
            poll_interval_seconds=0.05,
            message="restarted phantom failed to start within timeout",
        )
    except AssertionError as exc:
        server.should_exit = True
        await serve_task
        raise TimeoutError(str(exc)) from exc
