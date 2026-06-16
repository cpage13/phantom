"""H6 audit closure — AdMinter is supervised by composition-root TaskGroup.

Phase 2 § 3.2.5. Before this fix ``AdMinter.start()`` spawned the
refresh loop via ``asyncio.create_task`` — the task was unsupervised
and a silent exception in the mint loop left the runtime believing the
minter was healthy. The fix replaces ``start()``/``stop()`` with a
``run(stop_event)`` coroutine that the lifespan TaskGroup invokes.

Regression contract:

1. ``AdMinter.run`` exists and accepts a single ``stop_event`` arg.
2. ``AdMinter`` has no ``start()`` / ``stop()`` methods (no parallel
   schema — both gone with the surface they served).
3. A failure inside the refresh loop (with empty backoff schedule)
   propagates out of ``run()`` as a real exception — the supervising
   TaskGroup observes the failure and cancels siblings rather than
   the runtime silently dropping the minter.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest
from phantom.config.ad_mint import AdMintConfig
from phantom.refresh.ad_client_credentials import AdMinter, AuthUnavailableError


def _cfg() -> AdMintConfig:
    """Build a minimal AdMintConfig for tests; empty backoff = fail-fast."""
    return AdMintConfig.model_validate(
        {
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "primary_client_secret_env": "NONEXISTENT_PRIMARY_ENV",
            "secondary_client_secret_env": None,
            "scope": "api://test/.default",
            "endpoint": "files.example.com",
            "uid": "phantom-test",
            "authority_url": "https://login.microsoftonline.com",
            "refresh_seconds_before_expiry": 60,
            "refresh_jitter_seconds": 5,
            # Empty backoff — fail-fast on the first AuthUnavailableError.
            "ad_outage_retry_seconds": [],
        }
    )


class _StubCache:
    """Stub TokenCache — never invoked because mint always fails."""

    async def get(self, endpoint: str, uid: str) -> None:  # pragma: no cover
        return None

    async def set(self, **_kw: object) -> None:  # pragma: no cover
        pass

    async def start(self) -> None:  # pragma: no cover
        pass

    async def stop(self) -> None:  # pragma: no cover
        pass


def test_ad_minter_exposes_run_not_start() -> None:
    """``AdMinter`` has ``run(stop_event)``; ``start()``/``stop()`` are gone."""
    assert hasattr(AdMinter, "run"), "AdMinter.run must exist (H6 closure)"
    # Verify no parallel schema — the legacy spawn API is fully removed.
    assert not hasattr(AdMinter, "start"), (
        "AdMinter.start removed in H6 closure (no parallel schema)"
    )
    assert not hasattr(AdMinter, "stop"), "AdMinter.stop removed in H6 closure (no parallel schema)"
    # ``run`` is async and takes (self, stop_event).
    sig = inspect.signature(AdMinter.run)
    assert "stop_event" in sig.parameters
    assert inspect.iscoroutinefunction(AdMinter.run)


@pytest.mark.asyncio
async def test_ad_minter_run_propagates_exception_under_taskgroup() -> None:
    """An ``AuthUnavailableError`` with empty backoff propagates out of run().

    H6 closure regression: the failure is observable to the supervising
    TaskGroup. Pre-Phase-2 the same failure would have been swallowed
    by ``asyncio.create_task`` because the task was unsupervised.
    """
    minter = AdMinter(config=_cfg(), token_cache=_StubCache())  # type: ignore[arg-type]
    stop = asyncio.Event()
    with pytest.raises(BaseExceptionGroup) as exc_info:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(minter.run(stop), name="ad-minter")
    # The inner exception is AuthUnavailableError (empty backoff →
    # fail-fast); TaskGroup wraps in ExceptionGroup.
    inner = exc_info.value.exceptions
    assert any(isinstance(e, AuthUnavailableError) for e in inner), inner


@pytest.mark.asyncio
async def test_ad_minter_run_exits_cleanly_when_stop_event_set() -> None:
    """Setting the supervising ``stop_event`` exits ``run()`` without raising.

    With a non-empty backoff schedule the mint failure is retried per
    the schedule; setting the stop event mid-retry must unblock the
    backoff sleep and let ``run()`` exit normally (no exception, no
    hang).
    """
    cfg_dict = _cfg().model_dump()
    # Long backoff so the test must set stop_event to make progress.
    cfg_dict["ad_outage_retry_seconds"] = [3600]
    cfg = AdMintConfig.model_validate(cfg_dict)
    minter = AdMinter(config=cfg, token_cache=_StubCache())  # type: ignore[arg-type]
    stop = asyncio.Event()

    async def _drive() -> None:
        # Spawn the run loop; cancel via stop_event after a small delay.
        await asyncio.sleep(0.05)
        stop.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(minter.run(stop), name="ad-minter")
        tg.create_task(_drive(), name="driver")
    # Reached without exception — the stop_event path exits cleanly.
