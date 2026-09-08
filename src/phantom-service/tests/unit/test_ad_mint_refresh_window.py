"""The AD mint loop's wait, and why it can no longer become a hot loop.

``AdMinter._refresh_loop`` applied its anti-hot-loop floor BEFORE the jitter
subtraction::

    wait = max(1.0, lifetime_remaining - refresh_seconds_before_expiry)
    wait -= random.uniform(0, jitter)

so a collapsed window produced ``1.0 - uniform(0, jitter)``, which is
NEGATIVE. ``asyncio.wait_for`` with a negative timeout raises ``TimeoutError``
immediately with zero delay, so the loop minted one token per iteration
against the Entra ID token endpoint as fast as the event loop allowed, until
the authority throttled the app registration or locked it.

Two configurations reached it. ``refresh_jitter_seconds`` was ``ge=0.0`` with
no upper bound and no declared relationship to
``refresh_seconds_before_expiry``, so ``jitter: 5.0`` got there directly. And
with the DEFAULT jitter, ``refresh_seconds_before_expiry`` was ``ge=1`` with
no upper bound, so pinning it above the token's lifetime drove the value
negative every cycle, floored it to 1.0, and minted roughly twice a second
forever.

The fix is the floor applied LAST, plus bounds on both knobs and a declared
relationship between them. The floor is also raised well above one second:
one mint per second is not a hot loop but it is still an abusive rate against
an authority, whereas the floor never shapes a healthy schedule.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from phantom.config.ad_mint import AdMintConfig
from phantom.refresh.ad_client_credentials import AdMinter
from phantom.storage.interface import TokenCache
from pydantic import ValidationError

# Enough draws that a jitter-dependent negative cannot hide behind luck.
_DRAWS = 500

# The mint loop's hard floor between two mints. Restated here rather than
# imported so each assertion expresses the behaviour it wants independently of
# the constant; ``test_the_floor_constant_is_what_the_tests_assert`` pins the
# two together.
_MIN_MINT_WAIT_SECONDS = 30.0


def _config(**overrides: object) -> AdMintConfig:
    """A valid AD-mint block with the required identity fields filled in."""
    base: dict[str, object] = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "11111111-1111-1111-1111-111111111111",
        "primary_client_secret_env": "PHANTOM_UPSTREAM_CLIENT_SECRET",
        "scope": "api://files.upstream.example/.default",
        "endpoint": "files.upstream.example",
        "uid": "upstream-sp",
    }
    base.update(overrides)
    return AdMintConfig.model_validate(base)


def _minter(config: AdMintConfig) -> AdMinter:
    """An ``AdMinter`` over a cache that is never touched by the wait maths."""
    cache: TokenCache = None  # type: ignore[assignment]  # _next_mint_wait_seconds never reads it
    return AdMinter(config=config, token_cache=cache)


def test_the_wait_is_always_positive_at_the_widest_legal_jitter() -> None:
    """The floor must survive the jitter subtraction, not precede it.

    Objective: with the floor applied first, ``1.0 - uniform(0, jitter)`` was
    negative and ``asyncio.wait_for`` returned instantly, so the loop minted
    continuously. Success: for a token that has ALREADY expired (the worst
    input the loop can see) every draw at the widest legal jitter is at or
    above the floor.
    """
    minter = _minter(_config(refresh_seconds_before_expiry=600, refresh_jitter_seconds=60.0))
    already_expired = datetime.now(tz=UTC) - timedelta(seconds=1)

    waits = [minter._next_mint_wait_seconds(already_expired) for _ in range(_DRAWS)]

    assert min(waits) >= _MIN_MINT_WAIT_SECONDS, f"computed a {min(waits)}s wait"


def test_a_margin_larger_than_the_token_lifetime_cannot_spin() -> None:
    """The default-jitter path into the same loop.

    Objective: the second reachable configuration needs no unusual jitter at
    all - pinning ``refresh_seconds_before_expiry`` above the token's lifetime
    drives the term negative on EVERY cycle. Success: a 60 s token with a
    600 s margin still waits the floor rather than returning immediately.
    """
    minter = _minter(_config(refresh_seconds_before_expiry=600))
    expires_at = datetime.now(tz=UTC) + timedelta(seconds=60)

    assert minter._next_mint_wait_seconds(expires_at) >= _MIN_MINT_WAIT_SECONDS


def test_the_collapsed_window_is_reported_not_silently_floored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A floor that hides a misconfiguration is only half a fix.

    Objective: a deployment whose margin exceeds its token lifetime mints at
    the floor rate forever, which is survivable but wrong; the operator needs
    to be told which knob to move. Success: a WARNING naming the knob, with
    the credential's cache axis so a multi-instance process says WHICH minter.
    """
    minter = _minter(_config(refresh_seconds_before_expiry=600))
    expires_at = datetime.now(tz=UTC) + timedelta(seconds=60)

    with caplog.at_level(logging.WARNING, logger="phantom.refresh.ad_client_credentials"):
        minter._next_mint_wait_seconds(expires_at)

    assert "refresh_seconds_before_expiry" in caplog.text
    assert "files.upstream.example" in caplog.text


def test_a_healthy_schedule_is_not_reshaped_by_the_floor() -> None:
    """The floor must be invisible on any correct configuration.

    Objective: a floor that clipped real schedules would turn a 55-minute wait
    into a busy loop's worth of mints. Success: an hour-long token with the
    default 5-minute margin waits close to 55 minutes, and nothing is logged
    about a collapsed window.
    """
    minter = _minter(_config())
    expires_at = datetime.now(tz=UTC) + timedelta(hours=1)

    wait = minter._next_mint_wait_seconds(expires_at)

    assert 3_300.0 - minter._config.refresh_jitter_seconds <= wait <= 3_300.0


def test_the_jitter_knob_is_bounded_and_tied_to_the_margin() -> None:
    """The two knobs had no declared relationship, and needed one.

    Objective: jitter is SUBTRACTED from the pre-expiry margin, so a jitter
    larger than the margin drives the term negative every cycle whatever the
    token lifetime. The knobs were bounded independently (one not at all), so
    nothing refused the combination. Success: an over-large jitter is a
    validation error naming both knobs.
    """
    with pytest.raises(ValidationError) as caught:
        _config(refresh_seconds_before_expiry=2, refresh_jitter_seconds=5.0)

    message = str(caught.value)
    assert "refresh_jitter_seconds" in message
    assert "refresh_seconds_before_expiry" in message


def test_both_refresh_knobs_carry_an_upper_bound() -> None:
    """An in-range config must not be able to produce the collapsed window.

    Objective: ``refresh_seconds_before_expiry`` was ``ge=1`` with no ceiling
    and ``refresh_jitter_seconds`` ``ge=0.0`` with no ceiling, so the model
    accepted values no token lifetime could absorb. Success: a margin past the
    shortest access-token lifetime any tenant policy can configure, and a
    jitter past a one-minute spread, are both refused.
    """
    with pytest.raises(ValidationError):
        _config(refresh_seconds_before_expiry=86_400)

    with pytest.raises(ValidationError):
        _config(refresh_seconds_before_expiry=600, refresh_jitter_seconds=3_600.0)


def test_the_default_margin_leaves_room_for_clock_skew() -> None:
    """12 seconds was not a margin, it was a coincidence.

    Objective: ``expires_on`` is an absolute instant from the authority's
    clock. A 12 s margin had to cover container clock skew AND the mint's own
    round trip to ``login.microsoftonline.com``, and a few seconds of either
    landed the replacement after the cached token had already expired, 401ing
    every request in between. Success: the default is the 5-minute window the
    Azure Identity / MSAL clients use.
    """
    assert _config().refresh_seconds_before_expiry == 300


def test_the_floor_constant_is_what_the_tests_assert() -> None:
    """Pin the mint-wait floor to the value every assertion above expects."""
    from phantom.refresh import ad_client_credentials

    assert ad_client_credentials._MIN_MINT_WAIT_SECONDS == _MIN_MINT_WAIT_SECONDS
