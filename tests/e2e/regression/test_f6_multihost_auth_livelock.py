"""F6 via D2: a multi-host chain must not livelock on the wrong host's token.

The executor authenticates against the CURRENT step's host; the kickers keyed
their wake probe on ``row.endpoint``, which is the FIRST step's host, captured
once at admission and never updated. On a single-host chain the two strings
match and the defect is invisible. On a multi-host chain they differ:

1. Step 2's host has no usable credential slot, so the executor returns
   ``FailedAuth`` and the sender parks the row in ``auth_expired``.
2. The ``AuthKicker`` rescans at 1 Hz, probes step 1's host, finds it FRESH,
   and re-queues the row.
3. The sender drives step 2 against the same bad slot. Go to 1.

Nothing bounds that cycle: there is no attempt cap on the auth path, the send
deadline defaults to ``None``, and retention gates on ``updated_at``, which
every cycle refreshes. The observable cost is a known-bad credential re-sent
upstream at 1 Hz forever plus a saturation ledger churned at the same rate.

D2 records the host whose credential slot actually rejected the row in the new
``uploads.auth_blocked_host`` column, and both kickers key their probe on it.

**This witness needs no second host and no loopback alias.** The shipped e2e
route already lists ``emulator``, ``127.0.0.1`` and ``localhost`` on one
``phantom_bearer`` route, so two names for the same emulator are ordinary
config rather than a contrived topology, and the ``127.0.0.2`` environment
caveat does not apply here.

Assertion order is load-bearing and is the order the plan gives: the two
``attempts`` readings come FIRST, because they run on the pre-fix tree and fail
behaviourally, while the ``auth_blocked_host`` assertion references a field
that does not exist pre-fix and would fail on shape instead.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from phantom_client import ChainBodyRef, ChainEnvelope, ChainStep

from ..helpers.stack import E2EStack, boot_stack
from ..helpers.timing import await_until, settle_for

# The non-terminal park state a rejected credential lands the row in.
AUTH_EXPIRED_STATE: str = "auth_expired"

# The terminal state the row must reach once the BLOCKED host's slot goes
# fresh, which proves the fix wakes on the right key rather than never waking.
SUCCEEDED_STATE: str = "succeeded"

# The two names for the one emulator. Both are on the same ``phantom_bearer``
# route, and ``localhost`` resolves to the loopback the emulator already binds,
# so no alias and no second server are involved.
FIRST_STEP_HOST: str = "127.0.0.1"
BLOCKED_HOST: str = "localhost"

# The caller identity. One uid per chain, which is what makes the recorded host
# the only part of the bearer slot key the row was missing.
UID: str = "f6-uid"

# The bearer pushed into a slot. The emulator's ``/raw`` sink is auth-free, so
# any non-empty string delivers; the value is never validated upstream.
BEARER: str = "f6-bearer-token"

# Raw-sink paths, one per step, so the emulator read-back cannot confuse them.
FIRST_STEP_PATH: str = "f6/step-one-delivered.bin"
SECOND_STEP_PATH: str = "f6/step-two-blocked.bin"

BODY: bytes = b"phantom-f6-multihost-auth-livelock"

# Window for the row to park after step 1 delivers. The first retry interval is
# 0s and boot is warm, so this is headroom over a loaded host.
PARKED_BUDGET_SECONDS: float = 20.0

# The settle between the two ``attempts`` readings. The kicker rescans at 1.0s,
# so this window holds several rescans: pre-fix the row is re-queued and
# re-parked repeatedly inside it, post-fix nothing moves.
LIVELOCK_SETTLE_SECONDS: float = 4.0

# Window for the row to finish once the BLOCKED host's slot is fresh.
SUCCEEDED_BUDGET_SECONDS: float = 20.0


def _overrides() -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the multi-host witness.

    One ``phantom_bearer`` route covering both names of the one emulator. This
    mirrors the shipped ``tests/e2e/phantom-config.yml`` route, which already
    lists ``emulator``, ``127.0.0.1`` and ``localhost`` together: a chain
    addressing the upstream by two names in that list is ordinary config, which
    is the whole reachability argument for F6.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    return {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["emulator", FIRST_STEP_HOST, BLOCKED_HOST],
                "data_dir": "primary",
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["emulator", FIRST_STEP_HOST, BLOCKED_HOST],
                        "auth_mode": "phantom_bearer",
                    },
                ],
            },
        ],
        "retry": {"worker_count": 1, "poll_interval_ms": 50},
        "retention": {"reaper_interval_seconds": 3600},
    }


def _emulator_port(stack: E2EStack) -> str:
    """Return the emulator's ``host:port`` authority.

    Args:
        stack: The running stack, whose ``emulator_url`` is bound on
            ``127.0.0.1``.

    Returns:
        The authority substring, for example ``127.0.0.1:54321``.
    """
    return stack.emulator_url.rstrip("/").split("//", 1)[1]


def _multihost_envelope(stack: E2EStack, chain_id: UUID) -> ChainEnvelope:
    """Build the two-step chain whose steps address one emulator by two names.

    Step 1 targets ``127.0.0.1`` and delivers against the slot the test pushes.
    Step 2 targets ``localhost``, whose slot is deliberately absent, so the
    executor returns ``FailedAuth`` BEFORE contacting the emulator: the park is
    pre-flight and does not depend on the auth-free ``/raw`` sink rejecting
    anything.

    Args:
        stack: The running stack, for the emulator's authority.
        chain_id: The chain's identity and its upload row's primary key.

    Returns:
        The two-step envelope to submit.
    """
    port = _emulator_port(stack).split(":", 1)[1]
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="first_host_step",
                method="PUT",
                url=f"http://{FIRST_STEP_HOST}:{port}/raw/{FIRST_STEP_PATH}",
                headers={},
                body=ChainBodyRef(
                    kind="body_ref", name="body", content_type="application/octet-stream"
                ),
                capture=[],
                idempotency_header=None,
            ),
            ChainStep(
                name="blocked_host_step",
                method="PUT",
                url=f"http://{BLOCKED_HOST}:{port}/raw/{SECOND_STEP_PATH}",
                headers={},
                body=ChainBodyRef(
                    kind="body_ref", name="body", content_type="application/octet-stream"
                ),
                capture=[],
                idempotency_header=None,
            ),
        ],
        default_target=None,
    )


@pytest.mark.e2e
async def test_a_multihost_chain_does_not_livelock_on_the_wrong_hosts_token() -> None:
    """A row parked on host B is not woken by host A's fresh token.

    Objective: prove F6 end to end on ordinary multi-host config. The row must
    stay parked while only the FIRST step's host has a fresh slot, and it must
    still wake the moment the BLOCKED host's slot goes fresh.

    Success: two ``attempts`` readings taken a settle window apart are EQUAL
    (the row is parked and staying parked); the admin per-chain read names
    ``localhost`` as the blocked host; and pushing a token for ``localhost``
    drives the chain to ``succeeded``.

    Pre-fix observable: the two readings DIFFER, because the ``AuthKicker``
    re-queues the row against ``127.0.0.1``'s fresh slot at its 1 Hz rescan and
    the sender re-parks it each time. That is the livelock, and the final leg
    is never reached.
    """
    stack = await boot_stack(config_overrides=_overrides())
    try:
        # Seed the FIRST step's slot only. ``localhost`` gets nothing, which is
        # what makes step 2's credential the one that actually blocks the row.
        await stack.phantom_client.push_token(
            endpoint=FIRST_STEP_HOST,
            uid=UID,
            token=BEARER,
        )

        chain_id = uuid4()
        response = await stack.phantom_client.submit_chain(
            _multihost_envelope(stack, chain_id),
            body_refs={"body": BODY},
            uid=UID,
        )
        assert response.chain_id == chain_id, (
            f"the multi-host chain must be admitted and acked with its chain id; "
            f"got {response.chain_id!r}"
        )

        async def _parked() -> bool:
            snapshot = await stack.phantom_client.get_upload(chain_id)
            return snapshot.state == AUTH_EXPIRED_STATE

        await await_until(
            _parked,
            timeout_seconds=PARKED_BUDGET_SECONDS,
            message=(
                f"chain {chain_id} did not park in {AUTH_EXPIRED_STATE!r} within "
                f"{PARKED_BUDGET_SECONDS}s"
            ),
        )

        # Step 1 really delivered, so the park is step 2's and the two hosts are
        # both live names for the one emulator.
        delivered = stack.emulator.raw_body(FIRST_STEP_PATH)
        assert delivered is not None, (
            "step 1 must deliver against the 127.0.0.1 slot before the chain parks on "
            "the second host; nothing reached the emulator"
        )

        # THE WITNESS, and it runs on the pre-fix tree: two attempts readings a
        # settle window apart. Equal means parked and staying parked; different
        # means the kicker is re-queueing the row against the wrong host's slot.
        first_reading = (await stack.phantom_client.get_upload(chain_id)).attempts
        await settle_for(
            LIVELOCK_SETTLE_SECONDS,
            reason="let several kicker rescans run; a correctly parked row must not move",
        )
        second_reading = (await stack.phantom_client.get_upload(chain_id)).attempts
        assert second_reading == first_reading, (
            f"the row must stay parked while only {FIRST_STEP_HOST}'s slot is fresh; "
            f"attempts moved from {first_reading} to {second_reading}, which is the F6 "
            f"livelock: the kicker probed the FIRST step's host and re-queued a row "
            f"whose actual blocker is {BLOCKED_HOST}"
        )

        parked = await stack.phantom_client.get_upload(chain_id)
        assert parked.state == AUTH_EXPIRED_STATE, (
            f"the row must still be parked after the settle; got {parked.state!r}"
        )
        assert parked.auth_blocked_host == BLOCKED_HOST, (
            f"the admin per-chain read must name the host whose credential is blocking "
            f"this row; expected {BLOCKED_HOST!r}, got {parked.auth_blocked_host!r}"
        )

        # The counter-test: the fix must not simply stop waking rows. A fresh
        # slot on the BLOCKED host wakes it and the chain finishes.
        await stack.phantom_client.push_token(
            endpoint=BLOCKED_HOST,
            uid=UID,
            token=BEARER,
        )

        async def _succeeded() -> bool:
            snapshot = await stack.phantom_client.get_upload(chain_id)
            return snapshot.state == SUCCEEDED_STATE

        await await_until(
            _succeeded,
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
            message=(
                f"chain {chain_id} did not reach {SUCCEEDED_STATE!r} within "
                f"{SUCCEEDED_BUDGET_SECONDS}s after {BLOCKED_HOST}'s slot went fresh"
            ),
        )
        second_body = stack.emulator.raw_body(SECOND_STEP_PATH)
        assert second_body is not None, (
            "the blocked step must deliver once its own host's slot is fresh"
        )
        assert second_body.body == BODY, (
            f"the emulator must hold the chain's exact bytes; got {second_body.body!r}"
        )
    finally:
        await stack.tear_down()
