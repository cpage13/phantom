"""F1 regression: an unroutable LATER step parks in ``stored`` and the service keeps draining.

Admission route-checks only the FIRST step's URL and tolerates a miss on it
(``routes/admission.py`` returns ``route_name="unknown"``), so a chain whose
SECOND step targets a host with no configured ``RouteCfg`` is durably admitted
and acked 202. Before F1 the miss surfaced at send time as a bare ``ValueError``
out of ``resolve_route``, which the sender's worker loop did not catch: it
cancelled the sender TaskGroup, unwound the lifespan TaskGroup, and in
production asked uvicorn to stop. Startup recovery then re-claimed the same row
first on every restart (``claim_due`` orders by ``next_attempt_at ASC``), so one
producer-reachable payload crash-looped the whole service and stranded the
entire buffered backlog behind it.

After F1 the executor classifies the miss as ``RouteUnresolved`` and the sender
parks the row in terminal ``stored``: body retained, saturation slot retained,
never re-claimed, replay-eligible once the operator repairs the route config.

The load-bearing assertion is the SECOND chain. Reaching ``stored`` proves the
classification; a second, fully routable chain reaching ``succeeded`` afterwards
proves the poison row no longer strands the backlog and the worker pool is still
alive. The final re-read proves the classification is terminal rather than a
retry loop.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from phantom_client import ChainBodyRef, ChainEnvelope, ChainStep

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until, settle_for

# The terminal park state an unroutable row lands in: body retained, slot
# retained, replay-eligible once the route config is repaired.
STORED_STATE: str = "stored"

# The terminal state the second, fully routable chain must reach, which is what
# proves the poison row stranded nothing behind it.
SUCCEEDED_STATE: str = "succeeded"

# The host no ``RouteCfg`` matches. ``.invalid`` is reserved by RFC 2606 so it
# can never resolve, but nothing in this test ever dials it: the row is
# classified before any transport call.
UNROUTED_HOST: str = "unrouted.invalid"

# The ``last_error`` prefix F1 stamps. The token is
# ``route_unresolved:<host>:<step_name>`` and carries no URL, path, or query
# string, so a presigned destination cannot leak into the admin API.
UNROUTED_LAST_ERROR_PREFIX: str = f"route_unresolved:{UNROUTED_HOST}:"

# Bodies for the two chains. Distinct payloads so the emulator read-back cannot
# confuse them.
POISON_BODY: bytes = b"phantom-f1-unroutable-step-never-delivered"
HEALTHY_BODY: bytes = b"phantom-f1-healthy-chain-delivered-after-the-poison-row"

# Raw-sink paths (the key under which the emulator stores a forwarded body).
POISON_FIRST_STEP_PATH: str = "f1/poison-first-step.bin"
HEALTHY_PATH: str = "f1/healthy-after-poison.bin"

# Window for the poison row to reach ``stored``. Boot is warm and the first
# retry interval is 0s, so the classification lands on the first claim; the
# budget is headroom over a loaded host, not an expected wait.
STORED_BUDGET_SECONDS: float = 15.0

# Window for the second chain to reach ``succeeded``. It is submitted only after
# the poison row settled, so it waits on one claim plus one upstream round trip.
SUCCEEDED_BUDGET_SECONDS: float = 15.0

# A real settle before the terminal re-read, giving the worker pool several
# poll intervals in which it could (wrongly) re-claim the parked row.
TERMINAL_STABILITY_SETTLE_SECONDS: float = 2.5


def _overrides() -> dict[str, object]:
    """Build the ``config_overrides`` overlay whose routes cover the emulator only.

    One ``auth_mode: none`` route matching the emulator's hostnames. The
    unrouted host is deliberately absent from BOTH ``host_prefixes`` and
    ``routes``: ``host_prefixes`` governs which instance a submission dispatches
    to (resolved from the FIRST step's URL, which is the emulator), and
    ``routes`` governs whether a step's host resolves at send time. Only the
    second is missing here, which is exactly F1's premise.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    return {
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
                        "auth_mode": "none",
                    },
                ],
            },
        ],
        "retry": {"worker_count": 1, "poll_interval_ms": 50},
        "retention": {"reaper_interval_seconds": 3600},
    }


def _poison_envelope(stack: E2EStack, chain_id: UUID) -> ChainEnvelope:
    """Build the two-step chain whose SECOND step targets an unrouted host.

    Step 1 goes to the emulator's auth-free raw sink and succeeds, which is what
    makes this the common operator case: a chain that already delivered part of
    itself before the miss is discovered. Step 2 carries an ABSOLUTE URL on
    ``unrouted.invalid``, so ``_absolute_url`` returns it unchanged and
    ``resolve_route`` finds no matching host pattern.

    Args:
        stack: The running stack, for the emulator's base URL.
        chain_id: The chain's identity and its upload row's primary key.

    Returns:
        The two-step envelope to submit.
    """
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="routed_first_step",
                method="PUT",
                url=f"{stack.emulator_url.rstrip('/')}/raw/{POISON_FIRST_STEP_PATH}",
                headers={},
                body=ChainBodyRef(
                    kind="body_ref", name="body", content_type="application/octet-stream"
                ),
                capture=[],
                idempotency_header=None,
            ),
            ChainStep(
                name="unrouted_second_step",
                method="PUT",
                url=f"http://{UNROUTED_HOST}/v1/files/upload/x",
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


def _healthy_envelope(stack: E2EStack, chain_id: UUID) -> ChainEnvelope:
    """Build the single-step, fully routable chain submitted after the poison row.

    Args:
        stack: The running stack, for the emulator's base URL.
        chain_id: The chain's identity.

    Returns:
        The one-step envelope to submit.
    """
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="healthy_step",
                method="PUT",
                url=f"{stack.emulator_url.rstrip('/')}/raw/{HEALTHY_PATH}",
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


async def _await_row_state(stack: E2EStack, chain_id: UUID, state: str, *, budget: float) -> None:
    """Poll Phantom's admin API until ``chain_id`` reaches ``state``.

    Args:
        stack: The running stack, for its :class:`PhantomClient`.
        chain_id: The chain to poll.
        state: The row state to wait for.
        budget: Maximum total wait time in seconds.
    """

    async def _reached() -> bool:
        snapshot = await stack.phantom_client.get_upload(chain_id)
        return snapshot.state == state

    await await_until(
        _reached,
        timeout_seconds=budget,
        message=f"chain {chain_id} did not reach {state!r} within {budget}s",
    )


@pytest.mark.e2e
async def test_unroutable_later_step_parks_and_the_service_keeps_draining() -> None:
    """An unroutable second step parks in ``stored`` and the next chain still succeeds.

    Objective: prove F1 end to end. A durably admitted chain whose later step has
    no route must be classified rather than raised, and the service must keep
    draining the rest of the backlog.

    Success: the poison row reaches terminal ``stored`` with a
    ``route_unresolved:`` ``last_error`` naming only the host and the step; a
    second, fully routable chain submitted afterwards reaches ``succeeded`` and
    its body lands at the emulator; and after a settle the poison row is still
    ``stored`` with an unchanged ``attempts``.
    """
    stack = await boot_stack(config_overrides=_overrides())
    try:
        poison_id = uuid4()
        # Admission route-checks the FIRST step only, so this is accepted with a
        # 202 even though step 2's host matches no route.
        poison_response = await stack.phantom_client.submit_chain(
            _poison_envelope(stack, poison_id),
            body_refs={"body": POISON_BODY},
            uid="f1-uid",
        )
        assert poison_response.chain_id == poison_id, (
            f"the unroutable-later-step chain must still be admitted and acked with its "
            f"chain id; got {poison_response.chain_id!r}"
        )

        # Checkpoint 1: the row is CLASSIFIED, not raised. Pre-fix the worker
        # fault tears the instance's stores down, so this poll surfaces an
        # admin-API error rather than a clean timeout.
        await _await_row_state(stack, poison_id, STORED_STATE, budget=STORED_BUDGET_SECONDS)
        parked = await stack.phantom_client.get_upload(poison_id)
        assert parked.state == STORED_STATE, (
            f"an unroutable step must park the row in {STORED_STATE!r}; got {parked.state!r}"
        )
        assert parked.last_error is not None
        assert parked.last_error.startswith(UNROUTED_LAST_ERROR_PREFIX), (
            f"last_error must name the unmatched host and the step; expected a "
            f"{UNROUTED_LAST_ERROR_PREFIX!r} prefix, got {parked.last_error!r}"
        )
        # The token carries no URL material: an operator repairing the route
        # config needs the host and the step, and nothing else.
        assert "/v1/files/upload" not in parked.last_error, (
            f"last_error must not embed the step URL; got {parked.last_error!r}"
        )
        parked_attempts = parked.attempts

        # Checkpoint 2 (load-bearing): the poison row stranded nothing. A second,
        # fully routable chain is admitted and delivered by the same worker pool.
        healthy_id = uuid4()
        await stack.phantom_client.submit_chain(
            _healthy_envelope(stack, healthy_id),
            body_refs={"body": HEALTHY_BODY},
            uid="f1-uid",
        )
        await _await_row_state(stack, healthy_id, SUCCEEDED_STATE, budget=SUCCEEDED_BUDGET_SECONDS)
        delivered = stack.emulator.raw_body(HEALTHY_PATH)
        assert delivered is not None, (
            "the healthy chain's body must reach the emulator, proving the worker pool "
            "survived the unroutable row"
        )
        assert delivered.body == HEALTHY_BODY, (
            f"the emulator must hold the healthy chain's exact bytes; got {delivered.body!r}"
        )

        # Checkpoint 3: the classification is terminal, not a retry loop. Give
        # the worker pool several poll intervals to (wrongly) re-claim the row.
        await settle_for(
            TERMINAL_STABILITY_SETTLE_SECONDS,
            reason="let several sender polls run; the parked row must stay stored",
        )
        still = await stack.phantom_client.get_upload(poison_id)
        assert still.state == STORED_STATE, (
            f"a stored row must never be re-claimed by claim_due; got {still.state!r}"
        )
        assert still.attempts == parked_attempts, (
            f"the route-unresolved park burns no retry budget; attempts must stay "
            f"{parked_attempts}, got {still.attempts}"
        )
    finally:
        await stack.tear_down()
