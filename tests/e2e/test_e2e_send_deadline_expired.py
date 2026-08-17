"""Phase-3/4 BACKSTOP — a parked ``auth_expired`` row past its deadline → ``expired``.

The inverse of the refresh loop (``test_e2e_sigv4_resign_round_trip.py``): there
a credential re-push arrives in time and the parked row recovers; here NO re-push
ever comes, the route's ``send_deadline_seconds`` elapses, and Phantom gives up
terminally. This is the end-to-end proof that the ``expired`` send-deadline
(ADR-032) genuinely BOUNDS the reuse-the-loop park-and-retry cycle — without it a
row whose credential is never fixed would park forever.

The path under test:

* A stock ``PUT {phantom}/{bucket}/{key}`` hits the catch-all on an ``aws_sigv4``
  route with NO credential provisioned for the destination host. The executor's
  ``aws_sigv4`` arm finds no credential slot, marks the (absent) slot bad, and
  returns a ``FailedAuth`` BEFORE any upstream call — so the row PARKS in
  ``auth_expired`` with exactly ONE attempt recorded and nothing sent upstream.

* No credential is ever pushed. The route carries ``send_deadline_seconds = 1``.
  The :class:`CredentialKicker` ``_rescan`` sweep (which reads WALL-CLOCK
  ``datetime.now(tz=UTC)`` — it is NOT clock-injectable, so a real short wait is
  required, not a monkeypatched clock) runs on its ``1.0``-second interval, sees
  the parked row's ``received_at`` is now past ``received_at + 1s``, and — BEFORE
  the credential-freshness gate, so a still-credential-less parked row IS swept
  (the exact "re-push never came" case) — transitions it ``auth_expired →
  expired`` via the shared ``expire_row`` writer: the body is discarded, the
  saturation slot is left alone (already released at park), and the row is NEVER
  re-queued.

The assertions are the backstop's contract:

* terminal ``expired`` (a member of ``TERMINAL_STATES``);
* ``last_error == "send_deadline:1s"`` — the sweep's fingerprint, written ONLY by
  ``expire_row`` on the deadline transition. It is the proof the TRANSITION was
  the deadline sweep. It is NOT proof the body was discarded, and the header
  used to claim it was: before F3 ``expire_row`` stamped ``body_discarded_at``
  and zeroed the row's accounting without ever calling ``body_store.delete``, so
  the bytes survived while the row said they were gone. The observable proof is
  the RAM measurement below;
* ``ram_body_store_bytes`` (``GET /v1/admin/observability/ram-pressure``) rises
  above its pre-submission baseline while the row is parked with its body
  retained, then returns EXACTLY to that baseline once the row reaches
  ``expired``. The rise is what stops the fall from passing vacuously: if the
  deployment were not RAM-backed, the rise fails loudly instead of leaving the
  fall meaningless;
* ``attempts == 1`` — exactly the single park attempt; the sweep is a terminal
  transition, not a send, so a second attempt would mean the backstop did NOT
  fire and the row was re-queued instead;
* the object NEVER landed upstream (the row gave up while parked — it never
  reached the emulator sink);
* terminal stability — a re-poll after a settle confirms the row stays
  ``expired`` with ``attempts == 1`` (it is not re-admitted on a later rescan).

The ``aws_sigv4`` route is REQUIRED (not incidental): the ``CredentialKicker``
``_rescan`` is inert unless the instance has a signer-credential store (an
``aws_sigv4`` route), and its deadline sweep only fires for rows whose resolved
route's ``auth_mode`` is ``aws_sigv4``. The no-credential park is the cleanest
"re-push never comes" setup — there is nothing to push back.
"""

from __future__ import annotations

from uuid import UUID

import httpx
import pytest

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until, settle_for

# Phantom's buffering ack for an admitted raw intake (a healthy admission is
# 202; delivery rides the retry worker and is asserted via the row state).
INTAKE_ACCEPTED_STATUS: int = 202

# The SHORT per-route deadline (the ``Field(None, ge=1)`` floor — the smallest
# value the config accepts). The kicker sweep reads wall-clock time and is NOT
# clock-injectable, so this drives a REAL bounded wait rather than a fake clock.
SEND_DEADLINE_SECONDS: int = 1

# The terminal give-up state the deadline sweep transitions a forever-parked row
# into (ADR-032).
EXPIRED_STATE: str = "expired"

# The recoverable, NON-terminal park state a credential-less ``aws_sigv4`` row
# lands in before the deadline elapses.
AUTH_EXPIRED_STATE: str = "auth_expired"

# The exact ``last_error`` the shared ``expire_row`` writer stamps on the
# deadline transition (``f"send_deadline:{deadline}s"``). Asserting it pins the
# transition to the deadline SWEEP rather than some other terminal path. It says
# nothing about the bytes; the RAM measurement is what proves the discard.
DEADLINE_LAST_ERROR: str = f"send_deadline:{SEND_DEADLINE_SECONDS}s"

# Exactly one send attempt is ever made: the credential-less park records one
# attempt, and the deadline sweep is a terminal transition (not a send), so the
# count must stay 1 — a 2 would mean the row was re-queued and the backstop
# failed to fire.
EXPECTED_ATTEMPTS: int = 1

# Window for the parked row to FIRST reach ``auth_expired`` after admission
# (boot is warm; the retry worker's first interval is 0s).
AUTH_EXPIRED_BUDGET_SECONDS: float = 5.0

# Window for the kicker sweep to drive the parked row to ``expired`` once the
# deadline has elapsed. The deadline is 1s and the kicker's rescan interval is
# 1.0s, so the worst-case latency is ~2s of real time; this budget is generous
# headroom over that while staying well under the suite's per-test budget.
EXPIRED_BUDGET_SECONDS: float = 12.0

# A real settle after the row is observed ``expired``, to give at least one more
# kicker rescan pass a chance to (wrongly) re-admit the row — proving the
# terminal state is STABLE, not a transient the next sweep would undo.
TERMINAL_STABILITY_SETTLE_SECONDS: float = 2.5

# The raw PUT body. Its bytes never reach the sink (the row gives up while
# parked), but a distinctive payload keeps the intake faithful.
PAYLOAD: bytes = b"phantom-send-deadline-expired\x00\xff\xfe"

# A slash-bearing object key so the catch-all's path capture is exercised.
BUCKET: str = "deadline-bucket"
KEY: str = "nested/never-delivered.bin"
OBJECT_PATH: str = f"{BUCKET}/{KEY}"


def _deadline_overrides() -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the deadline-backstop path.

    An ``aws_sigv4`` route pointed at the live emulator host, carrying a SHORT
    ``send_deadline_seconds``. The ``aws_sigv4`` ``auth_mode`` is load-bearing:
    it wires the signer-credential store (so the :class:`CredentialKicker`
    ``_rescan`` is live rather than inert) and makes the kicker's deadline sweep
    own this route's parked rows. No credential is provisioned, so the first
    forward attempt parks the row in ``auth_expired``; the deadline then sweeps
    it terminal.

    ``phantom_default_target`` carries the BARE ``{EMULATOR_URL}`` token (no
    ``/raw`` suffix) so the rewritten step URL would address the path-style
    SigV4 sink — though delivery never happens here, the route still resolves to
    the emulator host so the kicker can route the persisted endpoint.

    ``persist_trigger.body_size_threshold_bytes = 0`` pins the configuration the
    RAM assertions depend on: it disables size-aware persistence, which is the
    documented meaning of 0. Left unpinned the threshold is probe-filled from
    host RAM, so on a host whose probe yields a very small threshold admission
    would enqueue an immediate RAM-to-disk migration and the "RAM rose above
    baseline" assertion would fail for a purely environmental reason.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    return {
        "phantom_default_target": "{EMULATOR_URL}",
        "storage": {"persist_trigger": {"body_size_threshold_bytes": 0}},
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
                        "auth_mode": "aws_sigv4",
                        "send_deadline_seconds": SEND_DEADLINE_SECONDS,
                    },
                ],
            },
        ],
    }


async def _raw_put(stack: E2EStack, *, path: str, body: bytes) -> UUID:
    """Drive a stock ``httpx.put`` through Phantom's catch-all, return the chain id.

    Args:
        stack: The running stack (for the ingress URL).
        path: The object path the stock client addresses (``bucket/key``).
        body: The raw request body.

    Returns:
        The minted chain id (parsed from ``X-Phantom-Upload-Id``).
    """
    async with httpx.AsyncClient() as client:
        resp = await client.put(f"{stack.phantom_url}/{path}", content=body)
    assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
        f"raw intake expected {INTAKE_ACCEPTED_STATUS} ack, got {resp.status_code}: {resp.text!r}"
    )
    upload_id = resp.headers.get("X-Phantom-Upload-Id")
    assert upload_id, "raw-intake ack must carry X-Phantom-Upload-Id (the minted chain id)"
    return UUID(upload_id)


async def _await_row_state(stack: E2EStack, chain_id: UUID, state: str, *, budget: float) -> None:
    """Poll Phantom's admin API until ``chain_id`` reaches ``state``.

    Args:
        stack: The running stack (for its :class:`PhantomClient`).
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
async def test_send_deadline_sweeps_parked_row_to_expired() -> None:
    """A forever-parked ``auth_expired`` row past its deadline → terminal ``expired`` (not retried).

    No credential is provisioned for an ``aws_sigv4`` route carrying
    ``send_deadline_seconds = 1``; a stock PUT parks in ``auth_expired`` with one
    attempt and no upstream call; after the real 1-second deadline elapses the
    CredentialKicker sweep transitions the row to terminal ``expired`` (body
    discarded, never re-queued). The body never reaches the sink, and the row
    stays ``expired`` on a subsequent rescan.
    """
    stack = await boot_stack(config_overrides=_deadline_overrides())
    try:
        # F3 baseline: RAM body bytes BEFORE anything is buffered. The expired
        # transition must return to exactly this number.
        baseline = (
            await stack.phantom_client.get_observability_ram_pressure()
        ).ram_body_store_bytes

        # No credential push: the route's destination host has NO signer slot, so
        # the first forward attempt parks the row in auth_expired with nothing
        # sent upstream — the "re-push never comes" setup.
        chain_id = await _raw_put(stack, path=OBJECT_PATH, body=PAYLOAD)

        # Checkpoint 1: the row PARKS in auth_expired (recoverable, NOT terminal)
        # with exactly one attempt and no upstream delivery.
        await _await_row_state(
            stack, chain_id, AUTH_EXPIRED_STATE, budget=AUTH_EXPIRED_BUDGET_SECONDS
        )
        parked = await stack.phantom_client.get_upload(chain_id)
        assert parked.state == AUTH_EXPIRED_STATE, (
            f"credential-less row must PARK in {AUTH_EXPIRED_STATE!r} before the deadline; "
            f"got {parked.state!r}"
        )
        assert parked.attempts == EXPECTED_ATTEMPTS, (
            f"the no-credential park is exactly one attempt; got attempts={parked.attempts}"
        )
        assert stack.emulator.s3_object(BUCKET, KEY) is None, (
            "a credential-less park makes no upstream call, so no object can be stored at the sink"
        )
        # F3: the parked row RETAINS its body, so RAM must be above baseline
        # here. This assertion is what keeps the post-expiry one honest: if the
        # body never lived in RAM, this fails loudly rather than making the
        # return-to-baseline vacuous.
        parked_ram = (
            await stack.phantom_client.get_observability_ram_pressure()
        ).ram_body_store_bytes
        assert parked_ram > baseline, (
            f"a parked auth_expired row retains its body, so RAM body bytes must exceed the "
            f"{baseline}-byte baseline; got {parked_ram}"
        )

        # Checkpoint 2: with NO credential re-push, the real 1s deadline elapses
        # and the kicker sweep transitions the parked row to terminal ``expired``.
        # The kicker reads wall-clock time, so this is a genuine bounded wait, not
        # a fake-clock advance.
        await _await_row_state(stack, chain_id, EXPIRED_STATE, budget=EXPIRED_BUDGET_SECONDS)
        expired = await stack.phantom_client.get_upload(chain_id)
        assert expired.state == EXPIRED_STATE, (
            f"the over-deadline parked row must give up to terminal {EXPIRED_STATE!r}; "
            f"got {expired.state!r}"
        )
        # The deadline sweep's fingerprint: written ONLY by ``expire_row`` on the
        # deadline transition, and the admin-observable proof the body was
        # discarded.
        assert expired.last_error == DEADLINE_LAST_ERROR, (
            f"the deadline transition must stamp last_error={DEADLINE_LAST_ERROR!r} "
            f"(the sweep's body-discard fingerprint); got {expired.last_error!r}"
        )
        # The sweep is a terminal transition, not a send: the count stays at the
        # single park attempt. A 2 would mean the row was re-queued (the backstop
        # failed to fire).
        assert expired.attempts == EXPECTED_ATTEMPTS, (
            f"the deadline sweep must NOT re-attempt the row; attempts must stay "
            f"{EXPECTED_ATTEMPTS}, got {expired.attempts}"
        )
        # The row gave up while parked — it never reached the emulator sink.
        assert stack.emulator.s3_object(BUCKET, KEY) is None, (
            "an expired (given-up) row must never have delivered its body upstream"
        )
        # F3, the load-bearing observation: the expired transition deletes the
        # BYTES, not just the row-side stamp. Before F3 ``expire_row`` stamped
        # ``body_discarded_at`` and zeroed the accounting while the RAM body
        # survived for the process lifetime, unreachable by the reaper (it
        # filters on the stamp), by RamBodyStore.list_orphans (it returns []),
        # and by the PersistController (it skips stamped rows).
        expired_ram = (
            await stack.phantom_client.get_observability_ram_pressure()
        ).ram_body_store_bytes
        assert expired_ram == baseline, (
            f"the expired transition must free the body bytes, returning RAM to the "
            f"{baseline}-byte baseline; got {expired_ram}"
        )

        # Checkpoint 3: terminal STABILITY. Give at least one more kicker rescan
        # pass a chance to (wrongly) re-admit the row, then confirm it is still
        # ``expired`` with the same single attempt — proof the backstop is a
        # genuine terminal give-up, not a transient a later sweep undoes.
        await settle_for(
            TERMINAL_STABILITY_SETTLE_SECONDS,
            reason="let one more CredentialKicker rescan pass run; the row must stay expired",
        )
        still = await stack.phantom_client.get_upload(chain_id)
        assert still.state == EXPIRED_STATE, (
            f"an expired row must NOT be re-admitted on a later rescan; got {still.state!r}"
        )
        assert still.attempts == EXPECTED_ATTEMPTS, (
            f"an expired row must never be re-attempted; attempts must stay "
            f"{EXPECTED_ATTEMPTS}, got {still.attempts}"
        )
        assert stack.emulator.s3_object(BUCKET, KEY) is None, (
            "an expired row stays un-delivered — no object may appear at the sink after settle"
        )
    finally:
        await stack.tear_down()
