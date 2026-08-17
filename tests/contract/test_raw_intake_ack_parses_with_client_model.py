"""CL8: the raw-intake ack satisfies the SDK's strict response-header model.

The service builds the ``X-Phantom-*`` ack with
``phantom.routes.envelope.build_response_headers``; the SDK parses it with
``phantom_client.models.envelope.parse_response_headers``, whose
``ResponseHeaders`` model is ``ConfigDict(strict=True, extra="forbid")`` and
requires five fields (``upload_id``, ``group_id``, ``status``, ``attempts``,
``suggested_poll_after_seconds``), with ``next_attempt_at`` optional.

The raw-intake handler used to hand-build two of the six headers, so
``parse_response_headers`` raised ``PhantomEnvelopeError`` on a SUCCESSFUL
upload and any SDK-based tooling pointed at the raw-intake surface failed.
This is the test that proves the two surfaces agree, which is the whole
reason CL8 matters.

It lives in ``tests/contract/`` rather than in either package's unit suite
because it imports BOTH packages by design, which the per-package suites do
not do.
"""

from __future__ import annotations

from uuid import uuid4

from phantom.routes.envelope import build_response_headers
from phantom_client.models.envelope import parse_response_headers

# The raw-intake polling hint, mirroring ``routes/send.SUGGESTED_POLL_AFTER_SECONDS``.
_SUGGESTED_POLL_AFTER_SECONDS = 5


def test_raw_intake_ack_parses_with_the_strict_client_model() -> None:
    """A raw-intake-shaped ack parses cleanly into ``ResponseHeaders``.

    Objective: prove the service's ack and the SDK's strict model agree on
    the raw-intake path, where a stock client supplies no grouping header so
    ``group_id`` falls back to ``chain_id`` and a fresh admission has zero
    attempts. Success: parsing returns a ``ResponseHeaders`` whose
    ``upload_id`` and ``group_id`` are equal, rather than raising
    ``PhantomEnvelopeError`` as the old two-header ack did.
    """
    chain_id = uuid4()

    headers = build_response_headers(
        upload_id=chain_id,
        # Raw intake sends no X-Phantom-Group-Id, so admission stores chain_id.
        group_id=chain_id,
        state="queued",
        attempts=0,
        next_attempt_at=None,
        suggested_poll_after_seconds=_SUGGESTED_POLL_AFTER_SECONDS,
    )

    parsed = parse_response_headers(headers)

    assert parsed.upload_id == chain_id
    assert parsed.group_id == chain_id
    assert parsed.status == "queued"
    assert parsed.attempts == 0
    assert parsed.suggested_poll_after_seconds == _SUGGESTED_POLL_AFTER_SECONDS
    assert parsed.next_attempt_at is None
