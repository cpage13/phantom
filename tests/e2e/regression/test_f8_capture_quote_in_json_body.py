"""F8 regression: a captured double quote no longer destroys the next step's body.

The executor rendered a JSON body by serializing the template FIRST and
regex-substituting the captured value into the serialized string, with no
escaping and no type check. A capture is upstream response data, so any
ordinary value carrying a character JSON escapes, a double quote or a
backslash or a newline, produced malformed JSON on the wire. The upstream
answered ``400``, the sender classified that ``Failed4xx``, the row went
terminal ``failed`` with ``last_error=4xx_status_400``, and the buffered
upload was permanently lost. Replay reproduced the failure identically,
because the capture was already persisted.

Nothing about the producer's chain was wrong. The value came back from the
upstream and Phantom corrupted it on the way out.

The fix substitutes into the PARSED body and serializes once at the end, so
``json.dumps`` does the escaping and malformed output is structurally
impossible rather than guarded against.

This is the operator-visible outcome end to end. The chain's first step posts
a quote-bearing name and captures the upstream's echo of it; the second step
sends that capture as a JSON body to the one emulator endpoint that PARSES the
body and answers ``400`` on malformed JSON, and echoes the name back again.
The chain reaches ``succeeded`` and the second step's own capture proves the
emulator read the quote intact.
"""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import httpx
import pytest

from ..helpers.stack import E2EStack, boot_stack
from ..helpers.timing import await_until

pytestmark = pytest.mark.e2e

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Phantom's buffering ack for an admitted envelope.
SEND_ACCEPTED_STATUS: int = 202

# Window for the two-step chain to reach terminal success.
SUCCEEDED_BUDGET_SECONDS: float = 20.0
SUCCEEDED_STATE: str = "succeeded"

# The captured value, and the whole point of the test: an ordinary product
# name carrying a double quote. Nothing exotic, nothing adversarial.
QUOTED_NAME: str = 'He said "hi"'

# The emulator's create-file endpoint. It is the ONE endpoint that json.loads
# the request body and answers 400 on a parse failure, which is what makes the
# pre-fix failure real rather than asserted.
CREATE_PATH: str = "/v1/files/create"

# The JSONPath the emulator's echo of ``fileName`` lands under.
ECHO_PATH: str = "$.fileInformation.name"


def _two_step_envelope(*, emulator_url: str, chain_id: UUID) -> dict[str, object]:
    """Build the capture-then-resend chain, as raw wire JSON.

    Step one posts the quote-bearing name and captures the upstream's echo of
    it. Step two sends that capture straight back as a whole-value placeholder
    in its own JSON body, which is the position the defect lived in, and
    captures the second echo so the delivered bytes are observable from the
    admin surface.

    The envelope is built as a plain dict rather than through the SDK so the
    test owns the exact wire bytes.

    Args:
        emulator_url: The live emulator's base URL.
        chain_id: The chain's identity.

    Returns:
        The envelope as a plain dict.
    """
    create_url = f"{emulator_url.rstrip('/')}{CREATE_PATH}"
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": create_url,
                "headers": {"Content-Type": "application/json"},
                "body": {"kind": "json", "value": {"fileName": QUOTED_NAME}},
                "capture": [{"name": "title", "from": ECHO_PATH}],
                "idempotency_header": None,
            },
            {
                "name": "resend_title",
                "method": "POST",
                "url": create_url,
                "headers": {"Content-Type": "application/json"},
                "body": {"kind": "json", "value": {"fileName": "{{create_file.title}}"}},
                "capture": [{"name": "echo", "from": ECHO_PATH}],
                "idempotency_header": None,
            },
        ],
        "default_target": None,
    }


async def _raw_send(
    client: httpx.AsyncClient,
    *,
    phantom_url: str,
    envelope: dict[str, object],
    bearer: str,
) -> httpx.Response:
    """POST a JSON envelope to ``/v1/send`` over raw HTTP.

    Args:
        client: An open httpx client.
        phantom_url: Phantom's ingress base URL.
        envelope: The envelope dict to serialise verbatim.
        bearer: The token for the ``Authorization`` header.

    Returns:
        The raw response, asserted on by the caller.
    """
    return await client.post(
        f"{phantom_url}/v1/send",
        content=json.dumps(envelope).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Phantom-Uid": DEFAULT_SUB,
            "Authorization": f"Bearer {bearer}",
        },
    )


async def test_quote_bearing_capture_is_delivered_not_failed() -> None:
    """A quote-bearing capture reaches the upstream as valid JSON.

    Objective: the F8 defect end to end, on ordinary producer data. The value
    is an upstream-supplied name with a double quote in it, resent as a JSON
    body by the next step.

    Success: the chain reaches ``succeeded``, and the second step's capture
    equals the original name, which proves the emulator PARSED the body
    Phantom sent and read the quote intact.

    Pre-fix: the second step's body is ``{"fileName": "He said "hi""}``, the
    emulator's ``json.loads`` raises, it answers ``400``, the sender terminates
    the row ``failed`` with ``last_error=4xx_status_400``, and the wait for
    ``succeeded`` times out.
    """
    stack: E2EStack = await boot_stack()
    try:
        chain_id = uuid4()
        async with httpx.AsyncClient() as client:
            resp = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=_two_step_envelope(emulator_url=stack.emulator_url, chain_id=chain_id),
                bearer=stack.fake_security_token(),
            )
        assert resp.status_code == SEND_ACCEPTED_STATUS, (
            f"the chain must be buffered with {SEND_ACCEPTED_STATUS}; "
            f"got {resp.status_code}: {resp.text!r}"
        )

        async def _succeeded() -> bool:
            snapshot = await stack.phantom_client.get_upload(chain_id)
            return snapshot.state == SUCCEEDED_STATE

        await await_until(
            _succeeded,
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
            message=(
                "the quote-bearing capture never reached the upstream as valid JSON; "
                "the row never reached 'succeeded'"
            ),
        )

        detail = await stack.phantom_client.get_upload(chain_id)
        captured_by_step = {cs.step_name: cs.values for cs in detail.captured}
        assert "resend_title" in captured_by_step, (
            f"the second step must have captured the upstream's echo; "
            f"got steps {list(captured_by_step)}"
        )
        assert captured_by_step["resend_title"]["echo"] == QUOTED_NAME, (
            "the upstream must have parsed Phantom's body and read the quote intact; "
            f"got {captured_by_step['resend_title']['echo']!r}"
        )
    finally:
        await stack.tear_down()
