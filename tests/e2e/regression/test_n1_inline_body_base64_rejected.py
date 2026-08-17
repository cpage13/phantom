"""N1 regression: a malformed inline base64 body is refused at ingress, not admitted.

``ChainBodyBytes.value_b64`` carried no validation beyond being a string, and
the executor decoded it with a bare ``base64.b64decode`` at send time. The
decoder raises ``binascii.Error`` (a ``ValueError`` subclass) for malformed
base64 and a bare ``ValueError`` for non-ASCII input, and the sender's worker
loop catches only ``sqlite3.OperationalError``. So a producer body of
``{"kind": "bytes", "value_b64": "A"}`` was admitted with a 202, then killed the
sender's task group on the first claim; startup recovery re-claimed the same row
first on every restart (``claim_due`` orders by ``next_attempt_at ASC``), so one
malformed payload permanently disabled the service and stranded the whole
buffered backlog.

N1 is two layers. Admission rejects the payload with a 422 ``envelope_invalid``
so no such row is ever durably admitted, and the executor classifies it as
``InlineBodyInvalid`` so rows admitted before the guard existed terminate as
``failed`` instead of killing the worker. This test covers the producer-facing
half over the wire.

**Both exception classes are parametrised**, because a guard that catches only
``binascii.Error`` passes on malformed base64 and reopens the crash loop on
non-ASCII input, which is the case a producer's own tooling reaches by emitting
a typographic character.

The envelope and the POST are built by hand rather than through the SDK, so the
assertions are about the wire contract rather than the SDK's error taxonomy.
"""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import ChainBodyRef, ChainEnvelope, ChainStep

from ..helpers.stack import E2EStack, boot_stack
from ..helpers.timing import await_until

pytestmark = pytest.mark.e2e

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# The HTTP status the ADR-017 error matrix maps ``envelope_invalid`` to.
ENVELOPE_INVALID_STATUS: int = 422

# The stable error code the parser raises. N1 adds no new code.
ENVELOPE_INVALID_CODE: str = "envelope_invalid"

# One case from each exception class the decoder can raise. "A" is one character
# more than a multiple of four, which raises binascii.Error; the second is
# non-ASCII, which raises a bare ValueError before base64 decoding is even
# attempted, because b64decode encodes a str to ASCII first.
MALFORMED_CASES: list[str] = ["A", "é"]

# The healthy follow-up chain: its body and the raw-sink path it lands on.
HEALTHY_BODY: bytes = b"phantom-n1-healthy-chain-after-the-refusal"
HEALTHY_PATH: str = "n1/healthy-after-refusal.bin"

# Window for the healthy chain to reach ``succeeded``.
SUCCEEDED_BUDGET_SECONDS: float = 15.0
SUCCEEDED_STATE: str = "succeeded"


def _malformed_envelope(*, emulator_url: str, chain_id: UUID, value_b64: str) -> dict[str, object]:
    """Build a one-step envelope whose inline body carries ``value_b64``.

    **The step URL points at the emulator on purpose.** Post-fix the parse runs
    inside ``_parse_and_resolve`` BEFORE ``resolve_and_admit``, so the 422 fires
    whatever the URL is. Pre-fix there is no parse rejection, so the request
    continues into ``resolve_and_admit``, which selects the instance from the
    first step's URL; a URL outside the instance's ``host_prefixes`` would
    return a dispatch error rather than the 202 that is this test's pre-fix
    signal.

    Args:
        emulator_url: The live emulator's base URL.
        chain_id: The chain's identity.
        value_b64: The producer-supplied base64 text under test.

    Returns:
        The envelope as a plain dict, so the test controls the exact wire bytes.
    """
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "inline_body_step",
                "method": "PUT",
                "url": f"{emulator_url.rstrip('/')}/raw/n1/never-admitted.bin",
                "headers": {},
                "body": {"kind": "bytes", "value_b64": value_b64},
                "capture": [],
                "idempotency_header": None,
            }
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


def _healthy_envelope(stack: E2EStack, chain_id: UUID) -> ChainEnvelope:
    """Build the well-formed single-step chain submitted after the refusal.

    Args:
        stack: The running stack, for the emulator's base URL.
        chain_id: The chain's identity.

    Returns:
        The one-step envelope, submitted through the SDK.
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


@pytest.mark.parametrize("value_b64", MALFORMED_CASES)
async def test_malformed_inline_base64_is_refused_at_ingress_and_the_service_survives(
    value_b64: str,
) -> None:
    """A malformed ``value_b64`` gets a 422 at ingress and wedges nothing.

    Objective: prove the producer-facing refusal for BOTH decoder failure
    classes, and prove the refusal left the service healthy.

    Success: the response is 422 with ``error.code == "envelope_invalid"``, the
    body carries no chain id (so nothing was durably admitted), and a
    well-formed chain submitted afterwards reaches ``succeeded`` with its bytes
    at the emulator.
    """
    stack = await boot_stack()
    try:
        chain_id = uuid4()
        async with httpx.AsyncClient() as client:
            resp = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=_malformed_envelope(
                    emulator_url=stack.emulator_url, chain_id=chain_id, value_b64=value_b64
                ),
                bearer=stack.fake_security_token(),
            )

        assert resp.status_code == ENVELOPE_INVALID_STATUS, (
            f"an undecodable value_b64 must be refused at ingress with "
            f"{ENVELOPE_INVALID_STATUS}; got {resp.status_code}: {resp.text!r}"
        )
        payload = resp.json()
        assert payload["error"]["code"] == ENVELOPE_INVALID_CODE, (
            f"the refusal must carry the stable {ENVELOPE_INVALID_CODE!r} code; got {payload!r}"
        )
        assert "chain_id" not in payload, (
            f"a refused envelope must not return a chain id; nothing was admitted. Got {payload!r}"
        )

        # The refusal left the service healthy: a well-formed chain still flows.
        healthy_id = uuid4()
        await stack.phantom_client.submit_chain(
            _healthy_envelope(stack, healthy_id),
            body_refs={"body": HEALTHY_BODY},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {stack.fake_security_token()}",
        )

        async def _succeeded() -> bool:
            snapshot = await stack.phantom_client.get_upload(healthy_id)
            return snapshot.state == SUCCEEDED_STATE

        await await_until(
            _succeeded,
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
            message=f"the healthy chain never reached {SUCCEEDED_STATE!r} after the refusal",
        )
        delivered = stack.emulator.raw_body(HEALTHY_PATH)
        assert delivered is not None and delivered.body == HEALTHY_BODY, (
            "the healthy chain's bytes must reach the emulator, proving the ingress refusal "
            "wedged nothing"
        )
    finally:
        await stack.tear_down()
