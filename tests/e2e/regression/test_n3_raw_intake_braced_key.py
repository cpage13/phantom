"""N3: a raw-intake object key containing braces is delivered, not failed.

Object keys are arbitrary bytes, and the catch-all deliberately puts the
bucket and key ONLY into the synthesized step's URL, because ``ChainStep.name``
and ``ChainBodyRef.name`` are regex-constrained. The executor then treated that
URL as a template: ``substitute`` found the ``{{...}}`` span, found no capture
behind it, and the row terminated ``failed`` with a ``template_unresolved:``
``last_error``. A valid upload was destroyed by its own key, the body was
retained but undeliverable, and an operator replay would have failed
identically.

A producer-supplied envelope with a braced key is rejected 422 at admission by
the parser's static placeholder pass, so this only ever bit raw intake, which
never runs that pass.

This is the operator-visible outcome, end to end: the upload delivers and the
object lands under the key the client asked for.
"""

from __future__ import annotations

from uuid import UUID

import httpx
import pytest

from ..helpers.assertions import assert_chain_reaches_state
from ..helpers.stack import E2EStack, boot_stack

# Phantom's buffering ack for an admitted raw intake.
INTAKE_ACCEPTED_STATUS: int = 202

# Budget for the row to reach terminal success through the retry worker.
SUCCEEDED_BUDGET_SECONDS: float = 15.0

# The object key a stock client is free to use. The braces are CONTENT.
BRACED_PATH: str = "bracedbucket/a{{b.c}}d.bin"

PAYLOAD: bytes = b"phantom-n3-braced-key-payload"


def _forward_as_is_overrides() -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the forward-as-is path.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``,
        with the emulator's auth-free ``/raw`` sink as the default target.
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
        "phantom_default_target": "{EMULATOR_URL}/raw",
    }


@pytest.mark.e2e
async def test_object_key_containing_braces_is_delivered_not_failed() -> None:
    """A braced object key reaches the upstream and the row succeeds.

    Objective: the operator-visible outcome, which is the whole point of the
    marker. Success: the row reaches ``succeeded``, the emulator recorded the
    object under a key whose braces survived, and the body bytes match.

    The key's braces survive SEMANTICALLY rather than byte-for-byte on the
    wire: uvicorn percent-decodes the request path at ingress and httpx
    re-encodes it at egress, so the sink sees the decoded key. That
    round-trip is pre-existing and out of N3's scope; what N3 fixes is the
    upload being destroyed rather than forwarded.
    """
    stack: E2EStack = await boot_stack(config_overrides=_forward_as_is_overrides())
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(f"{stack.phantom_url}/{BRACED_PATH}", content=PAYLOAD)
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )
        chain_id = UUID(resp.headers["X-Phantom-Upload-Id"])

        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded", (
            "a braced object key must be delivered; before N3 the row terminated "
            f"failed with last_error={detail.last_error!r}"
        )

        stored = stack.emulator.raw_body(BRACED_PATH)
        assert stored is not None, (
            f"no RawBody stored under {BRACED_PATH!r}; stored keys: "
            f"{sorted(stack.emulator._server.state.raw_bodies)}"
        )
        assert "{{b.c}}" in stored.path, (
            f"the braces must survive to the upstream key; got {stored.path!r}"
        )
        assert stored.body == PAYLOAD, "the forwarded body must be byte-identical"
    finally:
        await stack.tear_down()
