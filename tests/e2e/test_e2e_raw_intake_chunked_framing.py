"""F9 gate: a chunked raw upload is delivered instead of wedged.

uvicorn de-frames a chunked request body before the handler sees it, but it
still exposes the raw ``Transfer-Encoding`` header. The catch-all's strip set
held only ``host`` and ``content-length``, so ``Transfer-Encoding: chunked``
was persisted into the synthesized step. At egress httpx only ``setdefault``s
its own computed framing headers, so h11 gave the persisted value precedence
and emitted BOTH framing headers over a fixed-length body. The upstream
rejected the malformed request, and because the header was baked into the
persisted envelope, every retry reproduced it identically until the row died.
The operator saw a permanently undeliverable upload with no signal that
Phantom had corrupted the framing.

This is the whole failure mode end to end, including the retry loop: httpx
sends ``Transfer-Encoding: chunked`` when it is given a generator body, so
the request that reaches Phantom is genuinely chunked, exactly as a stock
client's streaming upload is.

Boot shape is taken from ``tests/e2e/test_e2e_raw_intake_forward_as_is.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# Phantom's buffering ack for an admitted raw intake (delivery is async).
INTAKE_ACCEPTED_STATUS: int = 202

# Upper bound on the forwarded body landing in the /raw sink.
DELIVERY_TIMEOUT_SECONDS: float = 10.0

# The payload, sent as a generator so httpx frames the request chunked.
CHUNKS: tuple[bytes, ...] = (b"phantom-chunked-", b"framing-payload-", b"three-parts")
CHUNKED_PAYLOAD: bytes = b"".join(CHUNKS)

OBJECT_PATH: str = "chunkedbucket/nested/streamed-object.bin"


def _forward_as_is_overrides(default_target: str) -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the forward-as-is path.

    Args:
        default_target: The ``phantom_default_target`` value, carrying the
            literal ``{EMULATOR_URL}`` token the settings builder rewrites.

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
        "phantom_default_target": default_target,
    }


async def _chunk_stream() -> AsyncIterator[bytes]:
    """Yield the payload in pieces, which makes httpx frame the request chunked."""
    for chunk in CHUNKS:
        yield chunk


async def _await_raw_delivery(stack: E2EStack, path: str) -> None:
    """Poll the emulator's /raw sink until ``path`` is stored."""
    read_url = f"{stack.emulator_url}/raw/{path}"
    async with httpx.AsyncClient() as client:

        async def _delivered() -> bool:
            resp = await client.get(read_url)
            return resp.status_code == 200

        await await_until(
            _delivered,
            timeout_seconds=DELIVERY_TIMEOUT_SECONDS,
            message=f"forwarded body never reached the /raw sink at {path!r}",
        )


@pytest.mark.e2e
async def test_chunked_raw_upload_is_delivered_and_not_wedged() -> None:
    """A chunked raw upload delivers, and no framing header reaches the upstream.

    Objective: prove the whole failure mode is gone, including the retry loop
    that used to reproduce the malformed framing on every attempt. Success:
    the row delivers (so the ``succeeded`` read-back does not time out), the
    emulator recorded the exact body bytes, and the recorded request carries
    no ``Transfer-Encoding`` header.
    """
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides("{EMULATOR_URL}/raw"),
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{OBJECT_PATH}",
                content=_chunk_stream(),
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )

        await _await_raw_delivery(stack, OBJECT_PATH)
        raw = stack.emulator.raw_body(OBJECT_PATH)
        assert raw is not None, f"no RawBody stored under {OBJECT_PATH!r}"
        assert raw.body == CHUNKED_PAYLOAD, (
            "byte round-trip broke: the sink's bytes differ from the streamed body"
        )
        # The sink records every inbound header with lowercased keys, so the
        # ABSENCE of the framing header is directly assertable.
        assert "transfer-encoding" not in raw.all_headers, (
            "a persisted Transfer-Encoding reached the upstream; the forwarded "
            f"headers were {sorted(raw.all_headers)}"
        )
        assert "expect" not in raw.all_headers
    finally:
        await stack.tear_down()
