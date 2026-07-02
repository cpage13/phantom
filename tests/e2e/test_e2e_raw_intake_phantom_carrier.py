"""Raw-intake destination resolution: the ``?phantom=<url>`` query carrier.

The documented contract (README "Use it in three moves", the
``phantom_default_target`` header in ``config/phantom.yaml.example``, and
``docs/operator-playbook.md``) is two-way destination resolution for a raw
path-style upload, first hit wins:

1. an explicit ``?phantom=<full-url>`` query carrier on the request, then
2. the configured ``phantom_default_target`` (which gets ``/{bucket}/{key}``
   appended).

The sibling file ``test_e2e_raw_intake_forward_as_is.py`` proves leg 2 (the
default target) and the neither-configured 421. This file proves leg 1, the
carrier, end to end against the real stack:

* the carrier ALONE names a real destination (no ``phantom_default_target``
  configured), and the body lands byte-identical at exactly the carrier URL;
* the carrier WINS over a configured ``phantom_default_target``: the body
  lands at the carrier destination and nothing lands on the default-target
  path derived from the request path.

Both tests ride the forward-as-is route (``auth_mode: none``) so the proof
isolates destination resolution, not auth. The carrier is a FULL url: the
resolver returns it verbatim and does NOT append the request path
(``routes/catch_all.py``, ``_resolve_destination``).
"""

from __future__ import annotations

import httpx
import pytest

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# Phantom's buffering ack for an admitted raw intake (delivery is async).
INTAKE_ACCEPTED_STATUS: int = 202

# Upper bound on the forwarded body landing in the /raw sink. Delivery rides
# the retry worker, so the read-back polls; ample on the loopback stack.
DELIVERY_TIMEOUT_SECONDS: float = 10.0

# Forward-as-is payload with a NUL and high bytes so byte-identity has teeth.
CARRIER_PAYLOAD: bytes = b"phantom-e2e-carrier\x00\xff\xfe-byte-identity"

# The path the stock client addresses on Phantom. Under carrier resolution
# this path is NOT the destination; the carrier url is.
REQUEST_PATH: str = "request-bucket/request-key.bin"

# The sink path the carrier names. Distinct from REQUEST_PATH so a hit here
# can only have come from the carrier.
CARRIER_PATH: str = "carrier-bucket/nested/carrier-key.bin"


def _carrier_overrides(*, default_target: str | None) -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the carrier tests.

    Reproduces the suite's ``primary`` instance with a single
    ``auth_mode: none`` route (the forward-as-is contract), exactly like the
    sibling ``test_e2e_raw_intake_forward_as_is.py`` overlay.

    Args:
        default_target: The ``phantom_default_target`` value. ``None`` leaves
            it unset (carrier-only resolution); the ``"{EMULATOR_URL}/raw"``
            token exercises carrier-beats-default.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    overrides: dict[str, object] = {
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
    }
    if default_target is not None:
        overrides["phantom_default_target"] = default_target
    return overrides


async def _await_raw_delivery(stack: E2EStack, path: str) -> bytes:
    """Poll the emulator's /raw sink until ``path`` is stored, return its bytes.

    Args:
        stack: The running stack (for ``emulator_url``).
        path: The sink path to poll (``bucket/key``, no ``raw/`` prefix).

    Returns:
        The bytes the sink stored under ``path``.
    """
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
        final = await client.get(read_url)
    return final.content


@pytest.mark.e2e
async def test_phantom_carrier_names_destination_without_default_target() -> None:
    """The ``?phantom=`` carrier alone routes the upload; no default target set.

    With ``phantom_default_target`` unset (the config that 421s a carrier-less
    request), a stock PUT carrying ``?phantom=<emulator>/raw/<carrier-path>``
    is admitted, buffered, and forwarded to exactly the carrier url. The body
    reads back byte-identical at the carrier path, and nothing is stored under
    the request path (the carrier is the whole destination; the request path
    is not appended).
    """
    stack = await boot_stack(
        config_overrides=_carrier_overrides(default_target=None),
    )
    try:
        carrier_url = f"{stack.emulator_url}/raw/{CARRIER_PATH}"
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{REQUEST_PATH}",
                params={"phantom": carrier_url},
                content=CARRIER_PAYLOAD,
            )

        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack for a carrier-addressed "
            f"PUT, got {resp.status_code}: {resp.text!r}"
        )

        delivered = await _await_raw_delivery(stack, CARRIER_PATH)
        assert delivered == CARRIER_PAYLOAD, (
            "byte round-trip broke: bytes read back from the carrier destination "
            f"differ from the PUT body (sent {len(CARRIER_PAYLOAD)} bytes, "
            f"got {len(delivered)} bytes)"
        )

        # The carrier is the FULL destination: nothing may appear under the
        # request path, which would mean the resolver appended it.
        assert REQUEST_PATH not in stack.emulator._server.state.raw_bodies, (
            "the request path must not be a sink key under carrier resolution; "
            f"stored keys: {sorted(stack.emulator._server.state.raw_bodies)}"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_phantom_carrier_wins_over_default_target() -> None:
    """The explicit carrier beats a configured ``phantom_default_target``.

    Both resolution inputs are present: ``phantom_default_target`` points at
    the /raw sink root (so default resolution would store under the request
    path), and the request carries ``?phantom=<emulator>/raw/<carrier-path>``.
    The documented rule is carrier-wins: the body must land at the carrier
    path, byte-identical, and the default-derived request path must stay
    empty.
    """
    stack = await boot_stack(
        config_overrides=_carrier_overrides(default_target="{EMULATOR_URL}/raw"),
    )
    try:
        carrier_url = f"{stack.emulator_url}/raw/{CARRIER_PATH}"
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{REQUEST_PATH}",
                params={"phantom": carrier_url},
                content=CARRIER_PAYLOAD,
            )

        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )

        delivered = await _await_raw_delivery(stack, CARRIER_PATH)
        assert delivered == CARRIER_PAYLOAD, (
            "carrier destination did not receive the byte-identical body"
        )

        # Default-target resolution would have stored under REQUEST_PATH.
        # Its absence is the carrier-wins proof.
        assert REQUEST_PATH not in stack.emulator._server.state.raw_bodies, (
            "the default target was taken despite an explicit ?phantom= carrier; "
            f"stored keys: {sorted(stack.emulator._server.state.raw_bodies)}"
        )
    finally:
        await stack.tear_down()
