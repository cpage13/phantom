"""F11 via D3: a raw SigV4 intake creates no bearer token slot.

Raw intake passes the client's ``Authorization`` header into the shared
admission prelude, which wrote it into the BEARER token cache with no
``auth_mode`` gate. On an ``aws_sigv4`` route that value is a per-request AWS
SigV4 credential string: useless as a bearer, and harmful in the cache. It
overwrites any real bearer for ``(host, uid)``, it creates or revives a
``fresh`` slot carrying garbage in the operator's own token view, and it fires
the token cache's wake handlers on every raw PUT so both kickers rescan.

The observable this test uses is the admin token surface, deliberately. The
churn story cannot be built: the bearer kicker skips every row whose resolved
route is not ``phantom_bearer``, so on an ``aws_sigv4`` host no row wakes and
a churn assertion is vacuous pre-fix; and if the raw intake targeted a parked
row's own host, that host is a ``phantom_bearer`` route, where D3 says the
cache write STILL happens and the assertions would fail post-fix. The
admin-tokens observable has no such problem: the write happens pre-fix and
does not post-fix, on the same route, with no kicker involved.

ADR-004 keeps bearer VALUES out of admin responses, so the observable is the
slot's existence and status, which is exactly what an operator sees.
"""

from __future__ import annotations

import httpx
import pytest

from ..helpers.stack import E2EStack, boot_stack

# Phantom's buffering ack for an admitted raw intake.
INTAKE_ACCEPTED_STATUS: int = 202

# A per-request AWS SigV4 credential string, in the shape a stock S3 client
# signs with. This is the value that used to land in the bearer cache.
CLIENT_SIGV4_AUTHORIZATION: str = (
    "AWS4-HMAC-SHA256 Credential=AKIATHROWAWAY/20260817/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, Signature=00000000deadbeef"
)

OBJECT_PATH: str = "poisonbucket/object-key.bin"
PAYLOAD: bytes = b"phantom-f11-payload"


def _sigv4_route_overrides() -> dict[str, object]:
    """Build a ``config_overrides`` overlay whose only route is ``aws_sigv4``.

    The emulator host is covered by an ``aws_sigv4`` route, which is the
    configuration in which the ungated write did its damage: the value cached
    there can never be read back, because that route re-signs from the
    host-keyed credential store.

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
                        "auth_mode": "aws_sigv4",
                    },
                ],
            },
        ],
        "phantom_default_target": "{EMULATOR_URL}/raw",
    }


@pytest.mark.e2e
async def test_raw_sigv4_intake_creates_no_token_slot() -> None:
    """A raw PUT carrying a SigV4 Authorization leaves the token surface empty.

    Objective: prove the poisoning is gone on the only observable that
    survives the kicker's own ``auth_mode`` guard, the admin-visible cache
    state. Success: ``GET /v1/admin/tokens`` shows no slot for the emulator
    host, and the raw PUT is still admitted with a 202 (the gate changes what
    is cached, never whether the upload is accepted).
    """
    stack: E2EStack = await boot_stack(config_overrides=_sigv4_route_overrides())
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{OBJECT_PATH}",
                content=PAYLOAD,
                headers={"Authorization": CLIENT_SIGV4_AUTHORIZATION},
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"the gate must not change admission; expected {INTAKE_ACCEPTED_STATUS}, "
            f"got {resp.status_code}: {resp.text!r}"
        )

        slots = await stack.phantom_client.list_tokens()
        assert slots == [], (
            "a raw intake on an aws_sigv4 route must create no bearer slot; "
            f"the admin token surface shows {[(s.endpoint, s.uid, s.status) for s in slots]}"
        )
    finally:
        await stack.tear_down()
