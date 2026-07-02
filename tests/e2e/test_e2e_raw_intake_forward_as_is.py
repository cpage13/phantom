"""Phase-1 forward-as-is gate — stock PUT → catch-all → /raw sink round-trip.

The HARD Phase-1 gate (plan TASK 1.4(a)): a stock object-storage client that
knows NOTHING of Phantom's ``POST /v1/send`` chain-envelope contract speaks a
plain ``PUT /{bucket}/{key}`` with a raw body. Phantom's root-mounted
catch-all (TASK 1.1) accepts it, the raw→envelope adapter (TASK 1.2)
synthesizes a 1-step chain, destination resolution (TASK 1.3) rewrites the
step URL to the live upstream via ``phantom_default_target``, the chain is
buffered, and the executor forwards the request AS-IS — ``auth_mode: none``,
NO re-signing (the ``aws_sigv4`` signer is Phase 4). The upstream is the
auth-free, token-free ``/raw`` sink (TASK 0.5), the forward-as-is oracle.

The byte round-trip is the gate's teeth: the exact bytes a stock ``httpx.put``
sent must be readable byte-identical at ``GET {emulator}/raw/{bucket}/{key}``
once delivery completes. This is a REAL ``boot_stack`` round-trip against a
running upstream — there is NO ``TestClient``-stub fallback (plan review B1).

The negative legs that fence the happy path:

* No destination configured (no ``?phantom=`` carrier, no
  ``phantom_default_target``) → 421 ``invalid_target``, nothing stored
  upstream (rejected BEFORE any durable write — never a forward loop back to
  Phantom). (TASK 1.4(b))
* Reserved ``X-Phantom-*`` markers are routing INPUTS to the catch-all, not
  upstream headers: they MUST NOT reach the upstream, while a benign upstream
  header survives. Asserted directly off the sink's ``all_headers`` capture.
  (TASK 1.4(c))

The ``{EMULATOR_URL}`` substitution token (TASK 1.3a) is what makes the boot
fireable: ``phantom_default_target`` cannot be an f-string referencing
``stack.emulator_url`` (which does not exist until ``boot_stack`` returns and
``config_overrides`` is consumed INSIDE it), so the pre-boot overlay carries
the literal token and ``_build_phantom_settings`` rewrites it to the live
ephemeral emulator base URL at merge time.
"""

from __future__ import annotations

import httpx
import pytest

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# Phantom's buffering ack for an admitted raw intake. The catch-all returns
# whatever ``resolve_and_admit`` resolves; a healthy admission is 202 (the
# coarse "buffered, will deliver" hint — delivery to the upstream is async and
# is asserted separately via the /raw read-back).
INTAKE_ACCEPTED_STATUS: int = 202

# Phantom's no-destination rejection. A raw request that names no real
# upstream is refused BEFORE any durable write (the loop hazard is never
# forwarded), with the same canonical envelope dispatch raises for an
# unroutable host.
INVALID_TARGET_STATUS: int = 421

# Upper bound on the forwarded body landing in the /raw sink. The intake ack
# is synchronous; delivery rides the retry worker, so the read-back polls. A
# few seconds is ample on the in-process loopback stack and stays well under
# the suite's < 60 s budget.
DELIVERY_TIMEOUT_SECONDS: float = 10.0

# The forward-as-is payload. Random-ish bytes (including a NUL and high bytes)
# so the byte-identity assertion has teeth — a codec or text-coercion bug
# would corrupt these.
FORWARD_PAYLOAD: bytes = b"phantom-e2e-payload\x00\xff\xfe-byte-identity"

# The object path a stock client addresses. A slash-bearing key so the
# ``{path:path}`` capture (Phantom's catch-all AND the sink's) is exercised
# end-to-end, not just a flat bucket/key.
BUCKET: str = "mybucket"
KEY: str = "nested/object-key.bin"
OBJECT_PATH: str = f"{BUCKET}/{KEY}"


def _forward_as_is_overrides(*, default_target: str | None) -> dict[str, object]:
    """Build a ``config_overrides`` overlay for the forward-as-is path.

    Reproduces the suite's ``primary`` instance verbatim EXCEPT the route's
    ``auth_mode`` is ``none``: the base ``phantom-config.yml`` route is
    ``phantom_bearer``, which on the tokenless raw-intake path (``uid=""``, no
    inbound bearer for the emulator host) would resolve no cache slot and fail
    the forward 401. ``none`` makes the executor skip auth injection and
    forward the request verbatim — the Phase-1 forward-as-is contract. (The
    ``instances`` list is replaced wholesale: ``_deep_merge_dict`` is
    overlay-wins for lists.)

    Args:
        default_target: The ``phantom_default_target`` value (the
            ``"{EMULATOR_URL}/raw"`` token for the happy path, or ``None`` to
            exercise the no-destination 421 leg).

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

    The intake ack is synchronous but delivery is async (the retry worker
    forwards the buffered step), so the read-back must poll. Reads back via
    the sink's auth-free ``GET /raw/{path}`` — the plan's preferred byte
    oracle (PUT-in == GET-out proves the full buffer→forward path).

    Args:
        stack: The running stack (for ``emulator_url``).
        path: The full forwarded path the sink keys on (``bucket/key``).

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
async def test_raw_intake_forwards_as_is_to_raw_sink() -> None:
    """Stock PUT round-trips byte-identical through the forward-as-is path.

    The Phase-1 gate: ``httpx.put({phantom}/mybucket/...)`` → catch-all →
    adapter → buffer → forward-as-is (``auth_mode: none``) → emulator ``/raw``
    sink → 202 from Phantom + the body readable byte-identical at
    ``GET {emulator}/raw/mybucket/...``.
    """
    stack = await boot_stack(
        # The pre-boot overlay carries the LITERAL "{EMULATOR_URL}" token, NOT
        # an f-string on stack.emulator_url (which does not exist yet); TASK
        # 1.3a's _build_phantom_settings rewrites it to the live emulator URL.
        config_overrides=_forward_as_is_overrides(default_target="{EMULATOR_URL}/raw"),
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{OBJECT_PATH}",
                content=FORWARD_PAYLOAD,
            )

        # Intake ack — the raw PUT was admitted + buffered (not yet delivered).
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )
        # The catch-all echoes the minted chain id so a producer can poll it.
        assert resp.headers.get("X-Phantom-Upload-Id"), (
            "raw-intake ack must carry X-Phantom-Upload-Id (the minted chain id)"
        )

        # Delivery — the buffered step forwarded verbatim to the live sink.
        delivered = await _await_raw_delivery(stack, OBJECT_PATH)
        assert delivered == FORWARD_PAYLOAD, (
            "byte round-trip broke: bytes read back from the /raw sink differ "
            f"from the PUT body (sent {len(FORWARD_PAYLOAD)} bytes, "
            f"got {len(delivered)} bytes)"
        )

        # Same truth from the typed emulator state (no HTTP) — the sink stored
        # exactly one RawBody under the forwarded path with the exact bytes.
        raw = stack.emulator._server.state.raw_bodies.get(OBJECT_PATH)
        assert raw is not None, (
            f"no RawBody stored under {OBJECT_PATH!r}; "
            f"stored keys: {sorted(stack.emulator._server.state.raw_bodies)}"
        )
        assert raw.body == FORWARD_PAYLOAD, "RawBody.body is not byte-identical to the PUT body"
    finally:
        await stack.tear_down()


# The full forwarded upload-verb set (the catch-all forwards all three; the
# /raw sink now accepts all three too). The happy-path test above proves PUT in
# depth; this parametrized test proves each verb forwards as-is to the /raw sink
# and is recorded on the stored RawBody.
_UPLOAD_VERBS = ("PUT", "POST", "PATCH")


@pytest.mark.e2e
@pytest.mark.parametrize("method", _UPLOAD_VERBS)
async def test_raw_intake_forwards_each_verb_records_method(method: str) -> None:
    """Each upload verb forwards as-is to the /raw sink with ``.method`` recorded.

    A stock ``httpx.request(method, ...)`` hits the catch-all (which forwards
    PUT/POST/PATCH), is buffered, and forwarded verbatim (``auth_mode: none``)
    to the auth-free ``/raw`` sink, which now accepts all three verbs. The
    assertion is the method round-trip (``raw_body(path).method == method``,
    read via the typed accessor) on top of byte-identity — proof the verb
    survives intake → forward-as-is → sink for every forwarded verb.
    """
    # A per-verb path so the three parametrizations never alias in shared state.
    path = f"verbbucket/forward-{method.lower()}.bin"
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides(default_target="{EMULATOR_URL}/raw"),
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method, f"{stack.phantom_url}/{path}", content=FORWARD_PAYLOAD
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack for {method}, "
            f"got {resp.status_code}: {resp.text!r}"
        )

        delivered = await _await_raw_delivery(stack, path)
        assert delivered == FORWARD_PAYLOAD, (
            f"byte round-trip broke for {method}: bytes read back from the /raw sink "
            "differ from the request body"
        )

        # The typed accessor proves both the body and the recorded verb.
        raw = stack.emulator.raw_body(path)
        assert raw is not None, f"no RawBody stored under {path!r} for {method}"
        assert raw.body == FORWARD_PAYLOAD, "RawBody.body is not byte-identical to the request body"
        assert raw.method == method, (
            f"the stored RawBody must record the inbound verb; expected {method!r}, "
            f"got {raw.method!r}"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_raw_intake_no_destination_421_no_write() -> None:
    """No carrier and no default target → 421 invalid_target, nothing stored.

    With ``phantom_default_target`` unset and no ``?phantom=`` query carrier, a
    raw ``PUT /bucket/key`` names no real upstream. Phantom rejects 421
    ``invalid_target`` BEFORE reading the body or attempting any write — the
    loop hazard (forwarding back to Phantom) is never taken, and the upstream
    sink stays empty.
    """
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides(default_target=None),
    )
    try:
        path = "unrouted-bucket/unrouted-key"
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{path}",
                content=b"should-never-be-forwarded",
            )

        assert resp.status_code == INVALID_TARGET_STATUS, (
            f"expected {INVALID_TARGET_STATUS} invalid_target with no destination "
            f"configured, got {resp.status_code}: {resp.text!r}"
        )
        # Canonical error envelope: {"error": {"code": ..., "message": ...}}.
        body = resp.json()
        assert body["error"]["code"] == "invalid_target", (
            f"expected canonical 'invalid_target' error envelope, got {body!r}"
        )

        # No durable write upstream — the request never left Phantom. (Give the
        # retry worker no excuse: the sink store is consulted directly.)
        assert path not in stack.emulator._server.state.raw_bodies, (
            "an unroutable raw PUT must not reach the upstream sink"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_raw_intake_strips_x_phantom_markers_forwards_benign() -> None:
    """Reserved X-Phantom-* markers are not forwarded; a benign header survives.

    A raw ``PUT`` carrying ``X-Phantom-Uid`` and ``X-Phantom-Idempotency-Key``
    (Phantom routing INPUTS) plus a benign ``X-Custom`` upstream header. The
    forwarded request must carry the benign header but NONE of the
    ``x-phantom-*`` markers — asserted directly off the sink's ``all_headers``
    capture (lowercased keys). Both the adapter (``_forwarded_headers``) and
    the executor strip the reserved prefix; this gate proves the net effect at
    the upstream.
    """
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides(default_target="{EMULATOR_URL}/raw"),
    )
    try:
        path = "markerbucket/marker-key"
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{path}",
                content=FORWARD_PAYLOAD,
                headers={
                    "X-Phantom-Uid": "u-should-be-stripped",
                    "X-Phantom-Idempotency-Key": "k-should-be-stripped",
                    "X-Custom": "keep",
                },
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )

        await _await_raw_delivery(stack, path)
        raw = stack.emulator._server.state.raw_bodies.get(path)
        assert raw is not None, f"no RawBody stored under {path!r}"

        # all_headers keys are lowercased by the sink. No reserved marker may
        # have reached the upstream; the benign header must have.
        phantom_markers = [k for k in raw.all_headers if k.startswith("x-phantom-")]
        assert not phantom_markers, (
            f"reserved X-Phantom-* markers leaked to the upstream: {phantom_markers}"
        )
        assert raw.all_headers.get("x-custom") == "keep", (
            f"benign upstream header X-Custom must be forwarded; "
            f"got headers: {sorted(raw.all_headers)}"
        )
    finally:
        await stack.tear_down()
