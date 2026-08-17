"""Phase-2 KEYSTONE — stock PUT → catch-all → Phantom RE-SIGNS → emulator SigV4 sink.

The single most important test of the SigV4 feature: it ties Phase 0 (the
emulator's path-style ``PUT /{bucket}/{key}`` SigV4 sink that recomputes the
signature from the inbound request and compares — :mod:`phantom_emulator.routers.s3`),
Phase 1 (the root catch-all + raw→envelope adapter), and Phase 2 (the
``aws_sigv4`` executor signer arm, the host-keyed :class:`CredentialStore`, the
:class:`CredentialKicker`, and the admin cred-push) into one round-trip.

Unlike the Phase-1 forward-as-is gate (``auth_mode: none``, no re-signing,
auth-free ``/raw`` sink), here the route's ``auth_mode`` is ``aws_sigv4``: a
stock ``httpx.put`` that knows NOTHING of SigV4 hits Phantom's catch-all,
Phantom rehydrates the buffered body and RE-SIGNS it with the stored AWS
credentials, and the emulator's path-style sink VALIDATES that signature by
recompute-and-compare. The sink stores the body ONLY on a faithful match
(``200``) and 403s any mismatch, so a stored, byte-identical object is direct
proof the re-sign was valid — there is no stub anywhere on the path.

The SAME AWS-documentation example key-pair lives on BOTH sides
(:data:`ACCESS_KEY_ID` / :data:`SECRET_ACCESS_KEY`): the emulator's
:class:`phantom_emulator.config.S3Cfg` validates against it (its field
defaults), and the test pushes it through the admin cred-push so Phantom signs
with it. A wrong/absent secret produces a divergent signature → the sink 403s →
Phantom PARKS the row in ``auth_expired`` (recoverable, NOT terminal) exactly as
a bearer 401 parks, reusing the existing sender path.

Four legs:

* **KEYSTONE (happy path)** — correct creds pushed → re-signed PUT validates →
  ``succeeded`` + the body is readable byte-identical from the sink's typed
  store. The byte read-back uses the typed ``stack.emulator.s3_object`` accessor
  rather than a ``GET`` because the sink's ``GET`` ALSO demands a valid SigV4
  signature (it is not an auth-free read), so a plain ``httpx.get`` would 403.

* **NEGATIVE (wrong cred parks)** — a wrong secret → the sink 403s → the row
  parks in ``auth_expired`` (asserted as the ROW STATE, not a terminal
  ``failed``), and nothing is stored upstream.

* **THE LOOP** — a wrong secret parks the row → the admin RE-PUSHES the correct
  creds → the credential-store ``set`` wakes the :class:`CredentialKicker`,
  which re-queues the parked row → Phantom re-signs with the fresh creds → the
  sink validates → ``succeeded`` + byte-identical body. The closure is asserted
  as the ``auth_expired`` → ``succeeded`` transition GATED ON the re-push (the
  row is verified parked BEFORE the correct creds are pushed), so a
  mis-partitioned kicker guard or a broken re-sign strands the row in
  ``auth_expired`` and fails cleanly at the ``succeeded`` poll. (Phase 3's
  ``expired`` send-deadline is not built yet; the loop only needs the re-push to
  wake + succeed.)

* **CORRUPTED SIGNATURE (validator has teeth)** — the Phase-4-enforced negative,
  a DIRECT Phantom-free PUT: sign a ``PUT {emulator}/{bucket}/{key}`` client-side
  with the CORRECT example pair, flip ONE hex digit of the ``Signature=`` token
  on the wire, and send it. The sink recomputes over the inbound request, the
  compare fails, and it returns ``403 SignatureDoesNotMatch`` WITHOUT storing —
  proof the validator rejects a bad signature on its own, independent of the
  re-sign path. (Distinct from NEGATIVE, which tampers the CRED on the Phantom
  side and asserts the PARK; this tampers the SIGNATURE on the wire and asserts
  the ``403`` directly.)

The ``{EMULATOR_URL}`` substitution token (TASK 1.3a) carries the live
ephemeral emulator base URL into ``phantom_default_target`` at merge time —
here the BARE token (no ``/raw`` suffix) so the rewritten step URL hits the
path-style SigV4 sink. The credential lookup (executor) and the admin-push key
both normalize through the same ``_hostname`` helper, which strips the port, so
both key on the loopback host (``127.0.0.1``) BY CONSTRUCTION.
"""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse
from uuid import UUID

import httpx
import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# The AWS-documentation example SigV4 key-pair. The SAME pair on BOTH sides:
# the emulator's ``S3Cfg`` field defaults validate against it, and the test
# pushes it through the admin cred-push so Phantom re-signs with it. A correct
# round-trip therefore needs no secret coordination beyond "use these".
ACCESS_KEY_ID: str = "AKIAIOSFODNN7EXAMPLE"
SECRET_ACCESS_KEY: str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# A deliberately WRONG secret (a real, well-formed key — just NOT the one the
# emulator validates against). Phantom signs with it, the recompute diverges,
# and the sink 403s. The access-key-id stays correct so the failure is purely
# a signature mismatch (the sink's credential-id equality check passes first).
WRONG_SECRET_ACCESS_KEY: str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYWRONGKEYY00"

# The credential-scope region the AWS example pair documents; both the emulator
# (``S3Cfg.region`` default) and the pushed credential use it.
REGION: str = "us-east-1"

# Phantom's buffering ack for an admitted raw intake (the catch-all returns
# whatever ``resolve_and_admit`` resolves; a healthy admission is 202 — delivery
# rides the retry worker and is asserted separately via the row state + sink).
INTAKE_ACCEPTED_STATUS: int = 202

# The admin cred-push success status (``204 No Content`` — the secret is never
# echoed, ADR-004).
CRED_PUSH_STATUS: int = 204

# Window for a re-signed PUT to land ``succeeded`` (boot is warm; the retry
# worker's first interval is 0s — a couple of round-trips on the in-process
# loopback stack). Generous, still well under the suite's per-test budget.
SUCCEEDED_BUDGET_SECONDS: float = 15.0

# Window for the 403'd row to park in ``auth_expired`` after the first attempt.
AUTH_EXPIRED_BUDGET_SECONDS: float = 5.0

# Bytes for the byte-identity assertion. A NUL and high bytes give the
# round-trip teeth — a codec/text-coercion bug would corrupt these, and the
# SigV4 payload hash is computed over exactly these bytes.
PAYLOAD: bytes = b"phantom-sigv4-resign\x00\xff\xfe-byte-identity"

# A slash-bearing object key so the sink's ``{key:path}`` capture is exercised,
# not just a flat bucket/key.
BUCKET: str = "mybucket"
KEY: str = "nested/resigned-object.bin"
OBJECT_PATH: str = f"{BUCKET}/{KEY}"

# The emulator's S3 signing service + the credential-scope service segment the
# direct-PUT corrupted-sig leg signs under (matching ``S3Cfg.service``); the
# emulator recomputes against this same service from the inbound scope.
S3_SERVICE: str = "s3"

# The emulator's path-style SigV4 sink returns this on ANY signature-validation
# failure (its single ``_SIG_MISMATCH`` vocabulary — status + detail).
SIGNATURE_MISMATCH_STATUS: int = 403
SIGNATURE_MISMATCH_DETAIL: str = "SignatureDoesNotMatch"

# A distinct object key for the direct corrupted-sig leg so it can never alias a
# co-resident keystone object in the same in-process emulator state.
CORRUPT_BUCKET: str = "mybucket"
CORRUPT_KEY: str = "corrupted/direct-put.bin"
CORRUPT_OBJECT_PATH: str = f"{CORRUPT_BUCKET}/{CORRUPT_KEY}"


def _sigv4_overrides() -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the SigV4 re-sign path.

    Reproduces the suite's ``primary`` instance, with the route's ``auth_mode``
    set to ``aws_sigv4`` (the base ``phantom-config.yml`` route is
    ``phantom_bearer``) and ``phantom_default_target`` carrying the BARE
    ``{EMULATOR_URL}`` token (no ``/raw`` suffix) so the rewritten step URL
    addresses the emulator's path-style SigV4 sink. The route ``hosts`` cover
    the loopback the emulator binds at; ``resolve_route`` matches on the
    port-stripped hostname, so ``127.0.0.1`` matches the live
    ``http://127.0.0.1:PORT`` target.

    The credential store and the CredentialKicker are wired unconditionally at
    boot, so no extra flag is needed — the ``aws_sigv4`` route is enough to make
    the signer arm fire and the kicker observe the parked rows.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    return {
        "phantom_default_target": "{EMULATOR_URL}",
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
    }


def _emulator_host(stack: E2EStack) -> str:
    """Return the destination host the SigV4 signer keys credentials on.

    The executor looks up credentials under ``_hostname(full_url)`` (the
    port-stripped, lower-cased hostname of the rewritten step URL), and the
    admin push normalizes its ``{dest_host}`` segment through the SAME helper.
    Deriving the host from ``stack.emulator_url`` (e.g. ``http://127.0.0.1:PORT``
    → ``127.0.0.1``) yields exactly that lookup key, so the pushed slot is the
    slot the signer reads — no port/normalization skew.

    Args:
        stack: The running stack.

    Returns:
        The normalized destination hostname (e.g. ``127.0.0.1``).
    """
    host = urlparse(stack.emulator_url).hostname
    assert host is not None, f"emulator_url has no hostname: {stack.emulator_url!r}"
    return host


async def _push_credential(stack: E2EStack, *, secret_access_key: str) -> None:
    """Admin-push a static SigV4 credential for the emulator host.

    Provisions the credential through ``PUT /v1/admin/credentials/{dest_host}``
    (loopback, no auth, ADR-004) so the executor's signer arm has a slot to
    sign with. ``set`` freshens the slot AND fires the credential-store wake
    handler the :class:`CredentialKicker` registered — which is what makes a
    re-push wake every parked row on that host (the loop-closing seam).

    Args:
        stack: The running stack (for the admin URL + emulator host).
        secret_access_key: The secret to provision — the correct AWS example
            secret for the happy/loop legs, or a wrong one for the negative leg.
    """
    url = f"{stack.phantom_admin_url}/v1/admin/credentials/{_emulator_host(stack)}"
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            url,
            json={
                "kind": "sigv4_static",
                "access_key_id": ACCESS_KEY_ID,
                "secret_access_key": secret_access_key,
                "region": REGION,
                "service": "s3",
                "session_token": None,
            },
        )
    assert resp.status_code == CRED_PUSH_STATUS, (
        f"admin cred-push expected {CRED_PUSH_STATUS}, got {resp.status_code}: {resp.text!r}"
    )


async def _raw_put(
    stack: E2EStack,
    *,
    path: str,
    body: bytes,
    method: str = "PUT",
    headers: dict[str, str] | None = None,
) -> UUID:
    """Drive a stock upload through Phantom's catch-all, return the chain id.

    A producer that knows nothing of SigV4 (or of Phantom's
    ``POST /v1/send`` chain-envelope contract) speaks a plain upload verb
    (PUT/POST/PATCH — the catch-all's forwarded set) on ``/{bucket}/{key}``
    with a raw body. Phantom admits + buffers it (202) and echoes the minted
    chain id in ``X-Phantom-Upload-Id``; delivery (the re-sign + forward) is
    async and is asserted via the row state.

    Args:
        stack: The running stack (for the ingress URL).
        path: The object path the stock client addresses (``bucket/key``).
        body: The raw request body.
        method: The upload verb to send (``PUT`` default; the per-verb test
            also drives ``POST`` / ``PATCH``).
        headers: Optional extra request headers, for the F7 arm that seeds a
            client-supplied ``Authorization`` the way a client-signed upload
            does.

    Returns:
        The minted chain id (parsed from ``X-Phantom-Upload-Id``) — the handle
        the admin API polls for the row state.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method, f"{stack.phantom_url}/{path}", content=body, headers=headers
        )
    assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
        f"raw intake expected {INTAKE_ACCEPTED_STATUS} ack, got {resp.status_code}: {resp.text!r}"
    )
    upload_id = resp.headers.get("X-Phantom-Upload-Id")
    assert upload_id, "raw-intake ack must carry X-Phantom-Upload-Id (the minted chain id)"
    return UUID(upload_id)


async def _await_row_state(stack: E2EStack, chain_id: UUID, state: str, *, budget: float) -> None:
    """Poll Phantom's admin API until ``chain_id`` reaches a (possibly non-terminal) state.

    Used for the ``auth_expired`` park checkpoint via a direct ``get_upload``
    poll on the live row state.

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
async def test_sigv4_resign_round_trip_keystone() -> None:
    """KEYSTONE — stock PUT re-signed by Phantom, validated by the emulator's SigV4 sink.

    Correct AWS example creds are pushed; a stock ``httpx.put`` hits the
    catch-all; Phantom re-signs the buffered body with those creds; the
    emulator's path-style sink recomputes the signature, matches, returns 200,
    and stores the body. The row reaches ``succeeded`` and the stored bytes are
    byte-identical to what the stock client sent — proof the re-sign was valid
    end-to-end (the sink stores ONLY on a faithful signature match).
    """
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        await _push_credential(stack, secret_access_key=SECRET_ACCESS_KEY)

        chain_id = await _raw_put(stack, path=OBJECT_PATH, body=PAYLOAD)

        # The re-signed PUT was accepted (200) by the SigV4 sink and the row
        # reached terminal success.
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        # Byte round-trip off the sink's typed store — the SigV4 sink stored the
        # object ONLY because Phantom's re-signed signature recomputed and
        # matched. (Read via the typed accessor: the sink's GET also demands a
        # valid SigV4 signature, so a plain httpx.get would 403.)
        stored = stack.emulator.s3_object(BUCKET, KEY)
        assert stored is not None, (
            f"no S3 object stored under {OBJECT_PATH!r}; the re-signed PUT was not validated"
        )
        assert stored.body == PAYLOAD, (
            "byte round-trip broke: bytes stored at the SigV4 sink differ from the PUT body "
            f"(sent {len(PAYLOAD)} bytes, stored {len(stored.body)} bytes)"
        )

        # Phantom re-signed with ``S3SigV4Auth``, so the stored object carries
        # the SIGNED ``x-amz-content-sha256`` header equal to the real body hash
        # — the header real S3 requires and the e2e previously masked. This is
        # the in-architecture bug fix proven end to end.
        expected_sha256 = hashlib.sha256(PAYLOAD).hexdigest()
        assert stored.all_headers.get("x-amz-content-sha256") == expected_sha256, (
            "the re-signed PUT must carry x-amz-content-sha256 == the real body hash; "
            f"got {stored.all_headers.get('x-amz-content-sha256')!r}"
        )
    finally:
        await stack.tear_down()


# A throwaway SigV4 Authorization in exactly the shape a client-signed upload
# sends. Starlette lower-cases it to ``authorization`` on the way in, which is
# what used to collide with botocore's canonical-cased ``Authorization``.
_CLIENT_AUTHORIZATION = (
    "AWS4-HMAC-SHA256 "
    "Credential=AKIACLIENTTHROWAWAY/20260101/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, "
    "Signature=00000000000000000000000000000000000000000000000000000000deadbeef"
)


@pytest.mark.e2e
async def test_sigv4_resign_replaces_a_client_supplied_authorization() -> None:
    """F7: a client-signed upload leaves exactly ONE Authorization on the wire.

    Objective: this is the real-world trigger. A stock S3 client signs its own
    request, so the raw intake arrives carrying ``authorization``
    (starlette lower-cases inbound names). botocore's map is case-insensitive
    and re-adds the header canonical-cased, so a key-by-key copy-back left the
    client's stale line beside Phantom's fresh one and the wire carried two.
    S3 answers 403 SignatureDoesNotMatch, which Phantom classifies as a bad
    credential for the whole destination.

    Success: the row still reaches ``succeeded`` (the emulator's sink
    validates the signature it recomputes, so it stores only on a faithful
    single signature), and the stored object records exactly one
    ``authorization`` value, which is Phantom's re-signature rather than the
    client's. The sink joins multi-value headers with ``", "``, so a duplicate
    would be observable as one value carrying two AWS4-HMAC-SHA256
    credentials.
    """
    key = "nested/client-signed.bin"
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        await _push_credential(stack, secret_access_key=SECRET_ACCESS_KEY)

        chain_id = await _raw_put(
            stack,
            path=f"{BUCKET}/{key}",
            body=PAYLOAD,
            headers={"Authorization": _CLIENT_AUTHORIZATION},
        )

        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        stored = stack.emulator.s3_object(BUCKET, key)
        assert stored is not None, (
            f"no S3 object stored under {BUCKET}/{key!r}; the re-signed PUT was not validated"
        )
        authorization = stored.all_headers.get("authorization")
        assert authorization is not None, "the forwarded request carried no Authorization"
        assert authorization.count("AWS4-HMAC-SHA256") == 1, (
            "exactly one Authorization must reach the upstream; the sink joins duplicates "
            f"with ', ', and it recorded {authorization!r}"
        )
        assert "AKIACLIENTTHROWAWAY" not in authorization, (
            "the client's superseded signature reached the upstream"
        )
        assert ACCESS_KEY_ID in authorization, (
            f"the forwarded Authorization must be Phantom's re-signature; got {authorization!r}"
        )
    finally:
        await stack.tear_down()


# The full forwarded upload-verb set (the catch-all forwards all three). The
# keystone above proves PUT in depth (incl. the x-amz-content-sha256 header and
# the park/loop legs); this parametrized test proves the verb itself round-trips
# through the re-sign path to the SigV4 sink and is recorded on the stored
# object — closing the POST/PATCH coverage on the aws_sigv4 arm.
_UPLOAD_VERBS = ("PUT", "POST", "PATCH")


@pytest.mark.e2e
@pytest.mark.parametrize("method", _UPLOAD_VERBS)
async def test_sigv4_resign_per_verb_round_trip(method: str) -> None:
    """Each upload verb (PUT/POST/PATCH) re-signs and lands with ``.method`` recorded.

    A stock ``httpx.request(method, ...)`` hits the catch-all (which forwards
    all three verbs); Phantom re-signs the buffered body; the emulator's SigV4
    sink validates via the SAME method-agnostic recompute, stores the body, and
    records the inbound verb in ``S3Object.method``. The assertion is the
    method round-trip (``stored.method == method``) on top of byte-identity —
    proof the verb survives intake → re-sign → sink for every forwarded verb,
    not just PUT.
    """
    # A per-verb key so the three parametrizations never alias in shared state.
    key = f"nested/resigned-{method.lower()}.bin"
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        await _push_credential(stack, secret_access_key=SECRET_ACCESS_KEY)

        chain_id = await _raw_put(stack, path=f"{BUCKET}/{key}", body=PAYLOAD, method=method)

        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        stored = stack.emulator.s3_object(BUCKET, key)
        assert stored is not None, (
            f"no S3 object stored under {BUCKET}/{key!r}; the re-signed {method} was not validated"
        )
        assert stored.body == PAYLOAD, (
            f"byte round-trip broke for {method}: stored bytes differ from the request body"
        )
        assert stored.method == method, (
            f"the stored object must record the inbound verb; expected {method!r}, "
            f"got {stored.method!r}"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_sigv4_wrong_credential_parks_auth_expired() -> None:
    """NEGATIVE — a wrong secret 403s at the sink and PARKS the row in ``auth_expired``.

    A well-formed but WRONG secret is pushed; the re-signed signature diverges;
    the emulator's sink 403s ``SignatureDoesNotMatch``; Phantom marks the cred
    slot bad and parks the row in ``auth_expired`` (a recoverable, NON-terminal
    state — re-queueable on a credential re-push), reusing the existing sender
    path. Nothing is stored upstream.
    """
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        await _push_credential(stack, secret_access_key=WRONG_SECRET_ACCESS_KEY)

        chain_id = await _raw_put(stack, path=OBJECT_PATH, body=PAYLOAD)

        # The wrong-cred forward 403s → the row parks in auth_expired (NOT a
        # terminal ``failed``: it waits for a credential re-push).
        await _await_row_state(stack, chain_id, "auth_expired", budget=AUTH_EXPIRED_BUDGET_SECONDS)
        parked = await stack.phantom_client.get_upload(chain_id)
        assert parked.state == "auth_expired", (
            f"wrong-cred row must PARK in auth_expired, not terminate; got {parked.state!r}"
        )

        # The body never landed upstream — the signature mismatch was rejected.
        assert stack.emulator.s3_object(BUCKET, KEY) is None, (
            "a 403'd (wrong-signature) PUT must not store an object at the sink"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_sigv4_refresh_loop_wrong_then_correct_credential() -> None:
    """THE LOOP — wrong cred parks → admin re-pushes correct cred → kicker wakes → succeeds.

    The full closed loop: a wrong secret parks the row in ``auth_expired``; the
    admin RE-PUSHES the correct secret; the credential-store ``set`` wakes the
    CredentialKicker, which re-queues the parked row; Phantom re-signs with the
    fresh creds; the sink validates and stores; the row reaches ``succeeded``
    with a byte-identical body.

    The proof of closure is the ``auth_expired`` → ``succeeded`` transition
    GATED ON the re-push: the row is verified parked in ``auth_expired`` BEFORE
    the correct credential is pushed, and only a CredentialKicker wake plus a
    successful re-sign on the fresh creds can drive a parked row to
    ``succeeded``. So ``succeeded`` after the re-push is itself proof the kicker
    fired and partitioned the ``aws_sigv4`` row correctly — a mis-partitioned
    guard would strand the row in ``auth_expired`` and fail this leg cleanly at
    the ``succeeded`` poll. (The intermediate ``queued`` state is deliberately
    NOT asserted: with the retry's 0s first interval the woken row races
    ``queued`` → ``attempting`` → ``succeeded`` faster than a poll can sample,
    so asserting it is an inherent race — the canonical bearer-recovery e2e
    skips it for the same reason.)
    """
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        # Stale (wrong) credential provisioned first.
        await _push_credential(stack, secret_access_key=WRONG_SECRET_ACCESS_KEY)

        chain_id = await _raw_put(stack, path=OBJECT_PATH, body=PAYLOAD)
        await _await_row_state(stack, chain_id, "auth_expired", budget=AUTH_EXPIRED_BUDGET_SECONDS)
        # Confirm the row is genuinely PARKED before the re-push, so the later
        # ``succeeded`` can only come from the re-push waking it (not a stale
        # in-flight attempt).
        parked = await stack.phantom_client.get_upload(chain_id)
        assert parked.state == "auth_expired", (
            f"loop precondition: row must be parked in auth_expired before the "
            f"re-push; got {parked.state!r}"
        )

        # Operator re-pushes the CORRECT credential. ``set`` freshens the slot
        # and fires the store's wake handler → the CredentialKicker re-queues
        # the parked row on this host.
        await _push_credential(stack, secret_access_key=SECRET_ACCESS_KEY)

        # The woken, re-signed retry validated and delivered. Reaching
        # ``succeeded`` from ``auth_expired`` is proof the kicker fired AND the
        # re-sign on the fresh creds was accepted by the SigV4 sink.
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

        # The recovered attempt stored the byte-identical body at the sink.
        stored = stack.emulator.s3_object(BUCKET, KEY)
        assert stored is not None, (
            f"no S3 object stored under {OBJECT_PATH!r} after the credential re-push + wake"
        )
        assert stored.body == PAYLOAD, (
            "byte round-trip broke after the refresh loop: bytes stored at the SigV4 sink "
            f"differ from the PUT body (sent {len(PAYLOAD)} bytes, stored {len(stored.body)} bytes)"
        )
    finally:
        await stack.tear_down()


def _corrupt_signature_hex(authorization: str) -> str:
    """Flip ONE hex digit of the ``Signature=`` token in a SigV4 ``Authorization``.

    The emulator's path-style sink parses the header with a regex whose signature
    group is ``[0-9a-f]+`` and then compares the recomputed signature against the
    declared one. Mutating a single hex digit to a DIFFERENT hex digit keeps the
    token well-formed — the header still PARSES, the credential id still matches,
    and the credential-scope date still aligns — so the rejection is purely the
    final signature-compare ``403`` (the validator-has-teeth path), not a
    malformed-header ``403``. This is exactly the "deliberately-corrupted signer"
    negative: an otherwise-correct request whose signature no longer matches its
    body.

    Args:
        authorization: A complete, validly-signed SigV4 ``Authorization`` header
            value (``AWS4-HMAC-SHA256 Credential=…, SignedHeaders=…,
            Signature=<hex>``).

    Returns:
        The same header with the last hex digit of ``Signature=`` flipped to a
        different hex digit, leaving every other field byte-identical.
    """
    marker = "Signature="
    idx = authorization.rindex(marker) + len(marker)
    head, sig = authorization[:idx], authorization[idx:]
    assert sig, "Authorization header carried an empty Signature= token"
    last = sig[-1]
    # Flip to a DIFFERENT hex digit so the token stays in ``[0-9a-f]+`` (the
    # emulator regex still matches) but the signature value changes.
    flipped = "0" if last != "0" else "1"
    assert flipped != last
    return head + sig[:-1] + flipped


@pytest.mark.e2e
async def test_sigv4_corrupted_signature_direct_put_rejected_403() -> None:
    """CORRUPTED SIGNATURE → 403 — a direct, Phantom-free PUT proves the sink has teeth.

    The enforced "validator has teeth" negative (no Phantom in the loop): sign a
    ``PUT {emulator}/{bucket}/{key}`` CLIENT-side with botocore's ``add_auth``
    using the CORRECT ``S3Cfg`` example pair (so the request is otherwise valid),
    then flip ONE hex digit of the ``Signature=`` token on the wire and send it
    with a plain ``httpx`` client. The emulator's path-style sink recomputes the
    signature over exactly the inbound request, the compare fails, and it returns
    ``403 SignatureDoesNotMatch`` WITHOUT storing the object.

    This is distinct from the wrong-cred park leg: that one tampers the CRED on
    the Phantom side and asserts the row PARKS in ``auth_expired``; this one
    tampers the SIGNATURE on the wire against the sink directly and asserts the
    ``403`` (and the absent object) with no Phantom involvement.
    """
    stack = await boot_stack(config_overrides=_sigv4_overrides())
    try:
        url = f"{stack.emulator_url}/{CORRUPT_OBJECT_PATH}"

        # Sign CLIENT-side with the CORRECT example pair via S3SigV4Auth — the
        # same signer Phantom now uses (and which emits + signs
        # x-amz-content-sha256, the header the emulator enforces), so the
        # UNCORRUPTED request would be a faithful 200. (Base SigV4Auth would omit
        # that header and 400 on the missing header BEFORE the signature check,
        # changing what this leg proves.)
        creds = Credentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
        aws_req = AWSRequest(method="PUT", url=url, data=PAYLOAD)
        S3SigV4Auth(creds, S3_SERVICE, REGION).add_auth(aws_req)
        signed_headers = dict(aws_req.prepare().headers)

        # Tamper exactly one hex digit of the signature so the request is now
        # internally inconsistent (correct akid/scope, wrong signature).
        signed_headers["Authorization"] = _corrupt_signature_hex(signed_headers["Authorization"])

        async with httpx.AsyncClient() as client:
            resp = await client.put(url, headers=signed_headers, content=PAYLOAD)

        assert resp.status_code == SIGNATURE_MISMATCH_STATUS, (
            f"a corrupted SigV4 signature must be rejected with "
            f"{SIGNATURE_MISMATCH_STATUS}; got {resp.status_code}: {resp.text!r}"
        )
        assert resp.json().get("detail") == SIGNATURE_MISMATCH_DETAIL, (
            f"the sink's signature-mismatch rejection must carry "
            f"detail={SIGNATURE_MISMATCH_DETAIL!r}; got {resp.text!r}"
        )

        # A 403'd (corrupted-signature) PUT must NOT store an object — the sink
        # validates BEFORE it stores, so a rejected signature leaves no trace.
        assert stack.emulator.s3_object(CORRUPT_BUCKET, CORRUPT_KEY) is None, (
            "a 403'd (corrupted-signature) direct PUT must not store an object at the sink"
        )
    finally:
        await stack.tear_down()
