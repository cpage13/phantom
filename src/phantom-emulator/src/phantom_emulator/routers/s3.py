"""Path-style S3 sink that VALIDATES the inbound SigV4 signature.

A deliberately small, in-suite substitute for MinIO/LocalStack: an upload
verb (PUT/POST/PATCH) on ``/{bucket}/{key}`` that recomputes the AWS SigV4
signature over the inbound request and compares it against a known test
key-pair (200 + store the body on match, 403 ``SignatureDoesNotMatch`` on
mismatch), plus a symmetric SigV4-validated ``GET /{bucket}/{key}`` that
returns the stored bytes. The upload-verb set mirrors Phantom's catch-all
forwarded set (``UPLOAD_METHODS``) so a forwarded POST/PATCH validates and
stores via the SAME method-agnostic recompute as a PUT — only GET stays the
read-back. No real AWS, no full S3 (no multipart / list / ACL / tagging /
versioning / copy / bare-bucket CreateBucket).

The validator ENFORCES ``x-amz-content-sha256`` (upload verbs and GET): a
request without that signed header is rejected ``400 "Missing required
header for this request: x-amz-content-sha256"`` — exactly as real S3 does
— and the value is treated as the canonical payload hash. Phantom's
``aws_sigv4`` signer now signs S3 with ``S3SigV4Auth``, which emits + signs
that header, so both sides agree on the SIGNED header set.

The validator NEVER calls botocore's :meth:`SigV4Auth.add_auth`. That is a
*client* signing path: it stamps wall-clock-now over the inbound
``X-Amz-Date`` and (for the S3 subclass) recomputes the payload hash —
either of which 403s a perfectly valid inbound signature. Instead it
**recomputes from the request's own declared ``SignedHeaders``** via the
lower-level ``canonical_request`` -> ``string_to_sign`` -> ``signature``
methods (which only *read* ``context['timestamp']`` and the request
headers, never mutating them), pinning the timestamp from the inbound
``X-Amz-Date`` and honoring the inbound ``x-amz-content-sha256``.

Base :class:`SigV4Auth`'s lower-level methods (NOT ``S3SigV4Auth.add_auth``)
are used for the recompute ON PURPOSE: the base path reads the inbound
``x-amz-content-sha256`` header verbatim, whereas ``S3SigV4Auth.add_auth``
would ``del`` + recompute it and CLOBBER the pinned inbound value. So even
though Phantom now SIGNS with ``S3SigV4Auth`` (so the header is present and
in ``SignedHeaders``), the validator deliberately recomputes via base
lower-level methods to honor the pinned inbound value — resolving the prior
"if Phantom ever switches… the validator must too" note (the validator
need NOT switch its recompute; it must only enforce the header and key on it).

See ``.agent/lifecycle/emulator_signing_DESIGNER.md`` §4-§5 and
``plan_06_22.md`` Tasks 0.2-0.4.
"""

from __future__ import annotations

import hmac
import logging
import re
from datetime import UTC, datetime
from typing import Annotated

from botocore.auth import SigV4Auth  # type: ignore[import-untyped]  # botocore ships no py.typed
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]
from fastapi import APIRouter, Depends, HTTPException, Request, Response

from phantom_emulator.config import S3Cfg
from phantom_emulator.routers._deps import UPLOAD_METHODS, get_state
from phantom_emulator.state import EmulatorState, S3Object

logger = logging.getLogger(__name__)

router = APIRouter()

StateDep = Annotated[EmulatorState, Depends(get_state)]

# First-segment values reserved by the emulator's other routers. The
# bare ``/{bucket}/{key:path}`` template is the most-greedy route and is
# registered LAST so registration-order first-match keeps the literal
# routes winning; this set is belt-and-suspenders so even a mis-ordered
# registration can never let an S3 path swallow a control/upstream path.
# ``v2`` is included for symmetry with ``v1`` (``POST /v2/files`` is a live
# alias) and to future-proof a later PUT/GET under those prefixes.
RESERVED_BUCKETS: frozenset[str] = frozenset({"v1", "v2", "oauth", "control", ".well-known"})

# The SigV4 ``Authorization`` header AWS documents verbatim:
#   AWS4-HMAC-SHA256 Credential=<AKID>/<YYYYMMDD>/<region>/<service>/aws4_request,
#   SignedHeaders=<h1;h2;…>, Signature=<hex>
_AUTH_RE = re.compile(
    r"^AWS4-HMAC-SHA256 "
    r"Credential=(?P<akid>[^/]+)/(?P<date>\d{8})/"
    r"(?P<region>[^/]+)/(?P<service>[^/]+)/aws4_request, "
    r"SignedHeaders=(?P<signed>[^,]+), "
    r"Signature=(?P<sig>[0-9a-f]+)$"
)

# Shared 403 for any signature-validation failure — the emulator's
# existing rejection vocabulary (routers/upstream.py uses the same
# code+detail for upload-PUT rejections).
_SIG_MISMATCH = HTTPException(status_code=403, detail="SignatureDoesNotMatch")

# The 400 real S3 returns when ``x-amz-content-sha256`` is absent. The signed
# header is part of the canonical request, so it cannot be injected post-sign —
# only a signer that EMITS + SIGNS it (``S3SigV4Auth``) satisfies real S3. This
# is kept DISTINCT from the 403 ``SignatureDoesNotMatch`` so a caller can assert
# the SAME status + detail real S3 emits for a missing-header request.
_MISSING_CONTENT_SHA256 = HTTPException(
    status_code=400,
    detail="Missing required header for this request: x-amz-content-sha256",
)


def _guard_reserved_bucket(bucket: str) -> None:
    """Reject a first path segment reserved by another emulator router.

    Raises:
        HTTPException: ``404`` when ``bucket`` is one of
            :data:`RESERVED_BUCKETS`, so an S3 path can never shadow an
            emulator control/upstream path.
    """
    if bucket in RESERVED_BUCKETS:
        raise HTTPException(status_code=404, detail="NoSuchBucket")


def _verify_sigv4(request: Request, body: bytes, s3cfg: S3Cfg) -> None:
    """Recompute the SigV4 signature over EXACTLY the inbound request and compare.

    Recompute-from-declared-``SignedHeaders``: the AWSRequest carries only
    the declared signed-header subset (inbound values, including ``host``
    and any ``x-amz-content-sha256``), the URL is reconstructed from the
    inbound request line + ``Host`` (never from config), the timestamp is
    pinned from the inbound ``X-Amz-Date``, and the signature is rebuilt
    via the lower-level ``canonical_request`` / ``string_to_sign`` /
    ``signature`` methods. Never calls ``add_auth`` (module docstring).

    Args:
        request: The inbound FastAPI request.
        body: The already-read request body (the PUT body, or ``b""`` for
            a GET). Fed as ``data=`` so botocore's ``payload()`` fallback
            hashes the right bytes when a client omits
            ``x-amz-content-sha256`` from the signed set.
        s3cfg: The known test credentials to validate against.

    Raises:
        HTTPException: ``403 SignatureDoesNotMatch`` on any failure
            (missing/garbled ``Authorization``, wrong credential id,
            credential-scope date mismatch, a declared header absent from
            the request, declared-headers divergence, or a signature
            mismatch). Returns ``None`` on a faithful recompute match.
    """
    m = _AUTH_RE.match(request.headers.get("authorization", ""))
    if m is None:
        raise _SIG_MISMATCH
    if not hmac.compare_digest(m["akid"], s3cfg.access_key_id):
        raise _SIG_MISMATCH

    # ENFORCE the signed payload-hash header (PUT and GET), as real S3 does: a
    # request without ``x-amz-content-sha256`` is rejected 400 (NOT 403). The
    # recompute below keys on the inbound value, so the header must be present.
    if "x-amz-content-sha256" not in request.headers:
        raise _MISSING_CONTENT_SHA256

    amz_date = request.headers.get("x-amz-date")
    # Internal consistency (NOT a wall-clock freshness window): the
    # X-Amz-Date day must equal the credential-scope date so the recompute
    # uses a single coherent RequestDateTime.
    if amz_date is None or amz_date[0:8] != m["date"]:
        raise _SIG_MISMATCH

    # Build the AWSRequest over EXACTLY the declared signed-header subset
    # (inbound values). A declared header that is not actually present is a
    # malformed/forged Authorization -> clean 403 (not a 500), and also
    # trips the guard below.
    signed_names = m["signed"].split(";")
    try:
        headers = {n: request.headers[n] for n in signed_names}
    except KeyError:
        raise _SIG_MISMATCH from None

    # URL from the INBOUND request line + inbound Host (never from config):
    # scheme + host(:port) + raw path + raw query, exactly as received.
    url = str(request.url)
    aws_req = AWSRequest(method=request.method, url=url, data=body, headers=headers)

    creds = Credentials(s3cfg.access_key_id, s3cfg.secret_access_key)
    # service/region from the inbound credential SCOPE, so the request's
    # own scope drives the comparison.
    auth = SigV4Auth(creds, m["service"], m["region"])
    # Pin the timestamp from the inbound X-Amz-Date. Only add_auth ever
    # SETS context['timestamp']; the lower-level methods only READ it
    # (string_to_sign appends it; scope/signature slice [0:8]).
    aws_req.context["timestamp"] = amz_date

    # Faithfulness guard: botocore's reproduced SignedHeaders MUST equal the
    # declared list. If our headers dict is exactly the declared set this
    # holds; a stray/missing header turns into a clean 403 instead of a
    # silent false-negative.
    if auth.signed_headers(auth.headers_to_sign(aws_req)) != m["signed"]:
        raise _SIG_MISMATCH

    # Lower-level recompute (skips the mutating _modify_request_before_signing
    # and the header-writing _inject_signature_to_request). canonical_request
    # reads x-amz-content-sha256 from the inbound header when present, so
    # UNSIGNED-PAYLOAD and a real hex digest both round-trip; absent, it
    # hashes `data=body`.
    canonical = auth.canonical_request(aws_req)
    string_to_sign = auth.string_to_sign(aws_req, canonical)
    expected = auth.signature(string_to_sign, aws_req)
    if not hmac.compare_digest(expected, m["sig"]):
        raise _SIG_MISMATCH


@router.api_route("/{bucket}/{key:path}", methods=list(UPLOAD_METHODS))
async def put_object(bucket: str, key: str, request: Request, state: StateDep) -> Response:
    """Validate an upload verb (PUT/POST/PATCH) on ``/{bucket}/{key}`` and store.

    Method-agnostic: ``_verify_sigv4`` recomputes over ``request.method``, so
    a forwarded POST/PATCH validates via the SAME recompute as a PUT. Returns
    ``200`` (empty body) and stores the bytes on
    :attr:`EmulatorState.s3_objects` (recording the inbound verb in
    :attr:`S3Object.method`) when the SigV4 signature recomputes;
    ``403 SignatureDoesNotMatch`` on any mismatch; ``413`` when the body
    exceeds the configured cap (checked before the recompute so an oversized
    body is rejected without hashing); ``404`` for a reserved bucket.
    """
    _guard_reserved_bucket(bucket)
    body = await request.body()
    if len(body) > state.cfg.s3.body_max_bytes:
        raise HTTPException(status_code=413, detail="body exceeds upstream cap")
    _verify_sigv4(request, body, state.cfg.s3)
    all_headers = {k.lower(): v for k, v in request.headers.items()}
    state.s3_objects[(bucket, key)] = S3Object(
        bucket=bucket,
        key=key,
        method=request.method,
        body=body,
        content_type=request.headers.get("content-type"),
        all_headers=all_headers,
        stored_at=datetime.now(UTC),
    )
    return Response(status_code=200)


@router.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request, state: StateDep) -> Response:
    """Validate a path-style ``GetObject`` and return the stored bytes.

    SigV4-validated for symmetry with :func:`put_object` (the emulator's
    real GET is authenticated). Returns the stored bytes on a valid
    signature; ``403 SignatureDoesNotMatch`` on a bad signature — checked
    BEFORE the store lookup so a bad-signature GET never leaks existence;
    ``404 NoSuchKey`` if the object is absent; ``404`` for a reserved
    bucket.
    """
    _guard_reserved_bucket(bucket)
    _verify_sigv4(request, b"", state.cfg.s3)
    obj = state.s3_objects.get((bucket, key))
    if obj is None:
        raise HTTPException(status_code=404, detail="NoSuchKey")
    return Response(
        content=obj.body,
        media_type=obj.content_type or "application/octet-stream",
    )
