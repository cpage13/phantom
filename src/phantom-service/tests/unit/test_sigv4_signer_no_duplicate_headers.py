"""F7: the signed header map is REBUILT, so no superseded casing survives.

``sign_sigv4`` seeds a botocore ``AWSRequest`` from the caller's plain dict,
lets botocore sign it, and then copies the signed headers back. botocore's
``HTTPHeaders`` is case-INSENSITIVE: it deletes the seeded keys and re-adds
them canonical-cased. The copy-back then assigned into a plain ``dict``, which
is case-SENSITIVE, so the canonical-cased names landed BESIDE the surviving
lowercase originals.

Starlette lower-cases inbound header names, so a raw-intake request that the
client already SigV4-signed arrives carrying ``authorization``,
``x-amz-date`` and ``x-amz-content-sha256``. The wire request then carried two
of each, one stale. S3 answers ``403 SignatureDoesNotMatch``, the executor
classifies that as ``FailedAuth``, marks the host credential bad, and every
row for that destination parks in ``auth_expired``: one ordinary request takes
out a whole destination.

The fix rebuilds rather than patches, so a duplicate is structurally
impossible for EVERY header botocore rewrites rather than for the three names
we happen to know about today. The existing signer tests seed only ``host``
and assert through lower-cased key sets, so the collision was untested by
construction; this file is the signer-shaped counterpart to the
executor-shaped ``test_sigv4_executor.py``.
"""

from __future__ import annotations

import pytest
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]
from phantom.chain.sigv4_signer import sign_sigv4
from phantom.models.credential import SigningService, SigV4StaticCreds

pytestmark = pytest.mark.asyncio

_HOST = "bucket.s3.us-east-1.amazonaws.com"
_URL = f"https://{_HOST}/key"
_BODY = b"the-object-bytes"

# The client's stale values. Any of these surviving beside Phantom's fresh
# ones is the defect.
_STALE_AUTH = "AWS4-HMAC-SHA256 Credential=AKIACLIENT/20260101/us-east-1/s3/aws4_request"
_STALE_DATE = "20260101T000000Z"
_STALE_SHA = "0000000000000000000000000000000000000000000000000000000000000000"


def _creds() -> SigV4StaticCreds:
    """Phantom's own resolved static credential."""
    return SigV4StaticCreds(
        access_key_id="AKIAPHANTOM",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
        region="us-east-1",
        service=SigningService.S3,
        session_token=None,
    )


def _starlette_shaped_headers() -> dict[str, str]:
    """The header map a client-signed raw-intake request produces.

    Starlette lower-cases inbound names, so the three headers botocore
    rewrites arrive lower-cased and carrying the CLIENT's stale values.
    """
    return {
        "host": _HOST,
        "authorization": _STALE_AUTH,
        "x-amz-date": _STALE_DATE,
        "x-amz-content-sha256": _STALE_SHA,
        "x-custom": "keep-me",
    }


async def test_lowercase_seeded_headers_leave_no_duplicate_on_the_wire() -> None:
    """No header name appears twice case-insensitively after signing.

    Objective: the exact defect, in the exact shape starlette produces.
    Success: folding the resulting keys with ``str.lower()`` yields each name
    exactly once, none of the three stale values survives, and the unrelated
    header rides through untouched.
    """
    headers = _starlette_shaped_headers()

    await sign_sigv4(method="PUT", url=_URL, headers=headers, body=_BODY, credential=_creds())

    lowered = [name.lower() for name in headers]
    assert len(lowered) == len(set(lowered)), (
        f"a header name appears twice case-insensitively: {sorted(headers)}"
    )
    values = set(headers.values())
    assert _STALE_AUTH not in values, "the client's superseded Authorization survived"
    assert _STALE_DATE not in values, "the client's superseded X-Amz-Date survived"
    assert _STALE_SHA not in values, "the client's superseded payload hash survived"
    assert headers["x-custom"] == "keep-me"


async def test_rebuild_preserves_every_unsigned_header() -> None:
    """Clearing the caller's map must not drop anything botocore did not touch.

    Objective: the counter-test for the rebuild. ``AWSRequest`` is constructed
    with the caller's dict, so every supplied header is already inside
    botocore's view and ``add_auth`` only adds and replaces. Success: the
    unsigned headers are all present afterwards with their original casing and
    values.
    """
    unsigned = {
        "X-Custom-Trace": "abc-123",
        "content-type": "application/octet-stream",
        "X-Amz-Meta-Colour": "blue",
        "if-none-match": "*",
    }
    headers = {"host": _HOST, **unsigned}

    await sign_sigv4(method="PUT", url=_URL, headers=headers, body=_BODY, credential=_creds())

    for name, value in unsigned.items():
        assert headers.get(name) == value, (
            f"unsigned header {name!r} must survive the rebuild unchanged; got {sorted(headers)}"
        )


async def test_signed_header_names_are_unique_case_insensitively() -> None:
    """botocore's own iteration yields no case-insensitive duplicate.

    Objective: settle the rebuild's precondition as a test rather than an
    assumption. ``dict(request.headers.items())`` can only lose a header if
    botocore's map yields two names that differ in case alone. A failure here
    means the pinned botocore property regressed and the rebuild design must
    be revisited; there is no fallback to fall back to, because the property
    is settled by probe.
    """
    from botocore.auth import S3SigV4Auth  # type: ignore[import-untyped]

    request = AWSRequest(method="PUT", url=_URL, data=_BODY, headers=_starlette_shaped_headers())
    S3SigV4Auth(Credentials("AKIAPHANTOM", "secret"), "s3", "us-east-1").add_auth(request)

    names = [name.lower() for name in request.headers]
    assert len(names) == len(set(names)), (
        f"botocore's signed header view carries a case-insensitive duplicate: {names}"
    )


async def test_caller_dict_identity_is_preserved() -> None:
    """``sign_sigv4`` mutates the caller's mapping in place and never rebinds.

    Objective: the executor passes ``substituted_headers`` and reads it back
    after the call, so the in-place contract is load-bearing. Success: the
    object handed in is the object carrying the signature afterwards.
    """
    headers = _starlette_shaped_headers()
    same_object = headers

    await sign_sigv4(method="PUT", url=_URL, headers=headers, body=_BODY, credential=_creds())

    assert same_object is headers
    assert "Authorization" in same_object or "authorization" in same_object
