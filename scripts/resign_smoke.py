#!/usr/bin/env python
"""OWNER-RUN, real-AWS SigV4 re-sign smoke. NOT CI. Off by default.

Plan § 6 / Phase 4 TASK 4.5 (the tier-3 "one-and-only check against the true
service"; `routing_design_session_state_06_22.md:70-75`, `:103`, SSO detail
`:59`). This is the optional real-service confirmation — the load-bearing
in-CI gate is the in-process emulator round-trip
(`tests/e2e/test_e2e_sigv4_resign_round_trip.py`), which a real SigV4 validator
already proves. This script confirms the SAME signing assumption on the
owner's LIVE account.

What it does (the ~25-line spec, verbatim from the design):
  - re-signs a PUT with ``botocore.auth.S3SigV4Auth`` (the SAME signer Phantom's
    ``aws_sigv4`` arm now dispatches for S3 — it EMITS + SIGNS the
    ``x-amz-content-sha256`` header real S3 requires) + the owner's SSO PROFILE
    credentials (your configured AWS SSO profile via ``AWS_PROFILE`` / the default
    profile, region ``us-east-1``,
    read from the botocore credential chain — botocore auto-refreshes the
    temporary SSO creds; NO secret is stored in this file);
  - sends the re-signed request with PLAIN ``urllib3`` (NOT an AWS SDK) to
    real S3 — the load-bearing point: ``urllib3`` puts the EXACT re-signed
    bytes on the wire, so boto3 cannot re-sign and mask a signing bug. This
    mirrors Phantom's forward path, which uses ``httpx``, not an AWS SDK;
  - asserts HTTP 200 + a BYTE-IDENTICAL round-trip (PUT the bytes, GET them
    back, assert equal);
  - makes and CLEANS UP a throwaway bucket (unique name; the object + bucket
    are deleted at the end, leaving no residue on the account).

OWNER-RUN gating (why this is never in CI):
  - it lives under ``scripts/`` (NOT ``tests/``) and is not a ``test_*`` file,
    so pytest NEVER collects it — no CI job invokes it
    (``.github/workflows/per_pr.yml`` runs only ``src/ tests/`` for lint /
    format / types and ``tests/`` for pytest);
  - it requires an ACTIVE SSO session and real S3 access, which CI does not
    have.

Run it (the owner, anytime — log in to your SSO profile and export it first):

    aws sso login --profile <your-sso-profile>
    export AWS_PROFILE=<your-sso-profile>
    uv run --no-project --with boto3 --with urllib3 python scripts/resign_smoke.py

``--no-project`` so it does NOT pull the workspace; ``--with boto3 --with
urllib3`` provide the two ephemeral deps (boto3 carries botocore, the only
import used here for ``Session``/``SigV4Auth``/the S3 bucket lifecycle;
``urllib3`` is the transport). A ``PASS`` (exit 0) confirms every assumption
on the owner's real account; any failure exits non-zero and is loud.

Deviation from the design note (recorded): the throwaway-bucket lifecycle
(create / delete-object / delete-bucket) uses botocore's own
``session.create_client("s3", …)`` rather than the ``boto3.client`` wrapper.
``boto3.client`` is a thin shim over exactly this botocore call, so the
behaviour is identical, the ``--with boto3`` dependency still satisfies the
import (boto3 pulls botocore), and the script stays type-checkable where only
botocore is installed. The load-bearing PUT/GET still goes over plain
``urllib3``; botocore signs but never transports the round-trip request.
"""

from __future__ import annotations

import sys
import uuid

import urllib3
from botocore.auth import S3SigV4Auth  # type: ignore[import-untyped]
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.session import Session  # type: ignore[import-untyped]

_REGION = "us-east-1"
_SERVICE = "s3"
_OK = 200
_KEY = "resign-smoke-object"
_BODY = b"phantom resign smoke: byte-identical round-trip probe\n"


def _signed_headers(session: Session, *, method: str, url: str, body: bytes) -> dict[str, str]:
    """Re-sign ``method url`` over ``body`` with SigV4 + the SSO profile creds.

    Builds a host-bound :class:`AWSRequest`, signs it in place with
    :class:`S3SigV4Auth` — the SAME signer Phantom's ``aws_sigv4`` arm now
    dispatches for S3 — which EMITS + SIGNS ``x-amz-content-sha256`` over
    ``body`` (the signed header real S3 requires; base ``SigV4Auth`` omits it and
    real S3 then 400s ``Missing required header for this request:
    x-amz-content-sha256``) and adds ``Authorization`` / ``X-Amz-Date``. Returns
    the prepared header set so plain urllib3 can replay the EXACT re-signed
    request.
    """
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError(
            "no AWS credentials resolved — run `aws sso login --profile <your-profile>` "
            "and `export AWS_PROFILE=<your-profile>` (or set a default profile) first"
        )
    request = AWSRequest(method=method, url=url, data=body)
    S3SigV4Auth(credentials, _SERVICE, _REGION).add_auth(request)
    prepared = request.prepare()
    return {str(name): str(value) for name, value in prepared.headers.items()}


def main() -> int:
    """Run the real-AWS re-sign round-trip; return 0 on PASS, 1 on FAIL."""
    session = Session()
    s3 = session.create_client(_SERVICE, region_name=_REGION)
    http = urllib3.PoolManager()
    bucket = f"phantom-resign-smoke-{uuid.uuid4().hex}"
    base = f"https://{bucket}.{_SERVICE}.{_REGION}.amazonaws.com"
    url = f"{base}/{_KEY}"

    # us-east-1 is S3's legacy default region and REJECTS an explicit
    # LocationConstraint (InvalidLocationConstraint); every other region requires it.
    if _REGION == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": _REGION},
        )
    try:
        put = http.request(
            "PUT",
            url,
            body=_BODY,
            headers=_signed_headers(session, method="PUT", url=url, body=_BODY),
        )
        if put.status != _OK:
            print(f"FAIL: PUT returned {put.status}: {put.data!r}", file=sys.stderr)
            return 1
        got = http.request(
            "GET",
            url,
            headers=_signed_headers(session, method="GET", url=url, body=b""),
        )
        if got.status != _OK:
            print(f"FAIL: GET returned {got.status}: {got.data!r}", file=sys.stderr)
            return 1
        if got.data != _BODY:
            print(
                f"FAIL: round-trip mismatch: sent {_BODY!r}, got {got.data!r}",
                file=sys.stderr,
            )
            return 1
    finally:
        s3.delete_object(Bucket=bucket, Key=_KEY)
        s3.delete_bucket(Bucket=bucket)

    print(f"PASS: re-signed PUT/GET 200 + byte-identical round-trip on {bucket}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
