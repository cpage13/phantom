"""AWS SigV4 request signer — the ``aws_sigv4`` executor arm's primitive.

One call to :func:`sign_sigv4` re-signs a single outbound request with the
botocore signer class DISPATCHED from the credential's
:class:`~phantom.models.credential.SigningService` (the ``_SERVICE_SIGNERS`` map)
and mutates the passed ``headers`` dict in place (adding ``Authorization`` /
``X-Amz-Date`` / ``X-Amz-Security-Token`` when a session token is present). For
the S3 service the dispatched signer is ``S3SigV4Auth``, which EMITS + SIGNS
``x-amz-content-sha256`` — the signed header real S3 requires (the base
``SigV4Auth`` hashes the payload for the signature but never emits the header, so
real S3 400s ``Missing required header for this request: x-amz-content-sha256``).
A credential whose service has no map entry raises :class:`SigV4SigningError`,
which the executor parks (``auth_expired``), never a bare ``KeyError``. The
signer runs per send attempt inside the executor (``chain/executor.py``), so a
retry hours after the upload was buffered re-signs with a FRESH timestamp — the
routing design's core requirement (a stale SigV4 signature would be rejected by
S3's clock-skew window).

The signer reads RESOLVED credential values only. A
:class:`~phantom.models.credential.SigV4StaticCreds` carries the literal
``access_key_id`` / ``secret_access_key`` / ``region`` (and optional
``session_token``) botocore needs at sign time — no env-var-name resolution
happens here. A :class:`~phantom.models.credential.ProfileRefCred` delegates to
botocore's credential chain (profile / default chain, with SSO/STS auto-refresh);
because that chain does blocking file/network I/O, it is resolved inside
:func:`asyncio.to_thread` so the event loop is never blocked.

The body bytes are NOT mutated: botocore computes the payload hash internally for
the signature, but the forwarded body stays byte-identical (the transparent-proxy
invariant). The signed ``Authorization`` / ``x-amz-*`` headers REPLACE any the
inbound request carried.
"""

from __future__ import annotations

import asyncio
import logging

# botocore ships no py.typed marker, so mypy cannot import its types; the
# inline ignores are required and are NOT redundant (warn_unused_ignores would
# flag them if they were). Mirrors the emulator's s3 router import block.
from botocore.auth import (  # type: ignore[import-untyped]  # botocore ships no py.typed
    S3SigV4Auth,
    SigV4Auth,
)
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]
from botocore.session import Session  # type: ignore[import-untyped]

from phantom.models.credential import (
    DestinationCredential,
    ProfileRefCred,
    SigningService,
    SigV4StaticCreds,
)

logger = logging.getLogger(__name__)

# The enum -> botocore-signer-class dispatch, EXHAUSTIVE over
# :class:`SigningService`. This is the ONLY module that may name a botocore
# signer class: a credential's ``service`` selects the signer here. The S3 entry
# is ``S3SigV4Auth``, which EMITS + SIGNS ``x-amz-content-sha256`` (real S3
# requires that signed header; the base ``SigV4Auth`` hashes the payload for the
# signature but never emits the header). Adding a service is a new enum member +
# a new row here, not a redesign. The map values are typed as the base
# ``SigV4Auth`` (the common supertype of every signer class).
_SERVICE_SIGNERS: dict[SigningService, type[SigV4Auth]] = {SigningService.S3: S3SigV4Auth}

# Region of last resort when a ``ProfileRefCred`` carries no explicit region and
# botocore's configured chain (env / profile / config file) also yields none.
# botocore's ``SigV4Auth`` requires a region string; ``us-east-1`` is S3's
# global default endpoint region and the conventional fallback.
_DEFAULT_REGION = "us-east-1"


class SigV4SigningError(Exception):
    """A SigV4 credential could not be resolved or the request could not be signed.

    Raised when a :class:`ProfileRefCred`'s botocore credential chain yields no
    usable credentials. The executor arm treats this exactly like a missing /
    bad credential slot: it marks the slot bad and parks the row in
    ``auth_expired`` (NOT terminal), so a re-push recovers it.
    """


async def sign_sigv4(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes,
    credential: DestinationCredential,
) -> None:
    """Re-sign one outbound request in place with the service-dispatched signer.

    The credential's :class:`~phantom.models.credential.SigningService` selects
    the botocore signer class via ``_SERVICE_SIGNERS``; for S3 that is
    ``S3SigV4Auth``, which emits + signs ``x-amz-content-sha256``. Mutates
    ``headers`` by adding the SigV4 ``Authorization`` header, the ``X-Amz-Date``
    timestamp, the signed ``x-amz-content-sha256``, and (when the credential
    carries a session token) ``X-Amz-Security-Token``. The request body is left
    untouched.

    Args:
        method: The HTTP method (e.g. ``"PUT"``).
        url: The fully-qualified outbound URL.
        headers: The outbound header map; signed headers are merged in place.
        body: The exact outbound body bytes (fed to botocore as ``data=`` so it
            computes the payload hash over the bytes actually forwarded).
        credential: The resolved destination credential — either inline static
            SigV4 keys or a profile/default-chain reference. Its ``service``
            selects the signer class.

    Raises:
        SigV4SigningError: When a :class:`ProfileRefCred` yields no credentials,
            or the credential's ``service`` has no ``_SERVICE_SIGNERS`` entry.
    """
    botocore_creds, region = await _resolve_credentials(credential)
    request = AWSRequest(method=method, url=url, data=body, headers=headers)
    # Service-dispatched signing: the credential's ``service`` selects the
    # botocore signer class. A map-miss raises ``SigV4SigningError`` (parkable by
    # the executor's ``except``), NEVER a bare ``KeyError`` (which would escape
    # the executor and crash the loop). ``credential.service`` is passed to
    # botocore's ``service_name`` slot DIRECTLY — a ``StrEnum`` member IS a
    # ``str``, so it is consumed verbatim and a store-reloaded raw-string
    # credential signs identically (no ``.value``).
    signer_class = _SERVICE_SIGNERS.get(credential.service)
    if signer_class is None:
        raise SigV4SigningError(f"no SigV4 signer registered for service {credential.service!r}")
    signer_class(botocore_creds, credential.service, region).add_auth(request)
    # ``add_auth`` mutates ``request.headers`` (a case-insensitive map). Copy the
    # signed headers back onto the caller's plain dict so the Authorization /
    # X-Amz-* values reach the transport. ``request.headers`` was seeded from the
    # same dict, so unsigned headers round-trip unchanged.
    for name, value in request.headers.items():
        headers[name] = value


async def _resolve_credentials(
    credential: DestinationCredential,
) -> tuple[Credentials, str]:
    """Return the botocore ``(Credentials, region)`` pair for ``credential``.

    Static creds are used as-is (no I/O). A profile reference delegates to
    botocore's blocking credential chain, run in a worker thread.
    """
    if isinstance(credential, SigV4StaticCreds):
        return (
            Credentials(
                credential.access_key_id,
                credential.secret_access_key,
                credential.session_token,
            ),
            credential.region,
        )
    if isinstance(credential, ProfileRefCred):
        return await asyncio.to_thread(_resolve_profile_ref, credential)
    # ``DestinationCredential`` is a closed 2-arm union; an unhandled arm is a
    # programming error, not a runtime input.
    raise SigV4SigningError(f"Unsupported credential variant: {credential!r}")  # pragma: no cover


def _resolve_profile_ref(credential: ProfileRefCred) -> tuple[Credentials, str]:
    """Resolve a profile/default-chain reference via botocore (BLOCKING).

    Runs inside :func:`asyncio.to_thread`. botocore reads the AWS config / shared
    credentials files and may refresh SSO/STS, all of which is blocking file and
    network I/O.

    Raises:
        SigV4SigningError: When botocore's chain yields no credentials.
    """
    session = Session(profile=credential.profile)
    resolved = session.get_credentials()
    if resolved is None:
        raise SigV4SigningError(
            f"botocore credential chain yielded no credentials for profile={credential.profile!r}"
        )
    frozen = resolved.get_frozen_credentials()
    botocore_creds = Credentials(
        frozen.access_key,
        frozen.secret_key,
        frozen.token,
    )
    region = credential.region or session.get_config_variable("region") or _DEFAULT_REGION
    return botocore_creds, region


__all__ = ["SigV4SigningError", "sign_sigv4"]
