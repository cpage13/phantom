"""Internal credential-store row + the structured destination credential.

This module is the credential-subsystem analogue of :mod:`phantom.models.token`
(a FAITHFUL copy per the 2026-06-23 owner directive: copy the token
implementation, differ only where forced). The forced differences from the
token shapes are:

* the slot is keyed by the destination **host alone** (``dest_host``); a SigV4
  step has no caller-supplied credential id, so the token cache's ``uid`` axis
  is dropped (ADR-002 is untouched; ``uid`` stays inert under ``aws_sigv4``);
* the value is a **structured tagged-union credential**
  (:data:`DestinationCredential`), not a bare ``bearer`` string.

:class:`CredCacheRow` is the boundary type between the SQLite
``credential_store`` table and Phantom's in-memory code. It is **internal
only**; the credential value never crosses an HTTP response boundary
(ADR-004). Admin-facing serialization uses :class:`CredentialSlot` instead,
which carries no secret material (only the credential *type* and status). The
admin push *into* the store uses :data:`CredentialPushBody`; that secret is
never returned in any response.

Everything around the value field (the ``observed_at`` / ``source`` /
``status`` columns, the internal-vs-admin two-model split) copies the token
shapes verbatim.

Both SigV4 credential variants carry an explicit, REQUIRED ``service`` (a
:class:`SigningService`), the scope sibling of ``region``. It is the AWS
service the signer signs for, declared at provision time (never defaulted /
inferred) so an unknown or missing service fails loud at the pydantic boundary
rather than being silently mis-signed. A :class:`StrEnum` member IS a ``str``,
so ``service`` serializes to its wire value (``"s3"``) for free at the SQLite
write and is re-coerced to the enum on read.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, NewType, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator

# The semantically-typed key: the lower-cased resolved destination hostname.
# COPY of the token cache's (endpoint, uid) key axis, DIFFERING only by dropping
# ``uid`` (a SigV4 step has no per-request credential id; host is the whole key).
HostCredKey = NewType("HostCredKey", str)

# NOTE: ``TypeAlias`` form intentional; matches phantom.models.token's enums.
# COPY of TokenStatus (models/token.py:26); VERBATIM.
CredentialStatus: TypeAlias = Literal["fresh", "bad", "unknown"]  # noqa: UP040
"""Freshness state of the cached credential (COPY of ``TokenStatus``).

* ``fresh``: most recently observed credential; presumed valid.
* ``bad``: last attempt with this credential returned 401/403; kept in the
  cache anyway per ADR-003 so the admin API can surface it.
* ``unknown``: slot exists but has not been used yet (rare).
"""

CredentialSource: TypeAlias = Literal["admin_push", "config"]  # noqa: UP040
"""How the cache slot's credential was last written.

COPY of ``TokenSource`` (models/token.py:18), DIFFERING by the writer set: no
``inbound_request`` (a producer does not push credentials on ``/v1/send`` for
pure SigV4), so just:

* ``admin_push``: operator pushed via the loopback admin credential endpoint.
* ``config``: a deployment declared static creds in config; materialized into
  the host-keyed store at settings-load (resolved literal values, never
  env-var names; those are resolved away at load time).
"""


class SigningService(StrEnum):
    """Closed set of AWS services Phantom can SigV4-sign for.

    A ``StrEnum``, so each member IS a ``str``: it serializes to its wire value
    (``"s3"``) for free at the pydantic boundary and at the SQLite write
    (``asdict`` + ``json.dumps``). Today the only implemented service is S3;
    adding another is a new member + a ``_SERVICE_SIGNERS`` entry (in
    ``chain.sigv4_signer``), not a redesign.

    The literal ``"s3"`` is defined HERE exactly once (the single source of the
    wire string); all logic references the symbol :attr:`SigningService.S3` by
    dot notation.
    """

    S3 = "s3"


def _coerce_signing_service(v: object) -> SigningService:
    """Coerce a wire value to :class:`SigningService` (a ``mode='before'`` body).

    Required under ``ConfigDict(strict=True)``: a bare ``service: SigningService``
    field under strict mode REJECTS the wire string ``"s3"`` (an
    ``is_instance_of`` error), so the three pydantic boundary models attach this
    as a ``@field_validator("service", mode="before")``. It maps ``"s3"`` ->
    :attr:`SigningService.S3`, raises a clean ``ValueError`` on an unknown string,
    and passes an already-enum member through unchanged.

    Args:
        v: The raw inbound value (a wire string, or an already-coerced member).

    Returns:
        The :class:`SigningService` member.

    Raises:
        ValueError: When ``v`` is not a known service string (the clean
            ``value_error`` the strict boundary surfaces as a ``ValidationError``).
    """
    if isinstance(v, SigningService):
        return v
    if isinstance(v, str):
        try:
            return SigningService(v)
        except ValueError:
            raise ValueError(f"{v!r} is not a valid SigningService") from None
    raise ValueError(f"{v!r} is not a valid SigningService")


@dataclass(frozen=True)
class SigV4StaticCreds:
    """RESOLVED static AWS SigV4 key-pair: literal values, never env-var names.

    Holds the credential VALUES botocore ``SigV4Auth`` needs at sign time
    (``SigV4Auth.__init__`` takes ``(credentials, service_name, region_name)``).
    STS / temporary credentials ride via ``session_token``.

    ``service`` is an EXPLICIT, REQUIRED scope input; the AWS service the
    signer signs for (the sibling of ``region``; the signer dispatches on it to
    select the botocore signer class). It is coerced to :class:`SigningService`
    at the pydantic boundary (the push body / config arm) and re-coerced on the
    SQLite read path. ``region`` and ``service`` precede the optional
    ``session_token`` because a frozen dataclass forbids a non-default field
    after a defaulted one and both are required.
    """

    access_key_id: str
    secret_access_key: str
    region: str
    service: SigningService
    session_token: str | None = None
    kind: Literal["sigv4_static"] = "sigv4_static"


@dataclass(frozen=True)
class ProfileRefCred:
    """A profile / default-chain REFERENCE; the resolver delegates to botocore.

    ``profile=None`` marks "the default chain". Nothing copyable is held at
    rest for this variant; botocore resolves (and auto-refreshes SSO/STS)
    credentials at sign time.

    ``service`` is REQUIRED here too (and is the FIRST field, because a frozen
    dataclass forbids a required field after a defaulted one and every other
    field on this variant is defaulted). It is declared even on the profile
    variant because botocore's credential chain supplies a region but NEVER a
    service; the signer needs the service to dispatch the signer class
    regardless of how the credentials themselves resolve.
    """

    service: SigningService
    profile: str | None = None
    region: str | None = None
    kind: Literal["profile_ref"] = "profile_ref"


# The structured credential VALUE; a 2-arm tagged union discriminated by
# ``kind`` (the FORCED difference vs the token cache's bare ``bearer`` string).
# ``BearerCred`` is intentionally NOT a member: a bearer is the EXISTING
# ``phantom_bearer`` token path, not a credential-store value.
DestinationCredential: TypeAlias = SigV4StaticCreds | ProfileRefCred  # noqa: UP040


@dataclass(frozen=True)
class CredCacheRow:
    """One credential slot; INTERNAL ONLY (the credential never crosses an
    HTTP response boundary, ADR-004).

    COPY of :class:`phantom.models.token.TokenCacheRow`: the ``observed_at`` /
    ``source`` / ``status`` columns are verbatim; the destination ``dest_host``
    plus the structured ``credential`` replace the token row's
    ``(endpoint, uid)`` plus ``bearer``. The credential persists at rest per
    ADR-003 (the store survives Phantom restart). Bad slots stay in the cache
    rather than being deleted so the admin API can surface "this is the only
    credential I have for this host, and it's bad".
    """

    dest_host: HostCredKey
    credential: DestinationCredential
    observed_at: datetime
    source: CredentialSource
    status: CredentialStatus


class CredentialSlot(BaseModel):
    """Admin-facing credential slot; NO secret material (ADR-004).

    COPY of :class:`phantom.models.admin.TokenSlot`. This is the only
    credential shape any admin HTTP response may carry (if a GET-list is ever
    added): it exposes the credential *type* (``kind``) and freshness, never
    the resolved secret.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    dest_host: str = Field(
        ...,
        description="The destination host this credential is keyed on.",
    )
    kind: Literal["sigv4_static", "profile_ref"] = Field(
        ...,
        description="The credential TYPE (never its value).",
    )
    last_updated: datetime = Field(
        ...,
        description="When this slot's credential was last written.",
    )
    status: CredentialStatus = Field(
        ...,
        description="Current freshness state (per ADR-003).",
    )


class SigV4StaticCredBody(BaseModel):
    """``PUT`` admin-push body for a static SigV4 key-pair.

    COPY of :class:`phantom.models.admin.TokenPushRequest` shape: resolved
    literals only (env-var names live only on the config route), never returned
    in any response (ADR-004).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["sigv4_static"] = Field(
        "sigv4_static", description="Union discriminator; always sigv4_static on this arm."
    )
    access_key_id: str = Field(
        ..., min_length=1, description="Resolved AWS access key id literal, never an env-var name."
    )
    secret_access_key: str = Field(
        ...,
        min_length=1,
        description="Resolved secret; never returned in any response (ADR-004).",
    )
    region: str = Field(
        ...,
        min_length=1,
        description="AWS region the signature is scoped to; the scope sibling of service.",
    )
    service: SigningService = Field(..., description="AWS service this credential signs for.")
    session_token: str | None = Field(
        None, description="STS session token for temporary credentials; None for long-lived keys."
    )

    @field_validator("service", mode="before")
    @classmethod
    def _validate_service(cls, v: object) -> SigningService:
        """Coerce the wire ``service`` string to :class:`SigningService` (strict mode)."""
        return _coerce_signing_service(v)


class ProfileRefCredBody(BaseModel):
    """``PUT`` admin-push body for a profile / default-chain reference."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["profile_ref"] = Field(
        "profile_ref", description="Union discriminator; always profile_ref on this arm."
    )
    profile: str | None = Field(
        None,
        description="Named AWS profile resolved at sign time; None means the default chain.",
    )
    region: str | None = Field(
        None,
        description="AWS region for the resolved profile; None defers to the profile or chain.",
    )
    service: SigningService = Field(..., description="AWS service this credential signs for.")

    @field_validator("service", mode="before")
    @classmethod
    def _validate_service(cls, v: object) -> SigningService:
        """Coerce the wire ``service`` string to :class:`SigningService` (strict mode)."""
        return _coerce_signing_service(v)


CredentialPushBody = Annotated[
    SigV4StaticCredBody | ProfileRefCredBody, Field(discriminator="kind")
]
"""The admin credential-push wire body (a discriminated union on ``kind``).

The handler (TASK 2.4) maps this 1:1 onto the internal frozen
:data:`DestinationCredential` variant. There is no ``BearerCredBody``; bearers
are not pushed to the SigV4 store.
"""


def credential_body_to_internal(
    body: SigV4StaticCredBody | ProfileRefCredBody,
) -> DestinationCredential:
    """Map the admin-push wire body 1:1 onto the internal frozen credential.

    The wire body carries RESOLVED LITERAL values (the admin push side does no
    env-var-name resolution; that is the config route's job at boot, GLOBAL
    §1.2(a) B1). This is a straight field copy from the validated
    :class:`SigV4StaticCredBody` / :class:`ProfileRefCredBody` Pydantic body to
    the matching frozen :class:`SigV4StaticCreds` / :class:`ProfileRefCred`
    dataclass that the store persists and the signer consumes.

    Args:
        body: The discriminated :data:`CredentialPushBody` (already validated
            and narrowed by Pydantic to one of the two arms via ``kind``).

    Returns:
        The internal :data:`DestinationCredential` variant matching ``body.kind``.
    """
    if isinstance(body, SigV4StaticCredBody):
        return SigV4StaticCreds(
            access_key_id=body.access_key_id,
            secret_access_key=body.secret_access_key,
            region=body.region,
            service=body.service,
            session_token=body.session_token,
        )
    return ProfileRefCred(service=body.service, profile=body.profile, region=body.region)
