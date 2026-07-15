"""In-process state stores for the phantom-emulator.

The emulator keeps every observable in RAM. Nothing here survives a
process restart — that is intentional: the emulator is test
infrastructure and a clean slate on each launch is the desired
property. See plan §1 "No persistence across restarts."

The container module :class:`EmulatorState` is constructed once at
startup by :mod:`phantom_emulator.app` and threaded into routers via
the FastAPI ``Depends`` mechanism.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig

if TYPE_CHECKING:  # pragma: no cover — typing-only imports
    from phantom_emulator.auth.jwks import RsaKeyPair
    from phantom_emulator.auth.jwt_minter import JwtMinter
    from phantom_emulator.failure.injection import FailureInjectionState

logger = logging.getLogger(__name__)


# Type alias for the opaque token embedded in presigned-style URLs.
# The emulator generates these; they round-trip back on PUT.
UploadToken = str

# Type alias for the Idempotency-Key header value.
IdempotencyKey = str


class UpstreamEventKind(StrEnum):
    """Closed kinds in the append-only two-step upstream event oracle."""

    METADATA_CREATE = "metadata_create"
    BODY_PUT = "body_put"


@dataclass(frozen=True)
class MetadataCreateEvent:
    """One successful metadata-create response, including cache hits."""

    occurred_at: datetime
    kind: UpstreamEventKind
    chain_id: UUID | None
    idempotency_key: IdempotencyKey | None
    file_id: UUID
    upload_token: UploadToken
    upload_url: str
    cache_hit: bool


@dataclass(frozen=True)
class BodyPutEvent:
    """One accepted upload PUT; unlike ``accepted_bodies``, never overwritten."""

    occurred_at: datetime
    kind: UpstreamEventKind
    chain_id: UUID | None
    idempotency_key: IdempotencyKey | None
    file_id: UUID
    upload_token: UploadToken
    upload_url: str
    body_hash: str
    body_size: int


UpstreamEvent = MetadataCreateEvent | BodyPutEvent


@dataclass
class IssuedToken:
    """A JWT that the emulator has minted and not yet revoked.

    Attributes:
        client_id: The OAuth2 client that requested the mint.
        expires_at: The token's ``exp`` claim, as a tz-aware datetime.
        jwt: The compact serialization (the wire form of the token).
        extra_claims: Any extra claims the test scenario injected.
    """

    client_id: str
    expires_at: datetime
    jwt: str
    extra_claims: dict[str, Any]


@dataclass
class PendingUpload:
    """A presigned upload URL the emulator has issued but not consumed.

    Attributes:
        upload_token: The opaque token embedded in the upload URL.
        file_id: The emulator-minted FileInformation.id.
        file_information: The JSON-ready response payload for the
            create call.
        metadata_kvs: The metadata.keyValueStore mapping from the
            create request (preserves ``phantom_local_uuid`` and any
            other keys byte-for-byte).
        created_at: When the URL was issued.
        presigned_ttl_seconds: Lifetime of the URL.
        signature: Opaque signature stub baked into the URL; the PUT
            handler verifies the inbound URL carries the same value.
    """

    upload_token: UploadToken
    file_id: UUID
    file_information: dict[str, Any]
    metadata_kvs: dict[str, str]
    created_at: datetime
    presigned_ttl_seconds: int
    signature: str


@dataclass
class AcceptedBody:
    """A body the emulator has accepted on the upload PUT endpoint.

    Attributes:
        upload_token: The opaque token the PUT consumed.
        body: The raw bytes the client uploaded.
        headers: All ``x-amz-meta-*`` request headers from the PUT,
            preserved for test assertion.
        content_encoding: The PUT's ``Content-Encoding`` header (or
            ``None`` if unset). Captured so transparent-proxy tests can
            assert byte-identity plus header preservation.
        all_headers: Every inbound HTTP header on the PUT, lowercased
            keys with original values. Captured so transparent-proxy
            tests can audit the full request envelope (e.g., that
            ``X-Phantom-*`` headers were stripped, that ``Authorization``
            carries the cached bearer byte-equal, that ``User-Agent``
            is preserved). Multi-value headers are joined with ``", "``
            per Starlette's header-dict semantics.
        accepted_at: Server-side timestamp at acceptance.
    """

    upload_token: UploadToken
    body: bytes
    headers: dict[str, str]
    content_encoding: str | None
    all_headers: dict[str, str]
    accepted_at: datetime


# Type alias for the path-style S3 address: (bucket, key).
S3ObjectKey = tuple[str, str]

# Type alias for the full forwarded path (no leading slash) that keys the
# auth-free /raw sink store. The path itself is the address — there is no
# bucket/key split and no token (contrast UploadToken / S3ObjectKey).
RawPath = str


@dataclass
class S3Object:
    """A body stored by a SigV4-validated path-style upload.

    Attributes:
        bucket: First path segment of ``<verb> /{bucket}/{key}``.
        key: Remaining path (the object key; may contain slashes).
        method: The inbound HTTP verb that stored it — one of the
            forwarded upload verbs (``PUT``/``POST``/``PATCH``). Captured
            so the e2e can assert the verb round-trips through the catch-all
            to the sink.
        body: Raw bytes the validated upload stored (byte-identical).
        content_type: The request ``Content-Type``, or ``None``.
        all_headers: Every inbound header (lowercased keys, original
            values), captured so round-trip / transparent-proxy
            assertions can audit the envelope — mirrors
            :attr:`AcceptedBody.all_headers`.
        stored_at: Server-side acceptance timestamp.
    """

    bucket: str
    key: str
    method: str
    body: bytes
    content_type: str | None
    all_headers: dict[str, str]
    stored_at: datetime


@dataclass
class RawBody:
    """A body stored by the auth-free, token-free /raw sink (TASK 0.5).

    The forward-as-is Phase-1 analogue of :class:`AcceptedBody`: no token,
    no auth — the full forwarded path itself is the key. Accepts any forwarded
    upload verb (PUT/POST/PATCH), recording it in :attr:`method`.
    ``all_headers`` is captured (lowercased keys, original values) so the e2e
    can assert that ``X-Phantom-*`` headers were stripped and a benign upstream
    header survived.

    Attributes:
        path: The full forwarded path (no leading slash) used as the store
            key — slash-bearing keys captured whole by the ``:path``
            convertor.
        method: The inbound HTTP verb that stored it — one of the forwarded
            upload verbs (``PUT``/``POST``/``PATCH``). Captured so the e2e
            can assert the verb round-trips through the catch-all to the sink.
        body: Raw bytes the unsigned, tokenless upload stored (byte-identical).
        content_type: The request ``Content-Type``, or ``None``.
        all_headers: Every inbound header (lowercased keys, original
            values) — mirrors :attr:`AcceptedBody.all_headers`.
        stored_at: Server-side acceptance timestamp.
    """

    path: str
    method: str
    body: bytes
    content_type: str | None
    all_headers: dict[str, str]
    stored_at: datetime


@dataclass
class IdempotencyEntry:
    """A cached create-call response keyed by ``Idempotency-Key``.

    Attributes:
        key: The header value the caller supplied.
        response_json: The cached response payload (returned verbatim
            on a replay).
        upload_token: The token in the cached response so the PUT path
            still resolves it.
        file_id: The minted FileInformation.id.
        expires_at: When the dedup window ends; entries past their TTL
            are ignored on lookup.
    """

    key: IdempotencyKey
    response_json: dict[str, Any]
    upload_token: UploadToken
    file_id: UUID
    expires_at: datetime


@dataclass
class EmulatorState:
    """In-process container for every mutable emulator observable.

    Construct exactly once at startup. Threaded into route handlers
    through a single :func:`get_state` dependency. The fields are
    plain ``dict`` / ``bool`` / dataclass references so tests can
    introspect the post-condition of a request without going through
    HTTP again.

    Attributes:
        cfg: The application configuration this state was built from.
        issued_tokens: Active tokens keyed by their JWT string.
        extra_claims: Claims to inject into the next mint (drained on
            use or persisted depending on test setup; per the plan,
            stored for the next mint and cleared after).
        pending_uploads: Pending presigned URLs keyed by upload token.
        file_id_to_token: Reverse lookup so ``GET /v1/files/{id}``
            can resolve a record minted on the create call.
        accepted_bodies: Latest accepted body keyed by upload token. This
            materialized view intentionally overwrites repeated PUTs.
        upstream_events: Append-only successful metadata-create and body-PUT
            oracle. It preserves every successful metadata response and accepted
            body side effect for exact successful-event cardinality and ordering
            assertions while the latest-value maps retain their established
            behavior. The ``error_rate_5xx`` branch's rejected attempts are
            observed separately by ``failure_state``; other 503 mechanisms are
            intentionally outside that narrow ledger.
        s3_objects: Path-style S3 sink store, keyed by ``(bucket, key)``;
            populated by a SigV4-validated ``PUT /{bucket}/{key}``.
        raw_bodies: Auth-free /raw sink store, keyed by the full forwarded
            path; populated by an unsigned, tokenless ``PUT /raw/{path}``
            (TASK 0.5). Distinct from ``accepted_bodies`` (token-keyed) and
            ``s3_objects`` ((bucket, key)-keyed) so the capture paths never
            collide.
        idempotency_cache: Idempotency dedup cache keyed by header.
        failure_state: Failure-injection policy + counters.
        auth_mode_overrides: Per-scope auth-mode overrides; absent
            keys fall back to ``cfg.auth.default_mode``.
        global_paused: When True every upstream endpoint returns 503.
        seed: Initial seed for the deterministic RNG.
        rng: Shared seeded RNG (also used by failure_state).
        jwt_minter: Bound after startup; performs signing.
        rsa_keys: Bound in RS256 mode after startup; None otherwise.
        started_at: Process start time for uptime calculation.
        plain_bearer_allowlist: Accepted bearer values when
            ``PLAIN_BEARER`` mode is active.
        api_key_secret: Required value of ``X-API-Key`` when
            ``API_KEY`` mode is active.
        static_jwt: Pre-minted JWT served by ``static_token`` mode.
        accepted_idempotency_keys: Log of idempotency-keys seen on
            create calls — used by the control surface so tests can
            assert that a request was deduped vs. served fresh.
    """

    cfg: AppConfig
    started_at: datetime
    seed: int = 0

    issued_tokens: dict[str, IssuedToken] = field(default_factory=dict)
    extra_claims: dict[str, Any] = field(default_factory=dict)
    pending_uploads: dict[UploadToken, PendingUpload] = field(default_factory=dict)
    file_id_to_token: dict[UUID, UploadToken] = field(default_factory=dict)
    accepted_bodies: dict[UploadToken, AcceptedBody] = field(default_factory=dict)
    upstream_events: list[UpstreamEvent] = field(default_factory=list)
    s3_objects: dict[S3ObjectKey, S3Object] = field(default_factory=dict)
    raw_bodies: dict[RawPath, RawBody] = field(default_factory=dict)
    accepted_idempotency_keys: dict[UploadToken, IdempotencyKey] = field(default_factory=dict)
    idempotency_cache: dict[IdempotencyKey, IdempotencyEntry] = field(default_factory=dict)
    auth_mode_overrides: dict[str, AuthMode] = field(default_factory=dict)
    global_paused: bool = False
    # When set, the S3 sink requires every SigV4-validated request to carry
    # an ``X-Amz-Security-Token`` header equal to this value (compared with
    # hmac.compare_digest) BEFORE the signature recompute; missing or unequal
    # tokens get the same 403 SignatureDoesNotMatch as a bad signature. None
    # (the default) leaves session-token handling to the recompute alone.
    # This is the T4 STS oracle: it proves the token MATTERED, rather than
    # accepting whatever token happened to be signed.
    expected_session_token: str | None = None
    plain_bearer_allowlist: set[str] = field(default_factory=set)
    api_key_secret: str | None = None
    static_jwt: str | None = None

    # Bound during startup — see app.create_app / lifespan.
    rng: random.Random = field(default_factory=random.Random)
    failure_state: FailureInjectionState | None = None
    jwt_minter: JwtMinter | None = None
    rsa_keys: RsaKeyPair | None = None
