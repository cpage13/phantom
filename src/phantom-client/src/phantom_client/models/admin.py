"""Admin-API filter and response models.

These are the Pydantic boundary types for the body shapes Phantom
accepts on bulk and filtered admin endpoints and the responses it
returns. Each request-body model uses ``extra="forbid"`` to surface
typos early; response models use ``extra="ignore"`` so future Phantom
additions don't fault the SDK.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from phantom_client.models.chain import CapturedStep, ChainState
from phantom_client.models.status import (
    AuthStatus,
    BodyLocation,
    StateBreakdown,
    TierBreakdown,
    UploadRow,
    UploadState,
)

# ---------------------------------------------------------------------------
# Filter bodies: request payloads.
# ---------------------------------------------------------------------------


class ExtractFilter(BaseModel):
    """Body of ``POST /v1/admin/chains/extract``.

    All fields are optional; an empty filter is allowed for the extract
    endpoint (the operator may want every row in a tar archive). For
    deletion the filter must be non-empty; see :class:`DeleteFilter`.

    Mirrors :class:`phantom.models.admin.ExtractFilter` byte-for-byte
    (the admin-models alignment contract test). ``since`` and the
    ``chain_ids`` elements opt out of strict mode so the FastAPI dict
    path can coerce the JSON ISO-string / uuid-strings this SDK emits;
    a malformed value still 422s server-side (R-EX1).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    state: UploadState | None = Field(None, description="Match by state.")
    route: str | None = Field(None, description="Match by route name.")
    since: datetime | None = Field(None, strict=False, description="Match by received_at >= since.")
    chain_ids: list[Annotated[UUID, Field(strict=False)]] | None = Field(
        None, description="Match exactly these chain_ids."
    )
    instance: str | None = Field(None, description="Scope to one instance id.")


class DeleteFilter(BaseModel):
    """Body of ``DELETE /v1/admin/chains``.

    At least one field MUST be non-None; guarded server-side and
    pre-flight-checked by :meth:`PhantomClient.bulk_delete`. Empty
    filters raise ``EmptyFilterError`` without hitting the network.

    Mirrors :class:`phantom.models.admin.DeleteFilter` byte-for-byte;
    ``since`` opts out of strict mode for the same wire-coercion reason
    as :class:`ExtractFilter.since` (R-EX1).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    state: UploadState | None = Field(None, description="Match by state.")
    route: str | None = Field(None, description="Match by route name.")
    since: datetime | None = Field(None, strict=False, description="Match by received_at >= since.")
    instance: str | None = Field(None, description="Scope to one instance id.")

    def is_empty(self) -> bool:
        """True iff no filter field is set (the unsafe 'delete-all' shape)."""
        return not any([self.state, self.route, self.since, self.instance])


class KeyValueMatchFilter(BaseModel):
    """Filter on ``metadata.key_value_store`` entries.

    Used by ``find_by_metadata`` to look up uploads stamped with a
    specific KVS pair (the canonical example is
    ``phantom_local_uuid -> "<uuid>"`` written by an upstream adapter).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    key: str = Field(..., min_length=1, description="Metadata KVS key to match.")
    value: str = Field(..., min_length=1, description="Metadata KVS value to match.")


# ---------------------------------------------------------------------------
# Destination-credential push bodies (the SigV4 re-sign surface).
# ---------------------------------------------------------------------------


class SigningService(StrEnum):
    """AWS service a destination credential SigV4-signs for.

    Mirrors the server's :class:`phantom.models.credential.SigningService`
    (the schema is intentionally duplicated across the process boundary per
    ADR-012). A ``StrEnum``, so each member IS its wire value (``"s3"``) and
    serializes for free at the pydantic boundary. Today S3 is the only
    implemented service; the literal ``"s3"`` is defined HERE exactly once on
    the client side, and all logic references the symbol
    :attr:`SigningService.S3` by dot notation.
    """

    S3 = "s3"


class SigV4StaticCredBody(BaseModel):
    """Admin credential-push body: a static SigV4 key-pair (resolved literals).

    Mirrors the server's :class:`phantom.models.credential.SigV4StaticCredBody`
    field-for-field, with one DELIBERATE difference: the client OMITS the
    server's ``@field_validator("service", mode="before")`` coercer. Under
    ``strict=True`` that means a raw wire string is rejected; callers MUST
    construct this body with a :class:`SigningService` MEMBER
    (``service=SigningService.S3``), which serializes to ``"s3"`` on the wire.
    The secret is never echoed in any response (ADR-004).
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


class ProfileRefCredBody(BaseModel):
    """Admin credential-push body: a profile / default-chain reference.

    Mirrors the server's :class:`phantom.models.credential.ProfileRefCredBody`.
    Like :class:`SigV4StaticCredBody`, the client omits the server's ``service``
    coercer: construct with a :class:`SigningService` member, not a raw string.
    """

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


CredentialPushBody = Annotated[
    SigV4StaticCredBody | ProfileRefCredBody, Field(discriminator="kind")
]
"""The admin credential-push wire body (a discriminated union on ``kind``).

Mirrors the server's :data:`phantom.models.credential.CredentialPushBody`. The
runtime value is always one of the two concrete ``BaseModel`` arms; the alias
itself is a typing construct, so a caller passes a constructed
:class:`SigV4StaticCredBody` / :class:`ProfileRefCredBody` instance.
"""


# ---------------------------------------------------------------------------
# Response shapes.
# ---------------------------------------------------------------------------


class ChainAdminStepDetail(BaseModel):
    """One step's request-envelope projection for the admin GET surface.

    Mirrors :class:`phantom.models.admin.ChainAdminStepDetail`. The
    envelope's per-step request shape is surfaced (name, method, url,
    headers, has_body) so admin tooling can render "what did the producer
    send?" without re-shipping payload bytes through the metadata
    response.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., description="Step name (envelope-defined, snake_case).")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        ...,
        description="HTTP method this step issues to upstream.",
    )
    url: str = Field(
        ...,
        description=(
            "Target URL or path; may contain {{step.var}} placeholders that "
            "Phantom resolves at execution time. Surfaced byte-equal to "
            "what was submitted."
        ),
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Outbound headers configured on this step at submission. "
            "Surfaced byte-equal to what was submitted; values may still "
            "contain unresolved {{step.var}} placeholders."
        ),
    )
    has_body: bool = Field(
        ...,
        description=(
            "True iff this step declared a body at submission (inline JSON/"
            "text/bytes OR a body_ref pointing at uploaded bytes). The body "
            "payload itself is fetched separately via the body or bundle "
            "endpoints."
        ),
    )


class ChainAdminDetail(BaseModel):
    """Response payload for ``GET /v1/admin/chains/{chain_id}``.

    Admin-only (loopback per ADR-004). Extends the wire-facing
    :class:`ChainResponse` with ``body_location`` so operators and
    E2E test helpers can inspect storage state.

    Mirrors :class:`phantom.models.admin.ChainAdminDetail`. The chain-
    model contract test at
    ``tests/contract/test_chain_models_alignment.py`` covers only the
    wire-facing :class:`ChainResponse`; this model is admin-specific
    and not part of that drift check.

    Round-2 extension (defender): :attr:`metadata` and :attr:`steps`
    surface the persisted request envelope so an operator can answer
    "what did the caller send?" via admin GET alone. An upstream
    create-file step's KVS lands in ``metadata``; per-step request
    shape lands in ``steps``.

    Phase 1 Slice 1.E (plan § 2.3.19 / § 2.3.20): the pre-Phase-1
    ``tier`` + ``committed`` pair collapsed into the single
    ``body_location`` discriminator. The wire shape returned by the
    service mirrors this collapse.

    Cycle-7 task 4.5 extension: ``received_at``, ``updated_at``,
    ``next_attempt_at``, ``sent_at``, ``group_id``, ``multifile_id``,
    and ``send_order`` are read straight off the upload row so the
    per-chain detail answers the delivery-time and grouping questions
    without a second lookup.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(..., description="The chain's id.")
    state: ChainState = Field(..., description="Current chain state.")
    received_at: datetime = Field(..., description="Ingress timestamp (UTC).")
    updated_at: datetime = Field(..., description="When the row last transitioned (UTC).")
    next_attempt_at: datetime | None = Field(
        None,
        description=(
            "When the sender will attempt this chain next (UTC); None when "
            "terminal or not scheduled."
        ),
    )
    sent_at: datetime | None = Field(
        None,
        description=(
            "UTC instant the upload was confirmed delivered upstream; "
            "None until delivered. Stamped once, never moved."
        ),
    )
    group_id: UUID = Field(
        ...,
        description=(
            "Query-grouping handle (the X-Phantom-Group-Id header value at "
            "admission, else chain_id; never null)."
        ),
    )
    multifile_id: UUID | None = Field(
        None,
        description=(
            "The multi-file set this upload belongs to; None when standalone. "
            "Distinct from the query group_id."
        ),
    )
    send_order: int = Field(
        0,
        ge=0,
        description=(
            "Recorded position within the multi-file set; 0 for standalone "
            "uploads. Display only, never enforced."
        ),
    )
    body_location: BodyLocation = Field(
        ...,
        description=(
            "Which body store currently holds the chain's body bytes. "
            "'ram' = the RAM body store; 'file' = the file body store. "
            "Replaces the pre-Phase-1 tier + committed pair."
        ),
    )
    last_step_completed: str | None = Field(
        None,
        description="Name of last terminal-success step; None if none yet.",
    )
    captured: list[CapturedStep] = Field(
        default_factory=list,
        description="Captured values from completed steps.",
    )
    attempts: int = Field(
        ...,
        ge=0,
        description="Attempt count on the current step.",
    )
    last_error: str | None = Field(
        None,
        description="Short error string from most recent failed attempt; None if never failed.",
    )
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Top-level metadata key-value store extracted from the chain "
            "envelope's create-file step. Empty dict when no step carries "
            "an inline JSON body with a ``metadata.key_value_store`` "
            "shape. Surfaced byte-equal to what the producer submitted."
        ),
    )
    steps: list[ChainAdminStepDetail] = Field(
        default_factory=list,
        description=(
            "Per-step request envelope projection. Each entry exposes "
            "method, url, headers, and has_body for the step as submitted."
        ),
    )


class GroupMember(BaseModel):
    """One upload that belongs to a query group.

    Mirrors :class:`phantom.models.admin.GroupMember` byte-for-byte
    (ADR-012); drift is caught by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(..., description="The member upload's chain_id (primary key).")
    state: ChainState = Field(..., description="The member's current chain state.")
    received_at: datetime = Field(..., description="Ingress timestamp (UTC).")
    sent_at: datetime | None = Field(
        None,
        description=(
            "UTC instant the member was confirmed delivered upstream; "
            "None until delivered. Stamped once, never moved."
        ),
    )
    attempts: int = Field(..., ge=0, description="Attempt count on the member's current step.")
    last_error: str | None = Field(
        None,
        description=(
            "Short error string from the member's most recent failed attempt; None if never failed."
        ),
    )
    send_order: int = Field(
        0,
        ge=0,
        description=(
            "The member's recorded position within its multi-file set; 0 for standalone uploads."
        ),
    )
    multifile_id: UUID | None = Field(
        None,
        description=(
            "The multi-file set the member belongs to; None when the member "
            "arrived standalone. Distinct from the query group_id."
        ),
    )


class GroupStatusResponse(BaseModel):
    """Synthesized rollup for one query group.

    Mirrors :class:`phantom.models.admin.GroupStatusResponse`
    byte-for-byte (ADR-012); drift is caught by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    group_id: UUID = Field(
        ..., description="The query group this rollup describes (the path parameter)."
    )
    total: int = Field(..., ge=0, description="Number of member uploads in the group.")
    counts_by_state: dict[ChainState, int] = Field(
        ...,
        description="Histogram of member states across the nine canonical ChainState values.",
    )
    all_finished: bool = Field(
        ...,
        description=(
            "True iff no member is in state queued or attempting. "
            "auth_expired and corrupted count as finished for the client: "
            "neither progresses without intervention. A token push that "
            "revives an auth_expired member flips this back to false "
            "while it re-attempts."
        ),
    )
    first_received_at: datetime | None = Field(
        None, description="Earliest member received_at (UTC); None when the group is empty."
    )
    last_sent_at: datetime | None = Field(
        None,
        description=(
            "Latest member sent_at (UTC) across members that have been delivered; "
            "None when no member has been delivered yet."
        ),
    )
    members: list[GroupMember] = Field(
        default_factory=list, description="One entry per member upload."
    )


class UploadStatusSummary(BaseModel):
    """The 'did my upload land' status projection for one upload.

    Mirrors :class:`phantom.models.admin.UploadStatusSummary`
    byte-for-byte (ADR-012); drift is caught by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(..., description="The upload's chain_id (primary key).")
    state: ChainState = Field(..., description="Current chain state.")
    received_at: datetime = Field(..., description="Ingress timestamp (UTC).")
    sent_at: datetime | None = Field(
        None,
        description=(
            "UTC instant the upload was confirmed delivered upstream; None until delivered."
        ),
    )
    attempts: int = Field(..., ge=0, description="Attempt count on the current step.")
    last_error: str | None = Field(
        None,
        description="Short error string from the most recent failed attempt; None if never failed.",
    )
    instance_id: str = Field(..., description="The instance (ADR-006) this upload belongs to.")
    multifile_id: UUID | None = Field(
        None,
        description=(
            "The multi-file set this upload belongs to; None when standalone. "
            "Distinct from the query group_id."
        ),
    )
    send_order: int = Field(
        0,
        ge=0,
        description=(
            "Recorded position within the multi-file set; 0 for standalone "
            "uploads. Display only, never enforced."
        ),
    )
    captured_file_id: str | None = Field(
        None,
        description=(
            "The upstream-assigned file id pulled from the captured file_information; "
            "None when not yet captured. Surfaced so the caller sees the correlation."
        ),
    )
    local_uuid: UUID | None = Field(
        None,
        description=(
            "The phantom_local_uuid carried on the upstream request's metadata key-value store."
        ),
    )


class IdentifierLookupResponse(BaseModel):
    """Result of an either-identifier lookup.

    Mirrors :class:`phantom.models.admin.IdentifierLookupResponse`
    byte-for-byte (ADR-012); drift is caught by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["captured_file_id", "local_uuid"] = Field(
        ..., description="Which identifier axis was queried."
    )
    value: str = Field(..., description="The identifier value queried.")
    found: bool = Field(..., description="True iff at least one upload matched.")
    matches: list[UploadStatusSummary] = Field(
        default_factory=list,
        description=(
            "Matching uploads; usually zero or one. A list because Phantom enforces no "
            "global uniqueness on either identifier."
        ),
    )


class BulkDeleteResponse(BaseModel):
    """Response payload for ``DELETE /v1/admin/chains``."""

    model_config = ConfigDict(strict=True, extra="forbid")

    deleted: int = Field(..., ge=0, description="Number of rows deleted.")


class ListUploadsResponse(BaseModel):
    """Response payload for ``GET /v1/admin/chains``.

    Mirrors :class:`phantom.models.admin.ListUploadsResponse` byte-for-byte;
    the field is ``uploads`` (not ``rows``). The duplication is enforced by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    uploads: list[UploadRow] = Field(
        default_factory=list,
        description="Operator-detail rows matching the request filter.",
    )
    next_cursor: str | None = Field(
        None,
        description="Opaque cursor for pagination; null when no more pages.",
    )


class UploadBundle(BaseModel):
    """Bundled metadata + body refs returned by :meth:`PhantomClient.fetch_bundle`.

    The server route ``GET /v1/admin/chains/{id}/bundle`` returns
    ``{"metadata": ..., "body_refs": {name: <hex>}}`` - a NAME -> bytes
    map, the same body-ref vocabulary an upload carries on intake. The
    SDK mirrors that richer shape: ``body_refs`` keeps every named ref
    distinct rather than collapsing to a single ``body`` (which would
    lose information for a multi-ref upload). The wire hex strings are
    decoded to ``bytes`` here (R-EX2).
    """

    model_config = ConfigDict(strict=True, extra="ignore", arbitrary_types_allowed=True)

    metadata: UploadRow = Field(..., description="The upload row metadata.")
    body_refs: dict[str, bytes] = Field(
        ...,
        description="Body bytes per declared body_ref name (decoded from the wire hex map).",
    )

    @field_validator("body_refs", mode="before")
    @classmethod
    def _decode_hex_body_refs(cls, value: object) -> object:
        """Decode the wire ``{name: hex}`` map to ``{name: bytes}``.

        The server emits each ref's bytes as a hex string (``bytes.hex()``);
        Pydantic's default JSON-to-``bytes`` path does not read hex, so the
        decode happens here before strict validation sees real ``bytes``. A
        non-hex string raises ``ValueError`` -> a parse error the caller
        sees, never a silent wrong-bytes result.
        """
        if isinstance(value, dict):
            return {
                name: (bytes.fromhex(ref) if isinstance(ref, str) else ref)
                for name, ref in value.items()
            }
        return value


class InstanceSummary(BaseModel):
    """One element of :attr:`AdminStatusResponse.instances`."""

    model_config = ConfigDict(strict=True, extra="ignore")

    id: str = Field(..., description="Instance id.")
    host_prefixes: list[str] = Field(
        default_factory=list,
        description="Upstream host prefixes this instance is bound to.",
    )
    refresh_strategy: Literal["wait", "ad_client_credentials"] = Field(
        ..., description="Refresh strategy configured for this instance (ADR-001)."
    )
    in_flight: int = Field(..., ge=0, description="In-flight upload count.")


class AdminStatusResponse(BaseModel):
    """Response for ``GET /v1/admin/status``."""

    model_config = ConfigDict(strict=True, extra="ignore")

    ready: bool = Field(..., description="True iff every instance is ready.")
    disk_usage_bytes: int = Field(
        ..., ge=0, description="Aggregate disk usage across all instances."
    )
    total_backlog: int = Field(..., ge=0, description="Aggregate non-terminal upload count.")
    instances: list[InstanceSummary] = Field(
        default_factory=list, description="Per-instance summary."
    )
    ad_reachability: Literal["reachable", "unreachable", "not_configured"] = Field(
        "not_configured",
        description=(
            "Whether Phantom can reach Azure AD for token minting. "
            "'not_configured' when no instance runs ad_client_credentials."
        ),
    )


class InstanceStatusResponse(BaseModel):
    """Response for ``GET /v1/admin/instances/{id}/status``.

    Mirrors :class:`phantom.models.admin.InstanceStatusResponse`
    byte-for-byte; enforced by the admin contract test.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(..., description="InstanceCfg.id.")
    ready: bool = Field(
        ...,
        description="True when the instance's storage layer is open and accepting writes.",
    )
    in_flight: TierBreakdown = Field(
        ...,
        description="In-flight count + bytes for this instance.",
    )
    by_state: StateBreakdown = Field(
        ...,
        description="Per-state count + bytes breakdown for this instance.",
    )
    auth: AuthStatus = Field(
        ...,
        description="Auth-cache summary for this instance.",
    )
    disk_usage_bytes: int = Field(
        ...,
        ge=0,
        description="Current bytes consumed by this instance's disk-tier data directory.",
    )
    degraded_durability: bool = Field(
        False,
        description=("True when disk write failures detected; surfaced in the admin status."),
    )


# ---------------------------------------------------------------------------
# Plan § 4.2.5: Observability admin response shapes (SDK mirror).
# Mirrors phantom.models.admin byte-for-byte; enforced by the admin
# contract test.
# ---------------------------------------------------------------------------


class CounterValue(BaseModel):
    """One counter entry from the observability surface."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., description="Canonical metric name.")
    description: str = Field(..., description="One-line human-readable description.")
    values: dict[str, int] = Field(
        ...,
        description="Label-value bucket → count.",
    )


class CountersResponse(BaseModel):
    """``GET /v1/admin/observability/counters`` response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    counters: list[CounterValue] = Field(
        ...,
        description="Every registered counter.",
    )


class GaugeValue(BaseModel):
    """One gauge entry from the observability surface."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., description="Canonical metric name.")
    description: str = Field(..., description="One-line human-readable description.")
    values: dict[str, float] = Field(
        ...,
        description="Label-value bucket → current value.",
    )


class GaugesResponse(BaseModel):
    """``GET /v1/admin/observability/gauges`` response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    gauges: list[GaugeValue] = Field(
        ...,
        description="Every registered gauge.",
    )


class RamPressureStatusResponse(BaseModel):
    """``GET /v1/admin/observability/ram_pressure`` response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    ram_body_store_bytes: int = Field(
        ...,
        ge=0,
        description="Current RamBodyStore.total_bytes() observation.",
    )
    ram_ceiling_bytes: int = Field(
        ...,
        ge=0,
        description="Configured BodyStoreCfg.ram_ceiling_bytes.",
    )
    pending_migrations: int = Field(
        ...,
        ge=0,
        description="In-flight PersistController migrations.",
    )
    persist_controller_queue_depth: int = Field(
        ...,
        ge=0,
        description="Current PersistController queue size.",
    )


# ---------------------------------------------------------------------------
# Phase 4 § 5.2.5 + cycle-7 seam 2 - Quarantine inventory (SDK mirror).
# ---------------------------------------------------------------------------


class QuarantineEntry(BaseModel):
    """One BACKUP (or anomaly) entry - SDK mirror of phantom.models.admin.

    One entry per backup, keyed by ``backup_id`` (the restore handle); an
    on-disk artifact no manifest claims surfaces as an anomaly entry
    (``anomaly`` true, ``backup_id`` null) and is not restorable.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    backup_id: UUID | None = Field(
        ...,
        description=(
            "The backup's uuid identity (the restore handle); null iff this "
            "entry is an anomaly (unmanifested artifact, not restorable)."
        ),
    )
    reason: Literal["corrupted", "mode_switch"] = Field(
        ...,
        description=(
            "Why the backup was taken: corrupted or mode_switch. Only "
            "mode_switch backups are restorable."
        ),
    )
    iso_display: str = Field(
        ...,
        description=(
            "Human-readable timestamp half of the artifact names; display "
            "and sort only, never identity."
        ),
    )
    db_path: str | None = Field(
        ...,
        description="Declared path of the backup's DB artifact, or null.",
    )
    body_path: str | None = Field(
        ...,
        description="Declared path of the backup's body-tree artifact, or null.",
    )
    has_db: bool = Field(
        ...,
        description="Whether the DB artifact is present on disk right now.",
    )
    has_body: bool = Field(
        ...,
        description="Whether the body-tree artifact is present on disk right now.",
    )
    bytes: int = Field(
        ...,
        ge=0,
        description="Total size in bytes of the artifacts present on disk.",
    )
    anomaly: bool = Field(
        ...,
        description=(
            "True for an unmanifested on-disk artifact: surfaced for the "
            "operator, never restorable."
        ),
    )


class QuarantineInventoryResponse(BaseModel):
    """``GET /v1/admin/quarantine`` response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    quarantines: list[QuarantineEntry] = Field(
        ...,
        description=(
            "One entry per backup (plus flagged anomalies) under the "
            "targeted instance data root(s)."
        ),
    )


class QuarantineRestoreResponse(BaseModel):
    """``POST /v1/admin/quarantine/restore`` response (SDK mirror)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    restored_db: str = Field(
        ...,
        description="Absolute path of the live DB after restore.",
    )
    restored_body: str = Field(
        ...,
        description="Absolute path of the live body-store root after restore.",
    )
    interim_backup_db: str | None = Field(
        ...,
        description="Path of the DB backed up before restore, or null.",
    )
    interim_backup_body: str | None = Field(
        ...,
        description="Path of the body tree backed up before restore, or null.",
    )
    restart_required: bool = Field(
        ...,
        description="Always true: restart in a disk-backed mode to serve restored data.",
    )
    detail: str = Field(
        ...,
        description="Human-readable instruction for the operator.",
    )


__all__ = [
    "AdminStatusResponse",
    "BulkDeleteResponse",
    "CounterValue",
    "CountersResponse",
    "CredentialPushBody",
    "DeleteFilter",
    "ExtractFilter",
    "GaugeValue",
    "GaugesResponse",
    "InstanceStatusResponse",
    "InstanceSummary",
    "KeyValueMatchFilter",
    "ListUploadsResponse",
    "ProfileRefCredBody",
    "QuarantineEntry",
    "QuarantineInventoryResponse",
    "QuarantineRestoreResponse",
    "RamPressureStatusResponse",
    "SigV4StaticCredBody",
    "SigningService",
    "UploadBundle",
]
