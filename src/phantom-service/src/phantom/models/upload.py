"""Internal persistent upload row.

The ``UploadRow`` model is Phantom's persistent representation of one
buffered chain. It is the schema-mirror for the ``uploads`` table in
the single persistent SQLite (post-Phase-1).

Phase 1: the old ``committed`` + ``tier`` columns collapsed into one
``body_location`` field (``Literal['ram', 'file']``) - the source of
truth for which BodyStore is holding the body files. The persist
controller is the sole writer of the ``ram`` → ``file`` transition
(plan § 0.5 invariant #6). The pre-Phase-1 ``Tier`` alias is gone
(plan § 2.3.8); admin response models that need a
storage-tier flavor now inline ``Literal["memory", "persisted"]``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, NewType, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from phantom.models.chain import ChainState

# NOTE: ``TypeAlias`` form is intentional - see phantom.models.chain for
# the rationale (drift-detection test depends on ``typing.get_args``).
UploadState: TypeAlias = ChainState  # noqa: UP040
"""Same enum on the wire and on disk. Snake-case canonical."""

BodyLocation: TypeAlias = Literal["ram", "file"]  # noqa: UP040
"""Which BodyStore currently holds the body files for this row.

Replaces the pre-Phase-1 ``Tier`` alias + ``committed`` boolean. Source
of truth for the body files' physical location. Flipped from ``ram``
to ``file`` by the PersistController after fsync; see plan § 0.5
invariant #6.
"""

StorageEncoding: TypeAlias = Literal["original", "zstd", "gzip"]  # noqa: UP040
"""Phantom-side storage compression marker; invisible to upstream.

Distinct from the wire ``Content-Encoding`` the client sent - Phantom
preserves the wire encoding end-to-end (req §5d).
"""

BodyHash = NewType("BodyHash", str)
"""SHA-256 hex of the raw body bytes the agent sent at ingress."""

StorageHash = NewType("StorageHash", str)
"""SHA-256 hex of the stored (post-encode) bytes as written to the body store."""


class BodyHashes(BaseModel):
    """Hash pair for one body_ref.

    body_hash:    verifies codec round-trip and end-to-end byte
                  identity (computed pre-encode at ingress, compared
                  post-decode at send).
    storage_hash: verifies storage integrity in RAM or on disk
                  (computed post-encode at ingress, compared pre-decode
                  at send).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    body_hash: BodyHash = Field(
        ...,
        description="SHA-256 hex of raw body bytes.",
    )
    storage_hash: StorageHash = Field(
        ...,
        description="SHA-256 hex of stored bytes.",
    )


class CapturedStepValues(BaseModel):
    """One step's captured values plus TTL bookkeeping."""

    model_config = ConfigDict(strict=True, extra="forbid")

    values: dict[str, Any] = Field(
        ...,
        description="Captured values keyed by capture name; JSON-shaped.",
    )
    captured_at: datetime = Field(
        ...,
        description="When this step produced its captures (UTC).",
    )
    expires_at: dict[str, datetime | None] = Field(
        ...,
        description=(
            "Per-capture expiry timestamp. None when the capture is "
            "non-expiring (ChainCapture.ttl_seconds was unset)."
        ),
    )


class CapturedValues(BaseModel):
    """Per-step captured-values store on the upload row."""

    model_config = ConfigDict(strict=True, extra="forbid")

    steps: dict[str, CapturedStepValues] = Field(
        default_factory=dict,
        description="Keyed by ChainStep.name; one entry per completed step.",
    )


class UploadRow(BaseModel):
    """The persistent upload row.

    Schema-mirrored in the single persistent ``uploads`` table
    (post-Phase-1). Each field maps to a SQLite column except
    ``captured_values`` which is serialized to ``captured_values_json``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(
        ...,
        description=("Upload identifier; the chain_id from the submitted envelope."),
    )
    instance_id: str = Field(
        ...,
        description="The InstanceCfg.id this upload belongs to (ADR-006).",
    )
    group_id: UUID = Field(
        ...,
        description=(
            "Query-grouping handle. Required: admission supplies the "
            "X-Phantom-Group-Id header value when present, else chain_id "
            "(every upload is a group of one by default)."
        ),
    )
    multifile_id: UUID | None = Field(
        None,
        description=(
            "Multi-file association id. NULL means standalone (the row is "
            "not part of a multi-file set)."
        ),
    )
    send_order: int = Field(
        0,
        ge=0,
        description=(
            "Recorded position within the multi-file set. Display only; never enforced at delivery."
        ),
    )
    route_name: str = Field(
        ...,
        description="Resolved RouteCfg.name for the chain's first step.",
    )
    state: UploadState = Field(
        ...,
        description="Current state in the upload state machine.",
    )
    body_location: BodyLocation = Field(
        ...,
        description=(
            "Whether body files for this chain live in RamBodyStore "
            "('ram') or FileBodyStore ('file'). Single durability "
            "commit point - flipped only by the PersistController. "
            "See strategy §3 invariant #1 / plan § 0.5 invariant #6."
        ),
    )
    attempts: int = Field(
        0,
        ge=0,
        description="Count of attempts made on the current step.",
    )
    next_attempt_at: datetime | None = Field(
        None,
        description="When the sender should next try this row.",
    )
    received_at: datetime = Field(
        ...,
        description="Ingress timestamp (UTC).",
    )
    sent_at: datetime | None = Field(
        None,
        description=(
            "When the upload was confirmed delivered upstream (UTC). "
            "Stamped once on confirmed delivery, never moved; survives "
            "replay. None until delivery."
        ),
    )
    updated_at: datetime = Field(
        ...,
        description="Most recent transition timestamp (UTC).",
    )
    last_error: str | None = Field(
        None,
        description=("Short error string from most recent failed attempt; None if never failed."),
    )
    endpoint: str = Field(
        ...,
        description=(
            "Hostname of the chain's FIRST step, computed once at admission "
            "and never updated. The admission-time cache axis (ADR-002): it "
            "is what the ingress bearer-cache write is keyed on. It is NOT "
            "the host a later step authenticates against, so the kickers' "
            "wake probe keys on ``auth_blocked_host`` instead (D2/F6)."
        ),
    )
    uid: str = Field(
        ...,
        description=(
            "The opaque X-Phantom-Uid received at ingress; the secondary "
            "axis of the (endpoint, uid) token-cache key (ADR-002)."
        ),
    )
    chain_envelope_json: str = Field(
        ...,
        description=("The validated ChainEnvelope persisted as JSON; survives restart."),
    )
    captured_values: CapturedValues = Field(
        default_factory=CapturedValues,
        description=("Captured-values store; serialized to/from captured_values_json column."),
    )
    current_step_index: int = Field(
        0,
        ge=0,
        description="Index of the next step to attempt.",
    )
    idempotency_key: str = Field(
        ...,
        description="Envelope.idempotency_key; reused on every attempt.",
    )
    chain_id_at_ingress: str | None = Field(
        None,
        description=(
            "The producer-supplied X-Phantom-Idempotency-Key captured at "
            "admission. Identifies a chain for admission-side dedup; "
            "distinct from ``idempotency_key`` (which Phantom forwards "
            "to upstream). None when the producer did not send the header. "
            "Stored on the row so admission can find an existing chain "
            "by the same ingress key even if the disk store's "
            "``idempotency_index`` entry was reaped."
        ),
    )
    capture_reexecution_active: bool = Field(
        ...,
        description=(
            "ADR-011 instance YAML knob, captured at ingress so mid-chain "
            "YAML changes don't surprise in-flight chains."
        ),
    )
    storage_encoding: StorageEncoding = Field(
        "original",
        description=("Phantom's added storage compression - invisible to upstream (req §5d)."),
    )
    body_size_bytes: int = Field(
        0,
        ge=0,
        description="Sum of all body_ref sizes for this upload (saturation accounting).",
    )
    body_discarded_at: datetime | None = Field(
        None,
        description=(
            "When the reaper deleted the body (per retention windows); None if body still present."
        ),
    )
    auth_blocked_host: str | None = Field(
        None,
        description=(
            "The host whose credential slot rejected this row, recorded when "
            "the sender parks it in ``auth_expired`` (D2/F6). AUTHORITATIVE "
            "only while the row is in ``auth_expired``, which is the only "
            "state either kicker reads it in; overwritten on every park, and "
            "inert history on a row a CAS writer (replay, cancel, "
            "mark_corrupted) moved out of that state. Distinct from "
            "``endpoint``, which is the FIRST step's host: the executor "
            "authenticates against the CURRENT step's host, so on a "
            "multi-host chain the two differ and only this one identifies "
            "the credential the row is actually waiting on. None on a row "
            "that has never parked on auth."
        ),
    )
    upstream_status_code: int | None = Field(
        None,
        description="Most recent upstream HTTP status.",
    )
    upstream_response_headers_json: str | None = Field(
        None,
        description="Most recent upstream response headers (JSON).",
    )
    last_step_completed: str | None = Field(
        None,
        description="Name of last terminal-success step; None if none yet.",
    )
    body_hashes: dict[str, BodyHashes] = Field(
        default_factory=dict,
        description=(
            "Per-body_ref hash pair, keyed by body_ref name. body_hash "
            "verifies end-to-end byte identity; storage_hash verifies "
            "storage integrity. Both are SHA-256 hex strings."
        ),
    )
