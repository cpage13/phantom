"""Upload-row, state-alias, and admin-status response models.

These mirror the shapes Phantom emits on the admin API:
- ``UploadRow`` - the row as returned by ``GET /v1/admin/chains/{chain_id}``
  and ``GET /v1/admin/chains``. ``extra="ignore"`` so unknown fields
  on the wire round-trip silently rather than failing the SDK; integration
  tests pin the documented field set.
- ``UploadState`` - alias for ``ChainState`` from
  :mod:`phantom_client.models.chain`. Re-exported here because the
  admin-side vocabulary uses "upload" while the wire vocabulary uses
  "chain"; both name the same set of states.
- ``TERMINAL_STATES`` - the SDK's default ``poll_until`` stopping
  condition. Includes ``auth_expired`` because no further attempt
  happens without external intervention (see §4.2 of the plan and
  ADR-011).
- ``SortKey`` - enum of valid ``sort`` query-param values.
- ``StatsResponse``, ``TokenSlot``, ``HealthResponse``,
  ``ReadyResponse`` - admin response payload shapes.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from phantom_client.models.chain import ChainState

# ---------------------------------------------------------------------------
# State alias + terminal set.
# ---------------------------------------------------------------------------

UploadState: TypeAlias = ChainState  # noqa: UP040 - match TypeAlias convention used by ChainState
"""Admin-API name for an upload's state. Same literal set as ChainState."""


TERMINAL_STATES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled", "stored", "corrupted", "auth_expired", "expired"}
)
"""SDK-facing 'no more work happens by itself' set.

The default stopping condition for :func:`phantom_client.poller.poll_until`.
Includes ``auth_expired`` because, from the SDK caller's standpoint, the
upload won't progress without external intervention (operator pushes a
fresh token via the admin API, ``ad_client_credentials`` mints one, or a
sibling request lands carrying a fresh ``Authorization`` header).
Includes ``corrupted`` (R6-5): it is a terminal ``ChainState`` (Phantom
never retries a body-verification failure), so a poll that excluded it
could only run to its deadline; this also matches
``poll_group_until_finished``, whose rollup counts ``corrupted``
members as finished.
Includes ``expired`` (ADR-032): truly terminal - the per-route
send-deadline elapsed, so the upload is dead, the body is released, and
it is never retried. Distinct from ``auth_expired``'s "stalled pending
external action": an ``expired`` row will never progress, by anyone.

Callers who need to poll *through* ``auth_expired`` - for example to
assert eventual delivery after kicking the row with a fresh token -
must override with a smaller set, typically
``frozenset({"succeeded", "failed"})``.
"""


# ---------------------------------------------------------------------------
# Sort key enum for list endpoints.
# ---------------------------------------------------------------------------


class SortKey(StrEnum):
    """Valid values for the ``sort`` query parameter on list endpoints."""

    NEXT_ATTEMPT_AT_ASC = "next_attempt_at_asc"
    NEXT_ATTEMPT_AT_DESC = "next_attempt_at_desc"
    RECEIVED_AT_DESC = "received_at_desc"


# ---------------------------------------------------------------------------
# Captured-values store - nested per step on each UploadRow.
# Duplicates phantom.models.upload.{CapturedStepValues,CapturedValues}
# byte-for-byte; the contract test enforces alignment.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Upload row - SDK view (extra="ignore" tolerates unknown fields).
# ---------------------------------------------------------------------------


class UploadRow(BaseModel):
    """SDK view of an upload row as emitted by Phantom's admin API.

    Uses ``extra="ignore"`` so unknown fields round-trip silently rather
    than as parse failures. Integration tests pin the documented field
    set; the round-trip exercise catches silent field-rename regressions.
    """

    model_config = ConfigDict(strict=False, extra="ignore")

    chain_id: UUID = Field(..., description="Upload identifier; the envelope's chain_id.")
    instance_id: str = Field(..., description="The instance.id this upload belongs to.")
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
    route_name: str = Field(..., description="Resolved RouteCfg.name for the chain's first step.")
    state: ChainState = Field(..., description="Current state in the upload state machine.")
    body_location: Literal["ram", "file"] = Field(
        ...,
        description=(
            "Which body store currently holds the row's body bytes "
            "(plan § 2.3.19/§2.3.20). Replaces the pre-Phase-1 "
            "tier + committed pair."
        ),
    )
    attempts: int = Field(0, ge=0, description="Number of attempts completed so far.")
    next_attempt_at: datetime | None = Field(
        None, description="When the sender will attempt this row next; null when terminal."
    )
    received_at: datetime = Field(..., description="When ingress accepted the upload.")
    sent_at: datetime | None = Field(
        None,
        description=(
            "When the upload was confirmed delivered upstream (UTC). "
            "Stamped once on confirmed delivery, never moved; survives "
            "replay. None until delivery."
        ),
    )
    updated_at: datetime = Field(..., description="When the row last transitioned.")
    last_error: str | None = Field(
        None, description="One-line summary of the most recent failure, if any."
    )
    endpoint: str = Field(..., description="The upstream hostname for the first step.")
    uid: str = Field(
        ...,
        description=(
            "The opaque X-Phantom-Uid received at ingress; the secondary "
            "axis of the (endpoint, uid) token-cache key (ADR-002)."
        ),
    )
    idempotency_key: str = Field(
        ..., description="Per-chain idempotency value forwarded to upstreams."
    )
    capture_reexecution_active: bool = Field(
        ...,
        description=(
            "Snapshot of the instance's capture_reexecution YAML knob taken at "
            "ingress. Bound to the row so mid-chain YAML edits do not change "
            "in-flight behavior. See ADR-011."
        ),
    )
    storage_encoding: str = Field(
        "original", description="Body codec (e.g., 'original', 'zstd', 'gzip')."
    )
    body_discarded_at: datetime | None = Field(
        None,
        description="When the body was dropped (after succeeded_body retention window).",
    )
    upstream_status_code: int | None = Field(
        None, description="HTTP status code from the most recent upstream attempt."
    )
    captured_values: CapturedValues = Field(
        default_factory=CapturedValues,
        description=(
            "Per-step captured-values store; populated as steps complete and "
            "kept for the row's succeeded_metadata_seconds retention window. "
            "Mirrors phantom.models.upload.UploadRow.captured_values."
        ),
    )
    last_step_completed: str | None = Field(
        None,
        description="Name of the most recent terminal-success step on this chain.",
    )


# ---------------------------------------------------------------------------
# Token slot - ADR-004 invariant: no bearer field ever.
# ---------------------------------------------------------------------------


class TokenSlot(BaseModel):
    """A token cache slot as seen by admin callers.

    The bearer value is NEVER present in admin responses (ADR-004 invariant).
    A separate regression test in phantom enforces that no admin endpoint
    body contains a substring matching the bearer-token regex.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    endpoint: str = Field(..., description="Upstream hostname (e.g., upstream.example.com).")
    uid: str = Field(..., description="The opaque caller-supplied identifier.")
    last_updated: datetime = Field(..., description="When the slot last received a write.")
    status: Literal["fresh", "bad", "unknown"] = Field(
        ...,
        description=(
            "Slot health. 'bad' means an upstream returned 401/403 against this "
            "bearer (per ADR-003 bad tokens are preserved, not deleted)."
        ),
    )


# ---------------------------------------------------------------------------
# Stats response - what /v1/admin/stats returns.
#
# The nested-component shapes (TierBreakdown, StateBreakdown,
# SaturationStatus, AuthStatus) duplicate phantom.models.admin so the SDK
# can stand alone; the contract test enforces byte-equality.
# ---------------------------------------------------------------------------


BodyLocation: TypeAlias = Literal["ram", "file"]  # noqa: UP040 - match TypeAlias convention
"""Which body store holds the row's body bytes - same literal set as
``phantom.models.upload.BodyLocation``. Replaces the pre-Phase-1 ``Tier``
alias (``memory`` / ``persisted``) per plan § 2.3.19 / § 2.3.20."""


class TierBreakdown(BaseModel):
    """A count + bytes pair for one storage tier or state."""

    model_config = ConfigDict(strict=True, extra="forbid")

    count: int = Field(..., ge=0, description="Number of rows in this slice.")
    bytes: int = Field(..., ge=0, description="Summed body bytes in this slice.")


class StateBreakdown(BaseModel):
    """Per-state count+bytes breakdown for the stats endpoint."""

    model_config = ConfigDict(strict=True, extra="forbid")

    queued: TierBreakdown = Field(..., description="Rows in the ``queued`` state.")
    attempting: TierBreakdown = Field(
        ..., description="Rows currently being attempted by the sender."
    )
    auth_expired: TierBreakdown = Field(
        ...,
        description="Rows parked waiting for a fresh token in the (endpoint, uid) cache.",
    )
    stored: TierBreakdown = Field(
        ...,
        description="Rows in ``stored`` (body recoverable, retry budget exhausted).",
    )
    succeeded_recent: TierBreakdown = Field(
        ...,
        description="Recently-succeeded rows still inside their retention window.",
    )
    failed_recent: TierBreakdown = Field(
        ...,
        description="Recently-failed rows still inside their retention window.",
    )


class SaturationStatus(BaseModel):
    """Current saturation cap state."""

    model_config = ConfigDict(strict=True, extra="forbid")

    max_in_flight: int = Field(..., ge=0, description="Current row-count cap.")
    max_in_flight_bytes: int = Field(..., ge=0, description="Current in-flight byte cap.")
    saturated: bool = Field(
        ...,
        description="True when either cap is at the limit and new submissions would be refused.",
    )


class AuthStatus(BaseModel):
    """Auth-cache summary for the stats endpoint."""

    model_config = ConfigDict(strict=True, extra="forbid")

    phantom_token_expires_at: datetime | None = Field(
        None,
        description=(
            "When configured ad_client_credentials' minted token expires; "
            "None when wait strategy is configured."
        ),
    )
    auth_expired_count: int = Field(
        ...,
        ge=0,
        description="Number of rows currently parked in ``auth_expired``.",
    )


class StatsResponse(BaseModel):
    """``GET /v1/admin/stats`` shape.

    Mirrors :class:`phantom.models.admin.StatsResponse` byte-for-byte;
    enforced by ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    in_flight: TierBreakdown = Field(
        ...,
        description="Aggregate count + bytes of in-flight rows (queued+attempting+auth_expired).",
    )
    by_state: StateBreakdown = Field(
        ...,
        description="Per-state count + bytes breakdown.",
    )
    body_location: dict[BodyLocation, TierBreakdown] = Field(
        ...,
        description=(
            "Per-body-location (ram / file) count + bytes breakdown. "
            "Replaces the pre-Phase-1 'tier' dict per plan § 2.3.19."
        ),
    )
    saturation: SaturationStatus = Field(
        ...,
        description="Current saturation cap state.",
    )
    auth: AuthStatus = Field(
        ...,
        description="Auth-cache summary (token expiry, auth_expired count).",
    )
    parked_total: int = Field(
        ...,
        ge=0,
        description=(
            "Count of parked rows: stored (retry budget exhausted, body recoverable) "
            "plus auth_expired (waiting for a token)."
        ),
    )


# ---------------------------------------------------------------------------
# Health and readiness shapes - small but uniform.
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    """Response payload for ``GET /v1/healthz`` (R12-1: public liveness).

    Mirrors the service's :class:`phantom.models.admin.HealthResponse`.
    ``status`` is the liveness signal; ``storage`` is the § 4D.2
    storage-health signal ('ok' when every instance has writable durable
    storage, 'degraded' otherwise) with ``storage_detail`` naming the
    degraded instance(s). The model tolerates extra fields
    (``extra="ignore"``) so an older SDK still decodes a newer service
    response; the fields are mirrored here so SDK consumers can read the
    storage status rather than have it silently dropped.
    """

    model_config = ConfigDict(strict=True, extra="ignore")

    status: Literal["ok"] = Field("ok", description="Always 'ok' when the process is up.")
    version: str = Field(..., description="Phantom service version.")
    storage: Literal["ok", "degraded"] = Field(
        "ok",
        description=(
            "Storage health (§ 4D.2). 'ok' when every instance has writable "
            "durable storage; 'degraded' when one or more instances booted with "
            "an unwritable data_dir (no durable buffering for that instance)."
        ),
    )
    storage_detail: str | None = Field(
        None,
        description=(
            "Human-readable storage fault naming the degraded instance(s) when "
            "'storage' is 'degraded'; None when storage is 'ok'."
        ),
    )


class ReadyResponse(BaseModel):
    """Response payload for ``GET /v1/readyz`` (R12-1: public readiness).

    Matches the service's :class:`phantom.models.admin.ReadyResponse`:
    a boolean readiness flag plus an optional human-readable detail
    string. The duplication is enforced by
    ``tests/contract/test_admin_models_alignment.py``.
    """

    model_config = ConfigDict(strict=True, extra="ignore")

    ready: bool = Field(..., description="True when DB open and accepting uploads.")
    detail: str | None = Field(
        None,
        description="Human-readable reason if not ready.",
    )


__all__ = [
    "TERMINAL_STATES",
    "AuthStatus",
    "BodyLocation",
    "CapturedStepValues",
    "CapturedValues",
    "HealthResponse",
    "ReadyResponse",
    "SaturationStatus",
    "SortKey",
    "StateBreakdown",
    "StatsResponse",
    "TierBreakdown",
    "TokenSlot",
    "UploadRow",
    "UploadState",
]
