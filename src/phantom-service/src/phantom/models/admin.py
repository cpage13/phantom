"""Admin endpoint request/response shapes.

Every admin endpoint emits one of these Pydantic models. Bearer tokens
never appear in any of these models (ADR-004) — :class:`TokenSlot`
deliberately has no ``bearer`` field.

See plan §5.4 for the canonical surface.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from phantom.models.chain import CapturedStep, ChainState
from phantom.models.upload import UploadRow, UploadState

logger = logging.getLogger(__name__)

# HTTP methods Phantom accepts inside an envelope step. Pinned here so
# the admin projection (``ChainAdminDetail.project_from_envelope``) and
# the strict-Literal on ``ChainAdminStepDetail.method`` stay in lockstep
# — if a future ChainStep ever grows a new method, both update here.
_ALLOWED_METHODS: frozenset[str] = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


class ChainAdminStepDetail(BaseModel):
    """One step's request-envelope projection for the admin GET surface.

    The persisted ChainStep carries method/url/headers/body plus
    capture directives; this projection drops capture directives and
    collapses the body to a single ``has_body`` flag (the body bytes
    are retrievable separately via ``GET /v1/admin/chains/{chain_id}/body``
    when the step's body is a ``body_ref`` and via the
    ``chain_envelope_json`` round-trip when inline). The goal is to
    answer the operator question "what did the producer send?" without
    re-shipping payload bytes through the metadata response.
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
    """Admin-only chain detail — extends ChainResponse with inspection fields.

    Returned only by ``GET /v1/admin/chains/{chain_id}``. Loopback per
    ADR-004. The wire-facing :class:`ChainResponse` stays unchanged;
    this additional model exists so admin tools (and E2E test helpers)
    can inspect ``body_location`` state without changing the SDK
    contract or perturbing the byte-for-byte chain-model drift
    detection in :file:`tests/contract/test_chain_models_alignment.py`.

    Round-2 extension (defender): the persisted request envelope is now
    surfaced via :attr:`metadata` and :attr:`steps`. The user's explicit
    requirement is "if it is ever persisted, all that metadata is also
    retrievable with the files and the body". The metadata KVS comes
    from the create-file step's JSON body (``step.body.value.metadata.
    key_value_store``); the per-step request envelope is surfaced
    structurally rather than via raw JSON so admin tooling can render
    it without re-parsing the chain envelope.

    The pre-Phase-1
    ``tier`` + ``committed`` fields collapsed into the single
    ``body_location`` discriminator. ``ram`` corresponds to bodies the
    RAM body store holds; ``file`` to bodies the file body store holds
    (PersistController flipped the column after fsync). The
    ``committed`` durability marker is gone — atomic admission +
    fsync-before-flip ordering make every persisted body durable by
    construction.

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
    body_location: Literal["ram", "file"] = Field(
        ...,
        description=(
            "Which body store currently holds the chain's body bytes. "
            "'ram' = the RAM body store; 'file' = the file body store "
            "(PersistController flipped after fsync per plan § 0.5 "
            "invariant #6). Replaces the pre-Phase-1 tier + committed "
            "pair."
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
            "envelope's create-file step (``step.body.value.metadata."
            "key_value_store``). Empty dict when no step carries an inline "
            "JSON body with a ``metadata.key_value_store`` shape (e.g., "
            "purely body-ref chains). Surfaced byte-equal to what the producer "
            "submitted."
        ),
    )
    steps: list[ChainAdminStepDetail] = Field(
        default_factory=list,
        description=(
            "Per-step request envelope projection. Each entry exposes "
            "method, url, headers, and has_body for the step as submitted. "
            "Empty list only when the envelope failed to round-trip "
            "(which would itself be a defect)."
        ),
    )

    @classmethod
    def project_from_envelope(
        cls,
        envelope_json: str,
    ) -> tuple[dict[str, str], list[ChainAdminStepDetail]]:
        """Project the persisted chain envelope for the admin GET surface.

        The persisted envelope is the validated :class:`ChainEnvelope`
        JSON (round-tripped through Pydantic at ingress). This helper
        extracts:

        - ``metadata`` — the first JSON-body step's
          ``body.value.metadata.key_value_store`` (string-to-string) when
          present; empty dict otherwise. An upstream create-file step
          puts the KVS here; pure body-ref chains have no KVS so the
          dict is empty.
        - ``steps`` — a :class:`ChainAdminStepDetail` per envelope step
          preserving name/method/url/headers and exposing
          ``has_body = step.body is not None``.

        Defensive parse: if the JSON is malformed (which would be a
        cross-cutting defect, not a normal admin condition), return
        empty structures rather than 500'ing. The operator can still
        inspect the raw row via ``list_uploads``.

        Args:
            envelope_json: The persisted ``chain_envelope_json`` column
                value from the upload row.

        Returns:
            A pair ``(metadata, steps)``.
        """
        try:
            parsed: Any = json.loads(envelope_json)
        except json.JSONDecodeError:
            logger.warning(
                "chain_envelope_json failed to parse; returning empty projection",
            )
            return {}, []
        if not isinstance(parsed, dict):
            return {}, []
        metadata = cls._extract_metadata_kvs(parsed)
        steps = cls._extract_step_projections(parsed)
        return metadata, steps

    @staticmethod
    def _extract_metadata_kvs(envelope: dict[str, Any]) -> dict[str, str]:
        """Pull the metadata KVS out of the first JSON-bodied step.

        Some upstreams put the KVS on the create-file step under
        ``body.value.metadata.keyValueStore`` — that create-file body is
        serialized with the upstream client's camelCase alias generator
        (the upstream client calls ``model_dump(by_alias=True,
        mode="json")``). For envelopes that came in already-snake-cased
        we also fall back to ``key_value_store``. Non-string values are
        skipped to keep the admin response strictly typed.
        """
        raw_steps = envelope.get("steps", [])
        if not isinstance(raw_steps, list):
            return {}
        for step in raw_steps:
            if not isinstance(step, dict):
                continue
            body = step.get("body")
            if not isinstance(body, dict):
                continue
            if body.get("kind") != "json":
                continue
            value = body.get("value")
            if not isinstance(value, dict):
                continue
            metadata = value.get("metadata")
            if not isinstance(metadata, dict):
                continue
            # The upstream convention uses camelCase on the wire.
            kvs = metadata.get("keyValueStore")
            if not isinstance(kvs, dict):
                kvs = metadata.get("key_value_store")
            if isinstance(kvs, dict):
                return {k: v for k, v in kvs.items() if isinstance(k, str) and isinstance(v, str)}
        return {}

    @staticmethod
    def _extract_step_projections(
        envelope: dict[str, Any],
    ) -> list[ChainAdminStepDetail]:
        """Project envelope steps into the admin-facing per-step detail list.

        Each step's headers dict is copied (string keys, string values)
        and ``has_body`` reflects whether the step declared any body
        discriminator. Unknown / malformed steps are skipped silently —
        the admin projection is best-effort; the underlying row is still
        intact and recoverable.
        """
        raw_steps = envelope.get("steps", [])
        if not isinstance(raw_steps, list):
            return []
        projections: list[ChainAdminStepDetail] = []
        for step in raw_steps:
            if not isinstance(step, dict):
                continue
            name = step.get("name")
            method = step.get("method")
            url = step.get("url")
            if not (isinstance(name, str) and isinstance(method, str) and isinstance(url, str)):
                continue
            if method not in _ALLOWED_METHODS:
                continue
            raw_headers = step.get("headers", {})
            headers: dict[str, str] = {}
            if isinstance(raw_headers, dict):
                headers = {
                    k: v
                    for k, v in raw_headers.items()
                    if isinstance(k, str) and isinstance(v, str)
                }
            has_body = step.get("body") is not None
            projections.append(
                ChainAdminStepDetail(
                    name=name,
                    # _ALLOWED_METHODS guards the Literal narrowing.
                    method=method,  # type: ignore[arg-type]
                    url=url,
                    headers=headers,
                    has_body=has_body,
                )
            )
        return projections


class GroupMember(BaseModel):
    """One upload that belongs to a query group."""

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
    """Synthesized rollup for one query group."""

    model_config = ConfigDict(strict=True, extra="forbid")

    group_id: UUID = Field(
        ..., description="The query group this rollup describes (the path parameter)."
    )
    total: int = Field(..., ge=0, description="Number of member uploads in the group.")
    counts_by_state: dict[ChainState, int] = Field(
        ...,
        description="Histogram of member states across the eight canonical ChainState values.",
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
    """The 'did my upload land' status projection for one upload."""

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
    """Result of an either-identifier lookup."""

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


class HealthResponse(BaseModel):
    """Liveness probe response.

    ``status`` is the liveness signal (always ``"ok"`` once the process
    is up). ``storage`` is the optional § 4D.2 storage-health signal: it
    reports whether every instance has writable durable storage, so an
    operator watching the liveness probe also sees a degraded-boot fault.
    This model is extras-tolerated by the contract gate (the SDK mirror
    uses ``extra="ignore"``), so the ``storage`` / ``storage_detail``
    fields are service-side additions that do not require an SDK change.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    status: Literal["ok"] = Field(..., description="Liveness signal.")
    version: str = Field(..., description="Phantom version string.")
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
    """Readiness probe response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    ready: bool = Field(..., description="True when DB open and accepting uploads.")
    detail: str | None = Field(
        None,
        description="Human-readable reason if not ready.",
    )


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
    """``GET /v1/admin/stats`` shape."""

    model_config = ConfigDict(strict=True, extra="forbid")

    in_flight: TierBreakdown = Field(
        ...,
        description="Aggregate count + bytes of in-flight rows (queued+attempting+auth_expired).",
    )
    by_state: StateBreakdown = Field(
        ...,
        description="Per-state count + bytes breakdown.",
    )
    body_location: dict[Literal["ram", "file"], TierBreakdown] = Field(
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


class InstanceSummary(BaseModel):
    """One instance's headline figures, included in the aggregate status."""

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(..., description="InstanceCfg.id.")
    host_prefixes: list[str] = Field(
        ...,
        description="The fnmatch host prefixes this instance accepts.",
    )
    refresh_strategy: Literal["wait", "ad_client_credentials"] = Field(
        ...,
        description="Selected refresh strategy plugin (ADR-001).",
    )
    in_flight: int = Field(
        ...,
        ge=0,
        description="Current in-flight row count for this instance.",
    )


class InstanceListResponse(BaseModel):
    """``GET /v1/admin/instances`` shape - the per-instance summary list.

    Envelope rather than a bare array, to match the prevailing admin-list
    convention (``GET /chains`` -> ``ListUploadsResponse``, ``GET /tokens``
    -> ``TokenListResponse``) and the SDK ``list_instances`` model (R-EX4).
    Wrapping in an object also leaves room to grow (e.g. a future cursor)
    without breaking the wire shape.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    instances: list[InstanceSummary] = Field(
        ...,
        description="Headline figures per configured instance.",
    )


class InstanceStatusResponse(BaseModel):
    """``GET /v1/admin/instances/{id}/status`` shape."""

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


class ResolvedDefaultsSummary(BaseModel):
    """Resolved-default values an operator can read back via admin.

    Echoes the saturation / storage / worker numbers the model
    validator picked from :func:`phantom.config.defaults.compute_defaults`
    output. Lets ``curl /v1/admin/status`` answer "what cap am I
    actually running under?" without parsing logs.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    max_in_flight: int = Field(
        ...,
        ge=0,
        description="Resolved saturation row-count cap.",
    )
    max_in_flight_bytes: int = Field(
        ...,
        ge=0,
        description="Resolved saturation byte cap.",
    )
    max_disk_bytes: int = Field(
        ...,
        ge=0,
        description="Resolved persisted-tier disk-usage ceiling.",
    )
    ram_ceiling_bytes: int = Field(
        ...,
        ge=0,
        description=(
            "Resolved RAM-tier body-store ceiling (post-rename from "
            "the pre-Phase-1 ``in_memory_max_bytes`` field; same "
            "semantic — bytes cap on the RamBodyStore)."
        ),
    )
    large_body_threshold_bytes: int = Field(
        ...,
        ge=0,
        description="Resolved large-body class size boundary.",
    )
    max_large_in_flight: int = Field(
        ...,
        ge=0,
        description="Resolved max concurrent in-flight large bodies.",
    )
    persist_body_size_threshold_bytes: int = Field(
        ...,
        ge=0,
        description="Resolved size-aware persist threshold.",
    )
    worker_count: int = Field(
        ...,
        ge=1,
        description="Resolved sender worker-pool size.",
    )
    observed_total_ram_bytes: int = Field(
        ...,
        ge=0,
        description="``psutil.virtual_memory().total`` observed at probe time.",
    )
    observed_free_disk_bytes: int = Field(
        ...,
        ge=0,
        description="``shutil.disk_usage(data_dir).free`` observed at probe time.",
    )
    observed_cpu_count: int = Field(
        ...,
        ge=1,
        description="``os.cpu_count() or 1`` observed at probe time.",
    )


class AdminStatusResponse(BaseModel):
    """``GET /v1/admin/status`` aggregate shape (ADR-007)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    ready: bool = Field(
        ...,
        description="True when every instance's storage layer is open and accepting writes.",
    )
    disk_usage_bytes: int = Field(
        ...,
        ge=0,
        description="Sum of all instances' disk-tier byte usage.",
    )
    total_backlog: int = Field(
        ...,
        ge=0,
        description="Sum of in-flight rows across every instance.",
    )
    instances: list[InstanceSummary] = Field(
        ...,
        description="Headline figures per instance.",
    )
    ad_reachability: Literal["reachable", "unreachable", "not_configured"] = Field(
        "not_configured",
        description=(
            "Set to reachable/unreachable when at least one instance "
            "configures ad_client_credentials."
        ),
    )
    resolved_defaults: ResolvedDefaultsSummary | None = Field(
        None,
        description=(
            "Resolved saturation/storage/worker defaults filled by the "
            "host probe at startup. Always populated when surfaced by "
            "the live admin route; ``None`` is permitted so unit tests "
            "can construct AdminStatusResponse without wiring a probe."
        ),
    )


class ListUploadsResponse(BaseModel):
    """``GET /v1/admin/chains`` shape — operator-detail rows + cursor."""

    model_config = ConfigDict(strict=True, extra="forbid")

    uploads: list[UploadRow] = Field(
        ...,
        description="The page of upload-row detail records.",
    )
    next_cursor: str | None = Field(
        None,
        description="Opaque cursor for pagination; null when no more pages.",
    )


class ExtractFilter(BaseModel):
    """``POST /v1/admin/chains/extract`` request body.

    The model is ``strict=True`` overall so typos (``extra="forbid"``) and
    type confusion on the string axes are rejected, but ``since`` and the
    ``chain_ids`` elements opt out of strict mode (``strict=False``). The
    reason is the WIRE path: FastAPI's ``Body()`` parses the request JSON
    to a ``dict`` and validates it via ``model_validate`` (the dict path),
    and under strict mode the dict path refuses to coerce a JSON string to
    a ``datetime`` / ``UUID`` (it demands native instances that JSON cannot
    carry). Relaxing only these two axes lets the documented filters be
    submitted as the natural ISO-string / uuid-string JSON while a
    MALFORMED string still raises a ``ValidationError`` -> 422 (R-EX1).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    state: UploadState | None = Field(None, description="Restrict to rows in this state.")
    route: str | None = Field(None, description="Restrict to rows whose resolved route matches.")
    since: datetime | None = Field(
        None,
        strict=False,
        description="Restrict to rows received at or after this UTC timestamp.",
    )
    chain_ids: list[Annotated[UUID, Field(strict=False)]] | None = Field(
        None, description="Restrict to this explicit list of chain_ids."
    )
    instance: str | None = Field(None, description="Restrict to a single instance's rows.")


class DeleteFilter(BaseModel):
    """``DELETE /v1/admin/chains`` request body. Rejects empty filter (ADR-004).

    ``since`` opts out of strict mode (``strict=False``) for the same
    wire reason as :class:`ExtractFilter.since`: the FastAPI dict path
    will not coerce a JSON string to a ``datetime`` under strict mode, so
    the documented since-window delete is otherwise un-submittable over
    HTTP. A malformed timestamp still raises a 422 (R-EX1).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    state: UploadState | None = Field(None, description="Restrict to rows in this state.")
    route: str | None = Field(None, description="Restrict to rows whose resolved route matches.")
    since: datetime | None = Field(
        None,
        strict=False,
        description="Restrict to rows received at or after this UTC timestamp.",
    )
    instance: str | None = Field(None, description="Restrict to a single instance's rows.")


class KeyValueMatchFilter(BaseModel):
    """Filter shape for ``?key_value_match=<key>:<value>`` lookups.

    Both fields require at least one character: an empty KVS key is not
    an addressable pair and an empty value match is meaningless. The
    constraint matches the SDK mirror byte-for-byte (ADR-012; round 2
    defender fix R2-1 re-aligned the drifted sides in the stricter
    direction).
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    key: str = Field(
        ...,
        min_length=1,
        description="Metadata key (under metadata.key_value_store).",
    )
    value: str = Field(
        ...,
        min_length=1,
        description="Required matching value.",
    )


class BulkDeleteResponse(BaseModel):
    """``DELETE /v1/admin/chains`` response shape."""

    model_config = ConfigDict(strict=True, extra="forbid")

    deleted: int = Field(..., ge=0, description="Number of rows deleted.")


class TokenSlot(BaseModel):
    """Admin-list view of a token cache slot. NO bearer field (ADR-004)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    endpoint: str = Field(
        ...,
        description="Upstream hostname (e.g., upstream.example.com).",
    )
    uid: str = Field(
        ...,
        description="The opaque caller-supplied identifier.",
    )
    last_updated: datetime = Field(
        ...,
        description="When this slot's bearer was last written.",
    )
    status: Literal["fresh", "bad", "unknown"] = Field(
        ...,
        description="Current freshness state (per ADR-003).",
    )


class TokenPushRequest(BaseModel):
    """``PUT /v1/admin/tokens/...`` request body."""

    model_config = ConfigDict(strict=True, extra="forbid")

    token: str = Field(
        ...,
        min_length=1,
        description="Bearer token value to store; never returned in any response.",
    )


class TokenListResponse(BaseModel):
    """``GET /v1/admin/tokens`` shape."""

    model_config = ConfigDict(strict=True, extra="forbid")

    tokens: list[TokenSlot] = Field(
        ...,
        description="Slot views of every (endpoint, uid) token cache entry (no bearer values).",
    )


# ---------------------------------------------------------------------------
# Plan § 4.2.5 — Observability admin response models.
# ---------------------------------------------------------------------------


class CounterValue(BaseModel):
    """One counter entry: name, description, and label_value → count map."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., description="Canonical metric name (e.g., invariant_violation_total).")
    description: str = Field(..., description="One-line human-readable description.")
    values: dict[str, int] = Field(
        ...,
        description=(
            "Label-value bucket → count. The empty-string bucket ('') holds the "
            "no-label total for unlabeled counters."
        ),
    )


class CountersResponse(BaseModel):
    """``GET /v1/admin/observability/counters`` shape."""

    model_config = ConfigDict(strict=True, extra="forbid")

    counters: list[CounterValue] = Field(
        ...,
        description="Every registered counter, with its current per-label counts.",
    )


class GaugeValue(BaseModel):
    """One gauge entry: name, description, and label_value → value map."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(..., description="Canonical metric name (e.g., saturation_balance).")
    description: str = Field(..., description="One-line human-readable description.")
    values: dict[str, float] = Field(
        ...,
        description=(
            "Label-value bucket → current value. The empty-string bucket is the no-label total."
        ),
    )


class GaugesResponse(BaseModel):
    """``GET /v1/admin/observability/gauges`` shape."""

    model_config = ConfigDict(strict=True, extra="forbid")

    gauges: list[GaugeValue] = Field(
        ...,
        description="Every registered gauge, with its current per-label values.",
    )


class RamPressureStatusResponse(BaseModel):
    """``GET /v1/admin/observability/ram_pressure`` shape (plan § 4.2.5)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    ram_body_store_bytes: int = Field(
        ...,
        ge=0,
        description="Current RamBodyStore.total_bytes() observation.",
    )
    ram_ceiling_bytes: int = Field(
        ...,
        ge=0,
        description="Configured BodyStoreCfg.ram_ceiling_bytes (the cap that triggers pressure).",
    )
    pending_migrations: int = Field(
        ...,
        ge=0,
        description="In-flight PersistController migrations (queue depth + active).",
    )
    persist_controller_queue_depth: int = Field(
        ...,
        ge=0,
        description="Current PersistController.enqueue queue size.",
    )


# ---------------------------------------------------------------------------
# Phase 4 § 5.2.5 — Quarantine inventory admin endpoint.
# ---------------------------------------------------------------------------


class QuarantineEntry(BaseModel):
    """One BACKUP (or one anomaly) surfaced via ``GET /v1/admin/quarantine``.

    Plan § 5.2.5 + cycle-7 seam 2. Backed by
    :func:`phantom.storage.integrity.list_quarantines`, which reads backup
    MANIFESTS: one entry per backup (keyed by :attr:`backup_id`), never one
    per loose artifact. An on-disk artifact no manifest claims surfaces as
    an ANOMALY entry (:attr:`anomaly` true, :attr:`backup_id` null) and is
    not restorable. Retention of quarantine artifacts is admin-initiated by
    default (operator removes when ready); no auto-aging in v1.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    backup_id: UUID | None = Field(
        ...,
        description=(
            "The backup's uuid identity (minted at creation; the key for "
            "``POST /v1/admin/quarantine/restore?backup_id=...``). ``null`` "
            "iff this entry is an anomaly (an unmanifested artifact has no "
            "identity and cannot be restored)."
        ),
    )
    reason: Literal["corrupted", "mode_switch"] = Field(
        ...,
        description=(
            "Why the backup was taken: ``corrupted`` (the integrity gate "
            "quarantined a failed-check DB + its body tree, or the body-less "
            "token-cache isolate) or ``mode_switch`` (an unsafe "
            "all_ram-over-populated-disk switch backed the live DB + body "
            "tree up before booting). A ``mode_switch`` backup is the only "
            "kind restorable via ``POST /v1/admin/quarantine/restore``. For "
            "an anomaly the reason is recovered from the artifact name for "
            "display only."
        ),
    )
    iso_display: str = Field(
        ...,
        description=(
            "The human-readable ``%Y%m%dT%H%M%SZ`` timestamp half of the "
            "artifact names. Display and sort material ONLY; the backup's "
            "identity is ``backup_id``."
        ),
    )
    db_path: str | None = Field(
        ...,
        description=(
            "Declared absolute path of the backup's DB artifact, or ``null`` "
            "for a body-only anomaly."
        ),
    )
    body_path: str | None = Field(
        ...,
        description=(
            "Declared absolute path of the backup's body-tree artifact, or "
            "``null`` for a body-less backup (token-cache isolate) or a "
            "db-only anomaly."
        ),
    )
    has_db: bool = Field(
        ...,
        description=(
            "Whether the DB artifact is present on disk right now (the "
            "restorability signal: a restore requires the DB half)."
        ),
    )
    has_body: bool = Field(
        ...,
        description="Whether the body-tree artifact is present on disk right now.",
    )
    bytes: int = Field(
        ...,
        ge=0,
        description=(
            "Total size in bytes of the artifacts present on disk (DB file "
            "size plus the body tree's recursive file-size sum)."
        ),
    )
    anomaly: bool = Field(
        ...,
        description=(
            "``true`` for an on-disk artifact no manifest claims: surfaced "
            "for the operator, never restorable. ``false`` for a real "
            "(manifested) backup."
        ),
    )


class QuarantineInventoryResponse(BaseModel):
    """``GET /v1/admin/quarantine`` shape (plan § 5.2.5 / cycle-7 seam 2)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    quarantines: list[QuarantineEntry] = Field(
        ...,
        description=(
            "One entry per BACKUP (plus flagged anomaly entries) under the "
            "targeted instance data root(s). Scoped to one instance when "
            "``?instance=`` is given, else aggregated across all instances. "
            "Empty list on a clean deployment."
        ),
    )


# ---------------------------------------------------------------------------
# § 1.5 / cycle-7 seam 2 - One-call admin restore of a mode-switch backup.
# The restore route addresses the backup by its identity as a QUERY
# parameter (``POST /v1/admin/quarantine/restore?backup_id=...``); there is
# no request-body model.
# ---------------------------------------------------------------------------


class QuarantineRestoreResponse(BaseModel):
    """``POST /v1/admin/quarantine/restore`` response (plan § 1.5)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    restored_db: str = Field(
        ...,
        description="Absolute path of the live DB after the backup was moved in.",
    )
    restored_body: str = Field(
        ...,
        description="Absolute path of the live body-store root after restore.",
    )
    interim_backup_db: str | None = Field(
        ...,
        description=(
            "Absolute path of the DB backed up from the live tree before the "
            "restore, or ``null`` when no live DB existed to back up."
        ),
    )
    interim_backup_body: str | None = Field(
        ...,
        description=(
            "Absolute path of the body tree backed up from the live tree "
            "before the restore, or ``null`` when no live body tree existed."
        ),
    )
    restart_required: bool = Field(
        ...,
        description=(
            "Always ``true`` in this design: the route STAGES the restore on "
            "disk; the running store keeps its old descriptor and will not "
            "serve the restored data until the operator restarts in a "
            "disk-backed mode."
        ),
    )
    detail: str = Field(
        ...,
        description="Human-readable instruction for the operator (e.g. restart steps).",
    )
