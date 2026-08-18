"""Admin endpoint router (loopback, no auth — ADR-004).

Plan § 2.3.19.
-------------------------------------------

The pre-Phase-1 admin surface branched on ``row.tier`` to pick the
matching upload store + body store half (11+ tier-discriminating
sites). Every site is collapsed to the single
:attr:`InstanceContext.store` + :attr:`InstanceContext.body_store`
references (the dual-store carry is gone from InstanceContext per
F-Slice1D-B). Body-location surfacing in responses uses the new
``body_location`` discriminator on :class:`UploadRow` —
:class:`ChainAdminDetail` carries ``body_location: Literal['ram',
'file']`` instead of ``tier`` + ``committed``.

Operator-facing filter: ``list_uploads`` accepts an optional
``body_location`` query parameter that the underlying store filter
respects.
"""

from __future__ import annotations

import importlib.metadata
import io
import json
import logging
import tarfile
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Final, Literal, get_args
from uuid import UUID

from fastapi import APIRouter, Body, Depends, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import StreamingResponse

from phantom.config.settings import AdminLookupCfg
from phantom.instances.context import InstanceContext, instance_storage_paths
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.admin import (
    AdminStatusResponse,
    AuthStatus,
    BulkDeleteResponse,
    ChainAdminDetail,
    CountersResponse,
    CounterValue,
    DeleteFilter,
    ExtractFilter,
    GaugesResponse,
    GaugeValue,
    GroupMember,
    GroupStatusResponse,
    IdentifierLookupResponse,
    InstanceListResponse,
    InstanceStatusResponse,
    InstanceSummary,
    ListUploadsResponse,
    QuarantineEntry,
    QuarantineInventoryResponse,
    QuarantineRestoreResponse,
    RamPressureStatusResponse,
    ResolvedDefaultsSummary,
    SaturationStatus,
    StateBreakdown,
    StatsResponse,
    TierBreakdown,
    TokenListResponse,
    TokenPushRequest,
    UploadStatusSummary,
)
from phantom.models.chain import ChainState
from phantom.models.credential import (
    CredentialPushBody,
    HostCredKey,
    credential_body_to_internal,
)
from phantom.models.errors import STATUS_FOR_CODE, ErrorCode, error_response
from phantom.models.upload import BodyLocation, UploadRow, UploadState
from phantom.observability.metrics import MetricsRegistry
from phantom.routes._version import ADMIN_ROUTER_PREFIX
from phantom.routing import host_key_for
from phantom.runtime.reload import RELOAD_FAILURE_ERRORS, apply_reload
from phantom.storage.errors import ReplayBodyDiscardedError, ReplayRefusedAttemptingError
from phantom.storage.integrity import (
    list_quarantines,
    load_backup_manifest,
    quarantine,
    restore_mode_switch_backup,
)
from phantom.storage.interface import StateTally
from phantom.storage.sqlite_store import PHANTOM_LOCAL_UUID_METADATA_KEY
from phantom.workers.saturation import (
    AdmissionGranted,
    SlotDelta,
    SlotReservation,
    row_holds_slot,
)

logger = logging.getLogger(__name__)

# The two states in which a group member is still moving. The group
# rollup's ``all_finished`` is the structural rule "no member is in this
# set" (NOT a terminal-set membership test: the storage and SDK terminal
# sets differ by corrupted/auth_expired, and both count as finished for
# the client because neither progresses without intervention).
_STILL_MOVING_STATES: Final[frozenset[ChainState]] = frozenset({"queued", "attempting"})

# Per-instance cap on rows packed into a single ``export.tar`` response.
# A deployed instance in the field accumulates at most a few thousand
# buffered bodies before manual intervention; this cap protects the
# in-memory tar builder from runaway buffer growth without losing
# real-world coverage. Operators needing more granular slices should use the
# ``state`` / ``route`` filter to scope the export.
_EXPORT_TAR_PER_INSTANCE_LIMIT = 10_000


def _admin_error(
    code: ErrorCode,
    message: str,
    *,
    instance_id: str = "unrouted",
    details: dict[str, Any] | None = None,
) -> Response:
    """Build a canonical :class:`ErrorEnvelope`-shaped admin response.

    Admin endpoints share Phantom's standard error envelope so the SDK
    can decode them via the same ``EXCEPTION_FOR_CODE`` machinery used
    for ingress errors. FastAPI's default ``HTTPException`` emits
    ``{"detail": ...}``; here we emit ``{"error": {...}}`` per plan
    §5.6 + ADR-010.

    Args:
        code: The stable ADR-010 error code (selects the HTTP status).
        message: Human-readable explanation for the operator.
        instance_id: The instance the error concerns, or ``"unrouted"``.
        details: Optional code-specific context surfaced in the envelope's
            ``error.details`` (the ADR-017 ``details`` shape for the code).
    """
    envelope = error_response(
        code,
        message,
        instance_id=instance_id,
        request_id="",
        details=details,
    )
    return Response(
        content=envelope.model_dump_json(),
        media_type="application/json",
        status_code=STATUS_FOR_CODE[code],
    )


def get_dispatcher() -> InstanceDispatcher:
    """Dependency placeholder — wired by the composition root."""
    raise NotImplementedError("InstanceDispatcher dependency must be overridden by app factory")


def get_version() -> str:
    """Phantom version string — overridden by the composition root."""
    return "0.1.0"


def get_resolved_defaults_summary() -> ResolvedDefaultsSummary | None:
    """Resolved-defaults summary — overridden by the composition root.

    Returns ``None`` in tests or partial wirings where the summary
    isn't built. Production always wires a concrete summary via
    :func:`phantom.app.create_app`.
    """
    return None


def get_metrics_registry() -> MetricsRegistry:
    """Process-wide :class:`MetricsRegistry` — wired by the composition root.

    Plan § 4.2.5. The composition root (``app.py.create_app``) overrides
    this dependency with ``lambda: app.state.metrics_registry``; admin
    observability endpoints depend on it to serialize the registry into
    JSON. Tests that exercise the endpoints may either rely on the
    create_app override path or pass their own override via the FastAPI
    test client's dependency overrides.
    """
    raise NotImplementedError("MetricsRegistry dependency must be overridden by app factory")


def get_data_root() -> Path:
    """Top-level ``Settings.storage.data_dir`` — wired by app factory.

    Plan § 5.2.5 / § 1.4 / § 1.5. This is the TOP-LEVEL data dir; the
    quarantine inventory and restore routes combine it with each instance's
    ``cfg.data_dir`` (via :func:`instance_storage_paths`) to reach the
    per-instance ``data_root`` where artifacts actually live (Finding F-1).
    Overridden in ``create_app`` with the resolved ``Settings.storage.data_dir``
    value.
    """
    raise NotImplementedError("data_root dependency must be overridden by app factory")


router = APIRouter(prefix=ADMIN_ROUTER_PREFIX)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
#
# Liveness (``GET /v1/healthz``) and readiness (``GET /v1/readyz``) moved
# to the PUBLIC health router (:mod:`phantom.routes.health`) in R12-1:
# this admin router now serves only the loopback-bound admin surface, so
# the orchestrator probe paths had to move to the public listener (the
# only surface a remote probe reaches by default). Their dependency
# placeholder ``get_degraded_instances`` and the ``_degraded_detail``
# helper moved with them.


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    instance: str | None = Query(None),
) -> StatsResponse:
    """Aggregate counters across instances (optionally scoped)."""
    targets = _scope_instances(dispatcher, instance)
    return await _aggregate_stats(targets)


async def _aggregate_stats(targets: list[InstanceContext]) -> StatsResponse:
    """Aggregate stats across the given instance list."""
    in_flight = TierBreakdown(count=0, bytes=0)
    by_state = StateBreakdown(
        queued=TierBreakdown(count=0, bytes=0),
        attempting=TierBreakdown(count=0, bytes=0),
        auth_expired=TierBreakdown(count=0, bytes=0),
        stored=TierBreakdown(count=0, bytes=0),
        succeeded_recent=TierBreakdown(count=0, bytes=0),
        failed_recent=TierBreakdown(count=0, bytes=0),
    )
    # Plan § 2.3.19: tier → body_location collapse.
    # Every row carries body_location ∈ {'ram', 'file'} (the source of
    # truth for which BodyStore holds its bytes).
    body_location_map: dict[BodyLocation, TierBreakdown] = {
        "ram": TierBreakdown(count=0, bytes=0),
        "file": TierBreakdown(count=0, bytes=0),
    }
    auth_expired_count = 0
    # ``stored`` is terminal, so the non-terminal loop below never sees it
    # and ``by_state.stored`` would stay structurally zero. We add a
    # second, read-only GROUP BY aggregate (counts_by_state) and consume
    # ONLY its ``stored`` entry here — the non-terminal entries, in_flight,
    # and body_location keep coming from the loop, so the loop is not
    # redundant. The two reads (auth_expired from the loop, stored from
    # the aggregate) are eventually-consistent by design; admin stats
    # tolerate the small skew under concurrent writes, so they are NOT
    # forced into one transaction.
    stored_tally = StateTally(count=0, bytes=0)
    for ctx in targets:
        rows = await ctx.store.list_non_terminal()
        for row in rows:
            body_location_map[row.body_location] = TierBreakdown(
                count=body_location_map[row.body_location].count + 1,
                bytes=body_location_map[row.body_location].bytes + row.body_size_bytes,
            )
            field = getattr(by_state, row.state, None)
            if isinstance(field, TierBreakdown):
                setattr(
                    by_state,
                    row.state,
                    TierBreakdown(count=field.count + 1, bytes=field.bytes + row.body_size_bytes),
                )
            if row.state in {"queued", "attempting", "auth_expired"}:
                in_flight = TierBreakdown(
                    count=in_flight.count + 1, bytes=in_flight.bytes + row.body_size_bytes
                )
            if row.state == "auth_expired":
                auth_expired_count += 1
        tallies = await ctx.store.counts_by_state()
        stored_this = tallies.get("stored", StateTally(count=0, bytes=0))
        stored_tally = StateTally(
            count=stored_tally.count + stored_this.count,
            bytes=stored_tally.bytes + stored_this.bytes,
        )
    by_state.stored = TierBreakdown(count=stored_tally.count, bytes=stored_tally.bytes)
    # Parked = the operator-owned non-success backlog: ``stored`` (terminal,
    # body recoverable) plus ``auth_expired`` (waiting for a token).
    parked_total = stored_tally.count + auth_expired_count
    saturated = any(ctx.saturation.saturated for ctx in targets)
    max_in_flight = sum(ctx.saturation.max_in_flight for ctx in targets)
    max_in_flight_bytes = sum(ctx.saturation.max_in_flight_bytes for ctx in targets)
    return StatsResponse(
        in_flight=in_flight,
        by_state=by_state,
        body_location=body_location_map,
        saturation=SaturationStatus(
            max_in_flight=max_in_flight,
            max_in_flight_bytes=max_in_flight_bytes,
            saturated=saturated,
        ),
        auth=AuthStatus(phantom_token_expires_at=None, auth_expired_count=auth_expired_count),
        parked_total=parked_total,
    )


# ---------------------------------------------------------------------------
# Per-instance & aggregate status (ADR-007)
# ---------------------------------------------------------------------------


# The identifier this implementation reports in GET /v1/admin/status.
# A ported implementation (e.g. phantom-go) reports its own value, so an
# operator can always tell which binary answered.
_IMPLEMENTATION_ID: str = "phantom-python"
# The installed distribution whose version the status route reports.
_SERVICE_DISTRIBUTION: str = "phantom-service"


def _service_version() -> str:
    """Resolve the installed phantom-service distribution version.

    Falls back to ``"unknown"`` when the distribution metadata is absent
    (an unpackaged source checkout), rather than failing the status route.
    """
    try:
        return importlib.metadata.version(_SERVICE_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


@router.get("/status", response_model=AdminStatusResponse)
async def get_admin_status(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    resolved_defaults: Annotated[
        ResolvedDefaultsSummary | None,
        Depends(get_resolved_defaults_summary),
    ],
) -> AdminStatusResponse:
    """Aggregate admin status."""
    summaries: list[InstanceSummary] = []
    total_backlog = 0
    total_disk_bytes = 0
    for ctx in dispatcher.all_instances():
        rows = await ctx.store.list_non_terminal()
        summaries.append(
            InstanceSummary(
                id=ctx.cfg.id,
                host_prefixes=list(ctx.cfg.host_prefixes),
                refresh_strategy="ad_client_credentials" if ctx.minter is not None else "wait",
                in_flight=ctx.saturation.in_flight,
            )
        )
        total_backlog += len(rows)
        # Sum body-file bytes per instance — see §6.1. Reads the store's
        # running total (CL6), seeded by one walk at boot and maintained by
        # the two writers, so the request does no filesystem work.
        total_disk_bytes += await ctx.file_body_store.total_bytes()
    return AdminStatusResponse(
        ready=True,
        disk_usage_bytes=total_disk_bytes,
        total_backlog=total_backlog,
        instances=summaries,
        ad_reachability="not_configured",
        resolved_defaults=resolved_defaults,
        implementation=_IMPLEMENTATION_ID,
        service_version=_service_version(),
    )


@router.get("/instances", response_model=InstanceListResponse)
async def list_instances(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> InstanceListResponse:
    """List all configured instances.

    Returns an ``{"instances": [...]}`` envelope (not a bare array) to
    match the SDK ``list_instances`` model and the prevailing admin-list
    convention shared by ``/chains`` and ``/tokens`` (R-EX4).
    """
    out: list[InstanceSummary] = []
    for ctx in dispatcher.all_instances():
        out.append(
            InstanceSummary(
                id=ctx.cfg.id,
                host_prefixes=list(ctx.cfg.host_prefixes),
                refresh_strategy="ad_client_credentials" if ctx.minter is not None else "wait",
                in_flight=ctx.saturation.in_flight,
            )
        )
    return InstanceListResponse(instances=out)


@router.get("/instances/{instance_id}/status", response_model=InstanceStatusResponse)
async def get_instance_status(
    instance_id: str,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> InstanceStatusResponse:
    """Per-instance admin status.

    Raises 421 ``instance_unknown`` when the named instance is not
    configured (plan §5.6 error table); the app-level handler converts
    that into a canonical ``ErrorEnvelope`` response.
    """
    ctx = dispatcher.by_id(instance_id)
    if ctx is None:
        raise UnknownInstanceError(instance_id)
    stats = await _aggregate_stats([ctx])
    return InstanceStatusResponse(
        id=ctx.cfg.id,
        ready=True,
        in_flight=stats.in_flight,
        by_state=stats.by_state,
        auth=stats.auth,
        # See §6.1 — per-instance body-file bytes via the existing store helper.
        disk_usage_bytes=await ctx.file_body_store.total_bytes(),
        degraded_durability=False,
    )


# ---------------------------------------------------------------------------
# Uploads
# ---------------------------------------------------------------------------


def _parse_key_value_match(raw: str) -> tuple[str, str]:
    """Parse one ``?key_value_match=`` wire value into its ``(key, value)`` pair.

    Two forms (round 2 defender fix R2-3):

    * Plain form, the established wire format: split at the FIRST
      colon, so plain keys pair with colon-bearing values unchanged
      (``ts:12:30:00`` is key ``ts``, value ``12:30:00``).
    * Quoted-key form, for keys the plain form cannot address: a value
      beginning with ``"`` carries the key as a double-quoted string
      with backslash escapes for ``"`` and ``\\``, then a colon, then
      the value (``"tag:env":prod`` is key ``tag:env``,
      value ``prod``). The SDK emits this form automatically when the
      key needs it.

    Key and value must both be non-empty (an empty KVS key is not an
    addressable pair and an empty value match is meaningless; the same
    ruling as the aligned ``KeyValueMatchFilter`` min_length=1
    contract, R2-1).

    Returns:
        The decoded ``(key, value)`` pair.

    Raises:
        KeyValueMatchInvalidError: When ``raw`` parses under neither
            form (the handler answers the canonical 422
            ``key_value_match_invalid`` envelope).
    """
    if raw.startswith('"'):
        key_chars: list[str] = []
        index = 1
        closed = False
        while index < len(raw):
            char = raw[index]
            if char == "\\":
                follower = raw[index + 1] if index + 1 < len(raw) else None
                if follower not in ('"', "\\"):
                    raise KeyValueMatchInvalidError(
                        raw=raw,
                        reason=('bad escape in the quoted key (only \\" and \\\\ are defined)'),
                    )
                key_chars.append(follower)
                index += 2
                continue
            if char == '"':
                closed = True
                index += 1
                break
            key_chars.append(char)
            index += 1
        if not closed:
            raise KeyValueMatchInvalidError(
                raw=raw, reason="the quoted key is missing its closing quote"
            )
        if index >= len(raw) or raw[index] != ":":
            raise KeyValueMatchInvalidError(
                raw=raw, reason="expected ':' immediately after the quoted key"
            )
        key = "".join(key_chars)
        value = raw[index + 1 :]
    else:
        if ":" not in raw:
            raise KeyValueMatchInvalidError(
                raw=raw, reason="the value must be 'key:value' (no colon found)"
            )
        key, value = raw.split(":", 1)
    if not key:
        raise KeyValueMatchInvalidError(raw=raw, reason="the key must be non-empty")
    if not value:
        raise KeyValueMatchInvalidError(raw=raw, reason="the value must be non-empty")
    return key, value


@router.get("/chains", response_model=ListUploadsResponse)
async def list_uploads(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    state: UploadState | None = Query(None),  # noqa: B008
    route: str | None = Query(None),
    multifile_id: UUID | None = Query(None),  # noqa: B008
    group_id: UUID | None = Query(None),  # noqa: B008
    since: datetime | None = Query(None),  # noqa: B008
    limit: int = Query(100, ge=1, le=1000),
    cursor: str | None = Query(None),
    instance: str | None = Query(None),
    key_value_match: str | None = Query(
        None,
        description=(
            "Match rows whose metadata key-value store carries this "
            "pair, written 'key:value' and split at the FIRST colon "
            "(so values may contain colons). A key that itself "
            "contains a colon, or begins with a double quote, rides "
            "the quoted-key form '\"<key>\":<value>' with backslash "
            "escapes for '\"' and '\\' inside the key (the SDK "
            "encodes this automatically). Key and value must both be "
            "non-empty; anything unparseable refuses with the 422 "
            "key_value_match_invalid envelope."
        ),
    ),
    body_location: Literal["ram", "file"] | None = Query(None),
) -> ListUploadsResponse:
    """List chains with filters + cursor pagination.

    Single-store pagination (plan § 2.3.19) —
    the pre-Phase-1 dual-store fan-out is gone with the schema
    collapse. The cursor opaquely carries the underlying store's
    continuation token.

    ``body_location`` query parameter scopes by which body store
    currently holds the chain's bytes (``ram`` vs ``file``).

    The ``key_value_match`` branch is a one-shot lookup (not paginated)
    because the underlying store helper returns at most ``limit`` rows
    per source with no continuation token. Its wire format is parsed by
    :func:`_parse_key_value_match`: the established first-colon split
    for plain keys, the quoted-key form for keys containing a colon
    (round 2 defender fix R2-3; see the query parameter description).

    The ``multifile_id`` filter returns the set ordered by
    ``send_order`` (recorded position); it is one-shot like
    ``key_value_match`` (a multi-file set is producer-scale small), so
    combining it with ``cursor`` is a 422 ``multifile_cursor_conflict``
    envelope and ``next_cursor`` is always null on its responses.
    ``group_id`` filters by the query-grouping handle and paginates
    like every other filter.

    Raises:
        MultifileCursorConflictError: When ``multifile_id`` is combined
            with ``cursor`` (422 ``multifile_cursor_conflict``
            envelope).
        KeyValueMatchInvalidError: When ``key_value_match`` does not
            parse (422 ``key_value_match_invalid`` envelope).
    """
    targets = _scope_instances(dispatcher, instance)
    if multifile_id is not None and cursor is not None:
        raise MultifileCursorConflictError(multifile_id=multifile_id, cursor=cursor)
    if key_value_match is not None:
        key, value = _parse_key_value_match(key_value_match)
        kv_rows: list[UploadRow] = []
        for ctx in targets:
            kv_rows.extend(await ctx.store.list_by_key_value(key, value, limit=limit))
        if body_location is not None:
            kv_rows = [r for r in kv_rows if r.body_location == body_location]
        return ListUploadsResponse(uploads=kv_rows[:limit], next_cursor=None)

    # Fan out across instances; the per-store helper handles the cursor.
    # Multi-instance pagination would need an outer cursor; today every
    # production deployment runs a single instance per Phantom process
    # (see CONTEXT.md "Topology and storage"), so the one-instance
    # path is the load-bearing one — the multi-instance fan-out
    # concatenates the per-instance pages.
    all_rows: list[UploadRow] = []
    next_cursor: str | None = None
    for ctx in targets:
        chunk, store_next = await ctx.store.list_uploads(
            state=state,
            route=route,
            multifile_id=multifile_id,
            group_id=group_id,
            since=since,
            limit=limit,
            cursor=cursor,
            instance=instance,
        )
        all_rows.extend(chunk)
        # Carry the last non-None cursor (single-instance: this is the
        # store's continuation; multi-instance: the last instance's
        # continuation, which the client uses on the next request).
        if store_next is not None:
            next_cursor = store_next

    if body_location is not None:
        all_rows = [r for r in all_rows if r.body_location == body_location]

    # Sort merged result for determinism across the multi-instance
    # concatenation. Default: matches the per-store ``ORDER BY
    # received_at ASC, chain_id ASC`` so the merge is monotone. With the
    # multifile filter the per-store order is ``send_order ASC``; the
    # merge must preserve it, with receipt time and chain_id as
    # deterministic tiebreaks.
    if multifile_id is not None:
        all_rows.sort(key=lambda r: (r.send_order, r.received_at, r.chain_id))
    else:
        all_rows.sort(key=lambda r: (r.received_at, r.chain_id))

    return ListUploadsResponse(
        uploads=all_rows[:limit],
        next_cursor=next_cursor,
    )


@router.get("/chains/{chain_id}", response_model=ChainAdminDetail)
async def get_upload(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> ChainAdminDetail:
    """Return a :class:`ChainAdminDetail` projection of one chain.

    Admin-only (loopback per ADR-004). Extends the wire-facing
    :class:`ChainResponse` with ``body_location`` so operators and E2E
    tests can inspect storage state without changing the SDK contract.

    Round-2 extension: the persisted request envelope is surfaced via
    :attr:`ChainAdminDetail.metadata` and :attr:`ChainAdminDetail.steps`
    so an operator can answer "what did the producer actually send?" without
    fetching the body. The envelope is deserialized from the row's
    ``chain_envelope_json`` column; no schema change is required.
    """
    row = await _find_upload(dispatcher, chain_id)
    if row is None:
        raise NotFoundError(f"chain {chain_id} not found")
    from phantom.models.chain import CapturedStep

    captured = [
        CapturedStep(step_name=step, values=dict(values.values))
        for step, values in row.captured_values.steps.items()
    ]
    metadata, steps = ChainAdminDetail.project_from_envelope(row.chain_envelope_json)
    return ChainAdminDetail(
        chain_id=row.chain_id,
        state=row.state,
        received_at=row.received_at,
        updated_at=row.updated_at,
        next_attempt_at=row.next_attempt_at,
        sent_at=row.sent_at,
        group_id=row.group_id,
        multifile_id=row.multifile_id,
        send_order=row.send_order,
        body_location=row.body_location,
        last_step_completed=row.last_step_completed,
        captured=captured,
        attempts=row.attempts,
        last_error=row.last_error,
        auth_blocked_host=row.auth_blocked_host,
        metadata=metadata,
        steps=steps,
    )


@router.get("/groups/{group_id}", response_model=GroupStatusResponse)
async def get_group_status(
    group_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    instance: str | None = Query(None),
) -> GroupStatusResponse:
    """Synthesized rollup for one query group (cycle-7 task 4.4).

    Fans out over instances via the aggregate idiom (a group can
    legitimately straddle instances; routing is per target URL prefix,
    ADR-006) and folds the members in Python. 404 ONLY when no row
    anywhere carries the id; because ``group_id`` is NOT NULL and
    defaults to ``chain_id`` at admission, a chain_id queried as a
    group id resolves to its self-evident singleton group (the accepted
    shared-value-space wart: ``total=1`` and the lone member's chain_id
    equals the queried id).

    ``counts_by_state`` carries ALL NINE canonical states (zero counts
    included) so the histogram has a deterministic shape;
    ``all_finished`` is the structural finished rule (see
    ``_STILL_MOVING_STATES``); ``first_received_at`` / ``last_sent_at``
    are the min/max joins over the members. ``?instance=`` narrows the
    rollup to one instance (404 when the group has no rows there).
    """
    targets = _scope_instances(dispatcher, instance)
    member_rows: list[UploadRow] = []
    for ctx in targets:
        member_rows.extend(await ctx.store.list_by_group_id(group_id))
    if not member_rows:
        raise NotFoundError(f"group {group_id} not found")
    # Deterministic member order across the multi-instance merge: the
    # per-store listing order (receipt time, chain_id tiebreak).
    member_rows.sort(key=lambda r: (r.received_at, r.chain_id))
    counts_by_state: dict[ChainState, int] = dict.fromkeys(get_args(ChainState), 0)
    for row in member_rows:
        counts_by_state[row.state] += 1
    all_finished = not any(row.state in _STILL_MOVING_STATES for row in member_rows)
    sent_stamps = [row.sent_at for row in member_rows if row.sent_at is not None]
    members = [
        GroupMember(
            chain_id=row.chain_id,
            state=row.state,
            received_at=row.received_at,
            sent_at=row.sent_at,
            attempts=row.attempts,
            last_error=row.last_error,
            send_order=row.send_order,
            multifile_id=row.multifile_id,
        )
        for row in member_rows
    ]
    return GroupStatusResponse(
        group_id=group_id,
        total=len(members),
        counts_by_state=counts_by_state,
        all_finished=all_finished,
        first_received_at=min(row.received_at for row in member_rows),
        last_sent_at=max(sent_stamps) if sent_stamps else None,
        members=members,
    )


@router.get("/uploads/by-captured-id/{captured_id}", response_model=IdentifierLookupResponse)
async def lookup_by_captured_id(
    captured_id: str,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    instance: str | None = Query(None),
) -> IdentifierLookupResponse:
    """Look up uploads by the upstream-assigned captured id (cycle-7 task 4.4).

    The identifier lives inside ``captured_values_json``; WHERE it lives
    is bound by per-instance deployment configuration
    (``InstanceCfg.admin_lookup``), so the service stays
    upstream-ignorant and the route takes no query params beyond
    ``?instance=``. Every targeted instance must carry the binding;
    otherwise the lookup refuses with ``lookup_not_configured`` (HTTP
    400) rather than guessing or silently skipping an instance (a
    found=false answer that ignored an unconfigured instance would be a
    lie). A miss is 200 with ``found=false``: the question is a
    membership test, not a resource fetch.

    The bindings are snapshotted ONCE, before the unconfigured guard,
    and the fan-out runs entirely off that snapshot: a config reload
    swapping the instance's live settings snapshot mid-request cannot
    make the loop's per-ctx re-read skip the instance holding the match
    (the found=false lie the round 2 adversary proved, finding R2-4).
    The answer is the truth as of guard time; the reloaded bindings
    serve the next request.
    """
    targets = _scope_instances(dispatcher, instance)
    # ONE read of each instance's live settings snapshot, before any
    # await (R2-4): the guard and the fan-out below must see the same
    # bindings. F5 moved ``admin_lookup`` off the (now frozen) cfg and
    # onto the snapshot, so this reads the one object a reload swaps.
    snapshot: list[tuple[InstanceContext, AdminLookupCfg]] = []
    unconfigured: list[str] = []
    for ctx in targets:
        settings_snapshot = ctx.current_settings()
        if settings_snapshot.admin_lookup is None:
            unconfigured.append(ctx.cfg.id)
        else:
            snapshot.append((ctx, settings_snapshot.admin_lookup))
    if unconfigured:
        raise LookupNotConfiguredError(instance_ids=tuple(unconfigured))
    matches: list[UploadStatusSummary] = []
    for ctx, lookup in snapshot:
        rows = await ctx.store.find_by_captured_value(
            lookup.capture_name, lookup.json_path, captured_id
        )
        matches.extend(_status_summary_for_row(row, lookup) for row in rows)
    matches.sort(key=lambda m: (m.received_at, m.chain_id))
    return IdentifierLookupResponse(
        kind="captured_file_id",
        value=captured_id,
        found=bool(matches),
        matches=matches,
    )


@router.get("/uploads/by-local-uuid/{local_uuid}", response_model=IdentifierLookupResponse)
async def lookup_by_local_uuid(
    local_uuid: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    instance: str | None = Query(None),
) -> IdentifierLookupResponse:
    """Look up uploads by their producer-minted local uuid (cycle-7 task 4.4).

    First-class promotion of the generic
    ``?key_value_match=phantom_local_uuid:<v>`` lookup: the metadata key
    is PINNED store-side (the caller never spells a key or a JSON path)
    and the result is the same status projection the captured-id lookup
    returns. Needs NO per-instance configuration (the key is a fixed
    Phantom convention). A miss is 200 with ``found=false``; matches is
    a list because Phantom enforces no global uniqueness on the key.

    The per-instance ``admin_lookup`` bindings (used only for the
    best-effort ``captured_file_id`` surfacing field here) are
    snapshotted once before the fan-out, the same reload-race posture
    as the by-captured-id route (R2-4): one read of each instance's
    live settings snapshot, before any await, so a mid-request snapshot
    swap cannot split the fan-out.
    """
    targets = _scope_instances(dispatcher, instance)
    # One read of each instance's live settings snapshot before any await
    # (R2-4 posture): the surfacing binding cannot change identity
    # mid-request. F5 moved ``admin_lookup`` off the frozen cfg.
    bindings = [(ctx, ctx.current_settings().admin_lookup) for ctx in targets]
    matches: list[UploadStatusSummary] = []
    for ctx, lookup in bindings:
        rows = await ctx.store.find_by_local_uuid(local_uuid)
        matches.extend(_status_summary_for_row(row, lookup) for row in rows)
    matches.sort(key=lambda m: (m.received_at, m.chain_id))
    return IdentifierLookupResponse(
        kind="local_uuid",
        value=str(local_uuid),
        found=bool(matches),
        matches=matches,
    )


def _status_summary_for_row(
    row: UploadRow,
    lookup: AdminLookupCfg | None,
) -> UploadStatusSummary:
    """Project one upload row into the lookup status summary.

    The two correlation fields are best-effort surfacing reads:
    ``captured_file_id`` resolves through the instance's
    ``admin_lookup`` binding when one exists (None otherwise; the row
    may simply not have captured it yet), and ``local_uuid`` mirrors the
    exact pinned envelope path the by-local-uuid lookup matches on.
    """
    return UploadStatusSummary(
        chain_id=row.chain_id,
        state=row.state,
        received_at=row.received_at,
        sent_at=row.sent_at,
        attempts=row.attempts,
        last_error=row.last_error,
        instance_id=row.instance_id,
        multifile_id=row.multifile_id,
        send_order=row.send_order,
        captured_file_id=(_captured_identifier_for_row(row, lookup) if lookup else None),
        local_uuid=_local_uuid_for_row(row),
    )


def _captured_identifier_for_row(row: UploadRow, lookup: AdminLookupCfg) -> str | None:
    """Read the configured captured identifier off one row's captured values.

    Walks the SAME path the store's ``find_by_captured_value`` JSON1
    extract uses (``steps.<capture_name>.values.<json_path>``) so what
    the lookup matches and what the response surfaces can never drift.
    Returns None when the step has not captured yet, the path does not
    resolve, or the landing value is not a string.
    """
    step = row.captured_values.steps.get(lookup.capture_name)
    if step is None:
        return None
    node: Any = step.values
    for part in lookup.json_path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node if isinstance(node, str) else None


def _local_uuid_for_row(row: UploadRow) -> UUID | None:
    """Read the pinned local-uuid metadata key off one row's envelope.

    Mirrors the store's ``find_by_local_uuid`` JSON1 path EXACTLY
    (``$.steps[0].body.value.metadata.keyValueStore.<pinned key>``,
    camelCase only): the surfaced value is precisely what the
    by-local-uuid lookup would match on, no more and no less. Returns
    None when the envelope does not parse, the path does not resolve,
    or the landing value is not a valid UUID string.
    """
    try:
        envelope: Any = json.loads(row.chain_envelope_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(envelope, dict):
        return None
    steps = envelope.get("steps")
    if not isinstance(steps, list) or not steps:
        return None
    node: Any = steps[0]
    for key in ("body", "value", "metadata", "keyValueStore"):
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    if not isinstance(node, dict):
        return None
    raw = node.get(PHANTOM_LOCAL_UUID_METADATA_KEY)
    if not isinstance(raw, str):
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


@router.get("/chains/{chain_id}/body")
async def get_upload_body(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> StreamingResponse:
    """Stream the body bytes."""
    ctx, row = await _find_upload_with_ctx(dispatcher, chain_id)
    if row is None or ctx is None:
        raise NotFoundError(f"chain {chain_id} not found")
    # Single body store reference; HybridBodyStore
    # routes the read to the RAM or file half by RAM-presence.
    body = await ctx.body_store.get_all(row.chain_id)
    payload = b"".join(body.values())
    return StreamingResponse(_chunk_bytes(payload), media_type="application/octet-stream")


def _chunk_bytes(data: bytes) -> AsyncIterator[bytes]:
    """Yield ``data`` once for streaming-friendly emission."""

    async def gen() -> AsyncIterator[bytes]:
        yield data

    return gen()


@router.get("/chains/{chain_id}/bundle")
async def get_upload_bundle(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Return metadata + body as a simple JSON envelope.

    A real multipart implementation would require an SDK; the simple
    JSON envelope is sufficient for admin tooling and the v1 milestone.
    """
    ctx, row = await _find_upload_with_ctx(dispatcher, chain_id)
    if row is None or ctx is None:
        raise NotFoundError(f"chain {chain_id} not found")
    body = await ctx.body_store.get_all(row.chain_id)
    bundle = {
        "metadata": json.loads(row.model_dump_json()),
        "body_refs": {name: data.hex() for name, data in body.items()},
    }
    return Response(content=json.dumps(bundle), media_type="application/json")


@router.post("/chains/extract")
async def extract_uploads(
    filter_body: Annotated[ExtractFilter, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> StreamingResponse:
    """Stream a tar of matching bodies plus a manifest.json."""
    targets = _scope_instances(dispatcher, filter_body.instance)
    return StreamingResponse(
        _build_tar_stream(targets, filter_body),
        media_type="application/x-tar",
    )


@router.get("/export.tar")
async def export_tar(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> StreamingResponse:
    """Stream every buffered body + manifest.json (ADR-005)."""
    targets = list(dispatcher.all_instances())
    return StreamingResponse(
        _build_tar_stream(
            targets,
            ExtractFilter(state=None, route=None, since=None, chain_ids=None, instance=None),
        ),
        media_type="application/x-tar",
    )


async def _build_tar_stream(
    targets: list[InstanceContext], filter_body: ExtractFilter
) -> AsyncIterator[bytes]:
    """Yield a single tar archive with manifest.json + every body.

    Built entirely in-memory, then yielded as one chunk. Tar archives need
    every member's size up front, so streaming-as-we-go is not free —
    materializing first keeps the implementation simple and matches the
    v1 export.tar / extract use case (producer-scale data, not GB+).

    With an empty filter the export returns every row in the store —
    including terminal states (``succeeded`` / ``failed`` / ``stored`` /
    ``cancelled`` / ``corrupted``). ADR-005's producer-recovery use case is
    "pull every buffered body the operator might want," which is exactly
    the terminal set: a `stored` chain whose retry budget exhausted is
    the primary thing an operator recovers via export.tar. Callers who
    want only non-terminal rows must opt in via the ``state`` filter.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        manifest: list[dict[str, Any]] = []
        for ctx in targets:
            # R8-5: every advertised ExtractFilter axis restricts the
            # tar. ``chain_ids`` names exact rows, so it is served by
            # point reads with the other axes applied as predicates (no
            # pagination interplay, no per-instance-limit truncation of
            # named rows); the listing path forwards ``since`` (received
            # at or after, the DeleteFilter semantics) into the store.
            chunk: list[UploadRow]
            if filter_body.chain_ids is not None:
                chunk = []
                for cid in filter_body.chain_ids:
                    row = await ctx.store.get(cid)
                    if row is None or row.instance_id != ctx.cfg.id:
                        continue
                    if filter_body.state is not None and row.state != filter_body.state:
                        continue
                    if filter_body.route is not None and row.route_name != filter_body.route:
                        continue
                    if filter_body.since is not None and row.received_at < filter_body.since:
                        continue
                    chunk.append(row)
            else:
                chunk, _ = await ctx.store.list_uploads(
                    state=filter_body.state,
                    route=filter_body.route,
                    since=filter_body.since,
                    limit=_EXPORT_TAR_PER_INSTANCE_LIMIT,
                )
            for row in chunk:
                manifest.append(
                    {
                        "chain_id": str(row.chain_id),
                        "instance_id": row.instance_id,
                        # Cycle-7 task 4.6: the query-grouping handle, so an
                        # operator can correlate exported bodies back to the
                        # group surface without a second lookup.
                        "group_id": str(row.group_id),
                        "state": row.state,
                        "endpoint": row.endpoint,
                        "received_at": row.received_at.isoformat(),
                        # Cycle-7 task 4.6: confirmed-delivery instant; null
                        # for never-delivered rows (the export's main
                        # audience: stored/failed bodies awaiting recovery).
                        "sent_at": (row.sent_at.isoformat() if row.sent_at else None),
                        "body_size_bytes": row.body_size_bytes,
                        "storage_encoding": row.storage_encoding,
                    }
                )
                # Pack the body bytes through the mode-selected body store.
                try:
                    body = await ctx.body_store.get_all(row.chain_id)
                except KeyError:
                    continue
                for name, data in body.items():
                    info = tarfile.TarInfo(name=f"bodies/{row.chain_id}/{name}")
                    info.size = len(data)
                    tf.addfile(info, io.BytesIO(data))
        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest_bytes)
        tf.addfile(info, io.BytesIO(manifest_bytes))
    yield buf.getvalue()


@router.post("/chains/{chain_id}/replay", response_model=UploadRow)
async def replay_upload(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> UploadRow | Response:
    """Reset attempts and re-queue.

    **This endpoint is NOT idempotent, deliberately.** Replay means "deliver
    this again", so a second call is a second instruction rather than a
    repeat of the first: the CAS re-queues from seven states INCLUDING
    ``succeeded``, so replaying a chain that has since been delivered
    delivers it a second time. There is no idempotency claim on this path
    (the ``idempotency_index`` is ingress-only) and no request key is
    consulted.

    **What that means for a lost response.** A read timeout does not tell you
    the replay did not happen: it may have landed, re-queued the row and
    delivered it, with only the response lost. The SDK therefore does NOT
    auto-retry this call on a may-have-landed transport failure (F12); it
    raises and leaves the decision to you. The safe check is the chain's own
    state through ``GET /v1/admin/chains/{chain_id}``, not a second POST. A
    caller with its own blind retry loop (``curl --retry``, a shell wrapper)
    can still double-deliver.

    The one natural guard is retention-dependent: the body-discard precheck
    below refuses a row whose body is gone, and at the default
    ``succeeded_body_seconds`` of 0 the body goes the moment the row
    succeeds, so a retry 409s. Any non-zero window leaves the door open.

    M-W4-F7 audit closure: ``replay`` refuses when the row is currently
    ``attempting`` (a sender is actively driving it). The store raises
    :class:`ReplayRefusedAttemptingError` from its in-write-lock state
    precheck, and the route returns the canonical 409
    ``replay_refused_attempting`` envelope so the operator can wait for the
    sender to finish (or cancel first).
    Round 1 defender fix (R1-1): pre-fix this refusal escaped as
    FastAPI's raw ``{"detail": ...}`` body, which the SDK could not
    dispatch on.

    Body-accounting refusal (cycle-7 phase 7 pre-round defender fix):
    the store raises :class:`ReplayBodyDiscardedError` when the row's
    ``body_discarded_at`` is stamped (the body is gone per the row's
    own accounting, on either discard leg), and the route returns the
    canonical 409 ``replay_body_discarded`` envelope; the row is
    left exactly as it was (``sent_at`` preserved). Without the
    refusal, the re-queued row would land in ``corrupted`` on the
    sender's next claim, laundering an operator action into a
    corruption signal.

    Raises:
        NotFoundError: When no instance holds the chain, including a
            row deleted between the lookup and the store's write lock
            (404 envelope).
        ReplayBodyDiscardedError: When the row's body was already
            discarded (409 ``replay_body_discarded`` envelope).
        ReplayRefusedAttemptingError: When a sender is actively
            driving the row (409 ``replay_refused_attempting``
            envelope).
    """
    ctx, row = await _find_upload_with_ctx(dispatcher, chain_id)
    if ctx is None or row is None:
        raise NotFoundError(f"chain {chain_id} not found")
    # R8-6: a replay re-queues the row into the in-flight set. A row
    # whose slot was RELEASED (the sender's terminal transitions; the
    # auth_expired park) must re-admit through the gate exactly as the
    # Kicker does on wake; queued/stored rows still hold their slot
    # and must not double-charge. Refusing the replay outright when the
    # gate is full keeps the ledger truthful: re-queueing without a
    # slot is the drift.
    #
    # This is one of the TWO surviving direct ``row_holds_slot``
    # consultations (ADR-036): it is a CURRENT-STATE question about a
    # row this line is not transitioning, asked to decide whether to
    # RESERVE at all. Every crossing below is settled by the gate.
    charged = not row_holds_slot(row.state, row.body_discarded_at)
    reservation: SlotReservation | None = None
    if charged:
        admission = await ctx.saturation.admit(row.body_size_bytes)
        if not isinstance(admission, AdmissionGranted):
            return _admin_error(
                "saturation_cap",
                "Replay refused: the saturation gate is at capacity and the "
                "replayed row would re-enter the in-flight set; retry after "
                "capacity frees.",
                instance_id=ctx.cfg.id,
            )
        reservation = admission.reservation
    try:
        outcome = await ctx.store.replay(chain_id)
    except Exception:
        # The write raised, so there is no outcome to settle against and
        # the row never re-entered the in-flight set: the reservation
        # goes straight back.
        if reservation is not None:
            await ctx.saturation.unwind(reservation)
        raise
    if outcome is None:
        # The row vanished between the lookup above and the store's
        # in-lock precheck (a delete race); same answer as the up-front
        # miss, and the same unwind: no write, no outcome.
        if reservation is not None:
            await ctx.saturation.unwind(reservation)
        raise NotFoundError(f"chain {chain_id} not found")
    # R9-4: the reservation above was taken against the PRE-FETCHED
    # state, which races the kicker's wake (charges + re-queues) and the
    # sender's terminal transitions (release) in both directions. The
    # gate settles both questions at once from the store's
    # in-transaction previous_state, the state the row was actually
    # re-queued FROM: a charge crossing consumes the reservation (or,
    # when none was taken, charges uncapped because the row is already
    # queued and refusing now could not un-queue it), and a no-crossing
    # result gives the reservation back. That replaces the four
    # hand-written arms this route used to run, and it is why the second
    # ``row_holds_slot`` consultation is gone.
    await ctx.saturation.settle(
        SlotDelta.from_replay(outcome, size_bytes=outcome.row.body_size_bytes),
        consumes=reservation,
    )
    return outcome.row


@router.post("/chains/{chain_id}/cancel", response_model=UploadRow)
async def cancel_upload(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> UploadRow:
    """Transition the row to ``cancelled`` if non-terminal.

    The cancel path OWNS the gate accounting for the row it cancels
    (R8-4): the sender's M-W4-F7 no-op deliberately skips its own
    settlement when its in-flight UPDATE finds the state changed,
    deferring to whoever changed it. The decision is made from the
    store's in-transaction ``previous_state`` and stamp (not the
    route's pre-fetch, which can race a sender/kicker transition),
    which the gate settles through :meth:`SaturationGate.settle`.
    """
    ctx, row = await _find_upload_with_ctx(dispatcher, chain_id)
    if ctx is None or row is None:
        raise NotFoundError(f"chain {chain_id} not found")
    outcome = await ctx.store.cancel(chain_id)
    await ctx.saturation.settle(
        SlotDelta.from_cancel(outcome, size_bytes=outcome.row.body_size_bytes)
    )
    return outcome.row


@router.delete("/chains/{chain_id}", status_code=204)
async def delete_upload(
    chain_id: UUID,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Hard delete one chain + its body.

    Settles the saturation gate against the removal (R8-4), using the
    accounting captured atomically with the DELETE.
    """
    ctx, row = await _find_upload_with_ctx(dispatcher, chain_id)
    if ctx is None or row is None:
        raise NotFoundError(f"chain {chain_id} not found")
    await ctx.body_store.delete(chain_id)
    accounting = await ctx.store.delete(chain_id)
    # ``accounting is None`` is a MISSING-ROW answer, not a crossing:
    # ``store.delete`` returns ``DeletedRowAccounting | None`` and no
    # adapter has a None arm, so the guard survives while the slot
    # predicate folds into ``from_removal``.
    if accounting is not None:
        await ctx.saturation.settle(
            SlotDelta.from_removal(accounting, size_bytes=accounting.body_size_bytes)
        )
    return Response(status_code=204)


@router.delete("/chains", response_model=BulkDeleteResponse)
async def bulk_delete_uploads(
    filter_body: Annotated[DeleteFilter, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> BulkDeleteResponse:
    """Bulk delete by filter.

    An all-None filter is refused with the 422
    ``bulk_delete_filter_empty`` envelope (ADR-004: an empty filter
    would mean "delete every row").

    **This endpoint is NOT idempotent.** The filter is re-evaluated against
    the LIVE table on every call, so its blast radius is not fixed to what
    the first request saw: a repeat can delete rows that became eligible in
    between (a chain that reached the filtered state, or crossed the
    ``since`` boundary, after the first call ran). A lost response is
    therefore not safe to answer with a second identical request. The SDK
    does not auto-retry this call on a may-have-landed transport failure
    (F12); re-read the affected rows and narrow the filter instead.

    Making the radius fixed would need the request to carry the row set it
    intended, which is a different feature.

    Raises:
        BulkDeleteFilterEmptyError: When no filter field is set (422
            ``bulk_delete_filter_empty`` envelope).
    """
    fields = (
        filter_body.state,
        filter_body.route,
        filter_body.since,
        filter_body.instance,
    )
    if all(v is None for v in fields):
        raise BulkDeleteFilterEmptyError()
    deleted = 0
    targets = _scope_instances(dispatcher, filter_body.instance)
    for ctx in targets:
        removed = await ctx.store.bulk_delete(
            state=filter_body.state,
            route=filter_body.route,
            since=filter_body.since,
        )
        # C1 closure: delete the corresponding body files alongside the
        # rows. Previously body files were leaked until the orphan
        # janitor's next sweep; the operator's "delete these uploads"
        # expectation includes the bodies. R8-4: the gate settles each
        # removal here, per the accounting captured atomically with the
        # DELETE, and releases for the rows that still held a slot.
        #
        # R10-D1 (the R8-3 family): the row DELETE above legalized a
        # same-chain_id re-POST at any later instant, so each late body
        # delete re-reads the live table immediately before acting and
        # steps aside when a new owner exists - the new row's accepted
        # bytes must not be wiped by the OLD row's cleanup (the bytes
        # would already be in the store: admission puts the body before
        # the row commits). The step-aside is safe for the new owner
        # because re-admission cleared the chain_id namespace before
        # its put (R11-1, _persist_row_and_claim): a committed new
        # owner holds exactly its own declared refs, so the old row's
        # leftovers cannot poison its get_all union. (The pre-R11-1
        # justification here - "removed with the new row's own body
        # lifecycle" - was false when the old row declared a ref the
        # new row omits; finding R11-1.) Old files that outlive this
        # loop entirely (a crash between the row DELETE and this
        # cleanup) sit in a dead namespace until a future re-POST's
        # clear or the orphan janitor collects them. The get-then-
        # delete sliver (a re-POST whose put has run but whose row has
        # not yet committed when the re-read sees no owner) is the
        # documented irreducible residual file deletion always
        # carries - unchanged by R11-1. The settlement stays keyed on
        # the OLD row's atomically captured accounting, which no
        # re-admission can touch.
        for entry in removed:
            if await ctx.store.get(entry.chain_id) is None:
                await ctx.body_store.delete(entry.chain_id)
            await ctx.saturation.settle(
                SlotDelta.from_removal(entry, size_bytes=entry.body_size_bytes)
            )
        deleted += len(removed)
    return BulkDeleteResponse(deleted=deleted)


# ---------------------------------------------------------------------------
# Token cache
# ---------------------------------------------------------------------------


@router.get("/tokens", response_model=TokenListResponse)
async def list_tokens(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    endpoint: str | None = Query(None),
) -> TokenListResponse:
    """List token slots (NO bearer values; ADR-004)."""
    out = []
    for ctx in dispatcher.all_instances():
        out.extend(await ctx.token_cache.list_slots(endpoint=endpoint))
    return TokenListResponse(tokens=out)


@router.put("/tokens/{endpoint}/{uid}", status_code=204)
async def push_token_one(
    endpoint: str,
    uid: str,
    body: Annotated[TokenPushRequest, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Push a bearer for one slot."""
    for ctx in dispatcher.all_instances():
        await ctx.token_cache.set(endpoint, uid, body.token, source="admin_push")
    return Response(status_code=204)


@router.put("/tokens/{endpoint}", status_code=204)
async def push_token_endpoint(
    endpoint: str,
    body: Annotated[TokenPushRequest, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Push a bearer to every slot at an endpoint."""
    for ctx in dispatcher.all_instances():
        for slot in await ctx.token_cache.list_slots(endpoint=endpoint):
            await ctx.token_cache.set(endpoint, slot.uid, body.token, source="admin_push")
    return Response(status_code=204)


@router.put("/tokens", status_code=204)
async def push_token_global(
    body: Annotated[TokenPushRequest, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Push a bearer to every cached slot."""
    for ctx in dispatcher.all_instances():
        for slot in await ctx.token_cache.list_slots():
            await ctx.token_cache.set(slot.endpoint, slot.uid, body.token, source="admin_push")
    return Response(status_code=204)


@router.delete("/tokens/{endpoint}/{uid}", status_code=204)
async def delete_token_one(
    endpoint: str,
    uid: str,
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Invalidate one ``(endpoint, uid)`` slot: mark it ``bad``, preserve it.

    Per ADR-003 a bad token is NOT deleted - it stays in the cache with
    ``status='bad'`` so ``GET /v1/admin/tokens`` still surfaces the slot
    and an operator can see exactly which credential needs replacement.
    This route therefore flips the slot's status rather than hard-deleting
    it (R-EX3); the SDK ``invalidate_token`` contract matches.
    """
    for ctx in dispatcher.all_instances():
        await ctx.token_cache.mark_bad(endpoint, uid)
    return Response(status_code=204)


@router.delete("/tokens", status_code=204)
async def delete_tokens_all(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Invalidate every slot across every instance: mark ``bad``, preserve.

    The bulk analogue of :func:`delete_token_one`; per ADR-003 the slots
    persist as ``status='bad'`` rather than being deleted (R-EX3).
    """
    for ctx in dispatcher.all_instances():
        await ctx.token_cache.mark_all_bad()
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Destination-credential store (SigV4)
# ---------------------------------------------------------------------------


@router.put("/credentials/{dest_host}", status_code=204)
async def push_credential_one(
    dest_host: str,
    body: Annotated[CredentialPushBody, Body()],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> Response:
    """Provision a destination credential for ``dest_host`` (loopback, no auth).

    The SigV4 analogue of :func:`push_token_one` (the admin token push), per the
    2026-06-23 copy directive. Differs only by the route prefix
    (``/credentials`` vs ``/tokens``), the key (the destination host alone — a
    SigV4 step has no caller-supplied ``uid``), and the structured
    :data:`CredentialPushBody` (vs the bare token string).

    The ``{dest_host}`` segment is normalized through the SAME ``host_key_for``
    helper the executor uses for its forward-time credential lookup
    (``phantom.routing``), so the push key equals the lookup key
    ``HostCredKey(host_key_for(full_url))`` BY CONSTRUCTION: a host pushed as
    ``S3.amazonaws.com`` resolves a request to ``s3.amazonaws.com`` (the
    silent-miss class the token push left latent is closed here).

    Each instance's :attr:`~phantom.instances.context.InstanceContext.signer_creds`
    store is ``set`` under that host key. ``set`` freshens the slot
    (``status='fresh'``) AND fires the store's wake handler, which the
    sigv4-flavoured :class:`~phantom.workers.kicker.Kicker` registered, so
    an operator re-pushing fresh credentials for a host wakes every parked
    ``auth_expired`` row on that host (the loop-closing seam, mirroring the
    token push waking the same class in its bearer flavour).

    The :data:`CredentialPushBody` now declares a REQUIRED ``service`` (the AWS
    service the credential signs for, coerced to a
    :class:`~phantom.models.credential.SigningService` at the strict boundary),
    so an operator PUT whose body omits ``service`` or names an unknown service
    is rejected ``422`` at the door (before any store write) by the body
    validator — the same fail-loud the config route applies at boot.

    A bearer-only deployment carries no credential store
    (``signer_creds is None``); such instances are skipped (the push is a no-op
    for them — the store is optional by construction, ADR design), never a
    crash.

    Returns ``204 No Content`` with NO body — the ``secret_access_key`` is
    therefore never echoed (ADR-004). The admin surface exposes credential
    STATUS only, never secret material.

    Args:
        dest_host: The destination host path segment (normalized at the door).
        body: The discriminated credential-push wire body (resolved literals).
        dispatcher: The instance dispatcher (the same DI seam the token push
            uses, overridden at composition root).

    Returns:
        An empty ``204`` response.
    """
    key = HostCredKey(host_key_for(dest_host))
    creds = credential_body_to_internal(body)
    for ctx in dispatcher.all_instances():
        if ctx.signer_creds is None:
            continue
        await ctx.signer_creds.set(key, creds, source="admin_push")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Hot reload (mirrors SIGHUP)
# ---------------------------------------------------------------------------


@router.post("/reload")
async def post_reload(request: Request) -> Response:
    """Trigger a settings reload from the configured YAML path.

    Mirrors SIGHUP: re-reads the YAML at ``app.state.settings_path``,
    builds fresh per-instance snapshots, swaps them in the
    :class:`SettingsHolder`, rebuilds each instance's retry strategy,
    and pushes new saturation caps into every gate.

    A restart-required block that changed (``ad_mint``, and the
    per-instance route block: ``routes``, ``host_prefixes``,
    ``data_dir``) does NOT fail the reload. It is refused: nothing is
    applied for that block, a WARNING names the instance and the drifted
    fields, and this endpoint still answers 200. A REJECTED reload is a
    different outcome, described below, and it applies nothing at all.

    Returns:
        200 ``{"reloaded_instances": [<id>, ...]}`` on success.
        422 ``ErrorEnvelope`` (``envelope_invalid``) on YAML parse
        failure, Pydantic validation failure, or a cross-field config
        invariant pydantic cannot express. The one such invariant today
        is the retention floor (F14): for every terminal state,
        ``retention.<state>_body_seconds`` must not exceed
        ``<state>_metadata_seconds``, because the reaper's metadata pass
        deletes rows without touching bodies and a body outliving its
        row is unreclaimable in RAM. Every 422 arm strikes BEFORE any
        snapshot swap, so the whole previous configuration keeps
        running; nothing is half-applied.
        404 ``ErrorEnvelope`` (``not_found``) when hot reload is
        disabled because ``create_app`` was given no ``settings_path``.
    """
    holder = getattr(request.app.state, "settings_holder", None)
    settings_path = getattr(request.app.state, "settings_path", None)
    instances = getattr(request.app.state, "instances", None)
    if holder is None or settings_path is None or instances is None:
        raise NotFoundError("Hot reload not configured for this process")
    try:
        reloaded = await apply_reload(holder, settings_path, instances)
    except RELOAD_FAILURE_ERRORS as exc:
        # One shared failure set with the SIGHUP path (R8-2): parse and
        # validation failures, file-read failures (vanished file,
        # non-UTF-8 byte) and cross-field config-invariant failures (the
        # retention floor, F14) all reject-and-keep-previous, in-envelope.
        return _admin_error("envelope_invalid", f"Settings reload failed: {exc}")
    return Response(
        content=json.dumps({"reloaded_instances": reloaded}),
        media_type="application/json",
        status_code=200,
    )


# ---------------------------------------------------------------------------
# Observability (plan § 4.2.5)
# ---------------------------------------------------------------------------


@router.get("/observability/counters", response_model=CountersResponse)
async def get_observability_counters(
    registry: Annotated[MetricsRegistry, Depends(get_metrics_registry)],
) -> CountersResponse:
    """Serialize the process-wide MetricsRegistry counters.

    Plan § 4.2.5. Returns one :class:`CounterValue` per registered
    counter; ``values`` maps the empty-string bucket plus any label
    values that have been incremented. Even unbumped counters surface
    a zero-valued bucket for stable response shape.
    """
    counters: list[CounterValue] = []
    for name, counter in registry.counters.items():
        counters.append(
            CounterValue(
                name=name,
                description=counter.description,
                values=dict(counter.snapshot()),
            )
        )
    return CountersResponse(counters=counters)


@router.get("/observability/gauges", response_model=GaugesResponse)
async def get_observability_gauges(
    registry: Annotated[MetricsRegistry, Depends(get_metrics_registry)],
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> GaugesResponse:
    """Serialize the process-wide MetricsRegistry gauges.

    Plan § 4.2.5. ``body_location_distribution`` is registered by the
    store but populated on demand here via a SQL grouping across every
    instance's persistent store (the gauge has no producer per plan
    § 4.2.2 to avoid invariant-coupled per-write drift).
    """
    gauges: list[GaugeValue] = []
    # Compute body_location_distribution on demand from the live store.
    body_location_counts: dict[str, float] = {"ram": 0.0, "file": 0.0}
    for ctx in dispatcher.all_instances():
        rows = await ctx.store.list_non_terminal()
        for row in rows:
            body_location_counts[row.body_location] = (
                body_location_counts.get(row.body_location, 0.0) + 1.0
            )
    for name, gauge in registry.gauges.items():
        if name == "body_location_distribution":
            values = body_location_counts
        else:
            values = dict(gauge.snapshot())
        gauges.append(
            GaugeValue(
                name=name,
                description=gauge.description,
                values=values,
            )
        )
    return GaugesResponse(gauges=gauges)


@router.get("/observability/ram_pressure", response_model=RamPressureStatusResponse)
async def get_observability_ram_pressure(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
) -> RamPressureStatusResponse:
    """Surface RAM-pressure status across all instances.

    Plan § 4.2.5. Aggregates ``ram_body_store.total_bytes()`` +
    ``persist_controller`` queue depth across every instance. Returns
    zero for fields whose instance configuration disables them (e.g.,
    ``persist_controller_queue_depth = 0`` in all_ram / all_disk where
    no PersistController is wired).
    """
    total_bytes = 0
    ceiling_bytes = 0
    pending = 0
    queue_depth = 0
    for ctx in dispatcher.all_instances():
        # ram_body_store may be None in some test fixtures; defensive read.
        ram_bs = getattr(ctx, "ram_body_store", None)
        if ram_bs is not None:
            total_bytes += await ram_bs.total_bytes()
        snapshot = ctx.current_settings()
        if snapshot.body_store.ram_ceiling_bytes is not None:
            ceiling_bytes += snapshot.body_store.ram_ceiling_bytes
        # PersistController exposes its internal queue size; queue + in-flight.
        if ctx.persist_controller is not None:
            queue_depth += ctx.persist_controller._queue.qsize()
            pending += queue_depth + len(ctx.persist_controller._in_flight)
    return RamPressureStatusResponse(
        ram_body_store_bytes=total_bytes,
        ram_ceiling_bytes=ceiling_bytes,
        pending_migrations=pending,
        persist_controller_queue_depth=queue_depth,
    )


# ---------------------------------------------------------------------------
# Quarantine inventory + restore (plan § 5.2.5 / § 1.4 / § 1.5)
#
# Quarantine artifacts are FLAT siblings in each instance's per-instance
# data_root (``<storage.data_dir>/<cfg.data_dir>/``), not the top-level data
# dir (Finding F-1). These routes resolve the per-instance paths via the
# dispatcher and an ``?instance=`` selector (the established admin idiom,
# mirroring ``/stats`` and ``/observability/gauges``).
# ---------------------------------------------------------------------------


@router.get("/quarantine", response_model=QuarantineInventoryResponse)
async def get_quarantine_inventory(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    data_root: Annotated[Path, Depends(get_data_root)],
    instance: str | None = Query(None),
) -> QuarantineInventoryResponse:
    """List backups (one entry each, plus anomalies) for one or all instances.

    Plan § 5.2.5 / cycle-7 seam 2. Returns ONE :class:`QuarantineEntry` per
    BACKUP under the targeted instance data root(s), read from the backup
    manifests (keyed by ``backup_id``), plus one flagged anomaly entry per
    on-disk artifact no manifest claims. Retention is admin-initiated by
    default (no auto-aging).

    Scope (Finding F-1):

    * ``?instance=<id>`` — scan just that instance's per-instance data_root.
    * omitted — scan every configured instance and concatenate the results
      (the natural "everything quarantined across the deployment" view).

    Raises:
        UnknownInstanceError: When ``?instance=`` names an unknown instance
            (becomes a 421 ``ErrorEnvelope``).
    """
    targets = _scope_instances(dispatcher, instance)
    entries: list[QuarantineEntry] = []
    for ctx in targets:
        paths = instance_storage_paths(data_root, ctx.cfg)
        entries.extend(
            QuarantineEntry(
                backup_id=e.backup_id,
                reason=e.reason,
                iso_display=e.iso_display,
                db_path=str(e.db_path) if e.db_path is not None else None,
                body_path=str(e.body_path) if e.body_path is not None else None,
                has_db=e.has_db,
                has_body=e.has_body,
                bytes=e.bytes,
                anomaly=e.anomaly,
            )
            for e in list_quarantines(paths.data_root)
        )
    return QuarantineInventoryResponse(quarantines=entries)


@router.post("/quarantine/restore", response_model=QuarantineRestoreResponse)
async def restore_quarantine_backup(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    data_root: Annotated[Path, Depends(get_data_root)],
    backup_id: Annotated[UUID, Query()],
    instance: str | None = Query(None),
) -> QuarantineRestoreResponse:
    """Move a ``mode_switch`` backup back into the live tree (plan § 1.5).

    Strategy E.1 + cycle-7 seam 2: an explicit, clobber-safe one-call admin
    restore addressed by IDENTITY
    (``POST /v1/admin/quarantine/restore?backup_id=...&instance=...``). The
    backup is resolved through its MANIFEST; the route never string-matches
    filenames, so an unmanifested artifact (an inventory anomaly) is simply
    not addressable here (R5-P unrepresentable). Backs up any CURRENT live
    data to a fresh ``mode_switch`` backup first, then moves the chosen
    backup into the now-empty live tree. Both moves are marked, so a crash
    at any point is finished forward by
    :func:`reconcile_interrupted_backup_move` on the next boot (review M-1).

    This STAGES the restore on disk; it does NOT hot-reattach the running
    store, which keeps its old file descriptor (Finding F-3). The operator
    must restart in a disk-backed mode (``hybrid`` / ``all_disk``) to serve
    the restored data; the response says so (``restart_required=True``).

    Scope (Finding F-1): ``?instance=`` targets whose live tree to restore
    INTO. Required when more than one instance is configured; defaults to the
    sole instance otherwise. A restore is a single-target destructive
    operation, so there is no aggregate form.

    Raises:
        UnknownInstanceError: When ``?instance=`` names an unknown instance
            (becomes a 421 ``ErrorEnvelope``).
        NotFoundError: When ``?instance=`` is omitted with more than one
            instance configured, or when ``backup_id`` names no restorable
            ``mode_switch`` backup manifest in the instance (becomes a 404
            ``ErrorEnvelope``).
        RestoreNoOpError: When the backup's DB half (the load-bearing half:
            it holds the upload rows that point at the body bytes) is absent
            on disk, the route refuses UP FRONT, before displacing any live
            data; and when the restore moves ran but the DB still did not
            land, it fails after the fact. Both become a 409 ``restore_noop``
            ``ErrorEnvelope`` so the operator retries rather than receiving
            a success-shaped response that stranded the upload metadata
            (findings H-1 / L-2 / R5-P).
    """
    ctx = _resolve_single_instance(dispatcher, instance)
    paths = instance_storage_paths(data_root, ctx.cfg)

    # 1. The chosen backup must exist AS A MANIFEST and be a mode_switch
    #    backup (the only restorable kind). Anomalies have no manifest, so
    #    they cannot reach past this point by construction.
    manifest = load_backup_manifest(paths.data_root, backup_id)
    if manifest is None or manifest.reason != "mode_switch":
        raise NotFoundError(
            f"No restorable mode_switch backup with backup_id {backup_id} "
            f"in instance {ctx.cfg.id!r}"
        )

    # 2. Refuse up front when the DB half is not on disk (R5-P inverse leg:
    #    a manifested backup whose DB artifact an interrupted move or an
    #    operator removed). Refusing BEFORE step 3 means a doomed restore
    #    never displaces the live tree at all.
    if not manifest.db_path.exists():
        raise RestoreNoOpError(
            backup_id=backup_id,
            instance_id=ctx.cfg.id,
            interim_backup_db=None,
            interim_backup_body=None,
            detail=(
                "the backup's DB artifact is absent on disk (has_db=false in "
                "the inventory); nothing was moved"
            ),
        )

    # 3. Back up any CURRENT live data first (clobber-safe; the interim state
    #    is preserved and itself becomes a manifested mode_switch backup in
    #    the inventory).
    interim = quarantine(paths.db_path, paths.bodies_root, reason="mode_switch")
    interim_backup_db = str(interim.db_path) if interim.has_db else None
    interim_backup_body = str(interim.body_path) if interim.has_body else None

    # 4. Move the chosen backup into the (now-empty) live tree, addressed by
    #    its manifest's declared paths.
    outcome = restore_mode_switch_backup(paths.db_path, paths.bodies_root, manifest)

    # 5. Fail LOUD when the DB did not land (findings H-1 / L-2 / R5-P). With
    #    the step-2 pre-check this is the residual race only (the artifact
    #    vanished between the check and the move, or the live DB could not be
    #    set aside): the live uploads.db was displaced to the step-3 interim
    #    backup and never replaced, so a success-shaped restart_required
    #    response would strand the upload metadata. The guard keys on
    #    db_moved alone (a body-only move is NOT a restore); a legitimately
    #    metadata-only backup still succeeds because its DB half moved.
    if not outcome.db_moved:
        raise RestoreNoOpError(
            backup_id=backup_id,
            instance_id=ctx.cfg.id,
            interim_backup_db=interim_backup_db,
            interim_backup_body=interim_backup_body,
            detail="the backup's DB artifact did not land in the live tree",
        )

    logger.warning(
        "Restored mode_switch backup backup_id=%s into instance %r (interim backup: db=%s body=%s)",
        backup_id,
        ctx.cfg.id,
        interim_backup_db,
        interim_backup_body,
    )
    return QuarantineRestoreResponse(
        restored_db=str(outcome.db_path),
        restored_body=str(outcome.body_path),
        interim_backup_db=interim_backup_db,
        interim_backup_body=interim_backup_body,
        restart_required=True,
        detail=(
            f"Restored backup {backup_id} into instance {ctx.cfg.id}. "
            "Restart Phantom in a disk-backed mode (hybrid or all_disk) to "
            "serve the restored data; the running store still holds the "
            "pre-restore database file descriptor."
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class UnknownInstanceError(Exception):
    """Raised when ``?instance=<id>`` or a path-param names an unknown instance.

    Its :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this into a
    canonical 421 ``ErrorEnvelope`` (plan §5.6 / ADR-010), the same
    shape the SDK's :data:`EXCEPTION_FOR_CODE` machinery expects.
    """

    def __init__(self, instance_id: str) -> None:
        super().__init__(f"Instance {instance_id!r} not configured")
        self.instance_id = instance_id


class NotFoundError(Exception):
    """Raised when an admin lookup names a resource (upload, body, slot) that doesn't exist.

    Its :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this into a canonical
    404 ``ErrorEnvelope`` with ``error.code='not_found'`` so phantom-client
    maps the response to :class:`PhantomNotFoundError` via the
    :data:`EXCEPTION_FOR_CODE` table. FastAPI's default 404 (``{"detail":
    "..."}``) does not parse against the envelope schema.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class RestoreNoOpError(Exception):
    """Raised when ``POST /v1/admin/quarantine/restore`` cannot land the DB half.

    Two legs, one failure meaning (findings H-1 / L-2 / R5-P): the backup's
    DB artifact is absent on disk (refused UP FRONT, before any live data is
    displaced), or the restore moves ran and the DB still did not land. In
    both cases the chosen ``mode_switch`` backup was NOT restored, so rather
    than return the success-shaped :class:`QuarantineRestoreResponse`
    (``restart_required=True``) and strand the buffered uploads behind a
    "looks fine" reply, the route fails loud. The
    :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this into a canonical 409
    ``ErrorEnvelope`` (``error.code='restore_noop'``) so the SDK raises
    :class:`phantom_client.errors.PhantomConflictError` and the operator
    retries. The interim backup (when one was taken) is named in the
    ``details`` so nothing is lost.

    Attributes:
        backup_id: Identity of the backup the operator asked to restore.
        instance_id: The instance whose live tree the restore targeted.
        interim_backup_db: Path of the DB backed up from the live tree before
            the restore attempt, or ``None`` when there was none (including
            the up-front refusal, which displaces nothing).
        interim_backup_body: Path of the body tree backed up before the
            restore attempt, or ``None`` when there was none.
        detail: One-line cause naming which leg refused.
    """

    def __init__(
        self,
        *,
        backup_id: UUID,
        instance_id: str,
        interim_backup_db: str | None,
        interim_backup_body: str | None,
        detail: str,
    ) -> None:
        super().__init__(
            f"Restore of mode_switch backup {backup_id} into instance "
            f"{instance_id!r} did not restore the backup: {detail}."
        )
        self.backup_id = backup_id
        self.instance_id = instance_id
        self.interim_backup_db = interim_backup_db
        self.interim_backup_body = interim_backup_body
        self.detail = detail


class LookupNotConfiguredError(Exception):
    """Raised when the by-captured-id lookup targets an unbound instance.

    ``GET /v1/admin/uploads/by-captured-id`` can only run where the
    deployment supplied the per-instance ``admin_lookup`` binding
    (``capture_name`` + ``json_path``); Phantom never guesses where an
    upstream identifier lives inside the captured values. The
    :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this into
    the canonical 400 ``ErrorEnvelope`` with
    ``error.code='lookup_not_configured'`` (cycle-7 task 4.3).

    Attributes:
        instance_ids: Every targeted instance missing the binding.
    """

    def __init__(self, *, instance_ids: tuple[str, ...]) -> None:
        joined = ", ".join(instance_ids)
        super().__init__(f"admin_lookup is not configured on instance(s): {joined}")
        self.instance_ids = instance_ids


class MultifileCursorConflictError(Exception):
    """Raised when ``GET /v1/admin/chains`` combines ``multifile_id`` with ``cursor``.

    The multifile listing is one-shot by design (ordered by
    ``send_order``, never paginated), so a cursor cannot apply to it.
    Its :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this
    into the canonical 422 ``multifile_cursor_conflict`` envelope.
    Pre-fix the refusal escaped as FastAPI's raw ``{"detail": ...}``
    body via bare ``HTTPException`` (round 2 defender fix R2-2).

    Attributes:
        multifile_id: The multifile set the request filtered on.
        cursor: The pagination token the request tried to combine.
    """

    def __init__(self, *, multifile_id: UUID, cursor: str) -> None:
        super().__init__(f"cursor {cursor!r} cannot be combined with multifile_id {multifile_id}")
        self.multifile_id = multifile_id
        self.cursor = cursor


class KeyValueMatchInvalidError(Exception):
    """Raised when a ``?key_value_match=`` value does not parse as ``key:value``.

    Its :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this
    into the canonical 422 ``key_value_match_invalid`` envelope.
    Pre-fix the refusal escaped as FastAPI's raw ``{"detail": ...}``
    body via bare ``HTTPException`` (round 2 defender fix R2-2).

    Attributes:
        raw: The query value exactly as supplied.
        reason: One-line parse failure cause.
    """

    def __init__(self, *, raw: str, reason: str) -> None:
        super().__init__(f"key_value_match {raw!r} is invalid: {reason}")
        self.raw = raw
        self.reason = reason


class BulkDeleteFilterEmptyError(Exception):
    """Raised when ``DELETE /v1/admin/chains`` carries an all-None filter body.

    An empty filter would mean "delete every row", which the bulk
    surface refuses by design (ADR-004). The
    :data:`ADMIN_ERROR_SPECS` entry (registered on the FastAPI app by
    :func:`register_admin_error_handlers`) converts this
    into the canonical 422 ``bulk_delete_filter_empty`` envelope.
    Pre-fix the refusal escaped as FastAPI's raw ``{"detail": ...}``
    body via bare ``HTTPException`` (round 2 defender fix R2-2).
    """

    def __init__(self) -> None:
        super().__init__("bulk delete requires a non-empty filter")


@dataclass(frozen=True)
class AdminErrorSpec[E: Exception]:
    """How one typed admin exception becomes an ``ErrorEnvelope``.

    The variation between the nine collapsed handlers was DATA, not logic: a
    code, a message built from the exception's own attributes, an optional
    details payload, and which instance the error concerns. Each handler was
    otherwise the same four lines around one :func:`_admin_error` call, and
    each needed its own ``type: ignore`` silencer because its narrow
    signature did not match ``add_exception_handler``'s base-``Exception``
    one.

    **The class is GENERIC on purpose.** Every message callable reads a NARROW
    attribute (``exc.instance_id``, ``exc.backup_id``, ``exc.instance_ids``),
    so a field typed over the base ``Exception`` would infer each lambda's
    parameter as ``Exception`` and reject the attribute access. Each entry in
    the table below is constructed at its own concrete type, so the lambdas
    check there; the table itself erases the parameter, because a mapping from
    an exception type to its own spec is a higher-kinded relation Python's
    type system cannot express.

    Attributes:
        code: The stable ADR-010 error code, which selects the HTTP status.
        message: Builds the operator-facing message from the exception.
        details: Builds the ADR-017 ``details`` payload, or ``None``.
        instance_id: Names the instance the error concerns; ``"unrouted"``
            when the error is not instance-scoped.
    """

    code: ErrorCode
    message: Callable[[E], str]
    details: Callable[[E], dict[str, Any] | None] = lambda _exc: None
    instance_id: Callable[[E], str] = lambda _exc: "unrouted"


ADMIN_ERROR_SPECS: dict[type[Exception], AdminErrorSpec[Any]] = {
    UnknownInstanceError: AdminErrorSpec[UnknownInstanceError](
        code="instance_unknown",
        message=lambda exc: f"Instance {exc.instance_id!r} not configured",
    ),
    NotFoundError: AdminErrorSpec[NotFoundError](
        code="not_found",
        message=lambda exc: exc.message,
    ),
    RestoreNoOpError: AdminErrorSpec[RestoreNoOpError](
        code="restore_noop",
        message=lambda exc: (
            f"Restore of backup {exc.backup_id} into instance {exc.instance_id} "
            f"did not restore the backup: {exc.detail}. Retry the restore "
            "(re-check GET /v1/admin/quarantine first). Any displaced live "
            "data was preserved as an interim mode_switch backup."
        ),
        details=lambda exc: {
            "backup_id": str(exc.backup_id),
            "instance_id": exc.instance_id,
            "interim_backup_db": exc.interim_backup_db,
            "interim_backup_body": exc.interim_backup_body,
            "cause": exc.detail,
        },
        instance_id=lambda exc: exc.instance_id,
    ),
    LookupNotConfiguredError: AdminErrorSpec[LookupNotConfiguredError](
        code="lookup_not_configured",
        message=lambda exc: (
            f"The by-captured-id lookup is not configured on instance(s) "
            f"{', '.join(exc.instance_ids)}: the deployment supplies the per-instance "
            "admin_lookup binding (capture_name + json_path); Phantom "
            "never guesses where the upstream identifier lives."
        ),
        details=lambda exc: {"unconfigured_instances": list(exc.instance_ids)},
        instance_id=lambda exc: exc.instance_ids[0] if exc.instance_ids else "unrouted",
    ),
    ReplayBodyDiscardedError: AdminErrorSpec[ReplayBodyDiscardedError](
        code="replay_body_discarded",
        message=lambda exc: (
            f"Replay refused for chain {exc.chain_id}: its body was "
            f"discarded at {exc.body_discarded_at.isoformat()} per the "
            "retention policy, so a replay would have nothing to send. "
            "The row is unchanged (sent_at preserved). Re-submit the "
            "upload through POST /v1/send if it must run again."
        ),
        details=lambda exc: {
            "chain_id": str(exc.chain_id),
            "body_discarded_at": exc.body_discarded_at.isoformat(),
        },
        instance_id=lambda exc: exc.instance_id,
    ),
    ReplayRefusedAttemptingError: AdminErrorSpec[ReplayRefusedAttemptingError](
        code="replay_refused_attempting",
        message=lambda exc: (
            f"Replay refused for chain {exc.chain_id}: a sender is "
            "actively driving the row (state 'attempting'), and a "
            "re-queue would clobber the in-flight attempt. The row is "
            "unchanged. Wait for the attempt to settle (or cancel the "
            "chain first), then retry the replay."
        ),
        details=lambda exc: {"chain_id": str(exc.chain_id)},
        instance_id=lambda exc: exc.instance_id,
    ),
    MultifileCursorConflictError: AdminErrorSpec[MultifileCursorConflictError](
        code="multifile_cursor_conflict",
        message=lambda _exc: (
            "cursor cannot be combined with multifile_id: multifile "
            "results are ordered by send_order and are not paginated "
            "(next_cursor is always null). Drop the cursor, or paginate "
            "without the multifile_id filter."
        ),
        details=lambda exc: {
            "multifile_id": str(exc.multifile_id),
            "cursor": exc.cursor,
        },
    ),
    KeyValueMatchInvalidError: AdminErrorSpec[KeyValueMatchInvalidError](
        code="key_value_match_invalid",
        message=lambda exc: (
            f"key_value_match {exc.raw!r} is invalid: {exc.reason}. "
            "Supply '<key>:<value>' with non-empty key and value."
        ),
        details=lambda exc: {"key_value_match": exc.raw},
    ),
    BulkDeleteFilterEmptyError: AdminErrorSpec[BulkDeleteFilterEmptyError](
        code="bulk_delete_filter_empty",
        message=lambda _exc: (
            "Bulk delete requires a non-empty filter: set at least one "
            "of state, route, since, or instance. An empty filter would "
            "delete every buffered row, which this surface refuses by "
            "design; delete individual chains via "
            "DELETE /v1/admin/chains/{chain_id} instead."
        ),
    ),
}
"""Every typed admin exception, and the envelope it becomes.

THE source of truth: :func:`register_admin_error_handlers` registers exactly
these keys, and :func:`_dispatch_admin_error` reads exactly these values, so a
new admin exception is one entry rather than a handler plus a registration
plus a silencer.
"""


async def _dispatch_admin_error(_request: Any, exc: Exception) -> Response:
    """Translate any spec'd admin exception into its canonical envelope.

    The ONE handler behind every entry in :data:`ADMIN_ERROR_SPECS`. Its
    signature is the one ``add_exception_handler`` is typed for, which is why
    the nine ``type: ignore`` silencers the bespoke handlers needed
    are gone: they existed only because each narrow signature disagreed with
    the registration's base-``Exception`` one.

    The lookup walks the MRO exactly as Starlette's own handler lookup does,
    so a subclass of a spec'd exception resolves to the same spec that got it
    routed here.

    Args:
        _request: The FastAPI request, unused (typed ``Any`` to keep imports
            light, as the bespoke handlers did).
        exc: The raised admin exception.

    Returns:
        The canonical ``ErrorEnvelope`` response for that exception's code.
    """
    spec = next(
        (ADMIN_ERROR_SPECS[klass] for klass in type(exc).__mro__ if klass in ADMIN_ERROR_SPECS),
        None,
    )
    if spec is None:  # pragma: no cover - only registered types reach this handler.
        raise exc
    return _admin_error(
        spec.code,
        spec.message(exc),
        instance_id=spec.instance_id(exc),
        details=spec.details(exc),
    )


async def request_validation_exception_handler(
    _request: Any,  # FastAPI Request, typed as Any to keep imports light
    exc: RequestValidationError,
) -> Response:
    """Translate FastAPI request-parameter validation into a 422 envelope.

    FastAPI raises :class:`RequestValidationError` when a typed path or
    query parameter fails coercion (a malformed UUID in
    ``/groups/{group_id}``, a missing required ``backup_id`` on the
    restore route). Without this handler the reply is FastAPI's raw
    ``{"detail": [...]}`` body, the one remaining escape from the
    ADR-017 envelope contract (round 6 defender fix R6-4; R1-1/R2-2
    closed the bare-``HTTPException`` escapes). The ``details`` payload
    carries FastAPI's per-parameter findings with the offending input
    values stripped (loc/msg/type only), keeping the envelope
    JSON-serializable for arbitrary inputs.
    """
    findings = [
        {
            "loc": list(err.get("loc", ())),
            "msg": str(err.get("msg", "")),
            "type": str(err.get("type", "")),
        }
        for err in exc.errors()
    ]
    return _admin_error(
        "request_invalid",
        "Request parameter validation failed; see details.errors.",
        details={"errors": findings},
    )


def register_admin_error_handlers(app: FastAPI) -> None:
    """Register every admin typed-error handler on ``app``.

    The single source of truth for the admin error-handler wiring
    (round 3 defender fix R3-1), now in two halves that say which is
    which: :data:`ADMIN_ERROR_SPECS` is the SOURCE (one entry per typed
    admin exception, carrying its code, message, details and instance),
    and the loop below is the WIRING (one registration per entry, all of
    them behind :func:`_dispatch_admin_error`). Adding an admin error is
    therefore one table entry, not a handler plus a registration plus a
    ``type: ignore``. The production app factory
    (``phantom.app.create_app``), the contract admin conftest, and
    every test fixture that mounts :data:`router` call this helper
    instead of registering handlers piecemeal, so an error added here
    reaches every tier at once and the registration sites cannot drift
    apart. ``RequestValidationError`` keeps its own bespoke handler: it
    is FastAPI's, not one of Phantom's typed admin exceptions, and it
    builds its details from ``exc.errors()`` rather than from a message
    template.

    Args:
        app: The FastAPI application mounting :data:`router`.
    """
    for exc_type in ADMIN_ERROR_SPECS:
        app.add_exception_handler(exc_type, _dispatch_admin_error)
    app.add_exception_handler(
        RequestValidationError,
        request_validation_exception_handler,  # type: ignore[arg-type]
    )


def _scope_instances(dispatcher: InstanceDispatcher, instance: str | None) -> list[InstanceContext]:
    """Return one instance if id is given, else every instance.

    Raises:
        UnknownInstanceError: When ``instance`` names an unconfigured
            instance. The app-level handler converts this into a 421
            ``ErrorEnvelope`` per plan §5.6.
    """
    if instance is None:
        return list(dispatcher.all_instances())
    ctx = dispatcher.by_id(instance)
    if ctx is None:
        raise UnknownInstanceError(instance)
    return [ctx]


def _resolve_single_instance(
    dispatcher: InstanceDispatcher, instance: str | None
) -> InstanceContext:
    """Resolve a single target instance for a destructive admin operation.

    Used by the quarantine restore route (plan § 1.5), which acts on exactly
    one instance's live tree. When ``instance`` is given it must name a
    configured instance; when omitted it defaults to the sole instance, and
    is otherwise required (a restore must say which live tree to write into).

    Args:
        dispatcher: The instance dispatcher.
        instance: The ``?instance=`` selector, or ``None``.

    Returns:
        The resolved :class:`InstanceContext`.

    Raises:
        UnknownInstanceError: When ``instance`` names an unconfigured
            instance (becomes a 421 ``ErrorEnvelope``).
        NotFoundError: When ``instance`` is omitted and more than one
            instance is configured, so the target is ambiguous (becomes a
            404 ``ErrorEnvelope``).
    """
    if instance is not None:
        ctx = dispatcher.by_id(instance)
        if ctx is None:
            raise UnknownInstanceError(instance)
        return ctx
    instances = dispatcher.all_instances()
    if len(instances) == 1:
        return instances[0]
    raise NotFoundError(
        "More than one instance is configured; specify ?instance=<id> to "
        "name which instance's live tree to restore into."
    )


async def _find_upload(dispatcher: InstanceDispatcher, chain_id: UUID) -> UploadRow | None:
    """Look up a row by chain_id across every instance."""
    _, row = await _find_upload_with_ctx(dispatcher, chain_id)
    return row


async def _find_upload_with_ctx(
    dispatcher: InstanceDispatcher, chain_id: UUID
) -> tuple[InstanceContext | None, UploadRow | None]:
    """Find ``(instance, row)`` across every instance (single store each)."""
    for ctx in dispatcher.all_instances():
        row = await ctx.store.get(chain_id)
        if row is not None:
            return ctx, row
    return None, None
