"""``PhantomClient`` — the SDK's public async facade.

The two-shape constructor accepts either a base URL string (the simple
path used by a thin upstream adapter) or a fully-configured :class:`ClientConfig`.
Internally everything routes through :class:`Transport`, which owns the
single :class:`httpx.AsyncClient`.

Public surface (re-exported from :mod:`phantom_client`):

- ``submit_chain`` — the canonical chain-submission method (ADR-010).
  No other names exist: ``send_chain``, ``send_request_chain``, and
  ``send_files`` are deliberate non-names.
- Admin: list/filter/replay/cancel/delete uploads, bulk delete,
  extract, export.tar, fetch body, fetch bundle, find by metadata.
- Group and identifier reads (cycle-7): :meth:`get_group_status`,
  :meth:`find_by_local_uuid`, and :meth:`find_by_captured_id` make
  every common "did my upload land" question a named one-call method.
- Token cache: list/push/invalidate slots (no bearer values ever
  returned).
- Status: health, ready, stats, admin status, instance status.
- Polling: :meth:`poll_until` and :meth:`poll_group_until_finished`
  wrap the standalone helpers for convenience.

The SDK is async-first; sync callers wrap with ``asyncio.run``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from types import TracebackType
from typing import Any, Self
from urllib.parse import quote
from uuid import UUID

import httpx

from phantom_client.config import ClientConfig, SubmitOptions
from phantom_client.errors import EmptyFilterError
from phantom_client.models.admin import (
    AdminStatusResponse,
    BulkDeleteResponse,
    ChainAdminDetail,
    CountersResponse,
    DeleteFilter,
    ExtractFilter,
    GaugesResponse,
    GroupStatusResponse,
    IdentifierLookupResponse,
    InstanceStatusResponse,
    InstanceSummary,
    ListUploadsResponse,
    ProfileRefCredBody,
    QuarantineInventoryResponse,
    QuarantineRestoreResponse,
    RamPressureStatusResponse,
    SigV4StaticCredBody,
    UploadBundle,
)
from phantom_client.models.chain import ChainEnvelope, ChainResponse
from phantom_client.models.status import (
    TERMINAL_STATES,
    HealthResponse,
    ReadyResponse,
    SortKey,
    StatsResponse,
    TokenSlot,
    UploadRow,
)
from phantom_client.poller import (
    DEFAULT_INITIAL_POLL_DELAY_SECONDS,
)
from phantom_client.poller import (
    poll_group_until_finished as _poll_group_until_finished,
)
from phantom_client.poller import (
    poll_until as _poll_until,
)
from phantom_client.transport import Transport

_LOG = logging.getLogger(__name__)

# Default read timeout used when the simple-constructor's ``timeout``
# kwarg is set. The kwarg is ignored when a full ClientConfig is passed.
_DEFAULT_READ_TIMEOUT_SECONDS = 30.0

# Admin path constants — single source of truth for the v1 admin surface.
_PATH_UPLOAD = "/v1/admin/chains/{chain_id}"
_PATH_UPLOADS = "/v1/admin/chains"
_PATH_UPLOAD_BODY = "/v1/admin/chains/{chain_id}/body"
_PATH_UPLOAD_BUNDLE = "/v1/admin/chains/{chain_id}/bundle"
_PATH_UPLOAD_REPLAY = "/v1/admin/chains/{chain_id}/replay"
_PATH_UPLOAD_CANCEL = "/v1/admin/chains/{chain_id}/cancel"
_PATH_UPLOADS_EXTRACT = "/v1/admin/chains/extract"
_PATH_EXPORT_TAR = "/v1/admin/export.tar"

# Cycle-7 group rollup + either-identifier lookups (plan § 6 task 5.1).
_PATH_GROUP_STATUS = "/v1/admin/groups/{group_id}"
_PATH_LOOKUP_BY_CAPTURED_ID = "/v1/admin/uploads/by-captured-id/{captured_id}"
_PATH_LOOKUP_BY_LOCAL_UUID = "/v1/admin/uploads/by-local-uuid/{local_uuid}"

_PATH_TOKENS = "/v1/admin/tokens"
_PATH_TOKEN_FOR = "/v1/admin/tokens/{endpoint}/{uid}"
_PATH_TOKEN_ENDPOINT = "/v1/admin/tokens/{endpoint}"

# Destination SigV4 credential push — host-keyed, the analogue of the
# per-(endpoint, uid) token slot above (the executor looks it up by host).
_PATH_CREDENTIAL_FOR = "/v1/admin/credentials/{dest_host}"

_PATH_STATS = "/v1/admin/stats"
# Liveness + readiness are the public, unprefixed probe paths (GET
# /v1/healthz, GET /v1/readyz). Phantom serves intake, admin, and health
# on one listener (loopback by default per ADR-004), so every path here
# rides the same base_url; these two just live outside the /v1/admin/
# prefix.
_PATH_HEALTH = "/v1/healthz"
_PATH_READY = "/v1/readyz"
_PATH_ADMIN_STATUS = "/v1/admin/status"
_PATH_INSTANCE_STATUS = "/v1/admin/instances/{instance_id}/status"
_PATH_INSTANCES = "/v1/admin/instances"

# Plan § 4.2.5 observability endpoints.
_PATH_OBSERVABILITY_COUNTERS = "/v1/admin/observability/counters"
_PATH_OBSERVABILITY_GAUGES = "/v1/admin/observability/gauges"
_PATH_OBSERVABILITY_RAM_PRESSURE = "/v1/admin/observability/ram_pressure"

# Plan § 5.2.5 quarantine inventory + § 1.5 restore.
_PATH_QUARANTINE = "/v1/admin/quarantine"
_PATH_QUARANTINE_RESTORE = "/v1/admin/quarantine/restore"


def _encode_key_value_match(key: str, value: str) -> str:
    """Encode one (key, value) pair for the ``?key_value_match=`` wire param.

    Plain keys ride the established ``key:value`` form (split server-side
    at the FIRST colon, so the value may contain colons). A key that the
    plain form cannot address, one containing a colon or beginning with
    a double quote, rides the quoted-key form the service parses:
    ``"<key>":<value>`` with backslash escapes for ``"`` and ``\\``
    inside the key (round 2 defender fix R2-3). Callers therefore never
    silently query the wrong key; every legal KVS key is addressable.
    """
    if ":" in key or key.startswith('"'):
        escaped = key.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}":{value}'
    return f"{key}:{value}"


class PhantomClient:
    """Async SDK facade over Phantom's HTTP surface.

    Two-shape constructor:

    - ``PhantomClient("http://phantom:8080")`` — base URL only.
    - ``PhantomClient(ClientConfig(...))`` — full configuration.

    Use as an async context manager to guarantee connection cleanup::

        async with PhantomClient("http://phantom:8080") as client:
            response = await client.submit_chain(envelope, body_refs)
    """

    def __init__(
        self,
        config_or_base_url: ClientConfig | str,
        *,
        timeout: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Build the client.

        Args:
            config_or_base_url: Either a complete :class:`ClientConfig`
                or a base URL string. When a string, a default
                :class:`ClientConfig` is synthesized with
                ``phantom_url=<string>``. Accepts the TCP form
                (``http://host:port``) or the Unix-domain-socket form
                (``unix:/abs/path.sock``); the UDS form is routed through
                a UDS transport automatically.
            timeout: When ``config_or_base_url`` is a string, overrides
                the default read timeout. Ignored when a
                :class:`ClientConfig` is passed (use
                :attr:`ClientConfig.timeouts` instead).
            transport: Optional :class:`httpx.AsyncBaseTransport` for
                test injection (e.g., :class:`httpx.ASGITransport`
                against an in-process Phantom). When set, the SDK uses
                it instead of opening real connections.
        """
        if isinstance(config_or_base_url, str):
            # ClientConfig has defaults for every field; mypy without the
            # Pydantic plugin can't see them, so the call-args are silenced.
            cfg = ClientConfig(phantom_url=config_or_base_url)  # type: ignore[call-arg]
            if timeout is not None:
                cfg = cfg.model_copy(
                    update={"timeouts": cfg.timeouts.model_copy(update={"read": timeout})}
                )
        else:
            if timeout is not None:
                _LOG.info(
                    "timeout=%.3f ignored because ClientConfig was passed; "
                    "set ClientConfig.timeouts.read instead",
                    timeout,
                )
            cfg = config_or_base_url
        self._config = cfg
        self._transport = Transport(cfg, transport=transport)

    # -----------------------------------------------------------------
    # Lifecycle.
    # -----------------------------------------------------------------

    async def __aenter__(self) -> Self:
        """Start the underlying transport."""
        await self._transport.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the underlying transport."""
        await self._transport.aclose()

    async def aclose(self) -> None:
        """Close the underlying transport. Idempotent."""
        await self._transport.aclose()

    # -----------------------------------------------------------------
    # Chain submission (the load-bearing operation).
    # -----------------------------------------------------------------

    async def submit_chain(
        self,
        envelope: ChainEnvelope,
        body_refs: dict[str, bytes] | None = None,
        *,
        uid: str | None = None,
        auth_token: str | None = None,
        options: SubmitOptions | None = None,
    ) -> ChainResponse:
        """Submit a chain envelope for buffered execution.

        Each chain submission MUST have a fresh ``envelope.chain_id``;
        re-using a chain_id triggers Phantom's idempotency-replay path
        (the existing row is returned). The SDK retries transport-class
        failures up to :class:`RetryPolicy.max_attempts`; every retry
        carries the same ``X-Phantom-Idempotency-Key`` so Phantom
        dedupes if it actually received the earlier attempt.

        Phantom routes the submission by the first step's URL (plus the
        optional ``options.instance_id`` override); there is no
        separate target header.

        Args:
            envelope: The chain to execute.
            body_refs: Bytes for each body_ref in the envelope, keyed
                by name. Required when the envelope contains any
                body_ref bodies; the keys must exactly match the
                ``name`` values declared in the envelope.
            uid: Maps to ``X-Phantom-Uid``. Defaults to
                ``ClientConfig.default_uid`` when omitted.
            auth_token: Full ``Authorization`` header value (e.g.,
                ``"Bearer <token>"``). Omitted when ``None``.
            options: Per-call submission overrides (grouping tags,
                instance override, idempotency key).

        Returns:
            The parsed :class:`ChainResponse` from Phantom's 202 reply.
        """
        effective_uid = uid if uid is not None else self._config.default_uid
        return await self._transport.submit_chain(
            envelope,
            body_refs,
            uid=effective_uid,
            auth_token=auth_token,
            options=options,
        )

    # -----------------------------------------------------------------
    # Status / polling / fetch.
    # -----------------------------------------------------------------

    async def get_upload(self, chain_id: UUID) -> ChainAdminDetail:
        """Return the current :class:`ChainAdminDetail` for ``chain_id``.

        The admin endpoint returns the extended detail shape (with
        ``body_location`` / ``attempts`` / ``last_error``) so operators
        and tests can inspect storage state. The wire shape on
        ``POST /v1/send`` (:class:`ChainResponse`) stays unchanged.
        """
        return await self._transport.get_json(
            _PATH_UPLOAD.format(chain_id=chain_id), model=ChainAdminDetail
        )

    async def list_uploads(
        self,
        *,
        state: str | None = None,
        route: str | None = None,
        multifile_id: UUID | None = None,
        group_id: UUID | None = None,
        since: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
        sort: SortKey = SortKey.NEXT_ATTEMPT_AT_ASC,
        instance: str | None = None,
        key_value_match: tuple[str, str] | None = None,
    ) -> tuple[list[UploadRow], str | None]:
        """List uploads matching the given filters.

        The ``multifile_id`` filter returns the multi-file set ordered by
        ``send_order``; it is one-shot (``next_cursor`` is always None and
        combining it with ``cursor`` is a server-side 422). ``group_id``
        filters by the query-grouping handle and paginates like every
        other filter.

        Returns:
            ``(rows, next_cursor)``. ``next_cursor`` is ``None`` when
            the result is the final page.
        """
        params: dict[str, Any] = {"limit": limit, "sort": sort.value}
        if state is not None:
            params["state"] = state
        if route is not None:
            params["route"] = route
        if multifile_id is not None:
            params["multifile_id"] = str(multifile_id)
        if group_id is not None:
            params["group_id"] = str(group_id)
        if since is not None:
            params["since"] = since.isoformat()
        if cursor is not None:
            params["cursor"] = cursor
        if instance is not None:
            params["instance"] = instance
        if key_value_match is not None:
            key, value = key_value_match
            params["key_value_match"] = _encode_key_value_match(key, value)
        envelope = await self._transport.get_json(
            _PATH_UPLOADS, model=ListUploadsResponse, params=params
        )
        return envelope.uploads, envelope.next_cursor

    async def find_by_metadata(
        self,
        *,
        key: str,
        value: str,
        instance: str | None = None,
    ) -> list[UploadRow]:
        """Return uploads whose ``metadata.key_value_store[key] == value``.

        Forwards to ``list_uploads`` with ``key_value_match`` set. Every
        legal KVS key is addressable exactly: a key containing a colon
        (or beginning with a double quote) is encoded on the wire via
        the service's quoted-key form transparently, so the lookup
        never queries the wrong key (round 2 defender fix R2-3).
        """
        rows, _ = await self.list_uploads(
            key_value_match=(key, value),
            instance=instance,
            limit=1000,
        )
        return rows

    async def poll_until(
        self,
        chain_id: UUID,
        *,
        terminal_states: frozenset[str] = TERMINAL_STATES,
        deadline: datetime | None = None,
        initial_delay_seconds: float = DEFAULT_INITIAL_POLL_DELAY_SECONDS,
    ) -> ChainAdminDetail:
        """Poll ``chain_id`` until terminal. See :func:`poll_until`."""
        return await _poll_until(
            self._transport,
            chain_id,
            terminal_states=terminal_states,
            deadline=deadline,
            initial_delay_seconds=initial_delay_seconds,
        )

    # -----------------------------------------------------------------
    # Group rollup + either-identifier lookups (cycle-7 task 5.1).
    # -----------------------------------------------------------------

    async def get_group_status(
        self, group_id: UUID, *, instance: str | None = None
    ) -> GroupStatusResponse:
        """Return the synthesized rollup for one query group.

        The rollup carries the member list, a per-state histogram, the
        structural ``all_finished`` flag (true iff no member is queued
        or attempting; ``auth_expired`` and ``corrupted`` count as
        finished), and the ``first_received_at`` / ``last_sent_at``
        joins. Every upload is a group of one by default (``group_id``
        falls back to ``chain_id`` at admission), so a bare
        ``chain_id`` resolves to its singleton group.

        Args:
            group_id: The query group's id (the value submitted as
                :attr:`SubmitOptions.group_id`, or a ``chain_id``).
            instance: Narrow the rollup to one instance's rows; omit to
                aggregate across every configured instance.

        Returns:
            The parsed :class:`GroupStatusResponse`.

        Raises:
            PhantomNotFoundError: When no upload anywhere (or anywhere
                within ``instance``) carries ``group_id``. The rollup
                is a resource fetch, so a miss 404s; the identifier
                lookups below answer misses with ``found=false``
                instead.
        """
        params: dict[str, Any] = {}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.get_json(
            _PATH_GROUP_STATUS.format(group_id=group_id),
            model=GroupStatusResponse,
            params=params,
        )

    async def find_by_local_uuid(
        self, local_uuid: UUID, *, instance: str | None = None
    ) -> IdentifierLookupResponse:
        """Look up uploads by their producer-minted local uuid.

        First-class promotion of the metadata key-value search: the
        ``phantom_local_uuid`` metadata key is pinned service-side, so
        callers never spell a key or a JSON path. A miss is a normal
        200 answer with ``found=false`` (a membership test, not a
        resource fetch).

        Args:
            local_uuid: The producer-minted uuid carried on the
                upstream request's metadata key-value store.
            instance: Narrow the search to one instance; omit to search
                every configured instance.

        Returns:
            The parsed :class:`IdentifierLookupResponse` with
            ``kind="local_uuid"``. Check ``found``, then read
            ``matches`` (a list because Phantom enforces no global
            uniqueness on the key; in practice zero or one entry).
        """
        params: dict[str, Any] = {}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.get_json(
            _PATH_LOOKUP_BY_LOCAL_UUID.format(local_uuid=local_uuid),
            model=IdentifierLookupResponse,
            params=params,
        )

    async def find_by_captured_id(
        self, value: str, *, instance: str | None = None
    ) -> IdentifierLookupResponse:
        """Look up uploads by the upstream-assigned captured identifier.

        Searches the captured values for the identifier the upstream
        minted (e.g. the real file id returned by the create step).
        WHERE that identifier lives is bound by per-instance deployment
        configuration (``admin_lookup``), so the service stays
        upstream-ignorant and this method takes only the value. A miss
        is a normal 200 answer with ``found=false``.

        Args:
            value: The opaque upstream-assigned identifier. Sent as a
                single path segment (percent-encoded here, decoded by
                the server).
            instance: Narrow the search to one instance; omit to search
                every configured instance.

        Returns:
            The parsed :class:`IdentifierLookupResponse` with
            ``kind="captured_file_id"``.

        Raises:
            PhantomBadRequestError: When any targeted instance lacks
                the ``admin_lookup`` binding (the service's
                ``lookup_not_configured`` envelope); it refuses rather
                than silently skipping an unconfigured instance.
        """
        params: dict[str, Any] = {}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.get_json(
            _PATH_LOOKUP_BY_CAPTURED_ID.format(captured_id=quote(value, safe="")),
            model=IdentifierLookupResponse,
            params=params,
        )

    async def poll_group_until_finished(
        self,
        group_id: UUID,
        *,
        deadline: datetime | None = None,
        initial_delay_seconds: float = DEFAULT_INITIAL_POLL_DELAY_SECONDS,
    ) -> GroupStatusResponse:
        """Poll ``group_id`` until ``all_finished``. See :func:`poll_group_until_finished`.

        The group twin of :meth:`poll_until`: submit several uploads
        sharing a ``group_id``, then make one call to wait for the
        whole set.
        """
        return await _poll_group_until_finished(
            self._transport,
            group_id,
            deadline=deadline,
            initial_delay_seconds=initial_delay_seconds,
        )

    async def fetch_body(self, chain_id: UUID) -> AsyncIterator[bytes]:
        """Stream the upload's body bytes as chunks."""
        return self._transport.stream_bytes(_PATH_UPLOAD_BODY.format(chain_id=chain_id))

    async def fetch_bundle(self, chain_id: UUID) -> UploadBundle:
        """Return metadata + body as a single :class:`UploadBundle`."""
        return await self._transport.get_json(
            _PATH_UPLOAD_BUNDLE.format(chain_id=chain_id), model=UploadBundle
        )

    async def extract(self, filter: ExtractFilter) -> AsyncIterator[bytes]:  # noqa: A002 — match plan signature
        """Stream a tar archive of every row matching the filter."""
        # Send the filter body via a streaming POST.
        client = self._transport._require_client()
        return self._extract_iter(client, filter)

    async def _extract_iter(
        self, client: httpx.AsyncClient, filter_: ExtractFilter
    ) -> AsyncIterator[bytes]:
        body = filter_.model_dump_json(by_alias=True)
        async with client.stream(
            "POST",
            _PATH_UPLOADS_EXTRACT,
            content=body,
            headers={"Content-Type": "application/json"},
        ) as response:
            if response.status_code >= 400:
                await response.aread()
                self._transport._raise_for_status(response)
            async for chunk in response.aiter_bytes():
                yield chunk

    async def export_tar(self) -> AsyncIterator[bytes]:
        """Stream the full ``/v1/admin/export.tar`` archive (ADR-005)."""
        return self._transport.stream_bytes(_PATH_EXPORT_TAR)

    # -----------------------------------------------------------------
    # Lifecycle ops.
    # -----------------------------------------------------------------

    async def replay(self, chain_id: UUID) -> UploadRow:
        """Re-queue a row for another upstream attempt.

        Raises:
            PhantomConflictError: With ``error_code ==
                'replay_body_discarded'`` when the row's body was
                already discarded per the retention policy
                (``body_discarded_at`` stamped); the row is left
                unchanged and the upload must be re-submitted through
                ``submit`` if it should run again. With ``error_code ==
                'replay_refused_attempting'`` when a sender is actively
                driving the row (state ``attempting``); the row is left
                unchanged, so wait for the attempt to settle (or
                ``cancel`` the chain first), then retry the replay.
        """
        return await self._transport.post_json(
            _PATH_UPLOAD_REPLAY.format(chain_id=chain_id),
            body=None,
            model=UploadRow,
        )

    async def cancel(self, chain_id: UUID) -> UploadRow:
        """Cancel a non-terminal row (transitions it to ``cancelled``)."""
        return await self._transport.post_json(
            _PATH_UPLOAD_CANCEL.format(chain_id=chain_id),
            body=None,
            model=UploadRow,
        )

    async def delete_upload(self, chain_id: UUID) -> None:
        """Delete one upload row (and its body)."""
        await self._transport.delete_no_body(_PATH_UPLOAD.format(chain_id=chain_id))

    async def bulk_delete(self, filter: DeleteFilter) -> int:  # noqa: A002 — match plan signature
        """Delete rows matching ``filter``.

        Pre-flights: an empty filter raises :class:`EmptyFilterError`
        without making the network call (server-side guard rejects
        empty filters too, but the SDK doesn't waste a round-trip).
        """
        if filter.is_empty():
            raise EmptyFilterError("bulk_delete refused: filter has no fields set")
        result = await self._transport.delete_json(
            _PATH_UPLOADS, body=filter, model=BulkDeleteResponse
        )
        return result.deleted

    # -----------------------------------------------------------------
    # Token cache (ADR-002, ADR-003, ADR-004).
    # -----------------------------------------------------------------

    async def list_tokens(
        self,
        *,
        endpoint: str | None = None,
        instance: str | None = None,
    ) -> list[TokenSlot]:
        """List token slots, optionally scoped to one endpoint/instance.

        Per ADR-004, bearer values are NEVER returned — each slot
        carries ``(endpoint, uid, last_updated, status)`` only.
        """
        params: dict[str, Any] = {}
        if endpoint is not None:
            params["endpoint"] = endpoint
        if instance is not None:
            params["instance"] = instance
        envelope = await self._transport.get_json(
            _PATH_TOKENS, model=_TokenListResponse, params=params
        )
        return envelope.tokens

    async def push_token(self, *, endpoint: str, uid: str, token: str) -> None:
        """Push a bearer into the ``(endpoint, uid)`` slot."""
        await self._transport.put_json(
            _PATH_TOKEN_FOR.format(endpoint=endpoint, uid=uid),
            body={"token": token},
        )

    async def push_token_for_endpoint(self, *, endpoint: str, token: str) -> None:
        """Push a bearer into every uid known for ``endpoint``."""
        await self._transport.put_json(
            _PATH_TOKEN_ENDPOINT.format(endpoint=endpoint),
            body={"token": token},
        )

    async def push_token_global(self, *, token: str) -> None:
        """Push a bearer into every slot regardless of endpoint."""
        await self._transport.put_json(_PATH_TOKENS, body={"token": token})

    async def invalidate_token(self, *, endpoint: str, uid: str) -> None:
        """Mark the ``(endpoint, uid)`` slot as bad (status=bad)."""
        await self._transport.delete_no_body(_PATH_TOKEN_FOR.format(endpoint=endpoint, uid=uid))

    async def invalidate_all_tokens(self) -> None:
        """Mark every slot as bad."""
        await self._transport.delete_no_body(_PATH_TOKENS)

    # -----------------------------------------------------------------
    # Destination credentials (SigV4 re-sign surface).
    # -----------------------------------------------------------------

    async def push_credential(
        self,
        *,
        dest_host: str,
        credential: SigV4StaticCredBody | ProfileRefCredBody,
    ) -> None:
        """Provision a destination SigV4 credential for ``dest_host`` (admin, loopback).

        Mirrors the server's ``PUT /v1/admin/credentials/{dest_host}`` (the SigV4
        analogue of :meth:`push_token`). The body is the discriminated
        ``CredentialPushBody``; ``dest_host`` is the real upstream host the
        credential signs for (e.g. ``"s3.amazonaws.com"``). Returns nothing
        (server replies ``204``; the secret is never echoed). A fresh push wakes
        any parked ``auth_expired`` rows on that host.

        Args:
            dest_host: Destination host key (normalized server-side via the same
                ``_hostname`` rule the executor uses for lookup).
            credential: A :class:`SigV4StaticCredBody` (static key-pair) or
                :class:`ProfileRefCredBody` (profile / default chain). Construct it
                with a :class:`SigningService` member (e.g. ``service=SigningService.S3``),
                NOT a raw string — the client model is strict and has no coercer.

        Example:
            >>> await client.push_credential(
            ...     dest_host="s3.amazonaws.com",
            ...     credential=SigV4StaticCredBody(
            ...         access_key_id="AKIA...", secret_access_key="wJal...",
            ...         region="us-east-1", service=SigningService.S3,
            ...     ),
            ... )
        """
        await self._transport.put_json(
            _PATH_CREDENTIAL_FOR.format(dest_host=quote(dest_host, safe="")),
            body=credential,
        )

    # -----------------------------------------------------------------
    # Status / health / stats.
    # -----------------------------------------------------------------

    async def get_stats(self, *, instance: str | None = None) -> StatsResponse:
        """Return the ``/v1/admin/stats`` snapshot."""
        params: dict[str, Any] = {}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.get_json(_PATH_STATS, model=StatsResponse, params=params)

    async def get_health(self) -> HealthResponse:
        """Return Phantom's health snapshot."""
        return await self._transport.get_json(_PATH_HEALTH, model=HealthResponse)

    async def get_ready(self) -> ReadyResponse:
        """Return Phantom's readiness snapshot."""
        return await self._transport.get_json(_PATH_READY, model=ReadyResponse)

    async def get_admin_status(self) -> AdminStatusResponse:
        """Return the aggregate admin-status view (ADR-007)."""
        return await self._transport.get_json(_PATH_ADMIN_STATUS, model=AdminStatusResponse)

    async def get_instance_status(self, instance_id: str) -> InstanceStatusResponse:
        """Return one instance's status (ADR-007)."""
        return await self._transport.get_json(
            _PATH_INSTANCE_STATUS.format(instance_id=instance_id),
            model=InstanceStatusResponse,
        )

    async def list_instances(self) -> list[InstanceSummary]:
        """List configured instances."""
        envelope = await self._transport.get_json(_PATH_INSTANCES, model=_InstanceListResponse)
        return envelope.instances

    # -----------------------------------------------------------------
    # Observability (plan § 4.2.5).
    # -----------------------------------------------------------------

    async def get_observability_counters(self) -> CountersResponse:
        """Return the process-wide MetricsRegistry counters snapshot."""
        return await self._transport.get_json(_PATH_OBSERVABILITY_COUNTERS, model=CountersResponse)

    async def get_observability_gauges(self) -> GaugesResponse:
        """Return the process-wide MetricsRegistry gauges snapshot."""
        return await self._transport.get_json(_PATH_OBSERVABILITY_GAUGES, model=GaugesResponse)

    async def get_observability_ram_pressure(self) -> RamPressureStatusResponse:
        """Return the RAM-pressure status aggregated across instances."""
        return await self._transport.get_json(
            _PATH_OBSERVABILITY_RAM_PRESSURE, model=RamPressureStatusResponse
        )

    # -----------------------------------------------------------------
    # Quarantine inventory (plan § 5.2.5).
    # -----------------------------------------------------------------

    async def get_quarantine_inventory(
        self, *, instance: str | None = None
    ) -> QuarantineInventoryResponse:
        """Return the backups (one entry each, plus anomalies) per instance.

        Plan § 5.2.5 / cycle-7 seam 2. One entry per BACKUP, keyed by
        ``backup_id`` (the restore handle), with ``has_db`` / ``has_body``
        reporting which artifacts are on disk; unmanifested artifacts
        surface as flagged anomaly entries. Empty on a clean deployment.

        Args:
            instance: Scope to one instance's per-instance data root; omit to
                aggregate across every configured instance.
        """
        params: dict[str, Any] = {}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.get_json(
            _PATH_QUARANTINE, model=QuarantineInventoryResponse, params=params
        )

    async def restore_quarantine_backup(
        self, *, backup_id: UUID, instance: str | None = None
    ) -> QuarantineRestoreResponse:
        """Restore a ``mode_switch`` backup pair back into the live tree.

        Plan § 1.5 / cycle-7 seam 2. The backup is addressed by IDENTITY
        (``backup_id`` as a query parameter); the service resolves it through
        the backup's manifest and never string-matches filenames. Backs up
        any current live data first (clobber-safe), then moves the chosen
        backup in. The restore is STAGED on disk; the response carries
        ``restart_required=True`` because the running store only serves the
        restored data after a restart in a disk-backed mode.

        Args:
            backup_id: The backup identity (from
                :attr:`QuarantineEntry.backup_id`) naming the ``mode_switch``
                backup to restore. Anomaly entries have no ``backup_id`` and
                cannot be restored.
            instance: The instance whose live tree to restore into. Required
                when more than one instance is configured; defaults to the
                sole instance otherwise.

        Raises:
            PhantomNotFoundError: When ``backup_id`` names no restorable
                ``mode_switch`` backup, or ``instance`` is omitted with
                multiple instances.
            PhantomConflictError: When the backup's DB half is absent on disk
                or did not land in the live tree (the service's 409
                ``restore_noop`` envelope); the backup was not restored.
        """
        params: dict[str, Any] = {"backup_id": str(backup_id)}
        if instance is not None:
            params["instance"] = instance
        return await self._transport.post_json(
            _PATH_QUARANTINE_RESTORE,
            body=None,
            model=QuarantineRestoreResponse,
            params=params,
        )


# ---------------------------------------------------------------------------
# Internal list-wrapper models — Phantom's token-list and instance-list
# replies are envelope-style but the SDK doesn't expose the envelope types
# (only the contained items). ListUploadsResponse is a public model and
# lives in phantom_client.models.admin so the contract test can reach it.
# ---------------------------------------------------------------------------

from pydantic import BaseModel, ConfigDict, Field  # noqa: E402 — late-bound by design


class _TokenListResponse(BaseModel):
    """Envelope for ``GET /v1/admin/tokens``."""

    model_config = ConfigDict(strict=True, extra="ignore")

    tokens: list[TokenSlot] = Field(
        default_factory=list,
        description="Slot views (no bearer values) of every token-cache entry.",
    )


class _InstanceListResponse(BaseModel):
    """Envelope for ``GET /v1/admin/instances``."""

    model_config = ConfigDict(strict=True, extra="ignore")

    instances: list[InstanceSummary] = Field(
        default_factory=list,
        description="Per-instance headline figures.",
    )


__all__ = ["PhantomClient"]
