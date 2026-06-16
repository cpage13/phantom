"""Two-step-upload upstream endpoints.

Two production endpoints + two stubs:

- ``POST /v1/files/create`` (also served at ``POST /v2/files``) —
  accepts JSON, mints a fresh upload-handle id + presigned
  upload URL, dedups on ``Idempotency-Key``, preserves
  ``metadata.keyValueStore``.
- ``PUT /v1/files/upload/{token}`` — accepts a body, stores it on
  :class:`EmulatorState.accepted_bodies` along with any
  ``x-amz-meta-*`` request headers.
- ``GET /v1/files/{file_id}`` — stub returning the same
  upload-handle payload the create call minted.
- ``POST /v1/files/search`` — stub returning ``[]``.

The ``/v2/files`` alias on the create-file endpoint exists because a
real upstream adapter may build its two-step chain envelope with the
create-file step targeting ``{files_api}/v2/files`` rather than
``/v1/files/create``. The emulator emulates a two-step file-upload
API, so it must answer the path a real upstream adapter sends. Both
paths resolve to the same :func:`create_file` handler, so the
idempotency cache, failure middleware, and ``/control/received`` log
behave identically regardless of which path the client used.

See plan §4.11.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from phantom_emulator.auth.modes import AuthModePolicy, authenticate
from phantom_emulator.routers._deps import get_state
from phantom_emulator.state import (
    AcceptedBody,
    EmulatorState,
    IdempotencyEntry,
)
from phantom_emulator.upload.correlation import echo_metadata_kvs, extract_metadata_kvs
from phantom_emulator.upload.presigned import PresignedTokenStore

logger = logging.getLogger(__name__)

router = APIRouter()

StateDep = Annotated[EmulatorState, Depends(get_state)]

# Idempotency-Key request header name (canonical per RFC 9110-style).
IDEMPOTENCY_HEADER: str = "idempotency-key"


def _resolve_auth_policy(state: EmulatorState, path: str) -> AuthModePolicy:
    """Return the :class:`AuthModePolicy` that governs ``path``.

    Per-scope overrides on :attr:`EmulatorState.auth_mode_overrides`
    win; the configured default is the fallback.
    """
    mode = state.auth_mode_overrides.get(path, state.cfg.auth.default_mode)
    return AuthModePolicy(
        mode=mode,
        plain_bearer_allowlist=frozenset(state.plain_bearer_allowlist),
        api_key_secret=state.api_key_secret,
        static_jwt=state.static_jwt,
    )


def _enforce_auth(request: Request, state: EmulatorState) -> None:
    """Raise ``401`` when the inbound request fails the configured policy."""
    if state.jwt_minter is None:
        raise HTTPException(status_code=500, detail="jwt_minter not initialized")
    policy = _resolve_auth_policy(state, request.url.path)
    headers = dict(request.headers.items())
    if not authenticate(headers, policy, state.jwt_minter):
        raise HTTPException(status_code=401, detail="invalid_token")


def _presigned_store(state: EmulatorState, base_url: str) -> PresignedTokenStore:
    """Lazily build the per-request presigned URL store.

    The store keeps state in its own ``_pending`` dict but writes back
    to :attr:`EmulatorState.pending_uploads` and
    :attr:`EmulatorState.file_id_to_token` after each mint so the GET
    stub and the PUT path can resolve tokens.
    """
    ttl = state.cfg.upstream.presigned_ttl_seconds
    store = PresignedTokenStore(base_url=base_url, default_ttl_seconds=ttl)
    # Hydrate the store from existing pending uploads so a fresh
    # request can still resolve previously-minted tokens.
    store.pending.update(state.pending_uploads)
    return store


def _maybe_cached_response(
    state: EmulatorState,
    idem_key: str | None,
    now: datetime,
) -> tuple[dict[str, Any], str] | None:
    """Return cached (response_json, upload_token) if the key is fresh."""
    if idem_key is None:
        return None
    entry = state.idempotency_cache.get(idem_key)
    if entry is None:
        return None
    if entry.expires_at <= now:
        del state.idempotency_cache[idem_key]
        return None
    return entry.response_json, entry.upload_token


@router.post("/v1/files/create")
@router.post("/v2/files")
async def create_file(
    request: Request,
    state: StateDep,
) -> JSONResponse:
    """Two-step-upload create-file endpoint.

    Served at both ``/v1/files/create`` and ``/v2/files``: the
    latter is a path some upstream adapters build into their
    create-file chain step, so the emulator must answer it for a
    real upstream adapter to reach the upstream. Both paths run this
    same handler.

    Reads the JSON body (tolerantly — invalid JSON returns 400),
    deduplicates on ``Idempotency-Key`` when present, mints a fresh
    upload-handle payload + presigned upload URL, and stamps
    ``metadata.keyValueStore`` back onto the response so
    ``phantom_local_uuid`` round-trips.
    """
    _enforce_auth(request, state)

    body_raw = await request.body()
    try:
        body_json: dict[str, Any] = json.loads(body_raw) if body_raw else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON: {exc}") from exc

    if not isinstance(body_json, dict):
        raise HTTPException(status_code=400, detail="request body must be a JSON object")

    now = datetime.now(UTC)
    idem_key = request.headers.get(IDEMPOTENCY_HEADER)

    cached = _maybe_cached_response(state, idem_key, now)
    if cached is not None:
        cached_json, cached_token = cached
        logger.info("idempotency hit key=%s token=%s", idem_key, cached_token[:8])
        return JSONResponse(cached_json)

    kvs = extract_metadata_kvs(body_json)
    file_id = uuid4()
    file_information: dict[str, Any] = {
        "id": str(file_id),
        "name": str(body_json.get("fileName", "")),
        "domain": str(body_json.get("domain", "")),
        "laneBaseName": str(body_json.get("laneBaseName", "")),
    }
    echo_metadata_kvs(file_information, kvs)

    base_url = str(request.base_url).rstrip("/")
    store = _presigned_store(state, base_url=base_url)
    upload_token, upload_url, record = store.mint(
        file_id=file_id,
        file_information=file_information,
        metadata_kvs=kvs,
        now=now,
    )
    # A real upstream mirrors the upload URL on the file payload as well.
    file_information["contentUrl"] = upload_url
    record.file_information = file_information

    # Persist on the shared state.
    state.pending_uploads[upload_token] = record
    state.file_id_to_token[file_id] = upload_token

    response_json: dict[str, Any] = {
        "fileInformation": file_information,
        "uploadUrl": upload_url,
    }

    if idem_key is not None:
        ttl = state.cfg.upstream.idempotency_dedup_window_seconds
        state.idempotency_cache[idem_key] = IdempotencyEntry(
            key=idem_key,
            response_json=response_json,
            upload_token=upload_token,
            file_id=file_id,
            expires_at=now + timedelta(seconds=ttl),
        )

    logger.info(
        "create_file file_id=%s upload_token=%s kvs_count=%d",
        file_id,
        upload_token[:8],
        len(kvs),
    )
    return JSONResponse(response_json)


@router.put("/v1/files/upload/{token}")
async def put_upload(
    token: str,
    request: Request,
    state: StateDep,
) -> Response:
    """Accept a body for a previously-minted upload token.

    Returns ``403`` for unknown or expired tokens; ``413`` if the body
    exceeds :attr:`UpstreamCfg.body_max_bytes`. Stores the bytes plus
    any ``x-amz-meta-*`` headers on :class:`EmulatorState`.
    """
    pending = state.pending_uploads.get(token)
    if pending is None:
        raise HTTPException(status_code=403, detail="SignatureDoesNotMatch")

    # Honor a per-policy signature-mismatch override.
    fs = state.failure_state
    if fs is not None:
        policy = fs.resolve(_scope_for_policy_lookup(token))
        if policy is not None and policy.presigned_signature_mismatch:
            raise HTTPException(status_code=403, detail="SignatureDoesNotMatch")

    now = datetime.now(UTC)
    expires_at = pending.created_at + timedelta(seconds=pending.presigned_ttl_seconds)
    if now >= expires_at:
        raise HTTPException(status_code=403, detail="SignatureDoesNotMatch")

    body = await request.body()
    if len(body) > state.cfg.upstream.body_max_bytes:
        raise HTTPException(status_code=413, detail="body exceeds upstream cap")

    x_amz_meta: dict[str, str] = {}
    all_headers: dict[str, str] = {}
    for key, value in request.headers.items():
        lower = key.lower()
        all_headers[lower] = value
        if lower.startswith("x-amz-meta-"):
            x_amz_meta[lower] = value

    state.accepted_bodies[token] = AcceptedBody(
        upload_token=token,
        body=body,
        headers=x_amz_meta,
        content_encoding=request.headers.get("content-encoding"),
        all_headers=all_headers,
        accepted_at=now,
    )

    # Record the matching idempotency key on the accepted body for
    # /control/received introspection.
    for entry in state.idempotency_cache.values():
        if entry.upload_token == token:
            state.accepted_idempotency_keys[token] = entry.key
            break

    logger.info("put_upload token=%s body=%d bytes", token[:8], len(body))
    return Response(status_code=200)


@router.get("/v1/files/{file_id}")
async def get_file(
    file_id: UUID,
    request: Request,
    state: StateDep,
) -> JSONResponse:
    """Resolve a file id to its create-time ``FileInformation`` payload."""
    _enforce_auth(request, state)
    token = state.file_id_to_token.get(file_id)
    if token is None:
        raise HTTPException(status_code=404, detail="file not found")
    pending = state.pending_uploads.get(token)
    if pending is None:
        raise HTTPException(status_code=404, detail="file not found")
    return JSONResponse(pending.file_information)


@router.post("/v1/files/search")
async def search_files(
    request: Request,
    state: StateDep,
) -> JSONResponse:
    """Search-files stub. Returns an empty list."""
    _enforce_auth(request, state)
    return JSONResponse({"results": []})


def _scope_for_policy_lookup(_token: str) -> Any:
    """Return the FailureScope used for upload-PUT policy lookup."""
    from phantom_emulator.failure.injection import FailureScope

    return FailureScope.UPSTREAM_FILES_UPLOAD
