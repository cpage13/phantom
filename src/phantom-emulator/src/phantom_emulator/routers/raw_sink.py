"""Auth-free, token-free path-style sink - the Phase-1 forward-as-is oracle.

A deliberately naked ``/raw/{path:path}`` that 2xxs ANY forwarded upload verb
(PUT/POST/PATCH) - plus a ``GET`` read-back - with NO authentication and NO
token lookup: the forward-as-is analogue of the SigV4 validator in
:mod:`phantom_emulator.routers.s3`. Where ``s3.py`` validates a re-signed
upload (the Phase-4 oracle), this sink 2xxs a bare, unsigned, tokenless
upload (the Phase-1 oracle) so a catch-all that forwards a single upload with
no preceding mint step has a real upstream to hit. The upload-verb set mirrors
the catch-all's forwarded set (``UPLOAD_METHODS``); the stored body records the
inbound verb in :attr:`RawBody.method`.

The full forwarded path (no leading slash) is the store key - there is no
bucket/key split and no token. Bodies land in
:attr:`EmulatorState.raw_bodies`, distinct from the token-keyed
``accepted_bodies`` and the ``(bucket, key)``-keyed ``s3_objects`` so the
capture paths never collide.

REGISTRATION ORDER IS LOAD-BEARING (about the path template, not the verb
set): this router MUST be registered BEFORE the ``s3.py`` ``/{bucket}/{key:path}``
catch-all. Both templates are ``:path`` catch-alls and an upload to ``/raw/foo``
matches BOTH; Starlette resolves by registration-order first-match, so only when
``raw_sink`` is registered first does ``/raw/...`` reach this auth-free sink
(200) instead of the SigV4 validator (which 403s an unsigned upload). See
``plan_06_22.md`` TASK 0.5 Step C.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from phantom_emulator.routers._deps import UPLOAD_METHODS, get_state
from phantom_emulator.state import EmulatorState, RawBody

logger = logging.getLogger(__name__)

router = APIRouter()

StateDep = Annotated[EmulatorState, Depends(get_state)]


@router.api_route("/raw/{path:path}", methods=list(UPLOAD_METHODS))
async def put_raw(path: str, request: Request, state: StateDep) -> Response:
    """Store an unsigned, tokenless upload body keyed by the full forwarded path.

    2xxs ANY forwarded upload verb (PUT/POST/PATCH). NO ``_enforce_auth``, NO
    ``pending_uploads``/token lookup, NO SigV4 recompute - the
    deliberately-naked Phase-1 sink. The ``{path:path}`` convertor captures the
    whole forwarded path (slash-bearing keys included) as the
    :attr:`EmulatorState.raw_bodies` key; the inbound verb is recorded in
    :attr:`RawBody.method` and the inbound query string, which the convertor
    never captures, in :attr:`RawBody.query` so a test can observe whether the
    forwarder preserved it.

    Returns ``200`` (empty body) on store; ``413`` when the body exceeds the
    reused ``upstream.body_max_bytes`` cap (checked before the store so an
    oversized body is rejected without retaining it).
    """
    body = await request.body()
    if len(body) > state.cfg.upstream.body_max_bytes:
        raise HTTPException(status_code=413, detail="body exceeds upstream cap")
    all_headers = {k.lower(): v for k, v in request.headers.items()}
    state.raw_bodies[path] = RawBody(
        path=path,
        method=request.method,
        query=request.url.query,
        body=body,
        content_type=request.headers.get("content-type"),
        all_headers=all_headers,
        stored_at=datetime.now(UTC),
    )
    return Response(status_code=200)


@router.get("/raw/{path:path}")
async def get_raw(path: str, state: StateDep) -> Response:
    """Return the bytes stored under ``path`` by :func:`put_raw`.

    NO auth (the e2e read-back assertion must succeed without a signature).
    Returns the stored bytes on hit; ``404 NoSuchKey`` when absent.
    """
    obj = state.raw_bodies.get(path)
    if obj is None:
        raise HTTPException(status_code=404, detail="NoSuchKey")
    return Response(
        content=obj.body,
        media_type=obj.content_type or "application/octet-stream",
    )
