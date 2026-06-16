"""Public, test-owned driver for the cross-package E2E suite.

This module models an upstream client of an upstream file-upload service
so the E2E suite can drive ``phantom`` end to end without depending on any
external client SDK. It provides three pieces:

* :func:`build_in_memory_upload_envelope` — builds the two-step upload
  chain envelope on the public ``phantom_client.models.chain`` shapes and
  the test-owned request model below.
* :class:`CreateFileRequest` / :class:`FileMetadata` — the request shapes
  the upstream client would send, carrying the camelCase wire
  serialization (``laneBaseName``, ``fileName``, ``keyValueStore``).
* :class:`PhantomDriver` — a small driver that mints a ``local_uuid``,
  deep-copies the request, stuffs ``phantom_local_uuid`` into the metadata
  KVS, builds the two-step chain envelope, and submits it via
  :meth:`phantom_client.PhantomClient.submit_chain`. It returns a simple
  :class:`DriverUploadResult` (``id`` + ``metadata``).

Why this exists. The E2E suite drives ``phantom`` (the generic service)
via ``phantom-client`` plus this small test-owned driver, modeling the
two-step "create file metadata, then PUT the bytes to a presigned URL"
upload shape an upstream client would perform.

Wire-shape fidelity is REQUIRED and load-bearing: Phantom's admin
JSONPath search hits ``$.steps[0].body.value.metadata.keyValueStore.<key>``
and the emulator's failure-injection + received-log surfaces depend on
the exact camelCase field names and the exact two-step shape this module
emits. The exact wire shape below is intentional, not incidental.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final
from uuid import UUID, uuid4

from phantom_client import PhantomClient
from phantom_client.config import SubmitOptions
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainResponse,
    ChainStep,
)
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Test-owned request model (camelCase, as the upstream client would send).
# ---------------------------------------------------------------------------


def _alias_generator(name: str) -> str:
    """Camel-case alias generator for the upstream wire form.

    Trailing underscores (reserved-keyword escapes) are stripped before
    camel-casing; everything else goes straight through
    ``pydantic.alias_generators.to_camel``. Keeping this identical to the
    upstream generator is what makes the JSON body Phantom and the
    emulator see byte-identical to the upstream client's request.
    """
    if name.endswith("_"):
        return to_camel(name[:-1])
    return to_camel(name)


class _CamelAliasModel(BaseModel):
    """Pydantic base supporting camelCase aliases on the wire."""

    model_config = ConfigDict(alias_generator=_alias_generator, populate_by_name=True)


class FileMetadata(_CamelAliasModel):
    """User-defined metadata for a file entry.

    Models the upstream client's metadata shape. The ``key_value_store``
    field serializes by alias as ``keyValueStore`` — Phantom's admin
    JSONPath search depends on that exact camelCase name.
    """

    key_value_store: dict[str, str] = Field(
        ...,
        description="Searchable user-defined metadata keys (C-style identifiers).",
    )


class CreateFileRequest(_CamelAliasModel):
    """Create-file request as the upstream client would send it.

    Carries only the fields the E2E suite exercises plus the optional
    ``created_timestamp_override``. ``by_alias`` serialization yields
    ``domain`` / ``laneBaseName`` / ``fileName`` / ``metadata`` (with
    nested ``keyValueStore``) — the exact wire shape the upstream client
    emits.
    """

    domain: str = Field(..., description="The domain for the file, e.g. `generic`.")
    lane_base_name: str = Field(..., description="The domainless lane name.")
    file_name: str = Field(..., description="The file name (need not be unique).")
    metadata: FileMetadata = Field(..., description="User-defined metadata.")
    created_timestamp_override: datetime | None = Field(
        default=None,
        description="If null, the file timestamp is set to the time of upload.",
    )


# ---------------------------------------------------------------------------
# Envelope builder for the two-step upload chain.
# ---------------------------------------------------------------------------

# The upstream presigned-URL TTL maximum is 7 days. Captures of
# ``upload_url`` and ``file_information`` are bounded by this window; what
# Phantom does when an upload's PUT lands after the capture has expired is
# governed by ADR-011's ``capture_reexecution`` per-instance YAML knob
# (owned by the service, not by this driver).
_PRESIGNED_URL_TTL_SECONDS: Final[int] = 7 * 24 * 60 * 60  # 604_800

# The upstream metadata-POST upload path (relative to the files_api base).
_FILES_V2_PATH: Final[str] = "/v2/files"

# Step names — visible in every captured-values admin query response.
_STEP_CREATE_FILE: Final[str] = "create_file"
_STEP_PUT_S3: Final[str] = "put_s3"

# The body-ref name; rides as multipart part ``body_refs[body]``.
_BODY_REF_NAME: Final[str] = "body"

# Per-step capture names + JSONPath expressions (camelCase response fields).
_CAPTURE_UPLOAD_URL_NAME: Final[str] = "upload_url"
_CAPTURE_UPLOAD_URL_PATH: Final[str] = "$.uploadUrl"
_CAPTURE_FILE_INFO_NAME: Final[str] = "file_information"
_CAPTURE_FILE_INFO_PATH: Final[str] = "$.fileInformation"

# The placeholder Phantom substitutes when running step 2.
_STEP_TWO_URL_TEMPLATE: Final[str] = f"{{{{{_STEP_CREATE_FILE}.{_CAPTURE_UPLOAD_URL_NAME}}}}}"

# The header name the upstream metadata API uses for idempotency dedup.
# Step 2 is a presigned-URL PUT - URL-idempotent and needs no header - so
# the value below is paired with step 1 only.
_IDEMPOTENCY_HEADER: Final[str] = "Idempotency-Key"

# S3 metadata-header prefix. Each key in ``metadata.key_value_store``
# becomes ``x-amz-meta-<key>`` with underscores rewritten to hyphens, so
# the presigned-URL signature continues to validate.
_AMZ_META_PREFIX: Final[str] = "x-amz-meta-"


def build_in_memory_upload_envelope(
    *,
    request: CreateFileRequest,
    files_api_base: str,
    local_uuid: UUID,
) -> tuple[ChainEnvelope, dict[str, bytes]]:
    """Build the two-step upload chain envelope.

    Built on the public :mod:`phantom_client.models.chain` shapes and the
    test-owned :class:`CreateFileRequest`. The returned ``body_refs`` is
    intentionally empty; the caller populates it with
    ``{"body": <contents>}`` immediately before submission.

    Args:
        request: A :class:`CreateFileRequest` whose
            ``metadata.key_value_store`` has already been augmented with
            ``phantom_local_uuid`` (caller's responsibility).
        files_api_base: Upstream Files API base URL. Trailing slashes
            tolerated.
        local_uuid: The minted upload identifier. Used as ``chain_id`` and
            as the source for ``idempotency_key`` (stable across retries).

    Returns:
        ``(envelope, body_refs)`` — ``body_refs`` empty here.
    """
    step_one = _build_create_file_step(request=request, files_api_base=files_api_base)
    step_two = _build_put_s3_step(request=request)
    envelope = ChainEnvelope(
        chain_id=local_uuid,
        idempotency_key=str(local_uuid),
        steps=[step_one, step_two],
        default_target=None,
    )
    body_refs: dict[str, bytes] = {}
    return envelope, body_refs


def _build_create_file_step(*, request: CreateFileRequest, files_api_base: str) -> ChainStep:
    """Build the ``create_file`` step (the upstream metadata POST).

    The body is the :class:`CreateFileRequest` serialized as JSON in its
    camelCase wire form (``by_alias=True``), matching what the upstream
    client sends.
    """
    url = files_api_base.rstrip("/") + _FILES_V2_PATH
    body_value = request.model_dump(by_alias=True, mode="json")
    return ChainStep(
        name=_STEP_CREATE_FILE,
        method="POST",
        url=url,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        body=ChainBodyJson(kind="json", value=body_value),
        capture=[
            _make_capture(
                name=_CAPTURE_UPLOAD_URL_NAME,
                from_path=_CAPTURE_UPLOAD_URL_PATH,
                ttl_seconds=_PRESIGNED_URL_TTL_SECONDS,
                sensitive=True,
            ),
            _make_capture(
                name=_CAPTURE_FILE_INFO_NAME,
                from_path=_CAPTURE_FILE_INFO_PATH,
                ttl_seconds=_PRESIGNED_URL_TTL_SECONDS,
                sensitive=False,
            ),
        ],
        idempotency_header=_IDEMPOTENCY_HEADER,
    )


def _make_capture(
    *,
    name: str,
    from_path: str,
    ttl_seconds: int,
    sensitive: bool = False,
) -> ChainCapture:
    """Construct a :class:`ChainCapture` with its alias-named ``from`` field.

    The field is declared with ``alias="from"`` (``from`` is a Python
    keyword) and ``populate_by_name=True``; construct via
    :meth:`model_validate` to avoid mypy's mis-inference about the
    canonical kwarg. The ``sensitive`` flag marks captures whose values
    (e.g. the presigned-PUT ``upload_url``) must be redacted from logs.
    """
    return ChainCapture.model_validate(
        {
            "name": name,
            "from": from_path,
            "ttl_seconds": ttl_seconds,
            "sensitive": sensitive,
        }
    )


def _build_put_s3_step(*, request: CreateFileRequest) -> ChainStep:
    """Build the ``put_s3`` step (presigned S3 PUT of the file bytes).

    The URL is the templated ``{{create_file.upload_url}}``; Phantom
    substitutes the value at execution time. The body rides as a
    :class:`ChainBodyRef` named ``"body"``. ``x-amz-meta-*`` headers are
    derived from the request's ``metadata.key_value_store`` with the
    underscore-to-hyphen substitution the upstream client applies.
    """
    headers = _build_s3_meta_headers(request)
    return ChainStep(
        name=_STEP_PUT_S3,
        method="PUT",
        url=_STEP_TWO_URL_TEMPLATE,
        headers=headers,
        body=ChainBodyRef(
            kind="body_ref",
            name=_BODY_REF_NAME,
            content_type="application/octet-stream",
        ),
        capture=[],
        idempotency_header=None,
    )


def _build_s3_meta_headers(request: CreateFileRequest) -> dict[str, str]:
    """Map ``metadata.key_value_store`` entries to ``x-amz-meta-*`` headers.

    Keys are lowered with underscores rewritten to hyphens; values pass
    through unchanged. Output ordering follows the input dict's insertion
    order.
    """
    headers: dict[str, str] = {}
    for key, value in request.metadata.key_value_store.items():
        headers[_AMZ_META_PREFIX + key.replace("_", "-")] = value
    return headers


# ---------------------------------------------------------------------------
# uid derivation - structural JWT `sub` extraction. No signature check.
# ---------------------------------------------------------------------------

_B64_DECODE_ERRORS: Final[tuple[type[BaseException], ...]] = (binascii.Error, ValueError)
_JSON_DECODE_ERRORS: Final[tuple[type[BaseException], ...]] = (ValueError, UnicodeDecodeError)


def derive_uid(token: str | None, *, fallback: str | None) -> str:
    """Extract the JWT ``sub`` claim from a bearer token.

    Models the upstream client's uid derivation: split on ``.``,
    base64url-decode the payload segment, JSON-parse it, read ``sub`` -
    structural extraction only, no signature verification. Returns
    ``fallback`` when the token is missing or unparseable; raises
    :class:`ValueError` if extraction fails and no fallback was supplied.
    """
    extracted = _try_extract_sub(token)
    if extracted is not None:
        return extracted
    if fallback is not None:
        return fallback
    raise ValueError("Cannot derive uid: token missing or non-JWT and no fallback set")


def _try_extract_sub(token: str | None) -> str | None:
    """Best-effort extraction of the JWT ``sub`` claim; ``None`` on failure."""
    if token is None:
        return None
    stripped = token.strip()
    if not stripped:
        return None
    if stripped.startswith("Bearer "):
        stripped = stripped[len("Bearer ") :].strip()
        if not stripped:
            return None
    parts = stripped.split(".")
    if len(parts) != 3:
        return None
    try:
        payload_bytes = _b64url_decode(parts[1])
    except _B64_DECODE_ERRORS:
        return None
    try:
        payload: Any = json.loads(payload_bytes)
    except _JSON_DECODE_ERRORS:
        return None
    if not isinstance(payload, dict):
        return None
    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub:
        return None
    return sub


def _b64url_decode(segment: str) -> bytes:
    """Base64url-decode a JWT segment, restoring stripped padding."""
    pad_len = (-len(segment)) % 4
    return base64.urlsafe_b64decode((segment + ("=" * pad_len)).encode("ascii"))


# ---------------------------------------------------------------------------
# PhantomDriver - models the upstream client's upload path.
# ---------------------------------------------------------------------------

# The metadata key under which the local UUID rides forward (ADR-008).
_LOCAL_UUID_METADATA_KEY: Final[str] = "phantom_local_uuid"


@dataclass(frozen=True)
class DriverUploadResult:
    """Result of :meth:`PhantomDriver.in_memory_upload`.

    Deliberately minimal - exposes only the fields the E2E suite reads off
    an upload: the chain ``id`` (the minted ``local_uuid``, which is the
    Phantom row id) and the mutated ``metadata`` (so callers can read
    ``result.metadata.key_value_store["phantom_local_uuid"]``).
    """

    id: UUID
    metadata: FileMetadata


class PhantomDriver:
    """Drives ``phantom`` via ``PhantomClient.submit_chain`` for the E2E suite.

    Models the upstream client's ``in_memory_upload`` pre-flight +
    submission, using only public types and an open :class:`PhantomClient`
    the caller owns. The driver does NOT own the client's lifecycle - the
    stack fixture opens and closes it.

    Args:
        client: An open :class:`PhantomClient` pointed at Phantom.
        files_api: The upstream Files API base URL (the emulator's URL in
            the E2E stack). Used as the metadata-POST base; Phantom routes
            by the first step's URL built from it.
        get_security_token: Optional bearer-token provider. When set, the
            returned token is sent as ``Authorization: Bearer <token>``
            and its JWT ``sub`` becomes ``X-Phantom-Uid``.
        uid_fallback: Fallback ``X-Phantom-Uid`` when the token is not a
            parseable JWT. ``None`` makes a non-JWT token a hard error.
    """

    def __init__(
        self,
        client: PhantomClient,
        *,
        files_api: str,
        get_security_token: Callable[[], str] | None = None,
        uid_fallback: str | None = None,
    ) -> None:
        self._client = client
        self._files_api = files_api
        self._get_security_token = get_security_token
        self._uid_fallback = uid_fallback

    async def in_memory_upload(
        self,
        request: CreateFileRequest,
        contents: bytes,
        *,
        options: SubmitOptions | None = None,
    ) -> DriverUploadResult:
        """Buffer the upload through Phantom; return the minted chain id.

        Models the upstream client's happy-path pre-flight: resolve the
        bearer token, derive the uid, mint a fresh ``local_uuid``,
        deep-copy the request, stuff ``phantom_local_uuid`` into the
        copy's metadata KVS, build the two-step envelope, and submit it.
        The caller-supplied ``request`` is NOT mutated.

        Args:
            request: The :class:`CreateFileRequest`. Not mutated.
            contents: The file bytes.
            options: Optional per-call :class:`SubmitOptions` (grouping
                tags, instance override); passed through to
                :meth:`PhantomClient.submit_chain` untouched.

        Returns:
            A :class:`DriverUploadResult` whose ``id`` is the minted
            ``local_uuid`` (= the Phantom row id) and whose ``metadata``
            is the deep-copied, ``phantom_local_uuid``-augmented metadata.
        """
        token = self._get_security_token() if self._get_security_token else None
        uid = derive_uid(token, fallback=self._uid_fallback)
        local_uuid = uuid4()
        request_copy = request.model_copy(deep=True)
        request_copy.metadata.key_value_store[_LOCAL_UUID_METADATA_KEY] = str(local_uuid)

        envelope, _ = build_in_memory_upload_envelope(
            request=request_copy,
            files_api_base=self._files_api,
            local_uuid=local_uuid,
        )
        body_refs = {_BODY_REF_NAME: contents}
        auth_header = f"Bearer {token}" if token else None

        await self._client.submit_chain(
            envelope,
            body_refs=body_refs,
            uid=uid,
            auth_token=auth_header,
            options=options,
        )

        log.debug(
            "PhantomDriver: submitted chain_id=%s files_api=%s body_bytes=%d uid=%s",
            local_uuid,
            self._files_api,
            len(contents),
            uid,
        )
        return DriverUploadResult(id=local_uuid, metadata=request_copy.metadata)


async def submit_chain_for_test(
    client: PhantomClient,
    *,
    request: CreateFileRequest,
    contents: bytes,
    files_api: str,
    uid: str,
    auth_token: str | None,
) -> ChainResponse:
    """Build + submit one upload-shaped chain for low-level reproducer tests.

    Thin convenience over :func:`build_in_memory_upload_envelope` +
    :meth:`PhantomClient.submit_chain` for callers (e.g. the subprocess
    harness) that already own the ``chain_id`` inside ``request``'s
    metadata and want a one-shot submit without the
    :class:`PhantomDriver` lifecycle. Mints a fresh ``chain_id``.
    """
    local_uuid = uuid4()
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=files_api,
        local_uuid=local_uuid,
    )
    return await client.submit_chain(
        envelope,
        body_refs={_BODY_REF_NAME: contents},
        uid=uid,
        auth_token=auth_token,
    )


__all__ = [
    "CreateFileRequest",
    "DriverUploadResult",
    "FileMetadata",
    "PhantomDriver",
    "build_in_memory_upload_envelope",
    "derive_uid",
]
