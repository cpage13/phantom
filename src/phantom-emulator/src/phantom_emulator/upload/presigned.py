"""Synthetic presigned-style upload URL mint and resolution.

Real S3 presigned URLs encode an expiry and a HMAC signature in their
query string. The emulator mirrors that shape so callers can't simply
strip query parameters and still hit the endpoint, while keeping the
crypto stub (the signature is a fresh opaque token stored on the
pending-upload record). The PUT handler reconstructs the same
signature from the inbound URL and compares.

See plan §4.8.
"""

from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from phantom_emulator.state import PendingUpload

logger = logging.getLogger(__name__)

# Length of the opaque upload token embedded in the URL. 32 bytes of
# url-safe randomness gives ~256 bits — more than enough for tests.
UPLOAD_TOKEN_BYTES: int = 32

# Length of the synthetic signature parameter. Sized to look like an
# S3 v4 signature without claiming to be one.
SIGNATURE_BYTES: int = 32


class PresignedTokenStore:
    """Mints and resolves synthetic presigned-style upload tokens.

    Maintains its own mapping between opaque tokens and pending
    uploads. Stores live on :class:`phantom_emulator.state.EmulatorState`;
    this class is a typed wrapper around that map so the upstream
    router doesn't have to manipulate the dict directly.
    """

    def __init__(self, *, base_url: str, default_ttl_seconds: int) -> None:
        """Initialize the store.

        Args:
            base_url: Origin of the upload endpoint (e.g.,
                ``http://127.0.0.1:54321``). The PUT path is appended
                during ``mint`` and the URL is returned to callers.
            default_ttl_seconds: TTL applied to URLs unless a per-call
                override is supplied.
        """
        self._base_url = base_url.rstrip("/")
        self._default_ttl = default_ttl_seconds
        self._pending: dict[str, PendingUpload] = {}

    @property
    def pending(self) -> dict[str, PendingUpload]:
        """Underlying token → PendingUpload mapping (for test introspection)."""
        return self._pending

    def mint(
        self,
        *,
        file_id: UUID,
        file_information: dict[str, Any],
        metadata_kvs: dict[str, str],
        presigned_ttl_seconds: int | None = None,
        now: datetime | None = None,
    ) -> tuple[str, str, PendingUpload]:
        """Mint a fresh upload token + fully-qualified URL.

        Args:
            file_id: The minted FileInformation.id this URL is for.
            file_information: JSON-ready response payload (kept on the
                pending record so the GET stub can return it later).
            metadata_kvs: ``metadata.keyValueStore`` from the create
                request (preserves ``phantom_local_uuid``).
            presigned_ttl_seconds: Override the store's default TTL.
            now: Override the wall clock (test injection).

        Returns:
            A tuple ``(upload_token, upload_url, pending_record)``.
        """
        ttl = presigned_ttl_seconds if presigned_ttl_seconds is not None else self._default_ttl
        current = now or datetime.now(UTC)
        upload_token = secrets.token_urlsafe(UPLOAD_TOKEN_BYTES)
        signature = secrets.token_urlsafe(SIGNATURE_BYTES)
        expires_epoch = int((current + timedelta(seconds=ttl)).timestamp())
        upload_url = (
            f"{self._base_url}/v1/files/upload/{upload_token}"
            f"?expires={expires_epoch}&sig={signature}"
        )
        record = PendingUpload(
            upload_token=upload_token,
            file_id=file_id,
            file_information=file_information,
            metadata_kvs=dict(metadata_kvs),
            created_at=current,
            presigned_ttl_seconds=ttl,
            signature=signature,
        )
        self._pending[upload_token] = record
        logger.debug(
            "minted upload_token=%s file_id=%s ttl=%ds",
            upload_token[:12] + "...",
            file_id,
            ttl,
        )
        return upload_token, upload_url, record

    def resolve(self, upload_token: str) -> PendingUpload | None:
        """Return the record for ``upload_token`` (or None if unknown)."""
        return self._pending.get(upload_token)

    def is_expired(self, upload_token: str, now: datetime) -> bool:
        """Whether the URL for ``upload_token`` is past its TTL.

        Returns ``True`` for unknown tokens — an unknown token is
        functionally equivalent to "expired and unrecoverable" from the
        caller's perspective.
        """
        record = self._pending.get(upload_token)
        if record is None:
            return True
        expires_at = record.created_at + timedelta(seconds=record.presigned_ttl_seconds)
        return now >= expires_at
