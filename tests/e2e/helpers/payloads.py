"""Builders for the upload payloads used by the E2E suite.

The builders here mint realistic :class:`CreateFileRequest` objects
plus their accompanying body bytes. They exist so each test stays a
three-liner (build, submit, assert) and the per-test data shape stays
consistent across the suite.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from tests.e2e._driver import CreateFileRequest, FileMetadata

# Default values model a representative upload shape - a generic
# domain, a `metadata_table` lane base, and an `uploader_id`. None of
# these are load-bearing for the smoke test; they exist so the
# emulator's `received` log carries something plausible.
DEFAULT_DOMAIN: str = "generic"
DEFAULT_LANE_BASE_NAME: str = "metadata_table"
DEFAULT_UPLOADER_ID: str = "12345"

# Default body for the smoke test — 64 bytes is tiny enough that the
# entire upload completes in milliseconds and large enough that the
# emulator's `body_size` assertion exercises a non-empty PUT.
DEFAULT_BODY: bytes = b"phantom-e2e-smoke-test-body-payload-0123456789abcdef0123456789abcd"


@dataclass(frozen=True)
class UploadPayload:
    """One realistic upload payload bundled with its body bytes."""

    request: CreateFileRequest
    body: bytes


def build_create_file_request(
    *,
    file_name: str | None = None,
    domain: str = DEFAULT_DOMAIN,
    lane_base_name: str = DEFAULT_LANE_BASE_NAME,
    uploader_id: str = DEFAULT_UPLOADER_ID,
    extra_metadata: dict[str, str] | None = None,
) -> CreateFileRequest:
    """Build a realistic :class:`CreateFileRequest`.

    The defaults model a typical upload pattern. Tests override fields
    when they need to assert specific propagation, e.g., a custom
    ``uploader_id`` round-tripping through the emulator's
    ``x-amz-meta-uploader-id`` header.

    Args:
        file_name: File name. Default is ``e2e_<unix-timestamp>``.
            Must satisfy the upstream pattern (alphanumerics plus
            ``!-_.*'()``).
        domain: The file domain.
        lane_base_name: The lane base name.
        uploader_id: Uploader id; ends up under
            ``metadata.key_value_store['uploader_id']`` and (per the
            driver's envelope builder) is echoed as the
            ``x-amz-meta-uploader-id`` header on the PUT step.
        extra_metadata: Optional extra key-value pairs merged into
            the metadata KVS. Useful for asserting custom keys
            round-trip.

    Returns:
        A constructed :class:`CreateFileRequest`. Not mutated by the
        downstream driver call.
    """
    file_name = file_name or _default_file_name()
    kvs: dict[str, str] = {
        "uploader_id": uploader_id,
    }
    if extra_metadata is not None:
        kvs.update(extra_metadata)
    return CreateFileRequest(
        domain=domain,
        lane_base_name=lane_base_name,
        file_name=file_name,
        metadata=FileMetadata(key_value_store=kvs),
    )


def build_upload_payload(
    *,
    file_name: str | None = None,
    body: bytes = DEFAULT_BODY,
    domain: str = DEFAULT_DOMAIN,
    lane_base_name: str = DEFAULT_LANE_BASE_NAME,
    uploader_id: str = DEFAULT_UPLOADER_ID,
    extra_metadata: dict[str, str] | None = None,
) -> UploadPayload:
    """Build a :class:`CreateFileRequest` + body pair for one upload."""
    request = build_create_file_request(
        file_name=file_name,
        domain=domain,
        lane_base_name=lane_base_name,
        uploader_id=uploader_id,
        extra_metadata=extra_metadata,
    )
    return UploadPayload(request=request, body=body)


def _default_file_name() -> str:
    """Return a unique file name composed only of upstream-allowed chars."""
    return f"e2e_{int(datetime.now(UTC).timestamp() * 1000)}"
