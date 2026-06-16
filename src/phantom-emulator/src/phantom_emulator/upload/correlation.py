"""Metadata key-value-store extraction and echo helpers.

An upstream adapter stamps a ``phantom_local_uuid`` (per ADR-008) onto
every upload via the upstream's ``metadata.keyValueStore`` field. The
emulator preserves this value byte-for-byte on the response so the
synthetic-upload-handle correlation end-to-end test can assert the
round-trip.

See plan §4.9 and ADR-008.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_metadata_kvs(body_json: dict[str, Any]) -> dict[str, str]:
    """Extract ``metadata.keyValueStore`` from a create-request body.

    Returns an empty dict if the field is absent or malformed; the
    emulator deliberately tolerates partial requests (plan §1
    "No schema-exhaustive request validation").

    Args:
        body_json: The parsed JSON body of ``POST /v1/files/create``.

    Returns:
        A dict mapping keyValueStore keys to string values. Values
        that aren't already strings are coerced via ``str()``.
    """
    metadata = body_json.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    kvs = metadata.get("keyValueStore")
    if not isinstance(kvs, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in kvs.items():
        if isinstance(key, str):
            result[key] = value if isinstance(value, str) else str(value)
    return result


def echo_metadata_kvs(
    file_information: dict[str, Any],
    kvs: dict[str, str],
) -> dict[str, Any]:
    """Stamp ``kvs`` onto ``file_information["metadata"]["keyValueStore"]``.

    The input ``file_information`` is mutated in-place AND returned so
    callers can chain. The upstream's response structure has a
    ``metadata`` container that holds ``keyValueStore``; we mirror that
    shape.

    Args:
        file_information: The JSON-ready upload-handle payload.
        kvs: The key-value-store entries to echo back.

    Returns:
        The same ``file_information`` reference (for chaining).
    """
    metadata = file_information.setdefault("metadata", {})
    metadata["keyValueStore"] = dict(kvs)
    return file_information
