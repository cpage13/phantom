"""Unit tests for the :class:`BodyHashes` model and related typed hashes."""

from __future__ import annotations

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash
from pydantic import ValidationError


def test_body_hashes_round_trips_fields() -> None:
    """Both hashes survive Pydantic construction and read back as strings."""
    hashes = BodyHashes(
        body_hash=BodyHash("a" * 64),
        storage_hash=StorageHash("b" * 64),
    )
    assert hashes.body_hash == "a" * 64
    assert hashes.storage_hash == "b" * 64


def test_body_hashes_validation_rejects_missing_body_hash() -> None:
    """Omitting ``body_hash`` raises ValidationError."""
    with pytest.raises(ValidationError):
        BodyHashes.model_validate({"storage_hash": "b" * 64})


def test_body_hashes_validation_rejects_missing_storage_hash() -> None:
    """Omitting ``storage_hash`` raises ValidationError."""
    with pytest.raises(ValidationError):
        BodyHashes.model_validate({"body_hash": "a" * 64})


def test_body_hashes_descriptions_are_non_empty() -> None:
    """Both fields have non-empty Pydantic descriptions per the 10/10 standard."""
    schema = BodyHashes.model_fields
    assert schema["body_hash"].description
    assert schema["storage_hash"].description


def test_body_hashes_extra_forbid() -> None:
    """Unknown keys are rejected via ``extra='forbid'``."""
    with pytest.raises(ValidationError):
        BodyHashes.model_validate(
            {
                "body_hash": "a" * 64,
                "storage_hash": "b" * 64,
                "stray": "value",
            }
        )
