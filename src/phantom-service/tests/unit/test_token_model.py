"""Unit tests for phantom.models.token."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from phantom.models.token import TokenCacheRow
from pydantic import ValidationError


def test_token_cache_row() -> None:
    """TokenCacheRow round-trips through JSON."""
    row = TokenCacheRow(
        endpoint="upstream.example.com",
        uid="user-1",
        bearer="Bearer abc",
        observed_at=datetime.now(tz=UTC),
        source="inbound_request",
        status="fresh",
    )
    blob = row.model_dump_json()
    rebuilt = TokenCacheRow.model_validate_json(blob)
    assert rebuilt.endpoint == row.endpoint
    assert rebuilt.bearer == row.bearer


def test_token_cache_row_strict() -> None:
    """TokenCacheRow rejects unknown fields."""
    with pytest.raises(ValidationError):
        TokenCacheRow.model_validate(
            {
                "endpoint": "x",
                "uid": "y",
                "bearer": "z",
                "observed_at": datetime.now(tz=UTC).isoformat(),
                "source": "inbound_request",
                "status": "fresh",
                "extra": "nope",
            },
        )


def test_token_cache_row_source_literal() -> None:
    """``source`` only accepts the three known origins."""
    with pytest.raises(ValidationError):
        TokenCacheRow(
            endpoint="x",
            uid="y",
            bearer="z",
            observed_at=datetime.now(tz=UTC),
            source="other",  # type: ignore[arg-type]
            status="fresh",
        )


def test_token_cache_row_status_literal() -> None:
    """``status`` only accepts fresh/bad/unknown."""
    with pytest.raises(ValidationError):
        TokenCacheRow(
            endpoint="x",
            uid="y",
            bearer="z",
            observed_at=datetime.now(tz=UTC),
            source="inbound_request",
            status="expired",  # type: ignore[arg-type]
        )
