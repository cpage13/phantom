"""Unit tests for ``phantom_client.config``."""

from __future__ import annotations

from uuid import uuid4

import pytest
from phantom_client.config import ClientConfig, RetryPolicy, SubmitOptions, Timeouts
from pydantic import ValidationError


def test_timeouts_defaults() -> None:
    """Timeouts default values match the documented matrix."""
    t = Timeouts()
    assert t.connect == 5.0
    assert t.read == 30.0
    assert t.write == 30.0
    assert t.pool == 5.0


def test_timeouts_strict_positive() -> None:
    """Timeouts reject non-positive values."""
    with pytest.raises(ValidationError):
        Timeouts(connect=0.0)
    with pytest.raises(ValidationError):
        Timeouts(read=-1.0)


def test_retry_policy_defaults() -> None:
    """RetryPolicy default values match the documented shape."""
    p = RetryPolicy()
    assert p.enabled is True
    assert p.max_attempts == 3
    assert p.backoff_initial_seconds == 0.5
    assert p.backoff_max_seconds == 8.0
    assert p.backoff_jitter is True


def test_retry_policy_max_attempts_min() -> None:
    """max_attempts must be >= 1."""
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=0)


def test_submit_options_strict() -> None:
    """SubmitOptions rejects unknown fields and bad types."""
    opts = SubmitOptions(instance_id="primary", order=0)
    assert opts.instance_id == "primary"
    assert opts.order == 0
    with pytest.raises(ValidationError):
        SubmitOptions.model_validate({"surprise": True})
    with pytest.raises(ValidationError):
        SubmitOptions(order=-1)


def test_submit_options_group_and_multifile_uuids() -> None:
    """group_id and multifile_id accept UUID instances directly."""
    gid = uuid4()
    mid = uuid4()
    opts = SubmitOptions(group_id=gid, multifile_id=mid)
    assert opts.group_id == gid
    assert opts.multifile_id == mid


def test_submit_options_grouping_defaults_are_none() -> None:
    """Both grouping tags default to None (server applies its defaults)."""
    opts = SubmitOptions()
    assert opts.group_id is None
    assert opts.multifile_id is None
    assert opts.order is None


def test_client_config_defaults() -> None:
    """ClientConfig defaults match the documented values."""
    c = ClientConfig()
    assert c.phantom_url == "http://127.0.0.1:8080"
    assert c.default_uid is None
    assert c.default_headers == {}
    assert isinstance(c.timeouts, Timeouts)
    assert isinstance(c.retry_policy, RetryPolicy)
    assert c.log_level == "INFO"


def test_client_config_extra_forbidden() -> None:
    """ClientConfig rejects unknown fields."""
    with pytest.raises(ValidationError):
        ClientConfig.model_validate({"phantom_url": "http://x", "surprise": 1})
