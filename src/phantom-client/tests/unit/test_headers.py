"""Unit tests for ``phantom_client.headers``."""

from __future__ import annotations

from uuid import uuid4

import phantom_client.headers as h
from phantom_client.config import SubmitOptions


def test_request_header_constants() -> None:
    """Every request-header constant matches its documented literal."""
    assert h.X_PHANTOM_UID == "X-Phantom-Uid"
    assert h.X_PHANTOM_INSTANCE == "X-Phantom-Instance"
    assert h.X_PHANTOM_GROUP_ID == "X-Phantom-Group-Id"
    assert h.X_PHANTOM_MULTIFILE_ID == "X-Phantom-Multifile-Id"
    assert h.X_PHANTOM_ORDER == "X-Phantom-Order"
    assert h.X_PHANTOM_IDEMPOTENCY_KEY == "X-Phantom-Idempotency-Key"


def test_response_header_constants() -> None:
    """Every response-header constant matches its documented literal."""
    assert h.X_PHANTOM_UPLOAD_ID == "X-Phantom-Upload-Id"
    assert h.X_PHANTOM_STATUS == "X-Phantom-Status"
    assert h.X_PHANTOM_ATTEMPTS == "X-Phantom-Attempts"
    assert h.X_PHANTOM_NEXT_ATTEMPT_AT == "X-Phantom-Next-Attempt-At"
    assert h.X_PHANTOM_SUGGESTED_POLL_AFTER == "X-Phantom-Suggested-Poll-After"


def test_excluded_headers_absent() -> None:
    """Deliberately-excluded and cycle-7-retired headers are not exported.

    The first three never existed in the SDK; the last three are the
    dead-header sweep (plan 06_09 task 5.3): the request-side batch
    tag was renamed onto the grouping axes, the target header was
    never read by the service (routing comes from the first step's
    URL), and the metadata header had no consumer anywhere.
    """
    excluded = (
        "X_PHANTOM_ROUTE",
        "X_PHANTOM_AUTH_MODE",
        "X_PHANTOM_STRATEGY",
        "X_PHANTOM_BATCH_ID",
        "X_PHANTOM_TARGET",
        "X_PHANTOM_METADATA",
    )
    for name in excluded:
        assert not hasattr(h, name), f"{name} should not exist in headers"


def test_build_headers_uid_only() -> None:
    """uid-only build emits exactly uid + idempotency key."""
    hdrs = h.build_request_headers(
        uid="abc",
        auth_token=None,
        options=None,
        sdk_idempotency_key="sdk-key",
    )
    assert hdrs == {
        h.X_PHANTOM_UID: "abc",
        h.X_PHANTOM_IDEMPOTENCY_KEY: "sdk-key",
    }


def test_build_headers_full_options() -> None:
    """Full option set produces every documented X-Phantom-* header.

    The exact-equality assertion doubles as the dead-header regression:
    nothing beyond the documented set is ever emitted.
    """
    gid = uuid4()
    mid = uuid4()
    opts = SubmitOptions(
        instance_id="primary",
        group_id=gid,
        multifile_id=mid,
        order=2,
        idempotency_key="opt-key",
    )
    hdrs = h.build_request_headers(
        uid="uid-1",
        auth_token="Bearer t",
        options=opts,
        sdk_idempotency_key="fallback",
    )
    assert hdrs == {
        "Authorization": "Bearer t",
        h.X_PHANTOM_UID: "uid-1",
        h.X_PHANTOM_INSTANCE: "primary",
        h.X_PHANTOM_GROUP_ID: str(gid),
        h.X_PHANTOM_MULTIFILE_ID: str(mid),
        h.X_PHANTOM_ORDER: "2",
        # Options' idempotency_key wins over the SDK fallback.
        h.X_PHANTOM_IDEMPOTENCY_KEY: "opt-key",
    }


def test_build_headers_options_no_idempotency_key() -> None:
    """Without an options idempotency_key, the SDK fallback wins."""
    opts = SubmitOptions(instance_id="primary")
    hdrs = h.build_request_headers(
        uid="u",
        auth_token=None,
        options=opts,
        sdk_idempotency_key="sdk-default",
    )
    assert hdrs[h.X_PHANTOM_IDEMPOTENCY_KEY] == "sdk-default"
    assert hdrs[h.X_PHANTOM_INSTANCE] == "primary"


def test_build_headers_order_zero_emitted() -> None:
    """order=0 must be emitted (don't trip 'falsy' bugs)."""
    opts = SubmitOptions(order=0)
    hdrs = h.build_request_headers(
        uid=None,
        auth_token=None,
        options=opts,
        sdk_idempotency_key="k",
    )
    assert hdrs[h.X_PHANTOM_ORDER] == "0"


def test_build_headers_omitted_grouping_tags_not_emitted() -> None:
    """None grouping tags emit no headers (server applies defaults)."""
    opts = SubmitOptions(idempotency_key="k2")
    hdrs = h.build_request_headers(
        uid=None,
        auth_token=None,
        options=opts,
        sdk_idempotency_key="k",
    )
    assert h.X_PHANTOM_GROUP_ID not in hdrs
    assert h.X_PHANTOM_MULTIFILE_ID not in hdrs
    assert h.X_PHANTOM_ORDER not in hdrs
