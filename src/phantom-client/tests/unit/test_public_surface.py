"""Public-surface tests for ``phantom_client``.

Pins the exported names so accidental renames or removals fail loudly.
"""

from __future__ import annotations

import phantom_client


def test_init_exports_phantom_client() -> None:
    """``phantom_client.PhantomClient`` resolves."""
    assert phantom_client.PhantomClient is not None


def test_init_exports_canonical_models() -> None:
    """Every ADR-010 model is reachable from the package root."""
    for name in (
        "ChainEnvelope",
        "ChainStep",
        "ChainBody",
        "ChainBodyJson",
        "ChainBodyText",
        "ChainBodyBytes",
        "ChainBodyRef",
        "ChainCapture",
        "ChainResponse",
        "CapturedStep",
        "ChainState",
    ):
        assert hasattr(phantom_client, name), name


def test_init_exports_admin_and_status_types() -> None:
    """Admin + status types reachable from the package root."""
    for name in (
        "AdminStatusResponse",
        "InstanceStatusResponse",
        "InstanceSummary",
        "BulkDeleteResponse",
        "UploadBundle",
        "ExtractFilter",
        "DeleteFilter",
        "KeyValueMatchFilter",
        "UploadRow",
        "UploadState",
        "TERMINAL_STATES",
        "SortKey",
        "StatsResponse",
        "TokenSlot",
        "HealthResponse",
        "ReadyResponse",
        "ResponseHeaders",
    ):
        assert hasattr(phantom_client, name), name


def test_init_exports_errors() -> None:
    """Every error class is reachable from the package root."""
    for name in (
        "PhantomClientError",
        "PhantomTransportError",
        "PhantomConnectError",
        "PhantomTimeoutError",
        "PhantomNetworkError",
        "PhantomHttpError",
        "PhantomBadRequestError",
        "PhantomUnauthorizedError",
        "PhantomNotFoundError",
        "PhantomConflictError",
        "PhantomPayloadTooLargeError",
        "PhantomUnprocessableError",
        "PhantomValidationError",
        "PhantomRateLimitedError",
        "PhantomServerError",
        "PhantomUnavailableError",
        "PhantomEnvelopeError",
        "PollDeadlineExceeded",
        "EmptyFilterError",
        "EXCEPTION_FOR_CODE",
    ):
        assert hasattr(phantom_client, name), name


def test_init_exports_config() -> None:
    """Config classes reachable from the package root."""
    for name in ("ClientConfig", "Timeouts", "RetryPolicy", "SubmitOptions"):
        assert hasattr(phantom_client, name), name


def test_init_exports_header_constants() -> None:
    """Every X-Phantom-* header constant reachable from the package root."""
    for name in (
        "X_PHANTOM_UID",
        "X_PHANTOM_INSTANCE",
        "X_PHANTOM_GROUP_ID",
        "X_PHANTOM_MULTIFILE_ID",
        "X_PHANTOM_ORDER",
        "X_PHANTOM_IDEMPOTENCY_KEY",
        "X_PHANTOM_UPLOAD_ID",
        "X_PHANTOM_STATUS",
        "X_PHANTOM_ATTEMPTS",
        "X_PHANTOM_NEXT_ATTEMPT_AT",
        "X_PHANTOM_SUGGESTED_POLL_AFTER",
    ):
        assert hasattr(phantom_client, name), name


def test_init_does_not_export_retired_header_constants() -> None:
    """The cycle-7 dead-header sweep removed these names for good."""
    for name in ("X_PHANTOM_BATCH_ID", "X_PHANTOM_TARGET", "X_PHANTOM_METADATA"):
        assert not hasattr(phantom_client, name), f"{name} must not exist"


def test_init_exports_group_methods_and_poller() -> None:
    """The cycle-7 group/lookup surface is reachable from the package root."""
    assert phantom_client.poll_group_until_finished is not None
    cls = phantom_client.PhantomClient
    for name in (
        "get_group_status",
        "find_by_local_uuid",
        "find_by_captured_id",
        "poll_group_until_finished",
    ):
        assert hasattr(cls, name), name


def test_no_excluded_aliases() -> None:
    """The SDK deliberately excludes these names."""
    for name in (
        "send_chain",
        "send_request_chain",
        "send_files",
        "send_passthrough",
        "Method_B",
    ):
        assert not hasattr(phantom_client, name), f"{name} must not exist"


def test_no_excluded_aliases_on_client() -> None:
    """PhantomClient deliberately does not name-alias the canonical methods."""
    cls = phantom_client.PhantomClient
    for name in ("send_chain", "send_request_chain", "send_files", "send_passthrough"):
        assert not hasattr(cls, name), f"{name} must not exist on PhantomClient"


def test_version_present() -> None:
    """``__version__`` is exported."""
    assert phantom_client.__version__ == "0.1.0"
