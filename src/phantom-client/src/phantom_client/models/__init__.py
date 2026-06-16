"""Public Pydantic boundary types for ``phantom_client``.

Re-exports the wire-protocol shapes (ADR-010), admin filter/response
models, and the upload-row view from each module. ``__all__`` is the
canonical re-export list; the package's top-level ``__init__`` imports
from here, so adding a model means a single edit (plus the underlying
module).
"""

from __future__ import annotations

from phantom_client.models.admin import (
    AdminStatusResponse,
    BulkDeleteResponse,
    ChainAdminDetail,
    ChainAdminStepDetail,
    DeleteFilter,
    ExtractFilter,
    GroupMember,
    GroupStatusResponse,
    IdentifierLookupResponse,
    InstanceStatusResponse,
    InstanceSummary,
    KeyValueMatchFilter,
    ListUploadsResponse,
    UploadBundle,
    UploadStatusSummary,
)
from phantom_client.models.chain import (
    CapturedStep,
    ChainBody,
    ChainBodyBytes,
    ChainBodyJson,
    ChainBodyRef,
    ChainBodyText,
    ChainCapture,
    ChainEnvelope,
    ChainResponse,
    ChainState,
    ChainStep,
)
from phantom_client.models.envelope import ResponseHeaders, parse_response_headers
from phantom_client.models.status import (
    TERMINAL_STATES,
    HealthResponse,
    ReadyResponse,
    SortKey,
    StatsResponse,
    TokenSlot,
    UploadRow,
    UploadState,
)

__all__ = [
    "TERMINAL_STATES",
    "AdminStatusResponse",
    "BulkDeleteResponse",
    # chain.py (ADR-010)
    "CapturedStep",
    "ChainAdminDetail",
    "ChainAdminStepDetail",
    "ChainBody",
    "ChainBodyBytes",
    "ChainBodyJson",
    "ChainBodyRef",
    "ChainBodyText",
    "ChainCapture",
    "ChainEnvelope",
    "ChainResponse",
    "ChainState",
    "ChainStep",
    "DeleteFilter",
    # admin.py
    "ExtractFilter",
    "GroupMember",
    "GroupStatusResponse",
    "HealthResponse",
    "IdentifierLookupResponse",
    "InstanceStatusResponse",
    "InstanceSummary",
    "KeyValueMatchFilter",
    "ListUploadsResponse",
    "ReadyResponse",
    # envelope.py
    "ResponseHeaders",
    "SortKey",
    # status.py
    "StatsResponse",
    "TokenSlot",
    "UploadBundle",
    "UploadRow",
    "UploadState",
    "UploadStatusSummary",
    "parse_response_headers",
]
