"""Internal token-cache row.

:class:`TokenCacheRow` is the boundary type between the SQLite
``token_cache`` table and Phantom's in-memory code. It is **internal
only** — the bearer field never crosses an HTTP response boundary
(ADR-004). Admin-facing serialization uses
:class:`phantom.models.admin.TokenSlot` instead, which has no bearer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

# NOTE: ``TypeAlias`` form intentional — see phantom.models.chain rationale.
TokenSource: TypeAlias = Literal["inbound_request", "admin_push", "plugin_mint"]  # noqa: UP040
"""How the cache slot's bearer was last written.

* ``inbound_request`` — a ``POST /v1/send`` ingress carried it.
* ``admin_push`` — operator pushed via ``PUT /v1/admin/tokens/...``.
* ``plugin_mint`` — the ``ad_client_credentials`` plugin minted it.
"""

TokenStatus: TypeAlias = Literal["fresh", "bad", "unknown"]  # noqa: UP040
"""Freshness state of the cached bearer.

* ``fresh`` — most recently observed bearer; presumed valid.
* ``bad`` — last attempt with this bearer returned 401/403; kept in
  the cache anyway per ADR-003 so the admin API can surface it.
* ``unknown`` — slot exists but has not been used yet (rare).
"""


class TokenCacheRow(BaseModel):
    """One row of the ``token_cache`` SQLite table.

    Primary key is ``(endpoint, uid)``. The bearer column persists per
    ADR-003 (the cache survives Phantom restart). Bad tokens stay in
    the cache rather than being deleted so the admin API can surface
    "this is the only token I have for this slot, and it's bad."
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    endpoint: str = Field(
        ...,
        description="Upstream hostname (ADR-002 cache axis).",
    )
    uid: str = Field(
        ...,
        description=(
            "Opaque caller-supplied credential identifier. Phantom never parses this (ADR-002)."
        ),
    )
    bearer: str = Field(
        ...,
        description=(
            "The opaque Authorization-header value. Includes the 'Bearer ' "
            "prefix iff the caller sent it that way; treated as bytes by "
            "Phantom (ADR-001). Never returned in admin responses (ADR-004)."
        ),
    )
    observed_at: datetime = Field(
        ...,
        description="When this bearer was last written (UTC).",
    )
    source: TokenSource = Field(
        ...,
        description="Where this bearer came from.",
    )
    status: TokenStatus = Field(
        ...,
        description="Whether the cached bearer last succeeded or failed.",
    )
