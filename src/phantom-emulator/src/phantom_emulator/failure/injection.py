"""Failure-injection policy state.

Tests drive the emulator's failure surface through this state object,
either directly (programmatic mode) or via the ``/control/*`` HTTP
surface (Docker mode). One :class:`FailurePolicy` per
:class:`FailureScope`; the middleware looks up the most-specific
policy for each request and applies the configured behavior.

The state object owns its own seeded RNG so probabilistic knobs
(``error_rate_5xx``, ``latency_ms`` range) are deterministic per seed.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class FailureScope(StrEnum):
    """Per-request failure scope.

    The middleware maps each URL path to one scope. ``GLOBAL`` is the
    fallback — if no scope-specific policy exists, the GLOBAL policy
    (if any) applies.
    """

    GLOBAL = "*"
    UPSTREAM_FILES_CREATE = "upstream.files.create"
    UPSTREAM_FILES_UPLOAD = "upstream.files.upload"
    UPSTREAM_FILES_GET = "upstream.files.get"
    AUTH_TOKEN = "auth.token"


class LatencyRange(BaseModel):
    """A range of millisecond delays for randomized latency injection."""

    model_config = ConfigDict(extra="forbid")

    min_ms: int = Field(..., ge=0, description="Minimum sleep in ms.")
    max_ms: int = Field(..., ge=0, description="Maximum sleep in ms.")


class FailurePolicy(BaseModel):
    """Policy applied to one :class:`FailureScope`.

    Pydantic model so it can serialize to/from the control API
    boundary; the middleware reads its fields directly.
    """

    model_config = ConfigDict(extra="forbid")

    scope: FailureScope = Field(..., description="Where this policy applies.")
    error_rate_5xx: float = Field(
        0.0,
        ge=0.0,
        le=1.0,
        description="Probability the request returns 503.",
    )
    auth_401_after_n_calls: int | None = Field(
        None,
        ge=0,
        description="Return 401 starting at the Nth successful call.",
    )
    latency_ms: int | LatencyRange | None = Field(
        None,
        description="Pre-handler sleep (or random range).",
    )
    body_cutoff_at_bytes: int | None = Field(
        None,
        ge=0,
        description="Truncate response body after this many bytes.",
    )
    slow_trickle_bytes_per_sec: int | None = Field(
        None,
        ge=1,
        description="Throttle response body to this byte rate.",
    )
    tcp_rst_on_request: bool = Field(
        False,
        description=(
            "Approximate RST via Connection: close + transport close "
            "mid-write. Documented limitation: not a raw-socket RST."
        ),
    )
    unavailable_until: datetime | None = Field(
        None,
        description="Return 503 until this absolute time.",
    )
    presigned_signature_mismatch: bool = Field(
        False,
        description="PUTs return 403 SignatureDoesNotMatch.",
    )


@dataclass(frozen=True)
class ErrorRate5xxEvent:
    """One 503 generated specifically by an ``error_rate_5xx`` draw."""

    scope: FailureScope
    occurred_at: datetime


class FailureInjectionState:
    """Mutable container for per-scope policies and counters.

    The object lives on :class:`phantom_emulator.state.EmulatorState`
    and is consulted by the middleware on every request. ``rng`` is
    seeded so the same test scenario produces the same coin-flips.
    """

    def __init__(self, *, seed: int = 0) -> None:
        """Build an empty state with no policies and an RNG keyed to ``seed``."""
        self.policies: dict[FailureScope, FailurePolicy] = {}
        self.call_counts: dict[FailureScope, int] = {}
        self.error_rate_5xx_events: list[ErrorRate5xxEvent] = []
        self.rng: random.Random = random.Random(seed)
        self._seed: int = seed

    def set_policy(self, policy: FailurePolicy) -> None:
        """Install or replace the policy for ``policy.scope``."""
        self.policies[policy.scope] = policy

    def clear_all(self) -> None:
        """Remove every installed policy and its accumulated observations."""
        self.policies.clear()
        self.call_counts.clear()
        self.error_rate_5xx_events.clear()

    def resolve(self, scope: FailureScope) -> FailurePolicy | None:
        """Return the most-specific policy for ``scope``.

        If no scope-specific policy is set, the ``GLOBAL`` policy is
        returned when present. ``None`` means "no failure injection."

        Args:
            scope: The scope of the inbound request.

        Returns:
            The policy to apply, or ``None``.
        """
        if scope in self.policies:
            return self.policies[scope]
        return self.policies.get(FailureScope.GLOBAL)

    def record_call(self, scope: FailureScope) -> int:
        """Increment and return the per-scope call counter."""
        self.call_counts[scope] = self.call_counts.get(scope, 0) + 1
        return self.call_counts[scope]

    def record_error_rate_5xx(self, scope: FailureScope) -> ErrorRate5xxEvent:
        """Append and return one 503 generated by the error-rate branch."""
        event = ErrorRate5xxEvent(scope=scope, occurred_at=datetime.now(UTC))
        self.error_rate_5xx_events.append(event)
        return event

    def error_rate_5xx_for_scope(self, scope: FailureScope) -> tuple[ErrorRate5xxEvent, ...]:
        """Return error-rate-generated 503 observations for ``scope``."""
        return tuple(event for event in self.error_rate_5xx_events if event.scope is scope)

    def error_rate_5xx_count(self, scope: FailureScope) -> int:
        """Return the error-rate-generated 503 count for ``scope``."""
        return len(self.error_rate_5xx_for_scope(scope))

    def set_seed(self, seed: int) -> None:
        """Reseed the RNG so subsequent draws are deterministic again."""
        self._seed = seed
        self.rng = random.Random(seed)

    @property
    def seed(self) -> int:
        """Return the last-seeded value (for status reporting)."""
        return self._seed
