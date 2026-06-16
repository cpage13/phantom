"""SDK configuration models.

The SDK accepts configuration in two shapes:

- ``PhantomClient("http://...")`` — base-URL only. The convenience path.
- ``PhantomClient(ClientConfig(...))`` — full configuration for
  advanced cases (custom timeouts, retry policy, default uid,
  bearer-on-loopback admin token, default headers).

Per-call submission knobs live on :class:`SubmitOptions`, passed
explicitly to :meth:`PhantomClient.submit_chain` when needed. They
map one-to-one onto the ``X-Phantom-*`` request headers.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Timeouts — passed verbatim to httpx.Timeout(...).
# ---------------------------------------------------------------------------


class Timeouts(BaseModel):
    """httpx timeout matrix.

    All values are seconds, floats, strictly positive. Pool acquisition
    is short (5s default) so callers fail fast when the connection pool
    is saturated; read/write timeouts are longer (30s default) so
    large body uploads don't trip them.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    connect: float = Field(5.0, gt=0.0, description="Connect timeout (seconds).")
    read: float = Field(30.0, gt=0.0, description="Read timeout (seconds).")
    write: float = Field(30.0, gt=0.0, description="Write timeout (seconds).")
    pool: float = Field(5.0, gt=0.0, description="Pool acquisition timeout (seconds).")


# ---------------------------------------------------------------------------
# Retry policy — transport-class retries only.
# ---------------------------------------------------------------------------


class RetryPolicy(BaseModel):
    """Retry policy for transport-class failures.

    The SDK retries on connection refused, read timeout, and similar
    transport errors. It does NOT retry on 5xx — Phantom IS the retry
    engine. Each retry carries the same ``X-Phantom-Idempotency-Key``
    so Phantom dedupes if it actually received the earlier attempt.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    enabled: bool = Field(True, description="Whether to retry transport-class failures.")
    max_attempts: int = Field(3, ge=1, description="Max attempt count including the initial.")
    backoff_initial_seconds: float = Field(
        0.5,
        gt=0.0,
        description="Initial backoff in seconds; doubled on each subsequent attempt.",
    )
    backoff_max_seconds: float = Field(
        8.0,
        gt=0.0,
        description="Backoff cap in seconds.",
    )
    backoff_jitter: bool = Field(True, description="Whether to apply +/-50% jitter to backoff.")


# ---------------------------------------------------------------------------
# Per-call submission options — map onto X-Phantom-* request headers.
# ---------------------------------------------------------------------------


class SubmitOptions(BaseModel):
    """Per-call submission knobs.

    Map one-to-one onto the optional ``X-Phantom-*`` request headers.
    None on a field means "don't send that header" — Phantom applies
    its configured default.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    instance_id: str | None = Field(
        None,
        description=(
            "X-Phantom-Instance override; advanced per ADR-006. Force routing "
            "to a specific instance regardless of host-prefix matching."
        ),
    )
    group_id: UUID | None = Field(
        None,
        description=(
            "Query-grouping tag, emitted as X-Phantom-Group-Id. Submit several "
            "uploads sharing one group_id to query them as a set (the group "
            "rollup and poll_group_until_finished). Defaults server-side to "
            "envelope.chain_id when omitted: every upload is a group of one."
        ),
    )
    multifile_id: UUID | None = Field(
        None,
        description=(
            "Multi-file set tag, emitted as X-Phantom-Multifile-Id. Marks this "
            "upload as part of a producer-defined multi-file set; omitted "
            "means standalone (the row stores NULL). Recorded and surfaced "
            "only; never enforced at delivery. Distinct from group_id."
        ),
    )
    order: int | None = Field(
        None,
        ge=0,
        description=(
            "X-Phantom-Order; the row's send_order (0) when omitted. Use to "
            "preserve client-defined ordering within a multi-file set."
        ),
    )
    idempotency_key: str | None = Field(
        None,
        description=(
            "X-Phantom-Idempotency-Key (chain-ingress dedup); the SDK defaults "
            "to str(envelope.chain_id) when omitted. **Distinct** from "
            "envelope.idempotency_key, which Phantom forwards to upstreams."
        ),
    )


# ---------------------------------------------------------------------------
# Full SDK configuration.
# ---------------------------------------------------------------------------


class ClientConfig(BaseModel):
    """Full SDK configuration.

    Most callers use the two-shape constructor with just a phantom_url
    string; only advanced cases instantiate :class:`ClientConfig`
    directly.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    phantom_url: str = Field(
        "http://127.0.0.1:8080",
        description="Phantom base URL (no path). Intake, admin, and health all ride this one URL.",
    )
    default_uid: str | None = Field(
        None,
        description=(
            "Used as X-Phantom-Uid when submit_chain doesn't override. "
            "An upstream adapter that passes uid per-call leaves this unset."
        ),
    )
    default_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Headers appended to every outbound request.",
    )
    # `default_factory=<BaseModel subclass>` is the idiomatic Pydantic way to
    # build a nested-default; without the Pydantic mypy plugin (which is
    # incompatible with this workspace's mypy 2.0 + Pydantic 2.13 combination),
    # mypy can't bind the callable's return type against the field's declared
    # type and emits `arg-type`. The runtime behavior is correct.
    timeouts: Timeouts = Field(
        default_factory=Timeouts,  # type: ignore[arg-type]
        description="Per-phase httpx timeouts (connect / read / write / pool).",
    )
    retry_policy: RetryPolicy = Field(
        default_factory=RetryPolicy,  # type: ignore[arg-type]
        description="Transport-class retry policy (httpx ConnectError / ReadTimeout / etc.).",
    )
    log_level: str = Field("INFO", description="SDK log level.")


__all__ = [
    "ClientConfig",
    "RetryPolicy",
    "SubmitOptions",
    "Timeouts",
]
