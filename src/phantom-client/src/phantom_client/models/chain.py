"""Request-chain envelope shapes (wire protocol per ADR-010).

This module is the canonical Python representation of the request-chain
envelope Phantom accepts on ``POST /v1/send``. It mirrors the schema
locked by ADR-010 byte-for-byte: same field names, same descriptions,
same validation rules, same discriminator. ``phantom.models.chain``
holds the authoritative copy on the service side; this module
duplicates it for the SDK so callers can construct envelopes without
depending on the service package. A drift-detection contract test at
``tests/contract/test_chain_models_alignment.py`` enforces byte-equality
between the two modules' models.

``ChainEnvelope.idempotency_key`` may be omitted or left blank by the
caller; a ``mode="before"`` validator then auto-defaults it to
``str(chain_id)`` so the field stays typed ``str`` (required) while still
permitting omission. A non-blank caller value always wins.

Author note: written from ADR-010 alone; no coordination with the
phantom-package author.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

# ---------------------------------------------------------------------------
# Chain state — set of literals matching ADR-010 §"Response".
# ---------------------------------------------------------------------------

# NOTE: ``TypeAlias`` form (not PEP-695 ``type X = ...``) is the workspace
# convention, matching ``phantom.models.chain``. The drift-detection
# contract test at ``tests/contract/test_chain_models_alignment.py``
# tolerates either form via ``__value__`` unwrap, but using the same
# form on both sides keeps Pydantic's emitted JSON schema inline (no
# ``$defs/ChainState`` indirection) which simplifies downstream tooling.
ChainState: TypeAlias = Literal[  # noqa: UP040 — see note above
    "queued",
    "attempting",
    "succeeded",
    "failed",
    "auth_expired",
    "stored",
    "cancelled",
    "corrupted",
    "expired",
]
"""Upload-row state as observed by SDK callers. See ADR-010 §Response.

``corrupted`` is a terminal state surfaced when body verification fails
on send (storage hash mismatch or codec round-trip drift); the row is
never retried. ``expired`` is a terminal state (ADR-032) reached when the
per-route send-deadline elapses: the upload is dead, the body is released,
and the row is never re-admitted — distinct from ``auth_expired``, which
is re-queued once a fresh token arrives.
"""


# ---------------------------------------------------------------------------
# Body variants — discriminated union over the ``kind`` literal.
# ---------------------------------------------------------------------------


class ChainBodyJson(BaseModel):
    """JSON request body.

    The ``value`` is a JSON object serialized as the request body when
    Phantom executes this step. String values inside may contain
    ``{{step_name.capture_name}}`` placeholders that Phantom substitutes
    server-side just before sending the step.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["json"] = Field("json", description="Discriminator tag.")
    value: dict[str, Any] = Field(
        ...,
        description=(
            "JSON object serialized as the request body. May contain "
            "{{step.var}} placeholders in string values."
        ),
    )


class ChainBodyText(BaseModel):
    """Plain-text request body."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["text"] = Field("text", description="Discriminator tag.")
    value: str = Field(
        ...,
        description="Text body. May contain {{step.var}} placeholders.",
    )
    content_type: str = Field(
        "text/plain",
        description="Content-Type header value sent to the upstream for this step.",
    )


class ChainBodyBytes(BaseModel):
    """Inline base64-encoded body.

    Use only for small payloads (< 64 KiB); larger blobs go via
    :class:`ChainBodyRef` so they ride as multipart parts rather than
    inflating the envelope.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["bytes"] = Field("bytes", description="Discriminator tag.")
    value_b64: str = Field(..., description="Base64-encoded body bytes.")
    content_type: str = Field(
        "application/octet-stream",
        description="Content-Type header value sent to the upstream for this step.",
    )


class ChainBodyRef(BaseModel):
    """Reference to a binary blob carried alongside the envelope as a multipart part."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["body_ref"] = Field("body_ref", description="Discriminator tag.")
    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=(
            "Reference name. The actual bytes ride as the multipart part "
            "body_refs[<name>]. Lowercase ASCII identifier; matching is by name."
        ),
    )
    content_type: str = Field(
        "application/octet-stream",
        description="Content-Type header value sent to the upstream for this step.",
    )


ChainBody = Annotated[
    ChainBodyJson | ChainBodyText | ChainBodyBytes | ChainBodyRef,
    Field(discriminator="kind"),
]
"""Discriminated union over the four body kinds, keyed by ``kind``."""


# ---------------------------------------------------------------------------
# Capture spec — one extraction from a step's response.
# ---------------------------------------------------------------------------


class ChainCapture(BaseModel):
    """A named extraction from a step's response.

    The captured value is stored on the upload row, can be referenced
    from later steps as ``{{step_name.capture_name}}``, and is surfaced
    via the admin API on terminal-success states for the row's
    ``succeeded_metadata_seconds`` retention window.
    """

    model_config = ConfigDict(strict=True, extra="forbid", populate_by_name=True)

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=(
            "Capture name; referenceable as {{step_name.name}} in later steps. "
            "Lowercase ASCII identifier."
        ),
    )
    from_path: str = Field(
        ...,
        alias="from",
        description=(
            "JSONPath expression evaluated against this step's response body. First match wins."
        ),
    )
    ttl_seconds: int | None = Field(
        None,
        ge=1,
        description=(
            "Optional capture-value lifetime in seconds. If set and a later step "
            "uses this value after the TTL has elapsed, capture-expiry "
            "re-execution behavior (ADR-011) applies. If None, the capture is "
            "treated as non-expiring."
        ),
    )
    sensitive: bool = Field(
        False,
        description=(
            "When True, log output redacts the captured value as <redacted>. "
            "Admin responses (loopback only) still surface the raw value. "
            "Use for captures that carry temporary credentials such as "
            "presigned PUT URLs."
        ),
    )


# ---------------------------------------------------------------------------
# Step — one HTTP request in the chain.
# ---------------------------------------------------------------------------


class ChainStep(BaseModel):
    """One HTTP request in the chain."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=(
            "Step name; referenced from later steps via {{name.capture_name}}. "
            "Lowercase ASCII identifier."
        ),
    )
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        ...,
        description="HTTP method.",
    )
    url: str = Field(
        ...,
        description=(
            "Target URL or path; may contain {{step_name.capture_name}} "
            "placeholders resolved at execution time."
        ),
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Outbound headers. Values may contain {{step_name.capture_name}} placeholders."
        ),
    )
    body: ChainBody | None = Field(
        None,
        description="Request body. Omit for body-less methods (GET, DELETE).",
    )
    capture: list[ChainCapture] = Field(
        default_factory=list,
        description=(
            "Fields to extract from this step's response. Captured values become "
            "available to later steps and are persisted on the upload row per "
            "ADR-009."
        ),
    )
    idempotency_header: str | None = Field(
        None,
        description=(
            "If set, Phantom sends this header name with the envelope's "
            "idempotency_key value on every attempt of this step. Required for "
            "ADR-011's re-execution behavior. Typical value: 'Idempotency-Key'."
        ),
    )


# ---------------------------------------------------------------------------
# Envelope — the top-level submission unit.
# ---------------------------------------------------------------------------


class ChainEnvelope(BaseModel):
    """A multi-step buffered upload, submitted as a unit to Phantom."""

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(
        ...,
        description=(
            "Caller-supplied chain identifier; also the Phantom upload row id "
            "and the synthetic upload-handle id returned by the upstream adapter."
        ),
    )
    idempotency_key: str = Field(
        ...,
        min_length=1,
        description=(
            "Per-chain idempotency value sent to upstreams that support it. "
            "Stable across Phantom retries of any step. May be omitted (or "
            "left blank) by the caller: when absent or whitespace-only it "
            "auto-defaults to str(chain_id); a non-blank caller value wins."
        ),
    )
    steps: list[ChainStep] = Field(
        ...,
        min_length=1,
        description=(
            "Ordered list of HTTP steps. Step N may reference captured values "
            "from any step M where M < N via {{step_name.capture_name}} "
            "substitution."
        ),
    )
    default_target: HttpUrl | None = Field(
        None,
        description=(
            "Optional default target URL applied to steps that don't specify one. "
            "If a step's `url` is a path (e.g., '/files') it's appended to "
            "this; if it's a full URL, this is ignored."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _default_idempotency_key(cls, data: Any) -> Any:
        """Default a missing or blank ``idempotency_key`` to ``str(chain_id)``.

        Runs ``mode="before"`` (on the raw input, ahead of field validation)
        so an omitted ``idempotency_key`` is filled BEFORE the required-field
        check rejects it; this lets the field stay typed ``str`` (required)
        while still permitting omission. A non-blank caller value is left
        untouched and still validated against ``min_length=1``. The injection
        fires only when ``chain_id`` is present, so a missing or invalid
        ``chain_id`` still surfaces its own field error rather than being
        masked. Non-dict input (already-built model, etc.) is passed through.
        """
        if (
            isinstance(data, dict)
            and not str(data.get("idempotency_key") or "").strip()
            and data.get("chain_id") is not None
        ):
            data = {**data, "idempotency_key": str(data["chain_id"])}
        return data


# ---------------------------------------------------------------------------
# Response shapes — what Phantom returns on submit and on
# ``GET /v1/admin/chains/{chain_id}``.
# ---------------------------------------------------------------------------


class CapturedStep(BaseModel):
    """Captured values from one step, surfaced in ChainResponse on terminal-success states."""

    model_config = ConfigDict(strict=True, extra="forbid")

    step_name: str = Field(..., description="The producing step's name.")
    values: dict[str, Any] = Field(
        ...,
        description=(
            "Captured values keyed by capture name. JSON-shaped; matches the "
            "captures declared on the step."
        ),
    )


class ChainResponse(BaseModel):
    """Phantom's reply to submit_chain and the shape of GET /v1/admin/chains/{chain_id}."""

    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(..., description="The chain's id (= envelope.chain_id).")
    state: ChainState = Field(..., description="Current upload-row state.")
    last_step_completed: str | None = Field(
        None,
        description=(
            "Name of the most recently terminal-success step in this chain. "
            "None if no step has completed."
        ),
    )
    captured: list[CapturedStep] = Field(
        default_factory=list,
        description=(
            "Per-step captured values. Populated as steps complete; persists "
            "per the row's retention window (succeeded_metadata_seconds default "
            "180s)."
        ),
    )


__all__ = [
    "CapturedStep",
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
]
