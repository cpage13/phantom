"""Request-chain envelope wire schema (ADR-010).

This module is the source-of-truth for the request-chain envelope. The
schema is duplicated byte-for-byte in ``phantom_client.models.chain``; a
workspace-level contract test enforces that the two copies remain
identical. If you edit anything here, edit the SDK copy too.

``ChainEnvelope.idempotency_key`` may be omitted or left blank by the
caller; a ``mode="before"`` validator then auto-defaults it to
``str(chain_id)`` so the field stays typed ``str`` (required) while still
permitting omission. A non-blank caller value always wins.

See:
- ADR-009: request-chain envelope mechanism.
- ADR-010: canonical Pydantic schema (this module).
- ADR-011: capture-expiry re-execution behavior.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

# NOTE: ``TypeAlias`` form (not ``type X = ...``) is intentional. The
# drift-detection test that compares ``phantom.models.chain`` against
# ``phantom_client.models.chain`` reads the state literal via
# ``typing.get_args``; on a 3.12+ ``type X = Literal[...]`` alias, ``get_args``
# returns the empty tuple, so we keep the runtime-visible ``TypeAlias`` form.
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
"""The nine canonical chain/upload states. Snake-case canonical for
``auth_expired``. There is no separate ``received`` state — ingress
inserts directly into ``queued``. ``corrupted`` is terminal and reached
only when body verification fails on send (storage hash mismatch or
codec round-trip drift); never retried. ``expired`` is terminal (ADR-032):
the per-route send-deadline elapsed, so the upload is dead, the body is
released, and the row is NEVER re-admitted — the OPPOSITE of
``auth_expired`` (which is re-queueable once a fresh token arrives).
"""


class ChainBodyJson(BaseModel):
    """JSON request body for one chain step."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["json"] = Field("json", description="Discriminator tag.")
    value: dict[str, Any] = Field(
        ...,
        description=(
            "JSON object serialized as the request body. String values may "
            "contain {{step_name.capture_name}} placeholders, resolved "
            "server-side just before each step executes. Substitution happens "
            "inside the parsed object and the body is serialized last, so a "
            "captured quote, backslash or newline is escaped correctly. A "
            "string that is EXACTLY one placeholder and is not an object key "
            "takes a captured object or array as real structure. Everywhere "
            "else the placeholder renders as text, so the capture must be a "
            "string, number, boolean or null; a captured object or array in "
            "an embedded position (any other text around it) or in an object "
            "KEY fails the step. Scalars are not type-converted: a captured "
            "number renders as its string form."
        ),
    )


class ChainBodyText(BaseModel):
    """Plain-text request body for one chain step."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["text"] = Field("text", description="Discriminator tag.")
    value: str = Field(
        ...,
        description=(
            "Text body; may contain {{step.var}} placeholders. A text body has "
            "no serializer to escape into, so every referenced capture must be "
            "a string, number, boolean or null and renders as its string form; "
            "a captured object or array fails the step."
        ),
    )
    content_type: str = Field(
        "text/plain",
        description="Content-Type header value sent to upstream.",
    )


class ChainBodyBytes(BaseModel):
    """Inline base64-encoded body. Use only for small payloads (< 64 KiB)."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["bytes"] = Field("bytes", description="Discriminator tag.")
    value_b64: str = Field(
        ...,
        description=(
            "Base64-encoded body bytes. Validated as decodable base64 at "
            "admission: a malformed value is rejected with a 422 "
            "envelope_invalid and no chain is admitted. Standard "
            "(non-urlsafe) base64; newline-wrapped MIME-style input decodes "
            "normally."
        ),
    )
    content_type: str = Field(
        "application/octet-stream",
        description="Content-Type header value.",
    )


class ChainBodyRef(BaseModel):
    """Reference to a binary blob carried as a multipart part alongside the envelope."""

    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["body_ref"] = Field("body_ref", description="Discriminator tag.")
    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=(
            "Reference name. The actual bytes ride as the multipart part "
            "body_refs[<name>]; matching is by name."
        ),
    )
    content_type: str = Field(
        "application/octet-stream",
        description="Content-Type header value.",
    )


ChainBody = Annotated[
    ChainBodyJson | ChainBodyText | ChainBodyBytes | ChainBodyRef,
    Field(discriminator="kind"),
]
"""Discriminated-union body type for a chain step. The ``kind`` field
selects the concrete shape.
"""


class ChainCapture(BaseModel):
    """A named extraction from a step's response body."""

    model_config = ConfigDict(strict=True, extra="forbid", populate_by_name=True)

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=("Capture name; referenceable as {{step_name.name}} in later steps."),
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
            "Optional capture-observation TTL in seconds: how long Phantom's "
            "local observation of this captured value stays fresh. This is "
            "Phantom's own clock, distinct from any upstream capability "
            "lifetime. If set and a later step uses the value after the TTL "
            "has elapsed, capture-expiry re-execution behavior (ADR-011) "
            "applies. None means non-expiring."
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


class ChainStep(BaseModel):
    """One HTTP request in the chain."""

    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description=("Step name; referenced from later steps via {{name.capture_name}}."),
    )
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        ...,
        description="HTTP method.",
    )
    url: str = Field(
        ...,
        description=(
            "Target URL or path; may contain {{step_name.capture_name}} "
            "placeholders resolved at execution time. Every referenced capture "
            "must be a string, number, boolean or null and is spliced as its "
            "string form with no percent-encoding added, so the template owns "
            "its own encoding; a captured object or array fails the step."
        ),
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Outbound headers. Values may contain {{step.var}} placeholders. "
            "Every referenced capture must be a string, number, boolean or "
            "null and renders as its string form; a captured object or array "
            "fails the step, and so does a captured string containing a "
            "carriage return, line feed or NUL, which cannot be sent in a "
            "header value."
        ),
    )
    body: ChainBody | None = Field(
        None,
        description="Request body. Omit for body-less methods (GET, DELETE).",
    )
    capture: list[ChainCapture] = Field(
        default_factory=list,
        description=(
            "Fields to extract from this step's response. Captured values "
            "become available to later steps and are persisted on the upload row."
        ),
    )
    idempotency_header: str | None = Field(
        None,
        description=(
            "Optional header name. If set, Phantom sends it with the envelope's "
            "idempotency_key on every attempt of this step. Capture reexecution "
            "still occurs when omitted and may create a duplicate; declaring an "
            "upstream-honored header is required for identity-safe reexecution "
            "but does not itself renew a returned capability. See ADR-011. "
            "Typical value: 'Idempotency-Key'."
        ),
    )


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
            "from any step M where M < N via {{step_name.capture_name}}."
        ),
    )
    default_target: HttpUrl | None = Field(
        None,
        description=(
            "Optional default target URL applied to steps that don't specify "
            "one. If a step's `url` is a path it's appended to this; if it's "
            "a full URL, this is ignored."
        ),
    )
    templated: bool = Field(
        True,
        description=(
            "Whether ``{{step_name.capture_name}}`` placeholder substitution "
            "runs for this chain. True (the default) is the authored-chain "
            "behaviour: URLs, header values and text/JSON bodies are treated "
            "as templates and resolved against captured values at send time. "
            "False marks the chain as literal: no substitution runs, no "
            "capture-TTL gate runs, and a brace span in any field is "
            "forwarded verbatim. Phantom's raw-intake catch-all sets False, "
            "because a synthesized chain has no captures and its URL carries "
            "a producer-supplied object key that may contain braces. A "
            "producer submitting literal chains may set it too."
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


class CapturedStep(BaseModel):
    """Captured values from one step, surfaced in ChainResponse on terminal-success."""

    model_config = ConfigDict(strict=True, extra="forbid")

    step_name: str = Field(..., description="The producing step's name.")
    values: dict[str, Any] = Field(
        ...,
        description=(
            "Captured values keyed by capture name; matches the step's declared captures."
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
            "Name of the most recently terminal-success step in this chain; None if none."
        ),
    )
    captured: list[CapturedStep] = Field(
        default_factory=list,
        description=(
            "Per-step captured values. Populated as steps complete; persists "
            "per the row's succeeded_metadata_seconds retention window."
        ),
    )
