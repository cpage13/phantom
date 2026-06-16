# 010. Request-chain envelope JSON schema (wire protocol)

ADR-009 specified the request-chain *mechanism* (multi-step buffered uploads with JSONPath capture and `{{step.var}}` substitution) but left the exact wire-protocol shape — JSON keys, body tagging, multipart conventions, method names — unspecified. The four 2026-05-12 strategy agents each invented a different shape under that gap. This ADR pins the schema so phantom, phantom-client, and phantom-emulator can build against one source of truth. Models are Pydantic v2 with `Field` descriptions on every attribute, matching the project's coding standards. Capture-expiry re-execution behavior (when a captured value's `ttl_seconds` is reached before later steps complete) is gated on upstream idempotency support and is the subject of ADR-011.

## Envelope (top-level)

```python
from typing import Annotated, Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

class ChainEnvelope(BaseModel):
    """A multi-step buffered upload, submitted as a unit to Phantom."""
    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID = Field(
        ...,
        description="Caller-supplied chain identifier; also the Phantom upload row id and the synthetic FileInformation.id returned by the upstream client.",
    )
    idempotency_key: str = Field(
        ...,
        min_length=1,
        description="Per-chain idempotency value sent to upstreams that support it. Stable across Phantom retries of any step.",
    )
    steps: list["ChainStep"] = Field(
        ...,
        min_length=1,
        description="Ordered list of HTTP steps. Step N may reference captured values from any step M where M < N via {{step_name.capture_name}} substitution.",
    )
    default_target: HttpUrl | None = Field(
        None,
        description="Optional default target URL applied to steps that don't specify one. If a step's `url` is a path (e.g., '/v2/files') it's appended to this; if it's a full URL, this is ignored.",
    )
```

## Step

```python
class ChainStep(BaseModel):
    """One HTTP request in the chain."""
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Step name; referenced from later steps via {{name.capture_name}}. Lowercase ASCII identifier.",
    )
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        ...,
        description="HTTP method.",
    )
    url: str = Field(
        ...,
        description="Target URL or path; may contain {{step_name.capture_name}} placeholders resolved at execution time.",
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Outbound headers. Values may contain {{step_name.capture_name}} placeholders.",
    )
    body: "ChainBody | None" = Field(
        None,
        description="Request body. Omit for body-less methods (GET, DELETE).",
    )
    capture: list["ChainCapture"] = Field(
        default_factory=list,
        description="Fields to extract from this step's response. Captured values become available to later steps and are persisted on the upload row per ADR-009.",
    )
    idempotency_header: str | None = Field(
        None,
        description="If set, Phantom sends this header name with the envelope's idempotency_key value on every attempt of this step. Required for ADR-011's re-execution behavior. Typical value: 'Idempotency-Key'.",
    )
```

## Body (discriminated union over `kind`)

```python
class ChainBodyJson(BaseModel):
    """JSON request body."""
    model_config = ConfigDict(strict=True, extra="forbid")
    kind: Literal["json"] = "json"
    value: dict[str, Any] = Field(
        ...,
        description="JSON object serialized as the request body. May contain {{step.var}} placeholders in string values.",
    )

class ChainBodyText(BaseModel):
    """Plain-text request body."""
    model_config = ConfigDict(strict=True, extra="forbid")
    kind: Literal["text"] = "text"
    value: str = Field(..., description="Text body. May contain {{step.var}} placeholders.")
    content_type: str = Field("text/plain", description="Content-Type header value.")

class ChainBodyBytes(BaseModel):
    """Inline base64-encoded body. Use only for small payloads (< 64 KiB); larger blobs go via body_ref."""
    model_config = ConfigDict(strict=True, extra="forbid")
    kind: Literal["bytes"] = "bytes"
    value_b64: str = Field(..., description="Base64-encoded body bytes.")
    content_type: str = Field("application/octet-stream", description="Content-Type header value.")

class ChainBodyRef(BaseModel):
    """Reference to a binary blob carried alongside the envelope as a multipart part."""
    model_config = ConfigDict(strict=True, extra="forbid")
    kind: Literal["body_ref"] = "body_ref"
    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Reference name. The actual bytes ride as the multipart part body_refs[<name>].",
    )
    content_type: str = Field("application/octet-stream", description="Content-Type header value.")

ChainBody = Annotated[
    ChainBodyJson | ChainBodyText | ChainBodyBytes | ChainBodyRef,
    Field(discriminator="kind"),
]
```

## Capture

```python
class ChainCapture(BaseModel):
    """A named extraction from a step's response."""
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Capture name; referenceable as {{step_name.name}} in later steps. Lowercase ASCII identifier.",
    )
    from_path: str = Field(
        ...,
        alias="from",
        description="JSONPath expression evaluated against this step's response body. First match wins.",
    )
    ttl_seconds: int | None = Field(
        None,
        ge=1,
        description="Optional capture-value lifetime in seconds. If set and a later step uses this value after the TTL has elapsed, capture-expiry re-execution behavior (ADR-011) applies. If None, the capture is treated as non-expiring.",
    )
```

## Multipart shape (when any body_ref is used)

When the envelope contains any `body_ref` body kinds, the chain is submitted as a multipart request:
- Part named `envelope` — content-type `application/json` — JSON-serialized `ChainEnvelope`.
- Part named `body_refs[<name>]` — content-type as declared in the body_ref — one part per `body_ref` body, where `<name>` matches the `name` field. Order is irrelevant; matching is by name.

When the envelope contains no `body_ref` bodies, submission is a single JSON request (content-type `application/json`), envelope serialized as the request body.

## Template substitution

Exactly one substitution form is supported: `{{step_name.capture_name}}`.

- Substitution evaluates server-side (Phantom-the-service) just before each step executes.
- Substitution is string-level: the placeholder is replaced with the captured value's string representation. For values captured as objects (e.g., a nested JSON), the substitution context (URL / header / JSON body string field) determines serialization.
- No nested templates. No expressions. No auth-fields-from-Phantom (no `{{auth.bearer}}`, no `{{now}}`). The caller supplies everything else explicitly.
- Unresolved placeholders at execution time cause the step (and the chain) to fail fast with a 4xx-shaped admin error — they do not retry.

## phantom-client method

Exactly one canonical submission method:

```python
class PhantomClient:
    async def submit_chain(
        self,
        envelope: ChainEnvelope,
        body_refs: dict[str, bytes] | None = None,
    ) -> ChainResponse:
        """Submit a request-chain envelope for buffered execution.

        Args:
            envelope: The chain to execute.
            body_refs: Bytes for each body_ref in the envelope, keyed by name.
                Required when the envelope contains any body_ref bodies; the keys
                must exactly match the `name` values declared in the envelope.

        Returns:
            ChainResponse with the chain's current state and (when terminal-success)
            captured values from each step.
        """
        ...
```

No other chain-submission method exists in phantom-client. `submit_chain` is the only name. `send_chain`, `send_request_chain`, etc. are not.

## Response

```python
ChainState = Literal[
    "queued",
    "attempting",
    "succeeded",
    "failed",
    "auth_expired",
    "stored",
    "cancelled",
]

class CapturedStep(BaseModel):
    """Captured values from one step, surfaced in ChainResponse on terminal-success states."""
    model_config = ConfigDict(strict=True, extra="forbid")
    step_name: str = Field(..., description="The producing step's name.")
    values: dict[str, Any] = Field(
        ...,
        description="Captured values keyed by capture name. JSON-shaped; matches the captures declared on the step.",
    )

class ChainResponse(BaseModel):
    """Phantom's reply to submit_chain and the shape of GET /v1/admin/uploads/{chain_id}."""
    model_config = ConfigDict(strict=True, extra="forbid")

    chain_id: UUID
    state: ChainState
    last_step_completed: str | None = Field(
        None,
        description="Name of the most recently terminal-success step in this chain. None if no step has completed.",
    )
    captured: list[CapturedStep] = Field(
        default_factory=list,
        description="Per-step captured values. Populated as steps complete; persists per the row's retention window (succeeded_metadata_seconds default 180s).",
    )
```

## Wire-protocol decisions every package follows

All packages (phantom, phantom-client, phantom-emulator) build their
wire-protocol handling against this schema. Specifically:

- Method name on phantom-client is `submit_chain` (not `send_request_chain`, not `send_chain`).
- Body shape is the discriminated union above (not `body_json` / `body_ref` separate fields).
- Capture shape is `list[ChainCapture]` with `name` / `from` / `ttl_seconds` (not `dict[var, jsonpath]` with sibling `capture_ttl_seconds`).
- Multipart parts use the names `envelope` and `body_refs[<name>]` (not `files`).
- Idempotency expressed as a single per-step `idempotency_header` (not separate `idempotency_header` + `idempotency_value`); the value is always the envelope's `idempotency_key`.

Status: Accepted
Date: 2026-05-12
