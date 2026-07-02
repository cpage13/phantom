"""Unit tests for phantom.models.chain — ADR-010 envelope schema."""

from __future__ import annotations

import json
from typing import get_args
from uuid import uuid4

import pytest
from phantom.models.chain import (
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
from pydantic import TypeAdapter, ValidationError


def test_chain_state_values() -> None:
    """The nine canonical chain states are present and exact."""
    assert set(get_args(ChainState)) == {
        "queued",
        "attempting",
        "succeeded",
        "failed",
        "auth_expired",
        "stored",
        "cancelled",
        "corrupted",
        "expired",
    }


def test_body_variants_strict() -> None:
    """Each body variant rejects unknown keys via extra='forbid'."""
    with pytest.raises(ValidationError):
        ChainBodyJson(kind="json", value={}, extra_field="nope")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ChainBodyText(kind="text", value="x", oops="nope")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ChainBodyBytes(kind="bytes", value_b64="AA==", extra="nope")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ChainBodyRef(kind="body_ref", name="body", garbage="nope")  # type: ignore[call-arg]


def test_body_discriminator() -> None:
    """The discriminated union dispatches by ``kind``."""
    adapter: TypeAdapter[ChainBody] = TypeAdapter(ChainBody)
    obj = adapter.validate_python({"kind": "json", "value": {"a": 1}})
    assert isinstance(obj, ChainBodyJson)
    obj2 = adapter.validate_python({"kind": "body_ref", "name": "body"})
    assert isinstance(obj2, ChainBodyRef)


def test_body_ref_name_pattern() -> None:
    """body_ref names must match the snake_case regex."""
    with pytest.raises(ValidationError):
        ChainBodyRef(kind="body_ref", name="Body")
    with pytest.raises(ValidationError):
        ChainBodyRef(kind="body_ref", name="0body")
    ChainBodyRef(kind="body_ref", name="body_1")


def test_capture_from_alias() -> None:
    """``from`` alias maps to ``from_path``; round-trips via JSON."""
    payload = {"name": "x", "from": "$.y", "ttl_seconds": 3600}
    obj = ChainCapture.model_validate(payload)
    assert obj.from_path == "$.y"
    assert obj.ttl_seconds == 3600

    dumped = obj.model_dump(by_alias=True)
    assert dumped["from"] == "$.y"


def test_capture_ttl_must_be_positive() -> None:
    """``ttl_seconds`` rejects 0 (must be >= 1)."""
    with pytest.raises(ValidationError):
        ChainCapture.model_validate({"name": "x", "from": "$.y", "ttl_seconds": 0})


def test_step_name_pattern() -> None:
    """Step names must be lowercase ASCII identifiers (snake_case)."""
    with pytest.raises(ValidationError):
        ChainStep(name="create-file", method="POST", url="https://x/y")
    step = ChainStep(name="create_file", method="POST", url="https://x/y")
    assert step.name == "create_file"


def test_step_method_literal() -> None:
    """Step method is restricted to the documented verbs."""
    with pytest.raises(ValidationError):
        ChainStep(name="x", method="OPTIONS", url="https://x/y")  # type: ignore[arg-type]


def test_capture_name_pattern() -> None:
    """Capture names must be snake_case."""
    with pytest.raises(ValidationError):
        ChainCapture.model_validate({"name": "Upload-Url", "from": "$.x"})


def test_envelope_requires_at_least_one_step() -> None:
    """An empty steps list is rejected at validation time."""
    with pytest.raises(ValidationError):
        ChainEnvelope(
            chain_id=uuid4(),
            idempotency_key="k",
            steps=[],
        )


def test_envelope_blank_idempotency_key_defaults_to_chain_id() -> None:
    """A blank ``idempotency_key`` auto-defaults to ``str(chain_id)`` (§ 2.2).

    A whitespace-only value is treated as absent (not a 422); the
    ``mode="before"`` validator fills it with ``str(chain_id)`` before the
    ``min_length=1`` required check would reject it. Supersedes the prior
    contract where a blank key was a validation error (Finding F-6).
    """
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key="",
        steps=[
            ChainStep(name="x", method="POST", url="https://a/b"),
        ],
    )
    assert envelope.idempotency_key == str(chain_id)


def test_envelope_omitted_idempotency_key_defaults_to_chain_id() -> None:
    """An OMITTED ``idempotency_key`` auto-defaults to ``str(chain_id)`` (§ 2.2).

    Exercised via ``model_validate_json`` (the wire path a raw-HTTP client
    hits): with the key absent entirely the ``mode="before"`` validator fills
    it, so the otherwise-required field accepts omission.
    """
    chain_id = uuid4()
    envelope = ChainEnvelope.model_validate_json(
        json.dumps(
            {
                "chain_id": str(chain_id),
                "steps": [{"name": "x", "method": "POST", "url": "https://a/b"}],
            }
        )
    )
    assert envelope.idempotency_key == str(chain_id)


def test_envelope_whitespace_idempotency_key_defaults_to_chain_id() -> None:
    """A whitespace-only ``idempotency_key`` is treated as absent (§ 2.2)."""
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key="   ",
        steps=[ChainStep(name="x", method="POST", url="https://a/b")],
    )
    assert envelope.idempotency_key == str(chain_id)


def test_envelope_nonblank_idempotency_key_preserved_verbatim() -> None:
    """A non-blank caller ``idempotency_key`` wins and is kept verbatim (§ 2.2)."""
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key="caller-supplied-key",
        steps=[ChainStep(name="x", method="POST", url="https://a/b")],
    )
    assert envelope.idempotency_key == "caller-supplied-key"


def test_envelope_missing_chain_id_still_errors_when_key_provided() -> None:
    """With only ``chain_id`` omitted, its required-error still surfaces (§ 2.2).

    The validator injects only when ``chain_id`` is present, so a missing
    ``chain_id`` is never masked by the idempotency-key default.
    """
    with pytest.raises(ValidationError) as exc_info:
        ChainEnvelope.model_validate(
            {
                "idempotency_key": "k",
                "steps": [{"name": "x", "method": "POST", "url": "https://a/b"}],
            }
        )
    assert any(err["loc"] == ("chain_id",) for err in exc_info.value.errors())


def test_envelope_both_identifiers_missing_surfaces_chain_id_error() -> None:
    """With BOTH identifiers omitted, the validator declines to inject (§ 2.2).

    The idempotency-key default never fires without a ``chain_id``, so the
    ``chain_id`` required-error surfaces rather than being swallowed.
    """
    with pytest.raises(ValidationError) as exc_info:
        ChainEnvelope.model_validate(
            {"steps": [{"name": "x", "method": "POST", "url": "https://a/b"}]}
        )
    error_locs = {err["loc"] for err in exc_info.value.errors()}
    assert ("chain_id",) in error_locs


def test_envelope_roundtrip() -> None:
    """A two-step upstream-style envelope round-trips through JSON."""
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key="abc-123",
        steps=[
            ChainStep(
                name="create_file",
                method="POST",
                url="https://upstream.example.com/v2/files",
                body=ChainBodyJson(kind="json", value={"file_name": "x.parquet"}),
                capture=[
                    ChainCapture.model_validate(
                        {"name": "upload_url", "from": "$.uploadUrl", "ttl_seconds": 604800},
                    ),
                    ChainCapture.model_validate(
                        {"name": "file_information", "from": "$.fileInformation"},
                    ),
                ],
                idempotency_header="Idempotency-Key",
            ),
            ChainStep(
                name="put_s3",
                method="PUT",
                url="{{create_file.upload_url}}",
                body=ChainBodyRef(kind="body_ref", name="body"),
            ),
        ],
    )
    raw = envelope.model_dump_json()
    rebuilt = ChainEnvelope.model_validate_json(raw)
    assert rebuilt.chain_id == chain_id
    assert len(rebuilt.steps) == 2
    assert rebuilt.steps[0].capture[0].from_path == "$.uploadUrl"


def test_chain_response_shape() -> None:
    """ChainResponse deserializes the documented shape (round-trip via JSON)."""
    chain_id = uuid4()
    obj = ChainResponse(
        chain_id=chain_id,
        state="succeeded",
        last_step_completed="put_s3",
        captured=[
            CapturedStep(
                step_name="create_file",
                values={"upload_url": "https://x", "file_information": {"id": "y"}},
            ),
        ],
    )
    blob = obj.model_dump_json()
    rebuilt = ChainResponse.model_validate_json(blob)
    assert rebuilt.chain_id == chain_id
    assert rebuilt.state == "succeeded"
    assert rebuilt.captured[0].step_name == "create_file"


def test_captured_step_strict() -> None:
    """CapturedStep enforces extra='forbid'."""
    with pytest.raises(ValidationError):
        CapturedStep.model_validate(
            {"step_name": "x", "values": {}, "extra": "nope"},
        )


def test_envelope_strict_no_extras() -> None:
    """ChainEnvelope rejects extra fields."""
    chain_id = str(uuid4())
    with pytest.raises(ValidationError):
        ChainEnvelope.model_validate(
            {
                "chain_id": chain_id,
                "idempotency_key": "k",
                "steps": [
                    {"name": "x", "method": "POST", "url": "https://x/y"},
                ],
                "extra_field": "nope",
            },
        )


def test_default_target_normalizes() -> None:
    """``default_target`` accepts a valid HTTP URL."""
    envelope = ChainEnvelope(
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[ChainStep(name="x", method="POST", url="/v2/files")],
        default_target="https://upstream.example.com",  # type: ignore[arg-type]
    )
    assert envelope.default_target is not None
    # Round-trip: emit and re-parse to confirm.
    blob = envelope.model_dump_json()
    json.loads(blob)
