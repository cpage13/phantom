"""Unit tests for ``phantom_client.models.chain`` (ADR-010 schema)."""

from __future__ import annotations

from typing import get_args
from uuid import uuid4

import pytest
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
from pydantic import TypeAdapter, ValidationError


def test_chain_state_values() -> None:
    """ChainState literal set matches ADR-010 §Response."""
    # ChainState is a ``TypeAlias`` to ``Literal[...]``; ``get_args``
    # yields the literal members directly (no ``__value__`` unwrap needed).
    assert set(get_args(ChainState)) == {
        "queued",
        "attempting",
        "succeeded",
        "failed",
        "auth_expired",
        "stored",
        "cancelled",
        "corrupted",
    }


def test_body_discriminator() -> None:
    """The body union is discriminated on the ``kind`` tag."""
    adapter: TypeAdapter[ChainBody] = TypeAdapter(ChainBody)
    body_ref = adapter.validate_python({"kind": "body_ref", "name": "blob"})
    assert isinstance(body_ref, ChainBodyRef)
    body_json = adapter.validate_python({"kind": "json", "value": {"a": 1}})
    assert isinstance(body_json, ChainBodyJson)
    body_text = adapter.validate_python({"kind": "text", "value": "hi"})
    assert isinstance(body_text, ChainBodyText)
    body_bytes = adapter.validate_python({"kind": "bytes", "value_b64": "QUJD"})
    assert isinstance(body_bytes, ChainBodyBytes)


def test_body_kind_required() -> None:
    """Missing ``kind`` raises a validation error."""
    adapter: TypeAdapter[ChainBody] = TypeAdapter(ChainBody)
    with pytest.raises(ValidationError):
        adapter.validate_python({"value": {"a": 1}})


def test_capture_from_alias_both_ways() -> None:
    """ChainCapture accepts both ``from_path`` and the ``from`` alias."""
    c1 = ChainCapture(name="upload_url", from_path="$.uploadUrl")
    assert c1.from_path == "$.uploadUrl"
    c2 = ChainCapture.model_validate({"name": "upload_url", "from": "$.uploadUrl"})
    assert c2.from_path == "$.uploadUrl"
    # Serializing by alias yields the wire form with "from".
    dumped = c2.model_dump(by_alias=True)
    assert dumped["from"] == "$.uploadUrl"
    assert "from_path" not in dumped


def test_capture_name_pattern() -> None:
    """Capture names are lowercase ASCII identifiers."""
    with pytest.raises(ValidationError):
        ChainCapture(name="Upload_URL", from_path="$.x")
    with pytest.raises(ValidationError):
        ChainCapture(name="123start", from_path="$.x")


def test_capture_ttl_seconds_positive() -> None:
    """ttl_seconds must be >= 1 when set."""
    ChainCapture(name="x", from_path="$.y", ttl_seconds=1)  # ok
    with pytest.raises(ValidationError):
        ChainCapture(name="x", from_path="$.y", ttl_seconds=0)


def test_step_validation() -> None:
    """ChainStep enforces method literal, name pattern, and body discriminator."""
    step = ChainStep(
        name="create_file",
        method="POST",
        url="/v2/files",
        body=ChainBodyJson(value={"a": 1}),
    )
    assert step.name == "create_file"
    assert step.method == "POST"
    assert step.headers == {}
    assert step.capture == []
    # Bad method
    with pytest.raises(ValidationError):
        ChainStep(name="x", method="OPTIONS", url="/y")  # type: ignore[arg-type]
    # Bad name
    with pytest.raises(ValidationError):
        ChainStep(name="Bad-Name", method="GET", url="/y")
    # extra forbidden
    with pytest.raises(ValidationError):
        ChainStep.model_validate({"name": "x", "method": "GET", "url": "/y", "unknown": 1})


def test_envelope_min_one_step() -> None:
    """Envelope rejects empty ``steps`` list."""
    chain_id = uuid4()
    with pytest.raises(ValidationError):
        ChainEnvelope(chain_id=chain_id, idempotency_key="k", steps=[])


def test_envelope_blank_idempotency_key_defaults_to_chain_id() -> None:
    """A blank idempotency_key auto-defaults to str(chain_id) (§ 2.2).

    A whitespace-only value is treated as absent (not a 422); the
    mode="before" validator fills it with str(chain_id) before the
    min_length=1 required check would reject it. Mirrors the service
    contract and supersedes the prior blank-is-rejected behavior (F-6).
    """
    chain_id = uuid4()
    step = ChainStep(name="s", method="GET", url="/x")
    env = ChainEnvelope(chain_id=chain_id, idempotency_key="", steps=[step])
    assert env.idempotency_key == str(chain_id)


def test_envelope_omitted_idempotency_key_defaults_to_chain_id() -> None:
    """An OMITTED idempotency_key auto-defaults to str(chain_id) (§ 2.2).

    Exercised via model_validate_json (the wire path): with the key absent
    entirely the mode="before" validator fills it.
    """
    chain_id = uuid4()
    env = ChainEnvelope.model_validate_json(
        f'{{"chain_id": "{chain_id}", "steps": [{{"name": "s", "method": "GET", "url": "/x"}}]}}'
    )
    assert env.idempotency_key == str(chain_id)


def test_envelope_whitespace_idempotency_key_defaults_to_chain_id() -> None:
    """A whitespace-only idempotency_key is treated as absent (§ 2.2)."""
    chain_id = uuid4()
    step = ChainStep(name="s", method="GET", url="/x")
    env = ChainEnvelope(chain_id=chain_id, idempotency_key="   ", steps=[step])
    assert env.idempotency_key == str(chain_id)


def test_envelope_nonblank_idempotency_key_preserved_verbatim() -> None:
    """A non-blank caller idempotency_key wins and is kept verbatim (§ 2.2)."""
    chain_id = uuid4()
    step = ChainStep(name="s", method="GET", url="/x")
    env = ChainEnvelope(chain_id=chain_id, idempotency_key="caller-key", steps=[step])
    assert env.idempotency_key == "caller-key"


def test_envelope_missing_chain_id_still_errors_when_key_provided() -> None:
    """With only chain_id omitted, its required-error still surfaces (§ 2.2)."""
    with pytest.raises(ValidationError) as exc_info:
        ChainEnvelope.model_validate(
            {
                "idempotency_key": "k",
                "steps": [{"name": "s", "method": "GET", "url": "/x"}],
            }
        )
    assert any(err["loc"] == ("chain_id",) for err in exc_info.value.errors())


def test_envelope_both_identifiers_missing_surfaces_chain_id_error() -> None:
    """With BOTH identifiers omitted, the validator declines to inject (§ 2.2)."""
    with pytest.raises(ValidationError) as exc_info:
        ChainEnvelope.model_validate({"steps": [{"name": "s", "method": "GET", "url": "/x"}]})
    error_locs = {err["loc"] for err in exc_info.value.errors()}
    assert ("chain_id",) in error_locs


def test_envelope_chain_id_uuid_via_json() -> None:
    """chain_id parses UUIDs from strings when validating JSON (wire path)."""
    chain_id = uuid4()
    step = ChainStep(name="s", method="GET", url="/x")
    env = ChainEnvelope.model_validate_json(
        f'{{"chain_id": "{chain_id}", "idempotency_key": "k", '
        f'"steps": [{{"name": "s", "method": "GET", "url": "/x"}}]}}'
    )
    assert env.chain_id == chain_id
    assert env.steps[0].name == step.name


def test_chain_response_roundtrip() -> None:
    """ChainResponse round-trips through JSON."""
    chain_id = uuid4()
    resp = ChainResponse(
        chain_id=chain_id,
        state="succeeded",
        last_step_completed="put_s3",
        captured=[
            CapturedStep(
                step_name="create_file",
                values={"upload_url": "https://s3/...", "file_information": {"id": "abc"}},
            )
        ],
    )
    payload = resp.model_dump_json()
    rev = ChainResponse.model_validate_json(payload)
    assert rev.chain_id == chain_id
    assert rev.state == "succeeded"
    assert rev.last_step_completed == "put_s3"
    assert len(rev.captured) == 1
    assert rev.captured[0].step_name == "create_file"
    assert rev.captured[0].values["upload_url"] == "https://s3/..."


def test_upstream_two_step_envelope_roundtrip() -> None:
    """The upstream two-step envelope (driving case in ADR-009) round-trips intact."""
    chain_id = uuid4()
    env = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="create_file",
                method="POST",
                url="https://upstream.example.com/v2/files",
                body=ChainBodyJson(value={"metadata": {"key_value_store": {}}}),
                capture=[
                    ChainCapture(name="upload_url", from_path="$.uploadUrl", ttl_seconds=604_800),
                    ChainCapture(
                        name="file_information",
                        from_path="$.fileInformation",
                        ttl_seconds=604_800,
                    ),
                ],
                idempotency_header="Idempotency-Key",
            ),
            ChainStep(
                name="put_s3",
                method="PUT",
                url="{{create_file.upload_url}}",
                body=ChainBodyRef(name="body"),
            ),
        ],
    )
    payload = env.model_dump_json(by_alias=True)
    rev = ChainEnvelope.model_validate_json(payload)
    assert rev.chain_id == chain_id
    assert len(rev.steps) == 2
    assert rev.steps[0].idempotency_header == "Idempotency-Key"
    assert rev.steps[0].capture[0].from_path == "$.uploadUrl"
    assert rev.steps[0].capture[0].ttl_seconds == 604_800
    assert isinstance(rev.steps[1].body, ChainBodyRef)
    assert rev.steps[1].body.name == "body"


def test_envelope_default_target_optional() -> None:
    """default_target defaults to None and accepts an HttpUrl when set."""
    chain_id = uuid4()
    env = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key="k",
        steps=[ChainStep(name="s", method="GET", url="/x")],
    )
    assert env.default_target is None
    env2 = ChainEnvelope.model_validate_json(
        f'{{"chain_id": "{chain_id}", "idempotency_key": "k", '
        f'"steps": [{{"name": "s", "method": "GET", "url": "/x"}}], '
        f'"default_target": "https://api.example.com"}}'
    )
    assert env2.default_target is not None
    # Pydantic HttpUrl normalizes trailing slash.
    assert str(env2.default_target).startswith("https://api.example.com")


def test_envelope_extra_forbidden() -> None:
    """Envelope rejects unknown top-level fields."""
    chain_id = uuid4()
    with pytest.raises(ValidationError):
        ChainEnvelope.model_validate_json(
            f'{{"chain_id": "{chain_id}", "idempotency_key": "k", '
            f'"steps": [{{"name": "s", "method": "GET", "url": "/x"}}], '
            f'"unknown_field": true}}'
        )
