"""Unit tests for phantom.chain.parser."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import uuid4

import pytest
from phantom.chain.parser import (
    ENVELOPE_MAX_BYTES,
    ParserError,
    parse_json_request,
    parse_multipart_request,
)


@dataclass
class _Part:
    name: str
    data: bytes
    content_type: str = "application/octet-stream"
    filename: str | None = None

    async def read(self, max_bytes: int) -> bytes:
        return self.data[:max_bytes]


async def _parts(*items: _Part) -> AsyncIterator[_Part]:
    for item in items:
        yield item


def _valid_envelope_json(chain_id: str | None = None) -> bytes:
    chain_id = chain_id or str(uuid4())
    return (
        b'{"chain_id":"'
        + chain_id.encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"create_file","method":"POST","url":"https://files/v2/files",'
        + b'"capture":[{"name":"upload_url","from":"$.uploadUrl"}]},'
        + b'{"name":"put_s3","method":"PUT",'
        + b'"url":"{{create_file.upload_url}}",'
        + b'"body":{"kind":"body_ref","name":"body"}}'
        + b"]}"
    )


@pytest.mark.asyncio
async def test_parse_json_valid() -> None:
    """A valid JSON envelope parses cleanly."""
    body = (
        b'{"chain_id":"'
        + str(uuid4()).encode()
        + b'","idempotency_key":"k","steps":[{"name":"x","method":"POST","url":"https://a/b"}]}'
    )
    envelope, body_refs = await parse_json_request(
        body, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    assert envelope.idempotency_key == "k"
    assert body_refs == {}


@pytest.mark.asyncio
async def test_parse_json_invalid_raises() -> None:
    """Malformed JSON raises envelope_invalid."""
    with pytest.raises(ParserError) as exc_info:
        await parse_json_request(
            b"{ not json", instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "envelope_invalid"


@pytest.mark.asyncio
async def test_json_body_too_large_rejected() -> None:
    """A body exceeding the cap raises envelope_invalid with body_too_large."""
    body = b"x" * 1000
    with pytest.raises(ParserError) as exc_info:
        await parse_json_request(
            body, instance_id="primary", request_id="r", max_buffered_bytes=100
        )
    assert exc_info.value.code == "envelope_invalid"
    assert exc_info.value.details.get("reason") == "body_too_large"


@pytest.mark.asyncio
async def test_multipart_happy() -> None:
    """Multipart with envelope + body_refs[body] parses."""
    parts = _parts(
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        _Part(name="body_refs[body]", data=b"hello", content_type="application/octet-stream"),
    )
    envelope, body_refs = await parse_multipart_request(
        parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    assert len(envelope.steps) == 2
    assert body_refs == {"body": b"hello"}


@pytest.mark.asyncio
async def test_multipart_missing_ref_raises() -> None:
    """A declared body_ref without a matching part raises body_ref_missing."""
    parts = _parts(
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        # no body_refs[body]
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_multipart_request(
            parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "body_ref_missing"


@pytest.mark.asyncio
async def test_multipart_orphan_raises() -> None:
    """A body_refs[part] without a matching envelope ref raises body_ref_orphan."""
    parts = _parts(
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        _Part(name="body_refs[body]", data=b"x"),
        _Part(name="body_refs[extra]", data=b"y"),
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_multipart_request(
            parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "body_ref_orphan"


@pytest.mark.asyncio
async def test_multipart_duplicate_body_ref_raises() -> None:
    """Two parts with the SAME body_refs[name] raise body_ref_duplicate (finding E-1).

    Silently last-wins dropped the first part's bytes with no signal to
    the producer — inconsistent with the structural rejection of
    body_ref_missing / body_ref_orphan. The parser now rejects the
    ambiguity.
    """
    parts = _parts(
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        _Part(name="body_refs[body]", data=b"first-body"),
        _Part(name="body_refs[body]", data=b"second-body"),
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_multipart_request(
            parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "body_ref_duplicate"
    assert exc_info.value.details.get("name") == "body"


@pytest.mark.asyncio
async def test_multipart_duplicate_envelope_part_raises() -> None:
    """Two parts named ``envelope`` raise envelope_duplicate (finding R3-9).

    The E-1 fix guarded duplicate ``body_refs[<name>]`` parts but not a
    duplicate ``envelope`` part — silently last-wins kept the SECOND
    envelope and dropped the first with no signal. The envelope carries
    the chain_id (row PK), destination, and step chain, so the producer could
    not tell which chain Phantom admitted. Distinct chain_ids here prove
    the first was dropped. Parity with the duplicate-step-name and
    duplicate-body_ref rejections: the parser must reject the ambiguity,
    never silently pick one.
    """
    parts = _parts(
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        _Part(name="envelope", data=_valid_envelope_json(), content_type="application/json"),
        _Part(name="body_refs[body]", data=b"hello"),
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_multipart_request(
            parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "envelope_duplicate"


@pytest.mark.asyncio
async def test_dangling_placeholder_rejected() -> None:
    """A {{step.var}} referencing an undeclared capture is rejected."""
    body = (
        b'{"chain_id":"'
        + str(uuid4()).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"a","method":"POST","url":"https://x/{{b.c}}"},'
        + b'{"name":"b","method":"POST","url":"https://x/b"}'
        + b"]}"
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_json_request(
            body, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "template_unresolved"


@pytest.mark.asyncio
async def test_duplicate_step_names_rejected() -> None:
    """Two steps with the same name raises envelope_invalid."""
    body = (
        b'{"chain_id":"'
        + str(uuid4()).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"x","method":"POST","url":"https://a/b"},'
        + b'{"name":"x","method":"POST","url":"https://a/b"}'
        + b"]}"
    )
    with pytest.raises(ParserError) as exc_info:
        await parse_json_request(
            body, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert exc_info.value.code == "envelope_invalid"


@pytest.mark.asyncio
async def test_envelope_part_size_capped() -> None:
    """An oversized envelope part raises envelope_invalid."""
    parts = _parts(
        _Part(name="envelope", data=b"x" * (ENVELOPE_MAX_BYTES + 10)),
    )
    with pytest.raises(ParserError):
        await parse_multipart_request(
            parts, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
