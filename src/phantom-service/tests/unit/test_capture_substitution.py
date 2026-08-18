"""F8: a captured value is substituted into the parsed body, not its serialization.

``substitute`` returned ``str(value)`` with no escaping and no type check, and
the JSON body arm serialized the template FIRST and regex-substituted into the
serialized string. Two harm classes followed from that ordering.

Class A is loud. A captured value carrying any character ``json.dumps``
escapes, a double quote or a backslash or a newline, produced malformed JSON:
the upstream answered 400, the sender drove the row terminal ``failed`` with
``last_error=4xx_status_400``, and the buffered upload was permanently lost.

Class B is silent and worse in kind. A captured object or array reached the
wire as a PYTHON REPR inside a JSON string (``"{'a': 1}"``), the request was
well-formed, the upstream accepted it and the row SUCCEEDED. Nothing anywhere
observed the corruption.

The fix substitutes into the parsed structure and serializes ONCE at the end,
which makes class A structurally impossible rather than escaped: every string
the walker produces is escaped by ``json.dumps``, not by a hand-written
escaper that has to be correct for every input. The three non-JSON contexts
(URL, header value, text body) have no serializer to protect them, so their
rule is a type gate rather than an escaper: a scalar splices as text exactly
as it does today, and a non-scalar is refused as ``CaptureNotRenderable``.

Two restraints bound the change and two tests pin them. Scalars are NOT
type-preserved: a captured ``7`` still renders ``"7"``, a ``True`` still
renders ``"True"``, because whether the upstream wants ``7`` or ``"7"`` is a
schema question Phantom cannot answer, and only the positions that cannot
possibly be right today move. And an object KEY can never take structure,
whole-value or not: Python raises ``TypeError: unhashable type`` at dict
construction, which would escape the executor into a sender that catches only
``sqlite3.OperationalError`` and crash-loop the service.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.chain import (
    ChainBodyJson,
    ChainBodyText,
    ChainEnvelope,
    ChainStep,
)
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.transport import UpstreamRequest, UpstreamResponse

_HOST = "up.example"

# The producing step every placeholder in this file references. One letter
# because ``_PLACEHOLDER_RE`` bounds both halves to ``[a-z][a-z0-9_]*``.
_PRODUCER = "s"


class _CapturingUpstream:
    """Stub :class:`UpstreamClient` recording the request it was handed."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []

    async def start(self) -> None:
        """No-op lifecycle hook."""
        return None

    async def stop(self) -> None:
        """No-op lifecycle hook."""
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        """Record ``req`` and answer 200 with an empty body."""
        self.requests.append(req)
        return UpstreamResponse(status=200, headers={}, body=b"")


class _UnusedTokenCache:
    """A TokenCache that must never be touched on a forward-as-is route."""

    async def get(self, endpoint: str, uid: str) -> None:
        """Fail loudly: an ``auth_mode=none`` route reads no token slot."""
        raise AssertionError("token_cache.get must not be called on an auth_mode=none route")

    async def set(self, endpoint: str, uid: str, bearer: str, *, source: object) -> None:
        """Fail loudly: an ``auth_mode=none`` route writes no token slot."""
        raise AssertionError("token_cache.set must not be called on an auth_mode=none route")

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        """Fail loudly: an ``auth_mode=none`` route marks no token slot."""
        raise AssertionError("token_cache.mark_bad must not be called on an auth_mode=none route")


def _executor(client: _CapturingUpstream | None = None) -> ChainExecutor:
    """Build an executor over one forward-as-is route matching ``_HOST``.

    ``_render_body`` is an INSTANCE method that reaches for
    ``self._captures_as_dict`` and ``self._substitute_or_literal``, so every
    body test binds a real executor exactly as ``test_executor.py`` does.

    Args:
        client: The upstream stub, when the test drives ``execute_one_step``.

    Returns:
        A configured :class:`ChainExecutor`.
    """
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="up", hosts=[_HOST], auth_mode="none")],
    )
    return ChainExecutor(
        token_cache=_UnusedTokenCache(),
        upstream_client=client if client is not None else _CapturingUpstream(),
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )


def _captured(**values: Any) -> CapturedValues:
    """Build a one-step capture store under the ``s`` producing step.

    Args:
        **values: The capture names and their JSON-shaped values.

    Returns:
        The populated :class:`CapturedValues`.
    """
    now = datetime.now(tz=UTC)
    return CapturedValues(
        steps={
            _PRODUCER: CapturedStepValues(
                values=dict(values),
                captured_at=now,
                expires_at=dict.fromkeys(values),
            )
        }
    )


def _json_step(value: dict[str, Any]) -> ChainStep:
    """Build a one-step PUT carrying ``value`` as its JSON body."""
    return ChainStep(
        name="upload",
        method="PUT",
        url=f"https://{_HOST}/o",
        headers={},
        body=ChainBodyJson(kind="json", value=value),
        capture=[],
        idempotency_header=None,
    )


def _text_step(value: str) -> ChainStep:
    """Build a one-step PUT carrying ``value`` as its text body."""
    return ChainStep(
        name="upload",
        method="PUT",
        url=f"https://{_HOST}/o",
        headers={},
        body=ChainBodyText(kind="text", value=value, content_type="text/plain"),
        capture=[],
        idempotency_header=None,
    )


def _render(
    executor: ChainExecutor,
    step: ChainStep,
    captured: CapturedValues,
    *,
    templated: bool = True,
) -> tuple[bytes, str | None, bool, Any]:
    """Drive ``_render_body`` through a DEFENSIVE unpack.

    ``_render_body``'s return tuple grows a fourth member (the refusal) with
    the fix, so a pre-fix run that unpacked four would raise ``ValueError``:
    a structural failure where the M2 gate requires a behavioural one.
    Indexing instead of unpacking is what lets the byte-identity test and the
    three class-A/class-B tests produce real results on BOTH trees.

    Args:
        executor: The bound executor.
        step: The step whose body is rendered.
        captured: The chain's captured values.
        templated: The envelope's ``templated`` marker (N3).

    Returns:
        ``(body_bytes, content_type, all_resolved, refusal_or_None)``.
    """
    result = executor._render_body(step, captured, {}, templated=templated)
    refusal = result[3] if len(result) > 3 else None
    return result[0], result[1], result[2], refusal


def _row(envelope: ChainEnvelope, captured: CapturedValues) -> UploadRow:
    """Build an ``attempting`` row around ``envelope`` for ``execute_one_step``."""
    now = datetime.now(tz=UTC)
    return UploadRow(
        chain_id=envelope.chain_id,
        instance_id="primary",
        group_id=envelope.chain_id,
        multifile_id=envelope.chain_id,
        send_order=0,
        route_name="up",
        state="attempting",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint=_HOST,
        uid="u",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=captured,
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


def test_quote_bearing_capture_renders_valid_json() -> None:
    """A captured double quote must not break the JSON body it lands in.

    Objective: class A, the review's own reproduction. The old path dumped the
    template to a string and spliced the raw value into it, so a quote closed
    the JSON string early.

    Success: the rendered bytes parse with ``json.loads`` and the value
    round-trips equal to the capture. Pre-fix ``json.loads`` raises
    ``JSONDecodeError`` on ``{"name": "He said "hi""}``.
    """
    title = 'He said "hi"'
    body_bytes, content_type, ok, refusal = _render(
        _executor(),
        _json_step({"name": f"{{{{{_PRODUCER}.title}}}}"}),
        _captured(title=title),
    )

    assert ok is True, "a resolvable placeholder must report all_resolved"
    assert refusal is None, f"a plain string capture is renderable; got {refusal!r}"
    assert content_type == "application/json"
    parsed = json.loads(body_bytes)
    assert parsed["name"] == title, (
        f"the captured title must round-trip through the body bytes; got {parsed!r}"
    )


def test_backslash_capture_renders_valid_json() -> None:
    """A captured backslash must not produce an invalid JSON escape.

    Objective: class A's second member. ``C:\\path`` spliced raw makes ``\\p``,
    which is not a legal JSON escape, so the upstream answered 400 and the row
    died ``failed`` exactly as the quote case did.

    Success: the bytes parse and the value round-trips equal to the capture.
    """
    path = "C:\\path"
    body_bytes, _, ok, refusal = _render(
        _executor(),
        _json_step({"name": f"{{{{{_PRODUCER}.p}}}}"}),
        _captured(p=path),
    )

    assert ok is True
    assert refusal is None
    parsed = json.loads(body_bytes)
    assert parsed["name"] == path, f"a backslash-bearing capture must round-trip; got {parsed!r}"


def test_object_capture_lands_as_structure() -> None:
    """A whole-value object capture is delivered as an object, not a repr.

    Objective: class B, the silent one. The old path rendered
    ``str({"a": 1})``, which is the PYTHON repr ``{'a': 1}``, inside a JSON
    string; the body parsed, the upstream accepted it and the row succeeded
    while the data delivered was wrong.

    Success: the parsed body carries a real object at that key. Pre-fix the
    value is the string ``{'a': 1}``, which is the assertion that fails.
    """
    obj = {"a": 1}
    body_bytes, _, ok, refusal = _render(
        _executor(),
        _json_step({"meta": f"{{{{{_PRODUCER}.obj}}}}"}),
        _captured(obj=obj),
    )

    assert ok is True
    assert refusal is None
    parsed = json.loads(body_bytes)
    assert parsed["meta"] == obj, (
        f"a whole-value object capture must be delivered as structure; got {parsed!r}"
    )


def test_embedded_non_scalar_is_refused() -> None:
    """An object capture inside a larger string is refused, not repr-spliced.

    Objective: the embedded position, where the result MUST be a string and no
    structure can be delivered, so refusing is the only honest answer.

    Success: the render returns ``CaptureNotRenderable`` with
    ``reason="non_scalar"`` naming the placeholder. This test cannot pass on
    the pre-fix tree, because ``CaptureNotRenderable`` is the symbol the fix
    introduces; the import is INSIDE the body so the module still collects
    there and the behavioural tests around it still run.
    """
    from phantom.chain.executor import CaptureNotRenderable

    _, _, _, refusal = _render(
        _executor(),
        _json_step({"name": f"file-{{{{{_PRODUCER}.obj}}}}.txt"}),
        _captured(obj={"a": 1}),
    )

    assert isinstance(refusal, CaptureNotRenderable), (
        f"an embedded non-scalar must be refused; got {refusal!r}"
    )
    assert refusal.reason == "non_scalar"
    assert refusal.site == "body"
    assert refusal.placeholder == f"{_PRODUCER}.obj"


@pytest.mark.asyncio
async def test_crlf_header_capture_is_refused() -> None:
    """A captured CR/LF in a header value is refused at substitution time.

    Objective: the one security-adjacent addition. Today the value is spliced,
    ``h11`` refuses to build the request with ``LocalProtocolError``, httpx
    surfaces that as an ``HTTPError``, the executor classifies it
    ``FailedNetwork`` and the row burns its whole retry budget on a request
    that can never be built, ending in ``stored``.

    Success: ``CaptureNotRenderable`` with ``reason="control_character"``
    naming the header, driven through ``execute_one_step`` so the real header
    site is exercised, and nothing reaches the upstream.
    """
    from phantom.chain.executor import CaptureNotRenderable

    client = _CapturingUpstream()
    envelope = ChainEnvelope(
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[
            ChainStep(
                name="upload",
                method="PUT",
                url=f"https://{_HOST}/o",
                headers={"X-Trace": f"{{{{{_PRODUCER}.v}}}}"},
                body=None,
                capture=[],
                idempotency_header=None,
            )
        ],
        default_target=None,
    )
    captured = _captured(v="a\r\nX-Injected: 1")

    result = await _executor(client).execute_one_step(_row(envelope, captured), body_refs={})

    assert isinstance(result, CaptureNotRenderable), (
        f"a CR/LF header capture must be refused; got {type(result).__name__}"
    )
    assert result.reason == "control_character"
    assert result.site == "header"
    assert result.header_name == "X-Trace"
    assert client.requests == [], "a refused header must never reach the upstream"


def test_non_scalar_in_object_key_is_refused_not_crashed() -> None:
    """An object capture in a KEY position is refused, and nothing raises.

    Objective: the object-key regression, and the only test whose absence
    would let a crash loop ship. A key is a string position that can NEVER
    take structure: ``{ {'a': 1}: 1 }`` raises ``TypeError: unhashable type``
    at dict construction, the exception escapes ``execute_one_step`` into a
    sender that catches only ``sqlite3.OperationalError``, the task group dies,
    recovery re-queues the row and the service crash-loops. That is exactly
    the failure class the catastrophic-guards phase exists to remove, and this
    input SUCCEEDS today.

    Success: ``CaptureNotRenderable`` with ``reason="non_scalar"`` and no
    exception escaping. Pre-fix the input renders ``{"{'a': 1}": "x"}`` and
    succeeds, so this test is red pre-fix on the assertion rather than on an
    exception. It is also the regression pin against the structural-key rule,
    under which it would raise ``TypeError``.
    """
    body_bytes, _, _, refusal = _render(
        _executor(),
        _json_step({f"{{{{{_PRODUCER}.obj}}}}": "x"}),
        _captured(obj={"a": 1}),
    )

    # The BEHAVIOURAL assertion comes first, and deliberately: it names no
    # symbol the pre-fix tree lacks, so the pre-fix run fails HERE on the
    # rendered repr key rather than on an ImportError, which would be a
    # structural failure where the M2 pair requires a behavioural one.
    assert refusal is not None, (
        f"a non-scalar in a key position must be refused, not spliced; "
        f"got body {body_bytes.decode('utf-8')!r}"
    )

    from phantom.chain.executor import CaptureNotRenderable

    assert isinstance(refusal, CaptureNotRenderable)
    assert refusal.reason == "non_scalar"
    assert refusal.placeholder == f"{_PRODUCER}.obj"


def test_unescaped_ascii_scalar_rendering_is_byte_identical() -> None:
    """Scalar rendering does not move, in either position or any context.

    Objective: the no-regression pin, and it is worth more than the
    red-to-green half. The tempting rule is that a whole-value placeholder
    should yield the capture's own JSON type, so a captured number becomes a
    JSON number. That changes the bytes of chains that work today and whether
    it is correct depends on the upstream's schema, which Phantom cannot see.
    So every scalar still renders through ``str()``.

    The table is scoped TWICE and both scopings are load-bearing. No non-ASCII
    value, because serializing last re-encodes non-ASCII as ``\\uXXXX`` (a
    declared encoding delta, same string after parsing). And no value carrying
    a character ``json.dumps`` escapes, because those are the headline fix,
    where today's bytes are malformed and identity is the thing being broken
    on purpose. Within those scopings the table DOES carry int, float, bool
    and null in the WHOLE-VALUE position, which is the case the walker's type
    test exists to keep identical.

    The expectation is computed by TODAY'S algorithm (dump-or-take the
    template, then string-replace ``str(value)``), so it is an independent
    oracle rather than a re-run of the new path.

    Success: identical bytes on both trees, for every cell.
    """
    executor = _executor()
    placeholder = f"{{{{{_PRODUCER}.v}}}}"
    # (label, value) pairs. Every string is ASCII and carries no character
    # ``json.dumps`` escapes; the four non-strings are the type-test cases.
    table: list[tuple[str, Any]] = [
        ("plain_ascii", "plain-ascii"),
        ("path_segment", "a/b"),
        ("apostrophe", "it's"),
        ("int", 7),
        ("float", 1.5),
        ("bool", True),
        ("null", None),
    ]
    positions: list[tuple[str, str]] = [
        ("whole_value", placeholder),
        ("embedded", f"pre-{placeholder}-post"),
    ]

    for label, value in table:
        captured = _captured(v=value)
        rendered_value = str(value)
        for position, template in positions:
            where = f"{label}/{position}"

            # Context 1: JSON body. Today: dump the template, then splice.
            json_body, _, ok, refusal = _render(executor, _json_step({"k": template}), captured)
            assert ok is True and refusal is None, f"{where}: a scalar must render, not refuse"
            expected_json = json.dumps({"k": template}).replace(placeholder, rendered_value)
            assert json_body == expected_json.encode("utf-8"), (
                f"{where}: JSON body bytes moved; expected {expected_json!r}, "
                f"got {json_body.decode('utf-8')!r}"
            )

            # Context 2: text body. No serializer, so the splice is the whole
            # rendering and it must not move either.
            text_body, _, text_ok, text_refusal = _render(executor, _text_step(template), captured)
            assert text_ok is True and text_refusal is None
            assert text_body == template.replace(placeholder, rendered_value).encode("utf-8"), (
                f"{where}: text body bytes moved"
            )

            # Contexts 3 and 4: the URL and a header value. Both call the one
            # send-side rule directly, which the type gate leaves untouched for
            # a scalar (the CR/LF test drives the real header site).
            for context in ("url", "header"):
                spliced, spliced_ok = executor._substitute_or_literal(
                    template, captured, templated=True
                )
                assert spliced_ok is True
                assert spliced == template.replace(placeholder, rendered_value), (
                    f"{where}: {context} text moved"
                )


def test_token_carries_no_capture_value() -> None:
    """The refusal's operator token carries identifiers only, never the value.

    Objective: the redaction guarantee. ``last_error`` reaches
    ``GET /v1/admin/chains/{chain_id}`` and the logs, and a capture is
    UPSTREAM RESPONSE DATA, which is the same hazard class as the presigned
    URL that put the identifier-only rule on the template variant, and can be
    worse.

    Success: neither the secret text nor the signature span appears anywhere
    in ``token()``; every field is an identifier or a closed literal.
    """
    from phantom.chain.executor import CaptureNotRenderable

    secret = {"k": "SECRET-VALUE", "u": "https://x.example/o?sig=deadbeef"}
    _, _, _, refusal = _render(
        _executor(),
        _json_step({"name": f"file-{{{{{_PRODUCER}.obj}}}}.txt"}),
        _captured(obj=secret),
    )

    assert isinstance(refusal, CaptureNotRenderable)
    token = refusal.token()
    assert "SECRET" not in token, f"the capture value must never reach last_error; got {token!r}"
    assert "?sig=" not in token, f"a signature span must never reach last_error; got {token!r}"
    assert token == f"upload:body:{_PRODUCER}.obj:non_scalar", (
        f"the token must be identifiers and closed literals only; got {token!r}"
    )


def test_literal_chain_body_is_untouched() -> None:
    """A literal chain's body passes through, braced key and braced value alike.

    Objective: the raw-intake inertness rule must survive the rewrite. A chain
    marked ``templated=False`` interprets no brace span anywhere, so the
    walker must return the body untouched at the top and the type gate must
    refuse nothing, even where a templated chain would refuse.

    Success: the bytes are the plain serialization of the authored body, and
    no refusal is produced.
    """
    value = {f"{{{{{_PRODUCER}.obj}}}}": f"a{{{{{_PRODUCER}.obj}}}}b"}
    body_bytes, _, ok, refusal = _render(
        _executor(),
        _json_step(value),
        _captured(obj={"a": 1}),
        templated=False,
    )

    assert ok is True, "a literal chain resolves trivially"
    assert refusal is None, "a literal chain substitutes nothing, so it can refuse nothing"
    assert body_bytes == json.dumps(value).encode("utf-8"), (
        f"a literal body must be forwarded verbatim; got {body_bytes.decode('utf-8')!r}"
    )
