"""N3: a chain marked ``templated=False`` is template-inert everywhere.

A stock object-storage client may PUT to a key that literally contains a
``{{...}}`` span. Nothing forbids it: object keys are arbitrary bytes, and the
catch-all deliberately puts the bucket and key ONLY into the step ``url``
because ``ChainStep.name`` and ``ChainBodyRef.name`` are regex-constrained.
The synthesized step therefore carried a literal URL that the executor treated
as a template: ``substitute`` found ``{{b.c}}``, found no capture named ``b``,
and the row terminated ``failed`` with a template error it never had a
template for. A valid upload was destroyed by its own key.

A producer-supplied envelope with a braced key is rejected 422 at admission by
the parser's static placeholder pass, so the defect surfaces at SEND for raw
intake only. That asymmetry is why the executor is the enforcement point, and
it is also why the marker needs an admission-side inert point: a producer who
sets ``templated=false`` and writes literal braces, which is the only reason
to set the flag, would otherwise still be rejected by that same static pass.

There are SIX inert points: the parser's static pass and the capture-TTL
pre-pass, plus the four substitution sites (URL, header values, JSON body,
text body). These tests cover all six, plus the two counter-properties that
bound the change: a templated chain is unaffected, and a persisted envelope
written before the field existed still deserializes as templated.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import (
    CaptureExpiredRewind,
    CaptureExpiredStored,
    ChainExecutor,
    Succeeded,
    TemplateUnresolved,
)
from phantom.chain.parser import ParserError, envelope_from_persistence_json, parse_json_request
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routes.catch_all import _synthesize_envelope
from phantom.routing import resolve_route
from phantom.transport import UpstreamRequest, UpstreamResponse

pytestmark = pytest.mark.asyncio

_HOST = "up.example"

# The object key a stock client is free to use, and which used to kill the
# upload. The braces are CONTENT, not a capture reference.
_BRACED_URL = f"https://{_HOST}/bucket/a{{{{b.c}}}}d"


class _CapturingUpstream:
    """Stub :class:`UpstreamClient` recording the request it was handed."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        return UpstreamResponse(status=200, headers={}, body=b"")


class _UnusedTokenCache:
    """A TokenCache that must never be touched on a forward-as-is route."""

    async def get(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.get must not be called on an auth_mode=none route")

    async def set(self, endpoint: str, uid: str, bearer: str, *, source: object) -> None:
        raise AssertionError("token_cache.set must not be called on an auth_mode=none route")

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.mark_bad must not be called on an auth_mode=none route")


def _executor(client: _CapturingUpstream) -> ChainExecutor:
    """Build an executor over one forward-as-is route matching ``_HOST``."""
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="up", hosts=[_HOST], auth_mode="none")],
    )
    return ChainExecutor(
        token_cache=_UnusedTokenCache(),
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )


def _row(
    envelope_json: str,
    *,
    step_index: int = 0,
    captured: CapturedValues | None = None,
) -> UploadRow:
    """Build an ``attempting`` row around a persisted envelope blob."""
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="up",
        state="attempting",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint=_HOST,
        uid="u",
        chain_envelope_json=envelope_json,
        captured_values=captured or CapturedValues(),
        current_step_index=step_index,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


def _literal_blob(chain_id: UUID, *, steps: str, templated: bool | None = None) -> str:
    """Build a persisted-envelope blob, optionally carrying ``templated``.

    Args:
        chain_id: The chain's identity.
        steps: The ``"steps"`` array JSON fragment, without the key.
        templated: ``None`` omits the field entirely (which is how every row
            written before N3 looks); otherwise it is emitted explicitly.

    Returns:
        The envelope as a JSON string.
    """
    marker = "" if templated is None else f',"templated":{str(templated).lower()}'
    return f'{{"chain_id":"{chain_id}","idempotency_key":"k"{marker},"steps":{steps}}}'


async def test_raw_intake_key_with_braces_is_forwarded_verbatim() -> None:
    """A raw-intake object key containing braces is forwarded, not template-failed.

    Objective: the defect itself, at the executor, built through the ADAPTER
    so the test is honest on both trees. ``_synthesize_envelope`` exists
    pre-fix, so this file imports no new symbol; pre-fix the adapter does not
    set the marker, the executor returns ``TemplateUnresolved`` and the fake
    upstream records nothing, which is the behavioural failure. Success: the
    request goes out with the braces intact, and the synthesized envelope
    carries the marker (which is why no separate adapter test exists).
    """
    envelope = _synthesize_envelope(
        resolved_url=_BRACED_URL,
        method="PUT",
        headers={},
        has_body=False,
    )

    client = _CapturingUpstream()
    result = await _executor(client).execute_one_step(
        _row(envelope.model_dump_json()), body_refs={}
    )

    # The BEHAVIOURAL assertions come first, and deliberately: they read no
    # attribute the pre-fix tree lacks, so the pre-fix run fails here (the
    # executor returns TemplateUnresolved and nothing is sent) rather than on
    # an AttributeError, which would be an API-surface failure instead.
    assert isinstance(result, Succeeded), (
        f"a braced object key must be delivered, not template-failed; got {type(result).__name__}"
    )
    assert client.requests[0].url == _BRACED_URL
    # Folded test 6: the adapter itself set the marker.
    assert envelope.templated is False, "the raw-intake adapter must mark its chain literal"


async def test_literal_producer_envelope_with_braces_is_admitted() -> None:
    """Inert point 1: the parser's static pass is skipped for a literal chain.

    Objective: this is the half of the marker that does not work without the
    admission-side gate. A producer who sets ``templated: false`` and writes
    literal braces, which is the only reason to set the flag, would otherwise
    be rejected 422 at admission and never reach the executor. Success: the
    literal envelope parses, and the counter-test proves the gate is the only
    thing that changed, because the SAME envelope with ``templated`` defaulted
    still raises ``template_unresolved``.
    """
    steps = f'[{{"name":"upload","method":"PUT","url":"{_BRACED_URL}"}}]'
    literal = _literal_blob(uuid4(), steps=steps, templated=False).encode()
    default = _literal_blob(uuid4(), steps=steps).encode()

    envelope, _ = await parse_json_request(
        literal, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    assert envelope.templated is False

    with pytest.raises(ParserError) as excinfo:
        await parse_json_request(
            default, instance_id="primary", request_id="r", max_buffered_bytes=10_000
        )
    assert excinfo.value.code == "template_unresolved"


async def test_literal_chain_skips_the_capture_ttl_gate() -> None:
    """Inert point 2: the capture-TTL pre-pass returns immediately.

    Objective: the gate walks ``find_placeholders`` over the URL, headers and
    body template, and for a literal chain those spans are not capture
    references and must not be read as expired ones. This shape CANNOT be
    produced through the raw-intake adapter: a synthesized chain has no
    captured values at all, so the gate already no-ops for it, which is why
    the envelope is hand-authored here with a real prior step and an EXPIRED
    capture. Point 2 is therefore inert for coherence rather than for the N3
    defect. Success: no capture-expiry result, and the request is sent with
    the literal text.
    """
    chain_id = uuid4()
    steps = (
        '[{"name":"create_file","method":"POST","url":"https://up.example/v2/files",'
        '"capture":[{"name":"upload_url","from":"$.uploadUrl","ttl_seconds":60}]},'
        '{"name":"put_s3","method":"PUT",'
        '"url":"https://up.example/o/{{create_file.upload_url}}"}]'
    )
    expired_at = datetime.now(tz=UTC) - timedelta(seconds=30)
    captured = CapturedValues(
        steps={
            "create_file": CapturedStepValues(
                values={"upload_url": "https://elsewhere.example/put"},
                captured_at=expired_at - timedelta(seconds=60),
                expires_at={"upload_url": expired_at},
            )
        }
    )

    client = _CapturingUpstream()
    result = await _executor(client).execute_one_step(
        _row(
            _literal_blob(chain_id, steps=steps, templated=False),
            step_index=1,
            captured=captured,
        ),
        body_refs={},
    )

    assert not isinstance(result, CaptureExpiredStored | CaptureExpiredRewind), (
        f"a literal chain must not consult capture TTLs; got {type(result).__name__}"
    )
    assert isinstance(result, Succeeded)
    assert client.requests[0].url == "https://up.example/o/{{create_file.upload_url}}"


async def test_literal_chain_forwards_braces_in_headers_and_body() -> None:
    """Inert points 4 to 6: header values and both body template kinds.

    Objective: the marker is chain-level, so every substitution site must
    honour it, not just the URL. Success: a braced header value and a braced
    text body both reach the upstream verbatim.
    """
    steps = (
        '[{"name":"upload","method":"PUT","url":"https://up.example/o",'
        '"headers":{"X-Meta":"{{a.b}}"},'
        '"body":{"kind":"text","value":"payload {{c.d}} tail"}}]'
    )

    client = _CapturingUpstream()
    result = await _executor(client).execute_one_step(
        _row(_literal_blob(uuid4(), steps=steps, templated=False)), body_refs={}
    )

    assert isinstance(result, Succeeded)
    sent = client.requests[0]
    assert sent.headers["X-Meta"] == "{{a.b}}"
    assert sent.body == b"payload {{c.d}} tail"


async def test_templated_chain_is_unchanged() -> None:
    """Counter-test: a chain without the marker behaves exactly as before.

    Objective: the ruling's constraint is that envelope-path chains are
    unchanged, and an assertion is worth more than a claim. Success: the same
    step content with the default ``templated=True`` and no captures still
    returns ``TemplateUnresolved``, carrying the identifier-only payload.
    """
    steps = f'[{{"name":"upload","method":"PUT","url":"{_BRACED_URL}"}}]'

    client = _CapturingUpstream()
    result = await _executor(client).execute_one_step(
        _row(_literal_blob(uuid4(), steps=steps)), body_refs={}
    )

    assert isinstance(result, TemplateUnresolved)
    assert result.site == "url"
    assert result.step_name == "upload"
    assert result.unresolved == ("b.c",)
    assert client.requests == [], "a template failure short-circuits before the transport"


async def test_persisted_envelope_without_the_field_defaults_to_templated() -> None:
    """A row written before N3 deserializes as templated, so there is no migration.

    Objective: pin the property that is the whole reason the marker lives on
    the envelope rather than on the row. The envelope is persisted as
    ``chain_envelope_json`` and re-validated on every claim, and a field WITH
    a default is omissible. Success: a blob carrying no ``templated`` key
    parses and reports ``True``, which is the behaviour that row already had.
    """
    blob = _literal_blob(
        uuid4(), steps='[{"name":"upload","method":"PUT","url":"https://up.example/o"}]'
    )
    assert '"templated"' not in blob

    assert envelope_from_persistence_json(blob).templated is True
