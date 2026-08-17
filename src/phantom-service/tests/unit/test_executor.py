"""Unit tests for phantom.chain.executor."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from phantom.chain.executor import (
    CaptureExpiredRewind,
    CaptureExpiredStored,
    ChainExecutor,
    Failed4xx,
    Failed5xx,
    FailedAuth,
    Succeeded,
    TemplateUnresolved,
)
from phantom.chain.parser import parse_json_request
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.upload import (
    CapturedStepValues,
    CapturedValues,
    UploadRow,
)
from phantom.routing import resolve_route
from phantom.transport import UpstreamRequest, UpstreamResponse

# -------- fakes --------------------------------------------------------------


class FakeUpstreamClient:
    """Stub :class:`UpstreamClient` that records sent requests and returns canned responses."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []
        self._responses: list[UpstreamResponse] = []

    def push(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None) -> None:
        """Queue one canned response."""
        self._responses.append(UpstreamResponse(status=status, headers=headers or {}, body=body))

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        if not self._responses:
            raise AssertionError("No response queued for upstream call")
        return self._responses.pop(0)


class FakeTokenCache:
    """In-memory :class:`TokenCache` for executor tests."""

    def __init__(self) -> None:
        from phantom.models.token import TokenCacheRow

        self.rows: dict[tuple[str, str], TokenCacheRow] = {}
        self.marked_bad: list[tuple[str, str]] = []

    async def get(self, endpoint: str, uid: str):
        return self.rows.get((endpoint, uid))

    async def set(self, endpoint: str, uid: str, bearer: str, *, source):
        from phantom.models.token import TokenCacheRow

        row = TokenCacheRow(
            endpoint=endpoint,
            uid=uid,
            bearer=bearer,
            observed_at=datetime.now(tz=UTC),
            source=source,
            status="fresh",
        )
        self.rows[(endpoint, uid)] = row
        return row

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        self.marked_bad.append((endpoint, uid))
        existing = self.rows.get((endpoint, uid))
        if existing:
            self.rows[(endpoint, uid)] = existing.model_copy(update={"status": "bad"})


# -------- helpers ------------------------------------------------------------


def _instance(routes: list[RouteCfg]) -> InstanceCfg:
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=routes,
    )


def _clock_at(t: datetime) -> Callable[[], datetime]:
    return lambda: t


async def _envelope_json(chain_id, idempotency_key="k") -> bytes:
    body = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"'
        + idempotency_key.encode()
        + b'","steps":['
        + b'{"name":"create_file","method":"POST","url":"https://files.example.com/v2/files",'
        + b'"body":{"kind":"json","value":{"name":"f"}},'
        + b'"capture":[{"name":"upload_url","from":"$.uploadUrl","ttl_seconds":3600}],'
        + b'"idempotency_header":"Idempotency-Key"},'
        + b'{"name":"put_s3","method":"PUT","url":"{{create_file.upload_url}}",'
        + b'"body":{"kind":"body_ref","name":"body"}}'
        + b"]}"
    )
    return body


async def _row(
    chain_id,
    *,
    captured: CapturedValues | None = None,
    step_index: int = 0,
    capture_reexecution: bool = False,
) -> UploadRow:
    body = await _envelope_json(chain_id)
    envelope, _ = await parse_json_request(
        body, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="upstream-files",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint="files.example.com",
        uid="user-1",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=captured or CapturedValues(),
        current_step_index=step_index,
        idempotency_key="k",
        capture_reexecution_active=capture_reexecution,
    )


# -------- tests --------------------------------------------------------------


@pytest.mark.asyncio
async def test_linear_two_step_success() -> None:
    """Two-step happy-path chain — step 0 succeeds with capture; step 1 succeeds."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/upload"}')
    instance = _instance(
        [
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="s3", hosts=["s3"], auth_mode="none"),
        ]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)

    result = await executor.execute_one_step(row, body_refs={"body": b"hi"})
    assert isinstance(result, Succeeded)
    assert result.next_step_index == 1
    assert result.chain_done is False
    assert "create_file" in result.captured.steps
    assert result.captured.steps["create_file"].values["upload_url"] == "https://s3/upload"

    # Now step 1 with the captured value.
    client.push(200, body=b"")
    row2 = await _row(chain_id, captured=result.captured, step_index=1)
    second = await executor.execute_one_step(row2, body_refs={"body": b"payload"})
    assert isinstance(second, Succeeded)
    assert second.chain_done is True
    # The PUT was directed at the captured upload_url.
    assert client.requests[1].url == "https://s3/upload"
    assert client.requests[1].body == b"payload"


@pytest.mark.asyncio
async def test_substitution_url_header_body() -> None:
    """Placeholders in URL, header, and body all resolve."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    client.push(200, body=b"{}")

    body_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"first","method":"POST","url":"https://x/y",'
        + b'"capture":[{"name":"v","from":"$.value"}]},'
        + b'{"name":"second","method":"POST","url":"https://x/{{first.v}}",'
        + b'"headers":{"X-Cap":"{{first.v}}"},'
        + b'"body":{"kind":"text","value":"value-is-{{first.v}}"}}'
        + b"]}"
    )
    envelope, _ = await parse_json_request(
        body_json, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    captured = CapturedValues(
        steps={
            "first": CapturedStepValues(
                values={"v": "hello"},
                captured_at=datetime.now(tz=UTC),
                expires_at={"v": None},
            )
        }
    )
    row = UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        route_name="r",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint="x",
        uid="u",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=captured,
        current_step_index=1,
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    instance = _instance([RouteCfg(name="r", hosts=["x"], auth_mode="none")])
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={})
    assert isinstance(result, Succeeded)
    assert client.requests[0].url == "https://x/hello"
    assert client.requests[0].headers["X-Cap"] == "hello"
    assert client.requests[0].body == b"value-is-hello"


@pytest.mark.asyncio
async def test_idempotency_header_carries_chain_key() -> None:
    """Step's ``idempotency_header`` is filled with the envelope's idempotency_key."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/upload"}')

    instance = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert client.requests[0].headers["Idempotency-Key"] == "k"


@pytest.mark.asyncio
async def test_capture_expiry_stored_default() -> None:
    """With capture_reexecution=False, expired capture → CaptureExpiredStored."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    instance = _instance(
        [
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="s3", hosts=["s3"], auth_mode="none"),
        ]
    )
    now = datetime.now(tz=UTC)
    captured = CapturedValues(
        steps={
            "create_file": CapturedStepValues(
                values={"upload_url": "https://s3/upload"},
                captured_at=now - timedelta(hours=2),
                expires_at={"upload_url": now - timedelta(hours=1)},  # expired
            )
        }
    )
    row = await _row(chain_id, captured=captured, step_index=1, capture_reexecution=False)
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: now,
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, CaptureExpiredStored)


@pytest.mark.asyncio
async def test_capture_expiry_reexecute_true() -> None:
    """With capture_reexecution=True, expired capture → CaptureExpiredRewind."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    instance = _instance(
        [
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="s3", hosts=["s3"], auth_mode="none"),
        ]
    )
    now = datetime.now(tz=UTC)
    captured = CapturedValues(
        steps={
            "create_file": CapturedStepValues(
                values={"upload_url": "https://s3/upload"},
                captured_at=now - timedelta(hours=2),
                expires_at={"upload_url": now - timedelta(hours=1)},
            )
        }
    )
    row = await _row(chain_id, captured=captured, step_index=1, capture_reexecution=True)
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: now,
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, CaptureExpiredRewind)
    assert result.rewind_to_step_index == 0


@pytest.mark.asyncio
async def test_401_phantom_bearer_marks_bad() -> None:
    """A 401 on a phantom_bearer route marks the slot bad and returns FailedAuth."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(401)
    instance = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, FailedAuth)
    assert ("files.example.com", "user-1") in cache.marked_bad


@pytest.mark.asyncio
async def test_template_unresolved_short_circuit() -> None:
    """Unresolved placeholder short-circuits without a network call."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    instance = _instance(
        [
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="s3", hosts=["s3"], auth_mode="none"),
        ]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    # Step 1 references {{create_file.upload_url}}, which we never captured.
    row = await _row(chain_id, step_index=1, captured=CapturedValues())
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, TemplateUnresolved)
    assert client.requests == []


@pytest.mark.asyncio
async def test_none_auth_mode_skips_token_lookup() -> None:
    """auth_mode='none' does not inject Authorization."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    # Intentionally no token written.
    client = FakeUpstreamClient()
    client.push(200, body=b"")

    body_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"put_s3","method":"PUT","url":"https://s3.example.com/upload",'
        + b'"body":{"kind":"text","value":"data"}}'
        + b"]}"
    )
    envelope, _ = await parse_json_request(
        body_json, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    row = UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        route_name="s3",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint="s3.example.com",
        uid="user-1",
        chain_envelope_json=envelope.model_dump_json(),
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    instance = _instance([RouteCfg(name="s3", hosts=["*"], auth_mode="none")])
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={})
    assert isinstance(result, Succeeded)
    assert "Authorization" not in client.requests[0].headers


@pytest.mark.asyncio
async def test_5xx_classified() -> None:
    """5xx response returns Failed5xx."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(503)
    instance = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, Failed5xx)


@pytest.mark.asyncio
async def test_4xx_non_auth_classified() -> None:
    """4xx (non-auth) response returns Failed4xx."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(400, body=b"bad request")
    instance = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    result = await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert isinstance(result, Failed4xx)
    assert result.body == b"bad request"


@pytest.mark.asyncio
async def test_per_route_timeout_threaded_onto_upstream_request() -> None:
    """A route with ``timeout_seconds=600`` produces ``UpstreamRequest.timeout_seconds=600`` (§5.2).

    Verifies the wiring: RouteCfg → ResolvedRoute → executor →
    UpstreamRequest. The httpx_client test covers the next hop into the
    actual httpx kwarg.
    """
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/u"}')
    instance = _instance(
        [
            RouteCfg(
                name="files",
                hosts=["files.example.com"],
                auth_mode="phantom_bearer",
                timeout_seconds=600.0,
            ),
        ]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    await executor.execute_one_step(row, body_refs={"body": b"x"})
    # The captured outbound request carried the per-route timeout.
    assert len(client.requests) == 1
    assert client.requests[0].timeout_seconds == 600.0


@pytest.mark.asyncio
async def test_route_without_timeout_emits_none_on_request() -> None:
    """Routes without ``timeout_seconds`` emit ``None`` so the transport falls back."""
    chain_id = uuid4()
    cache = FakeTokenCache()
    await cache.set("files.example.com", "user-1", "Bearer abc", source="inbound_request")
    client = FakeUpstreamClient()
    client.push(200, body=b'{"uploadUrl":"https://s3/u"}')
    instance = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    row = await _row(chain_id)
    await executor.execute_one_step(row, body_refs={"body": b"x"})
    assert client.requests[0].timeout_seconds is None


@pytest.mark.asyncio
async def test_x_phantom_headers_stripped_from_upstream() -> None:
    """``X-Phantom-*`` headers on chain steps never reach upstream.

    Phantom's reserved header namespace is internal to the producer-Phantom
    boundary. A misconfigured producer that puts ``X-Phantom-*`` into a
    step's headers must not see those headers leak through — the
    transparent-proxy invariant requires the upstream sees only the
    headers the producer would have sent if Phantom didn't exist.
    """
    chain_id = uuid4()
    cache = FakeTokenCache()
    client = FakeUpstreamClient()
    client.push(200, body=b"{}")

    body_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"only","method":"POST","url":"https://x/y",'
        + b'"headers":{'
        + b'"User-Agent":"producer/1.0",'
        + b'"X-Phantom-Idempotency-Key":"leaked-1",'
        + b'"X-Phantom-Uid":"leaked-2",'
        + b'"x-phantom-target":"leaked-3-lowercase",'
        + b'"X-Custom-Trace":"keep-me"'
        + b"}}"
        + b"]}"
    )
    envelope, _ = await parse_json_request(
        body_json, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    row = UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        route_name="r",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint="x",
        uid="u",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    instance = _instance([RouteCfg(name="r", hosts=["x"], auth_mode="none")])
    executor = ChainExecutor(
        token_cache=cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=instance,
    )
    result = await executor.execute_one_step(row, body_refs={})
    assert isinstance(result, Succeeded)
    sent = client.requests[0].headers
    # Non-reserved headers survive.
    assert sent.get("User-Agent") == "producer/1.0"
    assert sent.get("X-Custom-Trace") == "keep-me"
    # Every X-Phantom-* header is stripped, case-insensitively.
    leaked = [k for k in sent if k.lower().startswith("x-phantom-")]
    assert not leaked, f"X-Phantom-* leaked: {leaked}"


# -------- Q3: TemplateUnresolved carries identifiers, never template text ----

# A step URL that carries BOTH an unresolvable placeholder and a presigned
# credential. Since F4 preserves the raw-intake query string, this is the
# shape an ordinary object key produces, and the whole URL used to be
# persisted verbatim into ``last_error``.
_LEAKY_URL = "https://up.example/bucket/a{{b.c}}d?X-Amz-Signature=SECRET"


def _envelope_blob(*, step_name: str, url: str, headers: str = "", body: str = "") -> str:
    """Build a one-step envelope JSON blob for the persisted-envelope column.

    The parser's static placeholder pass rejects an unresolvable ``{{a.b}}``
    at admission, so these envelopes are written straight into the row's
    ``chain_envelope_json`` (which the executor re-validates for SHAPE only)
    rather than through ``parse_json_request``.

    Args:
        step_name: The single step's name.
        url: The step URL, which may carry a placeholder.
        headers: An optional ``,"headers":{...}`` JSON fragment.
        body: An optional ``,"body":{...}`` JSON fragment.

    Returns:
        The envelope as a JSON string.
    """
    return (
        '{"chain_id":"'
        + str(uuid4())
        + '","idempotency_key":"k","steps":[{"name":"'
        + step_name
        + '","method":"PUT","url":"'
        + url
        + '"'
        + headers
        + body
        + "}]}"
    )


def _row_for(envelope_json: str) -> UploadRow:
    """Build an ``attempting`` row around a hand-written envelope blob."""
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="r",
        state="attempting",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint="up.example",
        uid="u",
        chain_envelope_json=envelope_json,
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


def _q3_executor() -> ChainExecutor:
    """An executor over one forward-as-is route matching ``up.example``."""
    return ChainExecutor(
        token_cache=FakeTokenCache(),
        upstream_client=FakeUpstreamClient(),
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance([RouteCfg(name="up", hosts=["up.example"], auth_mode="none")]),
    )


@pytest.mark.asyncio
async def test_unresolved_url_template_reports_names_not_the_url() -> None:
    """The F4-created leak is closed at the source: no URL reaches the variant.

    Objective: ``TemplateUnresolved`` used to carry ``step.url`` verbatim, and
    the sender writes the variant straight into ``last_error``, which
    ``GET /v1/admin/chains/{chain_id}`` surfaces and the logs echo. Since F4
    preserves the query string, that URL can carry a full presigned
    credential. Success: the result names the step, the site and the
    placeholder NAMES, and no field carries the URL or the secret.
    """
    row = _row_for(_envelope_blob(step_name="upload", url=_LEAKY_URL))

    result = await _q3_executor().execute_one_step(row, body_refs={})

    # FIRST assertion, and deliberately field-name independent: it must be
    # reachable on the pre-fix tree, where the variant has no ``site`` or
    # ``unresolved`` attribute to read.
    assert "SECRET" not in repr(result)
    assert isinstance(result, TemplateUnresolved)
    assert result.site == "url"
    assert result.step_name == "upload"
    assert result.unresolved == ("b.c",)
    for leaked in ("SECRET", "?", "X-Amz"):
        assert leaked not in repr(result), f"the variant must not carry {leaked!r}"


@pytest.mark.asyncio
async def test_unresolved_header_template_reports_the_name_not_the_value() -> None:
    """The header arm reports the header NAME and never its value.

    Objective: a producer-authored header value can carry credential material
    beside a placeholder (``Authorization: Basic <literal>{{login.suffix}}``),
    and the old variant embedded ``header[<name>]=<value>``. The header NAME is
    safe (admission rejects any name that is not an RFC 7230 token), the value
    is not. Success: the typed shape names the header, the rendered token
    matches the documented form, and no field carries the value.
    """
    row = _row_for(
        _envelope_blob(
            step_name="upload",
            url="https://up.example/o",
            headers=',"headers":{"Authorization":"Bearer {{login.token}}"}',
        )
    )

    result = await _q3_executor().execute_one_step(row, body_refs={})

    assert isinstance(result, TemplateUnresolved)
    assert result.site == "header"
    assert result.header_name == "Authorization"
    assert result.unresolved == ("login.token",)
    assert result.token() == "upload:header[Authorization]:login.token"
    assert "Bearer" not in repr(result)
    assert "Bearer" not in result.token()


@pytest.mark.asyncio
async def test_body_template_failure_still_classifies() -> None:
    """Counter-test: the already-safe body arm keeps working and now reports names.

    Objective: the body site was never a leak (it carried only the step name),
    so the restructure must not change WHEN it fires, only what it says. The
    other half of this test's claim, that the row still terminates ``failed``,
    is asserted against a real store in
    ``test_template_unresolved_token.py``, which drives the sender.
    """
    row = _row_for(
        _envelope_blob(
            step_name="upload",
            url="https://up.example/o",
            body=',"body":{"kind":"text","value":"hello {{c.d}}"}',
        )
    )

    result = await _q3_executor().execute_one_step(row, body_refs={})

    assert isinstance(result, TemplateUnresolved)
    assert result.site == "body"
    assert result.step_name == "upload"
    assert result.unresolved == ("c.d",)
