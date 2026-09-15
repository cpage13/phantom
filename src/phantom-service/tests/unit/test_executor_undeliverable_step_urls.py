"""Step URLs and captures the executor must refuse instead of dying on.

Three defects that all end the same way: an exception or an unsendable request
escapes ``execute_one_step`` into a call stack that catches neither, so the
sender's ``asyncio.TaskGroup`` is cancelled, delivery stops service-wide, and
startup recovery re-claims the same row on the next boot and dies again.

* **An unparseable URL raised out of the guard meant to classify it.** The
  ``except ValueError`` arm around route resolution called ``sanitised_host_for``
  on the same URL whose parse had just raised, and that helper used
  ``urlparse``, whose ``.hostname`` raises ``ValueError('Invalid IPv6 URL')``.
  Admission route-checks only the FIRST step, so a later step's URL is never
  validated: ``https://{{s1.host}}/obj`` with an ``s1.host`` capture of ``[bad``
  renders ``https://[bad/obj``.
* **A relative URL whose QUERY held a URL value was read as absolute.** The test
  was ``"://" in url``, a substring search, so ``default_target`` was skipped and
  a pathless URL reached the route gate, matched a ``hosts: ['*']`` catch-all on
  its own path-plus-query, collected injected auth and was handed to httpx,
  which cannot dial it. That raises ``UnsupportedProtocol``, an
  ``httpx.HTTPError``, so it classified ``FailedNetwork`` and the row burned its
  whole retry budget on a request that could never be sent.
* **A lone UTF-16 surrogate crash-looped the process permanently.**
  ``json.loads`` accepts one, the F8 type gate passed it (it is a ``str`` and
  ``_CONTROL_CHARS`` matches only CR, LF and NUL), ``json.dumps`` and SQLite
  round-trip it, and then ``httpx`` raises ``UnicodeEncodeError`` - a
  ``ValueError`` that is not in the httpx exception hierarchy at all, so nothing
  between the transport and the TaskGroup catches it.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from phantom.chain.executor import (
    CaptureNotRenderable,
    ChainExecutor,
    ExecuteStepResult,
    RouteUnresolved,
    Succeeded,
)
from phantom.chain.parser import envelope_from_persistence_json
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.storage import SqliteTokenCache
from phantom.transport import UpstreamRequest, UpstreamResponse

_HOST = "files.example.com"
_UID = "user-1"

# A lone high surrogate. Valid to ``json.loads``, invalid to every UTF-8
# encoder, and therefore un-sendable in a URL or a text body.
_LONE_SURROGATE = "\ud800"


class FakeUpstreamClient:
    """Stub upstream that BUILDS the real httpx request before answering.

    Faithful where it has to be. ``HttpxUpstreamClient.send`` hands the URL,
    headers and body straight to ``httpx.AsyncClient.request``, and the whole
    surrogate defect lives in that construction: ``httpx.Request`` raises
    ``UnicodeEncodeError``, which is a ``ValueError`` and is not an
    ``httpx.HTTPError``, so it is caught by nothing between the transport and
    the supervising TaskGroup. A fake that only records its argument would make
    the un-sendable request look sendable and the test would prove nothing.
    """

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []
        self._responses: list[UpstreamResponse] = []

    def push(self, status: int, body: bytes = b"") -> None:
        """Queue one canned response."""
        self._responses.append(UpstreamResponse(status=status, headers={}, body=body))

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        httpx.Request(req.method, req.url, headers=req.headers, content=req.body or None)
        self.requests.append(req)
        if not self._responses:
            return UpstreamResponse(status=200, headers={}, body=b"{}")
        return self._responses.pop(0)


@pytest.fixture
async def token_cache(tmp_path: Path) -> AsyncIterator[SqliteTokenCache]:
    """A started token cache on a tmp SQLite file."""
    cache = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await cache.start()
    yield cache
    await cache.stop()


def _catch_all_instance() -> InstanceCfg:
    """An instance whose ONLY route is the documented ``hosts: ['*']`` catch-all.

    The catch-all is what makes these tests meaningful: it is the shape that
    used to match anything ``host_key_for`` returned, including a bare path and
    its query string.
    """
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="catch-all", hosts=["*"], auth_mode="none")],
    )


def _envelope_json(
    *,
    steps: list[dict[str, object]],
    default_target: str | None = None,
) -> str:
    """Serialize a producer-authored envelope with the given steps."""
    payload: dict[str, object] = {
        "chain_id": str(uuid4()),
        "idempotency_key": "idem-key-1",
        "steps": steps,
    }
    if default_target is not None:
        payload["default_target"] = default_target
    return json.dumps(payload)


def _row(
    envelope_json: str,
    *,
    step_index: int = 0,
    captured: CapturedValues | None = None,
) -> UploadRow:
    """Wrap a persisted envelope in the ``attempting`` row the executor takes."""
    envelope = envelope_from_persistence_json(envelope_json)
    return UploadRow(
        chain_id=envelope.chain_id,
        instance_id="primary",
        group_id=envelope.chain_id,
        multifile_id=envelope.chain_id,
        send_order=0,
        route_name="catch-all",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint=_HOST,
        uid=_UID,
        chain_envelope_json=envelope_json,
        captured_values=captured or CapturedValues(),
        current_step_index=step_index,
        idempotency_key=envelope.idempotency_key,
        capture_reexecution_active=False,
    )


async def _run(
    token_cache: SqliteTokenCache,
    client: FakeUpstreamClient,
    row: UploadRow,
) -> ExecuteStepResult:
    """Execute one step against a catch-all instance and return the classification."""
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_catch_all_instance(),
    )
    return await executor.execute_one_step(row, body_refs={})


# ---------------------------------------------------------------------------
# 1. An unparseable later-step URL.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unparseable_step_url_is_classified_and_never_escapes(
    token_cache: SqliteTokenCache,
) -> None:
    """``https://[bad/obj`` must park the row, not cancel every sender worker.

    Objective: admission route-checks only the FIRST step, so a step 2 URL
    rendered from a capture is never validated before send time. ``urlparse``
    raises ``ValueError('Invalid IPv6 URL')`` on this shape, ``_resolve_route``
    raised, the executor's handler caught it and then RAISED AGAIN from the same
    parse inside itself. Nothing above catches that: ``_drive_one`` catches four
    storage and index types and ``_worker_loop`` catches
    ``sqlite3.OperationalError``, so it reached the supervising TaskGroup, every
    sender worker was cancelled, delivery stopped service-wide, and the row was
    left ``attempting`` still holding its saturation slot.

    Success: a ``RouteUnresolved`` classification carrying the fixed ``<no-host>``
    token, no exception, and nothing sent.
    """
    envelope_json = _envelope_json(
        steps=[
            {"name": "one", "method": "GET", "url": f"https://{_HOST}/first"},
            {"name": "two", "method": "PUT", "url": "https://{{one.host}}/obj"},
        ]
    )
    captured = CapturedValues(
        steps={
            "one": CapturedStepValues(
                values={"host": "[bad"},
                captured_at=datetime.now(tz=UTC),
                expires_at={"host": None},
            )
        }
    )
    client = FakeUpstreamClient()

    result = await _run(token_cache, client, _row(envelope_json, step_index=1, captured=captured))

    assert isinstance(result, RouteUnresolved), (
        f"an unparseable URL must produce a clean classification; got {result!r}"
    )
    assert result.host == "<no-host>", (
        f"the classification rides into last_error, which the admin API surfaces; "
        f"got {result.host!r}"
    )
    assert client.requests == [], "an unsendable request must not reach the transport"


# ---------------------------------------------------------------------------
# 2. A relative URL whose query carries a URL value.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_relative_url_carrying_a_url_in_its_query_is_still_joined(
    token_cache: SqliteTokenCache,
) -> None:
    """``default_target`` must apply to ``/upload?callback=https://cb.example/done``.

    Objective: the absolute test was ``"://" in url``, an unanchored substring
    search, so a RELATIVE step whose QUERY carried a URL value answered True and
    resolution was skipped entirely.

    Success: the forwarded URL is the default target joined with the path, the
    callback parameter survives byte-for-byte, and the request is actually sent.
    """
    envelope_json = _envelope_json(
        steps=[
            {
                "name": "upload",
                "method": "PUT",
                "url": "/upload?callback=https://cb.example/done",
            }
        ],
        default_target=f"https://{_HOST}",
    )
    client = FakeUpstreamClient()
    client.push(200, b"{}")

    result = await _run(token_cache, client, _row(envelope_json))

    assert isinstance(result, Succeeded), result
    assert client.requests[0].url == (f"https://{_HOST}/upload?callback=https://cb.example/done"), (
        f"default_target must be applied to a relative URL; got {client.requests[0].url!r}"
    )


@pytest.mark.asyncio
async def test_a_relative_url_with_no_default_target_is_parked_not_retried(
    token_cache: SqliteTokenCache,
) -> None:
    """With no ``default_target`` the same URL is refused at the route gate.

    Objective: a URL with no dialable host can never be sent, so it must not be
    handed to the transport at all. It used to be keyed by ``host_key_for``'s
    whole-input fallback as its own path-plus-query, fnmatched by the documented
    ``hosts: ['*']`` catch-all, given injected auth, and then rejected by httpx
    with ``UnsupportedProtocol`` - an ``httpx.HTTPError``, so the executor
    classified it ``FailedNetwork`` and the row burned its ENTIRE retry budget.

    Success: ``RouteUnresolved`` (a park an operator can act on) rather than a
    retryable network failure, and nothing sent.
    """
    envelope_json = _envelope_json(
        steps=[
            {
                "name": "upload",
                "method": "PUT",
                "url": "/upload?callback=https://cb.example/done",
            }
        ]
    )
    client = FakeUpstreamClient()

    result = await _run(token_cache, client, _row(envelope_json))

    assert isinstance(result, RouteUnresolved), (
        f"a URL with no dialable host must match no route, catch-all included; got {result!r}"
    )
    assert result.host == "<no-host>"
    assert client.requests == []


# ---------------------------------------------------------------------------
# 3. An unencodable capture.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unencodable_capture_is_refused_before_it_is_persisted(
    token_cache: SqliteTokenCache,
) -> None:
    """A lone surrogate in an upstream response must never reach the row.

    Objective: the crash-loop is powered by PERSISTENCE. The value survives
    ``json.loads``, the F8 type gate, ``json.dumps`` and SQLite, so the row is
    written, the next attempt splices it into a URL, ``httpx`` raises
    ``UnicodeEncodeError`` (a ``ValueError``, and not an ``httpx.HTTPError``),
    the process dies, startup recovery resets the row to ``queued``, and the next
    claim dies again - stranding the whole backlog. Rejecting at CAPTURE time is
    what stops the value entering the database at all.

    Success: a terminal ``CaptureNotRenderable`` naming the capture and the
    ``unencodable`` reason, carrying identifiers only, and NO captured value
    merged onto the returned result.
    """
    envelope_json = _envelope_json(
        steps=[
            {
                "name": "create",
                "method": "POST",
                "url": f"https://{_HOST}/v1/files",
                "capture": [{"name": "token", "from_path": "upload.token"}],
            },
            {"name": "put", "method": "PUT", "url": "https://" + _HOST + "/{{create.token}}"},
        ]
    )
    client = FakeUpstreamClient()
    client.push(200, json.dumps({"upload": {"token": _LONE_SURROGATE}}).encode())

    result = await _run(token_cache, client, _row(envelope_json))

    assert isinstance(result, CaptureNotRenderable), (
        f"an unencodable capture must terminate the row; got {result!r}"
    )
    assert result.reason == "unencodable"
    assert result.placeholder == "create.token"
    token = result.token()
    assert _LONE_SURROGATE not in token, (
        f"last_error carries identifiers only; the captured value leaked into {token!r}"
    )


@pytest.mark.asyncio
async def test_an_already_persisted_surrogate_capture_terminates_instead_of_crashing(
    token_cache: SqliteTokenCache,
) -> None:
    """A row admitted before the capture-time guard must still recover.

    Objective: rejecting at capture time protects new rows, and does nothing for
    a row already carrying the poison - which is exactly the row recovery resets
    to ``queued`` and re-claims on every restart. The render-time gate is what
    turns that permanent crash-loop into one terminal row.

    Success: the URL site refuses with ``unencodable`` rather than raising, and
    nothing is handed to the transport.
    """
    envelope_json = _envelope_json(
        steps=[
            {"name": "create", "method": "POST", "url": f"https://{_HOST}/v1/files"},
            {"name": "put", "method": "PUT", "url": "https://" + _HOST + "/{{create.token}}"},
        ]
    )
    captured = CapturedValues(
        steps={
            "create": CapturedStepValues(
                values={"token": _LONE_SURROGATE},
                captured_at=datetime.now(tz=UTC),
                expires_at={"token": None},
            )
        }
    )
    client = FakeUpstreamClient()

    result = await _run(token_cache, client, _row(envelope_json, step_index=1, captured=captured))

    assert isinstance(result, CaptureNotRenderable), (
        f"a persisted unencodable capture must terminate the row; got {result!r}"
    )
    assert result.reason == "unencodable"
    assert result.site == "url"
    assert client.requests == []


@pytest.mark.asyncio
async def test_a_non_ascii_capture_is_refused_only_where_it_cannot_be_sent(
    token_cache: SqliteTokenCache,
) -> None:
    """``café`` is legal in a URL and illegal in a header, and the gate is per-site.

    Objective: keep the refusal precise rather than blanket. Verified against
    the pinned httpx: a header value is encoded ``ascii`` and a URL is encoded
    ``utf-8``, so a non-ASCII capture crash-looped the process through a HEADER
    (same ``UnicodeEncodeError`` escape as the surrogate) while being perfectly
    sendable in a path. Refusing it everywhere would terminate working chains.

    Success: the URL step sends, the header step refuses.
    """
    captured = CapturedValues(
        steps={
            "create": CapturedStepValues(
                values={"name": "café"},
                captured_at=datetime.now(tz=UTC),
                expires_at={"name": None},
            )
        }
    )
    url_envelope = _envelope_json(
        steps=[
            {"name": "create", "method": "POST", "url": f"https://{_HOST}/v1/files"},
            {"name": "put", "method": "PUT", "url": "https://" + _HOST + "/{{create.name}}"},
        ]
    )
    client = FakeUpstreamClient()
    client.push(200, b"{}")

    url_result = await _run(
        token_cache, client, _row(url_envelope, step_index=1, captured=captured)
    )
    assert isinstance(url_result, Succeeded), (
        f"a non-ASCII capture is sendable in a URL and must not be refused; got {url_result!r}"
    )

    header_envelope = _envelope_json(
        steps=[
            {"name": "create", "method": "POST", "url": f"https://{_HOST}/v1/files"},
            {
                "name": "put",
                "method": "PUT",
                "url": f"https://{_HOST}/obj",
                "headers": {"X-Trace": "{{create.name}}"},
            },
        ]
    )
    header_client = FakeUpstreamClient()

    header_result = await _run(
        token_cache, header_client, _row(header_envelope, step_index=1, captured=captured)
    )
    assert isinstance(header_result, CaptureNotRenderable), (
        f"a non-ASCII header value cannot be encoded by httpx; got {header_result!r}"
    )
    assert header_result.reason == "unencodable"
    assert header_result.header_name == "X-Trace"
    assert header_client.requests == []
