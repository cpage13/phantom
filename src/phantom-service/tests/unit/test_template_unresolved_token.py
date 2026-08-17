"""Q3: the persisted ``template_unresolved:`` token, pinned for all three sites.

``TemplateUnresolved`` is rendered verbatim into the row's ``last_error``,
which ``GET /v1/admin/chains/{chain_id}`` surfaces through
``ChainAdminDetail.last_error`` and which operator logs echo. Before Q3 the
variant carried the raw template: the whole step URL for the URL site, and
``header[<name>]=<value>`` for the header site. Since F4 preserves the
raw-intake query string, a step URL can carry a presigned
``X-Amz-Signature``, so that token was a credential disclosure triggered by an
ordinary object key.

Q3 restructures the variant so it CANNOT hold template text, and moves the
formatting onto the variant itself. This file pins the operator-visible
result: the exact ``last_error`` string for each of the three sites, driven
through the real sender against a real store, plus the terminal state, which
Q3 does not change.

The same word is also an ADR-017 error CODE raised by the parser at
admission. That surface is untouched by Q3 and is not tested here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.config.settings import InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# The routed host every step below targets.
_HOST = "up.example"

# The single step's name, which every token embeds.
_STEP = "upload"

# A step URL carrying an unresolvable placeholder AND a presigned credential:
# the shape F4 makes reachable from an ordinary raw-intake object key.
_LEAKY_URL = f"https://{_HOST}/bucket/a{{{{b.c}}}}d?X-Amz-Signature=SECRET"


class _FakeUpstream:
    """Stub upstream client. No test here reaches the transport."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        raise AssertionError("a template failure must short-circuit before the transport")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a real-store instance whose single route forwards as-is.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The tracked :class:`InstanceContext`.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="up", hosts=[_HOST], auth_mode="none")],
    )
    upstream = _FakeUpstream()
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=ChainExecutor(
            token_cache=tokens,
            upstream_client=upstream,
            resolve_route=resolve_route,
            clock=lambda: datetime.now(tz=UTC),
            instance=cfg,
        ),
        saturation=SaturationGate(
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    return track_instance(instance)


def _row(chain_id: UUID, *, url: str, headers: str = "", body: str = "") -> UploadRow:
    """Build an ``attempting`` row around a hand-written one-step envelope.

    The parser's static placeholder pass rejects an unresolvable ``{{a.b}}``
    at admission, so the envelope is written straight into the persisted
    column, which the executor re-validates for SHAPE only.

    Args:
        chain_id: The chain's identity and the row's primary key.
        url: The step URL.
        headers: An optional ``,"headers":{...}`` JSON fragment.
        body: An optional ``,"body":{...}`` JSON fragment.

    Returns:
        The persisted-shape :class:`UploadRow`.
    """
    envelope_json = (
        '{"chain_id":"'
        + str(chain_id)
        + '","idempotency_key":"k","steps":[{"name":"'
        + _STEP
        + '","method":"PUT","url":"'
        + url
        + '"'
        + headers
        + body
        + "}]}"
    )
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
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


async def _drive(instance: InstanceContext, row: UploadRow) -> UploadRow:
    """Insert ``row``, drive it through the sender, and return the re-read row."""
    await instance.store.insert(row)
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)
    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    return fresh


async def test_url_site_token_names_the_placeholder_not_the_url(tmp_path: Path) -> None:
    """The URL site persists ``<step>:url:<names>`` and no fragment of the URL.

    Objective: the operator-visible token is what Q3 exists to change, so pin
    it exactly rather than asserting a property of it. Success: ``last_error``
    equals the documented string and carries none of the URL, the query or the
    signature.
    """
    instance = await _build_instance(tmp_path)
    fresh = await _drive(instance, _row(uuid4(), url=_LEAKY_URL))

    assert fresh.last_error == f"template_unresolved:{_STEP}:url:b.c"
    assert fresh.state == "failed", "a template failure is terminal, which Q3 does not change"
    for leaked in ("SECRET", "X-Amz", "?", "https://", "bucket"):
        assert leaked not in (fresh.last_error or ""), (
            f"last_error must not carry {leaked!r}; got {fresh.last_error!r}"
        )


async def test_header_site_token_names_the_header_not_its_value(tmp_path: Path) -> None:
    """The header site persists ``<step>:header[<name>]:<names>``, never the value.

    Objective: the header NAME is bounded by admission's RFC 7230 token check,
    so it is safe to name; the value is producer text that can carry a literal
    credential beside the placeholder. Success: the documented token, with no
    fragment of the value.
    """
    instance = await _build_instance(tmp_path)
    fresh = await _drive(
        instance,
        _row(
            uuid4(),
            url=f"https://{_HOST}/o",
            headers=',"headers":{"Authorization":"Basic aGFyZGNvZGVk{{login.suffix}}"}',
        ),
    )

    assert fresh.last_error == f"template_unresolved:{_STEP}:header[Authorization]:login.suffix"
    assert fresh.state == "failed"
    assert "aGFyZGNvZGVk" not in (fresh.last_error or "")
    assert "Basic" not in (fresh.last_error or "")


async def test_body_site_token_names_the_placeholder(tmp_path: Path) -> None:
    """The body site persists ``<step>:body:<names>`` and still terminates failed.

    Objective: the body arm was already safe (it carried only the step name),
    so this is the counter-test that the restructure changed what it SAYS and
    not when it fires. Success: the documented token and the terminal
    ``failed`` state.
    """
    instance = await _build_instance(tmp_path)
    fresh = await _drive(
        instance,
        _row(
            uuid4(),
            url=f"https://{_HOST}/o",
            body=',"body":{"kind":"text","value":"hello {{c.d}}"}',
        ),
    )

    assert fresh.last_error == f"template_unresolved:{_STEP}:body:c.d"
    assert fresh.state == "failed"
