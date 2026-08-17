"""Single-writer stored transition (cycle-7 task 2.6, finding D7).

Three sender paths write the ``stored`` state: ``_on_stored`` (capture
expired), the budget-exhausted leg of ``_on_retryable_failure``, and
``_on_route_unresolved`` (no configured route matches a step's host, F1).
They converge on ONE private helper so the ``new_state="stored"``
literal has exactly one call site (one-writer-per-effect). These tests
pin that the paths still reach ``stored`` with their historical row
effects, that the rowcount=0 no-op path never clobbers a row taken by
admin cancel/replay, and that the literal stays single-sited. F1's own
path is covered by ``test_route_unresolved_parks.py``.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from phantom.chain.executor import Failed5xx
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.strategies.interface import UploadStrategy
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance


async def _build_instance(
    tmp_path: Path, *, retry_strategy: UploadStrategy | None = None
) -> InstanceContext:
    """Minimal real-store instance for driving the stored transitions.

    The store is real (the transitions under test are row writes); the
    executor, upstream client, and token cache are never touched by the
    handlers under test and stay as inert mocks.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=(
            retry_strategy if retry_strategy is not None else FixedIntervalsStrategy([1, 5])
        ),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


@pytest.mark.asyncio
async def test_capture_expired_path_reaches_stored(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``_on_stored`` (capture expired) lands the row in ``stored``.

    Row effects pinned: attempts NOT incremented on this path, the
    capture-expired last_error verbatim, no upstream status, no next
    attempt, sent_at untouched.
    """
    instance = await _build_instance(tmp_path)
    row = make_upload_row(state="attempting", route_name="files", attempts=2)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_stored(instance.store, row, last_error="capture_expired:auth-step")

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "stored"
    assert fresh.attempts == 2
    assert fresh.last_error == "capture_expired:auth-step"
    assert fresh.next_attempt_at is None
    assert fresh.upstream_status_code is None
    assert fresh.sent_at is None


@pytest.mark.asyncio
async def test_budget_exhausted_path_reaches_stored(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The exhausted retry budget leg lands the row in ``stored``.

    Row effects pinned: attempts incremented by one on this path, the
    typed 5xx last_error, the upstream status code recorded, no next
    attempt, sent_at untouched.
    """
    instance = await _build_instance(tmp_path, retry_strategy=FixedIntervalsStrategy([]))
    row = make_upload_row(state="attempting", route_name="files", attempts=0)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=503))

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "stored"
    assert fresh.attempts == 1
    assert fresh.last_error == "5xx_status_503"
    assert fresh.next_attempt_at is None
    assert fresh.upstream_status_code == 503
    assert fresh.sent_at is None


@pytest.mark.asyncio
async def test_stored_no_op_when_row_not_attempting(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Both stored paths respect the M-W4-F7 expected-state guard.

    A row that admin cancel took out of ``attempting`` between the
    sender's claim and the stored transition is NOT clobbered; the
    handlers observe rowcount=0 and leave the row alone.
    """
    instance = await _build_instance(tmp_path, retry_strategy=FixedIntervalsStrategy([]))
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)

    cancelled_one = make_upload_row(state="cancelled", route_name="files", attempts=1)
    await instance.store.insert(cancelled_one)
    await sender._on_stored(instance.store, cancelled_one, last_error="capture_expired:auth-step")
    fresh_one = await instance.store.get(cancelled_one.chain_id)
    assert fresh_one is not None
    assert fresh_one.state == "cancelled"
    assert fresh_one.last_error is None

    cancelled_two = make_upload_row(state="cancelled", route_name="files", attempts=1)
    await instance.store.insert(cancelled_two)
    await sender._on_retryable_failure(instance.store, cancelled_two, Failed5xx(status=503))
    fresh_two = await instance.store.get(cancelled_two.chain_id)
    assert fresh_two is not None
    assert fresh_two.state == "cancelled"
    assert fresh_two.last_error is None


def test_new_state_stored_literal_has_exactly_one_call_site() -> None:
    """The ``new_state="stored"`` literal appears at exactly one call site.

    D7 (one-writer-per-effect): every path that parks a row in
    ``stored`` must run through the single private helper. A second
    literal site means a second writer crept back in.
    """
    sender_path = Path(__file__).parent.parent.parent / "src" / "phantom" / "workers" / "sender.py"
    tree = ast.parse(sender_path.read_text())
    sites = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and any(
            kw.arg == "new_state"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value == "stored"
            for kw in node.keywords
        )
    ]
    assert len(sites) == 1, (
        f"new_state='stored' must have exactly one call site in workers/sender.py "
        f"(the single-writer helper); found {len(sites)}"
    )
