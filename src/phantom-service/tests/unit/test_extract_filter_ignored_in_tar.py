"""The extract tar endpoint must honor every ExtractFilter field it advertises (R8-5).

``POST /v1/admin/chains/extract`` takes an :class:`ExtractFilter` with
five fields: ``state``, ``route``, ``since``, ``chain_ids``, ``instance``.
The service model documents them as real restrictions ("Restrict to
rows updated at or after this UTC timestamp", "Restrict to this explicit
list of chain_ids") and the byte-mirrored SDK model
(``phantom_client.models.admin.ExtractFilter``) repeats them verbatim
("Match by updated_at >= since", "Match exactly these chain_ids"), so an
operator (or a tool over the SDK) reasonably expects them to filter.

``_build_tar_stream`` (``routes/admin.py``) only forwards ``state`` and
``route`` to ``store.list_uploads``:

    chunk, _ = await ctx.store.list_uploads(
        state=filter_body.state,
        route=filter_body.route,
        limit=_EXPORT_TAR_PER_INSTANCE_LIMIT,
    )

``since`` and ``chain_ids`` are dropped on the floor - silently. The
filter parses, the request succeeds, and the tar comes back with EVERY
row (modulo state/route), not the requested subset. ``list_uploads``
already accepts ``since`` (``storage/sqlite_store.py``), so the ``since``
leg is a pure wiring omission; ``chain_ids`` has no ``list_uploads``
parameter at all, so the route advertises a filter the storage layer
cannot honor.

Why it matters: a silent no-op filter is worse than an error. An
operator narrowing an emergency recovery to "just these three chain_ids"
or "everything since the outage started" gets a tar containing the
entire buffer - on a producer with thousands of buffered bodies that is a
much larger transfer than intended (the per-instance cap is 10,000
rows), it exposes bodies the operator did not ask to pull, and it gives
no signal that the narrowing was ignored. The contract the model
publishes is simply not met.

These tests drive the REAL ``extract_uploads`` route over a REAL
SqliteUploadStore + HybridBodyStore through the dispatcher, seed two
rows that differ on both axes, and assert the manifest carries ONLY the
requested subset. The fix forwards ``since`` to ``list_uploads`` (already
supported) and either adds a ``chain_ids`` filter to ``list_uploads`` or
post-filters the chunk by the requested id set before packing.
"""

from __future__ import annotations

import io
import json
import tarfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.admin import ExtractFilter
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

# Body bytes for the seeded rows (small; the test is about WHICH rows
# land in the manifest, not their contents).
_BODY_NAME: str = "body"
_BODY_BYTES: bytes = b"abc"

# Age gap between the two seeded rows so a "since yesterday" filter
# selects exactly one. Thirty days is unambiguous against any clock skew.
_OLD_AGE_DAYS: int = 30
_SINCE_WINDOW_DAYS: int = 1

# Generous gate caps - admission is not under test here.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

# R8-5 (fixed): chain_ids is served by point reads with the other axes
# as predicates; the listing path forwards since (received_at >=).


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose extract route the test exercises end to end."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await ram.start()
    await fbs.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
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
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
        ),
        codec_factory=MagicMock(),
        current_settings=MagicMock(),
    )
    return track_instance(instance)


def _stored_row(chain_id: UUID, received_at: datetime) -> UploadRow:
    """A ``stored`` row (the export's main audience) received at ``received_at``."""
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "emu",
            "group_id": uuid4(),
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "stored",
            "body_location": "ram",
            "received_at": received_at,
            "updated_at": received_at,
            "endpoint": "files.example.com",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": str(chain_id),
            "capture_reexecution_active": False,
            "body_size_bytes": len(_BODY_BYTES),
        }
    )


async def _seed_two_rows(instance: InstanceContext) -> tuple[UUID, UUID]:  # type: ignore[name-defined]
    """Insert one old row and one fresh row, each with a body; return (old, new)."""
    old_at = datetime.now(tz=UTC) - timedelta(days=_OLD_AGE_DAYS)
    new_at = datetime.now(tz=UTC)
    chain_id_old = uuid4()
    chain_id_new = uuid4()
    for chain_id, received_at in ((chain_id_old, old_at), (chain_id_new, new_at)):
        await instance.store.insert(_stored_row(chain_id, received_at))
        await instance.ram_body_store.put(chain_id, {_BODY_NAME: _BODY_BYTES})
    return chain_id_old, chain_id_new


async def _manifest_chain_ids(body_iterator: AsyncIterator[bytes]) -> set[str]:
    """Drain the tar stream and return the set of manifest chain_ids."""
    tar_bytes = b""
    async for chunk in body_iterator:
        tar_bytes += chunk
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tf:
        member = tf.extractfile("manifest.json")
        assert member is not None, "the tar must carry a manifest.json"
        entries = json.loads(member.read())
    return {entry["chain_id"] for entry in entries}


async def test_extract_chain_ids_filter_restricts_the_tar(tmp_path: Path) -> None:
    """An extract scoped to one chain_id must return only that row.

    Attack: seed two rows, then drive the REAL ``extract_uploads`` with
    ``chain_ids=[only_new]`` - the SDK-advertised "match exactly these
    chain_ids". The manifest must list exactly that one chain_id. Today
    the route drops ``chain_ids`` entirely and the manifest carries both
    rows, silently over-exporting.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    _chain_id_old, chain_id_new = await _seed_two_rows(instance)

    response = await admin_routes.extract_uploads(
        ExtractFilter(chain_ids=[chain_id_new]), dispatcher
    )
    manifest_ids = await _manifest_chain_ids(response.body_iterator)

    assert manifest_ids == {str(chain_id_new)}, (
        "extract with chain_ids=[one id] must restrict the tar to that id; got "
        f"{manifest_ids} - the chain_ids filter is advertised by the model but "
        "ignored by _build_tar_stream, so the whole buffer is exported"
    )


async def test_extract_since_filter_restricts_the_tar(tmp_path: Path) -> None:
    """An extract scoped to ``since`` must return only rows at/after it.

    Attack: seed one 30-day-old row and one fresh row, then drive the
    REAL ``extract_uploads`` with ``since=yesterday`` - the "everything
    since the outage started" recovery shape. ``list_uploads`` already
    supports ``since``, so this is a pure route-wiring omission: the
    manifest must carry only the fresh row, but today it carries both.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    _chain_id_old, chain_id_new = await _seed_two_rows(instance)

    since = datetime.now(tz=UTC) - timedelta(days=_SINCE_WINDOW_DAYS)
    response = await admin_routes.extract_uploads(ExtractFilter(since=since), dispatcher)
    manifest_ids = await _manifest_chain_ids(response.body_iterator)

    assert manifest_ids == {str(chain_id_new)}, (
        "extract with since=yesterday must restrict the tar to rows received "
        f"at/after it; got {manifest_ids} - the since filter is dropped by "
        "_build_tar_stream even though list_uploads supports it"
    )
