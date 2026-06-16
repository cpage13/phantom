"""zstd at rest vs the cycle-7 read surfaces: lookups, export, rollup.

Round 4 adversary hardening (iteration loop, task 7.3). The existing
zstd coverage (test_e2e_14_storage_encoding) proves always-encode at
rest plus decode-before-forward; the export coverage
(test_e2e_18_bulk_export_mixed) deliberately pins PASSTHROUGH so it can
compare bytes verbatim. The corner neither touches: a zstd deployment
exercising the cycle-7 READ surfaces while bodies are buffered encoded.

Pinned here over a live daemon + emulator wire:

* ``key_value_match`` and ``find_by_local_uuid`` resolve grouped
  members whose bodies sit zstd-encoded (the lookups ride the uploads
  DB, codec-independent BY CONSTRUCTION; this proves the construction).
* ``export.tar`` under zstd packs the STORED (encoded) bytes: each
  exported body opens with the zstd magic frame and decodes back to the
  exact submitted payload; the manifest rows carry the cycle-7 fields
  (``group_id``, null ``sent_at`` while parked) plus
  ``storage_encoding="zstd"`` so an operator knows how to decode.
* After the upstream heals, the group delivers: rollup flips
  ``all_finished``, members carry ``sent_at``, and the emulator
  received the RAW bytes (transparency holds on the grouped zstd path).
"""

from __future__ import annotations

import io
import json
import tarfile
from typing import Any
from uuid import UUID, uuid4

import pytest
import zstandard
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import build_in_memory_upload_envelope
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

pytestmark = pytest.mark.e2e

# Compressible payloads sized well above the zstd frame overhead so the
# encoded-at-rest bytes are visibly distinct from the raw payload.
_REPEAT_BLOCK = b"phantom-zstd-grouped-payload "
_BODY_REPEATS = 2_048
# The zstd frame magic (RFC 8878): every encoded body must open with it.
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
# Every request 5xxes while the group must stay buffered.
_FORCE_5XX_RATE = 1.0
# Upper bound for one member reaching succeeded after the heal.
_TERMINAL_BUDGET_SECONDS = 15.0
# The KVS pair the metadata lookup resolves (colon-bearing VALUE on a
# plain key: the established first-colon wire form, R2-3 ruling).
_KVS_KEY = "calibration"
_KVS_VALUE = "bay:7"
# The e2e stack's fixed credential-cache axis value.
_UID = "00000000-0000-0000-0000-000000000001"


def _body_for(member_index: int) -> bytes:
    """A distinct, highly compressible body per group member."""
    return (_REPEAT_BLOCK + str(member_index).encode()) * _BODY_REPEATS


async def _submit_grouped(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
    options: SubmitOptions,
) -> None:
    """Submit one grouped member with the KVS pair + local uuid."""
    request = build_create_file_request(file_name=f"r4_zstd_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    request.metadata.key_value_store[_KVS_KEY] = _KVS_VALUE
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=_UID,
        auth_token=f"Bearer {bearer}",
        options=options,
    )


async def _drain_export(pc: PhantomClient) -> bytes:
    """Stream the export tar and concatenate all chunks."""
    iter_ = await pc.export_tar()
    chunks: list[bytes] = []
    async for chunk in iter_:
        chunks.append(chunk)
    return b"".join(chunks)


def _parse_export_tar(
    tar_bytes: bytes,
) -> tuple[list[dict[str, Any]], dict[str, bytes]]:
    """Parse the export into (manifest entries, regular-file bytes)."""
    manifest: list[dict[str, Any]] | None = None
    files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            ext = tar.extractfile(member)
            data = ext.read() if ext is not None else b""
            files[member.name] = data
            if member.name in {"manifest.json", "./manifest.json"}:
                manifest = json.loads(data.decode())
    assert manifest is not None, f"manifest.json missing; files={list(files)}"
    return manifest, files


@pytest.mark.e2e
async def test_zstd_grouped_lookups_export_then_delivery() -> None:
    """Lookups + export stay honest over zstd-encoded buffered bodies."""
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "compression": {
                    "mode": "always",
                    "algorithm": "zstd",
                    "level": 3,
                },
            },
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        },
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Hold the upstream down so the group parks buffered + encoded.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=_FORCE_5XX_RATE,
            )
        )

        group_id = uuid4()
        multifile_id = uuid4()
        member_ids = [uuid4(), uuid4()]
        bodies = {member_ids[i]: _body_for(i) for i in range(len(member_ids))}
        for order, chain_id in enumerate(member_ids):
            await _submit_grouped(
                pc,
                chain_id=chain_id,
                body=bodies[chain_id],
                emulator_url=stack.emulator_url,
                bearer=bearer,
                options=SubmitOptions(  # type: ignore[call-arg]
                    group_id=group_id, multifile_id=multifile_id, order=order
                ),
            )

        # Encoded at rest: both rows carry storage_encoding=zstd with
        # the grouping persisted and sent_at honestly null.
        rows, _ = await pc.list_uploads(limit=50)
        rows_by_id = {r.chain_id: r for r in rows}
        for chain_id in member_ids:
            row = rows_by_id[chain_id]
            assert row.storage_encoding == "zstd"
            assert row.group_id == group_id
            assert row.multifile_id == multifile_id
            assert row.sent_at is None

        # The KVS lookup resolves both buffered members; the codec
        # never touches the lookup surface.
        by_kvs = await pc.find_by_metadata(key=_KVS_KEY, value=_KVS_VALUE)
        assert sorted(r.chain_id for r in by_kvs) == sorted(member_ids)

        # The local-uuid lookup resolves a single member exactly.
        by_uuid = await pc.find_by_local_uuid(member_ids[0])
        assert by_uuid.found is True
        assert [m.chain_id for m in by_uuid.matches] == [member_ids[0]]

        # Export while parked: manifest carries the cycle-7 fields and
        # the packed bodies are the STORED zstd bytes, decodable back
        # to the exact submitted payloads.
        manifest, files = _parse_export_tar(await _drain_export(pc))
        entries_by_id = {e["chain_id"]: e for e in manifest}
        decompressor = zstandard.ZstdDecompressor()
        for chain_id in member_ids:
            entry = entries_by_id[str(chain_id)]
            assert entry["group_id"] == str(group_id)
            assert entry["sent_at"] is None
            assert entry["storage_encoding"] == "zstd"
            packed = files[f"bodies/{chain_id}/body"]
            assert packed.startswith(_ZSTD_MAGIC), "export must pack the stored bytes"
            assert packed != bodies[chain_id]
            assert decompressor.decompress(packed) == bodies[chain_id]
            assert entry["body_size_bytes"] == len(packed), (
                "manifest size must describe the stored (encoded) bytes it packs"
            )

        # Heal the upstream; the group delivers; the rollup flips; the
        # emulator received the RAW bytes (zstd transparency holds).
        emulator.clear_failures()
        for chain_id in member_ids:
            await assert_chain_reaches_state(
                pc, chain_id, state="succeeded", timeout_seconds=_TERMINAL_BUDGET_SECONDS
            )
        rollup = await pc.get_group_status(group_id)
        assert rollup.all_finished is True
        assert rollup.total == len(member_ids)
        assert rollup.last_sent_at is not None
        for member in rollup.members:
            assert member.sent_at is not None
        for chain_id in member_ids:
            received = await assert_emulator_received(
                emulator,
                phantom_local_uuid=str(chain_id),
                body_size=len(bodies[chain_id]),
            )
            assert received.body_size == len(bodies[chain_id])
    finally:
        await stack.tear_down()
