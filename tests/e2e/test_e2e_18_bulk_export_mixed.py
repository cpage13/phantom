"""E2E-18 — Bulk export with mixed states.

Constructs three uploads that land in three different terminal states
(``succeeded``, ``failed``, ``auth_expired``), then streams the
``GET /v1/admin/export.tar`` archive and inspects the manifest plus
body files.

See ADR-005 for the export contract.
"""

from __future__ import annotations

import contextlib
import io
import json
import tarfile
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainBodyJson, ChainEnvelope, ChainStep
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

# Body for each upload — distinct content so we can verify the
# exporter's body-file association.
BODY_SUCCEEDED: bytes = b"phantom-e2e-export-succeeded-body"
BODY_FAILED: bytes = b"phantom-e2e-export-failed-body-content"
BODY_AUTH_EXPIRED: bytes = b"phantom-e2e-export-auth-expired-body"

SHARED_SUB: str = "00000000-0000-0000-0000-000000000018"
TERMINAL_WAIT_SECONDS: float = 10.0


@pytest.mark.e2e
async def test_e2e_18_bulk_export_mixed() -> None:
    """export.tar carries manifest + body files for three mixed-state rows."""
    stack = await boot_stack(
        config_overrides={
            "storage": {
                # Pin passthrough so the export.tar's stored body bytes
                # equal the bytes the client submitted — the test
                # compares verbatim. Always-encode + ``original`` is the
                # explicit passthrough opt-in under F3.
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
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
        bearer = stack.fake_security_token(sub=SHARED_SUB)

        # 1. Submit happy upload → succeeded.
        chain_s = uuid4()
        await _submit_happy(
            pc,
            chain_id=chain_s,
            body=BODY_SUCCEEDED,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            pc,
            chain_s,
            state="succeeded",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )

        # 2. Submit bad-path chain → failed.
        chain_f = uuid4()
        await _submit_bad_path(
            pc,
            chain_id=chain_f,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            pc,
            chain_f,
            state="failed",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )

        # 3. Inject 401, submit a third upload → auth_expired.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            ),
        )
        chain_a = uuid4()
        await _submit_happy(
            pc,
            chain_id=chain_a,
            body=BODY_AUTH_EXPIRED,
            emulator_url=stack.emulator_url,
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            pc,
            chain_a,
            state="auth_expired",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )
        emulator.clear_failures()

        # 4. Stream the tar export and parse it.
        tar_bytes = await _drain_export(pc)
        manifest, body_files = _parse_export_tar(tar_bytes)

        # Manifest must list all three rows. The manifest shape is
        # not strictly specified in ADR-005; we sanity-check the
        # entries key carries the chain_ids.
        manifest_chain_ids = _extract_chain_ids_from_manifest(manifest)
        expected = {chain_s, chain_f, chain_a}
        assert expected.issubset(manifest_chain_ids), (
            f"manifest missing chain_ids: expected={expected}, manifest={manifest_chain_ids}"
        )

        # Manifest states match. We extract the state for each entry
        # using a permissive shape match (an entries list with
        # ``chain_id``/``uid`` + ``state``).
        manifest_states = _extract_states_by_chain_id(manifest)
        assert manifest_states.get(chain_s) == "succeeded"
        assert manifest_states.get(chain_f) == "failed"
        assert manifest_states.get(chain_a) == "auth_expired"

        # Body files: per retention defaults, auth_expired retains
        # its body (the load-bearing recovery case). The
        # ``failed`` chain in this test is a JSON-only bad-path step
        # with no body_refs, so it has nothing to export; ``succeeded``
        # clears its body immediately (succeeded_body_seconds: 0).
        # The export should carry at least the auth_expired body.
        body_chain_ids = _extract_chain_ids_from_body_paths(body_files)
        assert chain_a in body_chain_ids, (
            f"expected auth_expired chain {chain_a} body in export; got bodies for {body_chain_ids}"
        )

        # Sanity: body bytes for the auth_expired chain match what
        # we sent. The auth_expired chain rode the happy envelope so
        # its body file should contain BODY_AUTH_EXPIRED.
        auth_body_content = _read_body_for_chain(body_files, chain_a)
        assert auth_body_content == BODY_AUTH_EXPIRED, (
            f"auth_expired chain {chain_a} body bytes mismatch: got {auth_body_content!r}"
        )

        # Total file count: 1 manifest + at least the auth_expired
        # body. tar files include directory entries; we filter to
        # regular files only.
        regular_files = [name for name, content in body_files.items() if content is not None]
        # Plus manifest.
        total = 1 + len(regular_files)
        assert total >= 2, f"expected at least 2 files (manifest + auth_expired body), got {total}"
    finally:
        await stack.tear_down()


async def _drain_export(pc: PhantomClient) -> bytes:
    """Stream the export tar and concatenate all chunks."""
    iter_ = await pc.export_tar()
    chunks: list[bytes] = []
    async for chunk in iter_:
        chunks.append(chunk)
    return b"".join(chunks)


def _parse_export_tar(tar_bytes: bytes) -> tuple[dict[str, object], dict[str, bytes | None]]:
    """Parse the tar archive into ``(manifest_json, files_dict)``.

    The ``files_dict`` maps tar member name → bytes for regular files,
    None for directories.
    """
    manifest: dict[str, object] | None = None
    files: dict[str, bytes | None] = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                files[member.name] = None
                continue
            ext = tar.extractfile(member)
            data = ext.read() if ext is not None else b""
            files[member.name] = data
            if member.name in {"manifest.json", "./manifest.json"}:
                manifest = json.loads(data.decode())
    if manifest is None:
        raise AssertionError(f"manifest.json missing from export tar; files={list(files)}")
    return manifest, files


def _extract_chain_ids_from_manifest(manifest: dict[str, object]) -> set[UUID]:
    """Walk the manifest and collect any UUID-shaped string values."""
    out: set[UUID] = set()

    def _walk(value: object) -> None:
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                out.add(UUID(value))
        elif isinstance(value, dict):
            for v in value.values():
                _walk(v)
        elif isinstance(value, list):
            for v in value:
                _walk(v)

    _walk(manifest)
    return out


def _extract_states_by_chain_id(manifest: dict[str, object]) -> dict[UUID, str]:
    """Find every {chain_id|uid: X, state: Y} dict in the manifest."""
    out: dict[UUID, str] = {}

    def _walk(value: object) -> None:
        if isinstance(value, dict):
            uid_val = value.get("uid") or value.get("chain_id")
            state_val = value.get("state")
            if isinstance(uid_val, str) and isinstance(state_val, str):
                with contextlib.suppress(ValueError):
                    out[UUID(uid_val)] = state_val
            for v in value.values():
                _walk(v)
        elif isinstance(value, list):
            for v in value:
                _walk(v)

    _walk(manifest)
    return out


def _extract_chain_ids_from_body_paths(files: dict[str, bytes | None]) -> set[UUID]:
    """Pull UUIDs out of body-file path names."""
    out: set[UUID] = set()
    for name in files:
        # The path layout is implementation-dependent; we just look
        # for any UUID-shaped segment.
        for segment in name.replace("\\", "/").split("/"):
            with contextlib.suppress(ValueError):
                out.add(UUID(segment))
    return out


def _read_body_for_chain(
    files: dict[str, bytes | None],
    chain_id: UUID,
) -> bytes | None:
    """Find a regular file whose path embeds ``chain_id`` and return its bytes.

    Tar paths use the body-store layout
    ``bodies/<shard>/<uid>/<body_ref_name>`` (FileBodyStore convention).
    The driver's envelope names the single body_ref ``body``, so the
    matching file lives at ``.../<chain_id>/body``.
    """
    chain_id_str = str(chain_id)
    for name, content in files.items():
        if chain_id_str in name and content is not None and not name.endswith(".json"):
            return content
    return None


async def _submit_happy(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit a happy two-step envelope via the driver's builder."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def _submit_bad_path(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit a chain at a 404 path so the upstream returns 4xx → failed."""
    step = ChainStep(
        name="bad_path_step",
        method="POST",
        url=f"{emulator_url}/v1/files/this-path-does-not-exist",
        headers={"Content-Type": "application/json"},
        body=ChainBodyJson(
            kind="json",
            value={
                "metadata": {
                    "keyValueStore": {"phantom_local_uuid": str(chain_id)},
                },
            },
        ),
        capture=[],
        idempotency_header=None,
    )
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[step],
        default_target=None,
    )
    await pc.submit_chain(
        envelope,
        body_refs=None,
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
    )
