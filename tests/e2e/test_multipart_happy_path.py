"""Multipart-specific E2E (plan § 6.2.6) — happy path.

Builds an envelope that uploads N files in one chain. Each file has
its own `(POST /v2/files, PUT <captured-url>)` step pair, with a
distinct named ``body_ref`` so the bytes ride alongside the envelope
as a multipart submission. Asserts every file reaches the emulator
with byte-identical contents.

The body-as-atomic-unit invariant (strategy §1 + plan § 0.4
glossary) is that a multipart body succeeds-or-fails as a unit: if
any constituent file's upstream step fails, the chain does not
report success. The happy-path test below verifies the success leg
of that invariant — every file lands intact.

Companion tests:

- :mod:`test_multipart_atomic_persist` — body-as-atomic-unit through
  the persist controller.
- :mod:`test_multipart_corrupted` — corrupted file → corrupted
  terminal state.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainStep,
)

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack

logger = logging.getLogger(__name__)

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Three files in the multipart chain.
MULTIPART_FILE_COUNT: int = 3

# Per-file body size (small; the test is about structure, not bytes).
PER_FILE_BODY_BYTES: int = 4096

# Per-chain end-to-end budget.
PER_CHAIN_BUDGET_SECONDS: float = 60.0

pytestmark = [pytest.mark.e2e]


def _build_one_file_pair(
    *,
    idx: int,
    chain_id: UUID,
    emulator_url: str,
    body_ref_name: str,
) -> tuple[ChainStep, ChainStep]:
    """Build the (POST, PUT) step pair for one file in the multipart chain."""
    body_value: dict[str, object] = {
        "fileName": f"multipart-{idx}-{chain_id.hex[:8]}.bin",
        "domain": "generic",
        "laneBaseName": "lane",
        "metadata": {
            "keyValueStore": {
                "phantom_local_uuid": str(chain_id),
                "multipart_index": str(idx),
            },
        },
    }
    post_step = ChainStep(
        name=f"create_file_{idx}",
        method="POST",
        url=emulator_url.rstrip("/") + "/v2/files",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        body=ChainBodyJson(kind="json", value=body_value),
        capture=[
            ChainCapture.model_validate(
                {
                    "name": "upload_url",
                    "from": "$.uploadUrl",
                    "ttl_seconds": 3600,
                    "sensitive": True,
                },
            ),
        ],
        idempotency_header=None,  # Distinct upload-token per file (no dedup).
    )
    put_step = ChainStep(
        name=f"put_s3_{idx}",
        method="PUT",
        url=f"{{{{create_file_{idx}.upload_url}}}}",
        headers={
            "x-amz-meta-multipart-index": str(idx),
            "x-amz-meta-phantom-local-uuid": str(chain_id),
        },
        body=ChainBodyRef(
            kind="body_ref",
            name=body_ref_name,
            content_type="application/octet-stream",
        ),
        capture=[],
        idempotency_header=None,
    )
    return post_step, put_step


def _build_multipart_envelope(
    *,
    chain_id: UUID,
    emulator_url: str,
    body_count: int,
) -> tuple[ChainEnvelope, list[str]]:
    """Build a multipart envelope with ``body_count`` file pairs.

    Returns the envelope plus the ordered list of body_ref names so
    the caller can populate ``body_refs={name: bytes}`` for the
    submit_chain call.
    """
    steps: list[ChainStep] = []
    names: list[str] = []
    for i in range(body_count):
        name = f"body_{i}"
        names.append(name)
        post, put = _build_one_file_pair(
            idx=i,
            chain_id=chain_id,
            emulator_url=emulator_url,
            body_ref_name=name,
        )
        steps.extend([post, put])
    envelope = ChainEnvelope(
        chain_id=chain_id,  # type: ignore[arg-type]
        idempotency_key=str(chain_id),
        steps=steps,
        default_target=None,
    )
    return envelope, names


async def test_multipart_happy_path_three_files(
    stack: E2EStack,
    phantom_client: PhantomClient,
) -> None:
    """Three-file multipart chain end-to-end; every file delivered intact.

    Submits one envelope carrying three separate POST+PUT pairs;
    asserts the chain reaches ``succeeded`` and the emulator's
    received-log contains exactly one entry per file with the
    correct body hash.
    """
    emulator = stack.emulator
    emulator.clear_received()
    emulator.clear_failures()
    bearer = stack.fake_security_token()

    chain_id = uuid4()
    envelope, names = _build_multipart_envelope(
        chain_id=chain_id,
        emulator_url=stack.emulator_url,
        body_count=MULTIPART_FILE_COUNT,
    )
    bodies: dict[str, bytes] = {n: secrets.token_bytes(PER_FILE_BODY_BYTES) for n in names}

    await phantom_client.submit_chain(
        envelope,
        body_refs=bodies,
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )

    # Wait for the chain to succeed.
    await assert_chain_reaches_state(
        phantom_client,
        chain_id,
        state="succeeded",
        timeout_seconds=PER_CHAIN_BUDGET_SECONDS,
    )

    # Emulator received exactly one entry per multipart_index 0..N-1
    # with the correct SHA-256. The emulator records x-amz-meta-*
    # headers with the full prefix as the key (lowercased).
    received_by_idx: dict[str, str] = {}
    for entry in emulator.received():
        if entry.metadata_kvs.get("phantom_local_uuid") != str(chain_id):
            continue
        idx_str = entry.x_amz_meta_headers.get("x-amz-meta-multipart-index")
        if idx_str is not None:
            received_by_idx[idx_str] = entry.body_hash

    assert len(received_by_idx) == MULTIPART_FILE_COUNT, (
        f"expected {MULTIPART_FILE_COUNT} multipart entries, got {len(received_by_idx)}; "
        f"received={received_by_idx}"
    )
    for i in range(MULTIPART_FILE_COUNT):
        expected = hashlib.sha256(bodies[f"body_{i}"]).hexdigest()
        actual = received_by_idx.get(str(i))
        assert actual == expected, (
            f"multipart index {i} body hash mismatch: expected {expected!r}, got {actual!r}"
        )
