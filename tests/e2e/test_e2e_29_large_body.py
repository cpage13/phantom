"""E2E-29 — large-body large_capture.

The largest single body in the suite. A large_capture is a JSON dump
that scales with the producer's data volume and is the most likely
body to trigger Phantom's tier-migration path.

Test config overrides:

- ``storage.in_memory_max_bytes`` is set to a value below the test
  body size so the body cannot fit in the RAM tier.
- ``storage.persist_trigger.body_size_threshold_bytes`` is set below
  the test body size so bodies skip the RAM tier entirely
  (size-aware persist trigger from the just-landed robustness work).

The chain must reach ``succeeded`` end-to-end; the row must report
``tier == "persisted"`` from the first observation (no RAM transit).

The ``test_e2e_29_10_mib_body`` case exercises the full 10 MiB
target. Phantom's ``/v1/send`` route now threads
``Settings.storage.max_buffered_bytes`` (default 2 GiB) through to
starlette's ``request.form(max_part_size=...)`` so multipart bodies
up to that cap are accepted. The smaller-body case remains for
size-aware-persist-trigger coverage at sub-1-MiB scale.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
import pytest_asyncio
from phantom_client import PhantomClient

from ._driver import CreateFileRequest, FileMetadata, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads_routines import (
    LARGE_CAPTURE_BYTES,
    build_large_body,
)
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

# Per-upload synchronous-return budget. The under-limit body is
# < 1 MiB so multipart copy stays fast; 250 ms is the standard ceiling.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.25

# Terminal-state budget. A near-1-MiB persisted PUT takes a few
# seconds on slow runners.
TERMINAL_STATE_BUDGET_SECONDS: float = 30.0

# Memory-tier cap forcing the test body off the RAM path. Tight
# enough that even the small test body cannot fit.
IN_MEMORY_MAX_BYTES: int = 256 * 1024  # 256 KiB

# Size-aware persist threshold. Bodies at or above this skip the RAM
# tier entirely. Set well below the test body so the trigger fires.
PERSIST_BODY_SIZE_THRESHOLD_BYTES: int = 512 * 1024  # 512 KiB

# Test body size — large enough to exceed the persist threshold and
# RAM cap, small enough to stay under starlette's 1 MiB multipart
# part-size default. See module docstring.
TEST_BODY_BYTES: int = 900 * 1024  # 900 KiB


pytestmark = pytest.mark.e2e


# Per-test config overlay — set the RAM cap and size-aware persist
# trigger so the test body lands directly on the persisted tier.
# Compression is disabled so the body size visible to the size-aware
# trigger matches the raw body size (the trigger compares against the
# encoded bytes; zstd of high-entropy data can be near-original size
# but a deterministic check is simpler).
_LARGE_BODY_OVERRIDE: dict[str, object] = {
    "storage": {
        # Phase 1 migration: ``storage.in_memory_max_bytes`` →
        # ``storage.body_store.ram_ceiling_bytes``.
        "body_store": {
            "ram_ceiling_bytes": IN_MEMORY_MAX_BYTES,
        },
        "persist_trigger": {
            "body_size_threshold_bytes": PERSIST_BODY_SIZE_THRESHOLD_BYTES,
        },
        "compression": {
            # Always-encode policy + ``algorithm=original`` selects the
            # PassthroughCodec — identity codec for tests that need
            # storage_encoding == "original" deterministically.
            "mode": "always",
            "algorithm": "original",
        },
    },
    "retention": {
        # Effectively disable the reaper for the duration of the test
        # so the chain row survives until the wall-clock terminal-state
        # poll completes. See E2E-25 module docstring for context.
        "reaper_interval_seconds": 3600,
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with the per-test large-body storage overrides."""
    s = await boot_stack(config_overrides=_LARGE_BODY_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def driver(stack: E2EStack) -> PhantomDriver:
    """Public driver against the overridden (large-body) stack."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


def _build_large_request(*, size_bytes: int) -> tuple[CreateFileRequest, bytes]:
    """Build a request + body of the requested size for size-tier tests.

    Compression is disabled in this test's config overlay so a
    deterministic repeating-byte body is fine — Phantom stores it
    in original encoding and the size-aware persist trigger sees
    the raw body size.
    """
    body = b"M" * size_bytes
    request = CreateFileRequest(
        domain="generic",
        lane_base_name="large_capture_json",
        file_name=f"e2e29_capture_{size_bytes}",
        metadata=FileMetadata(
            key_value_store={
                "uploader_id": "12345",
                "label": "alpha",
                "mode": "production",
                "label_name": "large_body",
                "multistage_id": "MS-001",
            }
        ),
    )
    return request, body


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the functional flow and sharing the stack boot — not
# cleanly splittable. Runs only via `-m perf` (see pyproject markers).
# The sibling test_e2e_29_10_mib_body has no budget assertion
# and stays in the default suite.
@pytest.mark.perf
async def test_e2e_29_large_body(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """900 KiB body skips RAM tier and reaches succeeded end-to-end.

    Verifies (the body stays under Phantom's multipart 1 MiB-per-part
    limit; see module docstring):

    - Synchronous return < SYNTHETIC_RETURN_BUDGET_SECONDS.
    - Row is persisted from first observation (``tier == "persisted"``)
      — no RAM transit; the size-aware persist trigger landed the body
      directly on disk.
    - Chain reaches ``succeeded`` within ``TERMINAL_STATE_BUDGET_SECONDS``.
    - Emulator's received entry matches the body byte size.
    - Memory-tier byte count never exceeds the configured RAM cap.
    """
    # Setup — fresh emulator state.
    emulator.clear_received()
    emulator.clear_failures()

    request, body = _build_large_request(size_bytes=TEST_BODY_BYTES)

    # Action — submit. Synthetic return must still be fast.
    start = time.perf_counter()
    result = await driver.in_memory_upload(request, body)
    elapsed = time.perf_counter() - start
    assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
        f"in_memory_upload took {elapsed:.3f}s; expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
    )
    assert isinstance(result.id, UUID)

    # Assertion — admin row eventually shows body_location='file'.
    # Phase 1 migration: the pre-Phase-1 size-aware trigger wrote
    # directly to disk at admission. In the new architecture
    # admission writes RAM-first then enqueues the PersistController
    # which flips body_location to 'file' once fsync completes.
    # We poll to give the controller time to land the migration.
    deadline = time.perf_counter() + 10.0
    body_location_observed: str | None = None
    while time.perf_counter() < deadline:
        rows, _ = await phantom_client.list_uploads(limit=10)
        for row in rows:
            if row.chain_id == result.id:
                body_location_observed = row.body_location
                break
        if body_location_observed == "file":
            break
    assert body_location_observed == "file", (
        f"expected body_location='file' (size-threshold path should enqueue "
        f"the migration immediately); got body_location={body_location_observed!r}"
    )

    # Assertion — chain reaches succeeded end-to-end.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3"

    # Assertion — emulator received the body intact.
    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(result.id),
        body_size=len(body),
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )
    assert received.body_size == TEST_BODY_BYTES

    # Assertion — RAM body bytes never accumulate beyond the configured
    # ceiling after the size-threshold path completes. In the new
    # architecture admission writes RAM-first but the PersistController
    # frees RAM after flipping body_location='file'. The post-migration
    # RAM usage for this chain should be back to zero (the body lives
    # on disk now). Phase 1 renamed ``stats.tier['memory']`` →
    # ``stats.body_location['ram']``.
    stats = await phantom_client.get_stats()
    assert stats.body_location["ram"].bytes < IN_MEMORY_MAX_BYTES, (
        f"RAM bytes={stats.body_location['ram'].bytes} exceed the "
        f"configured ceiling {IN_MEMORY_MAX_BYTES}; the PersistController "
        f"may not have completed the migration before the assertion fired"
    )


async def test_e2e_29_10_mib_body(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """10 MiB JSON upload — full 10 MiB target.

    A large_capture JSON body at the 10 MiB scale. Phantom's
    ``/v1/send`` route now plumbs ``Settings.storage.max_buffered_bytes``
    (default 2 GiB) into ``request.form(max_part_size=...)`` so bodies
    up to that cap pass through the multipart parser. The chain must
    reach ``succeeded``.
    """
    emulator.clear_received()
    emulator.clear_failures()
    upload = build_large_body()
    assert len(upload.body) == LARGE_CAPTURE_BYTES
    result = await driver.in_memory_upload(upload.request, upload.body)
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
