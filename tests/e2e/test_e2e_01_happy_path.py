"""E2E-1 — Happy two-step upload chain end-to-end.

The canonical satisfaction of the project hard rule that "no upload
request goes unemulated end-to-end." Exercises every package in the
stack on the happy path:

1. Test code calls ``driver.in_memory_upload(request, contents)``.
2. The driver mints a local UUID and builds the two-step chain.
3. phantom-the-service buffers the chain envelope, forwards step 1
   (metadata POST) to the emulator, captures ``upload_url`` and
   ``file_information`` from the response.
4. phantom-the-service forwards step 2 (PUT to the captured
   ``upload_url``) to the emulator with the file bytes.
5. The chain transitions to ``succeeded``; the emulator's
   ``/control/received`` log records the body and headers.
"""

from __future__ import annotations

from uuid import UUID

import pytest
from phantom_client import PhantomClient

from ._driver import PhantomDriver
from .helpers.assertions import (
    assert_chain_reaches_state,
    assert_emulator_received,
)
from .helpers.payloads import DEFAULT_BODY, build_create_file_request
from .helpers.stack import EmulatorControl

# Upper bound on the chain reaching `succeeded`. The plan calls for
# 5 s; the helper's default is 10 s. We use the helper default so the
# CI runner has a small cushion without exceeding the suite's
# < 60 s overall budget.
TERMINAL_STATE_BUDGET_SECONDS: float = 10.0


async def test_e2e_01_happy_path(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Happy path: end-to-end two-step upload chain reaches succeeded.

    Drives the upload through the public driver and asserts Phantom's
    buffering, capture, and emulator-delivery behaviour.
    """
    # Setup — fresh emulator state so this test's `received` entry
    # is the only one in the log.
    emulator.clear_received()
    emulator.clear_failures()

    request = build_create_file_request()
    contents = DEFAULT_BODY

    # Action — the driver buffers the upload through the live stack and
    # returns the minted chain id immediately (the buffering ack).
    result = await driver.in_memory_upload(request, contents)

    # Assertion — the minted chain id is a UUID and the deep-copied
    # metadata carries the local UUID under ``phantom_local_uuid``.
    assert isinstance(result.id, UUID), f"id is not UUID: {result.id!r}"
    assert result.metadata.key_value_store["phantom_local_uuid"] == str(result.id), (
        "phantom_local_uuid in the submitted metadata must match the local UUID"
    )

    # Assertion — Phantom-side terminal state.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3", (
        f"expected last_step_completed='put_s3', got {chain_response.last_step_completed!r}"
    )

    # The chain captured two steps' values — step 1 produces
    # `upload_url` and `file_information`; step 2 declares no
    # captures (per the driver's envelope builder).
    captured_by_name = {cs.step_name: cs.values for cs in chain_response.captured}
    assert "create_file" in captured_by_name, (
        f"missing create_file capture; got steps: {list(captured_by_name)}"
    )
    create_values = captured_by_name["create_file"]
    assert "upload_url" in create_values
    assert "file_information" in create_values

    file_info = create_values["file_information"]
    assert isinstance(file_info, dict), (
        f"file_information must deserialize as dict, got {type(file_info)}"
    )
    emulator_file_id = UUID(file_info["id"])
    assert emulator_file_id != result.id, (
        "emulator-minted FileInformation.id must be distinct from the driver's local UUID"
    )

    # Assertion — emulator-side observation.
    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(result.id),
        body_size=len(contents),
    )
    assert received.metadata_kvs["uploader_id"] == "12345", (
        f"uploader_id must round-trip through emulator's create endpoint: {received.metadata_kvs}"
    )
    assert received.x_amz_meta_headers["x-amz-meta-uploader-id"] == "12345", (
        f"uploader_id must round-trip as x-amz-meta- header on the PUT: "
        f"{received.x_amz_meta_headers}"
    )


# Explicit marker for the test selector; the conftest auto-marks too,
# but keeping this here makes the intent legible at the test site.
pytestmark = pytest.mark.e2e
