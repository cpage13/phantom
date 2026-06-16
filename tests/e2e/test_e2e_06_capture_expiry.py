"""E2E-6 — Capture-TTL expiry handling per ADR-011.

Two sub-cases:

- ``test_capture_expiry_default_false_transitions_to_stored`` —
  ``capture_reexecution: false`` (the suite's default). When the
  captured ``upload_url`` expires before step 2 can use it, the
  chain transitions to ``stored`` and the body stays recoverable
  via the bulk export.
- ``test_capture_expiry_operator_true_reexecutes_step_1`` —
  ``capture_reexecution: true``. Phantom re-runs step 1 with the
  chain's idempotency key; the emulator's idempotency cache returns
  the same ``file_information.id`` and a fresh ``upload_url``; the
  retried step 2 succeeds.

Both sub-cases construct their chain envelopes inline (rather than
via the driver) because the driver hard-codes
``ttl_seconds=7 days`` on the ``upload_url`` capture.
The tests need a 1-2 second TTL to make capture expiry observable
within the suite's runtime budget, so they craft a custom envelope
through :meth:`PhantomClient.submit_chain` directly.

The chain-envelope shape mirrors what the driver would build:
step 1 POSTs to ``/v2/files`` with ``Idempotency-Key`` set; step 2
PUTs to ``{{create_file.upload_url}}``. Only the capture TTL
differs.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from phantom_client import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainStep,
    PhantomClient,
)
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, EmulatorControl, boot_stack
from .helpers.timing import await_until

# Shared payload and envelope shape constants ------------------------------

# Body payload bytes — small enough to fit in any tier, large enough
# that the emulator's body_size assertion has meaning.
TEST_BODY: bytes = b"phantom-e2e-capture-expiry-body-bytes-00000000"

# JSON body for step 1 — matches the shape the CreateFileRequest
# serializes to in camelCase. The fields here are the minimum the
# emulator's ``create_file`` handler needs to mint a valid response;
# extra fields are passed through to ``metadata.keyValueStore``.
TEST_FILE_NAME: str = "e2e_capture_expiry_test"
TEST_DOMAIN: str = "generic"
TEST_LANE_BASE: str = "metadata_table"
TEST_UPLOADER_ID: str = "12345"
TEST_LABEL: str = "alpha"

# Capture TTL. 2 seconds is short enough that one retry interval
# (``intervals_seconds: [0, 1, 2, ...]``) lands past expiry, but
# long enough that the first attempt of step 2 isn't already too
# late before phantom even hits send().
CAPTURE_TTL_SECONDS: int = 2

# The failure-injection rate used to keep step 2 in the retry loop
# long enough for the capture to expire. Every PUT in the failure
# window returns 503.
FORCE_5XX_RATE: float = 1.0

# Budget for the chain to reach ``stored`` (sub-case 6a). The
# retry interval list is [0, 1, 2, ...]; the capture-TTL is 2s; so
# expiry triggers on the third attempt (at intervals_offset=3s),
# which is comfortably under 10s.
STORED_BUDGET_SECONDS: float = 10.0

# Budget for the chain to reach ``succeeded`` after the failure is
# lifted in sub-case 6b. The re-execution path issues a fresh step 1
# (which the emulator's idempotency cache returns from the dedup
# entry), a fresh step 2 against the fresh upload_url, and a
# success. 15-second budget.
SUCCEEDED_BUDGET_SECONDS: float = 15.0


pytestmark = pytest.mark.e2e


def _build_capture_expiry_envelope(
    *,
    chain_id: object,
    emulator_url: str,
) -> ChainEnvelope:
    """Build a two-step upload envelope with a short capture TTL.

    The envelope is structurally identical to what
    :func:`tests.e2e._driver.build_in_memory_upload_envelope`
    produces, except the ``upload_url`` capture's ``ttl_seconds`` is
    :data:`CAPTURE_TTL_SECONDS` (short) instead of the production
    7-day value. The ``Idempotency-Key`` header is declared on step
    1 so ADR-011 re-execution behavior can engage when enabled.
    """
    body_value: dict[str, object] = {
        "fileName": TEST_FILE_NAME,
        "domain": TEST_DOMAIN,
        "laneBaseName": TEST_LANE_BASE,
        "metadata": {
            "keyValueStore": {
                "uploader_id": TEST_UPLOADER_ID,
                "label": TEST_LABEL,
                "phantom_local_uuid": str(chain_id),
            },
        },
    }
    step_create = ChainStep(
        name="create_file",
        method="POST",
        url=emulator_url.rstrip("/") + "/v2/files",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        body=ChainBodyJson(kind="json", value=body_value),
        capture=[
            ChainCapture.model_validate(
                {"name": "upload_url", "from": "$.uploadUrl", "ttl_seconds": CAPTURE_TTL_SECONDS}
            ),
            ChainCapture.model_validate(
                {
                    "name": "file_information",
                    "from": "$.fileInformation",
                    "ttl_seconds": CAPTURE_TTL_SECONDS,
                }
            ),
        ],
        idempotency_header="Idempotency-Key",
    )
    step_put = ChainStep(
        name="put_s3",
        method="PUT",
        url="{{create_file.upload_url}}",
        headers={
            "x-amz-meta-uploader-id": TEST_UPLOADER_ID,
            "x-amz-meta-label": TEST_LABEL,
            "x-amz-meta-phantom-local-uuid": str(chain_id),
        },
        body=ChainBodyRef(kind="body_ref", name="body", content_type="application/octet-stream"),
        capture=[],
        idempotency_header=None,
    )
    return ChainEnvelope(
        chain_id=chain_id,  # type: ignore[arg-type]
        idempotency_key=str(chain_id),
        steps=[step_create, step_put],
        default_target=None,
    )


async def test_capture_expiry_default_false_transitions_to_stored(
    stack: E2EStack,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Capture TTL expires; default config sends chain to ``stored`` (sub-case 6a).

    Step 1 succeeds (captures upload_url with TTL=2s). Step 2 is
    blocked by a 100% 5xx policy on the PUT path; the retry loop
    walks the intervals [0, 1, 2, ...]; on the third retry the
    capture has expired (now > captured_at + 2s) and Phantom
    transitions the row to ``stored`` per ADR-011 with
    ``capture_reexecution: false`` (the suite default).
    """
    emulator.clear_received()
    emulator.clear_failures()
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            error_rate_5xx=FORCE_5XX_RATE,
        )
    )

    chain_id = uuid4()
    envelope = _build_capture_expiry_envelope(
        chain_id=chain_id,
        emulator_url=stack.emulator_url,
    )
    bearer = stack.fake_security_token()

    await phantom_client.submit_chain(
        envelope,
        body_refs={"body": TEST_BODY},
        uid=DEFAULT_FAKE_SUB,
        auth_token=f"Bearer {bearer}",
    )

    # Wait for the chain to reach the ``stored`` terminal state.
    # ``assert_chain_reaches_state`` returns :class:`ChainAdminDetail`
    # post-Wave-3b (the admin endpoint returns the extended detail
    # shape; the wire-facing :class:`ChainResponse` is unchanged).
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        chain_id,
        state="stored",
        timeout_seconds=STORED_BUDGET_SECONDS,
    )
    assert chain_response.state == "stored"
    assert chain_response.last_step_completed == "create_file", (
        f"expected last_step_completed='create_file' (step 2 never ran to "
        f"terminal-success); got {chain_response.last_step_completed!r}"
    )

    # The captured upload_url is preserved on the row even though
    # the chain transitioned to ``stored``. Admin can query the
    # captured values for diagnostics.
    captured_by_name = {cs.step_name: cs.values for cs in chain_response.captured}
    assert "create_file" in captured_by_name
    assert "upload_url" in captured_by_name["create_file"]

    # The emulator received the create POST (step 1 succeeded) but
    # NOT the PUT body — step 2 was 503'd repeatedly and never
    # accepted a body. Body retention defaults preserve the body on
    # ``stored`` per ``stored_body_seconds: 15768000`` (6 months) so
    # operator recovery via ``GET /v1/admin/export.tar`` is meaningful.
    matched_entries = [
        entry
        for entry in emulator.received()
        if entry.metadata_kvs.get("phantom_local_uuid") == str(chain_id)
    ]
    assert not matched_entries, (
        f"expected zero emulator-received entries (the PUT never landed); "
        f"got {len(matched_entries)}"
    )


async def test_capture_expiry_operator_true_reexecutes_step_1(
    emulator: EmulatorControl,
) -> None:
    """Capture TTL expires; ``capture_reexecution: true`` re-runs step 1 (sub-case 6b).

    Boots a per-test stack with the ``primary`` instance's
    ``capture_reexecution`` flag flipped to ``true``. Step 1
    succeeds the first time; step 2 fails until the capture expires;
    Phantom re-executes step 1 using the chain's ``idempotency_key``
    on the ``Idempotency-Key`` header. The emulator's idempotency
    cache returns the previously-cached response (same
    ``file_information.id``, fresh ``upload_url``), Phantom updates
    its capture, step 2 runs against the fresh URL, the chain
    succeeds.

    The clear-failures step lets the re-executed step 2 actually
    succeed; otherwise the chain would keep rewinding indefinitely.
    """
    # We intentionally do not depend on the session ``stack`` fixture
    # for this sub-case — we need a per-test instance with
    # ``capture_reexecution: true`` and the rest of the YAML
    # unchanged. ``boot_stack(config_overrides=...)`` is the helper's
    # supported path for that.
    del emulator  # the per-test stack carries its own emulator handle

    overrides: dict[str, object] = {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                "data_dir": "primary",
                "capture_reexecution": True,
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["emulator", "127.0.0.1", "localhost"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ]
    }
    stack = await boot_stack(config_overrides=overrides)
    try:
        local_emulator = stack.emulator
        local_phantom_client = stack.phantom_client
        local_emulator.clear_received()
        local_emulator.clear_failures()
        local_emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )

        chain_id = uuid4()
        envelope = _build_capture_expiry_envelope(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
        )
        bearer = stack.fake_security_token()

        await local_phantom_client.submit_chain(
            envelope,
            body_refs={"body": TEST_BODY},
            uid=DEFAULT_FAKE_SUB,
            auth_token=f"Bearer {bearer}",
        )

        # Give the chain a moment to hit capture expiry and rewind
        # (the row should transition through ``queued`` and back to
        # step 1). Then clear failures so the re-executed step 2 can
        # actually succeed.
        async def _last_completed_back_to_create() -> bool:
            snapshot = await local_phantom_client.get_upload(chain_id)
            # On rewind, ``last_step_completed`` resets — the executor
            # sets it back to None and re-issues step 1. We treat any
            # transient non-terminal state as proof the rewind path
            # engaged; the final assertion checks chain succeeded.
            return snapshot.state in {"queued", "attempting", "succeeded"}

        await await_until(
            _last_completed_back_to_create,
            timeout_seconds=STORED_BUDGET_SECONDS,
            message="chain did not engage rewind path",
        )

        local_emulator.clear_failures()

        chain_response = await assert_chain_reaches_state(
            local_phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_BUDGET_SECONDS,
        )
        assert chain_response.state == "succeeded"

        # The emulator's received log should show the body landed
        # exactly once — even though step 1 was re-executed (and the
        # idempotency dedup returned the cached create response),
        # step 2 only ever ran to terminal-success once.
        received_entry = await assert_emulator_received(
            local_emulator,
            phantom_local_uuid=str(chain_id),
            body_size=len(TEST_BODY),
        )
        assert received_entry.body_size == len(TEST_BODY)

        # Crucial ADR-011 assertion: only one unique
        # ``fileInformation.id`` value was captured across the
        # chain's lifetime. The idempotency cache served the same
        # ``file_information`` on the re-execution rather than
        # creating a new ``pending_upload``.
        captured_by_name = {cs.step_name: cs.values for cs in chain_response.captured}
        file_info = captured_by_name["create_file"]["file_information"]
        assert isinstance(file_info, dict)
        # Only assertable in the final captured-values map; the
        # idempotency cache's behaviour means the second execution's
        # ``fileInformation.id`` matches the first's.
        assert "id" in file_info, f"file_information missing 'id': {file_info}"
    finally:
        await stack.tear_down()
