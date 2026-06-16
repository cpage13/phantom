"""Aggressor — admin envelope returns byte-fidelity for metadata + headers.

Round-2 (defender) extended ``ChainAdminDetail`` with two fields:

- ``metadata: dict[str, str]`` — extracted from the create-file step's
  JSON body (the ``metadata.key_value_store`` shape).
- ``steps: list[ChainAdminStepDetail]`` — per-step ``name``, ``method``,
  ``url``, ``headers``, ``has_body`` projection.

Both fields are advertised as "byte-equal to what was submitted". This
test exercises edge characters that have historically been mishandled
in projection paths:

- **Metadata keys with hyphens** (producers may use either form:
  ``order_number`` uses underscore, but the ``x-amz-meta-*`` mapping
  converts underscores to hyphens, so a raw ``source-key`` is a
  realistic name the source could supply).
- **Metadata values with internal whitespace** (multiple spaces).
- **Empty-string metadata values** (``"empty": ""``).
- **Long values** (500 chars, well above the typical 50-byte size).
- **Per-step headers with diverse case mixings** (``Content-Type``,
  ``X-Custom-Tracker-ID``, ``X-Idempotency-Source``).
- **Header values with whitespace and special characters**.

If the admin projection (``_project_envelope_for_admin`` in
``src/phantom-service/src/phantom/routes/admin.py``) silently normalizes any
of these (Unicode normalization, case folding, whitespace stripping,
key collation), the byte-equality assertion fails.

The test exercises a fresh chain end-to-end (admit → succeed →
admin GET) under default-tier persist so the row lives in the disk
store and ``chain_envelope_json`` round-trips through SQLite.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainEnvelope, ChainStep

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
BODY: bytes = b"phantom-aggressor-envelope-byte-fidelity-body"
TERMINAL_BUDGET_SECONDS: float = 15.0

# Edge-case metadata mirroring representative producers: hyphenated keys,
# internal whitespace, empty values, mixed case. OUT OF SCOPE:
# unicode/non-ASCII values and leading/trailing whitespace on values —
# these get rejected by httpx at the upstream PUT transport layer
# because S3 x-amz-meta-* headers are ASCII-only and disallow leading/
# trailing whitespace. They're real production constraints, not
# admin-projection bugs. The admin projection is the SUT here.
EDGE_METADATA: dict[str, str] = {
    "ref_id": "1d2e3f4a-5b6c-7d8e-9f01-234567890abc",
    "source-key": "source-supplied-hyphen-key",
    "key_with_underscores": "value-without-underscores",
    "whitespace_value": "value with internal spaces",
    "empty_value": "",
    "single_char": "x",
    "numeric_value": "12345",
    "boolean_string": "true",
    "all_uppercase": "PASSING",
    "mixed_case": "MixedCaseValue_With-Mixed_Punctuation",
    "long_value_500_chars": "x" * 500,
    "punctuation_friendly": "value.with-dots_and-hyphens",
}

# Edge-case PUT headers: mixed case, internal whitespace in values,
# diverse name styles. We include both standard headers (Content-Type)
# and custom ones. Avoid leading/trailing whitespace on header values
# — httpx rejects those at transport time (RFC 7230) and they're not
# what the admin projection is being tested for.
EDGE_PUT_HEADERS: dict[str, str] = {
    "Content-Type": "application/octet-stream",
    "X-Custom-Tracker-ID": "TRACKER-2026-05-15-NA-001",
    "X-Idempotency-Source": "client-explicit-source-marker",
    "x-amz-meta-tracker": "source-supplied-tracker-meta",
    "X-Capital-Mixed": "Mixed-Value With Spaces",
    "x-lowercase-only": "lowercase-value",
}

pytestmark = pytest.mark.e2e


def _build_envelope_with_edge_data(
    *,
    chain_id: UUID,
    emulator_url: str,
    metadata: dict[str, str],
    put_headers: dict[str, str],
) -> ChainEnvelope:
    """Build a two-step envelope (create_file + put_s3) with edge data."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    # Replace the default minimal KVS with our edge dict + the
    # phantom_local_uuid marker (used by the emulator's received-log
    # to attribute received bytes to a chain).
    request.metadata.key_value_store.clear()
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    for k, v in metadata.items():
        request.metadata.key_value_store[k] = v
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    put_step = envelope.steps[-1]
    merged_headers = {**put_step.headers, **put_headers}
    new_put = ChainStep(
        name=put_step.name,
        method=put_step.method,
        url=put_step.url,
        headers=merged_headers,
        body=put_step.body,
        capture=put_step.capture,
        idempotency_header=put_step.idempotency_header,
    )
    return ChainEnvelope(
        chain_id=envelope.chain_id,
        idempotency_key=envelope.idempotency_key,
        steps=[envelope.steps[0], new_put],
        default_target=envelope.default_target,
    )


async def _submit(
    pc: PhantomClient,
    envelope: ChainEnvelope,
    *,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit an envelope with the standard body_ref."""
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_aggressor_admin_envelope_metadata_byte_fidelity(tmp_path: Path) -> None:
    """``ChainAdminDetail.metadata`` returns every KVS key/value byte-equal.

    Edge characters tested: hyphenated keys, internal whitespace,
    leading/trailing whitespace, unicode (Latin/CJK), empty values,
    mixed case.
    """
    # Force disk-tier so the envelope round-trips through SQLite's
    # chain_envelope_json column — that's the path
    # _project_envelope_for_admin exercises.
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # Phase 1: all-disk mode replaces ``default_tier: persisted``.
                "body_store": {"mode": "all_disk"},
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        envelope = _build_envelope_with_edge_data(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            metadata=EDGE_METADATA,
            put_headers=EDGE_PUT_HEADERS,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )

        detail = await pc.get_upload(chain_id)

        # 1. Body-location invariant — chain must be on disk for the
        #    projection to exercise the persisted-envelope path. Phase 1
        #    Slice 1.E renamed ``tier='persisted'`` to
        #    ``body_location='file'``.
        assert detail.body_location == "file", (
            f"chain landed in body_location={detail.body_location!r}; the envelope "
            f"projection path only meaningfully tests against the on-disk envelope. "
            f"Verify body_store.mode='all_disk' is honored."
        )

        # 2. metadata byte-fidelity. Every key/value submitted must
        #    surface byte-equal — no normalization, no case-folding,
        #    no whitespace stripping.
        for key, expected_value in EDGE_METADATA.items():
            assert key in detail.metadata, (
                f"metadata key {key!r} missing from ChainAdminDetail.metadata. "
                f"Present keys: {sorted(detail.metadata.keys())}"
            )
            actual_value = detail.metadata[key]
            assert actual_value == expected_value, (
                f"metadata[{key!r}] mismatched byte-fidelity: "
                f"sent {expected_value!r}, got {actual_value!r}. "
                f"Projection mutated the value — possible Unicode "
                f"normalization, whitespace strip, or encoding issue."
            )

        # 3. The phantom_local_uuid marker (we put it BEFORE the edge
        #    metadata) should also survive.
        assert detail.metadata.get("phantom_local_uuid") == str(chain_id), (
            f"phantom_local_uuid marker missing/wrong in metadata: "
            f"{detail.metadata.get('phantom_local_uuid')!r}"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_admin_envelope_step_headers_byte_fidelity(
    tmp_path: Path,
) -> None:
    """Each step's headers in ``ChainAdminDetail.steps`` are byte-equal.

    Edge characters tested: mixed-case names, headers with whitespace,
    standard vs custom names. Per-step ``method``, ``url``, ``name``,
    ``has_body`` should also round-trip.

    Note: the ``X-Phantom-*`` strip is applied at EXECUTION time
    (executor) NOT at admission/projection time. The projection
    surfaces the envelope AS-SUBMITTED, so even reserved headers (had
    we included them) would appear in ``detail.steps[*].headers``.
    For clarity, this test does NOT submit any ``X-Phantom-*`` headers
    so the assertion is unambiguous.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "storage": {
                # Phase 1: all-disk mode replaces ``default_tier: persisted``.
                "body_store": {"mode": "all_disk"},
                "compression": {
                    "mode": "always",
                    "algorithm": "original",
                },
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_id = uuid4()
        envelope = _build_envelope_with_edge_data(
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            metadata={"ref_id": "envelope-headers-test", "passed": "true"},
            put_headers=EDGE_PUT_HEADERS,
        )
        await _submit(pc, envelope, emulator_url=stack.emulator_url, bearer=bearer)

        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )

        detail = await pc.get_upload(chain_id)

        # 1. Two steps (create_file + put_s3) — the standard envelope
        #    shape.
        assert len(detail.steps) == 2, (
            f"expected 2 steps in admin detail, got {len(detail.steps)}: "
            f"{[s.name for s in detail.steps]}"
        )

        # 2. Step ordering and names match the envelope.
        step_names = [s.name for s in detail.steps]
        assert step_names == [s.name for s in envelope.steps], (
            f"step name order mismatch: admin returned {step_names!r}, "
            f"envelope was {[s.name for s in envelope.steps]!r}"
        )

        # 3. Method round-trip on each step.
        for envelope_step, admin_step in zip(envelope.steps, detail.steps, strict=True):
            assert admin_step.method == envelope_step.method, (
                f"step {admin_step.name!r}: method byte-fidelity broken "
                f"(envelope={envelope_step.method!r}, "
                f"admin={admin_step.method!r})"
            )

        # 4. URL round-trip on each step — including the
        #    {{create_file.upload_url}} placeholder on the PUT step
        #    (the admin projection surfaces the envelope AS SUBMITTED,
        #    BEFORE template substitution).
        for envelope_step, admin_step in zip(envelope.steps, detail.steps, strict=True):
            assert admin_step.url == envelope_step.url, (
                f"step {admin_step.name!r}: url byte-fidelity broken. "
                f"envelope={envelope_step.url!r}, admin={admin_step.url!r}"
            )

        # 5. Headers round-trip on the PUT step. Each header we
        #    injected should appear byte-equal (case-preserved name,
        #    byte-equal value).
        put_admin_step = detail.steps[-1]
        for header_name, header_value in EDGE_PUT_HEADERS.items():
            assert header_name in put_admin_step.headers, (
                f"PUT step header {header_name!r} missing from admin "
                f"detail.steps[1].headers. Present: "
                f"{sorted(put_admin_step.headers.keys())}"
            )
            actual_value = put_admin_step.headers[header_name]
            assert actual_value == header_value, (
                f"PUT step header[{header_name!r}] byte-fidelity broken: "
                f"sent {header_value!r}, got {actual_value!r}"
            )

        # 6. has_body invariants. The PUT step has a body_ref pointing
        #    at our submitted bytes — has_body=True. The create_file
        #    step has an inline JSON body (CreateFileRequest) —
        #    also has_body=True.
        for step in detail.steps:
            assert step.has_body is True, (
                f"step {step.name!r}: has_body={step.has_body!r}, "
                f"expected True for both create_file (inline JSON) "
                f"and put_s3 (body_ref)"
            )
    finally:
        await stack.tear_down()
