"""E2E — Item A: synthetic-workflow burst.

Drives the live three-package stack under a realistic upload load
shape (counts, sizes, two-step pattern, header set, one-then-burst
cadence) using entirely synthetic, meaning-free bytes. This is
Phantom's E2E test for the canonical *upload burst* shape.

The shape facts are reproduced here as fixed values modeling a
typical single-source pass.

The bearer token reaches the emulator value-equal: the emulator is
flipped to ``PLAIN_BEARER`` with the synthetic token on its allowlist
before the test runs, so a successful 12-chain run proves the exact
token value flowed end-to-end (any other value would 401 at the
emulator's auth gate).
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from phantom_client import PhantomClient
from phantom_emulator.auth.modes import AuthMode

from ._driver import CreateFileRequest, DriverUploadResult, FileMetadata, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads_routines import deterministic_body
from .helpers.stack import E2EStack, EmulatorControl
from .helpers.timing import settle_for

# ---------------------------------------------------------------------------
# Module-level named constants — Item A's pinned values, modeling a
# realistic single-source upload burst.
# ---------------------------------------------------------------------------

# Total uploads per Item A run (source range: 10-15 for a typical
# clean single-source pass; Item A fixes the count at 12 for determinism).
UPLOAD_COUNT: int = 12

# Small-parquet bucket (source: ~9 of the 12 uploads are 1-6 KB
# gzip-compressed parquet bodies).
SMALL_PARQUET_COUNT: int = 9
SMALL_PARQUET_MIN_BYTES: int = 1024
SMALL_PARQUET_MAX_BYTES: int = 6 * 1024

# The config-JSON blob (source range 2-10 KB).
CONFIG_JSON_BYTES: int = 5 * 1024

# The mid-sized HTML report (source range tens-of-KB to a few hundred KB).
MID_HTML_BYTES: int = 30 * 1024

# The large binary parquet (representative range 50 KB - 1 MB,
# typical 100-300 KB).
LARGE_PARQUET_BYTES: int = 500 * 1024

# Source range: ~8-10 `x-amz-meta-*` headers on the PUT. Item A
# carries 9 synthetic, domain-free identifier-style keys.
XAMZ_META_KEYS: tuple[str, ...] = (
    "tag_alpha",
    "tag_beta",
    "tag_gamma",
    "tag_delta",
    "tag_epsilon",
    "tag_zeta",
    "tag_eta",
    "tag_theta",
    "tag_iota",
)

# Deliberate compression of the source workflow's minutes-scale
# work phase. Short enough to
# keep the test fast; long enough to be observable in logs. The drift
# check does NOT assert on this constant — the source doc's "minutes"
# is intentionally uncomparable to the test's 0.1 s.
MEASUREMENT_PHASE_GAP_SECONDS: float = 0.1

# Fixed bearer-token value. The test's `Authorization` header is
# `f"Bearer {SYNTHETIC_BEARER_TOKEN}"`. The emulator is flipped to
# `PLAIN_BEARER` with this exact value on its allowlist so a
# successful 12-chain run proves the value flowed through unchanged.
SYNTHETIC_BEARER_TOKEN: str = "synthetic-token-item-a"

# ---------------------------------------------------------------------------
# Test-only constants.
# ---------------------------------------------------------------------------

# uid fallback for the driver when the bearer is not a JWT. The
# synthetic token deliberately is NOT a JWT (the production upload
# carries a real bearer; for the Phantom-side wire-shape regression we
# only need a fixed bearer value).
_UID_FALLBACK: str = "item-a-test"

# Per-chain terminal-state budget. Item A's largest body is 500 KB;
# 30 s gives slow CI runners headroom while staying well under the
# default succeeded_metadata retention window.
_TERMINAL_STATE_BUDGET_SECONDS: float = 30.0

# Synthetic-value pattern for the `x-amz-meta-*` keys. Short string
# values keep the request well under S3's 2 KB user-metadata limit
# (per the source doc, total KVS size is ~150-500 bytes).
_META_VALUE_PREFIX: str = "value-"

# Filename suffix tag -> extension mapping. The naming pattern is
# `synthetic_burst_<NN>_<tag>.<ext>` where the alphabet satisfies the
# `CreateFileRequest` regex `^[a-zA-Z0-9!\\-_.*'()]+$`.
_FILENAME_PATTERN: str = "synthetic_burst_{nn:02d}_{tag}.{ext}"

# Lane base name — generic, domain-free identifier conforming to the
# CreateFileRequest regex.
_LANE_BASE_NAME: str = "synthetic_burst_lane"

# File domain — matches the existing E2E suite's default.
_DOMAIN: str = "generic"


pytestmark = pytest.mark.e2e


def _meta_kvs() -> dict[str, str]:
    """Return the 9-key synthetic metadata KVS.

    Stable across the run so every PUT carries the same 9 metadata
    headers (with deterministic value strings per key).
    """
    return {key: f"{_META_VALUE_PREFIX}{key}" for key in XAMZ_META_KEYS}


def _expected_xamz_headers(kvs: dict[str, str]) -> dict[str, str]:
    """Project the KVS to its expected ``x-amz-meta-*`` header view.

    The upstream client's presigned-URL handler substitutes ``_`` -> ``-``
    when projecting KVS keys to S3 metadata headers; the driver's
    envelope builder mirrors that substitution exactly. The expected
    PUT-side header set is therefore the KVS with underscores in keys
    replaced by hyphens and the ``x-amz-meta-`` prefix attached.
    """
    return {f"x-amz-meta-{key.replace('_', '-')}": value for key, value in kvs.items()}


def _build_upload(
    *,
    index: int,
    size_tag: str,
    extension: str,
    size: int,
    kvs: dict[str, str],
) -> tuple[CreateFileRequest, bytes]:
    """Build one (CreateFileRequest, body) pair for a single upload.

    The body is produced by ``deterministic_body`` with a per-upload
    seed so the test is replayable; the body content has no semantic
    meaning. The ``file_name`` follows the
    ``synthetic_burst_<NN>_<tag>.<ext>`` convention.
    """
    file_name = _FILENAME_PATTERN.format(nn=index, tag=size_tag, ext=extension)
    seed = f"item-a-{index:02d}-{size_tag}".encode()
    body = deterministic_body(size, seed=seed)
    request = CreateFileRequest(
        domain=_DOMAIN,
        lane_base_name=_LANE_BASE_NAME,
        file_name=file_name,
        metadata=FileMetadata(key_value_store=dict(kvs)),
    )
    return request, body


def _build_sequence() -> list[tuple[CreateFileRequest, bytes]]:
    """Build the 12-upload Item A sequence in fixed order.

    Order: 9 small parquets (1-6 KB), 1 config JSON (5 KB), 1 mid
    HTML (30 KB), 1 large parquet (500 KB). Total = 12. The order is
    deterministic so the test is replayable; the order intentionally
    does not match a specific workflow's order (Item A models
    the *shape*, not a specific call sequence).
    """
    kvs = _meta_kvs()
    sequence: list[tuple[CreateFileRequest, bytes]] = []

    # 9 small parquets. Sizes are spread across the 1-6 KB bucket so
    # the test exercises the bucket's range, not just one point.
    span = SMALL_PARQUET_MAX_BYTES - SMALL_PARQUET_MIN_BYTES
    for offset in range(SMALL_PARQUET_COUNT):
        # offset / (count - 1) gives 0.0..1.0 across the bucket; for
        # count == 1 we just use the floor.
        denom = SMALL_PARQUET_COUNT - 1 if SMALL_PARQUET_COUNT > 1 else 1
        size = SMALL_PARQUET_MIN_BYTES + (span * offset) // denom
        sequence.append(
            _build_upload(
                index=len(sequence) + 1,
                size_tag="small",
                extension="parquet",
                size=size,
                kvs=kvs,
            )
        )

    # 1 config JSON (5 KB).
    sequence.append(
        _build_upload(
            index=len(sequence) + 1,
            size_tag="config",
            extension="json",
            size=CONFIG_JSON_BYTES,
            kvs=kvs,
        )
    )

    # 1 mid HTML (30 KB).
    sequence.append(
        _build_upload(
            index=len(sequence) + 1,
            size_tag="mid",
            extension="html",
            size=MID_HTML_BYTES,
            kvs=kvs,
        )
    )

    # 1 large parquet (500 KB).
    sequence.append(
        _build_upload(
            index=len(sequence) + 1,
            size_tag="large",
            extension="parquet",
            size=LARGE_PARQUET_BYTES,
            kvs=kvs,
        )
    )

    return sequence


def _configure_synthetic_bearer_auth(emulator: EmulatorControl) -> None:
    """Flip the emulator to PLAIN_BEARER with the synthetic token allowed.

    The emulator's default is ``oauth_client_credentials`` (JWT
    verify), which would 401 the test's fixed non-JWT bearer. We:

    1. Add ``SYNTHETIC_BEARER_TOKEN`` to the emulator's plain-bearer
       allowlist (direct state mutation — the control surface
       exposes ``set_auth_mode`` but not the allowlist seeder).
    2. Flip the default auth mode to ``PLAIN_BEARER``.

    Because the allowlist contains exactly one value, a successful
    POST proves the inbound ``Authorization: Bearer <token>``
    carried the expected literal value. Any drift (wrong value,
    missing header, header stripped) would 401 at the emulator's
    auth gate and the chain would never reach ``succeeded``.
    """
    # `emulator` is the typed wrapper; reach through to the live
    # Server's state to seed the allowlist. The wrapper deliberately
    # does not expose the allowlist (no production-side use case
    # needs it), so a direct attribute access is the established
    # test-only pattern (mirrors how `stack.get_instance` returns
    # phantom-service internals for tests).
    state = emulator._server.state
    state.plain_bearer_allowlist.add(SYNTHETIC_BEARER_TOKEN)
    emulator.set_auth_mode(AuthMode.PLAIN_BEARER)


async def test_synthetic_workflow_burst(
    stack: E2EStack,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Drive 12 sequential uploads under the Item A shape; assert end-to-end.

    The cadence: one upload at the start, a 0.1 s gap (compressed
    model of the source's minutes-scale measurement phase), then the
    remaining 11 uploads back-to-back, strictly sequential (each
    ``await``-ed before the next). Each upload is the two-step
    pattern (POST metadata + presigned PUT) carrying 9
    ``x-amz-meta-*`` headers on the PUT and value-equal
    ``Authorization: Bearer synthetic-token-item-a`` on the POST.
    """
    # Setup — fresh emulator state; flip to PLAIN_BEARER with the
    # synthetic token on the allowlist.
    emulator.clear_received()
    emulator.clear_failures()
    _configure_synthetic_bearer_auth(emulator)

    # Build the 12-upload sequence and verify its shape before
    # submission (defends against a builder regression that would
    # otherwise silently change what the test exercises).
    sequence = _build_sequence()
    assert len(sequence) == UPLOAD_COUNT, (
        f"sequence builder emitted {len(sequence)} uploads; expected {UPLOAD_COUNT}"
    )

    # Construct a public driver with the synthetic bearer. The conftest's
    # `driver` fixture uses a JWT bearer (`stack.fake_security_token`)
    # that wouldn't match the PLAIN_BEARER allowlist — this test owns
    # the bearer (a non-JWT), so it constructs its own driver with the
    # ``uid_fallback`` the non-JWT bearer requires.
    driver = PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=lambda: SYNTHETIC_BEARER_TOKEN,
        uid_fallback=_UID_FALLBACK,
    )

    # Action — one upload, a measurement-phase gap, then a serial
    # burst of the remaining 11.
    file_infos: list[DriverUploadResult] = []

    first_request, first_body = sequence[0]
    first_result = await driver.in_memory_upload(first_request, first_body)
    assert isinstance(first_result.id, UUID)
    file_infos.append(first_result)

    # Deliberate compression of the source's minutes-scale gap.
    await settle_for(
        MEASUREMENT_PHASE_GAP_SECONDS,
        reason="synthetic-workflow: compress the source's measurement-phase gap",
    )

    # The remaining 11 uploads, strictly sequential.
    for request, body in sequence[1:]:
        result = await driver.in_memory_upload(request, body)
        assert isinstance(result.id, UUID)
        file_infos.append(result)

    assert len(file_infos) == UPLOAD_COUNT

    # Assertion 1 — every chain reached `succeeded` on its second step
    # (`put_s3`). If any chain's POST were sent with the wrong bearer,
    # the emulator's PLAIN_BEARER gate would have 401'd it and the
    # chain would never reach `succeeded` — so the value-equality of
    # the bearer is proved by these 12 terminal-state assertions.
    async def _await_one(fi: DriverUploadResult, label: str) -> None:
        chain_response = await assert_chain_reaches_state(
            phantom_client,
            fi.id,
            state="succeeded",
            timeout_seconds=_TERMINAL_STATE_BUDGET_SECONDS,
        )
        assert chain_response.state == "succeeded", (
            f"chain {label}: expected succeeded, got {chain_response.state!r}"
        )
        assert chain_response.last_step_completed == "put_s3", (
            f"chain {label}: expected last_step_completed=put_s3, "
            f"got {chain_response.last_step_completed!r}"
        )

    await asyncio.gather(
        *(_await_one(fi, label=f"upload-{idx + 1:02d}") for idx, fi in enumerate(file_infos))
    )

    # Assertion 2 — the emulator's received-log records 12 distinct
    # PUT pairs whose body length matches the original `bytes` length
    # and whose `x-amz-meta-*` headers carry exactly the 9 expected
    # keys (no missing, no extra) with the correct values.
    expected_kvs = _meta_kvs()
    expected_headers = _expected_xamz_headers(expected_kvs)

    for (request, body), fi in zip(sequence, file_infos, strict=True):
        received = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(fi.id),
            body_size=len(body),
        )

        # Filter phantom_local_uuid out of the received header set —
        # the driver appends it during envelope construction and the
        # emulator records it as an
        # `x-amz-meta-phantom-local-uuid` PUT header. The Item A
        # contract is about the 9 user-supplied keys; the local UUID
        # is overhead, not part of the assertion.
        received_user_headers = {
            k: v
            for k, v in received.x_amz_meta_headers.items()
            if k != "x-amz-meta-phantom-local-uuid"
        }

        # Exact-set check (no missing, no extra) plus per-key value
        # equality. The set comparison is what enforces "exactly 9
        # `x-amz-meta-*` headers" — drift in either direction fails.
        assert set(received_user_headers) == set(expected_headers), (
            f"upload {request.file_name!r}: x-amz-meta header set mismatch; "
            f"missing={set(expected_headers) - set(received_user_headers)}, "
            f"extra={set(received_user_headers) - set(expected_headers)}"
        )
        for header, expected_value in expected_headers.items():
            assert received_user_headers[header] == expected_value, (
                f"upload {request.file_name!r}: header {header!r}: "
                f"expected {expected_value!r}, got {received_user_headers[header]!r}"
            )
