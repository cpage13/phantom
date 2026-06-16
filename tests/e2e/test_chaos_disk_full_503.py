"""Operational-chaos E2E: disk at the ``max_disk_bytes`` ceiling -> clean 503 (§ 5.C).

Adversary 2, the storage-saturation chaos. Phantom's ``max_disk_bytes`` is a hard
REFUSE-not-overflow ceiling: when observed disk usage is at or above it, admission
returns a clean ``503 disk_pressure`` (+ ``Retry-After``) instead of writing a body
that would trip ENOSPC, and an already-admitted in-flight upload is never lost to
the refusal. This exercises the CONFIGURED ceiling (``SaturationGate.admit``'s
``disk_usage + declared >= max_disk_bytes`` check), NOT a literal OS ENOSPC (no
deterministic harness technique for that; F-12).

The ``DiskPressureProbe`` refreshes the gate's cached usage every 30 s from
``FileBodyStore.total_bytes``, which is too slow and too non-deterministic for an
E2E; the gate exposes ``set_disk_usage_bytes`` (the exact setter the probe calls),
so the test drives the observation directly via the live ``InstanceContext`` and
asserts the admission decision deterministically.

* :func:`test_disk_at_ceiling_refuses_clean_503` - with the gauge pinned at the
  ceiling, a fresh ``POST /v1/send`` is refused with ``503 disk_pressure`` +
  ``Retry-After``; nothing is written for the refused chain (no overflow); when
  the gauge drops back, a later submit is admitted (the refusal is transient).
* :func:`test_in_flight_upload_survives_disk_pressure_refusal` - an upload admitted
  BEFORE the gauge rises still delivers to completion while a concurrent fresh
  submit is refused: disk pressure refuses NEW admissions, it does not drop
  already-buffered work (no in-flight loss).

Public e2e-light lane (§ 5.0): generic ``submit`` shapes + the emulator.

Falsifier: change the ceiling check to admit-and-overflow (or drop the
``disk_usage >= max`` guard) -> the at-ceiling submit is admitted (200/202), no
``503 disk_pressure`` fires -> RED.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from phantom_client import ChainResponse, PhantomClient, PhantomUnavailableError

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

pytestmark = pytest.mark.e2e

_DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"
# A small, fixed disk ceiling so the test math is auditable from the source. Any
# real body's declared size plus the pinned usage clears it.
_MAX_DISK_BYTES = 1_000_000
# Usage pinned AT the ceiling -> any further admission is refused.
_USAGE_AT_CEILING = _MAX_DISK_BYTES
# Usage well below the ceiling -> admissions flow again.
_USAGE_CLEAR = 0
_BODY = b"phantom-disk-pressure-chaos-body"
_TERMINAL_BUDGET_SECONDS = 15.0


async def _submit(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID | None = None,
) -> tuple[UUID, ChainResponse]:
    """Submit one upload-shaped two-step chain; return ``(chain_id, response)``."""
    chain_id = chain_id or uuid4()
    request = build_create_file_request(file_name=f"disk_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    response = await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=_DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id, response


def _pin_disk_usage(stack: E2EStack, value: int) -> None:
    """Set the live instance's cached disk-usage observation (what the probe sets)."""
    instance: Any = stack.get_instance("primary")
    instance.saturation.set_disk_usage_bytes(value)


async def test_disk_at_ceiling_refuses_clean_503(tmp_path: Path) -> None:
    """At the ceiling, a fresh submit gets 503 disk_pressure + Retry-After; clears after."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"saturation": {"max_disk_bytes": _MAX_DISK_BYTES}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Pin observed usage AT the ceiling: the next admission must refuse.
        _pin_disk_usage(stack, _USAGE_AT_CEILING)
        refused_chain_id = uuid4()
        with pytest.raises(PhantomUnavailableError) as excinfo:
            await _submit(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=_BODY,
                chain_id=refused_chain_id,
            )
        exc = excinfo.value
        assert exc.error_code == "disk_pressure", f"expected disk_pressure, got {exc.error_code!r}"
        assert exc.status_code == 503
        retry_after = exc.response_headers.get("retry-after") or exc.response_headers.get(
            "Retry-After"
        )
        assert retry_after is not None, (
            f"503 disk_pressure missing Retry-After; headers={dict(exc.response_headers)!r}"
        )

        # No overflow: the refused chain was never persisted (no body file, no row).
        assert not stack.body_path(refused_chain_id, "body").exists(), (
            "a disk-pressure-refused chain must not have written a body (overflow)"
        )
        rows, _ = await stack.phantom_client.list_uploads(limit=100)
        assert all(row.chain_id != refused_chain_id for row in rows), (
            "a disk-pressure-refused chain must not have an admission row"
        )

        # The refusal is transient: when usage drops, a fresh submit is admitted
        # and delivers end-to-end.
        _pin_disk_usage(stack, _USAGE_CLEAR)
        admitted_id, _ = await _submit(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            body=_BODY,
        )
        admitted = await assert_chain_reaches_state(
            stack.phantom_client,
            admitted_id,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )
        assert admitted.state == "succeeded"
    finally:
        await stack.tear_down()


async def test_in_flight_upload_survives_disk_pressure_refusal(tmp_path: Path) -> None:
    """An already-admitted upload still delivers while a fresh one is refused.

    Disk pressure refuses NEW admissions; it must not drop work already buffered.
    Admit one upload with the gauge clear, THEN raise the gauge to the ceiling and
    submit a second one (refused). The first still reaches ``succeeded``.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"saturation": {"max_disk_bytes": _MAX_DISK_BYTES}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # 1. Admit the in-flight upload with the gauge BELOW the ceiling.
        _pin_disk_usage(stack, _USAGE_CLEAR)
        in_flight_id, _ = await _submit(
            stack.phantom_client,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            body=_BODY,
        )

        # 2. Raise the gauge to the ceiling and prove a fresh submit is refused.
        _pin_disk_usage(stack, _USAGE_AT_CEILING)
        with pytest.raises(PhantomUnavailableError) as excinfo:
            await _submit(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=_BODY,
            )
        assert excinfo.value.error_code == "disk_pressure"

        # 3. The already-admitted upload is NOT lost - it still delivers.
        delivered = await assert_chain_reaches_state(
            stack.phantom_client,
            in_flight_id,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )
        assert delivered.state == "succeeded"
    finally:
        await stack.tear_down()
