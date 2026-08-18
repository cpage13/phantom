"""F14: an inverted retention window is refused, and the service keeps running.

The retention floor is the invariant that a body must never outlive the
row that owns it. It was checked at exactly one door, the lifespan's
boot-time guard block, and ``apply_reload`` never ran it. ``RetentionCfg``
carries no cross-field validator, so pydantic accepts an inverted pair,
and the reaper reads retention from the live snapshot on every sweep. A
reload therefore installed the very window the boot guard exists to
reject, and the next sweep ran against it: the metadata pass deletes rows
without touching bodies, so the rows went first and their RAM bytes
became unreclaimable, ``RamBodyStore.list_orphans`` returning ``[]`` by
design on the premise that a RAM ``chain_id`` always has a row.

The load-bearing half of this test is the RECOVERY leg. It is not enough
that a bad reload is refused; the operator must be able to fix the YAML
and reload again on the same process, and no delivery may be lost to the
refusal.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest

from tests.e2e._driver import build_in_memory_upload_envelope

from ..helpers.assertions import assert_chain_reaches_state
from ..helpers.payloads import build_create_file_request
from ..helpers.stack import E2EStack, boot_stack

# The suite's fake security token carries this ``sub`` claim.
_DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

_BODY: bytes = b"phantom-f14-retention-floor"

_TERMINAL_BUDGET_SECONDS: float = 15.0

# The violating pair: a succeeded body kept ten times longer than the row
# that owns it.
_BAD_METADATA_SECONDS: int = 60
_BAD_BODY_SECONDS: int = 600

# A coherent replacement, both finite and body <= metadata.
_GOOD_METADATA_SECONDS: int = 900
_GOOD_BODY_SECONDS: int = 120


async def _submit_one(stack: E2EStack, bearer: str) -> UUID:
    """Submit one upload-shaped chain at the emulator and return its id."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"f14_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": _BODY},
        uid=_DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    return chain_id


@pytest.mark.e2e
async def test_inverted_retention_reload_is_refused_and_the_service_keeps_running(
    tmp_path: Path,
) -> None:
    """A floor-violating reload answers 422, and the next valid one works.

    Objective: prove the config door end to end on the surface an
    operator drives. Success has three parts:

    1. ``POST /v1/admin/reload`` on an inverted ``succeeded`` pair
       answers 422 with ``error.code == "envelope_invalid"``. Pre-fix it
       answers 200 and the inverted window goes live.
    2. Rewriting to a coherent pair and reloading again answers 200. A
       refused reload must leave the process healthy rather than wedged,
       or the operator is stuck with a restart as their only recourse.
    3. A chain submitted afterwards still reaches ``succeeded``, so the
       refused reload cost no delivery.

    ``rewrite_yaml`` is correct here where the F5 e2e needed the surgical
    form: ``retention`` is a top-level MAPPING, which the helper
    deep-merges key by key, rather than a list it would replace whole.
    """
    stack = await boot_stack(tmp_path=tmp_path, enable_hot_reload=True)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        first = await _submit_one(stack, bearer)
        await assert_chain_reaches_state(
            stack.phantom_client,
            first,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )

        stack.rewrite_yaml(
            {
                "retention": {
                    "succeeded_metadata_seconds": _BAD_METADATA_SECONDS,
                    "succeeded_body_seconds": _BAD_BODY_SECONDS,
                }
            }
        )
        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            refused = await client.post("/v1/admin/reload")
        assert refused.status_code == 422, (
            "an inverted retention window is INVALID config, not a "
            f"restart-required block; the reload answered {refused.status_code}: "
            f"{refused.text!r}"
        )
        assert refused.json()["error"]["code"] == "envelope_invalid"

        stack.rewrite_yaml(
            {
                "retention": {
                    "succeeded_metadata_seconds": _GOOD_METADATA_SECONDS,
                    "succeeded_body_seconds": _GOOD_BODY_SECONDS,
                }
            }
        )
        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            accepted = await client.post("/v1/admin/reload")
        assert accepted.status_code == 200, (
            "a refused reload must leave the process able to accept the next "
            f"valid one; got {accepted.status_code}: {accepted.text!r}"
        )

        second = await _submit_one(stack, bearer)
        await assert_chain_reaches_state(
            stack.phantom_client,
            second,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )
    finally:
        await stack.tear_down()
