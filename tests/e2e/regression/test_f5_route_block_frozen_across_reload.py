"""F5 via D1: a reloaded route block changes nothing and delivery continues.

Before this fix ``apply_reload`` repointed every live
``InstanceContext.cfg`` at the freshly-loaded ``InstanceCfg``, so the
dispatcher, admission, both kickers and the admin status surfaces all
followed a reload while the executor kept the boot table. Editing
``host_prefixes`` or a route's ``auth_mode`` therefore split the readers'
view: the operator's change reached everything except the send path.

This regression drives the whole stack through the real admin reload
endpoint. It asserts the reload SUCCEEDS (a restart-required block is
refused, not an error), that the operator-visible status surface still
reports the boot prefixes (which is truthful, because the boot list is
what dispatch uses), and that delivery keeps working off the boot table.

The rewritten YAML really does validate: no validator ties
``auth_mode: aws_sigv4`` to the presence of a ``sigv4_credentials``
entry, so the 200 is reachable rather than an accident of a lenient
handler.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import yaml

from tests.e2e._driver import build_in_memory_upload_envelope

from ..helpers.assertions import assert_chain_reaches_state
from ..helpers.payloads import build_create_file_request
from ..helpers.stack import E2EStack, boot_stack

# The suite's fake security token carries this ``sub`` claim.
_DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# These tests assert on dispatch and delivery, not throughput.
_BODY: bytes = b"phantom-f5-frozen-route-block"

# A healthy in-process stack delivers a one-body chain well inside this.
_TERMINAL_BUDGET_SECONDS: float = 15.0

# A host prefix that matches NOTHING the emulator ever surfaces at, so
# the pre-fix dispatcher really does lose the instance.
_UNMATCHING_HOST_PREFIX: str = "no-such-host.invalid"


def _rewrite_frozen_blocks(stack: E2EStack) -> None:
    """Surgically drift ``host_prefixes`` and the route's ``auth_mode``.

    ``E2EStack.rewrite_yaml`` deep-merges mappings but replaces
    non-mappings wholesale, lists included. ``instances`` is a list, so a
    merge would drop the required ``id``, ``data_dir`` and ``routes``
    keys and the reload would answer 422 instead of the 200 this test
    asserts. Read, mutate ``instances[0]`` in place, write back.
    """
    assert stack.settings_path is not None, "enable_hot_reload=True required"
    raw: dict[str, Any] = yaml.safe_load(stack.settings_path.read_text())
    first_instance: dict[str, Any] = raw["instances"][0]
    first_instance["host_prefixes"] = [_UNMATCHING_HOST_PREFIX]
    first_instance["routes"][0]["auth_mode"] = "aws_sigv4"
    stack.settings_path.write_text(yaml.safe_dump(raw))


async def _submit_one(stack: E2EStack, bearer: str) -> UUID:
    """Submit one upload-shaped chain at the emulator and return its id."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"f5_{chain_id.hex[:12]}")
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
async def test_reloaded_route_block_changes_nothing_and_the_service_keeps_delivering(
    tmp_path: Path,
) -> None:
    """A reload that edits the frozen block is refused, and delivery survives.

    Objective: prove D1 end to end on the surfaces an operator touches.
    Success has three parts, in order of what fails first pre-fix:

    1. ``GET /v1/admin/status`` still reports the BOOT ``host_prefixes``
       after a reload that rewrote them. Pre-fix it reports the new list,
       which is a lie: dispatch still uses whatever the readers hold.
    2. The reload itself returns 200. A restart-required block is
       refused, not an error, so the operator's other edits still land.
    3. A second chain to the same host still reaches ``succeeded``. This
       is the load-bearing assertion: dispatch, admission, the executor
       and the bearer injection all resolve the boot table. Pre-fix the
       submission is rejected ``421 invalid_target``, because the
       dispatcher lost the prefix.
    """
    stack = await boot_stack(tmp_path=tmp_path, enable_hot_reload=True)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            boot_status = await client.get("/v1/admin/status")
        assert boot_status.status_code == 200, (
            f"admin status unavailable at boot: {boot_status.text!r}"
        )
        boot_prefixes = _prefixes_for(boot_status.json(), "primary")
        assert _UNMATCHING_HOST_PREFIX not in boot_prefixes

        # Prove the boot path first, so a later failure cannot be a
        # pre-existing delivery problem.
        first = await _submit_one(stack, bearer)
        await assert_chain_reaches_state(
            stack.phantom_client,
            first,
            state="succeeded",
            timeout_seconds=_TERMINAL_BUDGET_SECONDS,
        )

        _rewrite_frozen_blocks(stack)
        async with httpx.AsyncClient(base_url=stack.phantom_url) as client:
            reload_resp = await client.post("/v1/admin/reload")
            after_status = await client.get("/v1/admin/status")

        assert reload_resp.status_code == 200, (
            "a restart-required block must be REFUSED, not rejected; the "
            f"reload answered {reload_resp.status_code}: {reload_resp.text!r}"
        )
        assert after_status.status_code == 200
        assert _prefixes_for(after_status.json(), "primary") == boot_prefixes, (
            "admin status must report the BOOT host_prefixes after a reload "
            "that edited them, because the boot list is what dispatch uses"
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


def _prefixes_for(status_body: dict[str, Any], instance_id: str) -> list[str]:
    """Pull one instance's reported ``host_prefixes`` out of the status body."""
    for entry in status_body["instances"]:
        if entry["id"] == instance_id:
            prefixes: list[str] = list(entry["host_prefixes"])
            return prefixes
    raise AssertionError(f"instance {instance_id!r} absent from admin status")
