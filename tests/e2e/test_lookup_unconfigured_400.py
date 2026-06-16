"""The unconfigured-instance 400 envelope for the by-captured-id lookup, over the wire.

Cycle-7 plan 06_09 task 7.1(b), the leg ``test_sdk_group_and_lookups.py``
cannot reach: that module runs against the suite's default single
instance, which carries the ``admin_lookup`` binding. This module boots
a two-instance topology where ``alpha`` is configured and ``beta`` is
not, and pins the refusal posture end to end:

* scoped at the unconfigured instance: HTTP 400 with the canonical
  ``lookup_not_configured`` envelope (code, message, instance_id,
  request_id, details.unconfigured_instances), asserted on the raw
  JSON AND through the SDK's typed ``PhantomBadRequestError``;
* fan-out (no ``?instance=``): refuses because ANY targeted instance
  lacks the binding, naming exactly the unconfigured one(s) rather
  than silently narrowing the search;
* scoped at the configured instance: the lookup works (the 400 is the
  posture of the unconfigured instance, not a broken surface);
* the by-local-uuid asymmetry: that lookup needs NO per-instance
  configuration and answers for the unconfigured instance's rows.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_client.errors import PhantomBadRequestError
from phantom_emulator.auth.modes import AuthMode

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

pytestmark = pytest.mark.e2e

# Two instances: alpha carries the admin_lookup binding, beta does not.
CONFIGURED_INSTANCE: str = "alpha"
UNCONFIGURED_INSTANCE: str = "beta"

_SHARED_SUB: str = "00000000-0000-0000-0000-0000000000e0"

# Body for the two probe uploads; content is irrelevant to the lookup.
_PROBE_BODY: bytes = b"lookup-unconfigured-400-probe-body"

# Budget for one upload to deliver on the healthy in-process stack.
TERMINAL_BUDGET_SECONDS: float = 30.0

# HTTP budget for the raw envelope probe (loopback; generous backstop).
RAW_PROBE_TIMEOUT_SECONDS: float = 10.0


def _overrides() -> dict[str, object]:
    """``config_overrides``: alpha with the lookup binding, beta without.

    Both instances route to the suite's primary emulator (hosts overlap
    on loopback; submissions force the instance via the
    ``X-Phantom-Instance`` header, the established
    multi-instance-under-one-loopback pattern).
    """
    return {
        "instances": [
            {
                "id": CONFIGURED_INSTANCE,
                "host_prefixes": [f"emulator-{CONFIGURED_INSTANCE}"],
                "data_dir": CONFIGURED_INSTANCE,
                "capture_reexecution": False,
                "admin_lookup": {
                    "capture_name": "create_file",
                    "json_path": "file_information.id",
                },
                "routes": [
                    {
                        "name": f"{CONFIGURED_INSTANCE}_route",
                        "hosts": [f"emulator-{CONFIGURED_INSTANCE}", "127.0.0.1", "localhost"],
                        "auth_mode": "none",
                    },
                ],
            },
            {
                "id": UNCONFIGURED_INSTANCE,
                "host_prefixes": [f"emulator-{UNCONFIGURED_INSTANCE}"],
                "data_dir": UNCONFIGURED_INSTANCE,
                "capture_reexecution": False,
                # Deliberately NO admin_lookup block: this instance is
                # the unconfigured one the 400 posture protects.
                "routes": [
                    {
                        "name": f"{UNCONFIGURED_INSTANCE}_route",
                        "hosts": [f"emulator-{UNCONFIGURED_INSTANCE}", "127.0.0.1", "localhost"],
                        "auth_mode": "none",
                    },
                ],
            },
        ],
    }


async def _submit_to_instance(
    stack: E2EStack,
    *,
    instance_id: str,
    file_name: str,
) -> UUID:
    """Submit one upload routed to ``instance_id``; return its chain id.

    Mirrors the driver's pre-flight: the minted ``local_uuid`` rides
    both as the chain id and under the pinned ``phantom_local_uuid``
    metadata key, so the by-local-uuid lookup can hit the row.
    """
    local_uuid = uuid4()
    request = build_create_file_request(file_name=file_name)
    request.metadata.key_value_store["phantom_local_uuid"] = str(local_uuid)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=local_uuid,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": _PROBE_BODY},
        uid=_SHARED_SUB,
        auth_token=f"Bearer {stack.fake_security_token(sub=_SHARED_SUB)}",
        options=SubmitOptions(instance_id=instance_id),  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
    )
    return local_uuid


async def test_unconfigured_instance_refuses_with_the_400_envelope() -> None:
    """Every refusal leg of the unconfigured-instance posture, over the wire."""
    stack = await boot_stack(config_overrides=_overrides())
    try:
        pc: PhantomClient = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        stack.emulator.set_auth_mode(AuthMode.NONE)

        # One delivered upload per instance so both scopes hold real rows.
        alpha_chain = await _submit_to_instance(
            stack, instance_id=CONFIGURED_INSTANCE, file_name="lookup-alpha.bin"
        )
        beta_chain = await _submit_to_instance(
            stack, instance_id=UNCONFIGURED_INSTANCE, file_name="lookup-beta.bin"
        )
        await assert_chain_reaches_state(
            pc, alpha_chain, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        await assert_chain_reaches_state(
            pc, beta_chain, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )

        # The configured instance's binding works: pull the upstream-
        # assigned id off the captured values and resolve it back.
        detail = await pc.get_upload(alpha_chain)
        captured_by_step = {step.step_name: step.values for step in detail.captured}
        upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]
        assert isinstance(upstream_file_id, str)
        scoped_hit = await pc.find_by_captured_id(upstream_file_id, instance=CONFIGURED_INSTANCE)
        assert scoped_hit.found is True
        assert [m.chain_id for m in scoped_hit.matches] == [alpha_chain]

        # Scoped at the unconfigured instance: typed 400 through the SDK.
        with pytest.raises(PhantomBadRequestError) as scoped_exc:
            await pc.find_by_captured_id(upstream_file_id, instance=UNCONFIGURED_INSTANCE)
        assert scoped_exc.value.error_code == "lookup_not_configured"
        assert scoped_exc.value.status_code == httpx.codes.BAD_REQUEST
        assert scoped_exc.value.details == {"unconfigured_instances": [UNCONFIGURED_INSTANCE]}

        # Fan-out: refuses because ANY targeted instance lacks the
        # binding, naming exactly the unconfigured one (never silently
        # narrowing to the configured instance and answering found=false
        # about a scope it skipped).
        with pytest.raises(PhantomBadRequestError) as fanout_exc:
            await pc.find_by_captured_id(upstream_file_id)
        assert fanout_exc.value.error_code == "lookup_not_configured"
        unconfigured = fanout_exc.value.details["unconfigured_instances"]
        assert unconfigured == [UNCONFIGURED_INSTANCE]
        assert CONFIGURED_INSTANCE not in unconfigured

        # The raw canonical envelope, asserted off the wire bytes: the
        # {"error": {...}} wrapper with the stable code, an attribution
        # instance_id, a correlation request_id, and the details list.
        async with httpx.AsyncClient(timeout=RAW_PROBE_TIMEOUT_SECONDS) as raw:
            response = await raw.get(
                f"{stack.phantom_url}/v1/admin/uploads/by-captured-id/{upstream_file_id}",
                params={"instance": UNCONFIGURED_INSTANCE},
            )
        assert response.status_code == httpx.codes.BAD_REQUEST
        envelope = response.json()
        assert set(envelope) == {"error"}, f"canonical wrapper violated: {envelope!r}"
        error = envelope["error"]
        assert error["code"] == "lookup_not_configured"
        assert error["instance_id"] == UNCONFIGURED_INSTANCE
        assert error["details"] == {"unconfigured_instances": [UNCONFIGURED_INSTANCE]}
        assert isinstance(error["message"], str) and error["message"]
        # Admin error envelopes carry request_id as a string field; the
        # admin tier stamps it empty today (routes/admin.py _admin_error),
        # so pin presence + type, not content.
        assert isinstance(error["request_id"], str)

        # The asymmetry: by-local-uuid needs no per-instance binding and
        # answers for the unconfigured instance's rows, fan-out included.
        by_uuid = await pc.find_by_local_uuid(beta_chain)
        assert by_uuid.found is True
        assert [m.chain_id for m in by_uuid.matches] == [beta_chain]
        assert by_uuid.matches[0].instance_id == UNCONFIGURED_INSTANCE
        assert by_uuid.matches[0].captured_file_id is None, (
            "an unconfigured instance has no binding to surface a captured id through"
        )
    finally:
        await stack.tear_down()
