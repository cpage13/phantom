"""Mixed-health admin truth: a quarantined-fresh instance beside a healthy one.

Round 2 adversary seed (multi-instance partial-degrade). The TRUE
degraded-boot-beside-healthy axis (read-only substrate, no working
store) is already pinned by
``test_chaos_degraded_boot_readonly_datadir.py::test_multi_instance_one_degraded_one_healthy_isolation``:
health names only the hurt instance, ready flips false, the degraded
``/send`` 500s while the healthy one 202s. The corner that remained
untouched is the OTHER recoverable-fault posture beside a healthy
sibling: an instance whose uploads DB is corrupted at boot QUARANTINES
and serves fresh (ADR-025), and per ADR-017's "DB quarantine NOT in the
matrix" section that event deliberately does NOT degrade ``/health``
(its operator surfaces are the boot ERROR log, the quarantine counter,
and the inventory). This module pins that posture in a TWO-instance
process on the cycle-7 surfaces:

* ``/health`` stays ``storage: ok`` and ``/ready`` stays ready: the
  quarantine-and-serve-fresh boot is a working boot by design.
* The healthy instance admits, delivers, and resolves by-captured-id
  through the fan-out with the freshly-quarantined sibling configured
  beside it (no blend, no error).
* The quarantined instance answers admin reads honestly off its fresh
  store: a scoped lookup is a clean ``found=false``, never an error.
* The corrupted DB survives as a manifested ``corrupted``-reason
  quarantine entry in that instance's inventory (quarantine-not-delete,
  keyed by ``backup_id``).
* The quarantined instance still buffers and delivers NEW traffic and
  its fresh captures resolve (serve-fresh is not serve-nothing).
"""

from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.auth.modes import AuthMode

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

pytestmark = pytest.mark.e2e

HEALTHY_INSTANCE: str = "alpha"
QUARANTINED_INSTANCE: str = "beta"

_SHARED_SUB: str = "00000000-0000-0000-0000-0000000000e2"

# Body for the probe uploads; content is irrelevant to the truths here.
_PROBE_BODY: bytes = b"quarantined-beside-healthy-probe-body"

# Budget for one upload to deliver on the healthy in-process stack.
_TERMINAL_BUDGET_SECONDS: float = 30.0

# HTTP budget for the raw health/ready probes (loopback backstop).
_RAW_PROBE_TIMEOUT_SECONDS: float = 10.0

# Garbage bytes planted as the quarantined instance's uploads.db;
# anything that is not a SQLite file trips the boot integrity gate.
_GARBAGE_DB_BYTES: bytes = b"PHANTOM-R2-NOT-A-SQLITE-FILE" * 64

# The per-instance admin_lookup binding (the suite's standard shape).
_LOOKUP_BINDING: dict[str, str] = {
    "capture_name": "create_file",
    "json_path": "file_information.id",
}


def _two_instance_overrides() -> dict[str, object]:
    """Two instances, BOTH carrying the lookup binding, auth_mode none.

    Hosts overlap on loopback; submissions force the instance via
    ``SubmitOptions.instance_id`` (the established multi-instance-under-
    one-loopback pattern).
    """

    def _instance(instance_id: str) -> dict[str, object]:
        return {
            "id": instance_id,
            "host_prefixes": [f"emulator-{instance_id}"],
            "data_dir": instance_id,
            "capture_reexecution": False,
            "admin_lookup": dict(_LOOKUP_BINDING),
            "routes": [
                {
                    "name": f"{instance_id}_route",
                    "hosts": [f"emulator-{instance_id}", "127.0.0.1", "localhost"],
                    "auth_mode": "none",
                },
            ],
        }

    return {"instances": [_instance(HEALTHY_INSTANCE), _instance(QUARANTINED_INSTANCE)]}


async def _submit_to_instance(
    stack: E2EStack,
    *,
    instance_id: str,
    file_name: str,
) -> UUID:
    """Submit one upload routed to ``instance_id``; return its chain id."""
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


async def _captured_file_id(pc: PhantomClient, chain_id: UUID) -> str:
    """Pull the upstream-assigned id off a delivered chain's captures."""
    detail = await pc.get_upload(chain_id)
    captured_by_step = {step.step_name: step.values for step in detail.captured}
    upstream_file_id = captured_by_step["create_file"]["file_information"]["id"]
    assert isinstance(upstream_file_id, str)
    return upstream_file_id


async def test_quarantined_fresh_instance_beside_healthy_tells_the_truth(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """One corrupted-at-boot instance quarantines; its sibling stays whole."""
    data_root = tmp_path_factory.mktemp("quarantined-beside-healthy")
    # Plant the corruption BEFORE boot: beta's uploads.db is garbage.
    beta_root = data_root / QUARANTINED_INSTANCE
    beta_root.mkdir(parents=True)
    (beta_root / "uploads.db").write_bytes(_GARBAGE_DB_BYTES)

    stack = await boot_stack(tmp_path=data_root, config_overrides=_two_instance_overrides())
    try:
        pc: PhantomClient = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        stack.emulator.set_auth_mode(AuthMode.NONE)

        # 1. Quarantine-and-serve-fresh is a WORKING boot by design
        #    (ADR-025 + ADR-017's DB-quarantine section): the process
        #    probes stay green; the event's surfaces are the log, the
        #    counter, and the inventory asserted below.
        async with httpx.AsyncClient(
            base_url=stack.phantom_url, timeout=_RAW_PROBE_TIMEOUT_SECONDS
        ) as raw:
            health = (await raw.get("/v1/healthz")).json()
            ready = (await raw.get("/v1/readyz")).json()
        assert health["storage"] == "ok"
        assert health["storage_detail"] is None
        assert ready["ready"] is True

        # 2. The healthy instance admits, delivers, and resolves through
        #    the fan-out with the quarantined sibling configured beside it.
        alpha_chain = await _submit_to_instance(
            stack, instance_id=HEALTHY_INSTANCE, file_name="mixed-health-alpha.bin"
        )
        await assert_chain_reaches_state(
            pc, alpha_chain, state="succeeded", timeout_seconds=_TERMINAL_BUDGET_SECONDS
        )
        alpha_captured = await _captured_file_id(pc, alpha_chain)
        fanout = await pc.find_by_captured_id(alpha_captured)
        assert fanout.found is True, "the fan-out must resolve beside the quarantined sibling"
        assert [m.chain_id for m in fanout.matches] == [alpha_chain]

        # 3. The quarantined instance answers admin reads honestly off
        #    its fresh store: a scoped miss is found=false, never an error.
        scoped_miss = await pc.find_by_captured_id(alpha_captured, instance=QUARANTINED_INSTANCE)
        assert scoped_miss.found is False
        assert scoped_miss.matches == []

        # 4. The corrupted DB survived as a manifested corruption entry
        #    in that instance's inventory (quarantine-not-delete).
        inventory = await pc.get_quarantine_inventory(instance=QUARANTINED_INSTANCE)
        corruption_entries = [e for e in inventory.quarantines if e.reason == "corrupted"]
        assert len(corruption_entries) == 1, (
            f"expected exactly the boot quarantine; got {inventory.quarantines!r}"
        )
        assert corruption_entries[0].has_db is True

        # 5. The quarantined instance still buffers and delivers NEW
        #    traffic and its fresh captures resolve (ADR-025: serve-fresh
        #    is not serve-nothing).
        beta_chain = await _submit_to_instance(
            stack, instance_id=QUARANTINED_INSTANCE, file_name="mixed-health-beta.bin"
        )
        await assert_chain_reaches_state(
            pc, beta_chain, state="succeeded", timeout_seconds=_TERMINAL_BUDGET_SECONDS
        )
        beta_captured = await _captured_file_id(pc, beta_chain)
        scoped_hit = await pc.find_by_captured_id(beta_captured, instance=QUARANTINED_INSTANCE)
        assert scoped_hit.found is True
        assert [m.chain_id for m in scoped_hit.matches] == [beta_chain]
    finally:
        await stack.tear_down()
