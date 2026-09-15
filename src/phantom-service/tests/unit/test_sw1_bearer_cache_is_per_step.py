"""SW-1: the inbound bearer is cached for every bearer-protected STEP.

The executor resolves a route and picks an auth provider PER STEP, and
``BearerAuthProvider.prepare`` looks its slot up under
``host_key_for(<that step's url>)``. Admission used to read only the FIRST
step's ``auth_mode`` and write only the FIRST step's key, so a chain whose
bearer-protected step is not the first one cached nothing at all: the producer
handed Phantom a working token and the row parked in ``auth_expired`` against a
key no writer ever fills, unreachable by the kicker and unblockable except by an
admin push.

Every test here drives the REAL :func:`phantom.routes.admission.admit_chain`
against a two-route instance and then asserts on the token cache the executor
will read, never on the intermediate intent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.chain.auth_providers import BearerAuthProvider
from phantom.chain.executor import ChainExecutor
from phantom.compression import select_codec
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.models.chain import ChainEnvelope, ChainStep
from phantom.routes.admission import AdmissionInputs, admit_chain
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# Gate limits: ample headroom, so nothing here is refused for capacity.
_GATE_MAX_IN_FLIGHT = 10
_GATE_MAX_BYTES = 10_000_000

# The two destinations of the shape SW-1 describes. Step 1 PUTs the object to
# storage on a route Phantom injects nothing into; step 2 POSTs the completion
# callback to a bearer-protected API on a DIFFERENT host.
_STORAGE_HOST = "objects.example.com"
_CALLBACK_HOST = "callback.example.com"
_UID = "user-1"
_BEARER = "Bearer producer-supplied"


class _FakeUpstream:
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path, *, routes: list[RouteCfg]) -> InstanceContext:
    """A started single-instance context carrying the caller's route table."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    for component in (store, ram, fbs, tokens):
        await component.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[_STORAGE_HOST],
        data_dir="primary",
        routes=routes,
    )
    upstream = _FakeUpstream()
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=ChainExecutor(
            token_cache=tokens,
            upstream_client=upstream,
            resolve_route=resolve_route,
            clock=lambda: datetime.now(tz=UTC),
            instance=cfg,
        ),
        saturation=SaturationGate(
            max_in_flight=_GATE_MAX_IN_FLIGHT,
            max_in_flight_bytes=_GATE_MAX_BYTES,
            max_disk_bytes=_GATE_MAX_BYTES,
        ),
        codec_factory=lambda: select_codec(CompressionCfg(algorithm="original")),  # type: ignore[arg-type]
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                body_store=BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
            )
        ),
    )
    return track_instance(instance)


def _two_step_envelope() -> ChainEnvelope:
    """Step 1 to object storage, step 2 to the completion callback."""
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key=str(uuid4()),
        steps=[
            ChainStep(  # type: ignore[call-arg]
                name="put_object",
                method="PUT",
                url=f"https://{_STORAGE_HOST}/bucket/key",
            ),
            ChainStep(  # type: ignore[call-arg]
                name="notify",
                method="POST",
                url=f"https://{_CALLBACK_HOST}/v1/complete",
            ),
        ],
    )


def _inputs(envelope: ChainEnvelope, *, authorization: str | None = _BEARER) -> AdmissionInputs:
    return AdmissionInputs(
        request_id="r-1",
        uid_header=_UID,
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs={"body": b"payload"},
        authorization=authorization,
        content_encoding=None,
    )


async def test_bearer_step_two_gets_a_slot_the_executor_can_read(tmp_path: Path) -> None:
    """Objective: a chain whose bearer step is NOT step 1 must still be cached.

    Step 1 is on an ``auth_mode: none`` route and step 2 on a
    ``phantom_bearer`` route on a different host. The old gate read step 1's
    mode, cached nothing, and left step 2 to park in ``auth_expired``.

    Expected: after ``admit_chain``, ``BearerAuthProvider.prepare`` for step 2's
    URL returns ``AuthReady`` with the producer's credential attached, which is
    the only form of this assertion that proves the key admission WROTE is the
    key the executor READS.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="objects", hosts=[_STORAGE_HOST], auth_mode="none"),
            RouteCfg(name="callback", hosts=[_CALLBACK_HOST], auth_mode="phantom_bearer"),
        ],
    )
    envelope = _two_step_envelope()

    outcome = await admit_chain(_inputs(envelope), instance)
    assert outcome.status_code == 202

    provider = BearerAuthProvider(cache=instance.token_cache)
    headers: dict[str, str] = {}
    prepared = await provider.prepare(
        full_url=f"https://{_CALLBACK_HOST}/v1/complete",
        uid=_UID,
        method="POST",
        headers=headers,
        body=b"",
        chain_id=envelope.chain_id,
    )
    assert getattr(prepared, "url", None) == f"https://{_CALLBACK_HOST}/v1/complete", (
        "step 2 parked instead of readying: admission cached no slot for its host"
    )
    assert headers["Authorization"] == _BEARER


async def test_the_unauthenticated_first_step_host_is_not_cached(tmp_path: Path) -> None:
    """Objective: the per-step widening must not widen the D3 mode gate.

    Expected: the ``auth_mode: none`` host of step 1 gets NO slot. Forward-as-is
    injects nothing at egress, so a slot there can never be read back and the
    write would only reset a status to fresh and wake parked rows for nothing.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="objects", hosts=[_STORAGE_HOST], auth_mode="none"),
            RouteCfg(name="callback", hosts=[_CALLBACK_HOST], auth_mode="phantom_bearer"),
        ],
    )

    await admit_chain(_inputs(_two_step_envelope()), instance)

    assert await instance.token_cache.get(_STORAGE_HOST, _UID) is None
    assert await instance.token_cache.get(_CALLBACK_HOST, _UID) is not None


async def test_every_bearer_step_of_a_multi_host_chain_is_cached(tmp_path: Path) -> None:
    """Objective: two bearer-protected hosts in one chain both get a slot.

    Expected: both hosts resolve a slot, because the executor authenticates
    against whichever host the CURRENT step names and one key cannot serve two.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="objects", hosts=[_STORAGE_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="callback", hosts=[_CALLBACK_HOST], auth_mode="phantom_bearer"),
        ],
    )

    await admit_chain(_inputs(_two_step_envelope()), instance)

    for host in (_STORAGE_HOST, _CALLBACK_HOST):
        slot = await instance.token_cache.get(host, _UID)
        assert slot is not None, f"no slot cached for {host}"
        assert slot.bearer == _BEARER


async def test_a_sigv4_step_beyond_the_first_is_still_not_cached(tmp_path: Path) -> None:
    """Objective: the per-step gate keeps refusing ``aws_sigv4``, at any index.

    An inbound ``Authorization`` on a sigv4 route is a credential bound to one
    request's canonical form: useless as a bearer, and caching it would
    overwrite a real bearer for that slot and flip a bad slot healthy.

    Expected: the sigv4 host has no slot while the bearer host does, so the
    widening is over STEPS and not over modes.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="objects", hosts=[_STORAGE_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="callback", hosts=[_CALLBACK_HOST], auth_mode="aws_sigv4"),
        ],
    )

    await admit_chain(_inputs(_two_step_envelope()), instance)

    assert await instance.token_cache.get(_CALLBACK_HOST, _UID) is None
    assert await instance.token_cache.get(_STORAGE_HOST, _UID) is not None


async def test_a_rejected_chain_still_caches_nothing_for_any_step(tmp_path: Path) -> None:
    """Objective: the per-step widening must not reopen the V7 outcome gate.

    The same chain_id is submitted twice; the second is rejected 409 by the
    live-row pre-check, which sits AFTER row preparation.

    Expected: the second submission leaves the bearer that the FIRST one cached
    untouched, so a refused request still writes nothing anywhere.
    """
    instance = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="objects", hosts=[_STORAGE_HOST], auth_mode="none"),
            RouteCfg(name="callback", hosts=[_CALLBACK_HOST], auth_mode="phantom_bearer"),
        ],
    )
    envelope = _two_step_envelope()
    await admit_chain(_inputs(envelope), instance)
    await instance.token_cache.mark_bad(_CALLBACK_HOST, _UID)

    with pytest.raises(Exception, match="already in use"):
        await admit_chain(_inputs(envelope, authorization="Bearer second-attempt"), instance)

    slot = await instance.token_cache.get(_CALLBACK_HOST, _UID)
    assert slot is not None
    assert slot.status == "bad", "a rejected request flipped the slot back to fresh"
    assert slot.bearer == _BEARER, "a rejected request overwrote the cached bearer"
