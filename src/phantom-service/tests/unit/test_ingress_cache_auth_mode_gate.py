"""F11 via D3: the ingress bearer-cache write is gated on the route's auth_mode.

Raw intake passes the client's ``Authorization`` header into the shared
admission prelude, and admission wrote it into the BEARER token cache with no
``auth_mode`` gate at all. On an ``aws_sigv4`` route that value is a
per-request AWS SigV4 credential string bound to one canonical request: it is
useless as a bearer, and writing it does three concrete harms.

1. It OVERWRITES any real bearer pushed for ``(host, uid)``, because
   ``SqliteTokenCache.set`` upserts. An operator who pushed a working token
   for that slot loses it to garbage on the next raw PUT.
2. It creates or revives a ``fresh`` slot carrying that garbage, visible at
   ``GET /v1/admin/tokens``, which is the operator's own view of what tokens
   they have. A slot the executor previously marked ``bad`` flips back.
3. It fires the token cache's wake handlers on EVERY raw PUT, so both kickers
   perform a full non-terminal rescan per request.

D3 settles the rule: admission consults the resolved route's ``auth_mode``
before ``token_cache.set``; only ``phantom_bearer`` routes cache. Raw-intake
requests on bearer routes still cache, which remains the documented pilot
behaviour, so the bearer-route arm of the original finding is deliberately
left open.

The tests build one instance with three routes, one per ``auth_mode``, over
three distinct hosts, plus a fourth host with NO route. They use the real
``SqliteTokenCache`` so the assertions observe the actual store rather than a
mock's call log.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
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

# One host per auth_mode, plus one the route table does not cover.
BEARER_HOST = "bearer.example.com"
SIGV4_HOST = "sigv4.example.com"
NONE_HOST = "none.example.com"
UNROUTED_HOST = "unrouted.example.com"

# The uid raw intake pins for a stock client, and the axis the admin tokens
# surface shows the slot under.
RAW_UID = ""

# A per-request AWS SigV4 credential string, the value F11 must stop caching.
SIGV4_AUTHORIZATION = (
    "AWS4-HMAC-SHA256 Credential=AKIACLIENT/20260817/us-east-1/s3/aws4_request, "
    "SignedHeaders=host;x-amz-date, Signature=deadbeef"
)

# The real bearer an operator pushed for the same slot.
REAL_BEARER = "Bearer operator-pushed-token"


class _FakeUpstream:
    """Stub upstream client. Admission never forwards."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a real-store instance carrying one route per ``auth_mode``.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The tracked :class:`InstanceContext`.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    for started in (store, ram, fbs, tokens):
        await started.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[BEARER_HOST, SIGV4_HOST, NONE_HOST, UNROUTED_HOST],
        data_dir="primary",
        routes=[
            RouteCfg(name="bearer", hosts=[BEARER_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="sigv4", hosts=[SIGV4_HOST], auth_mode="aws_sigv4"),
            RouteCfg(name="none", hosts=[NONE_HOST], auth_mode="none"),
        ],
    )
    upstream = _FakeUpstream()
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()

    def codec_factory() -> object:  # type: ignore[type-arg]
        return select_codec(CompressionCfg(algorithm="original"))

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
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
        ),
        codec_factory=codec_factory,  # type: ignore[arg-type]
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                body_store=BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
            )
        ),
    )
    return track_instance(instance)


def _inputs(host: str, *, authorization: str) -> AdmissionInputs:
    """Build raw-intake-shaped admission inputs targeting ``host``.

    Mirrors what the catch-all passes: ``uid_header`` pinned to the empty
    string (a stock client sends no ``X-Phantom-Uid``) and the client's own
    ``Authorization`` forwarded into the prelude.

    Args:
        host: The first step's host, which selects the route.
        authorization: The inbound ``Authorization`` header value.

    Returns:
        The :class:`AdmissionInputs` for :func:`admit_chain`.
    """
    envelope = ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key=str(uuid4()),
        steps=[
            ChainStep(  # type: ignore[call-arg]
                name="upload",
                method="PUT",
                url=f"https://{host}/bucket/key",
            )
        ],
    )
    return AdmissionInputs(
        request_id="r-1",
        uid_header=RAW_UID,
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs={},
        authorization=authorization,
        content_encoding=None,
    )


async def test_bearer_route_still_caches_the_inbound_authorization(tmp_path: Path) -> None:
    """D3's explicit carve-out: a bearer route still caches on ingress.

    Objective: the documented pilot behaviour must survive the gate. Success:
    after admitting a chain whose first step is on the ``phantom_bearer``
    host with an ``Authorization`` header, the cache holds a ``fresh`` slot
    for that ``(host, uid)`` carrying that value.
    """
    instance = await _build_instance(tmp_path)

    await admit_chain(_inputs(BEARER_HOST, authorization=REAL_BEARER), instance)

    slot = await instance.token_cache.get(endpoint=BEARER_HOST, uid=RAW_UID)
    assert slot is not None, "a phantom_bearer route must still cache the inbound bearer"
    assert slot.bearer == REAL_BEARER
    assert slot.status == "fresh"


async def test_sigv4_route_does_not_cache_the_inbound_authorization(tmp_path: Path) -> None:
    """The F11 defect itself: an aws_sigv4 route writes no bearer slot.

    Objective: the inbound value on that route is a per-request SigV4
    credential, useless as a bearer and harmful in the cache. Success: no slot
    exists for that ``(host, uid)`` at all.
    """
    instance = await _build_instance(tmp_path)

    await admit_chain(_inputs(SIGV4_HOST, authorization=SIGV4_AUTHORIZATION), instance)

    assert await instance.token_cache.get(endpoint=SIGV4_HOST, uid=RAW_UID) is None


async def test_none_route_does_not_cache_the_inbound_authorization(tmp_path: Path) -> None:
    """The third arm of the closed set: forward-as-is caches nothing.

    Objective: on a ``none`` route Phantom injects nothing at egress, so a
    cached value can never be read back and writing it can only cause the
    wake-and-churn side effects. Success: no slot for that host.
    """
    instance = await _build_instance(tmp_path)

    await admit_chain(_inputs(NONE_HOST, authorization=REAL_BEARER), instance)

    assert await instance.token_cache.get(endpoint=NONE_HOST, uid=RAW_UID) is None


async def test_unroutable_first_step_does_not_cache(tmp_path: Path) -> None:
    """A first step matching no route caches nothing, and is still admitted.

    Objective: pin the judgement call so it cannot be quietly reversed.
    Caching a bearer for a destination Phantom has no route to cannot help
    delivery, and it CAN hurt, because the write fires the kickers' wake
    handlers for nothing. Admission still admits the chain, which is F1's
    premise: the executor classifies the miss at send time. Success: no slot
    for the unrouted host, and a successful admission outcome.
    """
    instance = await _build_instance(tmp_path)

    outcome = await admit_chain(_inputs(UNROUTED_HOST, authorization=REAL_BEARER), instance)

    assert await instance.token_cache.get(endpoint=UNROUTED_HOST, uid=RAW_UID) is None
    assert outcome.status_code == 202, (
        f"an unroutable chain is still admitted (F1's premise); got {outcome.status_code}"
    )


async def test_sigv4_intake_does_not_revive_a_bad_bearer_slot(tmp_path: Path) -> None:
    """The actual harm: a raw SigV4 intake must not resurrect a bad slot.

    Objective: demonstrate the damage rather than the write. An operator's
    bearer for ``(host, uid)`` that the executor marked ``bad`` used to be
    overwritten by the next raw PUT's SigV4 credential string AND flipped back
    to ``fresh``, so the operator's own token view lied twice over. Success:
    the slot is still ``bad`` and still carries the original bearer.
    """
    instance = await _build_instance(tmp_path)
    await instance.token_cache.set(SIGV4_HOST, RAW_UID, REAL_BEARER, source="admin_push")
    await instance.token_cache.mark_bad(SIGV4_HOST, RAW_UID)

    await admit_chain(_inputs(SIGV4_HOST, authorization=SIGV4_AUTHORIZATION), instance)

    slot = await instance.token_cache.get(endpoint=SIGV4_HOST, uid=RAW_UID)
    assert slot is not None, "the pre-existing slot must not be deleted either"
    assert slot.status == "bad", "a gated write must not flip a bad slot back to fresh"
    assert slot.bearer == REAL_BEARER, "the operator's bearer must not be overwritten"


async def test_no_wake_handler_fires_for_a_gated_write(tmp_path: Path) -> None:
    """The second half of the harm: no cache write means no kicker wake.

    Objective: ``token_cache.set`` fires the wake handlers both kickers
    register, so an ungated write made every raw PUT trigger a full
    non-terminal rescan per kicker. Success: the recording handler fires for
    the ``phantom_bearer`` admission and for neither of the gated ones.
    """
    instance = await _build_instance(tmp_path)
    woken: list[tuple[str, str]] = []

    async def _record(endpoint: str, uid: str) -> None:
        woken.append((endpoint, uid))

    instance.token_cache.register_wake_handler(_record)

    await admit_chain(_inputs(SIGV4_HOST, authorization=SIGV4_AUTHORIZATION), instance)
    await admit_chain(_inputs(NONE_HOST, authorization=REAL_BEARER), instance)
    assert woken == [], f"a gated admission must fire no wake handler; got {woken}"

    await admit_chain(_inputs(BEARER_HOST, authorization=REAL_BEARER), instance)
    assert woken == [(BEARER_HOST, RAW_UID)], (
        f"the bearer admission must still wake the kickers; got {woken}"
    )
