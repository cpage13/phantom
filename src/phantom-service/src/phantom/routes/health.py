"""Public liveness + readiness router.

Liveness (``GET /v1/healthz``) and readiness (``GET /v1/readyz``) are the
public, unprefixed probe paths. The single Phantom app serves intake,
admin, and these health probes on one listener (loopback by default per
ADR-004); this small router carries the two probe routes under their own
public, ``z``-suffixed names rather than behind the ``/v1/admin/`` prefix,
so an operator's container/orchestrator probe never has to reach an
admin-prefixed path.

Path choice: the root README advertises ``GET
http://localhost:8080/v1/healthz`` as the liveness probe, so
``/v1/healthz`` is the liveness path. Readiness pairs with it as
``/v1/readyz`` (the conventional k8s-style ``*z`` liveness/readiness pair
the ``healthz`` form signals). An earlier revision served these as the
admin-prefixed ``/v1/admin/health`` / ``/v1/admin/ready`` (no ``z``); they
were renamed to the public ``z``-suffixed paths (R12-1) and that rename is
the only contract change - the response shapes (:class:`HealthResponse`,
:class:`ReadyResponse`) are unchanged, so the SDK mirror and the contract
gate see a path rename, not a payload change.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends

from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.admin import HealthResponse, ReadyResponse
from phantom.routes._version import SEND_ROUTER_PREFIX
from phantom.runtime.startup_checks import DegradedInstance, degrade_action_hint

logger = logging.getLogger(__name__)


def get_version() -> str:
    """Phantom version string - overridden by the composition root.

    The health router keeps its OWN dependency placeholder (distinct from
    the admin router's ``get_version``) so the health routes carry their
    own seam and never depend on the admin router being mounted. The
    composition root (:func:`phantom.app.create_app`) overrides both.

    Returns:
        The version string; a placeholder default until overridden.
    """
    return "0.1.0"


def get_dispatcher() -> InstanceDispatcher:
    """Instance dispatcher - wired by the composition root.

    The health router's own placeholder (distinct from the admin
    router's) so the health routes resolve the live dispatcher through the
    one app's overrides. ``/v1/readyz`` reads it to report whether any
    instance is configured and live.

    Raises:
        NotImplementedError: When the composition root has not overridden
            the dependency (a wiring bug, not a runtime path).
    """
    raise NotImplementedError("InstanceDispatcher dependency must be overridden by app factory")


def get_degraded_instances() -> Sequence[DegradedInstance]:
    """The typed degraded set (overridden by the composition root; seam 3).

    One :class:`DegradedInstance` per instance whose boot returned a
    classified storage fault (no store / context was built, so the
    instance is absent from the dispatcher). Defaults to an EMPTY sequence
    (the normal all-healthy case), so a partial wiring or a test harness
    that does not override this reports ``ok`` / ``ready`` exactly as a
    healthy stack.

    Returns:
        The typed degraded set; empty when every instance booted healthy.
    """
    return ()


router = APIRouter(prefix=SEND_ROUTER_PREFIX)


def _degraded_detail(degraded_instances: Sequence[DegradedInstance]) -> str:
    """Format a one-line operator-facing detail for degraded instances.

    Names each degraded instance id, its classified reason, its fault
    detail, and the operator's next action, joined so a single
    ``/v1/readyz`` or ``/v1/healthz`` ``detail`` string surfaces every
    fault at once. Caller guarantees ``degraded_instances`` is non-empty.

    Args:
        degraded_instances: The typed degraded set (seam 3), one
            :class:`DegradedInstance` per degraded boot.

    Returns:
        A human-readable summary, e.g.
        ``"Storage unavailable for instance(s): primary [substrate_unwritable]:
        <fault> (action: ...)"``.
    """
    faults = "; ".join(
        f"{d.instance_id} [{d.reason.value}]: {d.detail} (action: {degrade_action_hint(d.reason)})"
        for d in sorted(degraded_instances, key=lambda d: d.instance_id)
    )
    return f"Storage unavailable for instance(s): {faults}"


@router.get("/healthz", response_model=HealthResponse)
async def get_health(
    version: Annotated[str, Depends(get_version)],
    degraded_instances: Annotated[Sequence[DegradedInstance], Depends(get_degraded_instances)],
) -> HealthResponse:
    """Liveness probe (the process is up).

    ``status`` stays ``"ok"`` (liveness is about the process being alive,
    not about storage health) so existing liveness checks are unaffected.
    The optional ``storage`` field is the fuller signal (§ 4D.2 / N-2):
    ``"ok"`` when every instance has writable storage, else
    ``"degraded"`` with ``storage_detail`` naming the degraded instance(s)
    and their faults. ``HealthResponse`` is extras-tolerated by the
    contract gate, so the added service-side field does not break the SDK
    mirror.
    """
    if degraded_instances:
        return HealthResponse(
            status="ok",
            version=version,
            storage="degraded",
            storage_detail=_degraded_detail(degraded_instances),
        )
    return HealthResponse(status="ok", version=version, storage="ok", storage_detail=None)


@router.get("/readyz", response_model=ReadyResponse)
async def get_ready(
    dispatcher: Annotated[InstanceDispatcher, Depends(get_dispatcher)],
    degraded_instances: Annotated[Sequence[DegradedInstance], Depends(get_degraded_instances)],
) -> ReadyResponse:
    """Readiness probe: true once every instance's DB is open and writable.

    Returns ``ready=false`` when no instance is configured (the existing
    behavior) OR when one or more instances booted DEGRADED (§ 4D.2): an
    unwritable per-instance ``data_dir`` means that instance has no durable
    buffering, so the deployment is not fully ready and the ``detail``
    names the degraded instance(s) and their faults.
    """
    if degraded_instances:
        return ReadyResponse(ready=False, detail=_degraded_detail(degraded_instances))
    instances = dispatcher.all_instances()
    if not instances:
        return ReadyResponse(ready=False, detail="No instances configured")
    return ReadyResponse(ready=True, detail=None)
