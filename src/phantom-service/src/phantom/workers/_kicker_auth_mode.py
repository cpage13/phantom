"""Shared kicker helper: a parked row's current-step resolved route.

Both :class:`phantom.workers.auth_kicker.AuthKicker` and
:class:`phantom.workers.credential_kicker.CredentialKicker` walk the SAME
``auth_expired`` rows on the SAME shared saturation gate. To stop them fighting
over each other's rows (plan §2.5), each kicker skips rows whose destination
``auth_mode`` is not its kind; each kicker ALSO sweeps rows past their route's
send-deadline (ADR-032). Both checks key off the SAME resolved route, so this
single helper resolves it ONCE per row — the kickers read ``.auth_mode`` for the
guard and ``.send_deadline_seconds`` for the sweep off one resolve, never two.
"""

from __future__ import annotations

from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.routing import ResolvedRoute, resolve_route


def row_resolved_route(row: UploadRow, instance: InstanceContext) -> ResolvedRoute:
    """Resolve the row's destination route from its persisted host.

    Reads ``row.endpoint`` — the already-normalized current-step hostname
    (``models/upload.py``: "Hostname of the current step's target", the ADR-002
    cache axis), set once at admission and never mutated — and resolves it
    through the importable :func:`phantom.routing.resolve_route`. This is the
    SAME host axis the cred-slot key and the freshness oracle use
    (``signer_creds.get(row.endpoint)``), so the guard and the freshness gate
    stay coherent; deriving it any other way (re-parsing the envelope,
    recomputing the step URL) risks resolving a different host → a different
    route → the wrong kicker.

    Returns the WHOLE :class:`~phantom.routing.ResolvedRoute` (not just the
    ``auth_mode``) so the caller's auth_mode guard (``.auth_mode``) AND its
    send-deadline sweep (``.send_deadline_seconds``, ADR-032) share ONE resolve
    per row inside the kicker's existing per-row ``try/except``.

    Args:
        row: The parked upload row whose destination route to derive.
        instance: The instance whose route table (``instance.cfg.routes``) to
            resolve against.

    Returns:
        The destination :class:`~phantom.routing.ResolvedRoute`.

    Raises:
        ValueError: When no route matches ``row.endpoint``. Since F5 froze
            the route block at boot, a route can no longer vanish from under
            a parked row at reload time; what survives is a chain admitted
            with a step whose host matches no route, because admission
            route-checks only the FIRST step and tolerates a miss. The
            CALLER wraps this per row and SKIPS that row: it must NOT abort
            the rescan pass.
    """
    return resolve_route(row.endpoint, instance.cfg)
