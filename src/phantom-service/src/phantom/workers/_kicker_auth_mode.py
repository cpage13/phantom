"""Shared kicker helper: a parked row's blocked-host resolved route.

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
    """Resolve the row's destination route from its recorded blocked host.

    Reads ``row.auth_blocked_host or row.endpoint``, the host whose credential
    slot actually rejected the row when the sender parked it (D2/F6), falling
    back to the FIRST step's admission-time host only for a row whose column is
    NULL. Resolves it through the importable
    :func:`phantom.routing.resolve_route`. This is the SAME expression the
    cred-slot key and the freshness oracle use, so the guard and the freshness
    gate stay coherent; deriving it any other way (``row.endpoint`` alone,
    re-parsing the envelope, recomputing the step URL) risks resolving a
    different host → a different route → the wrong kicker. That is not
    hypothetical: routes carry per-route ``auth_mode`` and admission
    route-checks only the FIRST step, so a chain whose step 1 is on a
    ``phantom_bearer`` route and whose step 2 is on an ``aws_sigv4`` route is
    legal config, and a row parked on the sigv4 host but partitioned on the
    bearer endpoint would be claimed by the kicker that can never wake it.

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
        ValueError: When no route matches the RECORDED host. Since F5 froze
            the route block at boot, a route can no longer vanish from under
            a parked row at reload time; what survives is a chain admitted
            with a step whose host matches no route, because admission
            route-checks only the FIRST step and tolerates a miss, plus a
            third cause since D2: a row parked on a hostless step URL records
            the sanitised ``<no-host>`` token, which matches no route unless
            a catch-all pattern covers it. The CALLER wraps this per row and
            SKIPS that row: it must NOT abort the rescan pass.
    """
    return resolve_route(row.auth_blocked_host or row.endpoint, instance.cfg)
