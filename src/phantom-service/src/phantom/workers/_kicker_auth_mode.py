"""Kicker helper: a parked row's blocked-host resolved route.

Both flavours of :class:`phantom.workers.kicker.Kicker` walk the SAME
``auth_expired`` rows on the SAME shared saturation gate. To stop them fighting
over each other's rows (plan §2.5), each flavour skips rows whose destination
``auth_mode`` is not its kind; each ALSO sweeps rows past their route's
send-deadline (ADR-032). Both checks key off the SAME resolved route, so this
helper resolves it ONCE per host: the loop reads ``.auth_mode`` for the guard
and ``.send_deadline_seconds`` for the sweep off one resolve, never two.

Since CL5 the caller computes the probe host ONCE per candidate and hands the
same string to this resolver and to the freshness oracle, so the guard and the
wake key sit on one host axis by construction rather than by two parallel
copies of the same expression (J1).
"""

from __future__ import annotations

from phantom.instances.context import InstanceContext
from phantom.routing import ResolvedRoute, resolve_route


def resolved_route_for_host(probe_host: str, instance: InstanceContext) -> ResolvedRoute:
    """Resolve a parked row's destination route from its probe host.

    The caller derives ``probe_host`` as ``row.auth_blocked_host or
    row.endpoint``, the host whose credential slot actually rejected the row
    when the sender parked it (D2/F6), falling back to the FIRST step's
    admission-time host only for a row whose column is NULL. Resolves it
    through the importable :func:`phantom.routing.resolve_route`. Taking the
    HOST rather than the row is what makes this the SAME string the cred-slot
    key and the freshness oracle use, so the guard and the freshness gate stay
    coherent; deriving it any other way (``row.endpoint`` alone, re-parsing the
    envelope, recomputing the step URL) risks resolving a
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
        probe_host: The recorded blocked host to resolve, already reduced from
            the candidate by the caller.
        instance: The instance whose route table (``instance.cfg.routes``) to
            resolve against.

    Returns:
        The destination :class:`~phantom.routing.ResolvedRoute`.

    Raises:
        ValueError: When no route matches ``probe_host``. Since F5 froze
            the route block at boot, a route can no longer vanish from under
            a parked row at reload time; what survives is a chain admitted
            with a step whose host matches no route, because admission
            route-checks only the FIRST step and tolerates a miss, plus a
            third cause since D2: a row parked on a hostless step URL records
            the sanitised ``<no-host>`` token, which matches no route unless
            a catch-all pattern covers it. The CALLER wraps this per row and
            SKIPS that row: it must NOT abort the rescan pass.
    """
    return resolve_route(probe_host, instance.cfg)
