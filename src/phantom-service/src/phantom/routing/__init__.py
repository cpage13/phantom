"""Route policy resolution — fnmatch over an instance's declared routes.

A route resolution maps the first-step URL of a chain to the per-instance
``RouteCfg`` that owns it. The resolution rule is "first match by host
fnmatch in declaration order"; catch-all routes (``hosts=['*']``) live
last by convention.

The composition root passes :func:`resolve_route` directly to consumers
that need it (the executor, the send route). There is no Protocol seam:
the function is the seam.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Literal, TypeAlias
from urllib.parse import urlparse

from phantom.config.settings import InstanceCfg

# The outbound-auth mode a route declares. Named here (rather than left as a
# bare inline Literal) so consumers that branch on it — the executor's auth
# arm, the two kickers' auth_mode guard — compare against an
# exhaustiveness-checkable type instead of raw strings (CONTEXT "no raw string
# comparisons when the value set is known"). Mirrors ``RouteCfg.auth_mode``
# (config/settings.py).
AuthMode: TypeAlias = Literal["phantom_bearer", "none", "aws_sigv4"]  # noqa: UP040


@dataclass(frozen=True)
class ResolvedRoute:
    """One resolved route policy for a URL within an instance.

    Attributes:
        route_name: The matched ``RouteCfg.name``.
        auth_mode: How Phantom authenticates the outbound request on this
            route (``phantom_bearer`` bearer-inject, ``aws_sigv4`` re-sign, or
            ``none``).
        timeout_seconds: Per-route HTTP timeout (None falls back to the
            upstream client's default of 30 s).
        send_deadline_seconds: Max wall-clock seconds a buffered upload may
            keep trying before it gives up to the terminal ``expired`` state
            (None = no deadline). Measured from ``row.received_at``; read by
            the executor's send-deadline gate and the kicker parked-row sweeps.
    """

    route_name: str
    auth_mode: AuthMode
    timeout_seconds: float | None = None
    send_deadline_seconds: int | None = None


def _hostname(url: str) -> str:
    """Return the hostname portion of ``url`` (lower-cased) or the URL itself."""
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()


def resolve_route(url: str, instance_cfg: InstanceCfg) -> ResolvedRoute:
    """Pick the first matching route in declaration order.

    Args:
        url: The first-step URL of the chain.
        instance_cfg: The instance whose routes to walk.

    Returns:
        A :class:`ResolvedRoute`.

    Raises:
        ValueError: When no route matches (caller maps to ``invalid_target``).
    """
    host = _hostname(url)
    for route in instance_cfg.routes:
        for pattern in route.hosts:
            if fnmatch.fnmatchcase(host, pattern.lower()):
                return ResolvedRoute(
                    route_name=route.name,
                    auth_mode=route.auth_mode,
                    timeout_seconds=route.timeout_seconds,
                    send_deadline_seconds=route.send_deadline_seconds,
                )
    raise ValueError(
        f"No route matched URL {url!r} (host={host!r}) for instance {instance_cfg.id!r}"
    )


__all__ = ["AuthMode", "ResolvedRoute", "resolve_route"]
