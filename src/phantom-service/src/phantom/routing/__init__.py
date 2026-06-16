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
from typing import Literal
from urllib.parse import urlparse

from phantom.config.settings import InstanceCfg


@dataclass(frozen=True)
class ResolvedRoute:
    """One resolved route policy for a URL within an instance.

    Attributes:
        route_name: The matched ``RouteCfg.name``.
        auth_mode: Whether Phantom injects ``Authorization`` on this route.
        timeout_seconds: Per-route HTTP timeout (None falls back to the
            upstream client's default of 30 s).
    """

    route_name: str
    auth_mode: Literal["phantom_bearer", "none"]
    timeout_seconds: float | None = None


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
                )
    raise ValueError(
        f"No route matched URL {url!r} (host={host!r}) for instance {instance_cfg.id!r}"
    )


__all__ = ["ResolvedRoute", "resolve_route"]
