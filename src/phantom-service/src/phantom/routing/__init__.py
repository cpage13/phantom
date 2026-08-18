"""Route policy resolution - fnmatch over an instance's declared routes.

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
from phantom.models.chain import ChainEnvelope

# The outbound-auth mode a route declares. Named here (rather than left as a
# bare inline Literal) so consumers that branch on it - the executor's auth
# arm, the two kickers' auth_mode guard - compare against an
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
            configured global default, ``upstream.timeout_seconds``).
        send_deadline_seconds: Max wall-clock seconds a buffered upload may
            keep trying before it gives up to the terminal ``expired`` state
            (None = no deadline). Measured from ``row.received_at``; read by
            the executor's send-deadline gate and the kicker parked-row sweeps.
    """

    route_name: str
    auth_mode: AuthMode
    timeout_seconds: float | None = None
    send_deadline_seconds: int | None = None


type HostKey = str
"""A URL normalised down to the string Phantom keys hosts on.

Usually a hostname, and NOT always one: see :func:`host_key_for`. The alias
exists so ``dict[HostKey, ...]`` at the credential and token stores reads as
what it is rather than as ``dict[str, ...]``.
"""


def host_key_for(url: str) -> HostKey:
    """Normalise ``url`` to the key Phantom looks hosts up by.

    **The fallback is the part to read.** When ``urlparse`` finds no host,
    for example because the step URL is a bare path, this returns the ENTIRE
    INPUT STRING lower-cased. That is the historical behaviour of the four
    helpers this one replaces, and it is deliberate here: a lookup key that
    misses is harmless, while silently substituting a placeholder would make
    two different pathless URLs share a cache slot. The name says ``host_key``
    rather than ``hostname`` for exactly this reason.

    **The output is UNSANITISED and must not be persisted or logged as-is.**
    A step URL is post-substitution producer data and can carry a query
    string holding a presigned ``X-Amz-Signature`` and ``X-Amz-Credential``,
    so under the fallback that credential material ends up inside the return
    value. Any caller writing a host into a persisted column (D2's
    ``uploads.auth_blocked_host``), into ``last_error`` (F1's
    ``RouteUnresolved.host``) or into a log line applies the § 4 / § 1.1.2
    sanitiser instead: parse the host, and record the fixed ``<no-host>``
    token when there is none. Lookup keys are the only safe consumer of the
    fallback, because they are never surfaced.

    Args:
        url: An absolute URL, or any string a caller wants keyed.

    Returns:
        The lower-cased hostname, or the lower-cased whole input when the
        URL carries no parseable host.
    """
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()


def resolve_first_step_url(envelope: ChainEnvelope) -> str:
    """Resolve the first step's URL, applying ``default_target`` if needed.

    A chain's first step may carry a path rather than an absolute URL, in
    which case the envelope's ``default_target`` supplies the origin. Both
    ingress routes need the resolved value before admission (for the route
    check, the degraded-boot guard and the admission-time endpoint), and
    both held a byte-identical copy of this until CL1.

    Args:
        envelope: The submitted chain envelope.

    Returns:
        The first step's absolute URL when one can be formed, else the
        step's own ``url`` unchanged.
    """
    first_step_url = envelope.steps[0].url
    if envelope.default_target and "://" not in first_step_url:
        first_step_url = str(envelope.default_target).rstrip("/") + (
            first_step_url if first_step_url.startswith("/") else "/" + first_step_url
        )
    return first_step_url


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
    host = host_key_for(url)
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


__all__ = [
    "AuthMode",
    "HostKey",
    "ResolvedRoute",
    "host_key_for",
    "resolve_first_step_url",
    "resolve_route",
]
