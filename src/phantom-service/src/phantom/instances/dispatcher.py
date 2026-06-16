"""URL-prefix → :class:`InstanceContext` dispatcher (ADR-006)."""

from __future__ import annotations

import fnmatch
import logging
from collections.abc import Iterable, Sequence
from urllib.parse import urlparse

from phantom.config.settings import InstanceCfg
from phantom.instances.context import InstanceContext

logger = logging.getLogger(__name__)


class InstanceNotFoundError(KeyError):
    """``X-Phantom-Instance`` names an instance that doesn't exist."""


class NoMatchingInstanceError(ValueError):
    """No instance's ``host_prefixes`` matched the URL."""


def _host_of(url: str) -> str:
    """Lower-cased hostname of ``url`` (the whole string if it has no host)."""
    return (urlparse(url).hostname or url).lower()


def _matches_host_prefixes(host: str, host_prefixes: Iterable[str]) -> bool:
    """True when ``host`` fnmatches any of ``host_prefixes`` (case-insensitive).

    The single shared host-prefix matching rule. Both
    :meth:`InstanceDispatcher.resolve` (routing a request to a live
    instance) and :func:`resolve_configured_instance_id` (mapping a
    request to its CONFIGURED instance id, including degraded instances
    with no live context, § 4D.2) consult this one predicate so routing
    and the degraded-boot guard cannot drift.

    Args:
        host: The already-lower-cased request host.
        host_prefixes: The instance's ``fnmatch`` host patterns.

    Returns:
        ``True`` when any prefix matches ``host``.
    """
    return any(fnmatch.fnmatchcase(host, prefix.lower()) for prefix in host_prefixes)


def resolve_configured_instance_id(
    instance_cfgs: Sequence[InstanceCfg],
    url: str,
    instance_header: str | None,
) -> str | None:
    """Map a request to its CONFIGURED instance id, or ``None`` if unrouted.

    Operates over the configured :class:`InstanceCfg` list (which exists
    regardless of boot outcome), NOT the live dispatcher, so it can name an
    instance that booted DEGRADED and therefore has no live context or
    dispatcher entry (§ 4D.2). Uses the same header-then-host-prefix
    precedence as :meth:`InstanceDispatcher.resolve` via the shared
    :func:`_matches_host_prefixes` predicate.

    Args:
        instance_cfgs: Configured instances in YAML order (``settings.instances``).
        url: The first-step URL of the chain (used for host-prefix matching);
            ignored when ``instance_header`` is given.
        instance_header: Optional ``X-Phantom-Instance`` value (advanced
            explicit-routing override, ADR-006).

    Returns:
        The matching ``InstanceCfg.id``, or ``None`` when the explicit header
        names no configured instance or no instance's ``host_prefixes`` match.
    """
    if instance_header is not None:
        for cfg in instance_cfgs:
            if cfg.id == instance_header:
                return cfg.id
        return None
    host = _host_of(url)
    for cfg in instance_cfgs:
        if _matches_host_prefixes(host, cfg.host_prefixes):
            return cfg.id
    return None


class InstanceDispatcher:
    """Resolve an inbound request to the owning :class:`InstanceContext`."""

    def __init__(self, instances: list[InstanceContext]) -> None:
        """Construct the dispatcher.

        Args:
            instances: All configured instances, in YAML order.
        """
        self._by_id: dict[str, InstanceContext] = {ctx.cfg.id: ctx for ctx in instances}
        self._ordered: list[InstanceContext] = list(instances)

    def resolve(self, url: str, instance_header: str | None) -> InstanceContext:
        """Pick the owning instance.

        Args:
            url: The first-step URL of the chain.
            instance_header: Optional ``X-Phantom-Instance`` value
                (advanced override, ADR-006).

        Returns:
            The matching :class:`InstanceContext`.

        Raises:
            InstanceNotFoundError: When the header names an unknown instance.
            NoMatchingInstanceError: When no instance's ``host_prefixes`` matches.
        """
        if instance_header is not None:
            ctx = self._by_id.get(instance_header)
            if ctx is None:
                raise InstanceNotFoundError(
                    f"X-Phantom-Instance {instance_header!r} not configured",
                )
            logger.warning(
                "Routing by explicit X-Phantom-Instance header (advanced path)",
            )
            return ctx
        host = _host_of(url)
        for ctx in self._ordered:
            if _matches_host_prefixes(host, ctx.cfg.host_prefixes):
                return ctx
        raise NoMatchingInstanceError(
            f"No instance accepts host {host!r}",
        )

    def all_instances(self) -> list[InstanceContext]:
        """Return every configured instance (for admin endpoints)."""
        return list(self._ordered)

    def by_id(self, instance_id: str) -> InstanceContext | None:
        """Look up an instance by id, or ``None`` if missing."""
        return self._by_id.get(instance_id)
