"""In-process metrics — simple counters and gauges.

Phantom's observability surface is structured logging + JSON-over-HTTP
admin endpoints. This module provides :class:`Counter` and :class:`Gauge`
primitives that hold values in-process; the admin endpoints (plan § 4.2.5)
serialize them to JSON.

NO dependency on ``prometheus_client`` or ``opentelemetry`` (plan § 4.3).
For Phantom's producer-deployment context (Pi-class hardware, single-process
service, loopback admin), a heavy metrics framework is overkill — the
in-process registry + JSON serialization is sufficient.

The :class:`MetricsRegistry` is constructed once in
:func:`phantom.app.create_app`'s FastAPI ``lifespan`` (the composition
root) and passed to every worker that emits metrics. Workers acquire
:class:`Counter` / :class:`Gauge` references from the registry at
construction time and hold them for the lifetime of the runtime.

See plan § 4.2.1 for the canonical metric list registered at composition
time + plan § 0.5 for the seven reliability invariants whose violations
this surface exposes.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Empty-string sentinel for the "no label" label-value bucket. A single
# counter/gauge stores its unlabeled value under this key so the
# storage shape is uniform across labeled and unlabeled metrics.
_NO_LABEL_BUCKET: str = ""


@dataclass
class Counter:
    """Monotonically increasing integer with labeled variants.

    Labels are encoded as a single ``label_value`` string. Counters
    without labels use the :data:`_NO_LABEL_BUCKET` sentinel (empty
    string) so the underlying dict shape is uniform.

    Attributes:
        name: Stable canonical identifier (e.g.,
            ``invariant_violation_total``). Surfaces via the admin
            endpoint.
        description: One-line human-readable explanation.
    """

    name: str
    description: str
    _values: dict[str, int] = field(default_factory=lambda: {_NO_LABEL_BUCKET: 0})
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self, label_value: str = _NO_LABEL_BUCKET, n: int = 1) -> None:
        """Increment the counter for ``label_value`` by ``n``.

        Args:
            label_value: Label-value bucket. Defaults to the no-label
                bucket (empty string).
            n: Increment amount. Defaults to 1.
        """
        async with self._lock:
            self._values[label_value] = self._values.get(label_value, 0) + n

    def snapshot(self) -> Mapping[str, int]:
        """Return a copy of the current label_value → count map.

        Returns:
            A fresh ``dict`` mirroring the counter's state at the call
            time. Dict reads are atomic under the GIL, so this is safe
            to call without holding ``_lock`` — the only risk is
            observing a value bumped between two reads, which is
            acceptable for a snapshot surface.
        """
        return dict(self._values)


@dataclass
class Gauge:
    """A value that can go up or down with labeled variants.

    Attributes:
        name: Stable canonical identifier (e.g., ``saturation_balance``).
        description: One-line human-readable explanation.
    """

    name: str
    description: str
    _values: dict[str, float] = field(default_factory=lambda: {_NO_LABEL_BUCKET: 0.0})
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def set(self, value: float, label_value: str = _NO_LABEL_BUCKET) -> None:
        """Set the gauge for ``label_value`` to ``value``.

        Args:
            value: New value. Integer values are accepted and stored as
                float; consumers serialize via ``json.dumps`` which
                emits them without a decimal when whole.
            label_value: Label-value bucket. Defaults to the no-label
                bucket.
        """
        async with self._lock:
            self._values[label_value] = float(value)

    async def inc(self, n: float = 1, label_value: str = _NO_LABEL_BUCKET) -> None:
        """Increment the gauge for ``label_value`` by ``n``."""
        async with self._lock:
            self._values[label_value] = self._values.get(label_value, 0.0) + float(n)

    async def dec(self, n: float = 1, label_value: str = _NO_LABEL_BUCKET) -> None:
        """Decrement the gauge for ``label_value`` by ``n``."""
        async with self._lock:
            self._values[label_value] = self._values.get(label_value, 0.0) - float(n)

    def snapshot(self) -> Mapping[str, float]:
        """Return a copy of the current label_value → value map."""
        return dict(self._values)


@dataclass
class MetricsRegistry:
    """Composition-root-owned bag of named counters and gauges.

    The runtime constructs one :class:`MetricsRegistry` and threads it
    through every worker that emits metrics (plan § 4.2.2). Workers
    call :meth:`register_counter` / :meth:`register_gauge` at
    construction time (idempotent — re-registration returns the
    existing instance) and hold the returned reference for the lifetime
    of the runtime.

    Attributes:
        counters: name → :class:`Counter`.
        gauges: name → :class:`Gauge`.
    """

    counters: dict[str, Counter] = field(default_factory=dict)
    gauges: dict[str, Gauge] = field(default_factory=dict)

    def register_counter(self, name: str, description: str) -> Counter:
        """Register a :class:`Counter` under ``name``; return the instance.

        Idempotent: re-registering the same ``name`` returns the
        existing instance (the description must match — re-registering
        with a different description is a programming error and raises
        ``ValueError``).

        Args:
            name: Stable canonical metric name. See plan § 4.2.1 for
                the registered set.
            description: One-line explanation shown via the admin
                endpoint.

        Returns:
            The :class:`Counter` instance (either freshly constructed
            or the previously-registered one).
        """
        existing = self.counters.get(name)
        if existing is not None:
            if existing.description != description:
                msg = (
                    f"Counter {name!r} re-registered with mismatched description: "
                    f"old={existing.description!r}, new={description!r}"
                )
                raise ValueError(msg)
            return existing
        counter = Counter(name=name, description=description)
        self.counters[name] = counter
        return counter

    def register_gauge(self, name: str, description: str) -> Gauge:
        """Register a :class:`Gauge` under ``name``; return the instance.

        Idempotent — see :meth:`register_counter`.
        """
        existing = self.gauges.get(name)
        if existing is not None:
            if existing.description != description:
                msg = (
                    f"Gauge {name!r} re-registered with mismatched description: "
                    f"old={existing.description!r}, new={description!r}"
                )
                raise ValueError(msg)
            return existing
        gauge = Gauge(name=name, description=description)
        self.gauges[name] = gauge
        return gauge


__all__ = ["Counter", "Gauge", "MetricsRegistry"]
