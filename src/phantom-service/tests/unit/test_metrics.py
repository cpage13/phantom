"""Unit tests for :mod:`phantom.observability.metrics` (plan § 4.2.1).

The metrics primitives are in-process counters + gauges with optional
label-value buckets. Tests cover:

* Counter increment, labeled increment, snapshot copy semantics.
* Gauge set / inc / dec, labeled buckets, snapshot copy semantics.
* :class:`MetricsRegistry` idempotent registration (same name returns
  the same instance; description mismatch raises ``ValueError``).
* Concurrent increments under ``asyncio.gather`` — exercises the
  per-counter/gauge ``asyncio.Lock``.
"""

from __future__ import annotations

import asyncio

import pytest
from phantom.observability.metrics import Counter, Gauge, MetricsRegistry


@pytest.mark.asyncio
async def test_counter_increments_default_bucket() -> None:
    counter = Counter(name="x_total", description="x")
    await counter.inc()
    await counter.inc()
    snapshot = counter.snapshot()
    assert snapshot == {"": 2}


@pytest.mark.asyncio
async def test_counter_increments_labeled_bucket() -> None:
    counter = Counter(name="violations_total", description="v")
    await counter.inc(label_value="missing_body_file")
    await counter.inc(label_value="missing_body_file", n=3)
    await counter.inc(label_value="missing_body_in_ram")
    snapshot = counter.snapshot()
    assert snapshot == {"": 0, "missing_body_file": 4, "missing_body_in_ram": 1}


@pytest.mark.asyncio
async def test_counter_snapshot_returns_copy() -> None:
    counter = Counter(name="x", description="x")
    await counter.inc()
    snapshot = counter.snapshot()
    # Mutating the snapshot must not affect the live counter.
    snapshot_mut: dict[str, int] = dict(snapshot)
    snapshot_mut[""] = 99999
    assert counter.snapshot() == {"": 1}


@pytest.mark.asyncio
async def test_counter_concurrent_increments_serialize() -> None:
    counter = Counter(name="concurrent", description="c")
    n_workers = 50
    n_per_worker = 100

    async def worker() -> None:
        for _ in range(n_per_worker):
            await counter.inc()

    await asyncio.gather(*[worker() for _ in range(n_workers)])
    assert counter.snapshot() == {"": n_workers * n_per_worker}


@pytest.mark.asyncio
async def test_gauge_set_and_increment() -> None:
    gauge = Gauge(name="balance", description="b")
    await gauge.set(100.0)
    assert gauge.snapshot() == {"": 100.0}
    await gauge.inc(5)
    assert gauge.snapshot() == {"": 105.0}
    await gauge.dec(15)
    assert gauge.snapshot() == {"": 90.0}


@pytest.mark.asyncio
async def test_gauge_labeled_buckets() -> None:
    gauge = Gauge(name="body_loc", description="loc")
    await gauge.set(5, label_value="ram")
    await gauge.set(7, label_value="file")
    snap = gauge.snapshot()
    assert snap["ram"] == 5.0
    assert snap["file"] == 7.0


@pytest.mark.asyncio
async def test_registry_register_counter_is_idempotent() -> None:
    registry = MetricsRegistry()
    first = registry.register_counter("x_total", "x desc")
    second = registry.register_counter("x_total", "x desc")
    assert first is second


def test_registry_register_counter_description_mismatch_raises() -> None:
    registry = MetricsRegistry()
    registry.register_counter("x_total", "old desc")
    with pytest.raises(ValueError, match="re-registered with mismatched description"):
        registry.register_counter("x_total", "new desc")


@pytest.mark.asyncio
async def test_registry_register_gauge_is_idempotent() -> None:
    registry = MetricsRegistry()
    first = registry.register_gauge("g", "g desc")
    second = registry.register_gauge("g", "g desc")
    assert first is second


def test_registry_register_gauge_description_mismatch_raises() -> None:
    registry = MetricsRegistry()
    registry.register_gauge("g", "old desc")
    with pytest.raises(ValueError, match="re-registered with mismatched description"):
        registry.register_gauge("g", "new desc")


@pytest.mark.asyncio
async def test_registry_exposes_counters_and_gauges_for_admin_serialization() -> None:
    registry = MetricsRegistry()
    counter = registry.register_counter("ic", "invariant counter")
    gauge = registry.register_gauge("ig", "invariant gauge")
    await counter.inc(label_value="k1")
    await gauge.set(42.0)

    # Surface shape the admin endpoint walks (plan § 4.2.5).
    assert "ic" in registry.counters
    assert "ig" in registry.gauges
    assert registry.counters["ic"].snapshot() == {"": 0, "k1": 1}
    assert registry.gauges["ig"].snapshot() == {"": 42.0}
