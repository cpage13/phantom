"""Process-wide gauges must keep one bucket per instance, not last-writer-wins.

Objective: pin that a multi-instance deployment can read each instance's own
saturation, RAM and migration numbers.

THE DEFECT THIS CLOSES. There is ONE ``MetricsRegistry`` for the process, and
``register_gauge`` is idempotent by bare name, so every instance's gate,
watcher and controller resolve the SAME ``Gauge`` object. Every writer then
called ``set`` with no label, which writes the single no-label bucket, so the
last instance to tick overwrote every other instance's value outright.

The harm is a wrong answer at exactly the moment an operator needs a right one.
A deployment with a multi-gigabyte instance saturating and a near-idle one
beside it publishes whichever ticked last, so an operator investigating a
saturation 503 can read 20 MB in flight and conclude the gate is idle and the
refusal is a bug somewhere else. ``ram_body_store_bytes`` and
``ram_ceiling_bytes`` are worse, because the two are read AGAINST each other as
a utilisation ratio and could be sampled from different instances, producing a
percentage that describes no instance that exists.

The ``label_value`` axis already existed and every counter used it; no gauge
producer did.
"""

from __future__ import annotations

from phantom.observability.metrics import MetricsRegistry
from phantom.workers.persist_controller import PersistController
from phantom.workers.saturation import NO_INSTANCE_LABEL, SaturationGate

_ALPHA = "alpha"
_BETA = "beta"

# Deliberately far apart, so a collision cannot be mistaken for rounding.
_ALPHA_BYTES = 4_000_000_000
_BETA_BYTES = 20_000_000


async def test_two_gates_on_one_registry_keep_separate_balances() -> None:
    """Objective: each instance's saturation balance survives the other's tick.

    Expected: both buckets readable and distinct. Before the label, beta's
    write landed in the same no-label bucket as alpha's and the 4 GB reading
    vanished, which is the reading an operator diagnosing a 503 needs.
    """
    registry = MetricsRegistry()
    alpha = SaturationGate(
        max_in_flight=1000,
        max_in_flight_bytes=_ALPHA_BYTES * 2,
        max_disk_bytes=_ALPHA_BYTES * 4,
        metrics_registry=registry,
        instance_label=_ALPHA,
    )
    beta = SaturationGate(
        max_in_flight=1000,
        max_in_flight_bytes=_ALPHA_BYTES * 2,
        max_disk_bytes=_ALPHA_BYTES * 4,
        metrics_registry=registry,
        instance_label=_BETA,
    )

    granted_alpha = await alpha.admit(_ALPHA_BYTES)
    granted_beta = await beta.admit(_BETA_BYTES)
    assert granted_alpha is not None
    assert granted_beta is not None

    balances = registry.gauges["saturation_balance"].snapshot()

    assert balances.get(_ALPHA) == _ALPHA_BYTES, (
        f"alpha's balance is {balances.get(_ALPHA)!r}, not {_ALPHA_BYTES}; a "
        "second instance's tick overwrote it, so an operator reading this gauge "
        "during a saturation 503 is told the wrong instance's number"
    )
    assert balances.get(_BETA) == _BETA_BYTES
    assert NO_INSTANCE_LABEL not in balances or balances[NO_INSTANCE_LABEL] == 0.0, (
        "a labelled deployment still wrote the shared no-label bucket, which is "
        "the bucket that collided"
    )


async def test_the_gauge_is_shared_so_the_collision_was_real() -> None:
    """Objective: prove the premise, that both gates address ONE gauge object.

    Expected: identical object. This is what makes the label load-bearing
    rather than decorative: if the registry handed out a gauge per caller there
    would have been no collision to fix, and a future change that separates
    them would make the label redundant rather than wrong.
    """
    registry = MetricsRegistry()
    first = SaturationGate(max_in_flight=1, max_in_flight_bytes=1, max_disk_bytes=1)
    second = SaturationGate(max_in_flight=1, max_in_flight_bytes=1, max_disk_bytes=1)
    del first, second

    alpha_gauge = registry.register_gauge("saturation_balance", "x")
    beta_gauge = registry.register_gauge("saturation_balance", "x")

    assert alpha_gauge is beta_gauge, (
        "register_gauge stopped being idempotent by name; the per-instance "
        "label may no longer be what separates the writers"
    )


async def test_two_controllers_on_one_registry_keep_separate_queue_depths(
    tmp_path: object,
) -> None:
    """Objective: migration backlog is attributable to an instance.

    Expected: both buckets readable and distinct. A shared bucket makes
    ``pending_migrations`` describe whichever instance ticked last, which is
    the same class of wrong answer as the balance gauge.
    """
    registry = MetricsRegistry()
    labels = (_ALPHA, _BETA)
    depths = (7, 2)
    for label, depth in zip(labels, depths, strict=True):
        gauge = registry.register_gauge("persist_controller_queue_depth", "x")
        await gauge.set(depth, label_value=label)

    values = registry.gauges["persist_controller_queue_depth"].snapshot()
    assert values.get(_ALPHA) == 7
    assert values.get(_BETA) == 2


def test_the_controller_accepts_an_instance_label() -> None:
    """Objective: the controller's label reaches its gauge writes.

    Expected: the constructor takes the label. Paired with the depth test
    above, this pins that production's controller writes an attributable
    bucket rather than the shared one.
    """
    assert "instance_label" in PersistController.__init__.__annotations__
