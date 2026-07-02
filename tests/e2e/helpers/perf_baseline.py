"""Per-machine performance baselines: captured first, gated after.

Cycle-7 plan 06_09 task 7.2. Wall-clock performance is machine-bound,
so the regression reference is a JSON baseline PER MACHINE CLASS
(keyed by a generic slug built from OS, CPU architecture, core count,
and RAM size, never the hostname, with the machine's facts attached so
the file is self-describing), stored under ``tests/e2e/perf_baselines/``
and committed: this class's file is the reference for this machine;
a machine without a file CAPTURES on its first run (no assertion) and
gates from the second run on.

Gate semantics and the tolerance band (the rationale is load-bearing,
keep it in sync with the constants below):

* ``delivered_throughput_uploads_per_second`` is gated by a FLOOR at
  ``THROUGHPUT_FLOOR_FRACTION`` of the baseline. Adjacent runs on a
  developer machine jitter by tens of percent (thermal state,
  background load, cache effects), so the floor sits at half the
  reference: a genuine halving of end-to-end throughput is beyond any
  observed noise and is exactly the signature of the regression
  classes this gate exists for (accidental serialization of the send
  path, lost worker concurrency, an admission bottleneck).
* ``admin_read_p95_seconds`` is gated by a CEILING at
  ``READ_P95_CEILING_FACTOR`` times the baseline. The p95 of
  millisecond-scale loopback reads can double under host load without
  meaning anything; the contention classes that matter (the reader
  blocking behind the writer, a checkpoint stall, an N+1 fan-out)
  shift p95 by an order of magnitude, so a 3x ceiling is generous to
  noise and still catches every real offender.

The band is deliberately asymmetric to each metric's failure mode and
deliberately NOT tighter: a flaky gate that operators learn to ignore
protects nothing.
"""

from __future__ import annotations

import logging
import math
import os
import platform
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

# Faults the RAM-size sysconf probe can raise on platforms without the
# names. Bound to a constant rather than an inline no-``as``
# except-tuple: ruff 0.15.x strips the parens from the inline form
# under Python 3.14, producing a syntax error (the project's
# established workaround; see tests/e2e/_harness/subprocess_harness.py).
_SYSCONF_PROBE_ERRORS: Final[tuple[type[BaseException], ...]] = (ValueError, OSError)

# Bump when the BASELINE FILE SHAPE changes (fields added/renamed); a
# baseline with a different schema version is unreadable as a reference
# and is re-captured.
BASELINE_SCHEMA_VERSION: int = 1

# Where the per-machine baseline files live (committed; one file per
# machine key).
PERF_BASELINES_DIR: Path = Path(__file__).resolve().parent.parent / "perf_baselines"

# Throughput floor: gate fails when the measured throughput drops below
# this fraction of the baseline. See the module docstring for the
# rationale behind the 50% band.
THROUGHPUT_FLOOR_FRACTION: float = 0.5

# Read-latency ceiling: gate fails when the measured admin-read p95
# exceeds this multiple of the baseline. See the module docstring for
# the rationale behind the 3x band.
READ_P95_CEILING_FACTOR: float = 3.0

# The p95 quantile used for the read-latency metric.
READ_LATENCY_QUANTILE: float = 0.95

# Machine-key slug: lowercase runs of anything non-alphanumeric become
# single hyphens.
_MACHINE_KEY_INVALID_RUNS: re.Pattern[str] = re.compile(r"[^a-z0-9]+")

# RAM-size divisor for the machine key's GiB component.
BYTES_PER_GIB: Final[int] = 1024**3

# Used when the probed facts produce an empty slug (exotic platform
# builds where the probes return empty strings); such a machine always
# captures into (and gates against) this shared key.
FALLBACK_MACHINE_KEY: str = "unknown-machine"


class MachineFacts(BaseModel):
    """The capturing machine's facts, attached to its baseline file."""

    model_config = ConfigDict(extra="forbid")

    os_name: str = Field(..., description="Operating system name (platform.system()).")
    os_release: str = Field(..., description="OS release string (platform.release()).")
    cpu_arch: str = Field(..., description="CPU architecture (platform.machine()).")
    cpu_count: int = Field(..., ge=1, description="Logical CPU count (os.cpu_count()).")
    total_ram_bytes: int = Field(
        ..., ge=0, description="Total physical RAM in bytes (0 when the probe is unavailable)."
    )
    python_version: str = Field(..., description="CPython version the suite ran under.")


class PerfMetrics(BaseModel):
    """One measured run's gated metrics plus its workload context."""

    model_config = ConfigDict(extra="forbid")

    delivered_throughput_uploads_per_second: float = Field(
        ...,
        gt=0,
        description=(
            "End-to-end throughput: uploads submitted concurrently across "
            "every instance divided by the wall time from first submission "
            "to the last upload reaching succeeded."
        ),
    )
    admin_read_p95_seconds: float = Field(
        ...,
        gt=0,
        description=(
            "p95 latency over the mixed admin reads (group rollup, "
            "identifier lookup, list page, detail) issued concurrently "
            "with the delivery storm."
        ),
    )
    admin_read_count: int = Field(
        ...,
        ge=1,
        description="How many admin reads the p95 was computed over (context, not gated).",
    )
    uploads_total: int = Field(
        ...,
        ge=1,
        description=(
            "Total uploads in the measured burst. A baseline whose "
            "workload size differs from the current run's is stale and "
            "must be re-captured, never gated against."
        ),
    )


class PerfBaseline(BaseModel):
    """The on-disk per-machine baseline file."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(
        ..., description="Baseline file shape version (BASELINE_SCHEMA_VERSION)."
    )
    machine_key: str = Field(..., description="Stable machine identity slug (the filename stem).")
    captured_at: datetime = Field(..., description="UTC capture time of this reference run.")
    facts: MachineFacts = Field(..., description="The capturing machine's facts.")
    metrics: PerfMetrics = Field(..., description="The reference metrics gates compare against.")


def machine_key() -> str:
    """Return this machine's stable identity slug.

    The key derives from generic machine-class facts (OS name, CPU
    architecture, core count, RAM size), NEVER from the hostname: this
    slug becomes a committed filename, and hostnames routinely embed
    usernames, hardware model names, and device serial numbers, none of
    which belong in the tree. Machines of the same class share a key,
    which is the intent: the baseline records what this class of
    machine can do, and the ``facts`` block keeps the file
    self-describing. An unseen machine class simply captures its own
    baseline on first run, which is the safe default. Slugged to
    lowercase alphanumerics and hyphens so it is a valid filename stem
    everywhere.
    """
    facts = collect_machine_facts()
    ram_gib = round(facts.total_ram_bytes / BYTES_PER_GIB)
    raw = f"{facts.os_name}-{facts.cpu_arch}-{facts.cpu_count}c-{ram_gib}g".lower()
    slug = _MACHINE_KEY_INVALID_RUNS.sub("-", raw).strip("-")
    return slug or FALLBACK_MACHINE_KEY


def collect_machine_facts() -> MachineFacts:
    """Probe the current machine's facts for the baseline file."""
    try:
        total_ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except _SYSCONF_PROBE_ERRORS:  # pragma: no cover - platform without the sysconf names
        total_ram = 0
    cpu_count = os.cpu_count()
    return MachineFacts(
        os_name=platform.system(),
        os_release=platform.release(),
        cpu_arch=platform.machine(),
        cpu_count=cpu_count if cpu_count is not None else 1,
        total_ram_bytes=total_ram,
        python_version=platform.python_version(),
    )


def baseline_path_for(key: str) -> Path:
    """Return the baseline file path for ``key`` under the committed dir."""
    return PERF_BASELINES_DIR / f"{key}.json"


def load_baseline(path: Path) -> PerfBaseline | None:
    """Load the baseline at ``path``; None when absent or shape-mismatched.

    A missing file means CAPTURE mode (first run on this machine). A
    file with a different ``schema_version`` is unreadable as a
    reference; it logs and reads as absent so the caller re-captures.
    """
    if not path.is_file():
        return None
    baseline = PerfBaseline.model_validate_json(path.read_text(encoding="utf-8"))
    if baseline.schema_version != BASELINE_SCHEMA_VERSION:
        logger.warning(
            "perf baseline %s has schema_version=%d (current %d); re-capturing",
            path,
            baseline.schema_version,
            BASELINE_SCHEMA_VERSION,
        )
        return None
    return baseline


def capture_baseline(path: Path, facts: MachineFacts, metrics: PerfMetrics) -> PerfBaseline:
    """Write a fresh baseline file for this machine and return it."""
    baseline = PerfBaseline(
        schema_version=BASELINE_SCHEMA_VERSION,
        machine_key=path.stem,
        captured_at=datetime.now(tz=UTC),
        facts=facts,
        metrics=metrics,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(baseline.model_dump_json(indent=2) + "\n", encoding="utf-8")
    logger.info("perf baseline CAPTURED at %s: %s", path, baseline.metrics)
    return baseline


def evaluate_gates(baseline: PerfBaseline, current: PerfMetrics) -> list[str]:
    """Compare ``current`` against ``baseline``; return breach descriptions.

    An empty list means every gate passed. A workload-size mismatch is
    reported as a breach instructing re-capture (comparing differently
    sized runs is meaningless, and silently passing would disable the
    gate).
    """
    breaches: list[str] = []
    if current.uploads_total != baseline.metrics.uploads_total:
        breaches.append(
            f"workload changed: baseline measured {baseline.metrics.uploads_total} uploads, "
            f"this run measured {current.uploads_total}; delete "
            f"{baseline_path_for(baseline.machine_key)} and re-capture"
        )
        return breaches
    throughput_floor = (
        baseline.metrics.delivered_throughput_uploads_per_second * THROUGHPUT_FLOOR_FRACTION
    )
    if current.delivered_throughput_uploads_per_second < throughput_floor:
        breaches.append(
            "delivered throughput regressed: "
            f"{current.delivered_throughput_uploads_per_second:.2f} uploads/s < floor "
            f"{throughput_floor:.2f} (baseline "
            f"{baseline.metrics.delivered_throughput_uploads_per_second:.2f} "
            f"x {THROUGHPUT_FLOOR_FRACTION})"
        )
    p95_ceiling = baseline.metrics.admin_read_p95_seconds * READ_P95_CEILING_FACTOR
    if current.admin_read_p95_seconds > p95_ceiling:
        breaches.append(
            "admin read p95 regressed: "
            f"{current.admin_read_p95_seconds:.4f}s > ceiling {p95_ceiling:.4f}s "
            f"(baseline {baseline.metrics.admin_read_p95_seconds:.4f}s "
            f"x {READ_P95_CEILING_FACTOR})"
        )
    return breaches


def percentile(samples: list[float], quantile: float) -> float:
    """Return the ``quantile`` percentile of ``samples`` (nearest-rank).

    Args:
        samples: Non-empty list of measurements.
        quantile: In (0, 1]; e.g. :data:`READ_LATENCY_QUANTILE`.

    Raises:
        ValueError: When ``samples`` is empty or ``quantile`` is out of
            range.
    """
    if not samples:
        raise ValueError("percentile() needs at least one sample")
    if not 0 < quantile <= 1:
        raise ValueError(f"quantile must be in (0, 1], got {quantile}")
    ordered = sorted(samples)
    rank = math.ceil(quantile * len(ordered))
    return ordered[rank - 1]
