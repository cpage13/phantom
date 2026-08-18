"""FixedIntervalsStrategy - schedule with a caller-supplied interval list."""

from __future__ import annotations

import random
from datetime import timedelta


class FixedIntervalsStrategy:
    """Return the i-th interval (optionally jittered); None past the end."""

    def __init__(self, intervals_seconds: list[int], *, jitter: float = 0.0) -> None:
        """Construct with a list of integer seconds + an optional jitter fraction.

        Args:
            intervals_seconds: e.g., ``[1, 5, 20, 60, 300]``.
            jitter: Uniform jitter fraction applied to each interval (e.g.
                ``0.2`` → ±20%), parity with
                :class:`~phantom.strategies.exponential_backoff.ExponentialBackoffStrategy`.
                ``0.0`` (the default) preserves the exact fixed-interval
                schedule. THUNDERING HERD (V5-C): without jitter, a backlog of
                rows that fail in the same poll round all get the SAME delay →
                the same ``next_attempt_at`` → they fire in lockstep on upstream
                recovery, hammering a recovering upstream with a synchronized
                burst. A non-zero jitter de-correlates the backlog so the retry
                spread widens.
        """
        self._intervals = list(intervals_seconds)
        self._jitter = jitter

    def schedule_next_attempt(
        self,
        *,
        attempts: int,
        since_received: timedelta,
        last_error: str | None,
        route_name: str,
    ) -> timedelta | None:
        """Return ``intervals[attempts]`` (jittered) as a timedelta, or None past the end."""
        del since_received, last_error, route_name
        if attempts >= len(self._intervals):
            return None
        base = float(self._intervals[attempts])
        if self._jitter:
            base = max(0.0, base * (1.0 + random.uniform(-self._jitter, self._jitter)))
        return timedelta(seconds=base)
