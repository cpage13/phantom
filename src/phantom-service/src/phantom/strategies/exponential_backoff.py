"""ExponentialBackoffStrategy - base * factor**attempts capped + jittered."""

from __future__ import annotations

import random
from datetime import timedelta


class ExponentialBackoffStrategy:
    """Capped, jittered exponential backoff.

    Returns ``None`` when ``attempts > max_attempts`` (when set) or
    ``since_received > max_duration_seconds`` (when set). ``-1`` for
    either field means "unbounded."
    """

    def __init__(
        self,
        *,
        base_seconds: float,
        factor: float,
        cap_seconds: float,
        jitter: float,
        max_attempts: int,
        max_duration_seconds: int,
    ) -> None:
        """Construct the strategy.

        Args:
            base_seconds: First-interval base (e.g., 5).
            factor: Multiplier per attempt (e.g., 4 → 5, 20, 80, ...).
            cap_seconds: Upper bound after exponentiation.
            jitter: Uniform jitter fraction applied (e.g., 0.2 → ±20%).
            max_attempts: Attempt budget; -1 = unbounded.
            max_duration_seconds: Wall-clock budget; -1 = unbounded.
        """
        self._base = base_seconds
        self._factor = factor
        self._cap = cap_seconds
        self._jitter = jitter
        self._max_attempts = max_attempts
        self._max_duration = max_duration_seconds

    def schedule_next_attempt(
        self,
        *,
        attempts: int,
        since_received: timedelta,
        last_error: str | None,
        route_name: str,
    ) -> timedelta | None:
        """Compute the next delay or ``None`` when budget is exhausted."""
        del last_error, route_name
        if 0 <= self._max_attempts <= attempts:
            return None
        if 0 <= self._max_duration <= int(since_received.total_seconds()):
            return None
        base = min(self._cap, self._base * (self._factor**attempts))
        jitter_factor = 1.0 + random.uniform(-self._jitter, self._jitter)
        seconds = max(0.0, base * jitter_factor)
        return timedelta(seconds=seconds)
