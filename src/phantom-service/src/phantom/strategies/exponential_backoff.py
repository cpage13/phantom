"""ExponentialBackoffStrategy - base * factor**attempts, jittered then capped."""

from __future__ import annotations

import random
import sys
from datetime import timedelta

from phantom.strategies.interface import MIN_RETRY_DELAY_SECONDS

# Stand-in for an exponential term that overflowed the float range. A row
# retrying under an unbounded attempt AND duration budget eventually drives
# ``factor ** attempts`` past ``sys.float_info.max`` and ``float.__pow__``
# raises OverflowError, which would take the sender worker (and with it the
# process) down. Saturating is the right answer because everything this large
# is clamped to ``cap_seconds`` by the cap below anyway.
_OVERFLOW_GROWTH_SECONDS: float = sys.float_info.max


class ExponentialBackoffStrategy:
    """Jittered, then capped, exponential backoff.

    Returns ``None`` when ``attempts > max_attempts`` (when set) or
    ``since_received > max_duration_seconds`` (when set). ``-1`` for
    either field means "unbounded."

    Order matters: jitter is applied to the raw exponential term and the cap
    is applied to the JITTERED value, so ``cap_seconds`` is the real ceiling
    on a scheduled delay. Capping first let a jitter factor above 1 return
    multiples of the cap. A final floor
    (:data:`~phantom.strategies.interface.MIN_RETRY_DELAY_SECONDS`) keeps the
    delay positive whatever the jitter draw was.
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
            cap_seconds: Upper bound on the JITTERED delay - the real
                ceiling, applied last but one (the positive-delay floor
                wins over it).
            jitter: Uniform jitter fraction applied (e.g., 0.2 → ±20%).
                Bounded at 1.0 by ``RetryStrategyCfg.jitter``.
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
        try:
            growth = self._base * (self._factor**attempts)
        except OverflowError:
            growth = _OVERFLOW_GROWTH_SECONDS
        jitter_factor = 1.0 + random.uniform(-self._jitter, self._jitter)
        # Jitter, THEN cap, THEN floor: the cap bounds what is actually
        # scheduled, and the floor guarantees a strictly positive delay so a
        # jitter draw can never turn the retry into an immediate re-claim.
        return timedelta(
            seconds=max(MIN_RETRY_DELAY_SECONDS, min(self._cap, growth * jitter_factor))
        )
