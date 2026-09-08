"""UploadStrategy Protocol - pure-function retry scheduling (req §5e)."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol

# Floor on any scheduled delay, shared by every strategy. A delay of zero
# means the row's ``next_attempt_at`` is already in the past, so the sender
# re-claims it on its very next poll (``retry.poll_interval_ms``, 250 ms by
# default) and a failing upstream is hammered in a tight loop for the whole
# retry budget. Jitter is what drives a computed delay to zero, so the floor
# is applied AFTER jitter, last of all. One second is long enough to break
# the loop and short enough to be invisible against the smallest schedule a
# strategy can be configured with.
MIN_RETRY_DELAY_SECONDS: float = 1.0


class UploadStrategy(Protocol):
    """Schedule the next retry delay.

    Implementations are pure-function: no I/O, no state across calls.
    The sender consults the strategy after every failed attempt.
    """

    def schedule_next_attempt(
        self,
        *,
        attempts: int,
        since_received: timedelta,
        last_error: str | None,
        route_name: str,
    ) -> timedelta | None:
        """Return the delay until the next attempt.

        Args:
            attempts: How many attempts have completed so far.
            since_received: Wall-clock duration since ingress.
            last_error: Short error string from the most recent attempt
                (``None`` on first call).
            route_name: The resolved route name (allows per-route logic).

        Returns:
            ``timedelta`` to wait before the next attempt, or ``None``
            when the budget is exhausted (chain → ``stored``).
        """
        ...
