"""UploadStrategy Protocol — pure-function retry scheduling (req §5e)."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol


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
