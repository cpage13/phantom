"""Retry-strategy plugins and the config-to-strategy builder."""

from __future__ import annotations

from typing import Final

from phantom.config.settings import RetryStrategyCfg
from phantom.strategies.exponential_backoff import ExponentialBackoffStrategy
from phantom.strategies.fixed_intervals import FixedIntervalsStrategy
from phantom.strategies.interface import UploadStrategy

# Fallback schedule when ``type == "fixed_intervals"`` but the YAML
# leaves ``intervals_seconds`` empty: quick first retries, then back
# off to five minutes (the long-standing composition-root default the
# builder carried in app.py before R5-2 moved it here).
_DEFAULT_FIXED_INTERVALS_SECONDS: Final[list[int]] = [1, 5, 20, 60, 300]


def build_retry_strategy(cfg: RetryStrategyCfg) -> UploadStrategy:
    """Build the configured retry strategy from a settings block.

    Shared by the composition root (boot) and the hot-reload handler
    (``apply_reload`` rebuilds :attr:`InstanceContext.retry_strategy`
    so reloaded retry parameters apply to subsequent scheduling
    decisions, per ADR-013).

    Args:
        cfg: The ``retry.default_strategy`` block.

    Returns:
        A :class:`FixedIntervalsStrategy` or
        :class:`ExponentialBackoffStrategy` per ``cfg.type``.
    """
    if cfg.type == "fixed_intervals":
        return FixedIntervalsStrategy(
            cfg.intervals_seconds or _DEFAULT_FIXED_INTERVALS_SECONDS,
            jitter=cfg.jitter,
        )
    return ExponentialBackoffStrategy(
        base_seconds=cfg.base_seconds,
        factor=cfg.factor,
        cap_seconds=cfg.cap_seconds,
        jitter=cfg.jitter,
        max_attempts=cfg.max_attempts,
        max_duration_seconds=cfg.max_duration_seconds,
    )


__all__ = [
    "ExponentialBackoffStrategy",
    "FixedIntervalsStrategy",
    "UploadStrategy",
    "build_retry_strategy",
]
