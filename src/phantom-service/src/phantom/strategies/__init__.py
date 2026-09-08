"""Retry-strategy plugins and the config-to-strategy builder."""

from __future__ import annotations

from phantom.config.settings import RetryStrategyCfg
from phantom.strategies.exponential_backoff import ExponentialBackoffStrategy
from phantom.strategies.fixed_intervals import FixedIntervalsStrategy
from phantom.strategies.interface import UploadStrategy


def build_retry_strategy(cfg: RetryStrategyCfg) -> UploadStrategy:
    """Build the configured retry strategy from a settings block.

    Shared by the composition root (boot) and the hot-reload handler
    (``apply_reload`` rebuilds :attr:`InstanceContext.retry_strategy`
    so reloaded retry parameters apply to subsequent scheduling
    decisions, per ADR-013).

    The builder substitutes nothing. It used to swap a five-step fallback
    schedule in for an empty ``intervals_seconds``, which the exported
    contract did not carry, so an implementation built from
    ``contracts/settings.schema.json`` (the ADR-035 acceptance basis) built an
    empty schedule and sent every row to ``stored`` after ONE failure. The
    default now lives on :class:`RetryStrategyCfg`, where it exports, and an
    explicitly empty list means what it says: no retries.

    Args:
        cfg: The ``retry.default_strategy`` block.

    Returns:
        A :class:`FixedIntervalsStrategy` or
        :class:`ExponentialBackoffStrategy` per ``cfg.type``.
    """
    if cfg.type == "fixed_intervals":
        return FixedIntervalsStrategy(cfg.intervals_seconds, jitter=cfg.jitter)
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
