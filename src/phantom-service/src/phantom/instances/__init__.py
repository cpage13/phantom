"""Per-instance composition (ADR-006)."""

from __future__ import annotations

from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import (
    InstanceDispatcher,
    InstanceNotFoundError,
    NoMatchingInstanceError,
)

__all__ = [
    "InstanceContext",
    "InstanceDispatcher",
    "InstanceNotFoundError",
    "NoMatchingInstanceError",
]
