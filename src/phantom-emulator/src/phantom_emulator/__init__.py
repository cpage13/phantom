"""phantom_emulator — generic upstream emulator with two-step-upload endpoints.

Re-exports the public surface:

- :class:`AppConfig` — full configuration model.
- :func:`start_server`, :class:`Server` — programmatic in-process startup.
- :class:`FailurePolicy` — failure-injection policy.
- :class:`AuthMode` — auth-mode enumeration.

See ``docs/architecture-intent.md`` for the broader design.
"""

from __future__ import annotations

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig
from phantom_emulator.failure.injection import FailurePolicy
from phantom_emulator.server import Server, start_server

__version__ = "0.1.0"

__all__ = [
    "AppConfig",
    "AuthMode",
    "FailurePolicy",
    "Server",
    "__version__",
    "start_server",
]
