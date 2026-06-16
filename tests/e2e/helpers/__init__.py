"""Helpers for the cross-package E2E suite.

The modules under this package compose the three-package stack
(phantom, phantom-client, phantom-emulator) into a single test fixture
so individual tests can assert against driver-side return values,
Phantom's admin API, and the emulator's `/control/received` log on one
shared scaffold.
"""

from __future__ import annotations
