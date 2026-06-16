"""Pytest fixtures shared across the phantom-emulator test suite."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _clear_phantom_emulator_env() -> Iterator[None]:
    """Remove any stray ``PHANTOM_EMULATOR_*`` env vars between tests.

    Prevents cross-test leakage when one test sets an env var via
    ``monkeypatch`` and a later test relies on the defaults.
    """
    snapshot = {k: v for k, v in os.environ.items() if k.startswith("PHANTOM_EMULATOR_")}
    for k in snapshot:
        del os.environ[k]
    try:
        yield
    finally:
        for k, v in snapshot.items():
            os.environ[k] = v
