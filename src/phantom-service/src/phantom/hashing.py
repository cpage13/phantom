"""Hashing helpers shared by ingress and the sender (ADR-014).

This module is a deliberate leaf: its only import is ``hashlib``, so neither
``phantom.routes`` nor ``phantom.workers`` can close an import cycle through
it. That property is the reason the helper lives here rather than being
imported from ``routes/admission.py``, where it used to be private: an
admission-to-sender import edge succeeds under the application's boot order
and raises ``ImportError`` under a bare ``pytest`` run of any of the ten unit
test modules that import ``phantom.routes.admission`` first (CL4).
"""

from __future__ import annotations

import hashlib


def sha256_hex(data: bytes) -> str:
    """SHA-256 hex of ``data``, top-level so ``asyncio.to_thread`` can target it."""
    return hashlib.sha256(data).hexdigest()
