"""Phantom runtime package.

The composition root — the SOLE site of long-lived task spawning in
production code (plan § 0.5 / § 4 / strategy §4) — is
:func:`phantom.app.create_app`'s FastAPI ``lifespan``. Every supervised
worker is scheduled under a single :class:`asyncio.TaskGroup` there; the
pre-commit ``forbid-asyncio-create-task-outside-composition`` hook
enforces that greppably.

The boot-time guards the lifespan runs before any instance opens its
store (umask, retention-floor, instance-isolation, the all_ram mode
guard, the DB integrity gate) live in
:mod:`phantom.runtime.startup_checks`, the single typed home shared by
the lifespan. That module also owns the one mode-wiring decision table
(:func:`phantom.runtime.startup_checks.build_body_store`).

Provenance: the dead-path ``compose_and_run`` / ``Runtime`` composition
shim was removed in the 2026-05-29 refinement (R-2). ``app.py``'s
lifespan is the one production composition root; its tests now drive
that real path (``create_app`` + the e2e ``boot_stack`` harness).
"""

from __future__ import annotations
