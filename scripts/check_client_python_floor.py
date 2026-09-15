#!/usr/bin/env python
"""Falsifiability: ``phantom-client`` really runs on the floor it advertises.

Review finding S13-9. ``src/phantom-client/pyproject.toml`` declares
``requires-python = ">=3.12"``, ``README.md``, the client README and
``CONTEXT.md`` all promise 3.12+, and CONTEXT.md adds the rule that code
landing in the SDK may not use a 3.13-only or 3.14-only feature. Every CI
lane ran only 3.14, and the workspace ``uv.lock`` is pinned ``>=3.14``
because the root and the two service-side members force that intersection.
So the declared floor was never RESOLVED and never EXECUTED: a 3.13-only
typing import could ship to PyPI and fail on a consumer's 3.12 interpreter
with every lane green.

This script is the half of the floor lane that a pytest run cannot cover.
It does two things:

1. ASSERTS THE RUNNING INTERPRETER IS THE DECLARED FLOOR, read out of the
   client's own ``pyproject.toml`` rather than hardcoded here. Without this
   the lane could be handed a 3.14 interpreter by a workflow edit and go on
   passing while proving nothing, which is the exact shape of every finding
   in the gates-that-cannot-fail cluster. The floor moves when the package
   says it moves.
2. IMPORTS EVERY MODULE IN THE PACKAGE. The unit suite imports what it
   exercises; a 3.13-only import in a module no test touches would still
   reach PyPI. Walking the package closes that gap.

This script runs UNDER THE FLOOR INTERPRETER, so unlike the rest of
``scripts/`` it must stay within that floor's language level. Keep it to
syntax and stdlib available on the declared floor; ``tomllib`` (3.11+) is
the newest thing used here.

Exit codes:
- 0: the interpreter is the declared floor and every module imports.
- 1: wrong interpreter, or a module does not import on the floor.
- 2: the client's ``pyproject.toml`` or its ``requires-python`` could not be
     read (inconclusive, surfaced loudly rather than a false clean exit).

Run via (on the floor interpreter, NOT the workspace's 3.14 venv):
    ``python scripts/check_client_python_floor.py``
"""

from __future__ import annotations

import importlib
import pkgutil
import re
import sys
import tomllib
from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
_CLIENT_PYPROJECT: Final[Path] = _REPO_ROOT / "src" / "phantom-client" / "pyproject.toml"
_PACKAGE_NAME: Final[str] = "phantom_client"

# Matches the lower bound of a ">=X.Y" requires-python specifier, which is the
# only form this package uses. Anything else is reported as inconclusive
# rather than guessed at.
_FLOOR_PATTERN: Final[re.Pattern[str]] = re.compile(r">=\s*(\d+)\.(\d+)")


class FloorCheckError(Exception):
    """The declared floor could not be determined, so the check is inconclusive."""


def declared_floor(pyproject: Path) -> tuple[int, int]:
    """Return the ``(major, minor)`` floor declared by ``requires-python``.

    Args:
        pyproject: Path to the client package's ``pyproject.toml``.

    Returns:
        The lower bound of the ``requires-python`` specifier.

    Raises:
        FloorCheckError: If the file is unreadable, unparseable, or its
            ``requires-python`` carries no ``>=X.Y`` lower bound.
    """
    try:
        raw = pyproject.read_bytes()
    except OSError as exc:
        raise FloorCheckError(f"cannot read {pyproject}: {exc}") from exc
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise FloorCheckError(f"{pyproject} is not valid TOML: {exc}") from exc
    project = data.get("project")
    specifier = project.get("requires-python") if isinstance(project, dict) else None
    if not isinstance(specifier, str):
        raise FloorCheckError(f"{pyproject}: no [project] requires-python to enforce")
    match = _FLOOR_PATTERN.search(specifier)
    if match is None:
        raise FloorCheckError(
            f"{pyproject}: requires-python {specifier!r} carries no '>=X.Y' lower "
            "bound, so there is no floor for this lane to run on"
        )
    return int(match.group(1)), int(match.group(2))


def check_interpreter(floor: tuple[int, int]) -> list[str]:
    """Return a message unless the running interpreter IS the declared floor.

    Equality, not ``>=``: a lane that satisfies the floor by running 3.14
    proves nothing about 3.12, and that is the state S13-9 found.
    """
    running = (sys.version_info.major, sys.version_info.minor)
    if running == floor:
        return []
    floor_text = f"{floor[0]}.{floor[1]}"
    running_text = f"{running[0]}.{running[1]}"
    return [
        f"this lane must run on the DECLARED FLOOR {floor_text}, but the "
        f"interpreter is {running_text} ({sys.executable}). Running above the "
        "floor makes the lane pass without ever exercising the floor, which is "
        "the false-green this check exists to prevent (S13-9)."
    ]


def check_every_module_imports() -> list[str]:
    """Return a message per package module that fails to import.

    Walks the whole ``phantom_client`` package rather than trusting the unit
    suite's import graph, so a 3.13-only construct in a module no test touches
    is still caught before it reaches PyPI.
    """
    try:
        package = importlib.import_module(_PACKAGE_NAME)
    except ImportError as exc:
        return [f"{_PACKAGE_NAME} does not import at all: {exc}"]

    search_paths = getattr(package, "__path__", None)
    if search_paths is None:
        return [f"{_PACKAGE_NAME} is not a package; nothing to walk"]

    names = [_PACKAGE_NAME]
    names.extend(module.name for module in pkgutil.walk_packages(search_paths, f"{_PACKAGE_NAME}."))
    violations: list[str] = []
    for name in names:
        try:
            importlib.import_module(name)
        except Exception as exc:
            violations.append(f"{name} does not import: {type(exc).__name__}: {exc}")
    if not violations:
        version = f"{sys.version_info.major}.{sys.version_info.minor}"
        print(f"all {len(names)} {_PACKAGE_NAME} modules import on Python {version}")
    return violations


def main() -> int:
    """Entry point for the declared-floor check."""
    try:
        floor = declared_floor(_CLIENT_PYPROJECT)
    except FloorCheckError as exc:
        print(f"floor check inconclusive: {exc}", file=sys.stderr)
        return 2

    violations = check_interpreter(floor)
    if violations:
        print("declared-floor violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1

    print(f"interpreter is the declared floor {floor[0]}.{floor[1]} ({sys.executable})")
    violations = check_every_module_imports()
    if violations:
        print("declared-floor violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
