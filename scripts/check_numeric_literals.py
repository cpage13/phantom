#!/usr/bin/env python
"""Pre-commit hook: forbid bare numeric literals that configure behaviour.

CONTEXT.md's coding-standards bullet says a fixed constant becomes a named
module-level constant with a comment explaining the value, and a tunable
parameter belongs in configuration. This gate enforces the half a reader can
check mechanically: a raw number at a call site, where nothing names it.

It replaces ``scripts/precommit/forbid_bare_numeric_literals.sh``, which was
inert for four independent reasons, any one of them sufficient:

1. Its last line was an unconditional ``exit 0``, so a hit only printed.
2. Its regex matched only indented WHOLE-LINE assignments, so a keyword
   argument like ``timeout=30`` inside a call was invisible to it.
3. It scanned ``src/*/src/`` only, leaving ``scripts/`` unchecked.
4. ``[+\\-*/]`` is an INVALID character range under BSD grep, which every
   script on this machine resolves: POSIX brackets do not treat ``\\`` as an
   escape, so it reads as a descending range from ``\\`` (0x5C) to ``*``
   (0x2A). The grep exited 2, ``2>/dev/null`` swallowed the message, and the
   hook printed nothing at all. An interactive shell here resolves ugrep,
   which ACCEPTS the range, so the defect was invisible to anyone testing the
   pattern by hand.

Defect 4 is why this is Python and not a better regex. The gate must pass
under BSD grep here and GNU grep on ``ubuntu-latest``, and no GNU grep exists
on the development machine to test against. A design that cannot be tested in
one of the two environments it must run in is the defect, not the bracket. An
AST checker has no grep in it and the question disappears.

THE RULE. A numeric literal is flagged when it CONFIGURES BEHAVIOUR AT A CALL
SITE, which is one of:

* a keyword-argument value in a call, or
* an assignment to a name that is not a module-level UPPER_CASE constant.

DEFAULT PARAMETER VALUES ARE OUT, and this is the rule's one non-obvious
boundary. A default is part of a DECLARED INTERFACE: it appears in the
signature, in the docstring's ``Args``, and in every caller's tooling, and
changing one is an API change reviewed as such. The convention's harm is an
unexplained number a reader must decode from context, and a default is decoded
by the parameter it defaults. The two defaults that look most like tunables
prove the point: ``compression/__init__.py``'s ``level: int = 3`` and
``file_body_store.py``'s ``shard_prefix_chars: int = 2`` are both fallbacks
for knobs that already live in ``config/settings.py``, so the convention's
"a tunable belongs in configuration" is already satisfied and flagging the
fallback would demand a constant for a value the settings layer owns and
describes.

A keyword argument at a CALL is the opposite: one caller's choice, invisible
from any interface, and exactly where a policy number hides.

Ruff's ``PLR2004`` (magic value in comparison) is deliberately NOT selected
instead of this; see the exit-gate's declined list.

Exit codes: 0 = pass, 1 = violation(s) found.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Where the convention applies: production source for all three packages, plus
# the repo's own tooling. The shell hook checked only the first, which is the
# coverage gap the findings record named against CONTEXT.md's convention.
_SCAN_GLOBS: tuple[tuple[str, str], ...] = (
    ("src", "*/src/**/*.py"),
    ("scripts", "**/*.py"),
)

# Structural rather than policy values: list indices, empty checks, a single
# step, a sign. Naming them adds no information a reader does not already have.
_STRUCTURAL_VALUES: frozenset[int] = frozenset({0, 1})

# Calls whose literals are VALIDATED CONFIGURATION, not bare numbers. Their
# meaning is carried by the adjacent ``description=``, which
# scripts/check_descriptions.py already forces to be non-empty, so ``ge=0``,
# ``le=65535`` and a documented default are the convention working rather
# than violating it.
_DESCRIBED_CALLS: frozenset[str] = frozenset({"Field", "Query", "Path", "Body", "Header"})

# Keyword names whose value is a protocol constant named by the keyword
# itself. An HTTP status is the number; ``_HTTP_NOT_FOUND = 404`` reads worse.
_SELF_NAMING_KEYWORDS: frozenset[str] = frozenset({"status", "status_code"})


def _described_call_lines(tree: ast.AST) -> set[int]:
    """Line numbers belonging to a ``Field()``-family call anywhere in ``tree``."""
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name in _DESCRIBED_CALLS:
            lines.update(
                getattr(child, "lineno", 0) for child in ast.walk(node) if hasattr(child, "lineno")
            )
    return lines


def _module_constant_lines(tree: ast.Module) -> set[int]:
    """Line numbers belonging to a module-level UPPER_CASE constant definition.

    That IS the convention's prescribed form, so everything on such a line is
    exempt, including the literals inside a tuple or a call the constant is
    bound to.
    """
    lines: set[int] = set()
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            targets: list[ast.expr] = list(stmt.targets)
        elif isinstance(stmt, ast.AnnAssign):
            targets = [stmt.target]
        else:
            continue
        if stmt.value is None:
            continue
        if not any(isinstance(t, ast.Name) and t.id.isupper() for t in targets):
            continue
        lines.update(
            getattr(child, "lineno", 0)
            for child in ast.walk(stmt.value)
            if hasattr(child, "lineno")
        )
    return lines


def _is_bare_number(node: ast.expr, exempt_lines: set[int]) -> bool:
    """Whether ``node`` is a policy-bearing numeric literal on a non-exempt line."""
    if not isinstance(node, ast.Constant):
        return False
    if isinstance(node.value, bool) or not isinstance(node.value, int | float):
        return False
    if node.value in _STRUCTURAL_VALUES:
        return False
    return node.lineno not in exempt_lines


def _violations_in(path: Path, tree: ast.Module) -> list[str]:
    """Every flagged literal in one parsed module, as reportable strings."""
    exempt = _module_constant_lines(tree) | _described_call_lines(tree)
    found: list[str] = []
    # Repo-relative when the file is in the tree, absolute otherwise: the
    # self-test drives this function against planted files under tmp_path.
    rel = path.relative_to(_REPO_ROOT) if path.is_relative_to(_REPO_ROOT) else path
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg in _SELF_NAMING_KEYWORDS:
                    continue
                if _is_bare_number(keyword.value, exempt):
                    found.append(
                        f"{rel}:{keyword.value.lineno}: keyword argument "
                        f"{keyword.arg}={ast.unparse(keyword.value)} at a call site is a "
                        "bare numeric literal; bind it to a named module-level constant "
                        "with its reason, or move it to configuration"
                    )
        elif isinstance(node, ast.Assign):
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id.isupper():
                continue
            if _is_bare_number(node.value, exempt):
                found.append(
                    f"{rel}:{node.value.lineno}: assignment to "
                    f"{ast.unparse(target)} is a bare numeric literal; bind it to a "
                    "named module-level constant with its reason, or move it to "
                    "configuration"
                )
    return found


def _scan_paths() -> list[Path]:
    """Every Python file in scope, caches excluded, in a stable order."""
    paths: list[Path] = []
    for root, pattern in _SCAN_GLOBS:
        paths.extend(
            p
            for p in sorted((_REPO_ROOT / root).glob(pattern))
            if "__pycache__" not in p.parts and p.name != Path(__file__).name
        )
    return paths


def find_violations(paths: list[Path] | None = None) -> list[str]:
    """Return one message per flagged literal across ``paths``.

    Args:
        paths: Files to check. Defaults to the whole configured scope, which
            is what the pre-commit entry point uses; a test passes its own.

    Returns:
        A list of ``path:line: reason`` strings, empty when the tree passes.
    """
    found: list[str] = []
    for path in paths if paths is not None else _scan_paths():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found.extend(_violations_in(path, tree))
    return found


def main() -> int:
    """Print every violation to stderr and exit non-zero when any exist."""
    violations = find_violations()
    if violations:
        print("Bare numeric literals in production code:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
