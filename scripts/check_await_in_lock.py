#!/usr/bin/env python
"""Pre-commit hook: forbid ``await`` inside a lock that guards in-memory state.

Replaces ``scripts/precommit/forbid_await_in_lock.sh``, which could never fail.

WHY THE SHELL HOOK WAS INERT. Its pattern was::

    grep -rEn 'async with [a-zA-Z_]+_lock:' --include='*.py' -A 50 src/

``[a-zA-Z_]+_lock`` matches a BARE NAME. Every lock acquisition in this
codebase is an ATTRIBUTE expression on ``self``: ``async with self._lock:``,
``async with self._write_lock:``. The character class excludes ``.``, so the
pattern matched ZERO lines in the entire tree while more than twenty real
acquisition sites existed. The hook ran as an active pre-commit entry and as a
CI job, so both batteries reported this invariant as enforced for as long as it
has existed. It was enforced nowhere, and the rule was already being violated
in main (see ``persist_controller.py``'s gauge write below).

WHY THIS IS NOT A ONE-CHARACTER FIX. Widening the class to reach
``self._write_lock`` would immediately flag the codebase's own correct code:
``SqliteUploadStore._write_txn`` holds ``self._write_lock`` precisely SO THAT
it can ``await conn.execute(...)`` inside it. A write transaction is a lock
whose entire purpose is to serialise awaited I/O. The shell hook had no
working configuration: too narrow and it matches nothing, wide enough to see
the attribute form and it condemns the correct sites. The rule needs to
distinguish two kinds of lock, which a line-oriented grep cannot do.

THE RULE. A lock is either

* an I/O lock, whose job is to serialise awaited work (the database write
  locks). Awaiting inside it is the point, and it is listed in
  :data:`IO_LOCK_ATTRS`; or
* a STATE lock, held only to make a read-modify-write of in-memory state
  atomic. Awaiting inside one of those widens the critical section across a
  suspension point, which is how a check-then-act race and a lock-ordering
  hazard get in. Those are what this gate forbids.

Everything not named in :data:`IO_LOCK_ATTRS` is treated as a state lock, so a
NEW lock is forbidden from awaiting by default and adding it to the allowlist
is a deliberate, reviewable act.

The gate is an AST walk rather than a regex for the same reason
``check_numeric_literals.py`` is: the fixed-window ``-A 50`` in the shell hook
could not see a lock body longer than fifty lines and could not tell a nested
function's ``await`` from the lock body's own. ``ast`` knows the block's real
extent, and it does not descend into a nested ``def`` or ``async def``, whose
awaits run later and outside the lock.

Run ``--selftest`` to prove the gate can still fail; CI invokes it, because a
gate nobody has watched reject is indistinguishable from the one this replaced.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Every ``self.<name>`` lock whose body may legitimately await. Each entry is a
# lock that exists to serialise I/O, so awaiting inside it is the contract
# rather than a violation. Anything absent is a state lock and may not await.
IO_LOCK_ATTRS: frozenset[str] = frozenset(
    {
        # SqliteUploadStore / SqliteTokenCache / SqliteCredentialStore: the
        # write lock IS the transaction boundary. `_write_txn` holds it across
        # `await conn.execute(...)` and `await conn.commit()` by design, so
        # that concurrent writers serialise on one connection.
        "_write_lock",
    }
)

_SCAN_GLOBS: tuple[tuple[str, str], ...] = (
    ("src/phantom-service/src", "**/*.py"),
    ("src/phantom-client/src", "**/*.py"),
    ("src/phantom-emulator/src", "**/*.py"),
)


def _lock_name(item: ast.withitem) -> str | None:
    """Return the lock's identifying name when the item looks like a lock.

    Args:
        item: One ``async with`` context item.

    Returns:
        The attribute name for ``self._lock``, the bare name for a local
        ``lock``, or ``None`` when the expression does not name a lock at all
        (so ``async with conn.execute(...)`` is not mistaken for one).
    """
    ctx = item.context_expr
    if isinstance(ctx, ast.Attribute) and "lock" in ctx.attr.lower():
        return ctx.attr
    if isinstance(ctx, ast.Name) and "lock" in ctx.id.lower():
        return ctx.id
    return None


class _AwaitInLockVisitor(ast.NodeVisitor):
    """Collect awaits that occur directly inside a state lock's block."""

    def __init__(self, path: Path) -> None:
        """Store the path used to render violations."""
        self.path = path
        self.violations: list[str] = []

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        """Flag awaits in the body when any acquired lock is a state lock."""
        names = [n for n in (_lock_name(i) for i in node.items) if n is not None]
        guarded = [n for n in names if n not in IO_LOCK_ATTRS]
        if guarded:
            for stmt in node.body:
                for await_node in _awaits_outside_nested_scopes(stmt):
                    rel = self.path.relative_to(_REPO_ROOT)
                    self.violations.append(
                        f"{rel}:{await_node.lineno}: await inside "
                        f"`async with self.{guarded[0]}` (state lock)"
                    )
        self.generic_visit(node)


def _awaits_outside_nested_scopes(node: ast.AST) -> list[ast.Await]:
    """Every ``await`` in ``node`` that runs inside the current block.

    Descends the tree but stops at a nested function or lambda, whose body
    executes later and outside whatever lock encloses the definition.

    Args:
        node: The statement to search.

    Returns:
        The awaits that actually run while the enclosing lock is held.
    """
    # A nested scope handed in directly is skipped whole. Testing only the
    # CHILDREN would descend into a `async def` that is itself a statement of
    # the lock body, which is exactly the shape the `nested` selftest pins.
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
        return []
    found: list[ast.Await] = []
    if isinstance(node, ast.Await):
        found.append(node)
    for child in ast.iter_child_nodes(node):
        found.extend(_awaits_outside_nested_scopes(child))
    return found


def _scan_paths() -> list[Path]:
    """Every Python file in scope, caches excluded, in a stable order."""
    paths: list[Path] = []
    for root, pattern in _SCAN_GLOBS:
        paths.extend(
            p for p in sorted((_REPO_ROOT / root).glob(pattern)) if "__pycache__" not in p.parts
        )
    return paths


def find_violations(paths: list[Path] | None = None) -> list[str]:
    """Return one message per await found inside a state lock.

    Args:
        paths: Files to check. Defaults to the whole configured scope, which
            is what the pre-commit entry point uses; a test passes its own.

    Returns:
        A list of ``path:line: reason`` strings, empty when the tree passes.
    """
    found: list[str] = []
    for path in paths if paths is not None else _scan_paths():
        visitor = _AwaitInLockVisitor(path)
        visitor.visit(ast.parse(path.read_text(encoding="utf-8")))
        found.extend(visitor.violations)
    return found


_SELFTEST_BAD = """
import asyncio

class C:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def m(self) -> None:
        async with self._lock:
            await self.other()
"""

_SELFTEST_GOOD = """
import asyncio

class C:
    def __init__(self) -> None:
        self._write_lock = asyncio.Lock()
        self._lock = asyncio.Lock()

    async def io(self) -> None:
        async with self._write_lock:
            await self.conn.execute("SELECT 1")

    async def state(self) -> None:
        async with self._lock:
            self.n += 1
        await self.emit()

    async def nested(self) -> None:
        async with self._lock:
            self.cb = lambda: self.later()

            async def deferred() -> None:
                await self.later()

            self.task = deferred
"""


def _selftest() -> int:
    """Prove the gate rejects the bad shape and accepts the good ones.

    The hook this replaces was inert for its whole life because nobody had
    watched it fail. A gate with no executed proof of rejection is worth
    nothing, so this runs in CI beside the gate itself.

    Returns:
        ``0`` when the gate behaves, ``1`` otherwise.
    """
    import tempfile

    ok = True
    with tempfile.TemporaryDirectory() as tmp:
        bad = Path(tmp) / "bad.py"
        bad.write_text(_SELFTEST_BAD, encoding="utf-8")
        good = Path(tmp) / "good.py"
        good.write_text(_SELFTEST_GOOD, encoding="utf-8")

        # The visitor renders paths relative to the repo root, so run the
        # selftest through the AST directly rather than through find_violations.
        for path, expect_hits, label in ((bad, 1, "state lock"), (good, 0, "io/nested")):
            visitor = _AwaitInLockVisitor(path)
            visitor.path = _REPO_ROOT / path.name
            visitor.visit(ast.parse(path.read_text(encoding="utf-8")))
            got = len(visitor.violations)
            if got != expect_hits:
                print(
                    f"selftest FAILED ({label}): expected {expect_hits} hit(s), got {got}",
                    file=sys.stderr,
                )
                ok = False

    if ok:
        print("selftest OK: rejects await in a state lock, accepts I/O locks and nested defs")
        return 0
    return 1


def main() -> int:
    """Print every violation to stderr and exit non-zero when any exist."""
    if "--selftest" in sys.argv[1:]:
        return _selftest()
    violations = find_violations()
    if violations:
        print("await inside a lock that guards in-memory state:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        print(
            "\nA state lock is held only to make a read-modify-write atomic. "
            "Awaiting inside one widens the critical section across a suspension "
            "point. Move the await out, or add the lock to IO_LOCK_ATTRS if it "
            "exists to serialise I/O.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
