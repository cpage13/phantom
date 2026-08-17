#!/usr/bin/env python
"""Pre-commit hook: tightened persist-handoff ordering check (plan § 4.2.6).

Two enforced constraints:

1. **Caller restriction.** Only ``workers/persist_controller.py`` is
   allowed to call ``store.mark_persisted(...)`` (or any method named
   ``mark_persisted`` on a store reference). The Phase 0 placeholder
   ``forbid-out-of-order-persist.sh`` matched ``SET body_location='file'``
   — that's the SQL underneath ``mark_persisted`` and is unique to the
   :class:`SqliteUploadStore` implementation; this script enforces the
   higher-level invariant against the caller side, which is what the
   single-writer manifest (plan § 0.5 invariant #6) actually requires.

2. **Persist-controller internal ordering.** Inside
   :func:`PersistController._migrate_one` (or wherever the actual
   migration is implemented), the call to ``file_body_store.put`` (or
   ``self._file.put``) MUST appear before the call to
   ``store.mark_persisted`` / ``self._store.mark_persisted``. The
   commit-last-column ordering (plan § 0.5) — fsync body files BEFORE
   flipping ``body_location='file'`` — is enforced by FileBodyStore's
   contract; this script ensures the call order at the source level so
   a refactor that reorders the calls is caught at pre-commit. That
   deferred half lives in two places inside
   ``phantom/storage/file_body_store.py``: ``_makedirs_durable``, which
   fsyncs the parent of every directory level the store creates, and the
   per-chain ``_sync_directory`` call after the renames, which makes the
   body FILE entries durable. Together they make the whole link chain
   from the store root to each body file durable before ``put()``
   returns.

The script walks the AST of the persist_controller.py module + a
greppable forbid-pass over the rest of src/.

Exit codes: 0 = pass, 1 = violation(s) found.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
_PERSIST_CONTROLLER_REL = "src/phantom-service/src/phantom/workers/persist_controller.py"
_SRC_ROOT = REPO_ROOT / "src"


def _ast_call_sites_of(name: str, tree: ast.AST) -> list[int]:
    """Return line numbers of every Call expression whose callable name
    contains ``name`` as the trailing attribute or function name.

    Matches both ``foo.mark_persisted(...)`` (Attribute) and any direct
    ``mark_persisted(...)`` (Name) call. Docstring and comment text is
    NOT matched because the AST walks code expressions only.
    """
    hits: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        called: str | None = None
        if isinstance(func, ast.Attribute):
            called = func.attr
        elif isinstance(func, ast.Name):
            called = func.id
        if called == name:
            hits.append(node.lineno)
    return hits


def check_mark_persisted_call_sites() -> list[str]:
    """Return violation messages for any production-code mark_persisted call
    outside ``workers/persist_controller.py``.

    Uses AST walk so docstring / comment text mentioning ``mark_persisted``
    does not register as a violation.
    """
    violations: list[str] = []
    if not _SRC_ROOT.exists():
        return violations
    for py_path in _SRC_ROOT.rglob("*.py"):
        rel = py_path.relative_to(REPO_ROOT)
        # Skip test trees + the canonical persist_controller location.
        parts = rel.parts
        if "tests" in parts:
            continue
        if rel.as_posix() == _PERSIST_CONTROLLER_REL:
            continue
        try:
            text = py_path.read_text(encoding="utf-8")
            tree = ast.parse(text)
        except (OSError, SyntaxError) as exc:
            violations.append(f"{rel}: failed to parse: {exc}")
            continue
        for lineno in _ast_call_sites_of("mark_persisted", tree):
            violations.append(
                f"{rel}:{lineno}: forbidden call to mark_persisted "
                f"outside workers/persist_controller.py (plan § 0.5 / § 4.2.6)"
            )
    return violations


def check_persist_controller_internal_ordering() -> list[str]:
    """Walk persist_controller.py AST; assert file.put precedes mark_persisted.

    The check scans every AsyncFunctionDef body for any call whose
    callable expression unparses to a string containing either
    ``file.put`` / ``_file.put`` / ``file_body_store.put`` or
    ``mark_persisted``. If both kinds are present, the earliest
    line-number put-call must precede the earliest mark_persisted call.
    """
    violations: list[str] = []
    path = REPO_ROOT / _PERSIST_CONTROLLER_REL
    if not path.exists():
        return violations
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [f"{_PERSIST_CONTROLLER_REL}: syntax error: {exc}"]

    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        put_lines: list[int] = []
        mark_lines: list[int] = []
        for subnode in ast.walk(node):
            if not isinstance(subnode, ast.Call):
                continue
            try:
                expr = ast.unparse(subnode)
            except Exception:  # pragma: no cover - unparse rarely fails
                continue
            if any(token in expr for token in ("file.put(", "_file.put(", "file_body_store.put(")):
                put_lines.append(subnode.lineno)
            if "mark_persisted(" in expr:
                mark_lines.append(subnode.lineno)
        if not mark_lines:
            continue
        if not put_lines:
            violations.append(
                f"{_PERSIST_CONTROLLER_REL}:{node.lineno}: function "
                f"{node.name!r} calls mark_persisted without a preceding "
                "file body-store put (plan § 0.5 commit-last-column ordering)"
            )
            continue
        if min(put_lines) > min(mark_lines):
            violations.append(
                f"{_PERSIST_CONTROLLER_REL}:{node.lineno}: function "
                f"{node.name!r} calls mark_persisted (line {min(mark_lines)}) "
                f"before file body-store put (line {min(put_lines)}) — "
                "fsync-before-flip ordering violation (plan § 0.5)"
            )
    return violations


def main() -> int:
    violations = check_mark_persisted_call_sites()
    violations.extend(check_persist_controller_internal_ordering())
    if violations:
        print("Persist-handoff ordering violations:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
