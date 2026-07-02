#!/usr/bin/env python
"""Falsifiability: ``list_by_key_value`` keys on the ``json_each`` form.

Plan § 0.1 / TASK 0.1b (acceptance criterion 3, the *static shape* gate).
``SqliteUploadStore.list_by_key_value`` takes an arbitrary, caller-supplied
KVS ``key`` (which may contain a ``"``). It MUST match that key via a
table-valued ``json_each`` over the FIXED, quote-free parent path
``$.steps[0].body.value.metadata.keyValueStore`` — binding the key as an
ordinary parameter (``je.key = ?``) so the user key NEVER enters a JSON-path
expression. The superseded, buggy form interpolated the key into a quoted
JSON-path label via :func:`_metadata_kvs_json_path` and matched it with
``json_extract(chain_envelope_json, ?)``; on the CI/deploy SQLite
(< ~3.50) an escaped ``\\"`` inside a quoted label fails to parse, so a
quote-bearing key (e.g. ``q"uote``) silently misses the lookup (memory
``sqlite-jsonpath-quote-escape-version-skew``; proven on SQLite 3.43.2 and
3.50.4).

This is a DEFENSE-IN-DEPTH check — ADDITIONAL to, never a substitute for,
the behavioral old-SQLite gate (``unit-phantom-old-sqlite``, TASK 0.1a),
which actually exercises the query on an asserted-old libsqlite3. The
behavioral test is blind on a modern (3.50.4) runner because the buggy form
*works* there; this static check proves the *shape* on EVERY runner, so a
refactor that reverts to the quoted-path / ``json_extract`` escaping form is
caught even where the behavioral test goes false-green.

Detection (the plan's exact logic) — performed against the parsed AST of the
``list_by_key_value`` method body, so docstring or comment text mentioning
either token is NOT matched (only real code expressions count):

- the method's SQL string literals MUST contain ``json_each(``; and
- the method MUST make NO call to ``_metadata_kvs_json_path`` — the call
  that builds the buggy interpolated quoted JSON-path label.

Either condition failing is a regression back toward the version-skew bug.

Exit codes:
- 0: ``list_by_key_value`` uses the ``json_each`` bound-parameter form.
- 1: the method has drifted (no ``json_each(`` literal, or it calls
     ``_metadata_kvs_json_path``) — a regression toward the buggy form.
- 2: the source file or the target method could not be found / parsed
     (inconclusive, surfaced loudly rather than a false clean exit).

Run via: ``uv run python scripts/check_kv_query_uses_json_each.py``
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_STORE_PY = (
    _REPO_ROOT / "src" / "phantom-service" / "src" / "phantom" / "storage" / "sqlite_store.py"
)
_METHOD_NAME = "list_by_key_value"
# The required table-valued-function marker of the correct (bound-parameter)
# form, and the forbidden helper whose presence marks the buggy interpolated
# quoted-JSON-path form.
_REQUIRED_SQL_MARKER = "json_each("
_FORBIDDEN_PATH_BUILDER = "_metadata_kvs_json_path"


def _find_method(tree: ast.AST, name: str) -> ast.AsyncFunctionDef | ast.FunctionDef | None:
    """Return the (sync or async) function/method def named ``name``, or None.

    Walks the whole tree so the method is found regardless of nesting depth
    inside its enclosing class.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    return None


def _sql_string_constants(node: ast.AST) -> list[str]:
    """Return every ``str`` constant literal within ``node``'s subtree.

    Only ``ast.Constant`` string nodes are collected — i.e. real string
    expressions in the code (the SQL fragments). The docstring is itself a
    string constant and is included here, but the docstring of this method
    describes the ``json_each`` form, so it cannot mask a regression: the
    required-marker presence test would still pass from the docstring text.
    The *forbidden-call* test below is what makes the gate sharp, and that
    one matches Call nodes only, never string text.
    """
    return [
        sub.value
        for sub in ast.walk(node)
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str)
    ]


def _calls_named(node: ast.AST, name: str) -> list[int]:
    """Return line numbers of every Call whose callee is named ``name``.

    Matches both a bare ``name(...)`` (``ast.Name``) and an attribute call
    ``obj.name(...)`` (``ast.Attribute``). Walks code expressions only, so a
    mention of ``name`` in a docstring or comment does NOT register — only a
    genuine call does. This is the regression marker for the buggy form.
    """
    hits: list[int] = []
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        func = sub.func
        called: str | None = None
        if isinstance(func, ast.Attribute):
            called = func.attr
        elif isinstance(func, ast.Name):
            called = func.id
        if called == name:
            hits.append(sub.lineno)
    return hits


def check_source(source: str, *, where: str) -> list[str]:
    """Return violation messages for the ``list_by_key_value`` shape in ``source``.

    Parses ``source``, locates the ``list_by_key_value`` method, and applies
    the two AST-level conditions (json_each present; ``_metadata_kvs_json_path``
    absent). ``where`` is used only to label messages. An empty list means the
    method uses the correct ``json_each`` bound-parameter form.

    Raises:
        SyntaxError: if ``source`` does not parse (the caller turns this into
            the inconclusive exit 2).
        LookupError: if the target method is not present in ``source``.
    """
    tree = ast.parse(source)
    method = _find_method(tree, _METHOD_NAME)
    if method is None:
        raise LookupError(f"{where}: method {_METHOD_NAME!r} not found")

    violations: list[str] = []
    sql_literals = _sql_string_constants(method)
    if not any(_REQUIRED_SQL_MARKER in lit for lit in sql_literals):
        violations.append(
            f"{where}:{method.lineno}: {_METHOD_NAME} no longer keys on a "
            f"{_REQUIRED_SQL_MARKER!r} SQL literal — the bound-parameter "
            "json_each form is required (plan § 0.1 / TASK 0.1b)"
        )
    forbidden_lines = _calls_named(method, _FORBIDDEN_PATH_BUILDER)
    if forbidden_lines:
        joined = ", ".join(str(n) for n in forbidden_lines)
        violations.append(
            f"{where}:{method.lineno}: {_METHOD_NAME} calls "
            f"{_FORBIDDEN_PATH_BUILDER}() (line(s) {joined}) — that is the "
            "buggy interpolated quoted-JSON-path form a quote-bearing key "
            "silently misses on old SQLite (plan § 0.1 / TASK 0.1b)"
        )
    return violations


def _selftest() -> int:
    """Prove the gate FAILS on the superseded buggy form (regression guard).

    Synthesizes a minimal ``list_by_key_value`` written in the old
    interpolated quoted-JSON-path / ``json_extract`` shape and asserts
    :func:`check_source` flags it. This demonstrates the falsifiability
    property without ever leaving the real source in the broken state. Run
    via ``--selftest``; exits 0 if the buggy form is correctly rejected, 1
    otherwise.
    """
    buggy = (
        "class S:\n"
        "    async def list_by_key_value(self, key, value):\n"
        '        """Old form: per-key quoted path + json_extract."""\n'
        "        json_path = _metadata_kvs_json_path(key)\n"
        "        sql = 'SELECT u.* FROM uploads u "
        "WHERE json_extract(u.chain_envelope_json, ?) = ?'\n"
        "        return await self._run(sql, [json_path, value])\n"
    )
    correct = (
        "class S:\n"
        "    async def list_by_key_value(self, key, value):\n"
        '        """New form: json_each over the fixed parent path."""\n'
        "        sql = ('SELECT u.* FROM uploads u, json_each(u.chain_envelope_json, "
        "$.parent) je WHERE je.key = ? AND je.value = ?')\n"
        "        return await self._run(sql, [key, value])\n"
    )
    buggy_violations = check_source(buggy, where="<selftest:buggy>")
    correct_violations = check_source(correct, where="<selftest:correct>")
    ok = bool(buggy_violations) and not correct_violations
    if ok:
        print("selftest OK: buggy interpolated form is rejected, json_each form passes.")
        return 0
    print("selftest FAILED:", file=sys.stderr)
    if not buggy_violations:
        print("  buggy interpolated form was NOT flagged (gate is blind!)", file=sys.stderr)
    if correct_violations:
        print(f"  json_each form was wrongly flagged: {correct_violations}", file=sys.stderr)
    return 1


def main() -> int:
    """Entry point for the ``list_by_key_value`` json_each shape check."""
    if "--selftest" in sys.argv[1:]:
        return _selftest()
    if not _STORE_PY.is_file():
        print(f"store module not found: {_STORE_PY}", file=sys.stderr)
        return 2
    rel = _STORE_PY.relative_to(_REPO_ROOT).as_posix()
    try:
        source = _STORE_PY.read_text(encoding="utf-8")
        violations = check_source(source, where=rel)
    except SyntaxError as exc:
        print(f"{rel}: failed to parse: {exc}", file=sys.stderr)
        return 2
    except LookupError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if violations:
        print("list_by_key_value json_each shape violations:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    print(f"{_METHOD_NAME} uses the json_each bound-parameter form (no quoted-path regression).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
