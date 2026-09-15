#!/usr/bin/env python
"""Falsifiability: every JSON-path lookup keys on the ``json_each`` form.

Plan § 0.1 / TASK 0.1b (acceptance criterion 3, the *static shape* gate),
widened after review finding S13-4.

``sqlite_store.py`` runs three lookups that reach into JSON1 documents:

- :meth:`SqliteUploadStore.list_by_key_value` (caller-supplied KVS key),
- :meth:`SqliteUploadStore.find_by_captured_value` (operator-supplied
  capture-step name),
- :meth:`SqliteUploadStore.find_by_local_uuid` (pinned metadata key).

All three MUST match their label via a table-valued ``json_each`` over a
FIXED, quote-free parent path, binding the label as an ordinary parameter
(``je.key = ?``), so a label NEVER enters a JSON-path expression. The
superseded, buggy form interpolated the label into a QUOTED JSON-path label
and matched it with ``json_extract(<column>, ?)``; on the CI/deploy SQLite
(< ~3.50) an escaped ``\\"`` inside a quoted label fails to parse, so a
quote-bearing label (e.g. ``q"uote``) silently misses the lookup (memory
``sqlite-jsonpath-quote-escape-version-skew``; proven on SQLite 3.43.2 and
3.50.4).

WHAT REVIEW FINDING S13-4 CHANGED. The first version of this gate checked
ONE of the three methods and looked for ONE helper BY NAME
(``_metadata_kvs_json_path``). The other two methods built the identical
escaped quoted-label shape through a different helper
(``_quote_json_path_label``), a name this gate never looked for, so the gate
was blind to two of the three sites and to any rename of the third. A gate
keyed on a helper's name is a gate that a rename silently disarms.

This version keys on the SHAPE of the construction instead, and scans the
WHOLE module rather than one method, so a re-introduced builder is caught at
its definition site whether or not a caller has reached for it yet.

THE FORBIDDEN SHAPE. Building a quoted JSON-path label means putting a
double-quote character around a value that is not a literal. Every source
spelling of that is flagged (docstrings are excluded, so prose about the bug
does not trip the gate; only real code expressions count):

1. an f-string where an interpolation is wrapped in double quotes,
   ``f'"{label}"'`` or ``f'{parent}."{label}"'``;
2. a string literal carrying the escaped-quote sequence ``\\"`` itself,
   which is what ``label.replace('"', '\\\\"')`` produces and what old
   SQLite cannot parse;
3. a ``.replace('"', ...)`` call, the quote-escaping step;
4. a ``+`` concatenation where one side is a literal starting or ending in
   a double quote and the other side is not a literal;
5. the ``%``-format and ``str.format`` spellings of the same wrap.

THE REQUIRED SHAPE. Each target method must additionally carry a
``json_each(`` SQL literal, so a rewrite that drops the table-valued form
altogether fails even if it happens to avoid every quoting spelling above.

KNOWN LIMIT, stated rather than papered over: an OPAQUE builder that
produces a quoted label without any of the five source spellings (for
instance ``json.dumps(label)``, whose output is a quoted, backslash-escaped
JSON string) would pass the shape scan. The required-marker rule still
forces a ``json_each(`` literal into every target method, and the
behavioral old-SQLite lane still exercises the query on a real < 3.50
libsqlite3. This gate is DEFENSE IN DEPTH, never a substitute for that lane,
which is itself blind on a modern (3.50.4) runner because the buggy form
*works* there.

Exit codes:
- 0: all three methods use the ``json_each`` bound-parameter form and no
     quoted-label construction exists in the module.
- 1: a method has drifted, or a quoted-label builder is present.
- 2: the source file could not be read/parsed, or a target method is not
     present (inconclusive, surfaced loudly rather than a false clean exit).

Run via:
    ``uv run python scripts/check_kv_query_uses_json_each.py --selftest``
    ``uv run python scripts/check_kv_query_uses_json_each.py``

The ``--selftest`` invocation runs FIRST in CI and in pre-commit, for the
reason S13-4 records: a gate with no executed proof of rejection is
indistinguishable from one that cannot fail.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
_STORE_PY: Final[Path] = (
    _REPO_ROOT / "src" / "phantom-service" / "src" / "phantom" / "storage" / "sqlite_store.py"
)

type FuncDef = ast.FunctionDef | ast.AsyncFunctionDef

# Every method that reaches into a JSON1 document with a caller- or
# operator-supplied label. All three are gated; scoping to one of them is the
# blindness S13-4 records.
_TARGET_METHODS: Final[tuple[str, ...]] = (
    "list_by_key_value",
    "find_by_captured_value",
    "find_by_local_uuid",
)

# The table-valued-function marker of the correct (bound-parameter) form.
_REQUIRED_SQL_MARKER: Final[str] = "json_each("

# The double-quote that opens and closes a JSON-path label, and the two
# characters an ESCAPED one occupies inside such a label. The escape is what
# SQLite < ~3.50 cannot parse, so its presence anywhere in real code is the
# bug's fingerprint.
_DOUBLE_QUOTE: Final[str] = '"'
_ESCAPED_QUOTE: Final[str] = '\\"'

# Method names whose call spelling participates in quote escaping / wrapping.
_REPLACE_METHOD: Final[str] = "replace"
_FORMAT_METHOD: Final[str] = "format"


def _is_str_constant(node: ast.AST) -> bool:
    """Return True when ``node`` is a ``str`` literal."""
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def _str_value(node: ast.AST) -> str | None:
    """Return the value of a ``str`` literal node, or None for anything else."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _docstring_node_ids(tree: ast.AST) -> frozenset[int]:
    """Return the ids of every docstring Constant node in ``tree``.

    A docstring is the first statement of a module, class, or function when
    that statement is a bare string. Excluding them keeps the gate matching
    real code expressions only, so prose describing the superseded buggy form
    (this module's own docstrings do exactly that) never registers as the
    form itself.
    """
    ids: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and _is_str_constant(first.value):
            ids.add(id(first.value))
    return frozenset(ids)


def _quoted_label_hits(node: ast.AST, docstring_ids: frozenset[int]) -> list[tuple[int, str]]:
    """Return ``(lineno, description)`` for every quoted-label construction.

    Walks ``node``'s subtree for the five source spellings of "wrap a
    non-literal value in double quotes" listed in the module docstring. This
    is the shape check that replaces the old name check: it does not care
    what the builder is called, or whether it is a helper at all.
    """
    hits: list[tuple[int, str]] = []
    for sub in ast.walk(node):
        # (1) f-string wrapping an interpolation in double quotes.
        if isinstance(sub, ast.JoinedStr):
            parts = sub.values
            for index, part in enumerate(parts):
                if not isinstance(part, ast.FormattedValue):
                    continue
                before = _str_value(parts[index - 1]) if index else None
                after = _str_value(parts[index + 1]) if index + 1 < len(parts) else None
                opens = before is not None and before.endswith(_DOUBLE_QUOTE)
                closes = after is not None and after.startswith(_DOUBLE_QUOTE)
                if opens or closes:
                    hits.append(
                        (sub.lineno, f"f-string wraps an interpolation in quotes: {_render(sub)}")
                    )
                    break
        # (2) a literal carrying the escaped-quote sequence itself.
        if isinstance(sub, ast.Constant) and id(sub) not in docstring_ids:
            literal = _str_value(sub)
            if literal is not None and _ESCAPED_QUOTE in literal:
                detail = f"literal carries the escaped quote {_ESCAPED_QUOTE!r}: {literal!r}"
                hits.append((sub.lineno, detail))
        # (3) quote-escaping call, and (5) the str.format wrap.
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
            attr = sub.func.attr
            if attr == _REPLACE_METHOD and sub.args and _str_value(sub.args[0]) == _DOUBLE_QUOTE:
                hits.append((sub.lineno, f"escapes quotes for a label: {_render(sub)}"))
            if attr == _FORMAT_METHOD:
                template = _str_value(sub.func.value)
                if template is not None and _DOUBLE_QUOTE in template:
                    hits.append((sub.lineno, f"str.format wraps a value in quotes: {_render(sub)}"))
        if isinstance(sub, ast.BinOp):
            # (4) concatenation with a quote-bearing literal edge.
            if isinstance(sub.op, ast.Add):
                for literal_side, other_side in ((sub.left, sub.right), (sub.right, sub.left)):
                    literal = _str_value(literal_side)
                    if literal is None or isinstance(other_side, ast.Constant):
                        continue
                    if literal.startswith(_DOUBLE_QUOTE) or literal.endswith(_DOUBLE_QUOTE):
                        hits.append(
                            (sub.lineno, f"concatenates a quote onto a value: {_render(sub)}")
                        )
                        break
            # (5) the %-format wrap.
            if isinstance(sub.op, ast.Mod):
                template = _str_value(sub.left)
                if template is not None and _DOUBLE_QUOTE in template:
                    hits.append((sub.lineno, f"%-format wraps a value in quotes: {_render(sub)}"))
    return hits


def _render(node: ast.AST) -> str:
    """Return a short, single-line source rendering of ``node`` for messages."""
    try:
        text = ast.unparse(node)
    except ValueError, TypeError, AttributeError:
        return "<unrenderable expression>"
    collapsed = " ".join(text.split())
    limit = _RENDER_CHAR_LIMIT
    return collapsed if len(collapsed) <= limit else f"{collapsed[:limit]}..."


# Message rendering only: keeps a long SQL expression from flooding the report.
_RENDER_CHAR_LIMIT: Final[int] = 100


def _function_defs_by_name(tree: ast.AST) -> dict[str, list[FuncDef]]:
    """Return every function/method definition in ``tree``, keyed by name.

    Names are not unique across classes, so each name maps to a list. The
    transitive walk below expands all same-named definitions, which can only
    widen what the gate inspects, never narrow it.
    """
    defs: dict[str, list[FuncDef]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            defs.setdefault(node.name, []).append(node)
    return defs


def _callee_names(node: ast.AST) -> set[str]:
    """Return the callee names invoked anywhere in ``node``'s subtree.

    Matches a bare ``name(...)`` and an attribute call ``obj.name(...)``, so
    a module-level helper and a ``self._helper(...)`` method both resolve.
    """
    names: set[str] = set()
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        func = sub.func
        if isinstance(func, ast.Attribute):
            names.add(func.attr)
        elif isinstance(func, ast.Name):
            names.add(func.id)
    return names


def _reachable_defs(root: FuncDef, defs: dict[str, list[FuncDef]]) -> list[FuncDef]:
    """Return ``root`` plus every module-local function it transitively calls.

    A fixpoint over the call graph restricted to functions defined in the
    same module. This is what makes the gate catch a builder that a target
    method reaches through a helper, no matter what either is named.
    """
    seen: set[int] = {id(root)}
    ordered: list[FuncDef] = [root]
    queue: list[FuncDef] = [root]
    while queue:
        current = queue.pop()
        for name in _callee_names(current):
            for candidate in defs.get(name, ()):
                if id(candidate) in seen:
                    continue
                seen.add(id(candidate))
                ordered.append(candidate)
                queue.append(candidate)
    return ordered


def _sql_string_constants(node: ast.AST, docstring_ids: frozenset[int]) -> list[str]:
    """Return every non-docstring ``str`` literal in ``node``'s subtree.

    Docstrings are excluded so a method whose prose mentions ``json_each``
    cannot satisfy the required-marker rule from documentation alone. That
    was a stated weakness of the first version of this gate; here the marker
    must come from a real SQL expression.
    """
    literals: list[str] = []
    for sub in ast.walk(node):
        if id(sub) in docstring_ids:
            continue
        value = _str_value(sub)
        if value is not None:
            literals.append(value)
    return literals


def check_source(source: str, *, where: str) -> list[str]:
    """Return violation messages for the JSON-path query shapes in ``source``.

    Parses ``source``, then applies three rules: every target method carries
    a ``json_each(`` SQL literal; no target method reaches a quoted-label
    construction through the module-local call graph; and no quoted-label
    construction exists anywhere in the module. An empty list means the
    module is clean.

    Args:
        source: Python source text of the storage module (or of a synthetic
            case, in the self-test).
        where: Label used in messages, normally the repo-relative path.

    Returns:
        One message per violation, empty when the shape holds.

    Raises:
        SyntaxError: if ``source`` does not parse (the caller turns this
            into the inconclusive exit 2).
        LookupError: if a target method is not present in ``source``.
    """
    tree = ast.parse(source)
    docstring_ids = _docstring_node_ids(tree)
    defs = _function_defs_by_name(tree)

    missing = [name for name in _TARGET_METHODS if name not in defs]
    if missing:
        raise LookupError(f"{where}: target method(s) not found: {', '.join(missing)}")

    violations: list[str] = []
    flagged_lines: set[int] = set()

    for name in _TARGET_METHODS:
        for method in defs[name]:
            literals = _sql_string_constants(method, docstring_ids)
            if not any(_REQUIRED_SQL_MARKER in lit for lit in literals):
                violations.append(
                    f"{where}:{method.lineno}: {name} no longer keys on a "
                    f"{_REQUIRED_SQL_MARKER!r} SQL literal; the bound-parameter "
                    "json_each form is required (plan § 0.1 / TASK 0.1b)"
                )
            for reached in _reachable_defs(method, defs):
                for lineno, detail in _quoted_label_hits(reached, docstring_ids):
                    via = "" if reached is method else f" via {reached.name}()"
                    violations.append(
                        f"{where}:{lineno}: {name}{via} builds a quoted JSON-path "
                        f"label ({detail}); that is the form a quote-bearing label "
                        "silently misses on old SQLite (plan § 0.1 / TASK 0.1b)"
                    )
                    flagged_lines.add(lineno)

    for lineno, detail in _quoted_label_hits(tree, docstring_ids):
        if lineno in flagged_lines:
            continue
        violations.append(
            f"{where}:{lineno}: quoted JSON-path label construction present in "
            f"the module ({detail}); the builder must not exist for any caller "
            "to reach for (finding D6 / S13-4)"
        )

    return violations


# --------------------------------------------------------------------------
# Self-test cases. Each is a minimal module that either carries the defect the
# gate exists to catch or is the sanctioned form. The gate must reject every
# defective case and accept the clean one; that pairing is the proof the gate
# can still fail, which is the whole point of S13-4.
#
# Cases are assembled from per-method source blocks rather than edited as
# text, so a case always parses and a reverted method is a whole replacement
# rather than a patch.
# --------------------------------------------------------------------------

_CASE_PRELUDE: Final[str] = (
    'PARENT = "$.steps[0].body.value.metadata.keyValueStore"\n'
    'STEPS = "$.steps"\n'
    'VALUES_PREFIX = "$.values."\n'
    "\n\n"
    "class S:\n"
)

# The sanctioned form of each target method, matching what the real storage
# module does today: a fixed quote-free parent path baked into the SQL, the
# label bound as an ordinary parameter.
_CLEAN_BODIES: Final[Mapping[str, str]] = {
    "list_by_key_value": (
        "    async def list_by_key_value(self, key, value):\n"
        '        """Sanctioned: json_each over the fixed parent path."""\n'
        "        sql = (\n"
        '            "SELECT u.* FROM uploads u, "\n'
        "            f\"json_each(u.chain_envelope_json, '{PARENT}') je \"\n"
        '            "WHERE je.key = ? AND CAST(je.value AS TEXT) = ?"\n'
        "        )\n"
        "        return await self._run(sql, [key, value])\n\n"
    ),
    "find_by_captured_value": (
        "    async def find_by_captured_value(self, capture_name, subpath, value):\n"
        '        """Sanctioned: json_each over the fixed steps path."""\n'
        '        step_values_path = f"{VALUES_PREFIX}{subpath}"\n'
        "        sql = (\n"
        '            "SELECT u.* FROM uploads u, "\n'
        "            f\"json_each(u.captured_values_json, '{STEPS}') je \"\n"
        '            "WHERE je.key = ? AND CAST(json_extract(je.value, ?) AS TEXT) = ?"\n'
        "        )\n"
        "        return await self._run(sql, [capture_name, step_values_path, value])\n\n"
    ),
    "find_by_local_uuid": (
        "    async def find_by_local_uuid(self, local_uuid):\n"
        '        """Sanctioned: json_each with a pinned, quote-free key."""\n'
        "        sql = (\n"
        '            "SELECT u.* FROM uploads u, "\n'
        "            f\"json_each(u.chain_envelope_json, '{PARENT}') je \"\n"
        '            "WHERE je.key = ? AND CAST(je.value AS TEXT) = ?"\n'
        "        )\n"
        '        return await self._run(sql, ["phantom_local_uuid", str(local_uuid)])\n\n'
    ),
}

# The historical defect, verbatim in shape: a helper escapes the embedded
# quote and wraps the label, and the method matches it with json_extract on a
# bound path. Deliberately named nothing the retired name-based gate looked
# for, which is the blindness S13-4 records.
_LEGACY_BUILDER: Final[str] = (
    "def _label(raw):\n"
    '    """Wrap a label for a JSON path, escaping any embedded quote."""\n'
    "    return '\"' + raw.replace('\"', '\\\\\"') + '\"'\n"
    "\n\n"
)


def _legacy_escaped_quote_body(method: str) -> str:
    """Return ``method`` rewritten to call the legacy escaped-quote builder."""
    return (
        "    async def " + method + "(self, label, value):\n"
        '        """Legacy: helper-built quoted JSON-path label + json_extract."""\n'
        '        json_path = PARENT + "." + _label(label)\n'
        '        sql = "SELECT u.* FROM uploads u WHERE json_extract(u.c, ?) = ?"\n'
        "        return await self._run(sql, [json_path, value])\n\n"
    )


def _inline_quoted_label_body(method: str) -> str:
    """Return ``method`` rewritten to build a quoted label inline, no helper."""
    return (
        "    async def " + method + "(self, label, value):\n"
        '        """Reverted: inline quoted JSON-path label + json_extract."""\n'
        "        json_path = f'{PARENT}.\"{label}\"'\n"
        '        sql = "SELECT u.* FROM uploads u WHERE json_extract(u.c, ?) = ?"\n'
        "        return await self._run(sql, [json_path, value])\n\n"
    )


def _no_json_each_body(method: str) -> str:
    """Return ``method`` rewritten to drop the ``json_each`` form entirely."""
    return (
        "    async def " + method + "(self, label, value):\n"
        '        """Reverted: plain json_extract on a bound path, no json_each."""\n'
        '        sql = "SELECT u.* FROM uploads u WHERE json_extract(u.c, ?) = ?"\n'
        "        return await self._run(sql, [label, value])\n\n"
    )


def _build_case(*, replace: str | None = None, body: str = "", prelude: str = "") -> str:
    """Assemble a self-test module, optionally swapping one method's body.

    Args:
        replace: Name of the target method to swap out, or None for the
            wholly sanctioned module.
        body: Replacement source for that method, indented as a class body.
        prelude: Extra module-level source (the legacy builder) placed before
            the class.

    Returns:
        Parseable Python source defining all three target methods.
    """
    parts: list[str] = [_CASE_PRELUDE.replace("class S:\n", prelude + "class S:\n")]
    for name in _TARGET_METHODS:
        parts.append(body if name == replace else _CLEAN_BODIES[name])
    return "".join(parts)


def _selftest() -> int:
    """Prove the gate rejects every defective shape and accepts the clean one.

    Runs the clean case (must produce no violations) and each defective case
    (must produce at least one violation naming the right method). Every
    defect is applied to each of the three target methods in turn, because the
    blindness S13-4 records was a gate scoped to one site. Exits 0 when every
    expectation holds, 1 otherwise. This runs BEFORE the real check in CI: a
    gate with no executed proof of rejection is indistinguishable from one
    that cannot fail (S13-4).
    """
    failures: list[str] = []

    clean = check_source(_build_case(), where="<selftest:clean>")
    if clean:
        failures.append(f"the sanctioned json_each form was wrongly flagged: {clean}")

    for method in _TARGET_METHODS:
        legacy = check_source(
            _build_case(
                replace=method, body=_legacy_escaped_quote_body(method), prelude=_LEGACY_BUILDER
            ),
            where=f"<selftest:legacy-escaped-quote:{method}>",
        )
        if not any(method in message for message in legacy):
            failures.append(
                f"the legacy escaped-quote builder reached from {method} was NOT "
                f"flagged (gate is blind to the very shape it exists to catch): {legacy}"
            )

        inline = check_source(
            _build_case(replace=method, body=_inline_quoted_label_body(method)),
            where=f"<selftest:inline-quoted:{method}>",
        )
        if not any(method in message for message in inline):
            failures.append(f"an inline quoted label in {method} was NOT flagged: {inline}")

        dropped = check_source(
            _build_case(replace=method, body=_no_json_each_body(method)),
            where=f"<selftest:no-json-each:{method}>",
        )
        if not any(method in message for message in dropped):
            failures.append(f"a missing json_each form in {method} was NOT flagged: {dropped}")

    if failures:
        print("selftest FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    covered = ", ".join(_TARGET_METHODS)
    print(
        "selftest OK: in each of "
        f"{covered} the legacy escaped-quote builder, an inline quoted label and "
        "a dropped json_each form are all rejected, and the sanctioned json_each "
        "form passes."
    )
    return 0


def main() -> int:
    """Entry point for the JSON-path query shape check."""
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
        print("JSON-path query shape violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    covered = ", ".join(_TARGET_METHODS)
    print(f"{covered} use the json_each bound-parameter form (no quoted-label regression).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
