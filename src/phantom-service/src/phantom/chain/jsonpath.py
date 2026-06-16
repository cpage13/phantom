"""JSONPath wrapper over ``jsonpath_ng``.

Three concerns:

* ``validate_path`` — compile a JSONPath; raise on syntax errors.
* ``extract`` — first-match evaluation against a parsed body.
* ``find_placeholders`` — scan a template string for ``{{step.var}}`` placeholders.
"""

from __future__ import annotations

import re
from typing import Any

import jsonpath_ng  # type: ignore[import-untyped]  # no stubs/py.typed marker available

# A placeholder reference is exactly ``{{step_name.capture_name}}``.
# Step and capture names are snake_case (ADR-010).
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\s*\}\}")


def validate_path(path: str) -> bool:
    """Compile ``path`` to verify syntax.

    Args:
        path: JSONPath expression.

    Returns:
        ``True`` if the expression compiles.

    Raises:
        ValueError: When the expression is not a valid JSONPath.
    """
    try:
        jsonpath_ng.parse(path)
    except Exception as exc:  # jsonpath_ng raises a tree of types
        raise ValueError(f"Invalid JSONPath {path!r}: {exc}") from exc
    return True


def extract(body: Any, path: str) -> Any:
    """Evaluate ``path`` against ``body`` and return the first match.

    Args:
        body: Parsed JSON value (dict, list, scalar).
        path: JSONPath expression.

    Returns:
        The first matched value, or ``None`` if no match.
    """
    expr = jsonpath_ng.parse(path)
    matches = expr.find(body)
    if not matches:
        return None
    return matches[0].value


def find_placeholders(template: str) -> list[tuple[str, str]]:
    """Return every ``{{step.var}}`` reference in ``template``.

    Args:
        template: String that may contain placeholders.

    Returns:
        List of ``(step_name, capture_name)`` tuples in document order.
        Duplicate references appear multiple times.
    """
    return [(m.group(1), m.group(2)) for m in _PLACEHOLDER_RE.finditer(template)]


def substitute(template: str, values: dict[str, dict[str, Any]]) -> tuple[str, bool]:
    """Replace placeholders in ``template`` using ``values``.

    Args:
        template: Template string.
        values: Mapping ``step_name -> {capture_name: value}``.

    Returns:
        ``(rendered, all_resolved)`` — ``all_resolved`` is ``False`` if
        any placeholder had no matching capture value.
    """
    resolved = True

    def _replace(m: re.Match[str]) -> str:
        nonlocal resolved
        step, name = m.group(1), m.group(2)
        step_values = values.get(step)
        if step_values is None or name not in step_values:
            resolved = False
            return m.group(0)
        return str(step_values[name])

    rendered = _PLACEHOLDER_RE.sub(_replace, template)
    return rendered, resolved
