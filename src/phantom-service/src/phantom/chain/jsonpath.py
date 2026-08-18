"""JSONPath wrapper over ``jsonpath_ng``, plus the placeholder vocabulary.

Four concerns:

* ``validate_path``: compile a JSONPath; raise on syntax errors.
* ``extract``: first-match evaluation against a parsed body.
* ``find_placeholders``: scan a template string for ``{{step.var}}``
  placeholders.
* ``whole_placeholder``: decide whether a string is EXACTLY one placeholder,
  which is the only position a captured object or array may replace a JSON
  node as structure rather than render into text (F8).

The last two split one question. ``find_placeholders`` answers "which captures
does this template reference", which the capture-TTL gate and the unresolved
classification need. ``whole_placeholder`` answers "is this string nothing but
a reference", which only the JSON walker needs. Both live here because the
placeholder pattern is this module's private property and neither predicate
should be re-derived by importing it across a module boundary.
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

    KNOWN AMBIGUITY, recorded rather than fixed by F8. ``None`` is returned
    both for "no match" and for "matched a JSON null", so a legitimately
    captured null is indistinguishable from an absent capture. The executor's
    required-capture check reads the absent meaning, so a chain whose upstream
    genuinely returns null for a referenced field retries to exhaustion and
    ends in ``stored``. Fixing it means changing this signature (a sentinel or
    a match/no-match pair) and every caller, which is out of F8's scope; F8
    declined it explicitly rather than absorbing it silently.

    Args:
        body: Parsed JSON value (dict, list, scalar).
        path: JSONPath expression.

    Returns:
        The first matched value, or ``None`` if no match. See the ambiguity
        note above before reading ``None`` as "absent".
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


def whole_placeholder(template: str) -> tuple[str, str] | None:
    """Return ``(step, capture)`` when ``template`` is EXACTLY one placeholder.

    ``None`` when it carries other text, two adjacent placeholders, or none.
    The distinction decides whether a dict or list capture may replace a JSON
    node WHOLE as structure; every other value renders into a string.

    Args:
        template: The string node under test. Inner spaces are allowed by the
            pattern, so ``{{ s.a }}`` qualifies.

    Returns:
        The ``(step_name, capture_name)`` pair, or ``None``.
    """
    match = _PLACEHOLDER_RE.fullmatch(template)
    if match is None:
        return None
    return match.group(1), match.group(2)


def substitute(template: str, values: dict[str, dict[str, Any]]) -> tuple[str, bool]:
    """Replace placeholders in ``template`` using ``values``, as TEXT.

    A TEXT-context helper, and only that. It renders every value through
    ``str()`` into a string, so it suits the URL, a header value and a text
    body. A JSON BODY does NOT use it on its serialization: rendering a JSON
    body by dumping the template and substituting into the dump is the F8
    defect, because an unescaped quote or backslash in a captured value
    produced malformed JSON that the upstream rejected. The executor walks the
    PARSED body instead and calls this helper only on the individual string
    nodes it renders as text, then serializes once at the end. Do not re-add a
    dump-then-substitute caller.

    Args:
        template: Template string.
        values: Mapping ``step_name -> {capture_name: value}``.

    Returns:
        ``(rendered, all_resolved)``. ``all_resolved`` is ``False`` if any
        placeholder had no matching capture value.
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
