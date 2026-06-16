"""Unit tests for phantom.chain.jsonpath."""

from __future__ import annotations

import pytest
from phantom.chain.jsonpath import extract, find_placeholders, substitute, validate_path


def test_validate_path_accepts() -> None:
    """A valid JSONPath compiles."""
    assert validate_path("$.foo.bar") is True


def test_validate_path_rejects() -> None:
    """An invalid JSONPath raises ValueError."""
    with pytest.raises(ValueError):
        validate_path("$.[[[invalid")


def test_extract_first_match() -> None:
    """``extract`` returns the first match by JSONPath."""
    body = {"a": {"b": 42, "c": "ok"}}
    assert extract(body, "$.a.b") == 42
    assert extract(body, "$.missing") is None


def test_extract_nested_list() -> None:
    """``extract`` handles list traversal."""
    body = {"steps": [{"id": "x"}, {"id": "y"}]}
    assert extract(body, "$.steps[0].id") == "x"


def test_find_placeholders_bare_string() -> None:
    """``find_placeholders`` returns step/var tuples."""
    template = "{{create_file.upload_url}}"
    assert find_placeholders(template) == [("create_file", "upload_url")]


def test_find_placeholders_in_url_path() -> None:
    """Placeholders embedded in a URL still extract."""
    template = "https://x/{{a.b}}/{{c.d}}"
    assert find_placeholders(template) == [("a", "b"), ("c", "d")]


def test_find_placeholders_ignores_invalid() -> None:
    """Non-matching brace shapes are not detected."""
    assert find_placeholders("{ x.y }") == []
    assert find_placeholders("{{X.Y}}") == []  # uppercase rejected


def test_substitute_basic() -> None:
    """``substitute`` replaces placeholders with values."""
    rendered, ok = substitute(
        "{{a.b}} and {{c.d}}",
        {"a": {"b": 1}, "c": {"d": "two"}},
    )
    assert ok is True
    assert rendered == "1 and two"


def test_substitute_reports_unresolved() -> None:
    """``substitute`` flags unresolved placeholders."""
    rendered, ok = substitute(
        "{{a.b}} {{c.d}}",
        {"a": {"b": 1}},
    )
    assert ok is False
    assert "{{c.d}}" in rendered
