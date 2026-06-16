"""Drift-detection contract test for the duplicated chain envelope schema.

The request-chain envelope schema is intentionally duplicated across two
packages:

- ``phantom.models.chain`` (service-side; how Phantom parses incoming
  envelopes).
- ``phantom_client.models.chain`` (SDK-side; how callers construct them).

The duplication keeps ``phantom_client`` independent of the service
package while preserving wire-protocol identity. This contract test
enforces *wire compatibility* between the two copies: any structural
divergence between them is a wire-protocol drift.

Enforced (must match across packages):

- Field names per model
- Field type annotations
- Required / optional status
- Default values (including ``default_factory`` output)
- Field aliases (``from_path`` → ``"from"``)
- Validation metadata (``pattern``, ``ge``, ``min_length``, ...)
- ``ChainState`` literal members (declaration syntax may differ —
  ``TypeAlias`` vs PEP-695 ``type X = ...`` — but the literal set must
  be the same)

Not enforced (documentation drift is surfaced but does not fail):

- ``Field(description=...)`` text
- Class and module docstrings
- ``__all__`` membership
- Source-file comment style

The optional report at the end of this file prints a summary of
description-level differences so reviewers can choose to reconcile docs
even though the wire stays compatible.

See:

- ADR-009: request-chain envelope mechanism.
- ADR-010: canonical Pydantic v2 schema.
"""

from __future__ import annotations

import difflib
import json
from typing import Any, get_args

import pytest
from phantom.models import chain as service_chain
from phantom_client.models import chain as client_chain
from pydantic import BaseModel
from pydantic.fields import FieldInfo

# The model classes that must remain wire-compatible across packages.
_MODEL_NAMES: tuple[str, ...] = (
    "ChainBodyJson",
    "ChainBodyText",
    "ChainBodyBytes",
    "ChainBodyRef",
    "ChainCapture",
    "ChainStep",
    "ChainEnvelope",
    "CapturedStep",
    "ChainResponse",
)


def _chain_state_members(module: Any) -> tuple[str, ...]:
    """Return ``ChainState`` literal members regardless of declaration syntax.

    On Python 3.12+, ``type X = Literal[...]`` exposes the alias body via
    ``__value__``. The classic ``X: TypeAlias = Literal[...]`` form does
    not need an unwrap. Detect either and read the args.
    """
    state = module.ChainState
    inner = getattr(state, "__value__", state)
    return get_args(inner)


def _default_signature(field: FieldInfo) -> tuple[bool, Any, bool]:
    """Compact, comparable summary of a field's default state.

    Returns ``(is_required, default_value, has_default_factory)``. For
    fields with a ``default_factory``, we substitute the factory's
    initial return value — independent function objects are not
    directly equal across modules, but they should produce identical
    initial state if the wire defaults match.
    """
    if field.is_required():
        return (True, None, False)
    if field.default_factory is not None:
        return (False, field.default_factory(), True)  # type: ignore[call-arg]
    return (False, field.default, False)


# ---------------------------------------------------------------------------
# Structural tests — must PASS for wire compatibility.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_field_names_match(model_name: str) -> None:
    """Both packages declare the same field set per model."""
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    s_fields = set(service_model.model_fields.keys())
    c_fields = set(client_model.model_fields.keys())
    assert s_fields == c_fields, (
        f"{model_name} divergent fields:\n"
        f"  service-only: {s_fields - c_fields}\n"
        f"  client-only:  {c_fields - s_fields}"
    )


_DOC_KEYS: frozenset[str] = frozenset({"description", "title"})
"""Schema keys we drop before comparing. ``description`` is the
caller-supplied doc text; ``title`` is Pydantic-auto-generated from
class/field names and is not part of the wire contract."""


def _strip_doc_keys(node: Any) -> Any:
    """Recursively remove documentation-only keys."""
    if isinstance(node, dict):
        return {k: _strip_doc_keys(v) for k, v in node.items() if k not in _DOC_KEYS}
    if isinstance(node, list):
        return [_strip_doc_keys(v) for v in node]
    return node


def _dereference(schema: dict[str, Any]) -> dict[str, Any]:
    """Inline every ``$ref`` against ``$defs`` so two schemas that differ
    only in inline-vs-ref'd subschema encoding compare equal.

    Pydantic emits ``$ref`` -> ``$defs`` indirection for some types
    (notably PEP-695 ``type X = ...`` aliases) and inlines others
    (notably ``TypeAlias`` aliases). Both are valid JSON Schema 2020-12
    encodings of the same wire shape, so dereferencing normalizes them
    before comparison. Sibling keys on a ``$ref`` node (rare, but
    possible when Pydantic colocates ``description`` with a ref) are
    layered on top of the inlined definition.

    The chain-models tree has no cycles, so simple recursive inlining
    is safe.
    """
    defs = schema.get("$defs", {})

    def _walk(node: Any) -> Any:
        if isinstance(node, dict):
            if "$ref" in node:
                ref = node["$ref"]
                if ref.startswith("#/$defs/"):
                    name = ref[len("#/$defs/") :]
                    if name in defs:
                        inlined = _walk(defs[name])
                        if isinstance(inlined, dict):
                            merged = dict(inlined)
                            for k, v in node.items():
                                if k != "$ref":
                                    merged[k] = _walk(v)
                            return merged
                        return inlined
            return {k: _walk(v) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        return node

    result = _walk(schema)
    if isinstance(result, dict):
        result.pop("$defs", None)
    return result  # type: ignore[no-any-return]


def _wire_schema(model: type[BaseModel]) -> dict[str, Any]:
    """Pydantic JSON schema, $ref-dereferenced and doc-key-stripped.

    The JSON schema is the canonical wire contract. Two models with the
    same dereferenced, doc-stripped JSON schema are wire-compatible
    regardless of which package's classes they reference or which alias
    syntax they use for shared literals.
    """
    return _strip_doc_keys(_dereference(model.model_json_schema()))  # type: ignore[no-any-return]


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_wire_schemas_match(model_name: str) -> None:
    """JSON wire schemas (description-stripped) must match across packages.

    This is the canonical wire-compatibility check. Cross-module class
    references resolve to the same class names inside ``$defs`` / ``$ref``
    when both packages use the same class names, which they do by ADR-010.
    """
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    s_schema = _wire_schema(service_model)
    c_schema = _wire_schema(client_model)
    if s_schema != c_schema:
        s_str = json.dumps(s_schema, indent=2, sort_keys=True)
        c_str = json.dumps(c_schema, indent=2, sort_keys=True)
        diff = "\n".join(
            difflib.unified_diff(
                s_str.splitlines(),
                c_str.splitlines(),
                fromfile=f"service:{model_name}",
                tofile=f"client:{model_name}",
                lineterm="",
            )
        )
        raise AssertionError(f"Wire schemas diverge for {model_name}:\n{diff}")


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_field_required_status_match(model_name: str) -> None:
    """Each field has the same required/optional status across packages."""
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    for name, s_field in service_model.model_fields.items():
        c_field = client_model.model_fields[name]
        assert s_field.is_required() == c_field.is_required(), (
            f"{model_name}.{name} required-status mismatch: "
            f"service={s_field.is_required()}, client={c_field.is_required()}"
        )


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_field_defaults_match(model_name: str) -> None:
    """Each non-required field produces the same initial default value."""
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    for name, s_field in service_model.model_fields.items():
        c_field = client_model.model_fields[name]
        s_state = _default_signature(s_field)
        c_state = _default_signature(c_field)
        assert s_state == c_state, (
            f"{model_name}.{name} default mismatch:\n"
            f"  service: required={s_state[0]}, default={s_state[1]!r}, factory={s_state[2]}\n"
            f"  client:  required={c_state[0]}, default={c_state[1]!r}, factory={c_state[2]}"
        )


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_field_aliases_match(model_name: str) -> None:
    """Each field's alias (if any) matches across packages."""
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    for name, s_field in service_model.model_fields.items():
        c_field = client_model.model_fields[name]
        assert s_field.alias == c_field.alias, (
            f"{model_name}.{name} alias mismatch: "
            f"service={s_field.alias!r}, client={c_field.alias!r}"
        )


@pytest.mark.parametrize("model_name", _MODEL_NAMES)
def test_field_metadata_match(model_name: str) -> None:
    """Each field's validation metadata matches across packages.

    Metadata covers Pydantic / annotated-types constraints like ``pattern``,
    ``ge``, ``le``, ``min_length``, etc. The metadata entries are typed
    constraint objects; we compare them by ``repr`` to avoid relying on
    cross-module ``__eq__`` semantics.
    """
    service_model = getattr(service_chain, model_name)
    client_model = getattr(client_chain, model_name)
    for name, s_field in service_model.model_fields.items():
        c_field = client_model.model_fields[name]
        s_meta = sorted(repr(m) for m in s_field.metadata)
        c_meta = sorted(repr(m) for m in c_field.metadata)
        assert s_meta == c_meta, (
            f"{model_name}.{name} validation metadata mismatch:\n"
            f"  service: {s_meta}\n"
            f"  client:  {c_meta}"
        )


def test_chain_state_members_match() -> None:
    """``ChainState`` has the same literal-member set across packages.

    The two packages may use different declaration syntax for ``ChainState``
    (``TypeAlias`` vs PEP-695 ``type X = ...``); the helper unwraps both
    forms. What matters for the wire is that the same seven state strings
    appear in both.
    """
    s_members = _chain_state_members(service_chain)
    c_members = _chain_state_members(client_chain)
    assert set(s_members) == set(c_members), (
        f"ChainState members differ:\n  service: {s_members}\n  client:  {c_members}"
    )


# ---------------------------------------------------------------------------
# Description drift report — informational, never fails.
# ---------------------------------------------------------------------------


def test_report_description_drift() -> None:
    """Print a report of fields whose ``Field(description=)`` text differs.

    Description drift is not a wire-compatibility problem. This test
    always passes; it exists so reviewers running ``pytest -s`` see a
    summary of where docs diverge and can decide whether to reconcile.
    """
    drifts: list[str] = []
    for model_name in _MODEL_NAMES:
        s_model = getattr(service_chain, model_name)
        c_model = getattr(client_chain, model_name)
        for name, s_field in s_model.model_fields.items():
            c_field = c_model.model_fields[name]
            if s_field.description != c_field.description:
                drifts.append(
                    f"  {model_name}.{name}:\n"
                    f"    service: {s_field.description!r}\n"
                    f"    client:  {c_field.description!r}"
                )
    if drifts:
        # T201 silenced: this is an informational test that surfaces a report
        # to stdout. ``pytest -s`` shows it; default capture suppresses it.
        # A logger here would be invisible without configuring caplog;
        # print is the documented way to surface diagnostic test output.
        print(  # noqa: T201
            f"\n[chain-model description drift] {len(drifts)} field(s) have "
            "divergent Field descriptions; review and reconcile if "
            "documentation consistency is desired:\n" + "\n".join(drifts)
        )
