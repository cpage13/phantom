#!/usr/bin/env python
"""Falsifiability: ``contracts/`` holds what an outside consumer can build against.

Review finding S13-2. ``contracts/`` is declared by ADR-035 and
``contracts/README.md`` to be the language-neutral acceptance basis a Go
implementation of Phantom builds against. Before this gate existed it had
exactly ONE check, ``scripts/export_contracts.py --check``, and that check is
CIRCULAR in two separate ways:

1. it compares the generator's output to the generator's OWN committed
   output, so it proves "committed bytes equal generator bytes" and nothing
   about whether those bytes are usable; and
2. the committed fixtures are round-tripped through the SAME pydantic model
   that emitted them, so the fixture is only ever asked whether its author
   still accepts it.

Nothing read the exported JSON Schemas at all. A Go implementation that
conformed EXACTLY to a published schema and was then rejected by the real
Python server would have found every CI lane green.

WHAT THIS GATE DOES INSTEAD. It reads ``contracts/`` the way a consumer in
another language reads it: as plain JSON, through a third-party JSON Schema
implementation, with neither pydantic nor ``phantom`` in the process.

1. Every ``*.schema.json`` must COMPILE as a JSON Schema. A schema a
   validator or code generator cannot compile is not a contract.
2. Every committed fixture must VALIDATE against its committed schema, under
   that third-party validator. This is the step that breaks the circularity:
   the fixture is judged by the published schema, not by the model that
   produced both.
3. Every component schema inside ``admin-openapi.json`` must compile too.
   That document is what an admin-client generator consumes.
4. Every file under ``contracts/fixtures/`` must be claimed by the mapping
   below, so a fixture added later cannot sit unvalidated.
5. The process must not have imported pydantic or ``phantom``. That is the
   structural proof of independence, asserted rather than assumed.

The schemas carry no ``$schema`` keyword, so the dialect is pinned here to
Draft 2020-12, which is what pydantic v2 emits. Pinning it also means a
future dialect change in the exporter fails here loudly instead of being
silently reinterpreted.

Exit codes:
- 0: every schema compiles and every fixture validates against it.
- 1: a schema does not compile, a fixture does not validate, a fixture is
     unclaimed, or the independence assertion failed.
- 2: ``contracts/`` or a named artifact is missing / unparseable
     (inconclusive, surfaced loudly rather than a false clean exit).

Run via:
    ``uv run python scripts/check_contract_schemas.py --selftest``
    ``uv run python scripts/check_contract_schemas.py``

The ``--selftest`` invocation runs FIRST in CI. It mutates the REAL committed
fixtures in memory the way a nonconforming port would (a missing required
field, an unknown extra field, an invented enum value) and requires the real
committed schemas to reject each one. A validation gate nobody has watched
reject anything is the finding this file exists to close, not a fix for it.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
_CONTRACTS_DIR: Final[Path] = _REPO_ROOT / "contracts"
_FIXTURES_DIRNAME: Final[str] = "fixtures"
_SCHEMA_SUFFIX: Final[str] = ".schema.json"
_OPENAPI_FILENAME: Final[str] = "admin-openapi.json"

# Which committed schema each committed fixture claims to exemplify. Every
# file under contracts/fixtures/ must appear here; an unclaimed fixture is a
# violation, because an unvalidated fixture is exactly the state S13-2 found.
_FIXTURE_TO_SCHEMA: Final[Mapping[str, str]] = {
    "fixtures/chain-envelope.example.json": "chain-envelope.schema.json",
    "fixtures/error-body.example.json": "error-body.schema.json",
}

# Modules whose presence would mean the check re-used the producer instead of
# reading the published artifacts as an outside consumer must.
_FORBIDDEN_MODULES: Final[tuple[str, ...]] = ("pydantic", "phantom")

# A value no enum in contracts/ can legitimately contain, and a property name
# no model declares. Used only by the self-test's mutations.
_SENTINEL_VALUE: Final[str] = "__contract_gate_sentinel__"
_SENTINEL_PROPERTY: Final[str] = "__contract_gate_unknown_property__"


class ContractCheckError(Exception):
    """An artifact is missing or unparseable, so the check is inconclusive."""


def _load_json(path: Path) -> Any:
    """Return the parsed JSON at ``path``.

    Args:
        path: Absolute path to a JSON artifact under ``contracts/``.

    Returns:
        The parsed document.

    Raises:
        ContractCheckError: If the file is missing or does not parse.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ContractCheckError(f"cannot read {path}: {exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ContractCheckError(f"{path} is not valid JSON: {exc}") from exc


def _relative(path: Path) -> str:
    """Return ``path`` relative to the repository root, for messages.

    Falls back to the absolute path when ``path`` lies outside the repository,
    which happens only when a caller points the checks at a copy of
    ``contracts/`` elsewhere. A message formatter must never be the thing that
    raises while reporting a violation.
    """
    try:
        return path.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _schema_files(contracts_dir: Path) -> list[Path]:
    """Return every ``*.schema.json`` in ``contracts_dir``, sorted."""
    return sorted(contracts_dir.glob(f"*{_SCHEMA_SUFFIX}"))


def _fixture_files(contracts_dir: Path) -> list[Path]:
    """Return every committed fixture file, sorted."""
    fixtures = contracts_dir / _FIXTURES_DIRNAME
    if not fixtures.is_dir():
        return []
    return sorted(p for p in fixtures.rglob("*") if p.is_file())


def check_schemas_compile(contracts_dir: Path) -> list[str]:
    """Return a message per schema that does not compile as JSON Schema.

    A schema a third-party validator refuses is not a contract a port can
    build against, however faithfully it mirrors the Python models.
    """
    violations: list[str] = []
    for path in _schema_files(contracts_dir):
        schema = _load_json(path)
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            violations.append(
                f"{_relative(path)}: not a valid Draft 2020-12 JSON Schema: {exc.message}"
            )
    return violations


def check_openapi_component_schemas(contracts_dir: Path) -> list[str]:
    """Return a message per OpenAPI component schema that does not compile.

    OpenAPI 3.1 component schemas ARE JSON Schema 2020-12, and they are what
    an admin-client generator in another language consumes. ``$ref`` targets
    are not resolved here; compilation is the property being asserted.
    """
    path = contracts_dir / _OPENAPI_FILENAME
    document = _load_json(path)
    if not isinstance(document, dict):
        raise ContractCheckError(f"{_relative(path)}: expected a JSON object at the top level")
    components = document.get("components")
    schemas = components.get("schemas") if isinstance(components, dict) else None
    if not isinstance(schemas, dict) or not schemas:
        raise ContractCheckError(
            f"{_relative(path)}: no components.schemas section; the admin contract "
            "carries no component schemas for a client generator to read"
        )
    violations: list[str] = []
    for name, schema in sorted(schemas.items()):
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            violations.append(
                f"{_relative(path)}: components.schemas.{name} is not a valid "
                f"Draft 2020-12 JSON Schema: {exc.message}"
            )
    return violations


def validate_instance(instance: Any, schema: Mapping[str, Any], *, where: str) -> list[str]:
    """Return one message per schema violation of ``instance``.

    Args:
        instance: The parsed JSON document under test.
        schema: The parsed, already-compiled JSON Schema to judge it by.
        where: Label used in messages, normally the fixture's path.

    Returns:
        A message per error, deepest-path-first, empty when the instance
        conforms.
    """
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path))
    return [
        f"{where}: {'/'.join(str(p) for p in error.absolute_path) or '<root>'}: {error.message}"
        for error in errors
    ]


def check_fixtures(contracts_dir: Path) -> list[str]:
    """Return a message per fixture that is unclaimed or does not validate.

    This is the circularity break named in S13-2: each committed fixture is
    judged by the committed SCHEMA, through a third-party validator, not by
    the pydantic model that emitted both.
    """
    violations: list[str] = []
    claimed = set(_FIXTURE_TO_SCHEMA)
    on_disk = {
        p.relative_to(contracts_dir).as_posix()
        for p in _fixture_files(contracts_dir)
        if p.suffix == ".json"
    }
    for unclaimed in sorted(on_disk - claimed):
        violations.append(
            f"contracts/{unclaimed}: no schema claimed for this fixture; add it to "
            "_FIXTURE_TO_SCHEMA so it is validated rather than shipped unchecked"
        )
    for missing in sorted(claimed - on_disk):
        raise ContractCheckError(f"contracts/{missing}: claimed fixture is not on disk")

    for fixture_name, schema_name in sorted(_FIXTURE_TO_SCHEMA.items()):
        schema = _load_json(contracts_dir / schema_name)
        instance = _load_json(contracts_dir / fixture_name)
        violations.extend(validate_instance(instance, schema, where=f"contracts/{fixture_name}"))
    return violations


def check_independence() -> list[str]:
    """Return a message if the producer's own code was imported.

    The gate's whole value is that it reads the published artifacts as an
    outside consumer. Asserting it rather than trusting it means a future
    convenience import that quietly reintroduces the circularity fails here.
    """
    imported = [name for name in _FORBIDDEN_MODULES if name in sys.modules]
    if not imported:
        return []
    return [
        f"the contract check imported {', '.join(imported)}; it must read "
        "contracts/ as an outside consumer does, with no producer code in the process"
    ]


def run_checks(contracts_dir: Path) -> list[str]:
    """Return every violation across all four checks, in a stable order."""
    violations: list[str] = []
    violations.extend(check_schemas_compile(contracts_dir))
    violations.extend(check_openapi_component_schemas(contracts_dir))
    violations.extend(check_fixtures(contracts_dir))
    violations.extend(check_independence())
    return violations


# --------------------------------------------------------------------------
# Self-test. The mutations below are applied to the REAL committed fixtures,
# so what is proven is that the REAL committed schemas reject a nonconforming
# consumer, not merely that the jsonschema library works.
# --------------------------------------------------------------------------


def _resolve(schema: Mapping[str, Any], node: Any) -> Any:
    """Follow a local ``$ref`` into ``$defs`` one hop, else return ``node``."""
    if not isinstance(node, dict):
        return node
    ref = node.get("$ref")
    prefix = "#/$defs/"
    if isinstance(ref, str) and ref.startswith(prefix):
        defs = schema.get("$defs")
        if isinstance(defs, dict):
            return defs.get(ref[len(prefix) :], node)
    return node


def _first_enum_path(schema: Mapping[str, Any], node: Any, instance: Any) -> list[str] | None:
    """Return the property path to the first enum-constrained value present.

    Walks the schema and the instance together, following one-hop ``$ref``s,
    and returns the path (a list of property names) of the first leaf whose
    resolved subschema declares an ``enum`` and whose value exists in the
    instance. Returns None when no such leaf exists.
    """
    resolved = _resolve(schema, node)
    if not isinstance(resolved, dict) or not isinstance(instance, dict):
        return None
    properties = resolved.get("properties")
    if not isinstance(properties, dict):
        return None
    for name, subschema in properties.items():
        if name not in instance:
            continue
        sub_resolved = _resolve(schema, subschema)
        if isinstance(sub_resolved, dict) and isinstance(sub_resolved.get("enum"), list):
            return [name]
        deeper = _first_enum_path(schema, subschema, instance[name])
        if deeper is not None:
            return [name, *deeper]
    return None


def _with_path_set(instance: Any, path: Sequence[str], value: Any) -> Any:
    """Return a deep-ish copy of ``instance`` with ``path`` set to ``value``."""
    copied = json.loads(json.dumps(instance))
    cursor = copied
    for name in path[:-1]:
        cursor = cursor[name]
    cursor[path[-1]] = value
    return copied


def _mutations(schema: Mapping[str, Any], instance: Any) -> list[tuple[str, Any]]:
    """Return ``(description, mutated_instance)`` pairs a bad port would produce.

    Three families, all derived from the schema rather than hand-written per
    fixture: a dropped required property, an unknown extra property, and an
    invented enum value.
    """
    cases: list[tuple[str, Any]] = []
    required = schema.get("required")
    if isinstance(required, list):
        for name in required:
            if not isinstance(instance, dict) or name not in instance:
                continue
            mutated = json.loads(json.dumps(instance))
            del mutated[name]
            cases.append((f"required property {name!r} dropped", mutated))
    if schema.get("additionalProperties") is False and isinstance(instance, dict):
        mutated = json.loads(json.dumps(instance))
        mutated[_SENTINEL_PROPERTY] = _SENTINEL_VALUE
        cases.append((f"unknown property {_SENTINEL_PROPERTY!r} added", mutated))
    enum_path = _first_enum_path(schema, schema, instance)
    if enum_path is not None:
        cases.append(
            (
                f"invented enum value at {'/'.join(enum_path)}",
                _with_path_set(instance, enum_path, _SENTINEL_VALUE),
            )
        )
    return cases


def _selftest(contracts_dir: Path) -> int:
    """Prove the committed schemas reject a nonconforming consumer.

    For every committed fixture: the unmutated fixture must validate, and
    every mutation must be rejected. Exits 0 when all expectations hold, 1
    otherwise. Runs BEFORE the real check in CI, because a validation gate
    with no executed proof of rejection is the finding this file closes, not
    a fix for it.
    """
    failures: list[str] = []
    checked = 0
    for fixture_name, schema_name in sorted(_FIXTURE_TO_SCHEMA.items()):
        schema = _load_json(contracts_dir / schema_name)
        instance = _load_json(contracts_dir / fixture_name)
        label = f"contracts/{fixture_name}"

        clean = validate_instance(instance, schema, where=label)
        if clean:
            failures.append(f"{label}: the committed fixture does not validate: {clean}")

        cases = _mutations(schema, instance)
        if not cases:
            failures.append(
                f"{label}: no mutation could be derived from {schema_name}; the "
                "self-test would prove nothing about this pair"
            )
        for description, mutated in cases:
            checked += 1
            if not validate_instance(mutated, schema, where=label):
                failures.append(
                    f"{label}: {schema_name} ACCEPTED a nonconforming document "
                    f"({description}); the schema has no teeth there"
                )

    failures.extend(check_independence())

    if failures:
        print("selftest FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(
        f"selftest OK: the committed fixtures validate, and the committed schemas "
        f"rejected all {checked} nonconforming mutations (missing required field, "
        "unknown extra field, invented enum value)."
    )
    return 0


def main() -> int:
    """Entry point for the contracts consumer-side validation gate."""
    if not _CONTRACTS_DIR.is_dir():
        print(f"contracts directory not found: {_CONTRACTS_DIR}", file=sys.stderr)
        return 2
    try:
        if "--selftest" in sys.argv[1:]:
            return _selftest(_CONTRACTS_DIR)
        violations = run_checks(_CONTRACTS_DIR)
    except ContractCheckError as exc:
        print(f"contracts check inconclusive: {exc}", file=sys.stderr)
        return 2
    if violations:
        print("contracts consumer-side validation failures:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    schema_count = len(_schema_files(_CONTRACTS_DIR))
    print(
        f"contracts/ is consumer-readable: {schema_count} schemas compile as "
        f"Draft 2020-12, {len(_FIXTURE_TO_SCHEMA)} fixtures validate against their "
        "committed schemas, and every admin-openapi component schema compiles."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
