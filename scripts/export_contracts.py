"""Generate Phantom's wire, admin, and config contracts as language-neutral artifacts.

Phantom's request-chain envelope, error envelope, settings shape, and admin
OpenAPI surface exist only as Python (Pydantic models + FastAPI routes). A
planned Go implementation needs a committed, language-neutral contract to
build against without reading Python. This script IS the one place that
contract gets produced: every artifact under ``contracts/`` is generated
FROM the Python source of truth (never hand-edited), so the two can never
silently drift apart.

Two modes:

- Default (write): regenerate every artifact in place under ``contracts/``.
- ``--check``: regenerate every artifact into a temporary directory, byte-
  compare it against the committed ``contracts/`` tree, print a per-file
  drift report, and exit 1 on any difference (0 when clean). This is the
  CI drift gate (``contracts-drift`` job in ``.github/workflows/per_pr.yml``).

Determinism: every JSON artifact is written with
``json.dumps(obj, indent=2, sort_keys=True)`` plus a single trailing
newline, so running the exporter twice in a row is byte-identical.

Exit codes:
- 0: wrote cleanly (write mode), or no drift found (``--check``).
- 1: drift found (``--check`` only).
- Any uncaught :class:`ContractExportError` propagates as a fatal error. It
  signals a bug in the exporter itself or an incompatible upstream model
  change (a temp-build-path leak into ``admin-openapi.json``, an
  operationId collision normalization could not resolve, or a fixture that
  fails to round-trip through its own model). That is not ordinary drift,
  so it is not folded into the drift report.

Run via: uv run python scripts/export_contracts.py
Check via: uv run python scripts/export_contracts.py --check
"""

from __future__ import annotations

import json
import logging
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Final
from uuid import UUID

import phantom_client.headers as wire_headers
from phantom.app import create_app
from phantom.config.settings import InstanceCfg, RouteCfg, Settings, StorageCfg
from phantom.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainResponse,
    ChainStep,
)
from phantom.models.errors import ErrorBody, ErrorEnvelope
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
CONTRACTS_DIR: Final[Path] = REPO_ROOT / "contracts"

_CHECK_FLAG: Final[str] = "--check"

# JSON pretty-print width for every generated artifact (determinism rule:
# same indent, sorted keys, one trailing newline, every time).
_JSON_INDENT: Final[int] = 2

# Fixed literal values for the canonical fixtures below. Deliberately NOT
# uuid4()/now(): a fixture must be byte-identical on every regeneration,
# and its values must read as obviously fake rather than real data.
_EXAMPLE_CHAIN_ID: Final[UUID] = UUID("00000000-0000-0000-0000-000000000001")
_EXAMPLE_IDEMPOTENCY_KEY: Final[str] = "example-idempotency-key-0001"
_EXAMPLE_REQUEST_ID: Final[str] = "00000000-0000-0000-0000-000000000002"
# Illustrative-only capture-observation TTL for the presigned-URL example
# capture; not a runtime default (see phantom.models.chain.ChainCapture).
_EXAMPLE_CAPTURE_TTL_SECONDS: Final[int] = 300


class ContractExportError(RuntimeError):
    """A fatal exporter defect: not ordinary drift, but a bug or incompatible model.

    Raised when a generated artifact fails an internal-consistency check
    the exporter itself is responsible for: the admin OpenAPI document
    embedding its own build-time temp path, or a fixture that fails to
    re-validate through the model it claims to exemplify.
    """


# ---------------------------------------------------------------------------
# Generic JSON canonicalization.
# ---------------------------------------------------------------------------


def _canonical_json(obj: dict[str, Any]) -> bytes:
    """Serialize ``obj`` as deterministic, diff-friendly JSON.

    Args:
        obj: A JSON-serializable object (schema dict, OpenAPI document,
            header map, or a model's ``model_dump(mode="json")`` output).

    Returns:
        UTF-8 bytes: ``_JSON_INDENT``-space indent, alphabetically sorted
        keys, and exactly one trailing newline. Running the exporter twice
        against an unchanged source tree produces byte-identical output.
    """
    return (json.dumps(obj, indent=_JSON_INDENT, sort_keys=True) + "\n").encode("utf-8")


def _relative_files_under(root: Path) -> set[str]:
    """Return every regular file under ``root`` as POSIX-style relative paths.

    Args:
        root: Directory to walk. Need not exist.

    Returns:
        An empty set if ``root`` does not exist (the natural state before
        the first write), otherwise every file's path relative to ``root``.
    """
    if not root.is_dir():
        return set()
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


# ---------------------------------------------------------------------------
# JSON Schema artifacts (straight from each Pydantic model).
# ---------------------------------------------------------------------------


def _schema_bytes(model: type[BaseModel]) -> bytes:
    """Return the canonical JSON encoding of a Pydantic model's JSON Schema.

    Args:
        model: The model whose ``model_json_schema()`` is the contract.

    Returns:
        Canonical JSON bytes (see :func:`_canonical_json`).
    """
    logger.info("generating JSON schema for %s.%s", model.__module__, model.__qualname__)
    return _canonical_json(model.model_json_schema())


# ---------------------------------------------------------------------------
# Admin OpenAPI artifact.
# ---------------------------------------------------------------------------


def _minimal_settings(data_dir: Path) -> Settings:
    """Build a minimal, production-shaped Settings for OpenAPI export only.

    Mirrors ``tests/e2e/test_vacuum_idle_gate.py::_exception_arm_settings``:
    one instance, one route, bearer auth. The app built from this Settings
    is only ever constructed here, never started (no lifespan), so this
    exists solely to satisfy :func:`phantom.app.create_app`'s constructor-
    time requirements (in particular, the settings validator's host probe
    needs an existing path to read disk-free stats from).

    Args:
        data_dir: An existing directory. Read (disk-usage probe) but never
            written to; the caller owns its lifetime.

    Returns:
        A valid single-instance :class:`Settings`.
    """
    hosts = ["files.example.com"]
    return Settings(
        storage=StorageCfg(data_dir=str(data_dir)),
        instances=[
            InstanceCfg(
                id="primary",
                host_prefixes=hosts,
                data_dir="primary",
                routes=[RouteCfg(name="upstream-files", hosts=hosts, auth_mode="phantom_bearer")],
            )
        ],
    )


def _assert_no_path_leak(schema: dict[str, Any], tmp_path: Path) -> None:
    """Fail loudly if the build-time temp directory leaked into the schema.

    Args:
        schema: The OpenAPI document about to be written.
        tmp_path: The temporary directory :func:`_minimal_settings` was
            pointed at for this build.

    Raises:
        ContractExportError: If the absolute temp path appears anywhere in
            the serialized schema.
    """
    needle = str(tmp_path)
    haystack = json.dumps(schema)
    if needle in haystack:
        raise ContractExportError(
            f"admin-openapi.json embeds the build-time temp path {needle!r}; "
            "a committed artifact must not carry any build-time path"
        )


def _collect_operations_by_id(schema: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Group every operation's ``(path, method)`` location by its current operationId.

    Args:
        schema: The raw OpenAPI document.

    Returns:
        A mapping from operationId to every ``(path, method)`` location
        that currently carries it. A value list with more than one entry
        is a collision.
    """
    locations_by_id: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for path, path_item in schema.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId")
            if operation_id is not None:
                locations_by_id[operation_id].append((path, method))
    return locations_by_id


def _normalize_openapi_operation_ids(schema: dict[str, Any]) -> dict[str, Any]:
    """Deterministically re-key any operationId collisions in ``schema``, in place.

    This is the one nondeterministic member found empirically in
    ``app.openapi()``'s output (every other part was byte-identical across
    repeated independent process runs). FastAPI's default
    ``generate_unique_id`` derives an operation's id from
    ``list(route.methods)[0]``: one arbitrary method plucked off a Python
    ``set``, used whenever a single route answers more than one HTTP
    method. That is exactly the shape of the two raw-intake catch-all
    routes in ``phantom.routes.catch_all`` (``raw_intake`` spans the
    upload verbs; ``raw_intake_unsupported`` spans
    GET/HEAD/DELETE/OPTIONS): every method on such a route is assigned
    the SAME operationId, and because ``set`` iteration order depends on
    Python's per-process string-hash randomization, WHICH method's name
    ends up embedded in that shared id changes from run to run (confirmed
    by diffing three independent ``uv run`` invocations of this exporter:
    three different winners). FastAPI itself warns "Duplicate Operation
    ID" for exactly this case.

    Every operationId used by more than one operation is replaced with an
    id derived only from that operation's own ``(path, method)`` pair,
    both already static and known at this point, so the result no longer
    depends on set iteration order. Ids that were already unique are left
    untouched.

    Args:
        schema: The raw OpenAPI document.

    Returns:
        The same dict, mutated in place.
    """
    for operation_id, locations in _collect_operations_by_id(schema).items():
        if len(locations) <= 1:
            continue
        logger.info(
            "normalizing %d operations colliding on operationId %r: %s",
            len(locations),
            operation_id,
            locations,
        )
        for path, method in locations:
            slug = re.sub(r"\W", "_", path).strip("_")
            schema["paths"][path][method]["operationId"] = f"{slug}_{method}"
    return schema


def _assert_unique_operation_ids(schema: dict[str, Any]) -> None:
    """Fail loudly if any operationId still collides after normalization.

    Args:
        schema: The OpenAPI document, after :func:`_normalize_openapi_operation_ids`.

    Raises:
        ContractExportError: If a collision remains, naming the id and
            every location that still carries it.
    """
    collisions = {
        operation_id: locations
        for operation_id, locations in _collect_operations_by_id(schema).items()
        if len(locations) > 1
    }
    if collisions:
        raise ContractExportError(
            f"operationId collisions remain after normalization: {collisions}"
        )


def _admin_openapi_bytes() -> bytes:
    """Build the FastAPI app and return its canonical OpenAPI document.

    The app is constructed (never started: no lifespan is entered) inside
    a fresh temporary directory so the Settings validator has a real,
    existing path to probe. Reading ``phantom.app.create_app`` confirms
    construction alone has no filesystem side effects of its own: logging
    setup, the non-loopback warning check, the metrics registry, and the
    per-instance settings snapshots are all in-memory. Every actual write
    (recovery, worker spawn, instance data-dir creation) happens inside the
    ASGI lifespan, which this function never enters.

    ``app.openapi()``'s output was diffed across repeated independent
    process runs; the only nondeterministic member found was a handful of
    colliding ``operationId`` values on the multi-method catch-all routes,
    fixed up by :func:`_normalize_openapi_operation_ids` (see its
    docstring for why). Everything else was byte-identical run to run.

    Returns:
        Canonical JSON bytes (see :func:`_canonical_json`).
    """
    with tempfile.TemporaryDirectory(prefix="phantom-contracts-openapi-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        logger.info("building FastAPI app for OpenAPI export (construction only, no lifespan)")
        app = create_app(_minimal_settings(tmp_path))
        schema = app.openapi()
        _assert_no_path_leak(schema, tmp_path)
        schema = _normalize_openapi_operation_ids(schema)
        _assert_unique_operation_ids(schema)
        return _canonical_json(schema)


# ---------------------------------------------------------------------------
# Wire header constants.
# ---------------------------------------------------------------------------


def _wire_headers_bytes() -> bytes:
    """Export every ``X-Phantom-*`` header constant's name and wire value.

    Introspects :mod:`phantom_client.headers` rather than importing each
    constant by name, so a header added or removed there is picked up
    automatically instead of silently drifting from this export.

    Returns:
        Canonical JSON bytes for a ``{constant_name: header_value}`` object.

    Raises:
        ContractExportError: If no ``X_PHANTOM_*`` constants are found
            (the module's naming convention changed underneath this script).
    """
    logger.info("exporting X-Phantom-* header constants from phantom_client.headers")
    constants: dict[str, str] = {
        name: value
        for name, value in vars(wire_headers).items()
        if name.startswith("X_PHANTOM_") and isinstance(value, str)
    }
    if not constants:
        raise ContractExportError(
            "no X_PHANTOM_* constants found on phantom_client.headers; "
            "the module's naming convention may have changed"
        )
    return _canonical_json(constants)


# ---------------------------------------------------------------------------
# Fixtures: one canonical example per wire model, construct -> validate -> dump.
# ---------------------------------------------------------------------------


def _build_example_chain_envelope() -> ChainEnvelope:
    """Construct the canonical two-step chain envelope fixture.

    Step one POSTs to create a file upstream and captures the presigned
    upload URL from its JSON response (marked ``sensitive`` per the
    capture's own docstring, which names presigned PUT URLs as exactly
    this case). Step two PUTs the buffered body to that captured URL via
    ``{{step.capture}}`` template substitution, carrying the body as a
    ``body_ref`` whose bytes ride alongside the envelope as a multipart
    part rather than inline in the JSON.
    """
    create_file_step = ChainStep(
        name="create_file",
        method="POST",
        url="https://files.example.com/v1/files",
        headers={"Content-Type": "application/json"},
        body=ChainBodyJson(value={"filename": "example.txt", "content_type": "text/plain"}),
        capture=[
            ChainCapture(
                name="upload_url",
                from_path="$.upload_url",
                ttl_seconds=_EXAMPLE_CAPTURE_TTL_SECONDS,
                sensitive=True,
            ),
        ],
        idempotency_header="Idempotency-Key",
    )
    upload_body_step = ChainStep(
        name="upload_body",
        method="PUT",
        url="{{create_file.upload_url}}",
        body=ChainBodyRef(name="file_bytes", content_type="text/plain"),
    )
    return ChainEnvelope(
        chain_id=_EXAMPLE_CHAIN_ID,
        idempotency_key=_EXAMPLE_IDEMPOTENCY_KEY,
        steps=[create_file_step, upload_body_step],
    )


def _build_example_error_envelope() -> ErrorEnvelope:
    """Construct the canonical error envelope fixture: a 404 ``not_found`` reply."""
    return ErrorEnvelope(
        error=ErrorBody(
            code="not_found",
            message="No upload found for the given chain_id.",
            instance_id="primary",
            request_id=_EXAMPLE_REQUEST_ID,
        )
    )


def _dump_validated[ModelT: BaseModel](instance: ModelT, model_cls: type[ModelT]) -> bytes:
    """Dump a model to canonical JSON, proving the JSON round-trips first.

    Serializes ``instance`` with ``model_dump(mode="json", by_alias=True)``,
    then re-validates the EXACT resulting bytes through
    ``model_cls.model_validate_json``. A fixture is never written unless
    the bytes about to land on disk are themselves accepted, as raw JSON,
    by the model they claim to exemplify.

    Deliberately ``model_validate_json`` rather than
    ``model_validate(json.loads(...))``: both ``ChainEnvelope`` and
    ``ErrorEnvelope`` set ``strict=True``, and pydantic's strict mode
    forbids the ordinary ``str`` -> ``UUID`` coercion when validating an
    already-parsed Python dict (a bare ``str`` for a ``UUID`` field reads
    as a type mismatch in "Python mode"). Parsing the same bytes as JSON
    text instead uses pydantic's JSON-parsing mode, which recognizes a
    field's canonical JSON string form (UUID, date, ...) even under
    ``strict=True``, because that string IS the only way JSON can carry
    that type. This is also the realistic path: a real caller (or a future
    Go implementation) receives these exact bytes as raw JSON over the
    wire, never as a pre-built Python dict.

    Args:
        instance: The constructed, in-memory model instance.
        model_cls: The model class to re-validate the dumped JSON against
            (equal to ``type(instance)`` at every call site; kept explicit
            so the re-validation type is never left to inference).

    Returns:
        Canonical JSON bytes, already proven to round-trip.

    Raises:
        ContractExportError: If re-validation fails.
    """
    dumped = instance.model_dump(mode="json", by_alias=True)
    encoded = _canonical_json(dumped)
    try:
        model_cls.model_validate_json(encoded)
    except ValidationError as exc:
        raise ContractExportError(
            f"{model_cls.__qualname__} fixture failed to round-trip through its own "
            f"JSON encoding: {exc}"
        ) from exc
    return encoded


def _chain_envelope_fixture_bytes() -> bytes:
    """Return the canonical chain-envelope fixture's validated JSON bytes."""
    logger.info("building and validating the chain-envelope fixture")
    return _dump_validated(_build_example_chain_envelope(), ChainEnvelope)


def _error_body_fixture_bytes() -> bytes:
    """Return the canonical error-envelope fixture's validated JSON bytes."""
    logger.info("building and validating the error-body fixture")
    return _dump_validated(_build_example_error_envelope(), ErrorEnvelope)


# ---------------------------------------------------------------------------
# README.
# ---------------------------------------------------------------------------

_README_TEXT: Final[str] = """# Phantom contracts

This directory holds Phantom's wire, admin, and config contracts as
language-neutral artifacts. Every file here is generated from the Python
source of truth by `scripts/export_contracts.py`. Do not hand-edit any
file in this directory. A hand edit is overwritten the next time the
exporter runs, and CI rejects a working tree where these files do not
match the generator's output.

## Regenerating

Run the exporter from the repository root:

```
uv run python scripts/export_contracts.py
```

To check for drift without writing anything (the same check CI runs):

```
uv run python scripts/export_contracts.py --check
```

`--check` regenerates every artifact into a temporary directory, compares
it byte for byte against this directory, prints a per-file report, and
exits 1 if anything differs.

## Files

- `chain-envelope.schema.json`: JSON Schema for `ChainEnvelope` (ADR-010).
- `chain-response.schema.json`: JSON Schema for `ChainResponse`.
- `error-body.schema.json`: JSON Schema for `ErrorEnvelope`, the
  `{"error": {...}}` shape Phantom returns on every error response.
- `settings.schema.json`: JSON Schema for the top-level `Settings` model
  (the YAML config shape).
- `admin-openapi.json`: the OpenAPI document for the intake, admin, and
  health surface.
- `wire-headers.json`: the `X-Phantom-*` header constant names and values.
- `fixtures/chain-envelope.example.json`: one canonical two-step chain
  envelope.
- `fixtures/error-body.example.json`: one canonical error envelope.

## Why this exists

A planned Go implementation of Phantom needs a contract to build against
that does not require reading Python. These artifacts, plus the e2e
conformance suite, are that contract. When a Python model in
`phantom.models` or `phantom.config.settings` changes, regenerate this
directory in the same change and commit the result.
"""


def _readme_bytes() -> bytes:
    """Return the fixed ``contracts/README.md`` content."""
    return _README_TEXT.encode("utf-8")


# ---------------------------------------------------------------------------
# Driver: generate, write, compare.
# ---------------------------------------------------------------------------


def _generate_artifacts() -> dict[str, bytes]:
    """Generate every contract artifact, keyed by path relative to ``contracts/``.

    The single function both modes share: write mode writes this mapping
    to ``contracts/`` directly, and ``--check`` writes the same mapping to
    a temporary directory before comparing. There is exactly one code path
    that decides what an artifact contains.

    Returns:
        A mapping from POSIX-style relative path to the exact bytes that
        belong at that path.
    """
    return {
        "chain-envelope.schema.json": _schema_bytes(ChainEnvelope),
        "chain-response.schema.json": _schema_bytes(ChainResponse),
        "error-body.schema.json": _schema_bytes(ErrorEnvelope),
        "settings.schema.json": _schema_bytes(Settings),
        "admin-openapi.json": _admin_openapi_bytes(),
        "wire-headers.json": _wire_headers_bytes(),
        "fixtures/chain-envelope.example.json": _chain_envelope_fixture_bytes(),
        "fixtures/error-body.example.json": _error_body_fixture_bytes(),
        "README.md": _readme_bytes(),
    }


def _write_artifacts(artifacts: dict[str, bytes], root: Path) -> None:
    """Write every artifact to its path under ``root``, creating directories as needed.

    Args:
        artifacts: Mapping from relative path to file bytes.
        root: Destination directory (``contracts/`` or a temp directory).
    """
    for rel_path, content in artifacts.items():
        target = root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        logger.info("wrote %s", target)


def _drift_report(generated_root: Path) -> list[str]:
    """Compare a freshly generated artifact tree against the committed ``contracts/`` tree.

    Args:
        generated_root: Directory a fresh :func:`_generate_artifacts` output
            was just written to.

    Returns:
        One human-readable line per drifted file: content that differs, an
        artifact the generator produces but ``contracts/`` lacks, or a file
        under ``contracts/`` the generator no longer produces. Empty when
        the two trees are byte-identical.
    """
    generated_files = _relative_files_under(generated_root)
    committed_files = _relative_files_under(CONTRACTS_DIR)
    lines: list[str] = []
    for rel_path in sorted(generated_files & committed_files):
        generated_bytes = (generated_root / rel_path).read_bytes()
        committed_bytes = (CONTRACTS_DIR / rel_path).read_bytes()
        if generated_bytes != committed_bytes:
            lines.append(
                f"MODIFIED   {rel_path} "
                f"({len(committed_bytes)} bytes committed vs {len(generated_bytes)} generated)"
            )
    for rel_path in sorted(generated_files - committed_files):
        lines.append(f"MISSING    {rel_path} (generated, but not present under contracts/)")
    for rel_path in sorted(committed_files - generated_files):
        lines.append(f"UNEXPECTED {rel_path} (present under contracts/, but no longer generated)")
    return lines


def _run_write() -> int:
    """Regenerate every artifact under ``contracts/`` in place.

    Returns:
        0 always (a failed generation raises rather than returning non-zero).
    """
    artifacts = _generate_artifacts()
    _write_artifacts(artifacts, CONTRACTS_DIR)
    sys.stdout.write(f"wrote {len(artifacts)} contract artifact(s) under {CONTRACTS_DIR}:\n")
    for rel_path in sorted(artifacts):
        sys.stdout.write(f"  {rel_path}\n")
    return 0


def _run_check() -> int:
    """Regenerate into a temp directory and byte-compare against ``contracts/``.

    Returns:
        1 if any drift is found (report printed to stdout), 0 if clean.
    """
    artifacts = _generate_artifacts()
    with tempfile.TemporaryDirectory(prefix="phantom-contracts-check-") as tmp_dir:
        generated_root = Path(tmp_dir)
        _write_artifacts(artifacts, generated_root)
        drift_lines = _drift_report(generated_root)
    if drift_lines:
        sys.stdout.write("contract drift detected:\n")
        for line in drift_lines:
            sys.stdout.write(f"  {line}\n")
        sys.stdout.write("\nRegenerate with: uv run python scripts/export_contracts.py\n")
        return 1
    sys.stdout.write("contracts/ matches the generator output for every artifact; no drift.\n")
    return 0


def main() -> int:
    """Entry point: regenerate contract artifacts, or check them for drift.

    Default mode (no arguments) regenerates every artifact under
    ``contracts/`` in place. ``--check`` regenerates into a temporary
    directory and byte-compares the result against the committed
    ``contracts/`` tree without writing, for use as a CI drift gate.

    Returns:
        Process exit code: see the module docstring's Exit codes section.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        if _CHECK_FLAG in sys.argv[1:]:
            return _run_check()
        return _run_write()
    except ContractExportError as exc:
        logger.error("contract export failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
