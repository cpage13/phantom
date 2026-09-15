"""Drift-detection contract test for the duplicated admin-response models.

Phantom's admin endpoints emit a set of Pydantic-shaped responses
defined in ``phantom.models.admin`` and ``phantom.models.upload``.
``phantom-client`` duplicates those models locally (under
``phantom_client.models.admin`` / ``phantom_client.models.status``) so
the SDK has no runtime dependency on the service package; that
duplication is what this test enforces. The duplication policy
mirrors the chain-envelope policy in
``test_chain_models_alignment.py``: byte-equality of the wire-relevant
shape, descriptions excluded from comparison, drift surfaces as a
test failure with a unified-diff report.

The admin surface differs from the chain envelope in one important
way: EVERY admin RESPONSE model is deliberately **extras-tolerated**
on the SDK side, while its service-side equivalent forbids extras.
That asymmetry is a contract decision, not drift. A published SDK
release outlives the service version it was written against, so a
caller pinned to an older client must not fault when a newer service
adds a response field. The service stays strict because it deploys as
one unit with its own models, where an unknown field is a real defect.

REQUEST bodies are strict on BOTH sides, because they travel the other
way: the SDK builds them and the service validates them, so tolerating
an unknown field would silently swallow a caller's typo. The SDK
declares which models those are in
``phantom_client.models.admin.REQUEST_BODY_MODELS``.

Because that one boolean is expected to differ, ``_wire_schema``
strips ``additionalProperties`` before comparing, and
``test_extras_policy_is_the_only_sanctioned_divergence`` asserts the
policy directly for every strict model. Stripping a key from a drift
test is only safe when something else pins it, and that test is what
pins it. Everything that actually binds the wire (types, defaults,
enums, required-ness) is still compared byte-for-byte.

The test runs in two modes:

- **Strict-match models**: the dereferenced, description-stripped,
  extras-policy-stripped JSON Schemas must match byte-for-byte.
- **Extras-tolerated models**: only the fields the SDK declares are
  compared. Service-emitted fields the SDK doesn't know about are
  allowed (the SDK drops them silently); SDK-declared fields the
  service doesn't emit are not, that's broken-on-arrival.

Both registries below are hand-written, and a hand-written registry is
only as good as its completeness check.
:func:`test_every_cross_package_duplicate_is_pinned_by_a_drift_test`
walks both packages and fails if any class duplicated across them is in
no registry at all, here or in the chain-envelope sibling. Without it a
duplicated model is unguarded exactly when someone forgets it, which is
when the guard matters (finding S12-5). The comment further down records
that six models had already escaped this net once, one of them drifted;
the fix then was to hand-add six names rather than to close the hole,
and the sweep found four more when it was finally added:
``SigV4StaticCredBody``, ``ProfileRefCredBody``, ``SigningService`` and
``ResolvedDefaultsSummary``.

See:

- ADR-004: admin endpoints loopback / no auth.
- ADR-007: admin status surface.
- ADR-010 (and ``test_chain_models_alignment.py``): the duplication
  policy this test extends to the admin surface.
"""

from __future__ import annotations

import difflib
import importlib
import importlib.util
import inspect
import json
import pkgutil
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from phantom.models import admin as service_admin
from phantom.models import credential as service_credential
from phantom.models import upload as service_upload
from phantom_client.models import admin as client_admin
from phantom_client.models import status as client_status
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Model registry.
#
# Each entry maps the canonical name used in this test (== the service-side
# class name) to the location it lives in each package and whether the SDK
# treats the model as strict-match or extras-tolerated. The SDK-side class
# name is required to match the service-side name; the model location is
# the only thing that varies.
# ---------------------------------------------------------------------------

_STRICT_MODELS: tuple[tuple[str, type[BaseModel], type[BaseModel]], ...] = (
    ("StatsResponse", service_admin.StatsResponse, client_status.StatsResponse),
    (
        "InstanceStatusResponse",
        service_admin.InstanceStatusResponse,
        client_admin.InstanceStatusResponse,
    ),
    ("TierBreakdown", service_admin.TierBreakdown, client_status.TierBreakdown),
    ("StateBreakdown", service_admin.StateBreakdown, client_status.StateBreakdown),
    (
        "SaturationStatus",
        service_admin.SaturationStatus,
        client_status.SaturationStatus,
    ),
    ("AuthStatus", service_admin.AuthStatus, client_status.AuthStatus),
    ("TokenSlot", service_admin.TokenSlot, client_status.TokenSlot),
    ("CapturedValues", service_upload.CapturedValues, client_status.CapturedValues),
    (
        "CapturedStepValues",
        service_upload.CapturedStepValues,
        client_status.CapturedStepValues,
    ),
    # Plan § 4.2.5 observability models.
    ("CounterValue", service_admin.CounterValue, client_admin.CounterValue),
    ("CountersResponse", service_admin.CountersResponse, client_admin.CountersResponse),
    ("GaugeValue", service_admin.GaugeValue, client_admin.GaugeValue),
    ("GaugesResponse", service_admin.GaugesResponse, client_admin.GaugesResponse),
    (
        "RamPressureStatusResponse",
        service_admin.RamPressureStatusResponse,
        client_admin.RamPressureStatusResponse,
    ),
    # Plan § 5.2.5 quarantine inventory (cycle-7 seam 2: keyed by backup_id).
    ("QuarantineEntry", service_admin.QuarantineEntry, client_admin.QuarantineEntry),
    (
        "QuarantineInventoryResponse",
        service_admin.QuarantineInventoryResponse,
        client_admin.QuarantineInventoryResponse,
    ),
    # Plan § 1.5 one-call admin restore. The restore REQUEST model is gone on
    # both sides (cycle-7 seam 2): the route addresses the backup by the
    # backup_id query parameter and takes no JSON body.
    (
        "QuarantineRestoreResponse",
        service_admin.QuarantineRestoreResponse,
        client_admin.QuarantineRestoreResponse,
    ),
    # Cycle-7 task 4.5: the group rollup and either-identifier lookup
    # surface (plan section 5; field lists copied exactly from the
    # client-api design sections 3 and 4 on both sides).
    ("GroupMember", service_admin.GroupMember, client_admin.GroupMember),
    (
        "GroupStatusResponse",
        service_admin.GroupStatusResponse,
        client_admin.GroupStatusResponse,
    ),
    (
        "UploadStatusSummary",
        service_admin.UploadStatusSummary,
        client_admin.UploadStatusSummary,
    ),
    (
        "IdentifierLookupResponse",
        service_admin.IdentifierLookupResponse,
        client_admin.IdentifierLookupResponse,
    ),
    # Round 2 adversary hardening: six shared admin models had escaped
    # this net entirely (neither alignment module covered them) although
    # both sides duplicate them deliberately per ADR-012. Five compared
    # byte-equal at discovery; the sixth, KeyValueMatchFilter, had
    # drifted (the client constrained key/value with min_length=1, the
    # service did not) and was re-aligned in the stricter direction by
    # the round 2 defender fix R2-1. All six are pinned strict here.
    (
        "ChainAdminDetail",
        service_admin.ChainAdminDetail,
        client_admin.ChainAdminDetail,
    ),
    (
        "ChainAdminStepDetail",
        service_admin.ChainAdminStepDetail,
        client_admin.ChainAdminStepDetail,
    ),
    ("ExtractFilter", service_admin.ExtractFilter, client_admin.ExtractFilter),
    ("DeleteFilter", service_admin.DeleteFilter, client_admin.DeleteFilter),
    (
        "BulkDeleteResponse",
        service_admin.BulkDeleteResponse,
        client_admin.BulkDeleteResponse,
    ),
    (
        "KeyValueMatchFilter",
        service_admin.KeyValueMatchFilter,
        client_admin.KeyValueMatchFilter,
    ),
    # Finding S12-5: the completeness sweep below found four more
    # duplicated types in NEITHER registry. Three are the request bodies
    # for PUT /v1/admin/credentials/{dest_host} (ADR-033), so unguarded
    # drift there fails every operator credential push at runtime with no
    # test turning red; the fourth is the resolved-defaults echo on the
    # admin status surface. All four compared byte-equal when pinned, so
    # this closed an unguarded gap rather than a live drift.
    (
        "SigV4StaticCredBody",
        service_credential.SigV4StaticCredBody,
        client_admin.SigV4StaticCredBody,
    ),
    (
        "ProfileRefCredBody",
        service_credential.ProfileRefCredBody,
        client_admin.ProfileRefCredBody,
    ),
    (
        "ResolvedDefaultsSummary",
        service_admin.ResolvedDefaultsSummary,
        client_admin.ResolvedDefaultsSummary,
    ),
)
"""Models compared by full schema byte-equality.

The comparison excludes ``additionalProperties``, which is expected to differ:
the service forbids extras and the SDK's RESPONSE models tolerate them. Request
bodies in this tuple forbid on both sides. Both halves of that rule are asserted
by :func:`test_extras_policy_is_the_only_sanctioned_divergence`.
"""


_EXTRAS_TOLERATED_MODELS: tuple[tuple[str, type[BaseModel], type[BaseModel]], ...] = (
    (
        "ListUploadsResponse",
        service_admin.ListUploadsResponse,
        client_admin.ListUploadsResponse,
    ),
    ("HealthResponse", service_admin.HealthResponse, client_status.HealthResponse),
    ("ReadyResponse", service_admin.ReadyResponse, client_status.ReadyResponse),
    ("UploadRow", service_upload.UploadRow, client_status.UploadRow),
    (
        "AdminStatusResponse",
        service_admin.AdminStatusResponse,
        client_admin.AdminStatusResponse,
    ),
    ("InstanceSummary", service_admin.InstanceSummary, client_admin.InstanceSummary),
)
"""Models where the SDK deliberately tolerates extra fields the service emits.

For each, the test asserts: every field the SDK declares is present in
the service schema with a compatible shape. Service-emitted fields not
known to the SDK are permitted (silently dropped by the SDK).
"""


_ALL_MODELS = _STRICT_MODELS + _EXTRAS_TOLERATED_MODELS


_SHARED_ENUMS: tuple[tuple[str, type[Enum], type[Enum]], ...] = (
    ("SigningService", service_credential.SigningService, client_admin.SigningService),
)
"""Closed enums duplicated across both packages.

An enum has no JSON Schema of its own to diff here; its wire contract is
the member-name to member-value map, compared by
:func:`test_shared_enum_members_match`. ``SigningService`` is REQUIRED on
every destination credential, so a member the SDK can emit and the
service cannot parse rejects a credential push outright.
"""


# ---------------------------------------------------------------------------
# Completeness sweep (finding S12-5).
#
# A hand-maintained registry silently stops guarding a model the moment
# someone forgets to add one. These helpers enumerate what is ACTUALLY
# duplicated across the two packages so the forgetting is what fails.
# ---------------------------------------------------------------------------


def _wire_classes(package_name: str) -> dict[str, str]:
    """Map every duplicated-candidate class name in a package to its module.

    Walks the package's own modules and keeps classes DEFINED there (a
    re-export under another module's name is the same class, not a second
    copy). Pydantic models and enums both count: both bind the wire.

    Args:
        package_name: Importable package, e.g. ``"phantom.models"``.

    Returns:
        ``{class name: defining module name}``.
    """
    package = importlib.import_module(package_name)
    found: dict[str, str] = {}
    for module_info in pkgutil.walk_packages(package.__path__, prefix=f"{package_name}."):
        module = importlib.import_module(module_info.name)
        for name, obj in vars(module).items():
            if not inspect.isclass(obj) or obj.__module__ != module_info.name:
                continue
            if issubclass(obj, BaseModel | Enum):
                found[name] = module_info.name
    return found


def _duplicated_class_names() -> dict[str, tuple[str, str]]:
    """Return ``{name: (service module, client module)}`` for every duplicate.

    A class name defined in BOTH packages is a deliberately duplicated
    wire type per ADR-012, and therefore something a drift test must pin.
    """
    service = _wire_classes("phantom.models")
    client = _wire_classes("phantom_client.models")
    return {name: (service[name], client[name]) for name in sorted(set(service) & set(client))}


def _chain_envelope_model_names() -> tuple[str, ...]:
    """Read the chain-envelope registry out of the sibling module, BY PATH.

    Loaded by file path rather than by dotted name on purpose: the service
    package ships its own ``tests`` package (``src/phantom-service/tests/
    __init__.py``), which SHADOWS the repo-root ``tests`` namespace package the
    moment ``src/phantom-service`` lands on ``sys.path``, which is exactly what
    happens in the full-suite run. A dotted ``tests.contract...`` import here
    works when this directory is run alone and fails in CI.

    Returns:
        The sibling's ``_MODEL_NAMES`` tuple.

    Raises:
        RuntimeError: When the sibling module cannot be loaded, which would
            otherwise leave the completeness check quietly under-informed.
    """
    sibling = Path(__file__).with_name("test_chain_models_alignment.py")
    spec = importlib.util.spec_from_file_location("_chain_models_registry", sibling)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the chain-envelope model registry from {sibling}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names: tuple[str, ...] = module._MODEL_NAMES
    return names


_REGISTERED_NAMES: frozenset[str] = frozenset(
    {name for name, _, _ in _ALL_MODELS}
    | {name for name, _, _ in _SHARED_ENUMS}
    | set(_chain_envelope_model_names())
)
"""Every name pinned by a drift test, here or in the chain-envelope sibling.

Read from the sibling module rather than copied, so deleting an entry
there surfaces as an unregistered duplicate here instead of a stale
copy quietly covering for it.
"""


# ---------------------------------------------------------------------------
# Schema-normalization helpers.
#
# Mirror the helpers in test_chain_models_alignment.py. The chain models
# share the same helper logic; the admin test duplicates rather than
# imports to keep the contract test self-contained — both files are
# load-bearing wire-protocol gates and should fail loudly on their own.
# ---------------------------------------------------------------------------


_DOC_KEYS: frozenset[str] = frozenset({"description", "title"})
"""Schema keys we drop before comparing. Description and title are
documentation-only; they don't bind the wire shape."""


def _strip_doc_keys(node: Any) -> Any:
    """Recursively remove documentation-only keys from a JSON Schema."""
    if isinstance(node, dict):
        return {k: _strip_doc_keys(v) for k, v in node.items() if k not in _DOC_KEYS}
    if isinstance(node, list):
        return [_strip_doc_keys(v) for v in node]
    return node


def _strip_extras_policy(node: Any) -> Any:
    """Recursively remove ``additionalProperties``, the ONE sanctioned divergence.

    The two packages deliberately disagree here, and only here. The service's
    response models forbid unknown fields; the SDK's tolerate them, because a
    published SDK release outlives the service version it was written against
    and a pinned 0.1.0 client must not fault when a 0.2.0 service adds a field.
    That asymmetry is a contract decision, not drift.

    It is stripped rather than accepted quietly, because leaving it in would
    force these models into the weaker names-and-required comparison, which
    would stop detecting per-field TYPE, DEFAULT and ENUM drift on sixteen
    models to express one boolean. Stripping keeps full byte-equality on
    everything that binds the wire, and
    :func:`test_extras_policy_is_the_only_sanctioned_divergence` asserts the
    policy itself directly, so it stays checked rather than merely ignored.
    """
    if isinstance(node, dict):
        return {k: _strip_extras_policy(v) for k, v in node.items() if k != "additionalProperties"}
    if isinstance(node, list):
        return [_strip_extras_policy(v) for v in node]
    return node


def _dereference(schema: dict[str, Any]) -> dict[str, Any]:
    """Inline every ``$ref`` against ``$defs`` so two schemas that differ
    only in inline-vs-ref'd subschema encoding compare equal.

    The admin-models tree has no cycles, so simple recursive inlining
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
    """Pydantic JSON schema: dereferenced, doc-stripped, extras-policy-stripped."""
    return _strip_extras_policy(  # type: ignore[no-any-return]
        _strip_doc_keys(_dereference(model.model_json_schema()))
    )


# ---------------------------------------------------------------------------
# Strict-match tests — for models that should be byte-equal across packages.
# ---------------------------------------------------------------------------


def test_every_cross_package_duplicate_is_pinned_by_a_drift_test() -> None:
    """Objective: no deliberately duplicated wire type can sit outside the drift gate.

    Expected outcome: every class defined as a Pydantic model or an enum
    in BOTH ``phantom.models`` and ``phantom_client.models`` appears in
    one of the registries, this module's ``_STRICT_MODELS`` /
    ``_EXTRAS_TOLERATED_MODELS`` / ``_SHARED_ENUMS``, or the chain
    envelope's ``_MODEL_NAMES``.

    Why it exists (finding S12-5): the registries are hand-written and
    nothing enumerated the packages to prove they were complete, so a
    duplicated model was unguarded exactly when someone forgot to add
    it. Six had already escaped once and one of those six had drifted;
    this sweep found four more, three of them the request bodies for the
    admin credential push.

    Falsifier: duplicate a new model across both packages, or delete an
    entry from either registry, and this goes RED naming the class and
    both defining modules.
    """
    duplicated = _duplicated_class_names()
    # Guard the guard: if the walk finds nothing, the sweep is broken and
    # every other assertion in this test would hold vacuously.
    assert duplicated, (
        "the cross-package sweep found NO duplicated classes, which cannot be "
        "true; _wire_classes is no longer walking the model packages"
    )
    unregistered = {
        name: modules for name, modules in duplicated.items() if name not in _REGISTERED_NAMES
    }
    assert not unregistered, (
        "duplicated wire types with no drift test pinning them:\n"
        + "\n".join(
            f"  {name}: service={service_module} client={client_module}"
            for name, (service_module, client_module) in sorted(unregistered.items())
        )
        + "\nAdd each to _STRICT_MODELS, _EXTRAS_TOLERATED_MODELS or _SHARED_ENUMS "
        "in this file (or to _MODEL_NAMES in test_chain_models_alignment.py for a "
        "chain-envelope type)."
    )


@pytest.mark.parametrize(
    ("enum_name", "service_enum", "client_enum"),
    _SHARED_ENUMS,
    ids=[name for name, _, _ in _SHARED_ENUMS],
)
def test_shared_enum_members_match(
    enum_name: str,
    service_enum: type[Enum],
    client_enum: type[Enum],
) -> None:
    """Objective: a duplicated closed enum offers identical members and wire values.

    Expected outcome: the member-name to member-value maps are equal in
    both directions. A member only the SDK knows is a value the service
    rejects; a member only the service knows is a value the SDK cannot
    represent.
    """
    service_members = {member.name: member.value for member in service_enum}
    client_members = {member.name: member.value for member in client_enum}
    assert service_members == client_members, (
        f"{enum_name} members diverge: service={service_members}, client={client_members}"
    )


@pytest.mark.parametrize(
    ("model_name", "service_model", "client_model"),
    _STRICT_MODELS,
    ids=[name for name, _, _ in _STRICT_MODELS],
)
def test_strict_wire_schemas_match(
    model_name: str,
    service_model: type[BaseModel],
    client_model: type[BaseModel],
) -> None:
    """Strict-match: the dereferenced JSON schemas must be byte-equal."""
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


@pytest.mark.parametrize(
    ("model_name", "service_model", "client_model"),
    _ALL_MODELS,
    ids=[name for name, _, _ in _ALL_MODELS],
)
def test_field_names_match(
    model_name: str,
    service_model: type[BaseModel],
    client_model: type[BaseModel],
) -> None:
    """Every SDK-declared field is also declared by the service.

    The extras-tolerated contract is one-directional: the service MAY
    emit fields the SDK doesn't know about (the SDK ignores them); the
    SDK MUST NOT declare fields the service doesn't emit (the SDK
    would error or populate a stale default).
    """
    s_fields = set(service_model.model_fields.keys())
    c_fields = set(client_model.model_fields.keys())
    client_only = c_fields - s_fields
    assert not client_only, (
        f"{model_name}: SDK declares fields the service doesn't emit: {client_only}"
    )


@pytest.mark.parametrize(
    ("model_name", "service_model", "client_model"),
    _ALL_MODELS,
    ids=[name for name, _, _ in _ALL_MODELS],
)
def test_field_required_status_alignment(
    model_name: str,
    service_model: type[BaseModel],
    client_model: type[BaseModel],
) -> None:
    """For shared fields: required-on-SDK implies required-on-service.

    The SDK MAY relax a service-required field to optional (it's the
    SDK's choice to accept incomplete payloads). The SDK MUST NOT
    require a field the service treats as optional — the SDK would
    reject valid service responses.
    """
    s_fields = service_model.model_fields
    c_fields = client_model.model_fields
    for name in set(s_fields) & set(c_fields):
        s_required = s_fields[name].is_required()
        c_required = c_fields[name].is_required()
        if c_required and not s_required:
            raise AssertionError(
                f"{model_name}.{name}: SDK requires field that service treats "
                f"as optional. Service might omit it; SDK would reject the response."
            )


@pytest.mark.parametrize(
    ("model_name", "service_model", "client_model"),
    _ALL_MODELS,
    ids=[name for name, _, _ in _ALL_MODELS],
)
def test_field_aliases_match(
    model_name: str,
    service_model: type[BaseModel],
    client_model: type[BaseModel],
) -> None:
    """Each shared field's wire alias matches across packages.

    The service determines the wire JSON keys via ``Field(alias=...)``;
    the SDK must use the same alias so it parses the actual wire bytes.
    """
    s_fields = service_model.model_fields
    c_fields = client_model.model_fields
    for name in set(s_fields) & set(c_fields):
        assert s_fields[name].alias == c_fields[name].alias, (
            f"{model_name}.{name} alias mismatch: "
            f"service={s_fields[name].alias!r}, client={c_fields[name].alias!r}"
        )


@pytest.mark.parametrize(
    ("model_name", "service_model", "client_model"),
    _STRICT_MODELS,
    ids=[name for name, _, _ in _STRICT_MODELS],
)
def test_extras_policy_is_the_only_sanctioned_divergence(
    model_name: str,
    service_model: type[BaseModel],
    client_model: type[BaseModel],
) -> None:
    """Objective: pin the ONE way the two packages are allowed to disagree.

    Expected: the service response model forbids unknown fields and the SDK's
    tolerates them.

    ``_wire_schema`` strips ``additionalProperties`` so the byte-equality test
    can keep checking everything that binds the wire (types, defaults, enums,
    required-ness) without one deliberate boolean forcing sixteen models into a
    weaker comparison. Stripping a key from a drift test is only safe if
    something else asserts it, which is this test.

    The asymmetry is a contract decision. A published SDK release outlives the
    service version it was written against, so a caller pinned to an older
    client must not fault when a newer service adds a response field. The
    service stays strict because it is deployed as a unit with its own models
    and an unknown field there is a real defect.
    """
    service_extras = service_model.model_config.get("extra")
    client_extras = client_model.model_config.get("extra")

    assert service_extras == "forbid", (
        f"{model_name}: the service model should forbid unknown fields, got {service_extras!r}"
    )

    if model_name in client_admin.REQUEST_BODY_MODELS:
        # A request body travels the other way. The SDK builds it and the
        # service validates it, so tolerating an unknown field there would
        # silently drop a caller's typo instead of surfacing it. Strict on
        # both sides is correct and is asserted rather than assumed.
        assert client_extras == "forbid", (
            f"{model_name} is a request body, so the SDK must forbid unknown "
            f"fields and surface a caller typo, got {client_extras!r}"
        )
        return

    assert client_extras == "ignore", (
        f"{model_name}: the SDK response model must tolerate unknown fields so "
        f"a pinned client survives a newer service, got {client_extras!r}"
    )
