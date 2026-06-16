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
way: a subset of admin response models are deliberately
**extras-tolerated** on the SDK side. ``UploadRow``,
``AdminStatusResponse``, ``InstanceSummary``, ``HealthResponse``,
``ReadyResponse``, and ``ListUploadsResponse`` use ``extra="ignore"``
on the SDK so additional service-emitted fields surface as silent
drops rather than parse failures. The service-side equivalents use
``extra="forbid"``. This is a documented invariant, not drift.

Because of that, the test runs in two modes:

- **Strict-match models** (both sides ``extra="forbid"``): the
  dereferenced, description-stripped JSON Schemas must match
  byte-for-byte.
- **Extras-tolerated models** (SDK ``extra="ignore"``, service
  ``extra="forbid"``): only the fields the SDK declares are
  compared. Service-emitted fields the SDK doesn't know about are
  allowed (the SDK drops them silently); SDK-declared fields the
  service doesn't emit are not — that's broken-on-arrival.

See:

- ADR-004: admin endpoints loopback / no auth.
- ADR-007: admin status surface.
- ADR-010 (and ``test_chain_models_alignment.py``): the duplication
  policy this test extends to the admin surface.
"""

from __future__ import annotations

import difflib
import json
from typing import Any

import pytest
from phantom.models import admin as service_admin
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
)
"""Models compared by full schema byte-equality (both sides forbid extras)."""


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
    """Pydantic JSON schema, $ref-dereferenced and doc-key-stripped."""
    return _strip_doc_keys(_dereference(model.model_json_schema()))  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Strict-match tests — for models that should be byte-equal across packages.
# ---------------------------------------------------------------------------


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
