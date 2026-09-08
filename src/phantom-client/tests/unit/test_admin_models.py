"""Unit tests for ``phantom_client.models.admin``."""

from __future__ import annotations

import json
from datetime import datetime
from uuid import uuid4

import pytest
from phantom_client.models import admin as admin_models
from phantom_client.models.admin import (
    AdminStatusResponse,
    BulkDeleteResponse,
    DeleteFilter,
    ExtractFilter,
    GroupStatusResponse,
    InstanceStatusResponse,
    InstanceSummary,
    KeyValueMatchFilter,
    UploadBundle,
)
from pydantic import BaseModel, ValidationError


def test_extract_filter_all_optional() -> None:
    """ExtractFilter accepts an empty body."""
    f = ExtractFilter()
    assert f.state is None
    assert f.route is None
    assert f.since is None
    assert f.chain_ids is None
    assert f.instance is None


def test_extract_filter_chain_ids_list() -> None:
    """ExtractFilter parses a list of UUIDs."""
    a, b = uuid4(), uuid4()
    f = ExtractFilter.model_validate_json(f'{{"chain_ids": ["{a}", "{b}"]}}')
    assert f.chain_ids is not None
    assert {*f.chain_ids} == {a, b}


def test_extract_filter_extra_forbidden() -> None:
    """ExtractFilter rejects unknown fields."""
    with pytest.raises(ValidationError):
        ExtractFilter.model_validate({"surprise": 1})


def test_delete_filter_is_empty() -> None:
    """DeleteFilter.is_empty distinguishes empty from non-empty."""
    assert DeleteFilter().is_empty() is True
    assert not DeleteFilter(state="failed").is_empty()
    assert not DeleteFilter(route="x").is_empty()
    assert not DeleteFilter(instance="primary").is_empty()
    since = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    assert not DeleteFilter(since=since).is_empty()


def test_key_value_match_filter_min_length() -> None:
    """KeyValueMatchFilter rejects empty strings."""
    KeyValueMatchFilter(key="phantom_local_uuid", value="x")  # ok
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="", value="x")
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="x", value="")


def test_bulk_delete_response() -> None:
    """BulkDeleteResponse parses with non-negative deleted count."""
    r = BulkDeleteResponse(deleted=5)
    assert r.deleted == 5
    with pytest.raises(ValidationError):
        BulkDeleteResponse(deleted=-1)


def test_instance_summary_refresh_strategy_literal() -> None:
    """InstanceSummary.refresh_strategy is wait or ad_client_credentials."""
    s = InstanceSummary(id="primary", refresh_strategy="wait", in_flight=0)
    assert s.refresh_strategy == "wait"
    with pytest.raises(ValidationError):
        InstanceSummary(id="x", refresh_strategy="nope", in_flight=0)  # type: ignore[arg-type]


def test_admin_status_response_basic() -> None:
    """AdminStatusResponse parses the documented shape."""
    r = AdminStatusResponse.model_validate(
        {
            "ready": True,
            "disk_usage_bytes": 1024,
            "total_backlog": 0,
            "instances": [{"id": "primary", "refresh_strategy": "wait", "in_flight": 0}],
        }
    )
    assert r.ready is True
    assert r.ad_reachability == "not_configured"
    assert len(r.instances) == 1


def test_instance_status_response_nested_shape() -> None:
    """InstanceStatusResponse parses the nested shape Phantom emits."""
    r = InstanceStatusResponse.model_validate(
        {
            "id": "primary",
            "ready": True,
            "in_flight": {"count": 2, "bytes": 1024},
            "by_state": {
                "queued": {"count": 2, "bytes": 1024},
                "attempting": {"count": 0, "bytes": 0},
                "auth_expired": {"count": 0, "bytes": 0},
                "stored": {"count": 0, "bytes": 0},
                "succeeded_recent": {"count": 0, "bytes": 0},
                "failed_recent": {"count": 0, "bytes": 0},
            },
            "auth": {"phantom_token_expires_at": None, "auth_expired_count": 0},
            "disk_usage_bytes": 0,
        }
    )
    assert r.id == "primary"
    assert r.ready is True
    assert r.in_flight.count == 2
    assert r.by_state.queued.count == 2
    assert r.auth.auth_expired_count == 0
    assert r.degraded_durability is False


def test_upload_bundle_carries_body_refs() -> None:
    """UploadBundle decodes the wire body_refs hex map to bytes (R-EX2).

    The server emits each ref's bytes as a hex string under ``body_refs``;
    the SDK model decodes that to a name -> bytes map, the richer shape
    that keeps every declared ref distinct.
    """
    chain_id = uuid4()
    group_id = uuid4()
    now = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    bundle = UploadBundle.model_validate(
        {
            "metadata": {
                "chain_id": str(chain_id),
                "instance_id": "primary",
                "group_id": str(group_id),
                "route_name": "primary",
                "state": "succeeded",
                "body_location": "ram",
                "received_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "endpoint": "x",
                "uid": "y",
                "idempotency_key": "k",
                "capture_reexecution_active": False,
            },
            "body_refs": {"body": b"hello".hex()},
        }
    )
    assert bundle.body_refs == {"body": b"hello"}
    assert bundle.metadata.chain_id == chain_id


# ---------------------------------------------------------------------------
# Forward-compatibility rule: request models forbid extras, responses ignore.
# ---------------------------------------------------------------------------


def _models_defined_in_admin_module() -> dict[str, type[BaseModel]]:
    """Every ``BaseModel`` subclass declared in ``phantom_client.models.admin``."""
    return {
        name: obj
        for name, obj in vars(admin_models).items()
        if isinstance(obj, type)
        and issubclass(obj, BaseModel)
        and obj.__module__ == admin_models.__name__
    }


def test_extras_policy_holds_for_every_model() -> None:
    """Objective: the module's documented extras rule holds for EVERY model.

    Expected: each model named in ``REQUEST_BODY_MODELS`` uses
    ``extra='forbid'`` and every other model in the module uses
    ``extra='ignore'``.

    Eleven response models had drifted to ``extra='forbid'`` against the rule
    the module docstring states, so a single new field on a service response
    turned into a ``PhantomEnvelopeError`` on every already-shipped client.
    This walks the module rather than listing names, so the twelfth model
    cannot drift either way without failing here.
    """
    wrong: list[str] = []
    for name, model in sorted(_models_defined_in_admin_module().items()):
        expected = "forbid" if name in admin_models.REQUEST_BODY_MODELS else "ignore"
        actual = model.model_config.get("extra")
        if actual != expected:
            wrong.append(f"{name}: extra={actual!r}, expected {expected!r}")
    assert not wrong, "extras policy violated:\n" + "\n".join(wrong)


def test_request_body_models_registry_names_real_models() -> None:
    """Objective: the request-side registry cannot rot.

    Expected: every name in ``REQUEST_BODY_MODELS`` is a model actually
    defined in the module, so a rename cannot silently move a request body
    onto the response side of the rule.
    """
    defined = set(_models_defined_in_admin_module())
    assert defined >= admin_models.REQUEST_BODY_MODELS, (
        f"registry names non-existent models: {sorted(admin_models.REQUEST_BODY_MODELS - defined)}"
    )


@pytest.mark.parametrize(
    "model_name",
    [
        "ChainAdminDetail",
        "ListUploadsResponse",
        "GroupStatusResponse",
        "IdentifierLookupResponse",
        "BulkDeleteResponse",
        "InstanceStatusResponse",
        "CountersResponse",
        "GaugesResponse",
        "RamPressureStatusResponse",
        "QuarantineInventoryResponse",
        "QuarantineRestoreResponse",
    ],
)
def test_named_response_models_tolerate_a_future_field(model_name: str) -> None:
    """Objective: each response model named in the review tolerates one new field.

    Expected: ``extra='ignore'``, so a service that adds a field does not fault
    an SDK release that predates it. These eleven were the ones actually found
    forbidding extras; the mechanical test above covers the rest.
    """
    model = _models_defined_in_admin_module()[model_name]
    assert model.model_config.get("extra") == "ignore"


def test_group_status_response_parses_an_unknown_field() -> None:
    """Objective: prove the tolerance end to end on the polled model.

    Expected: a rollup carrying a field this SDK has never heard of parses,
    and the known fields are intact. ``poll_group_until_finished`` re-parses
    this model on every iteration, so strictness here broke the poll outright.
    """
    group_id = uuid4()
    rollup = GroupStatusResponse.model_validate_json(
        json.dumps(
            {
                "group_id": str(group_id),
                "total": 1,
                "counts_by_state": {"succeeded": 1},
                "all_finished": True,
                "first_received_at": "2026-01-01T00:00:00+00:00",
                "last_sent_at": "2026-01-01T00:00:01+00:00",
                "members": [
                    {
                        "chain_id": str(uuid4()),
                        "state": "succeeded",
                        "received_at": "2026-01-01T00:00:00+00:00",
                        "sent_at": "2026-01-01T00:00:01+00:00",
                        "attempts": 1,
                        "last_error": None,
                        "send_order": 0,
                        "multifile_id": None,
                        # A field a future service release adds to each member.
                        "delivery_latency_ms": 1234,
                    }
                ],
                # A field a future service release adds to the rollup itself.
                "slowest_member_chain_id": str(uuid4()),
            }
        )
    )
    assert rollup.all_finished is True
    assert rollup.total == 1
    assert rollup.members[0].state == "succeeded"


# ---------------------------------------------------------------------------
# AdminStatusResponse.resolved_defaults (CONTEXT.md pilot admin surface).
# ---------------------------------------------------------------------------


def test_admin_status_surfaces_resolved_defaults() -> None:
    """Objective: the resolved-defaults echo reaches the caller.

    Expected: the nested block parses into
    :class:`ResolvedDefaultsSummary` with its values intact. The SDK omitted
    the field entirely and, being extras-tolerant, discarded the whole block
    with no warning - so an operator reading ``GET /v1/admin/status`` through
    this SDK could not see the caps Phantom was actually running under, which
    CONTEXT.md pins as pilot admin surface.
    """
    status = AdminStatusResponse.model_validate(
        {
            "ready": True,
            "disk_usage_bytes": 0,
            "total_backlog": 0,
            "instances": [],
            "resolved_defaults": {
                "max_in_flight": 64,
                "max_in_flight_bytes": 1_073_741_824,
                "max_disk_bytes": 8_589_934_592,
                "ram_ceiling_bytes": 536_870_912,
                "large_body_threshold_bytes": 8_388_608,
                "max_large_in_flight": 4,
                "persist_body_size_threshold_bytes": 4_194_304,
                "worker_count": 8,
                "observed_total_ram_bytes": 17_179_869_184,
                "observed_free_disk_bytes": 107_374_182_400,
                "observed_cpu_count": 8,
            },
        }
    )
    assert status.resolved_defaults is not None
    assert status.resolved_defaults.max_in_flight == 64
    assert status.resolved_defaults.worker_count == 8
    assert status.resolved_defaults.observed_cpu_count == 8


def test_admin_status_resolved_defaults_optional() -> None:
    """Objective: the field stays optional, matching the service.

    Expected: ``None`` when the response omits the block. The service's own
    field is optional so a status constructed without a probe still parses;
    an SDK that required it would reject a valid response.
    """
    status = AdminStatusResponse.model_validate(
        {"ready": True, "disk_usage_bytes": 0, "total_backlog": 0, "instances": []}
    )
    assert status.resolved_defaults is None


# ---------------------------------------------------------------------------
# DeleteFilter.is_empty: set means "is not None", not truthy.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filter_body",
    [
        DeleteFilter(route=""),
        DeleteFilter(instance=""),
    ],
    ids=["empty-route", "empty-instance"],
)
def test_is_empty_treats_an_empty_string_as_set(filter_body: DeleteFilter) -> None:
    """Objective: an empty-string field is a SET filter, not an empty one.

    Expected: ``is_empty()`` is False. Testing truthiness reported these as
    empty, so ``bulk_delete`` raised ``EmptyFilterError`` locally for a filter
    the service would have accepted and evaluated as "rows whose route is the
    empty string" - matching nothing. The service's own guard refuses only the
    all-None body, so the pre-flight has to draw the line in the same place.
    """
    assert filter_body.is_empty() is False


def test_is_empty_still_true_only_for_all_none() -> None:
    """Objective: the all-None body remains the one refused shape.

    Expected: True for the bare filter, False once any field is set. Widening
    the definition must not stop the pre-flight catching a genuine delete-all.
    """
    assert DeleteFilter().is_empty() is True
    assert DeleteFilter(state="failed").is_empty() is False
