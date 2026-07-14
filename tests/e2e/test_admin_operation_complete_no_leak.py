"""Operation-complete admin sweep with a bearer no-leak scanner (audit T8 / G13).

The existing ``src/phantom-service/tests/unit/test_admin_no_bearer.py`` sweeps
GET routes on a synthesized app and skips any route whose parameters it cannot
build. This module closes that gap on a real socket:

1. A typed manifest keyed by ``(method, path_template, scenario_id)`` samples
   EVERY admin operation, including PUT/DELETE/POST and the streaming, bundle,
   and tar surfaces. The manifest's operation projection is asserted equal to
   the live OpenAPI admin path+method set, so a newly added admin operation
   cannot go unsampled: it fails this test until a scenario is added.
2. A cached bearer sentinel is pushed into the token cache and its residence
   proven before the sweep. Every sampled response, its status phrase, all
   header values, and its full decoded body (JSON, text, raw stream, and tar
   member names plus contents) are scanned for the sentinel. Zero occurrences
   are required across all operations.
3. The scanner is self-proven: a synthetic payload carrying the sentinel must
   be detected, so a scanner that silently never matches cannot pass.
"""

from __future__ import annotations

import io
import secrets
import tarfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from phantom.app import create_app
from phantom.config.settings import Settings

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

DEFAULT_SUB = "00000000-0000-0000-0000-000000000001"
TERMINAL_BUDGET_SECONDS = 15.0
_ADMIN_PREFIX = "/v1/admin"
# Endpoint/uid the sentinel bearer is cached under. The endpoint is a
# synthetic upstream hostname; the sweep proves no admin response echoes the
# cached value regardless of which slot holds it.
_TOKEN_ENDPOINT = "files.upstream.example"
_TOKEN_UID = "t8-sentinel-uid"


@dataclass(frozen=True)
class Scenario:
    """One sampled admin operation and its expected behavior.

    ``path_template`` is the OpenAPI-shaped path (with ``{param}`` segments)
    used for the operation-completeness projection; ``path`` is the concrete
    request path. ``expected_status`` is a set so a small number of
    state-sensitive operations can declare an allowed pair with a comment.
    """

    scenario_id: str
    method: str
    path_template: str
    path: str
    expected_status: frozenset[int]
    json_body: dict[str, object] | None = None
    query: dict[str, str] = field(default_factory=dict)


def _live_admin_operations(settings: Settings) -> set[tuple[str, str]]:
    """Return the live OpenAPI ``(METHOD, path_template)`` admin operation set."""
    app = create_app(settings)
    spec = app.openapi()
    operations: set[tuple[str, str]] = set()
    for path, methods in spec["paths"].items():
        if path.startswith(_ADMIN_PREFIX):
            for method in methods:
                operations.add((method.upper(), path))
    return operations


async def _submit_and_succeed(stack: E2EStack, bearer: str) -> UUID:
    """Submit one chain through the SDK and wait for terminal success."""
    pc = stack.phantom_client
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"t8_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": secrets.token_bytes(4096)},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )
    await assert_chain_reaches_state(
        pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
    )
    return chain_id


def _looks_like_tar(body: bytes) -> bool:
    """True if ``body`` carries the POSIX tar magic at its fixed offset."""
    # The ustar magic sits at byte offset 257 of the first 512-byte header.
    return len(body) >= 265 and body[257:262] == b"ustar"


def _scan_targets(response: httpx.Response) -> Iterable[bytes]:
    """Yield every byte surface of a response that could carry a leak."""
    yield response.reason_phrase.encode("utf-8", "replace")
    for name, value in response.headers.items():
        yield name.encode("utf-8", "replace")
        yield value.encode("utf-8", "replace")
    body = response.content
    yield body
    # If the body is a tar archive, also scan member names and contents;
    # a leak could hide inside an exported manifest rather than the framing.
    # Detected by magic bytes, not URL, so synthetic responses scan too.
    if _looks_like_tar(body):
        with tarfile.open(fileobj=io.BytesIO(body)) as archive:
            for member in archive.getmembers():
                yield member.name.encode("utf-8", "replace")
                if member.isfile():
                    extracted = archive.extractfile(member)
                    if extracted is not None:
                        yield extracted.read()


def _contains_sentinel(response: httpx.Response, sentinel: bytes) -> bool:
    """True if the sentinel appears anywhere in the response's byte surfaces."""
    return any(sentinel in chunk for chunk in _scan_targets(response))


def _build_manifest(
    *,
    chain_ok: UUID,
    chain_delete: UUID,
    group_id: UUID,
    captured_id: str,
    local_uuid: UUID,
    sentinel: str,
) -> list[Scenario]:
    """Build one scenario per live admin operation, in a safe execution order."""
    ok = str(chain_ok)
    return [
        # --- Read surfaces (no state mutation) ---
        Scenario(
            "status", "GET", f"{_ADMIN_PREFIX}/status", f"{_ADMIN_PREFIX}/status", frozenset({200})
        ),
        Scenario(
            "stats", "GET", f"{_ADMIN_PREFIX}/stats", f"{_ADMIN_PREFIX}/stats", frozenset({200})
        ),
        Scenario(
            "instances",
            "GET",
            f"{_ADMIN_PREFIX}/instances",
            f"{_ADMIN_PREFIX}/instances",
            frozenset({200}),
        ),
        Scenario(
            "instance_status",
            "GET",
            f"{_ADMIN_PREFIX}/instances/{{instance_id}}/status",
            f"{_ADMIN_PREFIX}/instances/primary/status",
            frozenset({200}),
        ),
        Scenario(
            "counters",
            "GET",
            f"{_ADMIN_PREFIX}/observability/counters",
            f"{_ADMIN_PREFIX}/observability/counters",
            frozenset({200}),
        ),
        Scenario(
            "gauges",
            "GET",
            f"{_ADMIN_PREFIX}/observability/gauges",
            f"{_ADMIN_PREFIX}/observability/gauges",
            frozenset({200}),
        ),
        Scenario(
            "ram_pressure",
            "GET",
            f"{_ADMIN_PREFIX}/observability/ram_pressure",
            f"{_ADMIN_PREFIX}/observability/ram_pressure",
            frozenset({200}),
        ),
        Scenario(
            "chain_list_paginated",
            "GET",
            f"{_ADMIN_PREFIX}/chains",
            f"{_ADMIN_PREFIX}/chains",
            frozenset({200}),
            query={"limit": "50"},
        ),
        Scenario(
            "chain_detail_state_matrix",
            "GET",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}",
            f"{_ADMIN_PREFIX}/chains/{ok}",
            frozenset({200}),
        ),
        Scenario(
            "chain_not_found",
            "GET",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}",
            f"{_ADMIN_PREFIX}/chains/{uuid4()}",
            frozenset({404}),
        ),
        Scenario(
            "body_present",
            "GET",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}/body",
            f"{_ADMIN_PREFIX}/chains/{ok}/body",
            frozenset({200}),
        ),
        Scenario(
            "extract_bundle_mixed",
            "GET",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}/bundle",
            f"{_ADMIN_PREFIX}/chains/{ok}/bundle",
            frozenset({200}),
        ),
        Scenario(
            "group_rollup",
            "GET",
            f"{_ADMIN_PREFIX}/groups/{{group_id}}",
            f"{_ADMIN_PREFIX}/groups/{group_id}",
            frozenset({200}),
        ),
        Scenario(
            "lookup_by_captured_id",
            "GET",
            f"{_ADMIN_PREFIX}/uploads/by-captured-id/{{captured_id}}",
            f"{_ADMIN_PREFIX}/uploads/by-captured-id/{captured_id}",
            frozenset({200}),
        ),
        Scenario(
            "lookup_by_local_uuid",
            "GET",
            f"{_ADMIN_PREFIX}/uploads/by-local-uuid/{{local_uuid}}",
            f"{_ADMIN_PREFIX}/uploads/by-local-uuid/{local_uuid}",
            frozenset({200}),
        ),
        Scenario(
            "quarantine_inventory",
            "GET",
            f"{_ADMIN_PREFIX}/quarantine",
            f"{_ADMIN_PREFIX}/quarantine",
            frozenset({200}),
        ),
        Scenario(
            "token_list",
            "GET",
            f"{_ADMIN_PREFIX}/tokens",
            f"{_ADMIN_PREFIX}/tokens",
            frozenset({200}),
        ),
        Scenario(
            "export_tar",
            "GET",
            f"{_ADMIN_PREFIX}/export.tar",
            f"{_ADMIN_PREFIX}/export.tar",
            frozenset({200}),
        ),
        Scenario(
            "extract_stream",
            "POST",
            f"{_ADMIN_PREFIX}/chains/extract",
            f"{_ADMIN_PREFIX}/chains/extract",
            frozenset({200}),
            json_body={"state": "succeeded"},
        ),
        # --- Token mutations (push the sentinel value, then read/delete) ---
        Scenario(
            "token_put_one",
            "PUT",
            f"{_ADMIN_PREFIX}/tokens/{{endpoint}}/{{uid}}",
            f"{_ADMIN_PREFIX}/tokens/{_TOKEN_ENDPOINT}/{_TOKEN_UID}",
            frozenset({204}),
            json_body={"token": sentinel},
        ),
        Scenario(
            "token_put_endpoint",
            "PUT",
            f"{_ADMIN_PREFIX}/tokens/{{endpoint}}",
            f"{_ADMIN_PREFIX}/tokens/{_TOKEN_ENDPOINT}",
            frozenset({204}),
            json_body={"token": sentinel},
        ),
        Scenario(
            "token_put_global",
            "PUT",
            f"{_ADMIN_PREFIX}/tokens",
            f"{_ADMIN_PREFIX}/tokens",
            frozenset({204}),
            json_body={"token": sentinel},
        ),
        Scenario(
            "credential_put_bad",
            "PUT",
            f"{_ADMIN_PREFIX}/credentials/{{dest_host}}",
            f"{_ADMIN_PREFIX}/credentials/s3.amazonaws.com",
            # Malformed credential body: well-formed JSON, invalid discriminator.
            frozenset({422}),
            json_body={"kind": "not_a_real_credential_kind"},
        ),
        # --- Conflict / not-found surfaces ---
        Scenario(
            "bulk_delete_filtered_empty_rejected",
            "DELETE",
            f"{_ADMIN_PREFIX}/chains",
            f"{_ADMIN_PREFIX}/chains",
            frozenset({422}),
            json_body={},
        ),
        Scenario(
            "restore_unknown_backup_conflict",
            "POST",
            f"{_ADMIN_PREFIX}/quarantine/restore",
            f"{_ADMIN_PREFIX}/quarantine/restore",
            # Unknown backup id: never a silent success.
            frozenset({404, 409}),
            query={"backup_id": str(uuid4())},
        ),
        Scenario(
            "reload_success",
            "POST",
            f"{_ADMIN_PREFIX}/reload",
            f"{_ADMIN_PREFIX}/reload",
            frozenset({200}),
        ),
        # State-sensitive: a terminal chain either re-queues or conflicts;
        # both are declared, and the sentinel scan runs either way.
        Scenario(
            "cancel_terminal",
            "POST",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}/cancel",
            f"{_ADMIN_PREFIX}/chains/{ok}/cancel",
            frozenset({200, 409}),
        ),
        Scenario(
            "replay_terminal",
            "POST",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}/replay",
            f"{_ADMIN_PREFIX}/chains/{ok}/replay",
            frozenset({200, 409}),
        ),
        # --- Destructive deletes, run last ---
        Scenario(
            "chain_delete",
            "DELETE",
            f"{_ADMIN_PREFIX}/chains/{{chain_id}}",
            f"{_ADMIN_PREFIX}/chains/{chain_delete}",
            frozenset({204}),
        ),
        Scenario(
            "token_delete_one",
            "DELETE",
            f"{_ADMIN_PREFIX}/tokens/{{endpoint}}/{{uid}}",
            f"{_ADMIN_PREFIX}/tokens/{_TOKEN_ENDPOINT}/{_TOKEN_UID}",
            frozenset({204}),
        ),
        Scenario(
            "token_delete_all",
            "DELETE",
            f"{_ADMIN_PREFIX}/tokens",
            f"{_ADMIN_PREFIX}/tokens",
            frozenset({204}),
        ),
    ]


async def test_admin_operation_complete_and_no_bearer_leak(tmp_path: Path) -> None:
    """Every admin operation is sampled and no response echoes the cached bearer."""
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        enable_hot_reload=True,
        config_overrides={
            "storage": {"body_store": {"mode": "all_disk"}},
            "retention": {
                "succeeded_metadata_seconds": 300,
                "succeeded_body_seconds": 300,
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        sentinel = f"t8-bearer-sentinel-{secrets.token_hex(16)}"
        sentinel_bytes = sentinel.encode("utf-8")

        # Self-prove the scanner: a payload carrying the sentinel must be seen.
        control = httpx.Response(
            200, headers={"content-type": "text/plain"}, content=sentinel_bytes
        )
        assert _contains_sentinel(control, sentinel_bytes), (
            "scanner failed to detect a sentinel it was handed; the sweep would be vacuous"
        )
        clean_control = httpx.Response(200, content=b"nothing to see here")
        assert not _contains_sentinel(clean_control, sentinel_bytes), (
            "scanner reported a false positive on clean content"
        )

        # Deliver a chain and mine the identifiers the read scenarios need.
        chain_ok = await _submit_and_succeed(stack, bearer)
        chain_delete = await _submit_and_succeed(stack, bearer)
        detail = await stack.phantom_client.get_upload(chain_ok)
        group_id = detail.group_id
        captured_id: str | None = None
        for step in detail.captured:
            file_info = step.values.get("file_information")
            if isinstance(file_info, dict) and "id" in file_info:
                captured_id = str(file_info["id"])
                break
        assert captured_id is not None, "chain_ok did not capture an upstream file id"

        async with httpx.AsyncClient(base_url=stack.phantom_admin_url, timeout=15.0) as http:
            # Cache the sentinel bearer and prove its residence before the sweep.
            put = await http.put(
                f"{_ADMIN_PREFIX}/tokens/{_TOKEN_ENDPOINT}/{_TOKEN_UID}",
                json={"token": sentinel},
            )
            assert put.status_code == 204
            listing = await http.get(f"{_ADMIN_PREFIX}/tokens")
            assert listing.status_code == 200
            slots = listing.json()["tokens"]
            assert any(
                slot["endpoint"] == _TOKEN_ENDPOINT and slot["uid"] == _TOKEN_UID for slot in slots
            ), "sentinel token slot is not resident in the cache before the sweep"
            # The residence proof itself must not have leaked the value.
            assert not _contains_sentinel(listing, sentinel_bytes), (
                "token list leaked the cached bearer value"
            )

            manifest = _build_manifest(
                chain_ok=chain_ok,
                chain_delete=chain_delete,
                group_id=group_id,
                captured_id=captured_id,
                local_uuid=chain_ok,
                sentinel=sentinel,
            )

            # Operation-completeness: the manifest samples exactly the live
            # admin operation set, so a new admin route cannot go unsampled.
            sampled = {(s.method, s.path_template) for s in manifest}
            live = _live_admin_operations(stack.settings)
            assert sampled == live, (
                "admin operation projection drift.\n"
                f"unsampled live operations: {sorted(live - sampled)}\n"
                f"manifest operations not live: {sorted(sampled - live)}"
            )

            # Run every scenario, asserting its declared status and scanning
            # every byte surface for the cached bearer.
            leaks: list[str] = []
            status_mismatches: list[str] = []
            for scenario in manifest:
                response = await http.request(
                    scenario.method,
                    scenario.path,
                    json=scenario.json_body,
                    params=scenario.query or None,
                )
                if response.status_code not in scenario.expected_status:
                    status_mismatches.append(
                        f"{scenario.scenario_id} ({scenario.method} {scenario.path_template}): "
                        f"got {response.status_code}, expected {sorted(scenario.expected_status)}"
                    )
                if _contains_sentinel(response, sentinel_bytes):
                    leaks.append(
                        f"{scenario.scenario_id} ({scenario.method} {scenario.path_template})"
                    )
            assert not status_mismatches, "admin operation status mismatches:\n" + "\n".join(
                status_mismatches
            )
            assert not leaks, (
                "cached bearer leaked in admin responses (values suppressed): " + ", ".join(leaks)
            )
    finally:
        await stack.tear_down()
