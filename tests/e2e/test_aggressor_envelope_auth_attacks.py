"""Aggressor (Part 5.B) - malformed envelopes, bad routes, and auth attacks.

Drives the public ``POST /v1/send`` surface (and the loopback admin
token-push surface) with hostile envelope and auth shapes, asserting
each is handled cleanly: a definite status code, no crash, no data loss,
and a truthful admin interface afterward.

Envelope shapes (all reject cleanly, the process stays up):

* Malformed JSON bytes -> ``envelope_invalid`` (422).
* A well-formed JSON object that is the WRONG schema (missing required
  fields) -> ``envelope_invalid`` (422).
* An envelope carrying an EXTRA, undeclared top-level field -> rejected
  by the model's ``extra="forbid"`` as ``envelope_invalid`` (422).

Routing shapes:

* A first-step URL whose host matches NO configured instance ->
  ``invalid_target`` (421).
* An explicit ``X-Phantom-Instance`` header naming an unconfigured
  instance -> ``instance_unknown`` (421).

Auth shapes (verified against the live admission path, which is a
transparent buffer, NOT an ingress auth gate - it caches whatever
Authorization arrives and lets the upstream decide):

* An ABSENT Authorization header -> still ADMITTED (202); Phantom
  buffers and forwards with no bearer, and the upstream's own auth
  decision (here a 401) parks the chain in ``auth_expired``.
* An EXPIRED / MALFORMED bearer -> still ADMITTED (202); the upstream
  401s and the chain parks in ``auth_expired``.
* A token that expires mid-flight: a fresh token pushed via
  ``PUT /v1/admin/tokens/{endpoint}/{uid}`` (the SDK's ``push_token``,
  the correct verb) wakes the parked chain and it completes.

Test-tree boundary (§ 5.0): public e2e-light lane, generic shapes and
raw HTTP only.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
TERMINAL_BUDGET_SECONDS: float = 30.0
AUTH_EXPIRED_BUDGET_SECONDS: float = 8.0

pytestmark = pytest.mark.e2e


def _well_formed_envelope_dict(*, emulator_url: str, chain_id: UUID) -> dict[str, object]:
    """A valid one-step JSON envelope as a raw dict (mutated per attack)."""
    return {
        "chain_id": str(chain_id),
        "idempotency_key": str(chain_id),
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": f"{emulator_url}/v2/files",
                "headers": {"Content-Type": "application/json"},
                "body": {
                    "kind": "json",
                    "value": {
                        "domain": "generic",
                        "laneBaseName": "history_parquet_data",
                        "fileName": f"env-{chain_id.hex[:8]}",
                        "metadata": {"keyValueStore": {"uploader_id": "12345"}},
                    },
                },
                "capture": [],
                "idempotency_header": None,
            }
        ],
        "default_target": None,
    }


def _headers(
    *,
    bearer: str | None,
    instance: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, str]:
    """Build ingress headers; omit Authorization entirely when ``bearer`` is None."""
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "X-Phantom-Uid": DEFAULT_SUB,
        "X-Phantom-Idempotency-Key": idempotency_key or str(uuid4()),
    }
    if bearer is not None:
        headers["Authorization"] = f"Bearer {bearer}"
    if instance is not None:
        headers["X-Phantom-Instance"] = instance
    return headers


async def _post_raw(
    client: httpx.AsyncClient, *, phantom_url: str, content: bytes, headers: dict[str, str]
) -> httpx.Response:
    """POST raw bytes to ``/v1/send``."""
    return await client.post(f"{phantom_url}/v1/send", content=content, headers=headers)


async def _assert_admin_alive_and_empty(stack: E2EStack) -> None:
    """The process is up, ready, and holds no row (admin-truthfulness probe)."""
    health = await stack.phantom_client.get_health()
    assert health.status == "ok", "process must stay up after a malformed submission"
    ready = await stack.phantom_client.get_ready()
    assert ready.ready, "instance must stay ready after a malformed submission"
    rows, _ = await stack.phantom_client.list_uploads(limit=500)
    assert len(rows) == 0, f"a rejected submission must not create a row; rows={rows}"


# ---------------------------------------------------------------------------
# Malformed / wrong-schema / extra-field envelopes.
# ---------------------------------------------------------------------------


async def test_malformed_json_is_envelope_invalid(tmp_path: Path) -> None:
    """Truncated / non-JSON bytes are a clean ``envelope_invalid`` (422)."""
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        bearer = stack.fake_security_token()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=b'{"chain_id": "not-closed", "steps": [',  # truncated JSON
                headers=_headers(bearer=bearer),
            )
        assert resp.status_code == 422, (
            f"malformed JSON should be 422 envelope_invalid; got {resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "envelope_invalid"
        await _assert_admin_alive_and_empty(stack)
    finally:
        await stack.tear_down()


async def test_wrong_schema_object_is_envelope_invalid(tmp_path: Path) -> None:
    """A valid JSON object missing required fields is ``envelope_invalid`` (422)."""
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        bearer = stack.fake_security_token()
        # Well-formed JSON, but nothing a ChainEnvelope expects.
        wrong = {"hello": "world", "items": [1, 2, 3], "nested": {"a": True}}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(wrong).encode("utf-8"),
                headers=_headers(bearer=bearer),
            )
        assert resp.status_code == 422, (
            f"wrong-schema object should be 422 envelope_invalid; got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "envelope_invalid"
        await _assert_admin_alive_and_empty(stack)
    finally:
        await stack.tear_down()


async def test_extra_top_level_field_is_rejected_by_forbid(tmp_path: Path) -> None:
    """An undeclared extra top-level field is rejected (``extra="forbid"``).

    ChainEnvelope sets ``extra="forbid"``; a submission that smuggles an
    extra top-level key must be rejected ``envelope_invalid`` (422), not
    silently accepted with the field dropped.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        envelope = _well_formed_envelope_dict(emulator_url=stack.emulator_url, chain_id=chain_id)
        envelope["totally_unexpected_field"] = "smuggled"
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(envelope).encode("utf-8"),
                headers=_headers(bearer=bearer, idempotency_key=str(chain_id)),
            )
        assert resp.status_code == 422, (
            f"an extra top-level field should be 422 envelope_invalid (extra=forbid); got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "envelope_invalid"
        await _assert_admin_alive_and_empty(stack)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Routing: no matching instance.
# ---------------------------------------------------------------------------


async def test_route_matching_no_instance_is_invalid_target(tmp_path: Path) -> None:
    """A first-step host matching no configured instance -> 421 ``invalid_target``."""
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        # A host that no instance's host_prefixes match.
        envelope = _well_formed_envelope_dict(emulator_url=stack.emulator_url, chain_id=chain_id)
        envelope["steps"][0]["url"] = "http://nonexistent.invalid.example/v2/files"  # type: ignore[index]
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(envelope).encode("utf-8"),
                headers=_headers(
                    bearer=bearer,
                    idempotency_key=str(chain_id),
                ),
            )
        assert resp.status_code == 421, (
            f"an unroutable target should be 421 invalid_target; got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "invalid_target"
        await _assert_admin_alive_and_empty(stack)
    finally:
        await stack.tear_down()


async def test_explicit_unknown_instance_header_is_instance_unknown(tmp_path: Path) -> None:
    """An ``X-Phantom-Instance`` naming an unconfigured instance -> 421 ``instance_unknown``."""
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        envelope = _well_formed_envelope_dict(emulator_url=stack.emulator_url, chain_id=chain_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(envelope).encode("utf-8"),
                headers=_headers(
                    bearer=bearer,
                    instance="no-such-instance",
                    idempotency_key=str(chain_id),
                ),
            )
        assert resp.status_code == 421, (
            f"an unknown X-Phantom-Instance should be 421 instance_unknown; got "
            f"{resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "instance_unknown"
        await _assert_admin_alive_and_empty(stack)
    finally:
        await stack.tear_down()


# ---------------------------------------------------------------------------
# Auth: Phantom is a transparent buffer, not an ingress auth gate.
# ---------------------------------------------------------------------------


async def test_absent_authorization_is_admitted_and_parks_auth_expired(tmp_path: Path) -> None:
    """No Authorization header: the upload is still buffered, parks ``auth_expired``.

    Phantom does not reject at ingress for a missing bearer (it is a
    transparent buffer per ADR-001/003): the chain is admitted (202),
    forwarded with no bearer, and the upstream's 401 parks it in
    ``auth_expired`` - durably held, never lost. The admin surface
    reports the parked row.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"retry": {"worker_count": 2, "poll_interval_ms": 100}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # The upstream 401s on every call, so a no-bearer (or any) attempt parks.
        stack.emulator.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, auth_401_after_n_calls=0)  # type: ignore[call-arg]
        )
        chain_id = uuid4()
        envelope = _well_formed_envelope_dict(emulator_url=stack.emulator_url, chain_id=chain_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(envelope).encode("utf-8"),
                headers=_headers(bearer=None, idempotency_key=str(chain_id)),
            )
        assert resp.status_code == 202, (
            f"a no-Authorization submit must still be admitted (transparent buffer); got "
            f"{resp.status_code}: {resp.text}"
        )

        # The chain is durably buffered and parks in auth_expired (the
        # upstream 401 with no usable token).
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="auth_expired",
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        )
        assert detail.state == "auth_expired"

        # Admin truthfulness: the parked row shows up in stats.
        stats = await stack.phantom_client.get_stats(instance="primary")
        assert stats.auth.auth_expired_count >= 1, (
            f"parked row should be counted in auth_expired_count; stats.auth={stats.auth}"
        )
        assert stats.parked_total >= 1, f"parked_total should count the parked row; {stats}"
    finally:
        await stack.tear_down()


async def test_malformed_bearer_is_admitted_and_parks_auth_expired(tmp_path: Path) -> None:
    """A malformed bearer is buffered, forwarded, and parks ``auth_expired``.

    A junk bearer ("Bearer not-a-jwt") is not rejected at ingress; the
    upstream rejects it (401) and the chain parks in ``auth_expired``,
    durably held for a later good token.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"retry": {"worker_count": 2, "poll_interval_ms": 100}},
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        stack.emulator.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, auth_401_after_n_calls=0)  # type: ignore[call-arg]
        )
        chain_id = uuid4()
        envelope = _well_formed_envelope_dict(emulator_url=stack.emulator_url, chain_id=chain_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _post_raw(
                client,
                phantom_url=stack.phantom_url,
                content=json.dumps(envelope).encode("utf-8"),
                headers=_headers(
                    bearer="not-a-jwt-at-all",
                    idempotency_key=str(chain_id),
                ),
            )
        assert resp.status_code == 202, (
            f"a malformed bearer must still be admitted (the upstream decides auth); got "
            f"{resp.status_code}: {resp.text}"
        )
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="auth_expired",
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        )
        assert detail.state == "auth_expired"
    finally:
        await stack.tear_down()


async def test_token_expires_midflight_then_fresh_push_resumes(tmp_path: Path) -> None:
    """A token expiring mid-flight then a fresh one pushed via PUT resumes the chain.

    Models a token whose validity lapses while the upload is buffered:
    the upstream 401s and the chain parks in ``auth_expired``. The
    operator pushes a fresh token via ``PUT /v1/admin/tokens/{endpoint}/{uid}``
    (the SDK ``push_token``, the correct verb), the bearer kicker re-queues
    the parked chain, and it reaches ``succeeded``. The buffered upload
    was never lost across the credential gap.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"retry": {"worker_count": 2, "poll_interval_ms": 100}},
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        # The stale token 401s until a fresh one is pushed.
        stack.emulator.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, auth_401_after_n_calls=0)  # type: ignore[call-arg]
        )

        stale = stack.fake_security_token(extra_claims={"token_version": "stale"})
        chain_id = uuid4()
        req = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
        req.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
        envelope, _ = build_in_memory_upload_envelope(
            request=req,
            files_api_base=stack.emulator_url,
            local_uuid=chain_id,
        )
        await pc.submit_chain(
            envelope,
            body_refs={"body": b"phantom-token-midflight-body"},
            uid=DEFAULT_SUB,
            auth_token=f"Bearer {stale}",
        )

        async def _parked() -> bool:
            d = await pc.get_upload(chain_id)
            return d.state == "auth_expired"

        await await_until(
            _parked,
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
            message=f"chain {chain_id} never parked in auth_expired",
        )

        # Operator lifts the 401 and pushes a fresh token via the correct
        # verb (PUT /v1/admin/tokens/{endpoint}/{uid}).
        stack.emulator.clear_failures()
        fresh = stack.fake_security_token(extra_claims={"token_version": "fresh"})
        endpoint = urlparse(stack.emulator_url).hostname or ""
        await pc.push_token(endpoint=endpoint, uid=DEFAULT_SUB, token=fresh)

        detail = await assert_chain_reaches_state(
            pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        assert detail.state == "succeeded"

        # Admin truthfulness: the parked count has drained back to zero.
        stats = await pc.get_stats(instance="primary")
        assert stats.auth.auth_expired_count == 0, (
            f"auth_expired_count should drain after the fresh-token resume; {stats.auth}"
        )
    finally:
        await stack.tear_down()
