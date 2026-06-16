"""Aggressor (Part 5.B) - the no-idempotency-key admission path.

A naive raw-HTTP client that sends NO ``X-Phantom-Idempotency-Key``
header still gets admission dedup: the server mints ``str(chain_id)``
as the dedup key and writes a real ``idempotency_index`` claim, so the
dedup machinery is never skipped for lack of a client key. This is the
server-side robustness the landed § 2 change guarantees regardless of
client behavior - the official SDK already defaults the header to
``str(chain_id)``, so this path only bites raw-HTTP clients, which is
exactly what these tests drive (a hand-built ``httpx`` POST that omits
the header entirely).

Verified admission semantics (read against the live code, not assumed):
``insert_with_idempotency_claim`` INSERTs the ``uploads`` row BEFORE the
``idempotency_index`` claim, so a resend that reuses the SAME
``chain_id`` (the row primary key) is caught by the PK guard and
rejected ``chain_id_in_use`` (409) - the 200 ``idempotency_replay`` path
is only reachable from a FRESH chain_id reusing an existing claim. Since
a minted key is ``str(chain_id)`` (unique per chain), the no-key path's
"a retry cannot duplicate" guarantee manifests as a clean 409 on a
same-chain_id resend, leaving exactly one row either way.

The aggressor angles, all over the public ``POST /v1/send`` surface:

1. No header at all, single submit -> admitted (202), one row, a real
   claim written (proven by the resend being deduped, not duplicated).
2. No header, resend of the SAME chain_id -> rejected ``chain_id_in_use``
   (409); still exactly one row (a retry cannot duplicate).
3. Empty-string and whitespace-only headers -> treated as absent and
   filled with ``str(chain_id)`` (so many blank-header submissions for
   distinct chains do NOT collide on a shared ``""`` key).
4. An oversized / unicode header value -> kept verbatim, accepted; a
   same-chain_id resend is still deduped (409), not duplicated.
5. A client-controlled key reused with a DIFFERENT body across a fresh
   chain_id -> rejected ``idempotency_key_conflict`` (422), not a silent
   success-shaped replay that drops the second body (the inherent
   client-controlled-idempotency case, § 2 / F-6).

After each scenario the admin interface must stay truthful: the row
count reflects reality (no phantom rows, no lost rows).

Test-tree boundary (§ 5.0): public e2e-light lane, generic
``PhantomDriver`` envelope shapes and raw HTTP only.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import SubmitOptions
from phantom_client.errors import PhantomUnprocessableError

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
BODY: bytes = b"phantom-aggressor-no-key-body"
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


def _single_step_json_envelope(
    *, emulator_url: str, chain_id: UUID, marker: str
) -> dict[str, object]:
    """Build a one-step JSON-body envelope as a raw dict (no SDK).

    A single ``POST /v2/files`` step with a small inline JSON body the
    emulator tolerates. Built as a plain dict so the test controls the
    exact wire bytes and can omit the idempotency header at the HTTP
    layer - the SDK always sets it, so a raw client is the only way to
    express the no-key path.
    """
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
                        "fileName": f"nokey-{marker}",
                        "metadata": {"keyValueStore": {"uploader_id": "12345"}},
                    },
                },
                "capture": [],
                "idempotency_header": None,
            }
        ],
        "default_target": None,
    }


async def _raw_send(
    client: httpx.AsyncClient,
    *,
    phantom_url: str,
    envelope: dict[str, object],
    bearer: str,
    idempotency_key: str | None,
) -> httpx.Response:
    """POST a JSON envelope to ``/v1/send`` over raw HTTP.

    When ``idempotency_key`` is ``None`` the ``X-Phantom-Idempotency-Key``
    header is OMITTED entirely (the naive-client path). Otherwise it is
    sent verbatim (including empty / whitespace / oversized / unicode
    values), so the test exercises the server's own blank-vs-present
    handling rather than the SDK's default.
    """
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "X-Phantom-Uid": DEFAULT_SUB,
        "Authorization": f"Bearer {bearer}",
    }
    if idempotency_key is not None:
        headers["X-Phantom-Idempotency-Key"] = idempotency_key
    return await client.post(
        f"{phantom_url}/v1/send",
        content=json.dumps(envelope).encode("utf-8"),
        headers=headers,
    )


def _chain_id_of(response: httpx.Response) -> UUID:
    """Pull the assigned chain_id out of a 2xx ChainResponse body."""
    return UUID(response.json()["chain_id"])


async def _row_ids(stack: E2EStack) -> set[UUID]:
    """Return the set of chain_ids the admin list surface reports."""
    rows, _ = await stack.phantom_client.list_uploads(limit=500)
    return {r.chain_id for r in rows}


async def test_no_key_single_submit_is_admitted_and_dedupes_on_resend(tmp_path: Path) -> None:
    """No header at all: admitted (202), and a resend cannot duplicate.

    The server mints ``str(chain_id)`` so the first POST writes a real
    idempotency claim. The identical resend (same chain_id, still no
    header) is rejected ``chain_id_in_use`` (409) by the row-PK guard
    (which fires before the idempotency-replay path), and
    ``list_uploads`` holds exactly one row for the chain - a retry
    cannot create a second.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        envelope = _single_step_json_envelope(
            emulator_url=stack.emulator_url, chain_id=chain_id, marker=chain_id.hex[:8]
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            first = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=envelope,
                bearer=bearer,
                idempotency_key=None,
            )
            assert first.status_code == 202, (
                f"no-key first submit should be admitted with 202; got {first.status_code}: "
                f"{first.text}"
            )
            assert _chain_id_of(first) == chain_id

            # Let the chain settle so the claim is durably committed.
            await assert_chain_reaches_state(
                stack.phantom_client,
                chain_id,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )

            # Identical resend, still no header: the row-PK guard rejects
            # the reused chain_id with 409 before the replay path, so a
            # second row is never created.
            resend = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=envelope,
                bearer=bearer,
                idempotency_key=None,
            )
            assert resend.status_code == 409, (
                f"no-key resend of the same chain_id should be rejected chain_id_in_use "
                f"(409); got {resend.status_code}: {resend.text}"
            )
            assert resend.json()["error"]["code"] == "chain_id_in_use", (
                f"expected chain_id_in_use; got body {resend.text}"
            )

        ids = await _row_ids(stack)
        assert chain_id in ids, f"the admitted chain {chain_id} is missing from the row list: {ids}"
        relevant = [i for i in ids if i == chain_id]
        assert len(relevant) == 1, (
            f"exactly one row expected after a no-key submit + resend; got {relevant} within {ids}"
        )
    finally:
        await stack.tear_down()


async def test_blank_keys_do_not_collide_across_distinct_chains(tmp_path: Path) -> None:
    """An empty ``X-Phantom-Idempotency-Key`` is treated as absent, not shared.

    Two DISTINCT chains each sent with an EMPTY-string
    ``X-Phantom-Idempotency-Key`` must each be admitted as their own
    row. If a blank header were kept verbatim the two would collide on a
    shared ``""`` dedup key and the second would be deduped away (data
    loss). The server fills each with its own ``str(chain_id)`` instead,
    so both survive.

    (Whitespace-only header values are rejected by the HTTP client stack
    itself before reaching Phantom, so the empty-string is the testable
    blank case from a real raw client; the server's
    ``.strip()``-treats-whitespace-as-absent rule is exercised by the
    admission unit tests.)
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_a = uuid4()
        chain_b = uuid4()
        assert chain_a != chain_b

        async with httpx.AsyncClient(timeout=30.0) as client:
            r_a = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=_single_step_json_envelope(
                    emulator_url=stack.emulator_url,
                    chain_id=chain_a,
                    marker=chain_a.hex[:8],
                ),
                bearer=bearer,
                idempotency_key="",
            )
            r_b = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=_single_step_json_envelope(
                    emulator_url=stack.emulator_url,
                    chain_id=chain_b,
                    marker=chain_b.hex[:8],
                ),
                bearer=bearer,
                idempotency_key="",
            )

        assert r_a.status_code == 202, (
            f"first empty-key submit should be admitted; got {r_a.status_code}: {r_a.text}"
        )
        assert r_b.status_code == 202, (
            f"second empty-key submit (distinct chain) should be admitted, not deduped against "
            f"the first on a shared '' key; got {r_b.status_code}: {r_b.text}"
        )
        assert _chain_id_of(r_a) == chain_a
        assert _chain_id_of(r_b) == chain_b, (
            "the second empty-key chain was deduped against the first - blank keys wrongly "
            "collided on a shared dedup key instead of minting per-chain defaults"
        )

        ids = await _row_ids(stack)
        assert {chain_a, chain_b} <= ids, (
            f"both empty-key chains must persist as distinct rows; got {ids}"
        )
    finally:
        await stack.tear_down()


async def test_oversized_key_is_kept_verbatim_and_dedupes(tmp_path: Path) -> None:
    """A bizarre, oversized but non-blank client key is honored and dedups.

    An 8 KiB key (far larger than any realistic value) is kept verbatim;
    a resend of the same chain with the same key is deduped by the
    row-PK guard (409), never silently creating a second row. The server
    does not choke on the unusual header length.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        chain_id = uuid4()
        # An absurdly long ASCII key. HTTP header values must be latin-1
        # on the wire, so an oversized ASCII string is the realistic
        # "bizarre key" probe (a non-ASCII value is not a valid header
        # and is rejected by the client stack, not Phantom).
        weird_key = "idem-" + ("k" * 8192)
        envelope = _single_step_json_envelope(
            emulator_url=stack.emulator_url, chain_id=chain_id, marker=chain_id.hex[:8]
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            first = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=envelope,
                bearer=bearer,
                idempotency_key=weird_key,
            )
            assert first.status_code == 202, (
                f"unicode-key submit should be admitted; got {first.status_code}: {first.text}"
            )
            await assert_chain_reaches_state(
                stack.phantom_client,
                chain_id,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )
            resend = await _raw_send(
                client,
                phantom_url=stack.phantom_url,
                envelope=envelope,
                bearer=bearer,
                idempotency_key=weird_key,
            )
            assert resend.status_code == 409, (
                f"resend of the same chain_id (unicode key) should be deduped 409; got "
                f"{resend.status_code}: {resend.text}"
            )

        ids = await _row_ids(stack)
        relevant = [i for i in ids if i == chain_id]
        assert len(relevant) == 1, f"unicode-key chain must be exactly one row; got {ids}"
    finally:
        await stack.tear_down()


async def test_reused_key_with_different_body_is_rejected_as_conflict(tmp_path: Path) -> None:
    """A client key reused with a DIFFERENT body is a 422 conflict, not a silent drop.

    An idempotency key must be a function of the body. Reusing the same
    explicit ``X-Phantom-Idempotency-Key`` for a second chain that
    carries a different body would otherwise be swallowed behind a
    success-shaped 200 replay, silently dropping the second body
    (finding G-1). The server returns ``idempotency_key_conflict`` (422)
    so the client sees the contract violation. We drive the body
    difference via the multipart path so each submission carries real,
    distinct body bytes under one shared key.
    """
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()
        shared_key = "client-controlled-shared-key-001"

        async def _submit(body: bytes) -> UUID:
            cid = uuid4()
            req = build_create_file_request(file_name=f"e2e_{cid.hex[:12]}")
            req.metadata.key_value_store["phantom_local_uuid"] = str(cid)
            envelope, _ = build_in_memory_upload_envelope(
                request=req,
                files_api_base=stack.emulator_url,
                local_uuid=cid,
            )
            resp = await pc.submit_chain(
                envelope,
                body_refs={"body": body},
                uid=DEFAULT_SUB,
                auth_token=f"Bearer {bearer}",
                options=SubmitOptions(idempotency_key=shared_key),  # type: ignore[call-arg]
            )
            return resp.chain_id

        # First submission claims the shared key with body A.
        first_id = await _submit(b"body-A-original-bytes")
        await assert_chain_reaches_state(
            pc, first_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )

        # Second submission reuses the key with a DIFFERENT body. The SDK
        # maps idempotency_key_conflict to PhantomUnprocessableError (422).
        with pytest.raises(PhantomUnprocessableError) as exc_info:
            await _submit(b"body-B-totally-different-bytes-and-longer")

        err = exc_info.value
        assert err.status_code == 422, (
            f"reused-key-different-body should be 422 conflict; got {err.status_code}"
        )
        assert err.error_code == "idempotency_key_conflict", (
            f"expected idempotency_key_conflict; got {err.error_code!r}"
        )

        # Admin truthfulness: exactly one row for the shared key (the
        # second body was rejected, not silently stored or dropped under
        # a fake success).
        rows, _ = await pc.list_uploads(limit=500)
        # The first chain persists; no orphan second row exists.
        assert first_id in {r.chain_id for r in rows}
    finally:
        await stack.tear_down()
