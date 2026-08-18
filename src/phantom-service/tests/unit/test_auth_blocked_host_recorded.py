"""F6 via D2 — the park records the credential that actually failed.

The executor keys auth on the CURRENT step's host; the kickers keyed their wake
probe on ``row.endpoint``, the FIRST step's host, captured once at admission.
On a multi-host chain those are different strings, so a fresh token for step
1's host re-queued a row whose blocker was step 2's host, forever, at the
kickers' 1 Hz rescan.

D2 adds ``uploads.auth_blocked_host``: the sender records the host whose
credential slot rejected the row, and both kickers key their freshness probe
AND their ``auth_mode`` partition on it.

The cases below cover the whole rule:

* the executor reports the host it authenticated against, on both auth arms;
* the sender's park writes it, and a transition back through the shared writer
  clears it, while the three CAS exits leave it as inert history;
* both kickers probe the recorded host rather than the endpoint, wake on a
  fresh credential for that host, and fall back to the endpoint on NULL;
* a cross-route chain is owned by the kicker that can actually wake it;
* the recorded value is a parsed hostname or the fixed ``<no-host>`` token,
  never raw producer input, on both arms;
* the derived boot-gate column set carries the new column.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor, FailedAuth
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.credential import HostCredKey, SigningService, SigV4StaticCreds
from phantom.models.upload import UploadRow
from phantom.routing import resolve_route
from phantom.runtime.startup_checks import EXPECTED_UPLOADS_COLUMNS
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.kicker import AWS_SIGV4_FLAVOUR, PHANTOM_BEARER_FLAVOUR, Kicker
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started

# The two hosts a multi-host chain addresses. ``FIRST_HOST`` is what admission
# pins on the row as ``endpoint``; ``BLOCKED_HOST`` is what step 2 actually
# authenticates against, and therefore what the row is waiting on.
FIRST_HOST: str = "files.example.com"
BLOCKED_HOST: str = "objects.example.com"

# The sanitised placeholder the executor records for a step URL with no
# parseable host. Duplicated from ``chain/executor.py`` deliberately: the test
# pins the literal an operator sees, so importing the constant would let a
# rename pass silently.
NO_HOST_TOKEN: str = "<no-host>"

# A hostless step URL carrying credential material in its query string. This is
# the shape the sanitisation rule exists for: ``host_key_for`` returns the WHOLE
# INPUT when urlparse finds no host, and the recorded value is persisted and
# surfaced on four admin paths.
HOSTLESS_STEP_URL: str = "/v1/files/x?sig=SECRET"

UID: str = "user-1"


def _sigv4_creds() -> SigV4StaticCreds:
    """A static SigV4 key-pair for the credential store."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="secret",
        region="us-east-1",
        service=SigningService.S3,
    )


class _FakeUpstream:
    """An upstream that 200s everything, so no test depends on the network."""

    async def start(self) -> None:
        """No-op lifecycle hook."""

    async def stop(self) -> None:
        """No-op lifecycle hook."""

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        """Return a bare 2xx with an empty JSON body."""
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(
    tmp_path: Path,
    *,
    routes: list[RouteCfg] | None = None,
    with_signer_creds: bool = False,
) -> tuple[InstanceContext, SqliteCredentialStore | None]:
    """Build a started :class:`InstanceContext` with a real store and executor.

    Args:
        tmp_path: The test's temporary directory, for the SQLite files.
        routes: The instance's route block. Defaults to one ``phantom_bearer``
            route covering both hosts, which is the multi-host shape F6 is
            about.
        with_signer_creds: When True, attach a started credential store, which
            the ``aws_sigv4`` arm and the sigv4 kicker need.

    Returns:
        ``(instance, signer_creds_or_None)``.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()

    signer_creds: SqliteCredentialStore | None = None
    if with_signer_creds:
        signer_creds = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
        await signer_creds.start()
        track_started(signer_creds)

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=routes
        or [
            RouteCfg(
                name="both-hosts",
                hosts=[FIRST_HOST, BLOCKED_HOST],
                auth_mode="phantom_bearer",
            )
        ],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
        signer_creds=signer_creds,
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=SaturationGate(
            max_in_flight=100,
            max_in_flight_bytes=10_000_000,
            max_disk_bytes=1_000_000_000,
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
        signer_creds=signer_creds,
    )
    return track_instance(instance), signer_creds


def _two_host_envelope_json(chain_id: UUID, *, second_step_url: str) -> str:
    """Serialize a two-step envelope whose steps target two different hosts.

    Args:
        chain_id: The chain's identity.
        second_step_url: Step 2's URL, which is the one the executor
            authenticates against when ``current_step_index`` is 1.

    Returns:
        The persisted envelope JSON.
    """
    return json.dumps(
        {
            "chain_id": str(chain_id),
            "idempotency_key": "k",
            "default_target": None,
            "steps": [
                {
                    "name": "first_host_step",
                    "method": "POST",
                    "url": f"https://{FIRST_HOST}/v2/files",
                    "headers": {},
                    "body": {"kind": "json", "value": {"name": "f"}},
                    "capture": [],
                    "idempotency_header": None,
                },
                {
                    "name": "blocked_host_step",
                    "method": "PUT",
                    "url": second_step_url,
                    "headers": {},
                    "body": {"kind": "text", "value": "payload"},
                    "capture": [],
                    "idempotency_header": None,
                },
            ],
        }
    )


def _row_on_second_step(chain_id: UUID, *, second_step_url: str) -> UploadRow:
    """An ``attempting`` row positioned on the chain's SECOND step.

    ``endpoint`` is the FIRST step's host, exactly as admission writes it, which
    is what makes the two keys differ.

    Args:
        chain_id: The chain's identity.
        second_step_url: Step 2's URL.

    Returns:
        The row, ready to drive through the executor.
    """
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "both-hosts",
            "state": "attempting",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": FIRST_HOST,
            "uid": UID,
            "chain_envelope_json": _two_host_envelope_json(
                chain_id, second_step_url=second_step_url
            ),
            "current_step_index": 1,
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        },
    )


def _parked_row(
    *,
    endpoint: str,
    auth_blocked_host: str | None,
    route_name: str = "both-hosts",
) -> UploadRow:
    """An ``auth_expired`` row with the two host axes set independently.

    Args:
        endpoint: The FIRST step's host, the pre-D2 probe key.
        auth_blocked_host: The recorded blocked host, or None to exercise the
            fallback.
        route_name: The row's recorded route name (display only).

    Returns:
        The parked row.
    """
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": route_name,
            "state": "auth_expired",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": endpoint,
            "uid": UID,
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": 100,
            "auth_blocked_host": auth_blocked_host,
        },
    )


# ---------------------------------------------------------------------------
# 1-2: the executor reports the host it actually authenticated against.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_auth_carries_the_current_step_host(tmp_path: Path) -> None:
    """The executor reports the host it actually authenticated against.

    Objective: prove ``FailedAuth.blocked_host`` is the CURRENT step's host on
    the bearer arm, which is what makes the recorded column right.

    Success: driving step 2 of a two-host chain with a fresh slot for step 1's
    host ONLY returns ``FailedAuth`` whose ``blocked_host`` is step 2's host,
    which is NOT ``row.endpoint``.
    """
    instance, _ = await _build_instance(tmp_path)
    await instance.token_cache.set(FIRST_HOST, UID, "Bearer good", source="inbound_request")

    row = _row_on_second_step(uuid4(), second_step_url=f"https://{BLOCKED_HOST}/put/obj")
    result = await instance.executor.execute_one_step(row, {})

    assert isinstance(result, FailedAuth), f"step 2 must park on auth; got {result!r}"
    assert result.blocked_host == BLOCKED_HOST, (
        f"the recorded host must be the host the executor authenticated against; "
        f"expected {BLOCKED_HOST!r}, got {result.blocked_host!r}"
    )
    assert result.blocked_host != row.endpoint, (
        "the whole defect is that the row's endpoint is a DIFFERENT host; "
        "a test where the two agree proves nothing"
    )


@pytest.mark.asyncio
async def test_failed_auth_carries_the_dest_host_on_sigv4(tmp_path: Path) -> None:
    """The sigv4 arm reports the current step's host too.

    Objective: the same guarantee on the credential arm, where the host is
    wrapped in :class:`HostCredKey` for the store lookup.

    Success: a row on an ``aws_sigv4`` route with no credential provisioned
    returns ``FailedAuth`` whose ``blocked_host`` is the current step's host.
    """
    instance, _ = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="sigv4", hosts=[FIRST_HOST, BLOCKED_HOST], auth_mode="aws_sigv4"),
        ],
        with_signer_creds=True,
    )

    row = _row_on_second_step(uuid4(), second_step_url=f"https://{BLOCKED_HOST}/put/obj")
    result = await instance.executor.execute_one_step(row, {})

    assert isinstance(result, FailedAuth), f"step 2 must park on auth; got {result!r}"
    assert result.blocked_host == BLOCKED_HOST, (
        f"the sigv4 arm must record the current step's host; expected {BLOCKED_HOST!r}, "
        f"got {result.blocked_host!r}"
    )


# ---------------------------------------------------------------------------
# 3-4b: the write path, and both halves of the read-path invariant.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_park_records_the_blocked_host(tmp_path: Path) -> None:
    """The park writes the executor's host straight into the column.

    Objective: the write path end to end, from the executor's result through
    the sender's park to the persisted column.

    Success: after ``_on_auth_failure`` the re-read row is ``auth_expired``
    with ``auth_blocked_host`` equal to step 2's host.
    """
    instance, _ = await _build_instance(tmp_path)
    await instance.token_cache.set(FIRST_HOST, UID, "Bearer good", source="inbound_request")

    row = _row_on_second_step(uuid4(), second_step_url=f"https://{BLOCKED_HOST}/put/obj")
    await instance.store.insert(row)
    result = await instance.executor.execute_one_step(row, {})
    assert isinstance(result, FailedAuth)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_auth_failure(instance.store, row, result)

    parked = await instance.store.get(row.chain_id)
    assert parked is not None
    assert parked.state == "auth_expired", f"the row must park; got {parked.state!r}"
    assert parked.auth_blocked_host == BLOCKED_HOST, (
        f"the park must record the blocked host; expected {BLOCKED_HOST!r}, "
        f"got {parked.auth_blocked_host!r}"
    )


@pytest.mark.asyncio
async def test_transitions_through_the_shared_writer_clear_the_blocked_host(
    tmp_path: Path,
) -> None:
    """The shared writer leaves the column consistent with the state it wrote.

    Objective: pin the half of the invariant ``record_attempt_result`` DOES
    guarantee. The column is written unconditionally from a parameter
    defaulting to None, so any transition through this writer that does not
    pass a host clears it.

    Success: a parked row driven back to ``queued`` and then to ``succeeded``
    through the shared writer ends with ``auth_blocked_host is None``.
    """
    instance, _ = await _build_instance(tmp_path)
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    await instance.store.insert(row)

    await instance.store.record_attempt_result(
        row.chain_id,
        new_state="queued",
        attempts=row.attempts,
        next_attempt_at=None,
        last_error=None,
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="auth_expired",
    )
    await instance.store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=row.attempts,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed="blocked_host_step",
        expected_state="queued",
    )

    done = await instance.store.get(row.chain_id)
    assert done is not None
    assert done.state == "succeeded"
    assert done.auth_blocked_host is None, (
        "a transition through the shared writer that passes no host must clear the "
        f"column; got {done.auth_blocked_host!r}"
    )


@pytest.mark.asyncio
async def test_the_cas_exits_leave_the_blocked_host_as_inert_history(tmp_path: Path) -> None:
    """The three CAS exits leave the recorded host behind, by design.

    Objective: pin the OTHER half honestly, so nobody reads the column as a
    global invariant or "fixes" the CAS writers by surprise later. ``replay``,
    ``cancel`` and ``mark_corrupted`` move a row out of ``auth_expired``
    without going through ``record_attempt_result``, so the value survives on a
    row that has left the state. No reader acts on it: both kickers filter on
    ``state == 'auth_expired'`` before they read the column, which is what
    makes the read path correct without a clearing sweep.

    Success: all three rows have LEFT ``auth_expired`` and all three still
    carry their recorded host.
    """
    instance, _ = await _build_instance(tmp_path)

    replayed = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    cancelled = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    corrupted = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    for row in (replayed, cancelled, corrupted):
        await instance.store.insert(row)

    await instance.store.replay(replayed.chain_id)
    await instance.store.cancel(cancelled.chain_id)
    await instance.store.mark_corrupted(corrupted.chain_id, "file_body_missing_on_recovery")

    for row in (replayed, cancelled, corrupted):
        after = await instance.store.get(row.chain_id)
        assert after is not None
        assert after.state != "auth_expired", (
            f"the CAS writer must move the row out of auth_expired; got {after.state!r}"
        )
        assert after.auth_blocked_host == BLOCKED_HOST, (
            "the CAS exits deliberately do NOT clear the column; the value is inert "
            f"history on a row in {after.state!r}, and got {after.auth_blocked_host!r}"
        )


# ---------------------------------------------------------------------------
# 5-8: both kickers probe the recorded host.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auth_kicker_probes_the_recorded_host_not_the_endpoint(tmp_path: Path) -> None:
    """A fresh token for the ENDPOINT does not wake a row blocked elsewhere.

    Objective: the F6 defect itself, on the bearer side. Both hosts resolve to
    one ``phantom_bearer`` route, or the kicker would skip the row at its
    ``auth_mode`` partition before ever reaching the probe.

    Success: one kicker pass leaves the row in ``auth_expired`` and takes no
    saturation slot.

    Pre-fix failure mode: the row is re-queued, because the probe read the
    endpoint's fresh slot.
    """
    instance, _ = await _build_instance(tmp_path)
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    await instance.store.insert(row)
    await instance.token_cache.set(FIRST_HOST, UID, "Bearer good", source="inbound_request")

    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()

    after = await instance.store.get(row.chain_id)
    assert after is not None
    assert after.state == "auth_expired", (
        f"a fresh token for {FIRST_HOST} must not wake a row blocked on "
        f"{BLOCKED_HOST}; the row moved to {after.state!r}"
    )
    assert instance.saturation.in_flight == 0, (
        "a row that must not wake must not take a saturation slot either"
    )


@pytest.mark.asyncio
async def test_credential_kicker_probes_the_recorded_host_not_the_endpoint(
    tmp_path: Path,
) -> None:
    """The same on the sigv4 side, with a fresh credential for the endpoint only.

    Objective: the F6 defect on the credential arm. Both hosts are on one
    ``aws_sigv4`` route, for the same partition reason as the bearer case.

    Success: one kicker pass leaves the row parked.
    """
    instance, signer_creds = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="sigv4", hosts=[FIRST_HOST, BLOCKED_HOST], auth_mode="aws_sigv4"),
        ],
        with_signer_creds=True,
    )
    assert signer_creds is not None
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST, route_name="sigv4")
    await instance.store.insert(row)
    await signer_creds.set(HostCredKey(FIRST_HOST), _sigv4_creds(), source="admin_push")

    await Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)._rescan()

    after = await instance.store.get(row.chain_id)
    assert after is not None
    assert after.state == "auth_expired", (
        f"a fresh credential for {FIRST_HOST} must not wake a row blocked on "
        f"{BLOCKED_HOST}; the row moved to {after.state!r}"
    )


@pytest.mark.asyncio
async def test_a_cross_route_chain_is_owned_by_the_blocked_hosts_kicker(tmp_path: Path) -> None:
    """The auth_mode partition moves onto the recorded host with the probe.

    Objective: the partition move, which is the only case that distinguishes it
    from resolving the route on ``row.endpoint`` while probing the recorded
    host. Routes carry per-route ``auth_mode`` and admission route-checks only
    the FIRST step, so a chain whose step 1 is on a bearer route and whose step
    2 is on a sigv4 route is legal config. Resolving on the endpoint would hand
    that row to the bearer kicker, which probes a token cache the sigv4 host's
    credential never lives in.

    Success: the sigv4 kicker claims and wakes the row; the bearer kicker
    skips it.
    """
    instance, signer_creds = await _build_instance(
        tmp_path,
        routes=[
            RouteCfg(name="bearer", hosts=[FIRST_HOST], auth_mode="phantom_bearer"),
            RouteCfg(name="sigv4", hosts=[BLOCKED_HOST], auth_mode="aws_sigv4"),
        ],
        with_signer_creds=True,
    )
    assert signer_creds is not None
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST, route_name="bearer")
    await instance.store.insert(row)
    await signer_creds.set(HostCredKey(BLOCKED_HOST), _sigv4_creds(), source="admin_push")

    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()
    after_bearer = await instance.store.get(row.chain_id)
    assert after_bearer is not None
    assert after_bearer.state == "auth_expired", (
        "the bearer kicker must skip a row whose BLOCKED host is on a sigv4 route; "
        f"the row moved to {after_bearer.state!r}"
    )

    await Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)._rescan()
    after_cred = await instance.store.get(row.chain_id)
    assert after_cred is not None
    assert after_cred.state == "queued", (
        "the sigv4 kicker owns the row, because the host actually blocking it is "
        f"on the sigv4 route; the row is {after_cred.state!r}"
    )


@pytest.mark.asyncio
async def test_a_fresh_token_on_the_blocked_host_still_wakes_the_row(tmp_path: Path) -> None:
    """The counter-test: the fix must not simply stop waking rows.

    Objective: prove the new key is a REDIRECTION of the probe rather than a
    refusal. A fresh token for the host the row is actually blocked on wakes it.

    Success: the next kicker pass re-queues the row.
    """
    instance, _ = await _build_instance(tmp_path)
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=BLOCKED_HOST)
    await instance.store.insert(row)
    await instance.token_cache.set(FIRST_HOST, UID, "Bearer good", source="inbound_request")

    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()
    still_parked = await instance.store.get(row.chain_id)
    assert still_parked is not None
    assert still_parked.state == "auth_expired"

    await instance.token_cache.set(BLOCKED_HOST, UID, "Bearer good", source="inbound_request")
    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()

    woken = await instance.store.get(row.chain_id)
    assert woken is not None
    assert woken.state == "queued", (
        f"a fresh token for the BLOCKED host must wake the row; it is {woken.state!r}"
    )


@pytest.mark.asyncio
async def test_a_null_blocked_host_falls_back_to_the_endpoint(tmp_path: Path) -> None:
    """A row with no recorded host still wakes on its endpoint's slot.

    Objective: pin D2's fallback so a later reader does not delete it as dead
    code. Note that this row shape cannot arise from an upgrade under the
    current schema policy: adding the column bumped ``SCHEMA_VERSION`` and the
    boot gate discards a DB at a different version rather than migrating it, so
    no row parked by the pre-D2 code survives. The test therefore constructs
    the shape directly, and the fallback is defence in depth.

    Success: the row is re-queued.
    """
    instance, _ = await _build_instance(tmp_path)
    row = _parked_row(endpoint=FIRST_HOST, auth_blocked_host=None)
    await instance.store.insert(row)
    await instance.token_cache.set(FIRST_HOST, UID, "Bearer good", source="inbound_request")

    await Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)._rescan()

    woken = await instance.store.get(row.chain_id)
    assert woken is not None
    assert woken.state == "queued", (
        f"a NULL column must fall back to row.endpoint; the row is {woken.state!r}"
    )


# ---------------------------------------------------------------------------
# 9-10b: the derived column set, and the sanitisation rule on both arms.
# ---------------------------------------------------------------------------


def test_the_uploads_schema_carries_the_new_column() -> None:
    """The boot gate's derived column set gained ``auth_blocked_host``.

    Objective: prove the new column joins ``EXPECTED_UPLOADS_COLUMNS``, which
    is derived from ``schema.sql`` at import. That is what makes the boot
    gate's subset test cover the new read: a DB missing the column discards and
    boots fresh rather than failing at the first park.

    Success: the column is in the derived set.
    """
    assert "auth_blocked_host" in EXPECTED_UPLOADS_COLUMNS, (
        "the boot gate derives its expected column set from schema.sql; a column "
        "the DDL carries must appear there"
    )


async def _park_hostless_row(tmp_path: Path, *, auth_mode: str) -> UploadRow:
    """Drive a row with a hostless step URL through the executor and the park.

    A ``hosts: ["*"]`` catch-all route is what lets the row reach the auth arm
    at all: without it ``resolve_route`` raises and the row is classified
    ``RouteUnresolved`` instead.

    Args:
        tmp_path: The test's temporary directory.
        auth_mode: The catch-all route's auth mode, which selects the arm.

    Returns:
        The re-read parked row.
    """
    instance, _ = await _build_instance(
        tmp_path,
        routes=[RouteCfg(name="catch-all", hosts=["*"], auth_mode=auth_mode)],  # type: ignore[arg-type]  # the caller passes a literal member of AuthMode
        with_signer_creds=auth_mode == "aws_sigv4",
    )
    row = _row_on_second_step(uuid4(), second_step_url=HOSTLESS_STEP_URL)
    await instance.store.insert(row)
    result = await instance.executor.execute_one_step(row, {})
    assert isinstance(result, FailedAuth), f"the row must reach the auth arm; got {result!r}"

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_auth_failure(instance.store, row, result)
    parked = await instance.store.get(row.chain_id)
    assert parked is not None
    return parked


def _assert_sanitised(parked: UploadRow) -> None:
    """Assert the recorded host is the fixed token and leaks no producer text.

    Args:
        parked: The re-read parked row.
    """
    assert parked.auth_blocked_host == NO_HOST_TOKEN, (
        f"a hostless step URL must record the fixed placeholder; got {parked.auth_blocked_host!r}"
    )
    for leak in ("?", "sig", "SECRET", "/v1/files"):
        assert leak not in (parked.auth_blocked_host or ""), (
            f"the persisted, admin-surfaced column must carry no producer URL text; "
            f"{leak!r} appears in {parked.auth_blocked_host!r}"
        )


@pytest.mark.asyncio
async def test_a_hostless_step_url_never_leaks_a_query_string_into_the_column(
    tmp_path: Path,
) -> None:
    """The bearer arm records the placeholder, never the raw path and query.

    Objective: the sanitisation rule. ``host_key_for`` is not a hostname function:
    it returns the WHOLE INPUT lower-cased when urlparse finds no host, and a
    step URL can legitimately be a bare path carrying a presigned query string.
    ``auth_blocked_host`` is persisted and surfaced on four admin paths, so it
    takes the parsed hostname or a fixed token and nothing else.

    Success: the parked row's column is the ``<no-host>`` literal and contains
    none of the URL's own text.
    """
    _assert_sanitised(await _park_hostless_row(tmp_path, auth_mode="phantom_bearer"))


@pytest.mark.asyncio
async def test_the_sigv4_arm_sanitises_the_recorded_host_too(tmp_path: Path) -> None:
    """The credential arm applies the same rule at its own construction sites.

    Objective: pin the rule on the OTHER arm. The bearer test exercises the
    slot-check site only, and the sigv4 sites are the ones where passing
    ``str(dest_host)`` would be identity over ``host_key_for`` and would reinstate
    exactly the raw-input fallback the rule forbids.

    Success: the same assertions as the bearer case.
    """
    _assert_sanitised(await _park_hostless_row(tmp_path, auth_mode="aws_sigv4"))
