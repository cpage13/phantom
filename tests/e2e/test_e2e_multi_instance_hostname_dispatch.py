"""§9.3 — multi-instance dispatch via the genuine no-header HOSTNAME path.

E2E-20 and the cross-instance-isolation aggressor both route with the
``X-Phantom-Instance`` header because the in-process harness binds every
emulator on ``127.0.0.1`` (differing only by port) and the dispatcher
matches hostname IGNORING port — so two emulators are indistinguishable by
hostname and the header is the only way to force routing. That leaves the
PRODUCTION dispatch path (``InstanceDispatcher.resolve(url, None)`` →
``host_prefixes`` fnmatch on the chain's first-step URL hostname) covered
only by unit tests (``test_dispatcher.py``), with no end-to-end coverage.

This test closes that gap the lightweight way the human directed (no
containers): it binds the two emulators to DISTINCT loopback IPs
(``127.0.0.1`` and ``127.0.0.2``) and gives each Phantom instance the
matching IP as its sole ``host_prefix``. The chain's first-step URL then
carries the distinct IP as its hostname, so the dispatcher resolves the
owning instance by hostname ALONE — no ``X-Phantom-Instance`` header is
sent. Because the executor makes a real TCP connection to that URL, the
upload also physically lands on the right emulator, and the emulator's
presigned PUT URL (minted from the inbound ``request.base_url``) keeps the
second step on the same IP. The whole chain stays on the correct instance
end-to-end.

Mechanism choice (pinned in the R-2 execution log): distinct loopback IPs.
Falling back to the header would test nothing this file exists to test, so
a second loopback IP is a hard prerequisite. Linux binds all of
``127.0.0.0/8`` out of the box. macOS only has ``127.0.0.1`` on ``lo0``
until an alias is added, so this module's autouse preflight
(``ensure_second_loopback``) first attempts the alias non-interactively
(``sudo -n``) and, when ``127.0.0.2`` still does not bind, FAILS (never
skips) with a message naming ``scripts/dev/ensure_loopback_alias.sh``, so
a darwin box cannot silently lose this coverage.

Two tests:

* ``test_uploads_route_by_hostname_no_header`` — two uploads, each built
  for its instance's IP-hostname, route to the correct emulator with NO
  header; the other emulator never sees the chain; admin scoping confirms
  each row landed in its own instance's store. Falsifier: break the
  host_prefix match (e.g. give both instances the same prefix, or strip
  the routing) → the wrong instance serves / NoMatchingInstance → RED.
* ``test_corrupt_one_instance_db_quarantines_only_that_instance`` — the
  §9.1/§9.2 guards are per-instance-scoped: pre-seed instance B's DB with
  corruption, boot, and assert ONLY B's data_dir holds a quarantine
  artifact while A boots clean; then a fresh upload routes by hostname to
  EACH instance and succeeds (B recovered on a fresh empty DB; A was never
  touched). Falsifier: scope the integrity gate process-wide instead of
  per-instance → A also quarantines / B's corruption taints A → RED.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import aiosqlite
import pytest
from phantom_client import PhantomClient
from phantom_emulator.auth.modes import AuthMode

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import build_create_file_request
from .helpers.stack import EmulatorControl, boot_stack, ensure_second_loopback

# Distinct loopback IPs. The primary emulator + instance "prod" live
# on 127.0.0.1; the extra emulator + instance "staging" on 127.0.0.2.
PROD_HOST: str = "127.0.0.1"
STAGING_HOST: str = "127.0.0.2"

PROD_INSTANCE: str = "prod"
STAGING_INSTANCE: str = "staging"
PROD_DATA_DIR: str = "prod"
STAGING_DATA_DIR: str = "staging"

PROD_BODY: bytes = b"phantom-e2e-hostname-dispatch-prod-body"
STAGING_BODY: bytes = b"phantom-e2e-hostname-dispatch-staging-body"
SHARED_SUB: str = "00000000-0000-0000-0000-0000000000aa"

TERMINAL_BUDGET_SECONDS: float = 15.0


pytestmark = [pytest.mark.e2e]


@pytest.fixture(autouse=True)
def _second_loopback_preflight() -> None:
    """Preflight: the second loopback IP is a hard prerequisite here.

    On darwin, tries one non-interactive lo0-alias attempt and then FAILS
    (never skips) when ``STAGING_HOST`` is still not bindable, naming the
    fixer script. Linux binds 127.0.0.0/8 natively, so this is a no-op
    there. See :func:`tests.e2e.helpers.stack.ensure_second_loopback`.
    """
    ensure_second_loopback(STAGING_HOST)


def _two_instance_overrides() -> dict[str, object]:
    """config_overrides declaring two IP-scoped, fully-isolated instances.

    Each instance's sole ``host_prefix`` is its loopback IP, so the
    dispatcher resolves by the chain's first-step URL hostname with NO
    header. ``auth_mode: none`` matches E2E-20 — under inprocess mode each
    emulator has its own ephemeral issuer, so a single JWT cannot satisfy
    both; dropping upstream auth keeps the test focused on dispatch.
    """
    return {
        "instances": [
            {
                "id": PROD_INSTANCE,
                "host_prefixes": [PROD_HOST],
                "data_dir": PROD_DATA_DIR,
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "prod_emulator",
                        "hosts": [PROD_HOST],
                        "auth_mode": "none",
                    },
                ],
            },
            {
                "id": STAGING_INSTANCE,
                "host_prefixes": [STAGING_HOST],
                "data_dir": STAGING_DATA_DIR,
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "staging_emulator",
                        "hosts": [STAGING_HOST],
                        "auth_mode": "none",
                    },
                ],
            },
        ],
    }


async def _submit_by_hostname(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    body: bytes,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit one chain routed PURELY by the first-step URL hostname.

    Crucially there is NO ``SubmitOptions(instance_id=...)`` — the envelope's
    first-step URL (built from ``emulator_url``, which carries the instance's
    distinct loopback IP) is the only routing signal; the dispatcher
    resolves the owning instance from its hostname.
    """
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
        # No options → no X-Phantom-Instance header → production hostname path.
    )


async def test_uploads_route_by_hostname_no_header(tmp_path: Path) -> None:
    """Two uploads route to the correct instance by URL hostname, no header."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=1,
        extra_emulator_hosts=[STAGING_HOST],
        config_overrides=_two_instance_overrides(),
    )
    try:
        pc = stack.phantom_client
        prod_emulator: EmulatorControl = stack.emulator
        staging_emulator: EmulatorControl = stack.extra_emulators[0]
        prod_url = stack.emulator_url
        staging_url = stack.extra_emulator_urls[0]

        # Sanity: the harness actually bound the emulators to distinct IPs,
        # so the first-step URL hostnames differ (the whole premise).
        assert f"//{PROD_HOST}:" in prod_url, prod_url
        assert f"//{STAGING_HOST}:" in staging_url, staging_url

        for emu in (prod_emulator, staging_emulator):
            emu.clear_received()
            emu.clear_failures()
            emu.set_auth_mode(AuthMode.NONE)

        bearer = stack.fake_security_token(sub=SHARED_SUB)

        # Submit one chain to each instance using ONLY the URL hostname.
        chain_prod = uuid4()
        await _submit_by_hostname(
            pc, chain_id=chain_prod, body=PROD_BODY, emulator_url=prod_url, bearer=bearer
        )
        chain_staging = uuid4()
        await _submit_by_hostname(
            pc, chain_id=chain_staging, body=STAGING_BODY, emulator_url=staging_url, bearer=bearer
        )

        # Both reach succeeded.
        await assert_chain_reaches_state(
            pc, chain_prod, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        await assert_chain_reaches_state(
            pc, chain_staging, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )

        # Each emulator's received log carries exactly the chain routed to
        # it by hostname — no cross-talk.
        await assert_emulator_received(
            prod_emulator, phantom_local_uuid=str(chain_prod), body_size=len(PROD_BODY)
        )
        await assert_emulator_received(
            staging_emulator, phantom_local_uuid=str(chain_staging), body_size=len(STAGING_BODY)
        )
        prod_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in prod_emulator.received()}
        staging_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in staging_emulator.received()}
        assert str(chain_staging) not in prod_uuids, (
            f"hostname dispatch leaked staging chain {chain_staging} to the prod emulator"
        )
        assert str(chain_prod) not in staging_uuids, (
            f"hostname dispatch leaked prod chain {chain_prod} to the staging emulator"
        )

        # Admin scoping confirms each row landed in its OWN instance's store
        # — the dispatcher picked the instance by hostname, so the row was
        # admitted into that instance's SQLite.
        prod_rows, _ = await pc.list_uploads(instance=PROD_INSTANCE)
        prod_ids = {r.chain_id for r in prod_rows}
        assert chain_prod in prod_ids
        assert chain_staging not in prod_ids, f"{chain_staging} leaked into {PROD_INSTANCE} scope"

        staging_rows, _ = await pc.list_uploads(instance=STAGING_INSTANCE)
        staging_ids = {r.chain_id for r in staging_rows}
        assert chain_staging in staging_ids
        assert chain_prod not in staging_ids, f"{chain_prod} leaked into {STAGING_INSTANCE} scope"
    finally:
        await stack.tear_down()


async def test_corrupt_one_instance_db_quarantines_only_that_instance(tmp_path: Path) -> None:
    """The boot-time integrity gate is per-instance-scoped under multi-instance.

    Pre-seed instance B (staging)'s DB with corruption, boot the two-instance
    stack, and assert ONLY B's data_dir holds a quarantine artifact (A is
    untouched). Then a fresh upload routes by hostname to EACH instance and
    succeeds — B recovered on a fresh empty DB, A never saw B's corruption.
    """
    # Seed staging's per-instance DB with a corrupt file BEFORE boot.
    # Layout: <data_dir>/<instance.data_dir>/uploads.db (app.py:175).
    staging_root = tmp_path / STAGING_DATA_DIR
    staging_root.mkdir(parents=True, exist_ok=True)
    staging_db = staging_root / "uploads.db"
    async with aiosqlite.connect(str(staging_db)) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()
    # Overwrite the whole main file with garbage so PRAGMA integrity_check
    # fails deterministically (a header smudge can survive WAL recovery).
    staging_db.write_bytes(b"\x00\xff" * 4096)

    stack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=1,
        extra_emulator_hosts=[STAGING_HOST],
        config_overrides=_two_instance_overrides(),
    )
    try:
        prod_root = tmp_path / PROD_DATA_DIR

        # ONLY staging quarantined; prod booted clean (no artifact under it).
        staging_quarantines = list(staging_root.glob("uploads.corrupted.*.db"))
        prod_quarantines = list(prod_root.glob("uploads.corrupted.*.db"))
        assert len(staging_quarantines) == 1, (
            f"staging's corrupt DB should be quarantined exactly once; found {staging_quarantines}"
        )
        assert prod_quarantines == [], (
            f"prod must NOT quarantine on staging's corruption (per-instance gate); "
            f"found {prod_quarantines}"
        )
        # Both instances have a fresh live DB at their canonical path.
        assert staging_db.exists()
        assert (prod_root / "uploads.db").exists()

        pc = stack.phantom_client
        prod_emulator: EmulatorControl = stack.emulator
        staging_emulator: EmulatorControl = stack.extra_emulators[0]
        for emu in (prod_emulator, staging_emulator):
            emu.clear_received()
            emu.clear_failures()
            emu.set_auth_mode(AuthMode.NONE)
        bearer = stack.fake_security_token(sub=SHARED_SUB)

        # Both instances still SERVE — route a fresh upload to each by
        # hostname and reach succeeded. B recovered on its fresh DB.
        chain_prod = uuid4()
        await _submit_by_hostname(
            pc, chain_id=chain_prod, body=PROD_BODY, emulator_url=stack.emulator_url, bearer=bearer
        )
        chain_staging = uuid4()
        await _submit_by_hostname(
            pc,
            chain_id=chain_staging,
            body=STAGING_BODY,
            emulator_url=stack.extra_emulator_urls[0],
            bearer=bearer,
        )
        await assert_chain_reaches_state(
            pc, chain_prod, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        await assert_chain_reaches_state(
            pc, chain_staging, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
    finally:
        await stack.tear_down()
