"""An operator's credential push must survive a concurrent SigV4 prepare.

Objective: pin that the SigV4 provider can never mark a credential ``bad`` on
the strength of a read that a concurrent push has already made stale.

THE DEFECT THIS CLOSES. ``SigV4AuthProvider.prepare`` reads the host's
credential slot, and when the slot is missing or bad it parks the row. Before
parking it also marked the slot bad, EAGERLY, with an unconditional
``UPDATE credential_store SET status = 'bad' WHERE dest_host = ?``. There is an
await between that read and that write, so they are not atomic.

Walk the two cases the branch covers. When the slot was genuinely absent the
UPDATE matched zero rows and did nothing. When the slot was already bad it set
bad again and did nothing. So in the case it was written for, the eager write
never changed a single row. What it COULD do was land after an operator's push
had committed ``fresh`` in the gap, flip that credential to ``bad``, and leave
every row parked on a host whose credential the operator had just fixed. The
push answered 204 and did nothing, and the credential then read as bad for no
reason at all.

This surfaced as a flaky e2e test,
``test_admin_sts_credential_survives_restart_and_signs_token``, which failed
once on a Linux runner and passed on re-run. The e2e test uploads first and
pushes the credential second, so the sender's first attempt, which reads an
empty slot, races the push. A loaded runner is enough to land the push inside
the window. The flake was never a test problem; it was this bug, observed
intermittently.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.chain.auth_providers import AuthParked, SigV4AuthProvider
from phantom.models.credential import (
    CredCacheRow,
    HostCredKey,
    SigningService,
    SigV4StaticCreds,
)
from phantom.storage.credential_store import SqliteCredentialStore

_HOST = HostCredKey("s3.us-east-1.amazonaws.com")
_URL = "https://s3.us-east-1.amazonaws.com/bucket/object.bin"


def _static_creds() -> SigV4StaticCreds:
    """A resolved static SigV4 key-pair, the shape an operator pushes."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
        region="us-east-1",
        service=SigningService.S3,
        session_token="FQoGZXItoken",
    )


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """Started credential store backed by a tmp SQLite file."""
    s = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await s.start()
    yield s
    await s.stop()


async def _prepare(provider: SigV4AuthProvider) -> object:
    """Drive one prepare against the fixed test URL."""
    return await provider.prepare(
        full_url=_URL,
        uid="unused",
        method="PUT",
        headers={},
        body=b"payload",
        chain_id=uuid4(),
    )


async def test_a_push_landing_mid_prepare_is_not_marked_bad(
    store: SqliteCredentialStore,
) -> None:
    """Objective: the operator's fresh credential survives the race.

    Expected: the slot reads ``fresh`` after prepare returns. The interleaving
    is forced rather than hoped for: the provider's read returns the empty
    slot, and the operator's push commits BEFORE the provider acts on that
    read. Before the fix the provider's stale eager write then flipped the
    just-pushed credential to ``bad``.
    """
    real_get = store.get

    async def read_then_operator_pushes(dest_host: HostCredKey) -> CredCacheRow | None:
        stale = await real_get(dest_host)
        # The operator's push commits in the gap between the provider's read
        # and its write, which is exactly what a loaded runner produced.
        await store.set(dest_host, _static_creds(), source="admin_push")
        return stale

    store.get = read_then_operator_pushes  # type: ignore[method-assign]
    outcome = await _prepare(SigV4AuthProvider(store=store))
    store.get = real_get  # type: ignore[method-assign]

    assert isinstance(outcome, AuthParked), (
        "the provider saw an empty slot, so parking this attempt is correct"
    )
    row = await store.get(_HOST)
    assert row is not None
    assert row.status == "fresh", (
        "the provider marked the operator's just-pushed credential bad on the "
        "strength of a read the push had already made stale; the push answered "
        "204 and every row for this host stays parked"
    )


async def test_an_absent_slot_still_parks(store: SqliteCredentialStore) -> None:
    """Objective: the fix did not stop the provider parking a missing slot.

    Expected: ``AuthParked`` with a 401, and no row invented. Removing the
    eager write must not change what the caller sees on the ordinary path.
    """
    outcome = await _prepare(SigV4AuthProvider(store=store))

    assert isinstance(outcome, AuthParked)
    assert outcome.status == 401
    assert await store.get(_HOST) is None


async def test_a_bad_slot_still_parks_and_stays_bad(store: SqliteCredentialStore) -> None:
    """Objective: a credential already marked bad stays bad and still parks.

    Expected: ``AuthParked``, and the slot still ``bad``. This is the other half
    of the branch, pinned so the fix cannot quietly un-bad a credential that a
    real 401 marked bad.
    """
    await store.set(_HOST, _static_creds(), source="admin_push")
    await store.mark_bad(_HOST)

    outcome = await _prepare(SigV4AuthProvider(store=store))

    assert isinstance(outcome, AuthParked)
    row = await store.get(_HOST)
    assert row is not None
    assert row.status == "bad"
