"""A rejected request must not mark bad the credential that replaced it.

Objective: pin the fence on the post-response ``mark_bad`` for both auth slots,
the bearer token cache and the SigV4 credential store.

THE DEFECT THIS CLOSES. When an upstream answers 401 or 403, the executor marks
the route's slot bad so the row parks until a fresh credential arrives. That
write was unconditional: ``UPDATE ... SET status = 'bad' WHERE <key>``. But the
rejection arrives a full network round trip after ``prepare`` read the slot and
signed with it, and in that time an operator may have pushed a replacement. The
unconditional write then flipped the REPLACEMENT to bad, on the strength of a
rejection it had nothing to do with.

That interleaving is not rare. An operator pushes a new credential precisely
because requests are failing, so a request signed with the old credential
returning 401 during the push is the ordinary incident-response case. Without
the fence, the push most likely to be clobbered was the one meant to end the
outage: it answered 204, read as bad, and left every row for the host parked.

The fence is the ``observed_at`` of the exact credential the request was sent
with. ``prepare`` records it on ``AuthReady``, the executor hands it back, and
the store flips the slot only if it still holds that credential. A newer push
has a newer ``observed_at`` and is untouched. An operator's explicit
invalidation passes no fence and stays unconditional, because an operator
saying a credential is bad is absolute.

This is the same defect class as the flaky e2e credential test, one await
earlier; see ``test_sigv4_mark_bad_race.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.chain.auth_providers import AuthReady, SigV4AuthProvider
from phantom.models.credential import HostCredKey, SigningService, SigV4StaticCreds
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.token_cache import SqliteTokenCache

_HOST = HostCredKey("s3.us-east-1.amazonaws.com")
_URL = "https://s3.us-east-1.amazonaws.com/bucket/object.bin"
_ENDPOINT = "files.example.com"
_UID = "producer-1"


def _creds(secret: str) -> SigV4StaticCreds:
    """A static SigV4 credential distinguishable by its secret."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key=secret,
        region="us-east-1",
        service=SigningService.S3,
    )


@pytest.fixture
async def creds(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """Started credential store backed by a tmp SQLite file."""
    s = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await s.start()
    yield s
    await s.stop()


@pytest.fixture
async def tokens(tmp_path: Path) -> AsyncIterator[SqliteTokenCache]:
    """Started token cache backed by a tmp SQLite file."""
    s = SqliteTokenCache(str(tmp_path / "token_cache.db"))
    await s.start()
    yield s
    await s.stop()


async def test_a_rejection_cannot_mark_bad_the_credential_that_replaced_it(
    creds: SqliteCredentialStore,
) -> None:
    """Objective: the operator's replacement survives the old one's 401.

    Expected: the slot reads ``fresh`` with the REPLACEMENT secret. The request
    was signed with the old credential; the replacement landed during its round
    trip; the rejection must not reach the replacement.
    """
    await creds.set(_HOST, _creds("old-secret"), source="admin_push")
    used = await creds.get(_HOST)
    assert used is not None

    await creds.set(_HOST, _creds("new-secret"), source="admin_push")
    replaced = await creds.get(_HOST)
    assert replaced is not None
    assert replaced.observed_at != used.observed_at, (
        "precondition: the two pushes must carry distinct observed_at values, "
        "otherwise this test cannot tell them apart"
    )

    await creds.mark_bad(_HOST, observed_at=used.observed_at)

    row = await creds.get(_HOST)
    assert row is not None
    assert row.status == "fresh", (
        "a rejection of the OLD credential marked the operator's replacement bad; "
        "the push meant to end the outage now reads as failed"
    )
    assert isinstance(row.credential, SigV4StaticCreds)
    assert row.credential.secret_access_key == "new-secret"


async def test_a_rejection_still_marks_bad_the_credential_it_used(
    creds: SqliteCredentialStore,
) -> None:
    """Objective: the fence does not disable legitimate marking.

    Expected: ``bad``. With no replacement in flight, the credential the
    request was signed with is exactly the one the fence names, so the flip
    must land. This is what keeps the fix from failing open.
    """
    await creds.set(_HOST, _creds("only-secret"), source="admin_push")
    used = await creds.get(_HOST)
    assert used is not None

    await creds.mark_bad(_HOST, observed_at=used.observed_at)

    row = await creds.get(_HOST)
    assert row is not None
    assert row.status == "bad"


async def test_an_explicit_invalidation_stays_unconditional(
    creds: SqliteCredentialStore,
) -> None:
    """Objective: an operator's explicit invalidation is absolute.

    Expected: ``bad`` whatever the slot holds. No fence is passed, so nothing
    about a concurrent push can soften an operator saying a credential is bad.
    """
    await creds.set(_HOST, _creds("any-secret"), source="admin_push")

    await creds.mark_bad(_HOST)

    row = await creds.get(_HOST)
    assert row is not None
    assert row.status == "bad"


async def test_a_rejected_bearer_cannot_mark_bad_the_token_that_replaced_it(
    tokens: SqliteTokenCache,
) -> None:
    """Objective: the same fence holds on the bearer token cache.

    Expected: ``fresh`` with the replacement bearer. The bearer arm has the
    identical round-trip window and the identical incident-response timing.
    """
    await tokens.set(_ENDPOINT, _UID, "old-bearer", source="admin_push")
    used = await tokens.get(_ENDPOINT, _UID)
    assert used is not None

    await tokens.set(_ENDPOINT, _UID, "new-bearer", source="admin_push")
    replaced = await tokens.get(_ENDPOINT, _UID)
    assert replaced is not None
    assert replaced.observed_at != used.observed_at, (
        "precondition: the two pushes must carry distinct observed_at values"
    )

    await tokens.mark_bad(_ENDPOINT, _UID, observed_at=used.observed_at)

    row = await tokens.get(_ENDPOINT, _UID)
    assert row is not None
    assert row.status == "fresh", (
        "a rejection of the OLD bearer marked the operator's replacement bad"
    )
    assert row.bearer == "new-bearer"


async def test_the_provider_carries_the_fence_from_prepare_to_mark_bad(
    creds: SqliteCredentialStore,
) -> None:
    """Objective: the executor-facing contract actually threads the fence.

    Expected: ``prepare`` reports the ``observed_at`` of the credential it
    signed with, and handing that back to ``mark_bad`` after a replacement
    leaves the replacement ``fresh``. This is the path the executor drives on a
    401, so it pins the wiring and not only the store.
    """
    await creds.set(_HOST, _creds("old-secret"), source="admin_push")
    provider = SigV4AuthProvider(store=creds)

    outcome = await provider.prepare(
        full_url=_URL,
        uid="unused",
        method="PUT",
        headers={},
        body=b"payload",
        chain_id=uuid4(),
    )
    assert isinstance(outcome, AuthReady)
    signed_with = await creds.get(_HOST)
    assert signed_with is not None
    assert outcome.slot_observed_at == signed_with.observed_at

    await creds.set(_HOST, _creds("new-secret"), source="admin_push")
    await provider.mark_bad(
        host_key=str(_HOST), uid="unused", slot_observed_at=outcome.slot_observed_at
    )

    row = await creds.get(_HOST)
    assert row is not None
    assert row.status == "fresh"
