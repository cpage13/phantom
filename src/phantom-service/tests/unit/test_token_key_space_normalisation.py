"""One destination, one token-cache key, whatever the spelling.

The ``(endpoint, uid)`` token cache had three writers and only admission
normalised (review 2026-09-08):

* E2 the admin token push, endpoint push and invalidate routes wrote and
  matched on the RAW ``{endpoint}`` path segment, while ``BearerAuthProvider``
  resolves through ``host_key_for``. The DDL is ``PRIMARY KEY (endpoint,
  uid)`` with no ``COLLATE NOCASE``, so a push to ``S3.Example.COM`` INSERTED
  A SECOND ROW rather than freshening the one the provider reads, and still
  answered 204.
* SW-2 ``AdMinter._mint`` wrote ``endpoint`` verbatim from YAML. An operator
  writing ``ad_mint.endpoint: "https://files.upstream.example"``, the natural
  spelling for something called an endpoint, got successful mints and a fresh
  slot on the admin surface while every row parked in ``auth_expired`` and the
  kicker, which probes the normalised host, woke none of them. Nothing logged
  an error anywhere.

Both halves are the same rule: normalise at the writer, through the one
helper the reader uses.
"""

from __future__ import annotations

import logging
import sys
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from phantom.config.ad_mint import AdMintConfig
from phantom.refresh.ad_client_credentials import AdMinter
from phantom.routing import host_key_for
from phantom.storage.interface import TokenCache

from ._admin_truth_app import build_admin_app, build_instance

# The spelling an operator reasonably types, and the key every reader
# actually looks up. The gap between these two strings is the whole defect.
_MIXED_CASE_HOST = "Files.Upstream.EXAMPLE"
_LOOKUP_KEY = "files.upstream.example"
_URL_SPELLING = "https://files.upstream.example"

_UID = "upstream-sp"


def _ad_config(endpoint: str, **overrides: object) -> AdMintConfig:
    """A valid AD-mint block writing to ``endpoint``.

    Args:
        endpoint: The ``ad_mint.endpoint`` spelling under test.
        overrides: Any other field to pin for one test.

    Returns:
        The validated config.
    """
    base: dict[str, object] = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "client_id": "11111111-1111-1111-1111-111111111111",
        "primary_client_secret_env": "PHANTOM_UPSTREAM_CLIENT_SECRET",
        "scope": "api://files.upstream.example/.default",
        "endpoint": endpoint,
        "uid": _UID,
    }
    base.update(overrides)
    return AdMintConfig.model_validate(base)


class _RecordingCache:
    """A TokenCache that records the key each write addressed."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []

    async def set(self, *, endpoint: str, uid: str, bearer: str, source: str) -> None:
        """Record the ``(endpoint, uid)`` the minter wrote under."""
        del bearer, source
        self.writes.append((endpoint, uid))


class _FakeAccessToken:
    """The two attributes the minter reads off an azure-identity token."""

    def __init__(self) -> None:
        self.token = "minted-token"
        self.expires_on = int((datetime.now(tz=UTC) + timedelta(hours=1)).timestamp())


class _FakeCredential:
    """Stand-in for ``azure.identity.aio.ClientSecretCredential``."""

    def __init__(self, **_kwargs: Any) -> None:
        """Accept and ignore the tenant/client/secret/authority kwargs."""

    async def get_token(self, _scope: str) -> _FakeAccessToken:
        """Answer with a token that expires in an hour."""
        return _FakeAccessToken()

    async def close(self) -> None:
        """No-op."""


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [_URL_SPELLING, _MIXED_CASE_HOST, _LOOKUP_KEY])
async def test_the_minter_writes_the_normalised_endpoint_key(
    configured: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SW-2: a mint must land where the reader looks.

    Objective: the minter wrote ``config.endpoint`` verbatim, so a URL-shaped
    or mixed-case YAML value minted into a key space nothing reads, with no
    error anywhere. Success: all three spellings of the same destination write
    the single normalised key, which is what ``host_key_for`` yields for the
    reader.
    """
    # The minter imports azure-identity lazily inside ``_mint``; binding the
    # module in sys.modules keeps the real (heavy, network-capable) SDK out
    # of a unit test whose subject is the cache KEY, not the mint itself.
    monkeypatch.setitem(
        sys.modules,
        "azure.identity.aio",
        types.SimpleNamespace(ClientSecretCredential=_FakeCredential),
    )
    cache = _RecordingCache()
    minter = AdMinter(
        config=_ad_config(configured),
        token_cache=cache,  # type: ignore[arg-type]  # records writes, nothing else
    )

    await minter._mint("the-secret", "api://files.upstream.example/.default")

    assert cache.writes == [(_LOOKUP_KEY, _UID)]
    assert cache.writes[0][0] == host_key_for(configured)


@pytest.mark.asyncio
async def test_the_minter_still_logs_the_operator_spelling(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SW-2: normalising the key must not rename the endpoint in the logs.

    Objective: the operator diagnoses against the string they wrote in YAML,
    so the collapsed-window warning must keep naming that string rather than
    the derived key. Success: the URL spelling appears in the warning.
    """
    cache: TokenCache = None  # type: ignore[assignment]  # the wait maths never reads it
    minter = AdMinter(
        config=_ad_config(_URL_SPELLING, refresh_seconds_before_expiry=600),
        token_cache=cache,
    )

    with caplog.at_level(logging.WARNING, logger="phantom.refresh.ad_client_credentials"):
        minter._next_mint_wait_seconds(datetime.now(tz=UTC) + timedelta(seconds=60))

    assert _URL_SPELLING in caplog.text


@pytest.mark.asyncio
async def test_token_push_freshens_the_slot_the_reader_resolves(tmp_path: Path) -> None:
    """E2: a mixed-case push must land on the reader's row, not beside it.

    Objective: with the raw path segment written, ``PUT /v1/admin/tokens/
    Files.Upstream.EXAMPLE/<uid>`` inserted a SECOND row and answered 204,
    while the provider kept reading the lower-cased one. Success: the cache
    holds exactly one slot for the destination, under the lookup key.
    """
    ctx = await build_instance(tmp_path, "primary")
    await ctx.token_cache.set(_LOOKUP_KEY, _UID, "Bearer stale", source="admin_push")
    client = TestClient(build_admin_app([ctx]))

    response = client.put(
        f"/v1/admin/tokens/{_MIXED_CASE_HOST}/{_UID}", json={"token": "Bearer fresh"}
    )

    assert response.status_code == 204, response.text
    slots = await ctx.token_cache.list_slots()
    assert [(slot.endpoint, slot.uid) for slot in slots] == [(_LOOKUP_KEY, _UID)]


@pytest.mark.asyncio
async def test_endpoint_wide_push_reaches_the_reader_s_slots(tmp_path: Path) -> None:
    """E2: the endpoint-wide push must enumerate the normalised key space.

    Objective: the endpoint push looked its slots up under the raw segment,
    found none, and wrote nothing while answering 204, so an operator
    refreshing every credential at a destination silently refreshed none.
    Success: the existing slot is freshened, and no second slot appears.
    """
    ctx = await build_instance(tmp_path, "primary")
    await ctx.token_cache.set(_LOOKUP_KEY, _UID, "Bearer stale", source="admin_push")
    await ctx.token_cache.mark_bad(_LOOKUP_KEY, _UID)
    client = TestClient(build_admin_app([ctx]))

    response = client.put(f"/v1/admin/tokens/{_MIXED_CASE_HOST}", json={"token": "Bearer fresh"})

    assert response.status_code == 204, response.text
    slots = await ctx.token_cache.list_slots()
    assert len(slots) == 1
    assert slots[0].status == "fresh", (
        "the endpoint-wide push enumerated a key space nothing writes, so the "
        "bad slot the operator was replacing stayed bad"
    )


@pytest.mark.asyncio
async def test_invalidate_marks_the_reader_s_slot_bad(tmp_path: Path) -> None:
    """E2: invalidation must reach the slot the provider is using.

    Objective: matched on the raw segment, ``DELETE /v1/admin/tokens/
    Files.Upstream.EXAMPLE/<uid>`` flipped nothing and answered 204, so a
    credential the operator had just declared bad stayed in use. Success: the
    slot is marked bad.
    """
    ctx = await build_instance(tmp_path, "primary")
    await ctx.token_cache.set(_LOOKUP_KEY, _UID, "Bearer live", source="admin_push")
    client = TestClient(build_admin_app([ctx]))

    response = client.delete(f"/v1/admin/tokens/{_MIXED_CASE_HOST}/{_UID}")

    assert response.status_code == 204, response.text
    slots = await ctx.token_cache.list_slots()
    assert [slot.status for slot in slots] == ["bad"]


@pytest.mark.asyncio
async def test_the_uid_axis_is_not_normalised(tmp_path: Path) -> None:
    """The uid is opaque to Phantom and must stay byte-exact.

    Objective: ``host_key_for`` lower-cases, and the cache key has two axes;
    applying it to the credential identifier would fold two distinct
    identities onto one slot. CONTEXT.md says Phantom never parses the uid.
    Success: a mixed-case uid keeps its casing through the push.
    """
    ctx = await build_instance(tmp_path, "primary")
    client = TestClient(build_admin_app([ctx]))

    response = client.put(
        f"/v1/admin/tokens/{_MIXED_CASE_HOST}/SP-Upper", json={"token": "Bearer fresh"}
    )

    assert response.status_code == 204, response.text
    slots = await ctx.token_cache.list_slots()
    assert [(slot.endpoint, slot.uid) for slot in slots] == [(_LOOKUP_KEY, "SP-Upper")]
