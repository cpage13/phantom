"""Extract tar since + chain_ids filters over the wire (TEST-6).

Wire-contract regression line for R-EX1 (fixed). This test drives the
SDK ``extract`` call with ``since`` + ``chain_ids`` filters against a
real listener and asserts the tar is narrowed exactly. Before the fix it
failed on a 422 at the HTTP boundary, NOT on a logic error in the R8-5
fix; the fix made the two affected filter axes wire-submittable.

Background. The R8-5 fix made every advertised ``ExtractFilter`` axis
actually restrict the tar: before it, ``since`` and ``chain_ids`` were
silently dropped, so an operator narrowing an emergency recovery got
MORE rows than asked for - a silent data-over-exposure. That route-side
fix is correct and unit-pinned (``test_extract_filter_ignored_in_tar``,
which calls ``extract_uploads`` with Python objects). What no test
covered until now is the WIRE contract: ``POST /v1/admin/chains/extract``
with these filters submitted as JSON over a real listener.

The finding (R-EX1). The wire-facing ``phantom.models.admin.ExtractFilter``
is declared ``model_config = ConfigDict(strict=True, ...)``. FastAPI's
``Body()`` parses the request JSON to a dict and validates it via
``model_validate`` (the DICT path), and Pydantic strict mode rejects a
string -> ``UUID`` and a string -> ``datetime`` on that path:

* ``ExtractFilter.model_validate({"chain_ids": ["<uuid-str>"]})`` raises
  (``Input should be an instance of UUID``).
* ``ExtractFilter.model_validate({"since": "<iso-str>"})`` raises
  likewise for ``datetime``.
* ``ExtractFilter.model_validate_json('{"chain_ids": ["<uuid-str>"]}')``
  is fine - the JSON path is lenient - but FastAPI does not use it.

So the two axes the R8-5 fix made meaningful, ``chain_ids`` and
``since``, are UN-SUBMITTABLE through the SDK / HTTP boundary: the
operator gets a 422 instead of a narrowed tar. (The string-typed axes
``state`` / ``route`` / ``instance`` are unaffected, which is why
``DeleteFilter(route=...)`` works in the bulk-delete e2e; the sibling
``DeleteFilter.since`` has the same latent defect.)

What this test pins (the intended fix). Seed three real uploads, read
each row's true ``received_at`` off the admin detail so the ``since``
cutoff is computed precisely (no clock guessing, no sleeps), then assert
two narrowings over the wire:

* ``chain_ids`` alone -> the tar manifest carries exactly the named
  subset (the third row excluded).
* ``chain_ids`` AND ``since`` together -> the ``since`` predicate
  further restricts WITHIN the named set (the oldest named row dropped
  because its ``received_at`` precedes the cutoff) - the exact R8-5
  compose behavior.

The fix (R-EX1). ``ExtractFilter.since`` / ``ExtractFilter.chain_ids``
(and ``DeleteFilter.since``) now opt out of strict mode at the field
level on both the service and the SDK, so the FastAPI dict path coerces
the natural ISO-string / uuid-string JSON these filters carry. The
model stays ``strict=True`` overall (typos still ``extra="forbid"``) and
a malformed UUID / timestamp still raises a 422 - the validation of bad
input is not loosened.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import tarfile
from uuid import UUID, uuid4

import pytest
from phantom_client import ExtractFilter, PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# Number of rows to seed; three lets one be excluded by chain_ids and a
# different one by the since cutoff.
SEEDED_ROW_COUNT: int = 3

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# Body bytes per seeded upload; distinct per row so a misassociated body
# would show up, though the manifest membership is the load-bearing check.
SEED_BODY_PREFIX: bytes = b"phantom-extract-filter-e2e-body-"

# Shared sub for the seeded uploads.
SHARED_SUB: str = "00000000-0000-0000-0000-000000000006"

# Budget for a seeded upload to park in auth_expired (body retained).
PARK_BUDGET_SECONDS: float = 20.0


async def _seed_parked_upload(stack: E2EStack, *, index: int) -> UUID:
    """Submit one real upload and park it in ``auth_expired``.

    auth_expired retains the body on disk per the suite retention
    defaults, so every seeded row appears in the tar with a body - the
    crispest shape for asserting manifest membership.
    """
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"extract-{index}-{chain_id.hex[:8]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={BODY_REF_NAME: SEED_BODY_PREFIX + str(index).encode()},
        uid=SHARED_SUB,
        auth_token=f"Bearer {stack.fake_security_token(sub=SHARED_SUB)}",
    )
    await assert_chain_reaches_state(
        stack.phantom_client,
        chain_id,
        state="auth_expired",
        timeout_seconds=PARK_BUDGET_SECONDS,
    )
    return chain_id


async def _drain_extract(pc: PhantomClient, filter_: ExtractFilter) -> bytes:
    """Stream the extract tar for ``filter_`` and concatenate all chunks."""
    iter_ = await pc.extract(filter_)
    chunks: list[bytes] = []
    async for chunk in iter_:
        chunks.append(chunk)
    return b"".join(chunks)


def _manifest_chain_ids(tar_bytes: bytes) -> set[UUID]:
    """Parse the tar and return the set of chain_ids its manifest lists.

    The manifest is ``manifest.json`` at the tar root, a list of entries
    each carrying a ``chain_id`` string. Reading the manifest membership
    is the load-bearing assertion - the body files merely accompany it.
    """
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
        for member in tar.getmembers():
            if member.name in {"manifest.json", "./manifest.json"} and member.isfile():
                extracted = tar.extractfile(member)
                data = extracted.read() if extracted is not None else b""
                manifest = json.loads(data.decode())
                return _collect_chain_ids(manifest)
    raise AssertionError(f"manifest.json missing from extract tar; bytes={len(tar_bytes)}")


def _collect_chain_ids(manifest: object) -> set[UUID]:
    """Walk the manifest and collect every ``chain_id`` field value."""
    out: set[UUID] = set()

    def _walk(value: object) -> None:
        if isinstance(value, dict):
            cid = value.get("chain_id")
            if isinstance(cid, str):
                with contextlib.suppress(ValueError):
                    out.add(UUID(cid))
            for v in value.values():
                _walk(v)
        elif isinstance(value, list):
            for v in value:
                _walk(v)

    _walk(manifest)
    return out


async def test_extract_chain_ids_and_since_filters_narrow_the_tar() -> None:
    """The tar carries exactly the chain_ids subset, further cut by since."""
    stack = await boot_stack(
        config_overrides={
            # all_disk so every seeded body lands on disk; the extract tar
            # carries them under the FileBodyStore layout.
            "storage": {"body_store": {"mode": "all_disk"}},
        },
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # Hold the upstream at 401 so every upload parks in auth_expired
        # with its body retained on disk.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            ),
        )

        seeded: list[UUID] = []
        for index in range(SEEDED_ROW_COUNT):
            seeded.append(await _seed_parked_upload(stack, index=index))
        emulator.clear_failures()

        # Read each row's true received_at and sort oldest-first so the
        # since cutoff lands precisely between two rows.
        details = [await pc.get_upload(cid) for cid in seeded]
        details.sort(key=lambda d: d.received_at)
        oldest, middle, newest = details[0], details[1], details[2]

        # --- Axis 1: chain_ids alone names an exact subset. ---
        # Request the oldest + newest by id; the middle must be absent.
        subset = {oldest.chain_id, newest.chain_id}
        tar_ids = _manifest_chain_ids(
            await _drain_extract(pc, ExtractFilter(chain_ids=list(subset)))
        )
        assert tar_ids == subset, (
            f"chain_ids filter did not narrow the tar to the named subset: "
            f"got {tar_ids}, expected {subset} (middle row {middle.chain_id} should be absent)"
        )

        # --- Axis 2: chain_ids AND since compose (the R8-5 fix). ---
        # Cutoff strictly between the oldest and middle received_at; with
        # all three named, the oldest must drop out on the since predicate
        # while the middle + newest survive.
        cutoff = oldest.received_at + (middle.received_at - oldest.received_at) / 2
        assert oldest.received_at < cutoff < middle.received_at, (
            "computed cutoff is not strictly between the oldest and middle rows; "
            f"oldest={oldest.received_at} cutoff={cutoff} middle={middle.received_at}"
        )
        combined_ids = _manifest_chain_ids(
            await _drain_extract(
                pc,
                ExtractFilter(chain_ids=seeded, since=cutoff),
            )
        )
        expected_combined = {middle.chain_id, newest.chain_id}
        assert combined_ids == expected_combined, (
            "since did not further restrict within the chain_ids set (R8-5): "
            f"got {combined_ids}, expected {expected_combined} "
            f"(oldest {oldest.chain_id} should be dropped by the since predicate)"
        )

        # Guard: the cutoff must actually exclude something, else axis 2 is
        # vacuous. The oldest is in the named set but absent from the tar.
        assert oldest.chain_id not in combined_ids, (
            "the since cutoff excluded nothing; the R8-5 assertion would be vacuous"
        )
        # And a since-less extract of the same ids returns all three -
        # proof the exclusion is the since predicate, not a missing row.
        all_ids = _manifest_chain_ids(await _drain_extract(pc, ExtractFilter(chain_ids=seeded)))
        assert all_ids == set(seeded), (
            f"chain_ids extract without since should return all three seeded rows; got {all_ids}"
        )
    finally:
        await stack.tear_down()
