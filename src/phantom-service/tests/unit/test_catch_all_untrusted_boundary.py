"""Regression tests for the raw-intake catch-all's untrusted-boundary findings.

``routes/catch_all.py`` accepts a stock object-storage upload from anyone
who can reach the port and turns it into a chain. Three defects broke that
translation:

* **A legal S3 key was written to the WRONG OBJECT behind a 202.** The
  destination was composed from the ALREADY percent-decoded path
  parameter, so a key containing an encoded ``#`` or ``?`` re-entered the
  URL with that byte live. ``PUT /bucket/my%23key`` became
  ``https://target/bucket/my#key``; ``_with_forwarded_query`` partitions
  on ``#`` and httpx drops fragments at send time, so the bytes went
  upstream under the key ``my`` while Phantom had already returned 202
  with an ``X-Phantom-Upload-Id``. The ``?`` variant turns key bytes into
  query parameters, which an ``aws_sigv4`` route then folds into
  Phantom's own signature over a canonical request the client never made.
* **Repeated inbound headers collapsed to last-value-wins.**
  ``Headers.items()`` yields each raw occurrence, so accumulating into a
  ``dict[str, str]`` OVERWROTE. A client that sends a header twice and
  signs over both got one forwarded, the upstream rejected the client's
  own signature 403, and with ``retry.max_attempts`` defaulting to ``-1``
  the row retried a request that could never validate while holding its
  saturation slot and buffered body.
* **A documented escape hatch did not exist.** The reserved-prefix
  comment told the reader a bucket named ``v1`` was addressable via
  ``?phantom=``; the guard ran first, so it was not, and the refusal was
  a bare 404 with no envelope and no reason.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from phantom.models.chain import ChainEnvelope
from phantom.routes import catch_all as catch_all_routes
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.testclient import TestClient

from .test_catch_all_route import _DEFAULT_TARGET, _TARGET_HOST, _build_app

# Object keys whose percent-encoded bytes are URL delimiters once decoded.
# Both are legal S3 keys and both are what a stock client puts on the wire.
_HASH_KEY_WIRE = "my%23key"
_HASH_KEY_DECODED = "my#key"
_QUERY_KEY_WIRE = "my%3Ffoo=bar"
_QUERY_KEY_DECODED = "my?foo=bar"


def _query_req(query: str) -> Request:
    """Build the minimal ``Request`` ``_resolve_destination`` needs."""
    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/b/k",
        "query_string": query.encode(),
        "headers": Headers({}).raw,
    }
    return Request(scope)


async def _persisted_step_url(app_and_ctx: tuple[object, object], upload_id: str) -> str:
    """Read the synthesized step URL back out of the persisted envelope."""
    _, ctx = app_and_ctx
    row = await ctx.store.get(UUID(upload_id))  # type: ignore[attr-defined]
    assert row is not None
    return ChainEnvelope.model_validate_json(row.chain_envelope_json).steps[0].url


# ---------------------------------------------------------------------------
# Finding 5 - a decoded key byte must never re-enter the URL as a delimiter.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_encoded_hash_in_key_does_not_become_a_fragment(tmp_path: Path) -> None:
    """A key containing ``#`` reaches the upstream as the key, not a fragment.

    Objective: the transparent-proxy contract says the upstream receives
    the key the client sent. Success is a synthesized step URL that still
    carries the ``%23`` the client put on the wire, and that contains no
    live ``#`` for the fragment logic (or httpx) to cut the key at.

    Pre-fix the URL was ``https://host/mybucket/my#key``, so the object
    would have landed upstream under the key ``my`` behind a 202.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put(f"/mybucket/{_HASH_KEY_WIRE}", content=b"abc")

    assert response.status_code == 202, response.text
    url = await _persisted_step_url((app, ctx), response.headers["X-Phantom-Upload-Id"])
    assert url == f"https://{_TARGET_HOST}/mybucket/{_HASH_KEY_WIRE}"
    assert "#" not in url


@pytest.mark.asyncio
async def test_encoded_question_mark_in_key_does_not_become_a_query(
    tmp_path: Path,
) -> None:
    """A key containing ``?`` reaches the upstream as the key, not query text.

    Objective: key bytes after a live ``?`` become query parameters, which
    on an ``aws_sigv4`` route are signed by Phantom into a canonical
    request the client never made. Success is the ``%3F`` surviving and no
    live ``?`` appearing in the synthesized URL at all.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put(f"/mybucket/{_QUERY_KEY_WIRE}", content=b"abc")

    assert response.status_code == 202, response.text
    url = await _persisted_step_url((app, ctx), response.headers["X-Phantom-Upload-Id"])
    assert url == f"https://{_TARGET_HOST}/mybucket/{_QUERY_KEY_WIRE}"
    assert "?" not in url


def test_resolver_reencodes_the_decoded_delimiters() -> None:
    """The resolver unit: the decoded byte is escaped back before the join.

    Objective: pin the fix at the function the route calls, on exactly the
    two decoded values an ASGI server produces for the wire forms above.
    Success is each delimiter coming back percent-encoded.
    """
    assert (
        catch_all_routes._resolve_destination(
            f"mybucket/{_HASH_KEY_DECODED}", _query_req(""), _DEFAULT_TARGET
        )
        == f"{_DEFAULT_TARGET}/mybucket/{_HASH_KEY_WIRE}"
    )
    assert (
        catch_all_routes._resolve_destination(
            f"mybucket/{_QUERY_KEY_DECODED}", _query_req(""), _DEFAULT_TARGET
        )
        == f"{_DEFAULT_TARGET}/mybucket/{_QUERY_KEY_WIRE}"
    )


def test_ordinary_keys_are_byte_identical_after_reencoding() -> None:
    """Counter-test: a normal key is untouched by the re-encode.

    Objective: the escape must not rewrite the request line for the
    ordinary case, which would break a client's own signature over the
    true path. Success is byte equality for keys made of unreserved and
    sub-delim characters, which are legal unescaped in a path.
    """
    for key in ("mykey", "My.Object-Key", "a/b/c.txt", "a+b", "x~y_z", "a(1),b;c=d", "e!f$g"):
        assert (
            catch_all_routes._resolve_destination(f"bucket/{key}", _query_req(""), _DEFAULT_TARGET)
            == f"{_DEFAULT_TARGET}/bucket/{key}"
        )


def test_forwarded_query_is_unaffected_by_the_reencode() -> None:
    """Counter-test: a legitimately-forwarded query still rides through.

    Objective: the re-encode covers the PATH only; a query-addressed
    operation must still reach the upstream as that operation, byte for
    byte. Success is the inbound query text appended verbatim after the
    escaped path.
    """
    raw = "partNumber=3&uploadId=ABC&X-Amz-Signature=DEADBEEF"
    assert (
        catch_all_routes._resolve_destination(
            f"bucket/{_HASH_KEY_DECODED}", _query_req(raw), _DEFAULT_TARGET
        )
        == f"{_DEFAULT_TARGET}/bucket/{_HASH_KEY_WIRE}?{raw}"
    )


@pytest.mark.asyncio
async def test_explicit_carrier_url_is_not_reencoded(tmp_path: Path) -> None:
    """Counter-test: the ``?phantom=`` carrier is a URL, not a path segment.

    Objective: the explicit carrier's value is already a full URL supplied
    by the producer, so escaping it would corrupt its own scheme, host and
    query. Success is the carrier value reaching the step unchanged.
    """
    app, ctx = await _build_app(tmp_path, default_target=None)
    client = TestClient(app)
    carrier = f"https://{_TARGET_HOST}/mybucket/mykey"

    response = client.put(f"/mybucket/mykey?phantom={carrier}", content=b"abc")

    assert response.status_code == 202, response.text
    url = await _persisted_step_url((app, ctx), response.headers["X-Phantom-Upload-Id"])
    assert url == carrier


# ---------------------------------------------------------------------------
# Finding 6 - repeated inbound headers must survive the forward.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_header_values_are_combined_not_dropped(tmp_path: Path) -> None:
    """Both values of a twice-sent header reach the synthesized step.

    Objective: RFC 7230 permits repeated field lines and a SigV4 client
    may sign over both, so dropping one makes the upstream reject the
    client's own signature 403 forever (``retry.max_attempts`` defaults to
    ``-1``). Success is one field line holding both values, comma-joined
    with no space so an upstream re-canonicalising it reproduces what the
    client signed.

    Pre-fix the accumulator was a plain ``dict[str, str]`` assignment, so
    only the LAST occurrence survived.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put(
        "/mybucket/repeated-key",
        content=b"abc",
        headers=[
            ("X-Amz-Meta-Tag", "first"),
            ("X-Amz-Meta-Tag", "second"),
            ("X-Custom", "only"),
        ],
    )

    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    step = ChainEnvelope.model_validate_json(row.chain_envelope_json).steps[0]
    lowered = {name.lower(): value for name, value in step.headers.items()}
    assert lowered["x-amz-meta-tag"] == "first,second"
    # A singly-sent header is untouched by the combining rule.
    assert lowered["x-custom"] == "only"


def test_repeated_header_under_two_casings_combines_once() -> None:
    """Field names are case-insensitive, so two casings are ONE field.

    Objective: a signer canonicalises header names to lower case before
    combining, so Phantom must too, or a client that varied the casing
    would get two forwarded lines where it signed one. Success is a single
    entry carrying both values.
    """

    class _StubRequest:
        """Minimal stand-in exposing only the ``headers`` the copier reads."""

        def __init__(self, raw: list[tuple[bytes, bytes]]) -> None:
            self.headers = Headers(raw=raw)

    request = _StubRequest(
        [
            (b"x-amz-meta-tag", b"first"),
            (b"X-Amz-Meta-Tag", b"second"),
        ]
    )

    forwarded = catch_all_routes._forwarded_headers(request)  # type: ignore[arg-type]

    assert len(forwarded) == 1
    assert next(iter(forwarded.values())) == "first,second"


@pytest.mark.asyncio
async def test_repeated_phantom_marker_is_still_stripped(tmp_path: Path) -> None:
    """Counter-test: combining does not resurrect a stripped header.

    Objective: the reserved-marker and hop-by-hop strips run per
    occurrence, so a repeated ``X-Phantom-*`` must still leave nothing
    behind. Success is no ``x-phantom-`` name on the persisted step.
    """
    app, ctx = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put(
        "/mybucket/marker-key",
        content=b"abc",
        headers=[
            ("X-Phantom-Uid", "one"),
            ("X-Phantom-Uid", "two"),
            ("X-Custom", "keep"),
        ],
    )

    assert response.status_code == 202, response.text
    row = await ctx.store.get(UUID(response.headers["X-Phantom-Upload-Id"]))
    assert row is not None
    step = ChainEnvelope.model_validate_json(row.chain_envelope_json).steps[0]
    lowered = {name.lower() for name in step.headers}
    assert not any(name.startswith("x-phantom-") for name in lowered)
    assert "x-custom" in lowered


# ---------------------------------------------------------------------------
# Finding 8 - the reserved-prefix guard is unconditional, and says so.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserved_prefix_is_not_addressable_via_the_explicit_carrier(
    tmp_path: Path,
) -> None:
    """A ``?phantom=`` carrier does NOT unlock a reserved first segment.

    Objective: the comment claimed the explicit carrier bypassed this
    guard, which was never true; the decision recorded in
    ``_RESERVED_FIRST_SEGMENTS`` is that it must not become true, because
    a namespace reservation controlled by producer-supplied input is no
    reservation. Success is the refusal firing WITH the carrier present,
    and nothing durably written.
    """
    app, ctx = await _build_app(tmp_path, default_target=None)
    client = TestClient(app)

    response = client.put(
        f"/v1/mykey?phantom=https://{_TARGET_HOST}/v1/mykey",
        content=b"abc",
    )

    assert response.status_code == 404, response.text
    assert await _store_is_empty(ctx)


@pytest.mark.asyncio
async def test_reserved_prefix_refusal_carries_the_canonical_envelope(
    tmp_path: Path,
) -> None:
    """The refusal names its reason instead of being a bare 404.

    Objective: a bare ``Response(status_code=404)`` told a producer
    nothing, so "this name is reserved" was indistinguishable from "this
    path does not exist". Success is the canonical ``{"error": {...}}``
    envelope with ``not_found`` and a ``reserved_path_prefix`` reason
    naming the segment.
    """
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put("/oauth/token", content=b"abc")

    assert response.status_code == 404, response.text
    error = response.json()["error"]
    assert error["code"] == "not_found"
    assert error["details"]["reason"] == "reserved_path_prefix"
    assert error["details"]["segment"] == "oauth"


@pytest.mark.asyncio
async def test_unreserved_first_segment_still_admits(tmp_path: Path) -> None:
    """Counter-test: only the reserved names are refused.

    Objective: the guard must stay a namespace check, not a general
    refusal. Success is an ordinary bucket admitting as before.
    """
    app, _ = await _build_app(tmp_path, default_target=_DEFAULT_TARGET)
    client = TestClient(app)

    response = client.put("/v1beta-bucket/mykey", content=b"abc")

    assert response.status_code == 202, response.text


async def _store_is_empty(ctx: object) -> bool:
    """True when the instance's upload store holds no rows."""
    chain_ids = await ctx.store.list_all_chain_ids()  # type: ignore[attr-defined]
    return len(chain_ids) == 0
