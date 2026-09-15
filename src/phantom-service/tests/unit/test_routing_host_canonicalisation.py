"""One parser for policy and dial, and one key for one destination.

Two defects in ``phantom.routing``, both invisible until a specific spelling
turned up.

**(a) The route table was not an allowlist.** Policy was decided by fnmatch over
``urlparse(url).hostname`` while the request was dialled by httpx/rfc3986, and
the two parsers disagree about an authority containing a backslash. Verified in
this repo's venv: ``https://s3.evil.test\\.x.amazonaws.com/steal`` is host
``s3.evil.test\\.x.amazonaws.com`` to ``urlparse``, which fnmatches the pattern
``s3.*.amazonaws.com``, and host ``s3.evil.test`` to httpx, which is where the
request actually goes. Because GET is permitted and responses are JSONPath-
captured and surfaced by the admin API, that is a read primitive as well as an
egress one.

**(b) One destination had two keys.** ``host_key_for`` took the whole-input
fallback for a bare ``host:port`` authority and the hostname branch for a URL,
so a credential pushed or configured for ``minio.local:9000`` was stored under a
key the executor's lookup could never produce: every row parked, and a re-push
could not fix it because the wake path is port-stripped too. Bracketed IPv6 was
worse - ``[::1]`` stored ``[::1]`` and looked up ``::1``, so an IPv6 destination
could not be provisioned at all. That falsifies an invariant asserted verbatim
in ``config/settings.py``, ``routes/admin.py`` and CONTEXT.md: push key ==
lookup key BY CONSTRUCTION.
"""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.chain import ChainEnvelope, ChainStep
from phantom.routing import (
    dialled_host_for,
    host_key_for,
    is_absolute_url,
    resolve_first_step_url,
    resolve_route,
)

# The authority the two parsers read differently. Written with a real backslash.
_SPLIT_BRAIN_URL = "https://s3.evil.test\\.x.amazonaws.com/steal"

# Every spelling of ONE destination that the credential push, the boot-time
# config and the executor's forward-time lookup can each produce.
_PORTED_SPELLINGS = ("minio.local:9000", "https://minio.local:9000/bucket/key")
_IPV6_SPELLINGS = ("[::1]", "https://[::1]/bucket/key", "https://[::1]:9000/bucket/key")
_PLAIN_SPELLINGS = (
    "s3.amazonaws.com",
    "S3.AMAZONAWS.COM",
    "https://s3.amazonaws.com/bucket/key",
    "https://S3.amazonaws.com:443/bucket/key",
)


def _instance(*patterns: str) -> InstanceCfg:
    """An instance with one route whose host patterns are ``patterns``."""
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="dest", hosts=list(patterns), auth_mode="none")],
    )


# ---------------------------------------------------------------------------
# (a) Policy is decided by the parser that dials.
# ---------------------------------------------------------------------------


def test_a_backslash_authority_is_routed_as_the_host_httpx_will_dial() -> None:
    """The route table must authorise the destination the request actually reaches.

    Objective: an operator writing ``hosts: ['s3.*.amazonaws.com']`` is naming
    AWS. Deciding policy with ``urlparse`` and dialling with httpx let a URL
    match that pattern and then connect to ``s3.evil.test``, which turns the
    route table from an allowlist into a suggestion - and, since GET is
    permitted and captures are surfaced by the admin API, into a read primitive.

    Success: the host both this module and httpx agree on is ``s3.evil.test``,
    and the AWS pattern therefore does NOT match while a pattern naming the real
    destination does.
    """
    assert httpx.URL(_SPLIT_BRAIN_URL).host == "s3.evil.test", (
        "the premise: this is the host the transport dials"
    )
    assert dialled_host_for(_SPLIT_BRAIN_URL) == "s3.evil.test"

    with pytest.raises(ValueError, match="No route matched"):
        resolve_route(_SPLIT_BRAIN_URL, _instance("s3.*.amazonaws.com"))

    resolved = resolve_route(_SPLIT_BRAIN_URL, _instance("s3.evil.test"))
    assert resolved.route_name == "dest", (
        "a pattern naming the host the request really goes to must still match"
    )


def test_the_route_error_message_carries_the_host_and_not_the_url() -> None:
    """A no-match raise must not put producer URL text into an exception message.

    Objective: the message is formatted by callers into logs, and a step URL is
    post-substitution producer data that can carry a presigned
    ``X-Amz-Signature``. The same rule ``RouteUnresolved`` and
    ``auth_blocked_host`` follow.

    Success: the raise names the parsed host, and none of the query text.
    """
    url = "https://nomatch.example/key?X-Amz-Signature=DEADBEEFCAFE"
    with pytest.raises(ValueError) as excinfo:
        resolve_route(url, _instance("other.example"))
    message = str(excinfo.value)
    assert "nomatch.example" in message
    assert "DEADBEEFCAFE" not in message
    assert "?" not in message


@pytest.mark.parametrize(
    "undialable",
    [
        "/bucket/key?X-Amz-Signature=DEADBEEFCAFE",
        "/upload?callback=https://cb.example/done",
        "https://[bad/obj",
        "https://a b.com/x",
        "",
    ],
)
def test_a_url_with_no_dialable_host_matches_no_route_not_even_the_catch_all(
    undialable: str,
) -> None:
    """``hosts: ['*']`` must not be a way for an unroutable URL to pass the gate.

    Objective: ``host_key_for`` returns the WHOLE INPUT lower-cased when it can
    parse no host, and ``fnmatch(anything, '*')`` is True, so a bare path and its
    query string used to be keyed as a "host" and matched by the documented
    catch-all convention. The row then collected injected auth and was handed to
    a transport that could never dial it.

    Success: every undialable spelling raises, so the caller classifies it
    (``RouteUnresolved``) rather than sending it.
    """
    with pytest.raises(ValueError):
        resolve_route(undialable, _instance("*"))


def test_a_bare_recorded_host_still_resolves_for_the_kickers() -> None:
    """The strictness must not break the callers that legitimately pass a bare host.

    Objective: the kickers' parked-row sweeps resolve
    ``auth_blocked_host or endpoint``, which is a bare hostname with no scheme,
    to decide which flavour owns a row. Refusing that would leave every parked
    row unwakeable.

    Success: a bare host resolves exactly as its URL spelling does.
    """
    assert resolve_route("files.example.com", _instance("files.example.com")).route_name == "dest"
    assert (
        resolve_route("https://files.example.com/x", _instance("files.example.com")).route_name
        == "dest"
    )


# ---------------------------------------------------------------------------
# (b) One destination, one key.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("spellings", "expected"),
    [
        (_PORTED_SPELLINGS, "minio.local"),
        (_IPV6_SPELLINGS, "::1"),
        (_PLAIN_SPELLINGS, "s3.amazonaws.com"),
    ],
    ids=["ported", "ipv6", "plain"],
)
def test_every_spelling_of_one_destination_produces_one_key(
    spellings: tuple[str, ...], expected: str
) -> None:
    """A credential pushed for a destination must be findable by the forward path.

    Objective: the push (``PUT /v1/admin/credentials/{dest_host}``) and the
    boot-time ``sigv4_credentials[].dest_host`` pass a bare authority; the
    executor passes a full step URL. ``host_key_for`` is the ONLY thing making
    those the same key, and it did not: ``minio.local:9000`` keyed itself
    whole while ``https://minio.local:9000/x`` keyed ``minio.local``, and
    ``[::1]`` keyed ``[::1]`` while the URL keyed ``::1``. An IPv6 destination
    was therefore impossible to provision, and a ported one parked every row
    with no way for a re-push to help.

    Success: every spelling folds to the one canonical key.
    """
    keys = {host_key_for(spelling) for spelling in spellings}
    assert keys == {expected}, (
        f"one destination must have one key; {spellings!r} produced {sorted(keys)!r}"
    )


def test_the_recorded_host_and_the_lookup_key_are_the_same_string() -> None:
    """The wake path must probe the key the credential is stored under.

    Objective: the sender persists ``auth_blocked_host`` from the sanitising
    form and the kickers probe ``HostCredKey(auth_blocked_host)`` against the
    store the executor filled with ``HostCredKey(host_key_for(url))``. If those
    two normalisations differ by so much as a port, a fresh push never wakes the
    row it was pushed for.

    Success: for every spelling that names a host, the strict form and the
    lookup form agree.
    """
    for url in (*_PORTED_SPELLINGS, *_IPV6_SPELLINGS, *_PLAIN_SPELLINGS, _SPLIT_BRAIN_URL):
        strict = dialled_host_for(url)
        if strict is None:
            continue  # a bare authority has no URL form to persist
        assert strict == host_key_for(url), (
            f"the persisted host and the lookup key diverge for {url!r}: "
            f"{strict!r} vs {host_key_for(url)!r}"
        )


def test_a_pathless_key_is_still_the_whole_input() -> None:
    """The documented lookup fallback survives, because a miss must stay a miss.

    Objective: ``host_key_for`` deliberately returns the whole input lower-cased
    when no host can be parsed, so two different pathless URLs cannot collide on
    one cache slot. Only the ROUTE decision and the PERSISTED/LOGGED forms were
    tightened; the lookup fallback is unchanged and still carries the warning
    that its output must never be surfaced.

    Success: the fallback still applies, and the strict form refuses the same
    input.
    """
    pathless = "/Bucket/Key?X-Amz-Signature=SIG"
    assert host_key_for(pathless) == pathless.lower()
    assert dialled_host_for(pathless) is None


# ---------------------------------------------------------------------------
# The shared absolute-URL test.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "absolute"),
    [
        ("https://x.example/a", True),
        ("http://x.example/a", True),
        ("s3+custom.v2://x.example/a", True),
        ("/upload?callback=https://cb.example/done", False),
        ("upload?next=ftp://x/y", False),
        ("//x.example/a", False),
        ("://x.example", False),
    ],
)
def test_the_absolute_url_test_is_anchored_at_position_zero(url: str, absolute: bool) -> None:
    """``"://" in url`` is a substring search and was wrong for a query value.

    Objective: both ``resolve_first_step_url`` and the executor's
    ``_absolute_url`` carried the identical unanchored test, so a relative step
    whose query held a URL value skipped ``default_target`` resolution.

    Success: the shared predicate answers on the SCHEME, at position zero.
    """
    assert is_absolute_url(url) is absolute


def test_resolve_first_step_url_joins_a_relative_url_holding_a_url_in_its_query() -> None:
    """The ingress-side copy of the same test must behave identically.

    Objective: ``resolve_first_step_url`` feeds the admission route check, the
    degraded-boot guard and the admission-time ``endpoint``. Reading a relative
    URL as absolute there records the wrong endpoint on the row, which is the
    axis the bearer cache and the kickers key on.

    Success: ``default_target`` is applied and the query survives byte-for-byte.
    """
    envelope = ChainEnvelope(
        chain_id=uuid4(),
        idempotency_key="k",
        default_target="https://files.example.com",
        steps=[
            ChainStep(
                name="upload",
                method="PUT",
                url="/upload?callback=https://cb.example/done",
            )
        ],
    )
    assert resolve_first_step_url(envelope) == (
        "https://files.example.com/upload?callback=https://cb.example/done"
    )
