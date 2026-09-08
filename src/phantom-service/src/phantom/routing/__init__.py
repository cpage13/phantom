"""Route policy resolution - fnmatch over an instance's declared routes.

A route resolution maps the first-step URL of a chain to the per-instance
``RouteCfg`` that owns it. The resolution rule is "first match by host
fnmatch in declaration order"; catch-all routes (``hosts=['*']``) live
last by convention.

The composition root passes :func:`resolve_route` directly to consumers
that need it (the executor, the send route). There is no Protocol seam:
the function is the seam.

**One parser decides both the policy and the dial.** Every host in this
module comes out of :func:`_parsed_host`, which is ``httpx.URL`` - the same
rfc3986 parser :class:`~phantom.transport.httpx_client.HttpxUpstreamClient`
hands the URL to. A second parser is not a style choice here: ``urlparse``
and rfc3986 disagree about an authority containing a backslash
(``https://s3.evil.test\\.x.amazonaws.com/steal`` is host
``s3.evil.test\\.x.amazonaws.com`` to ``urlparse`` and ``s3.evil.test`` to
httpx), so deciding policy with one and dialling with the other meant the
route table matched a host the request never went to. It has to be the
dialling parser or the table is not an allowlist.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from typing import Literal, TypeAlias

import httpx

from phantom.config.settings import InstanceCfg
from phantom.models.chain import ChainEnvelope

# An absolute URL, anchored at position ZERO: scheme, then ``://``. The test
# this replaced was ``"://" in url``, an unanchored substring search that read
# a RELATIVE url whose QUERY carried a URL value (``/upload?cb=https://x/``) as
# already absolute, so ``default_target`` was never applied and a pathless URL
# reached the route gate and the transport. RFC 3986 section 3.1 fixes the
# scheme grammar to ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ).
_ABSOLUTE_URL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")

# A string that is a bare AUTHORITY rather than a URL: ``minio.local:9000``,
# ``[::1]``, ``s3.amazonaws.com``. The admin credential push and the boot-time
# ``sigv4_credentials`` config both key on a spelling like this, while the
# executor keys on a full step URL, and :func:`host_key_for` has to fold both
# onto ONE key or a pushed credential is unreachable. Excluding ``/``, ``?``
# and ``#`` is what keeps a bare PATH (``/bucket/key?X-Amz-Signature=...``) out
# of this branch: a path's first segment is not a hostname.
_BARE_AUTHORITY_RE = re.compile(r"^[^/?#\s]+$")

# The outbound-auth mode a route declares. Named here (rather than left as a
# bare inline Literal) so consumers that branch on it - the executor's auth
# arm, the two kickers' auth_mode guard - compare against an
# exhaustiveness-checkable type instead of raw strings (CONTEXT "no raw string
# comparisons when the value set is known"). Mirrors ``RouteCfg.auth_mode``
# (config/settings.py).
AuthMode: TypeAlias = Literal["phantom_bearer", "none", "aws_sigv4"]  # noqa: UP040


@dataclass(frozen=True)
class ResolvedRoute:
    """One resolved route policy for a URL within an instance.

    Attributes:
        route_name: The matched ``RouteCfg.name``.
        auth_mode: How Phantom authenticates the outbound request on this
            route (``phantom_bearer`` bearer-inject, ``aws_sigv4`` re-sign, or
            ``none``).
        timeout_seconds: Per-route HTTP timeout (None falls back to the
            configured global default, ``upstream.timeout_seconds``).
        send_deadline_seconds: Max wall-clock seconds a buffered upload may
            keep trying before it gives up to the terminal ``expired`` state
            (None = no deadline). Measured from ``row.received_at``; read by
            the executor's send-deadline gate and the kicker parked-row sweeps.
    """

    route_name: str
    auth_mode: AuthMode
    timeout_seconds: float | None = None
    send_deadline_seconds: int | None = None


type HostKey = str
"""A URL normalised down to the string Phantom keys hosts on.

Usually a hostname, and NOT always one: see :func:`host_key_for`. The alias
exists so ``dict[HostKey, ...]`` at the credential and token stores reads as
what it is rather than as ``dict[str, ...]``.
"""


def _parsed_host(candidate: str) -> str | None:
    """Return the lower-cased host ``httpx`` reads out of ``candidate``.

    THE one parse in this module, so policy and dial cannot diverge. Returns
    ``None`` for every input httpx gives no authority for, including the ones
    it rejects outright: ``httpx.URL`` raises ``IDNAError`` (a ``ValueError``)
    on a malformed A-label and ``InvalidURL`` on other unparseable forms, and
    a caller deciding an allowlist must see "no host", never an exception.

    ``.lower()`` is applied here rather than trusted from httpx: httpx
    normalises the case of a full URL's authority but leaves the case of a
    scheme-relative ``//Example.COM`` alone, and both spellings must key the
    same slot.

    Args:
        candidate: A URL, or a ``//authority`` fragment.

    Returns:
        The lower-cased host, or ``None`` when there is no parseable one.
    """
    try:
        parsed = httpx.URL(candidate)
    except httpx.InvalidURL, ValueError:
        return None
    return parsed.host.lower() or None


def is_absolute_url(url: str) -> bool:
    """Report whether ``url`` carries its own scheme, anchored at position zero.

    The one absolute-URL test, shared by :func:`resolve_first_step_url` and the
    executor's ``_absolute_url`` so the two cannot drift. Both previously asked
    ``"://" in url``, which is a substring search: a RELATIVE step whose QUERY
    holds a URL value (``/upload?callback=https://cb.example/done``) answered
    True, so the envelope's ``default_target`` was skipped and a pathless URL
    went to the route gate and then to httpx, which cannot dial it.

    Args:
        url: The step URL, before any ``default_target`` join.

    Returns:
        ``True`` when ``url`` begins with an RFC 3986 scheme followed by
        ``://``.
    """
    return _ABSOLUTE_URL_RE.match(url) is not None


def dialled_host_for(url: str) -> str | None:
    """Return the host a request to ``url`` would actually be DIALLED at.

    The STRICT form: ``None`` unless ``url`` is an absolute URL whose authority
    httpx can parse, so a bare path, a relative URL and a malformed authority
    are all "no host". That is what makes it the form safe to persist or log -
    it can only ever return a parsed hostname - and
    ``phantom.chain.auth_providers.sanitised_host_for`` is the thin wrapper that
    turns its ``None`` into the fixed ``<no-host>`` token.

    Args:
        url: The step URL being persisted or logged.

    Returns:
        The lower-cased dialled host, or ``None`` when ``url`` names none.
    """
    if not is_absolute_url(url):
        return None
    return _parsed_host(url)


def route_host_for(target: str) -> str | None:
    """Return the host route policy and the credential key are decided on.

    Accepts BOTH shapes its callers hold. The executor and admission pass an
    absolute step URL. The kickers pass a bare RECORDED host
    (``auth_blocked_host``, else ``endpoint``) with no scheme, and the admin
    credential push and boot-time ``sigv4_credentials`` pass a bare
    ``dest_host`` authority. Folding them here is what lets one key space serve
    all of them.

    ``None`` when the target names no host at all: a bare path, a relative URL,
    a malformed authority. :func:`resolve_route` turns that into a miss, so
    such a target matches NO pattern, ``hosts: ['*']`` included. That is what
    makes the route table an allowlist. A pathless step URL used to be keyed as
    its own path-plus-query by :func:`host_key_for`'s fallback, which the
    documented catch-all convention happily fnmatched, so an unroutable URL
    passed the route gate, collected injected auth and was handed to a
    transport that could never send it.

    Args:
        target: An absolute URL, or a bare ``host`` / ``host:port`` / ``[ipv6]``
            authority.

    Returns:
        The canonical lower-cased host, or ``None`` when there is none.
    """
    host = dialled_host_for(target)
    if host is not None:
        return host
    if is_absolute_url(target) or not _BARE_AUTHORITY_RE.match(target):
        return None
    # A bare authority is not a URL, so httpx reads ``minio.local:9000`` as
    # scheme ``minio.local`` and ``files.example.com`` as an empty authority.
    # Re-parsing it as scheme-relative applies the authority grammar, which is
    # the only way a pushed credential key and a forward-time lookup key can be
    # the same string.
    return _parsed_host(f"//{target}")


def host_key_for(url: str) -> HostKey:
    """Normalise ``url`` to the key Phantom looks hosts up by.

    **One destination, one key, whatever the spelling.** The credential store
    (ADR-033) and the token cache are written from three places that spell a
    destination differently: the executor passes a full step URL, the admin
    push passes the bare ``{dest_host}`` path segment, and boot-time config
    passes ``sigv4_credentials[].dest_host``. This folds all three onto the
    same string, so the invariant ``config/settings.py``, ``routes/admin.py``
    and CONTEXT.md all assert - push key == lookup key by construction - is
    true rather than nearly true. ``minio.local:9000``,
    ``https://minio.local:9000/x``, ``[::1]`` and ``https://[::1]:9000/x``
    previously produced FOUR keys across TWO key spaces; a credential pushed
    for a ported or IPv6 host was stored where no lookup could reach it and
    every row targeting it parked forever. The canonical form is the host
    alone: lower-cased, port-stripped, IPv6 unbracketed - the same form
    :func:`route_host_for` yields and the same form the kickers' recorded
    ``auth_blocked_host`` probe carries, so the wake path agrees too.

    **The fallback is the part to read.** When no host can be parsed at all,
    for example because the step URL is a bare path, this returns the ENTIRE
    INPUT STRING lower-cased. That is deliberate: a lookup key that misses is
    harmless, while silently substituting a placeholder would make two
    different pathless URLs share a cache slot. The name says ``host_key``
    rather than ``hostname`` for exactly this reason.

    **The output is UNSANITISED and must not be persisted or logged as-is.**
    A step URL is post-substitution producer data and can carry a query
    string holding a presigned ``X-Amz-Signature`` and ``X-Amz-Credential``,
    so under the fallback that credential material ends up inside the return
    value. Any caller writing a host into a persisted column (D2's
    ``uploads.auth_blocked_host``), into ``last_error`` (F1's
    ``RouteUnresolved.host``) or into a log line calls
    :func:`dialled_host_for` (or its ``<no-host>``-defaulting wrapper
    ``phantom.chain.auth_providers.sanitised_host_for``) instead. Lookup keys
    are the only safe consumer of the fallback, because they are never
    surfaced.

    Args:
        url: An absolute URL, a bare ``host`` / ``host:port`` / ``[ipv6]``
            authority, or any string a caller wants keyed.

    Returns:
        The canonical lower-cased host, or the lower-cased whole input when
        no host can be parsed.
    """
    return route_host_for(url) or url.lower()


def resolve_first_step_url(envelope: ChainEnvelope) -> str:
    """Resolve the first step's URL, applying ``default_target`` if needed.

    A chain's first step may carry a path rather than an absolute URL, in
    which case the envelope's ``default_target`` supplies the origin. Both
    ingress routes need the resolved value before admission (for the route
    check, the degraded-boot guard and the admission-time endpoint), and
    both held a byte-identical copy of this until CL1.

    Args:
        envelope: The submitted chain envelope.

    Returns:
        The first step's absolute URL when one can be formed, else the
        step's own ``url`` unchanged.
    """
    first_step_url = envelope.steps[0].url
    if envelope.default_target and not is_absolute_url(first_step_url):
        first_step_url = str(envelope.default_target).rstrip("/") + (
            first_step_url if first_step_url.startswith("/") else "/" + first_step_url
        )
    return first_step_url


def resolve_route(url: str, instance_cfg: InstanceCfg) -> ResolvedRoute:
    """Pick the first matching route in declaration order.

    Policy is decided on :func:`route_host_for`, the host the transport will
    actually DIAL, and never on :func:`host_key_for`'s permissive lookup
    fallback. Two consequences, both deliberate. An authority the two parsers
    read differently (a backslash in ``https://s3.evil.test\\.x.amazonaws.com``)
    is matched as ``s3.evil.test``, the host the request goes to, so a pattern
    can no longer authorise a destination the request never visits. And a URL
    with no dialable host - a bare path, a relative URL, a malformed authority
    - matches NOTHING, catch-all ``hosts: ['*']`` included, instead of being
    keyed as its own path-plus-query and fnmatched by it. The caller already
    treats the raise as a miss: the executor classifies it ``RouteUnresolved``
    and parks the row for operator repair, and admission records
    ``route_name='unknown'``.

    Args:
        url: The first-step URL of the chain, or - from the kickers' parked-row
            sweeps - a bare recorded host with no scheme.
        instance_cfg: The instance whose routes to walk.

    Returns:
        A :class:`ResolvedRoute`.

    Raises:
        ValueError: When ``url`` carries no dialable host, or when no route
            matches (caller maps to ``invalid_target``).
    """
    host = route_host_for(url)
    if host is None:
        raise ValueError(
            f"URL for instance {instance_cfg.id!r} carries no dialable host, so no route "
            f"can match it (the URL is not repeated here: it is producer data that can "
            f"carry a presigned credential)"
        )
    for route in instance_cfg.routes:
        for pattern in route.hosts:
            if fnmatch.fnmatchcase(host, pattern.lower()):
                return ResolvedRoute(
                    route_name=route.name,
                    auth_mode=route.auth_mode,
                    timeout_seconds=route.timeout_seconds,
                    send_deadline_seconds=route.send_deadline_seconds,
                )
    # The HOST, never the URL: this message reaches logs through callers that
    # re-raise or format it, and a step URL is post-substitution producer data
    # that can carry a presigned ``X-Amz-Signature``. ``host`` here is
    # ``policy_host_for``'s output, which is a parsed hostname or nothing.
    raise ValueError(f"No route matched host {host!r} for instance {instance_cfg.id!r}")


__all__ = [
    "AuthMode",
    "HostKey",
    "ResolvedRoute",
    "dialled_host_for",
    "host_key_for",
    "is_absolute_url",
    "resolve_first_step_url",
    "resolve_route",
    "route_host_for",
]
