"""Per-route auth providers: prepare this request's auth, or park the row.

Until U8 the executor dispatched on ``ResolvedRoute.auth_mode`` in TWO places
96 lines apart, the slot-lookup-and-park block and the 401/403 mark-bad block,
with nothing linking them except that a third auth mode would have to edit
both. Both are now one selection over three providers.

**Prepare-or-park is the whole contract.** A provider either readies the
request (mutating the caller's header dict in place, exactly as the inline arms
did, and reporting the URL to send) or reports that the row must park in
``auth_expired`` with the status and the SANITISED host to persist.

**The sigv4 arm has TWO park legs and the bearer arm has one**, which is the
asymmetry that shapes this module: attaching a bearer token cannot fail, while
signing can fail after a perfectly good credential was found. A provider
interface modelling only "look up a slot" could not absorb that leg, so the
contract is the whole preparation rather than the lookup.

**``none`` gets a provider too.** Leaving one mode inline while the other two
generalise is exactly the special case this deduplication exists to remove, and
it would leave a fourth ``auth_mode ==`` comparison in the executor.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from phantom.chain.headers import set_header
from phantom.chain.query import filter_raw_query
from phantom.chain.sigv4_signer import SigV4SigningError, sign_sigv4
from phantom.models.credential import CredCacheRow, HostCredKey
from phantom.routing import dialled_host_for, host_key_for
from phantom.storage.interface import CredentialStore, TokenCache

logger = logging.getLogger(__name__)

# Placeholder written into ``RouteUnresolved.host`` and into
# ``FailedAuth.blocked_host`` when the step URL carries no parseable host.
# NEVER substitute the raw URL here: the URL is post-substitution producer
# data that can carry a query string with presigned credentials,
# ``RouteUnresolved.host`` is persisted verbatim into ``last_error``, and
# ``FailedAuth.blocked_host`` is persisted into ``uploads.auth_blocked_host``
# (D2/F6); the admin API surfaces both.
NO_HOST_TOKEN = "<no-host>"

# The AWS SigV4 query-string ("presigned") credential set, lower-cased. A
# client that presigned its request carries its whole credential here rather
# than in a header. On an ``aws_sigv4`` route Phantom's own signature is
# authoritative (ADR-033), so this set is superseded material and is stripped
# before signing; every other parameter survives byte-for-byte.
_SIGV4_PRESIGN_QUERY_PARAMS: frozenset[str] = frozenset(
    {
        "x-amz-algorithm",
        "x-amz-credential",
        "x-amz-date",
        "x-amz-expires",
        "x-amz-security-token",
        "x-amz-signature",
        "x-amz-signedheaders",
    }
)


def sanitised_host_for(url: str) -> str:
    """Return the host to PERSIST or LOG for ``url``.

    The counterpart to :func:`phantom.routing.host_key_for`, and the two are
    deliberately different functions. ``host_key_for`` normalises for LOOKUP
    and falls back to the whole input, which is safe only because a cache key
    is never surfaced. This one is what a persisted column or a log line gets:
    a parsed hostname, or the fixed :data:`NO_HOST_TOKEN` when the URL has no
    host at all, so producer-supplied path and query text can never reach the
    admin API through a host field.

    **TOTAL, and that is load-bearing.** It is called from the executor's
    ``except ValueError`` handler for route resolution, so a raise here escapes
    that handler uncaught, kills the sender's ``asyncio.TaskGroup`` and stops
    delivery service-wide. The previous implementation used ``urlparse``, whose
    ``.hostname`` raises ``ValueError('Invalid IPv6 URL')`` on exactly the input
    that sent the caller into that handler (a step 2 URL rendered from a capture
    as ``https://[bad/obj``). :func:`~phantom.routing.dialled_host_for` swallows
    every parse failure into ``None``, so this returns a token instead.

    Sharing that one parser also keeps the wake path honest: the value returned
    here is persisted as ``uploads.auth_blocked_host`` and the kickers probe
    ``HostCredKey(auth_blocked_host)``, so it must be the SAME string
    ``host_key_for`` produces for the same destination.

    Args:
        url: The absolute step URL being authenticated against.

    Returns:
        The lower-cased hostname, or :data:`NO_HOST_TOKEN`.
    """
    return dialled_host_for(url) or NO_HOST_TOKEN


@dataclass(frozen=True)
class AuthReady:
    """Auth is attached; send this request.

    Attributes:
        url: The URL to forward, which is the input URL for every provider
            except the sigv4 one: that provider strips a client's superseded
            presigned credential span before signing (ADR-033), and the URL it
            SIGNED is the URL that must be SENT. Signing one URL and
            forwarding another is a canonical-query mismatch that earns a 403
            SignatureDoesNotMatch on every presigned upload.
        slot_observed_at: The ``observed_at`` of the exact credential or token
            this request was sent with, or ``None`` when no slot was used. It is
            the FENCING TOKEN the caller must hand back to :meth:`mark_bad` if
            the upstream answers 401 or 403. Without it the post-response
            mark-bad flipped whatever the slot held by the time the response
            arrived, and an operator pushes a new credential precisely when
            requests are failing, so the push most likely to be clobbered was
            the one meant to fix the outage.
    """

    url: str
    slot_observed_at: datetime | None = None


@dataclass(frozen=True)
class AuthParked:
    """The row must park in ``auth_expired``; carries what ``FailedAuth`` requires.

    Attributes:
        status: The status to record for the park (401 for every
            preparation-time refusal; the upstream's own status when a live
            response drove it).
        blocked_host: The SANITISED host whose slot rejected this row (D2/F6),
            already through :func:`sanitised_host_for`, because it rides
            ``FailedAuth.blocked_host`` into a persisted column the admin API
            surfaces on four paths.
    """

    status: int
    blocked_host: str


type AuthOutcome = AuthReady | AuthParked


class AuthSlotProvider(Protocol):
    """One route's outbound-auth policy, as a prepare-or-park pair."""

    async def prepare(
        self,
        *,
        full_url: str,
        uid: str,
        method: str,
        headers: dict[str, str],
        body: bytes,
        chain_id: UUID,
    ) -> AuthOutcome:
        """Attach this route's auth to ``headers``, or report that the row must park.

        ``headers`` is MUTATED IN PLACE, which is what the inline arms did and
        what botocore does inside the signer; a provider that returned a new
        dict would change what the signer writes into.
        """
        ...

    async def mark_bad(self, *, host_key: str, uid: str, slot_observed_at: datetime | None) -> None:
        """Flip this route's slot for ``host_key`` to ``bad`` after a 401/403.

        ``slot_observed_at`` is :attr:`AuthReady.slot_observed_at` from the
        ``prepare`` that produced the rejected request. The flip applies only if
        the slot STILL holds that credential, so a newer push that landed during
        the round trip survives the rejection of the request it did not sign.
        """
        ...


@dataclass(frozen=True)
class BearerAuthProvider:
    """``phantom_bearer``: inject the cached ``(endpoint, uid)`` bearer."""

    cache: TokenCache

    async def prepare(
        self,
        *,
        full_url: str,
        uid: str,
        method: str,
        headers: dict[str, str],
        body: bytes,
        chain_id: UUID,
    ) -> AuthOutcome:
        """Inject the cached bearer, or park when the slot is absent or bad.

        **The lookup is keyed on THIS step's URL**, because the executor picks a
        provider per step and a chain can cross hosts. Admission is the writer
        that has to agree: it earmarks the producer's inbound ``Authorization``
        for one ``(host_key_for(step url), uid)`` key per step whose route is
        ``phantom_bearer``, not for the first step alone. When it wrote only the
        first key, a chain whose bearer-protected step came later parked here
        against a key no writer ever filled, and no kicker could wake it.

        Parking happens BEFORE any forwarding, so a producer cannot work around
        a missing slot by putting the credential in that step's own headers: a
        miss never reaches the send, and a hit overwrites the header anyway.

        The write goes through :func:`~phantom.chain.headers.set_header`, which
        DROPS every other casing of ``Authorization`` before inserting Phantom's
        own. Both halves of that matter and neither is cosmetic.

        The producer's credential must not be forwarded. This route exists to
        substitute Phantom's bearer for whatever the client held, and
        ``routes/catch_all.py`` deliberately preserves an inbound
        ``Authorization`` into the synthesized step; ASGI lower-cases it on the
        way in, so the persisted header is ``authorization``. A producer-authored
        envelope can spell it that way too, and admission's header-name check
        only tests RFC 7230 token characters. Assigning the canonical spelling
        beside the lower-cased one left the producer's credential in the map.

        And a duplicate is not merely untidy: verified against the pinned httpx,
        a request built from a dict holding both spellings emits BOTH raw names
        on the wire. Once anything rotates the slot the two values disagree, the
        upstream 401s, the executor marks the slot bad, every buffered row for
        that ``(endpoint, uid)`` parks, the kicker wakes them, and they re-send
        the same persisted stale header beside the fresh bearer and mark it bad
        again - a livelock holding a valid credential. The sigv4 arm was already
        immune because ``sign_sigv4`` rebuilds the map; this is the equivalent.
        """
        del method, body, chain_id  # A bearer injection reads neither.
        slot = await self.cache.get(host_key_for(full_url), uid)
        if slot is None or slot.status == "bad":
            return AuthParked(status=401, blocked_host=sanitised_host_for(full_url))
        set_header(headers, "Authorization", slot.bearer)
        return AuthReady(url=full_url, slot_observed_at=slot.observed_at)

    async def mark_bad(self, *, host_key: str, uid: str, slot_observed_at: datetime | None) -> None:
        """Mark the token slot bad, but only if it still holds the rejected bearer."""
        await self.cache.mark_bad(host_key, uid, observed_at=slot_observed_at)


@dataclass(frozen=True)
class SigV4AuthProvider:
    """``aws_sigv4``: re-sign the request from the host-keyed destination credential.

    The host-keyed credential slot is the refreshable slot, the analogue of the
    ``(endpoint, uid)`` token slot. A missing or already-bad credential,
    INCLUDING ``store is None`` (no store wired), parks the row in
    ``auth_expired`` (NOT terminal) to await a credential re-push, and leaves
    the slot untouched. A credential that is present but cannot sign, such as a
    ProfileRefCred whose botocore chain yields nothing, additionally marks the
    slot bad, because only there does the write change anything a reader sees.
    """

    store: CredentialStore | None

    async def prepare(
        self,
        *,
        full_url: str,
        uid: str,
        method: str,
        headers: dict[str, str],
        body: bytes,
        chain_id: UUID,
    ) -> AuthOutcome:
        """Strip any superseded presigned span, then re-sign, or park on either leg."""
        del uid  # This store is keyed on the destination host alone (ADR-033).
        # F4 precedence: Phantom's signature is authoritative on this route
        # (ADR-033), so a client's presigned query credential is superseded.
        # The STRIPPED url is what gets signed AND what the caller must send,
        # which is why AuthReady carries it: signing one URL while forwarding
        # another is a canonical-query mismatch that earns a 403
        # SignatureDoesNotMatch on every presigned upload.
        stripped = _strip_presigned_query(full_url)
        # ``dest_host`` is the credential-store KEY and keeps the raw-input
        # fallback; the parked host is the SANITISED one. Stripping the
        # presigned query span never touches the host, so both name the same
        # host either side of the strip.
        dest_host = HostCredKey(host_key_for(stripped))
        blocked = sanitised_host_for(stripped)
        if stripped != full_url:
            # ``blocked``, NOT ``host_key_for``. This is the one code path in
            # the service GUARANTEED to be holding a presigned credential, and
            # ``host_key_for`` returns the WHOLE INPUT lower-cased when it finds
            # no host - so a pathless step URL such as
            # ``/bucket/key?X-Amz-Credential=AKIA...&X-Amz-Signature=...``
            # wrote the access-key id and the signature into the operator log at
            # INFO. Its own docstring says the output must not be logged as-is.
            # The sanitised host is already computed two lines up.
            logger.info(
                "stripped client presigned credentials on aws_sigv4 route for "
                "chain_id=%s dest_host=%s",
                chain_id,
                blocked,
            )
        row_cred = await self._credential_for(dest_host)
        if row_cred is None or row_cred.status == "bad":
            # Park WITHOUT writing the slot. This leg used to mark the slot bad
            # first, "so a stale-but-present credential cannot look fresh". It
            # could never achieve that and it could do real harm.
            #
            # It could not help: an absent slot has no row, so the UPDATE matched
            # nothing; an already-bad slot was set bad again; and an undecodable
            # row, which is the only "stale but present" case, is invisible to
            # every reader already, because every reader goes through
            # ``store.get``, including the kicker's oracle, and ``get`` answers
            # ``None`` for a row it cannot decode. Nothing consults the raw
            # status column, so flipping it changed nothing observable.
            #
            # It could harm: the read above and a write here are separated by an
            # await, so an operator's credential push could commit ``fresh`` in
            # that gap and the write would then flip it to ``bad`` on the
            # strength of a read the push had made stale. The push answered 204
            # and did nothing, and every row for the host stayed parked. That is
            # the flaky ``test_admin_sts_credential_survives_restart_and_signs_
            # token`` failure, pinned deterministically by
            # ``test_sigv4_mark_bad_race.py``.
            return AuthParked(status=401, blocked_host=blocked)
        try:
            # Re-sign THIS request now (fresh X-Amz-Date) over the rehydrated
            # body. botocore mutates ``headers`` in place; the body stays
            # byte-identical (transparent-proxy invariant).
            await sign_sigv4(
                method=method,
                url=stripped,
                headers=headers,
                body=body,
                credential=row_cred.credential,
            )
        except SigV4SigningError:
            # The SECOND park leg: the credential was found and signing still
            # failed. Bearer auth has no analogue, which is why the provider
            # contract is prepare-or-park rather than lookup-a-slot.
            logger.warning(
                "aws_sigv4 credential resolution failed for host %s; "
                "marking slot bad and parking (auth_expired)",
                dest_host,
            )
            # Fenced on the credential that failed to sign. Resolving a
            # ProfileRefCred can take a botocore round trip, which is time for
            # an operator's corrected push to land; that push must survive.
            await self._mark_bad(dest_host, observed_at=row_cred.observed_at)
            return AuthParked(status=401, blocked_host=blocked)
        return AuthReady(url=stripped, slot_observed_at=row_cred.observed_at)

    async def mark_bad(self, *, host_key: str, uid: str, slot_observed_at: datetime | None) -> None:
        """Flip the host-keyed cred slot to ``bad`` after a 401/403.

        Symmetric to the bearer mark-bad: the row stays parked until a fresh
        credential re-push freshens the slot (the kicker wakes on ``fresh``).
        Fenced on ``slot_observed_at``, so it refuses to flip a credential that
        replaced the one this request was signed with.
        """
        del uid  # Host-keyed store (ADR-033).
        await self._mark_bad(HostCredKey(host_key), observed_at=slot_observed_at)

    async def _credential_for(self, dest_host: HostCredKey) -> CredCacheRow | None:
        """Return the host-keyed credential row, or ``None`` when unusable.

        ``None`` covers both "no credential store wired" (no route needs
        ``aws_sigv4`` in this deployment) and "no slot for this host yet"; the
        caller treats both identically (missing credential means park).
        """
        if self.store is None:
            return None
        return await self.store.get(dest_host)

    async def _mark_bad(self, dest_host: HostCredKey, *, observed_at: datetime | None) -> None:
        """Flip the host's credential slot to ``bad`` (a no-op without a store).

        ``observed_at`` fences the write to the credential that was actually
        used; see :meth:`CredentialStore.mark_bad`.
        """
        if self.store is None:
            return
        await self.store.mark_bad(dest_host, observed_at=observed_at)


@dataclass(frozen=True)
class NoAuthProvider:
    """``none``: forward as-is, with no Phantom-injected auth."""

    async def prepare(
        self,
        *,
        full_url: str,
        uid: str,
        method: str,
        headers: dict[str, str],
        body: bytes,
        chain_id: UUID,
    ) -> AuthOutcome:
        """Ready by construction: this route asked Phantom to attach nothing."""
        del uid, method, headers, body, chain_id
        return AuthReady(url=full_url)

    async def mark_bad(self, *, host_key: str, uid: str, slot_observed_at: datetime | None) -> None:
        """A no-op: Phantom holds no slot for this route to mark.

        A 401 from a route Phantom never authenticated still parks the row
        (pre-existing behaviour), and no kicker owns it. That is visible in
        ``auth_blocked_host`` rather than changed by it.
        """
        del host_key, uid, slot_observed_at


def _strip_presigned_query(url: str) -> str:
    """Return ``url`` with the AWS SigV4 presigned parameter set removed.

    ADR-033 makes Phantom's host-keyed signature authoritative on an
    ``aws_sigv4`` route, so a client's presigned credential in the query is
    superseded material; forwarding both earns a 4xx for presenting two
    authentication mechanisms. The exact analogue of F7's superseded-header
    removal, in the other carrier. The whole set is removed rather than only
    ``x-amz-signature``, because botocore signs the canonical query string it
    is handed and orphaned ``X-Amz-Credential`` and ``X-Amz-Date`` parameters
    would put the client's credential identifiers inside Phantom's signature.

    Every non-presigned parameter survives byte-for-byte through
    :func:`filter_raw_query`. A fragment is split off before the query span and
    re-attached after it.

    Args:
        url: The absolute step URL about to be signed and forwarded.

    Returns:
        ``url`` with the presigned parameters removed, and with no bare
        trailing ``?`` when nothing survives.
    """
    head, hash_sep, fragment = url.partition("#")
    base, question, raw = head.partition("?")
    if not question:
        return url
    kept = filter_raw_query(raw, keep=lambda key: key.lower() not in _SIGV4_PRESIGN_QUERY_PARAMS)
    out = f"{base}?{kept}" if kept else base
    return f"{out}{hash_sep}{fragment}" if hash_sep else out
