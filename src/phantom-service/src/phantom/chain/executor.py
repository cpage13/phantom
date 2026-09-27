"""Chain executor - the load-bearing primitive.

One call to :meth:`ChainExecutor.execute_one_step` runs **one step**
of the chain identified by ``row.current_step_index``. The executor
owns:

1. The capture-TTL gate (ADR-011) - checked first.
2. ``{{step.var}}`` placeholder substitution in URL, headers, and body.
   A JSON body is substituted INSIDE the parsed structure and serialized
   ONCE at the end (F8), so ``json.dumps`` does the escaping and a captured
   quote or backslash cannot produce malformed output; the three text
   contexts have no serializer, so they run a type gate instead and refuse
   a capture that cannot be spliced as text. Substitution is skipped
   ENTIRELY for a chain marked ``templated=False`` (N3): nothing is
   substituted, nothing is refused and the capture-TTL gate returns
   immediately, so a brace span is forwarded as content. The raw-intake
   catch-all sets that marker, because an object key may legally contain
   braces.
3. Idempotency-header injection per the step's ``idempotency_header``, which
   runs BEFORE auth so the header is inside a SigV4 signature and under the
   same hop-by-hop strip as any other; a name that collides with a framing or
   reserved header is refused rather than smuggled past the strip.
4. Auth injection - looks up ``token_cache.get(endpoint, uid)`` when the
   route is ``phantom_bearer``.
5. The HTTP send through :class:`UpstreamClient`, with EXACTLY ONE framing
   mechanism on the wire: hop-by-hop, framing and connection-scoped headers
   (plus whatever ``Connection`` names) are stripped from every step, so the
   only framing is the ``Content-Length`` the transport computes over the
   bytes actually forwarded. ``Content-Encoding: aws-chunked`` and
   ``x-amz-decoded-content-length`` describe the BODY, not the hop, and are
   forwarded. Every header Phantom itself writes goes through
   :mod:`phantom.chain.headers`, so no name can reach the wire twice in two
   casings.
6. Capture extraction from the response (JSONPath, first-match).
7. Result classification (Succeeded / 4xx / 5xx / FailedAuth /
   FailedNetwork / TemplateUnresolved / CaptureNotRenderable /
   CaptureExpiredStored / RouteUnresolved / InlineBodyInvalid).
   ``TemplateUnresolved`` carries IDENTIFIERS ONLY (step, site, placeholder
   names): it is persisted into ``last_error``, which the admin API
   surfaces, and a step URL or a header value can carry credential
   material. ``CaptureNotRenderable`` carries the same rule for the same
   reason, and its hazard is larger: a capture is upstream response data.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, assert_never

import httpx

from phantom.chain.auth_providers import (
    AuthParked,
    AuthSlotProvider,
    BearerAuthProvider,
    NoAuthProvider,
    SigV4AuthProvider,
    sanitised_host_for,
)
from phantom.chain.headers import has_header, set_header
from phantom.chain.jsonpath import extract, find_placeholders, substitute, whole_placeholder
from phantom.chain.parser import (
    InlineBodyDecodeError,
    decode_inline_body_b64,
    envelope_from_persistence_json,
)
from phantom.config.settings import InstanceCfg
from phantom.models.chain import (
    ChainBodyBytes,
    ChainBodyJson,
    ChainBodyRef,
    ChainBodyText,
    ChainEnvelope,
    ChainStep,
)
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routing import AuthMode, ResolvedRoute, host_key_for, is_absolute_url
from phantom.storage.interface import CredentialStore, TokenCache
from phantom.transport.interface import UpstreamClient, UpstreamRequest

logger = logging.getLogger(__name__)

# Phantom's reserved header namespace. Producers use ``X-Phantom-*``
# headers at the producer→Phantom ingress boundary (X-Phantom-Uid,
# X-Phantom-Idempotency-Key, X-Phantom-Instance, X-Phantom-Group-Id,
# X-Phantom-Multifile-Id, X-Phantom-Order). These headers are
# Phantom's internal control channel and must NEVER be forwarded to
# upstream - that would violate the transparent-proxy invariant (producers
# that put X-Phantom-* into a chain step's headers would otherwise see
# them leak through).
_PHANTOM_RESERVED_HEADER_PREFIX = "x-phantom-"

# Headers that describe the CONNECTION Phantom terminated rather than the
# MESSAGE Phantom forwards. Three groups, named so a reader checking the RFC
# against this comment does not find it wrong:
#   * RFC 7230 section 6.1 hop-by-hop: connection, keep-alive,
#     proxy-authenticate, proxy-authorization, te, trailer, upgrade.
#   * Framing uvicorn has already consumed or the transport recomputes:
#     transfer-encoding, content-length.
#   * Peer negotiation and host rewriting: expect, host.
# Forwarding any of them produces a request whose framing or addressing
# contradicts the message, and because the header is persisted, every retry
# reproduces it. Content-Encoding: aws-chunked and x-amz-decoded-content-length
# are deliberately NOT here: they describe the BODY, which is forwarded
# byte-identically. See the task's per-header determination before adding to
# this set.
_HOP_BY_HOP_HEADERS: frozenset[str] = frozenset(
    {
        "connection",
        "content-length",
        "expect",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _hop_by_hop_names(headers: Mapping[str, str]) -> frozenset[str]:
    """Return the hop-by-hop set plus the tokens this request's Connection names.

    RFC 7230 makes every header listed in ``Connection`` hop-by-hop for that
    connection, so ``Connection: keep-alive, X-Custom-Hop`` makes
    ``X-Custom-Hop`` connection-scoped too. Forwarding a header the client
    marked connection-scoped is the same class of error as forwarding the
    framing itself.

    Args:
        headers: The inbound or persisted header mapping. ``Mapping``, not
            ``dict``: the catch-all calls this with starlette's
            ``request.headers``.

    Returns:
        The lower-cased names to drop for this one request.
    """
    listed: set[str] = set()
    for name, value in headers.items():
        if name.lower() != "connection":
            continue
        listed.update(token.strip().lower() for token in value.split(",") if token.strip())
    return _HOP_BY_HOP_HEADERS | listed


_TEXT_SCALARS: tuple[type, ...] = (str, int, float, bool)
"""Capture types that can be spliced into a TEXT context (F8).

``bool`` is an ``int`` subclass so it is already covered, and it is listed for
the reader rather than for the check. ``None`` is admitted separately at the
gate, because it renders as the string ``"None"`` exactly as it does today and
the required-capture guard already turns a referenced-but-absent capture into a
retryable ``CaptureIncomplete`` before substitution runs.
"""

_CONTROL_CHARS = re.compile(r"[\r\n\x00]")
"""Characters that make a header value un-sendable: ``h11`` refuses them."""


def _wire_encodable(value: str, *, site: Literal["url", "header", "body"]) -> bool:
    """Report whether ``value`` can be encoded onto the wire at ``site``.

    The two codecs the pinned httpx applies, verified against it rather than
    recalled. A HEADER value is encoded ``ascii``, so any non-ASCII character
    refuses. A URL (and a text body, which the executor encodes itself) is
    encoded ``utf-8``, so only a LONE SURROGATE refuses.

    Why this is a gate and not a ``try``: the failure httpx raises is
    ``UnicodeEncodeError``, which is a ``ValueError`` and is not part of the
    httpx exception hierarchy at all. ``HttpxUpstreamClient.send`` catches
    ``httpx.HTTPError``, this executor catches ``httpx.HTTPError``, the sender's
    ``_drive_one`` catches four storage and index types and ``_worker_loop``
    catches ``sqlite3.OperationalError``, so the exception reaches the
    supervising ``asyncio.TaskGroup``, the process drains, startup recovery
    resets the row to ``queued``, and the next claim crashes on it again. A
    value that cannot be encoded has to be refused before it is handed over.

    Args:
        value: The rendered text about to be spliced into the request.
        site: Which part of the request it is going into.

    Returns:
        ``True`` when the value survives that site's encoding.
    """
    try:
        value.encode("ascii" if site == "header" else "utf-8")
    except UnicodeEncodeError:
        return False
    return True


@dataclass(frozen=True)
class Succeeded:
    """Step completed; ``captured`` is the new (or augmented) CapturedValues snapshot."""

    captured: CapturedValues
    next_step_index: int
    chain_done: bool
    step_name: str
    upstream_status: int
    upstream_headers: dict[str, str]


@dataclass(frozen=True)
class FailedAuth:
    """Phantom-injected auth was rejected (401/403).

    Attributes:
        status: The upstream status, 401 or 403.
        observed_at: When the rejection was observed.
        blocked_host: The lower-cased host whose credential slot rejected this
            row, which is the CURRENT step's host and NOT necessarily
            ``row.endpoint`` (D2/F6). Both kickers key their freshness probe on
            it, so a wrong value here re-creates the livelock rather than
            merely mis-reporting it.
    """

    status: int
    observed_at: datetime
    blocked_host: str


@dataclass(frozen=True)
class Failed4xx:
    """Non-auth 4xx - terminal.

    The upstream response body is deliberately NOT carried (finding SL4-9).
    No consumer read it: the sender's arm formats ``4xx_status_{status}`` and
    nothing else touches the variant. Holding it was a latent disclosure
    vector rather than dead weight alone, because a 4xx body routinely echoes
    the request's own signature or credential identifiers and the field sat
    one format string away from ``last_error``, which the admin API surfaces.
    That is the hazard ``TemplateUnresolved`` and ``CaptureNotRenderable``
    were restructured to make structurally impossible; this is the same rule
    applied to the one variant that carried upstream bytes.
    """

    status: int


@dataclass(frozen=True)
class Failed5xx:
    """5xx - retry-eligible."""

    status: int


@dataclass(frozen=True)
class FailedNetwork:
    """Transport-class failure (timeout, connect refused)."""

    error: str


@dataclass(frozen=True)
class CaptureExpiredStored:
    """A referenced capture's TTL elapsed; row should transition to ``stored``."""

    producing_step: str


@dataclass(frozen=True)
class CaptureExpiredRewind:
    """ADR-011 reexecute=True - caller rewinds to ``rewind_to_step_index`` and re-queues."""

    producing_step: str
    rewind_to_step_index: int


@dataclass(frozen=True)
class TemplateUnresolved:
    """A ``{{step.capture}}`` placeholder did not resolve at execution time.

    Carries IDENTIFIERS ONLY, never template text. ``step.url`` and header
    values are producer-controlled and, since F4 preserves the raw-intake
    query string, a URL can carry a presigned ``X-Amz-Signature`` and
    ``X-Amz-Credential``. This variant is rendered verbatim into
    ``last_error``, which ``GET /v1/admin/chains/{chain_id}`` surfaces and the
    logs echo, so putting the template into it is a credential disclosure. The
    same rule and the same reasoning as ``RouteUnresolved``'s token (F1).

    The ``body`` site is also reached by a ``ChainBodyRef`` whose bytes are
    absent, which is a missing-body condition rather than a template failure;
    it would render an EMPTY name list. F2's declared-versus-returned check in
    the sender raises ``BodyMissingError`` before ``execute_one_step`` is
    called, so that arm is unreachable for a real row. If a path is later found
    that still reaches it, give the missing-ref case its own ``site`` member
    rather than letting it borrow ``body``.

    Attributes:
        step_name: The step whose template did not resolve. Regex-constrained
            to ``^[a-z][a-z0-9_]*$`` by ``ChainStep``, so it cannot carry a
            URL fragment.
        site: WHICH PART of the step failed, as a closed ``Literal``:
            ``"url"``, ``"header"`` or ``"body"``. A closed member rather than
            a formatted string, per the repo's enums-not-strings rule.
        unresolved: The ``producing_step.capture_name`` pairs the failing
            template references, in template order. Both halves are bounded by
            ``_PLACEHOLDER_RE``'s ``[a-z][a-z0-9_]*`` groups, so neither can
            carry URL text.
        header_name: The header whose value failed, and ``None`` for the
            ``url`` and ``body`` sites. Safe to include because admission
            rejects any header name that is not an RFC 7230 token
            (``routes/admission.py`` header-name validation), so it contains
            no ``?``, ``&``, ``/`` or whitespace. The header VALUE is never
            included.
    """

    step_name: str
    site: Literal["url", "header", "body"]
    unresolved: tuple[str, ...]
    header_name: str | None = None

    def token(self) -> str:
        """Render the operator-visible ``last_error`` payload.

        THE single formatter. Putting it on the variant rather than in the
        sender makes the redaction guarantee total: no consumer anywhere can
        format this type a different way, which is the same reasoning that
        put the guarantee in the type instead of in the callers.

        Returns:
            ``"<step>:<site>:<names>"``, with the header site rendered as
            ``header[<name>]``. Examples:
            ``upload:url:b.c``,
            ``upload:header[Authorization]:login.token``,
            ``upload:body:``.
        """
        where = f"header[{self.header_name}]" if self.site == "header" else self.site
        return f"{self.step_name}:{where}:{','.join(self.unresolved)}"


@dataclass(frozen=True)
class CaptureNotRenderable:
    """A capture cannot be rendered into the context its placeholder sits in (F8).

    Three reasons, and all are terminal. A NON-SCALAR (a captured object or
    array) can only be delivered as structure, and only one position offers
    that: a JSON body node that is exactly one placeholder and is not a key.
    Anywhere else, the result has to be a string, and the old path spliced a
    PYTHON REPR (``{'a': 1}``) into it, which the upstream accepted as
    plausible data. A CONTROL CHARACTER (CR, LF or NUL) in a captured header
    value cannot be sent at all: ``h11`` refuses to build the request, httpx
    surfaces that as an ``HTTPError``, the executor classified it
    ``FailedNetwork`` and the row burned its whole retry budget on a request
    that could never exist. An UNENCODABLE value is the worse sibling of that
    one: a lone UTF-16 surrogate, which ``json.loads`` accepts and both
    ``json.dumps`` and SQLite round-trip happily, makes httpx raise
    ``UnicodeEncodeError`` - a ``ValueError`` that is not in the httpx
    exception hierarchy, so nothing between here and the supervising TaskGroup
    catches it and the process crash-loops on the row forever.

    Terminal ``failed`` rather than retryable, because a retry is provably
    futile: the capture is already persisted, so re-rendering it produces the
    same refusal.

    Raised from TWO points. At RENDER time, when a persisted capture is about
    to be spliced into this step's URL, header or text body. And at CAPTURE
    time, for the ``unencodable`` reason only, when the value is extracted from
    an upstream response - the earlier point, chosen so the landmine never
    reaches the database at all, with the render-time gate keeping its job of
    clearing rows that were persisted before this guard existed.

    Carries IDENTIFIERS ONLY, never the captured value, and that is a proven
    property rather than a hope. ``TemplateUnresolved`` carries the same rule
    because ``last_error`` reached an admin surface holding a presigned URL; a
    capture is UPSTREAM RESPONSE DATA, which is the same hazard class and can
    be worse. Every field below is an identifier or a closed literal.

    Attributes:
        step_name: The CONSUMING step, whose template holds the placeholder,
            or - for a capture-time refusal - the PRODUCING step whose response
            the value came out of. Regex-constrained to ``^[a-z][a-z0-9_]*$``
            by ``ChainStep`` either way.
        site: WHICH PART of the step refused, as a closed ``Literal``, the
            same three members ``TemplateUnresolved`` uses. A capture-time
            refusal reports ``body``, the response body it was extracted from.
        placeholder: ``producing_step.capture_name``. Both halves are bounded
            by ``_PLACEHOLDER_RE``'s ``[a-z][a-z0-9_]*`` groups, so neither
            can carry URL or JSON text. A capture-time refusal names the
            capture the same way, so the operator token reads identically.
        reason: Which rule refused, as a closed ``Literal``.
        header_name: The header whose value refused, and ``None`` for the
            ``url`` and ``body`` sites. Safe for the same reason
            ``TemplateUnresolved``'s is: admission rejects any header name
            that is not an RFC 7230 token. The header VALUE is never included.
    """

    step_name: str
    site: Literal["url", "header", "body"]
    placeholder: str
    reason: Literal["non_scalar", "control_character", "unencodable"]
    header_name: str | None = None

    def token(self) -> str:
        """Render the operator-visible ``last_error`` payload.

        THE single formatter, on the variant rather than in the sender for the
        same reason ``TemplateUnresolved``'s is: no consumer anywhere can
        format this type a different way, so the redaction guarantee is total.

        Returns:
            ``"<step>:<site>:<placeholder>:<reason>"``, with the header site
            rendered as ``header[<name>]``. Examples:
            ``upload:body:login.meta:non_scalar``,
            ``upload:header[X-Trace]:login.tag:control_character``.
        """
        where = f"header[{self.header_name}]" if self.site == "header" else self.site
        return f"{self.step_name}:{where}:{self.placeholder}:{self.reason}"


@dataclass(frozen=True)
class CaptureIncomplete:
    """A 2xx response was MISSING a capture a later step requires (finding R7-5-B).

    The step returned a success status, but a capture this step DECLARES - and
    that a downstream step REFERENCES via ``{{step.capture}}`` - extracted to
    ``None`` (the response body was truncated / incomplete / a buggy proxy
    returned 2xx before the body was whole - the D-05/D-13/PAR "don't-trust"
    hazard). Advancing the step on the 2xx alone would strand the downstream
    step on an unresolvable template forever (the row wedges in ``attempting``
    holding a saturation slot). So this is classified as a RETRYABLE failure:
    the same step re-runs (and produces the captures once the upstream returns a
    complete body), bounded by the chain's max-attempts (then → ``stored``).
    """

    upstream_status: int
    missing_captures: tuple[str, ...]


@dataclass(frozen=True)
class SendDeadlineExpired:
    """The per-route send-deadline elapsed (now - received_at > deadline).

    Strategy-agnostic give-up backstop (ADR-032): the row has been trying past
    its route's ``send_deadline_seconds`` ceiling, so it transitions to the
    terminal ``expired`` state (body released, never re-admitted). Returned by
    :meth:`ChainExecutor._check_send_deadline` for a claimed/``attempting`` row;
    the sender routes it to the shared ``expire_row`` writer.
    """

    deadline_seconds: int


@dataclass(frozen=True)
class RouteUnresolved:
    """No configured route matches this step's absolute URL.

    Admission route-checks only the FIRST step's URL and records
    ``route_name='unknown'`` on a miss (``routes/admission.py``), so a chain
    whose LATER step targets a host with no ``RouteCfg`` is durably admitted
    with a 202 and only discovers the miss here, at send time.
    ``resolve_route`` raises ``ValueError`` on no match. Letting that escape
    kills the sender's TaskGroup and, through the composition root, the
    process; recovery then re-claims this row first on every restart, so the
    service crash-loops and the whole backlog is stranded. Classifying the
    miss lets the sender park the row instead.

    Attributes:
        host: The lower-cased hostname that matched no route. The operator
            needs exactly this to repair the instance's route config.
        step_name: The chain step whose URL carried that host.
    """

    host: str
    step_name: str


@dataclass(frozen=True)
class InlineBodyInvalid:
    """A step's inline base64 body cannot be decoded (N1).

    Admission rejects malformed ``value_b64`` with a 422 since N1, so this
    classifies rows admitted before that guard existed, or through any other
    insertion path: ``envelope_from_persistence_json`` re-validates the
    envelope's SHAPE but not its base64. Terminal, because the payload is
    producer data that can never become valid; no operator action and no
    replay can fix it.

    Attributes:
        step_name: The step whose inline body failed to decode.
        reason: The decoder's own description of the defect. Logged, not
            persisted, so ``last_error`` stays a short stable token.
    """

    step_name: str
    reason: str


ExecuteStepResult = (
    Succeeded
    | FailedAuth
    | Failed4xx
    | Failed5xx
    | FailedNetwork
    | CaptureExpiredStored
    | CaptureExpiredRewind
    | TemplateUnresolved
    | CaptureNotRenderable
    | CaptureIncomplete
    | SendDeadlineExpired
    | RouteUnresolved
    | InlineBodyInvalid
)


class ChainStepIndexError(Exception):
    """``current_step_index`` points past the end of the persisted chain.

    An EXCEPTION rather than an ``ExecuteStepResult`` member, deliberately
    (Q2). The state is unreachable through every writer of that column:
    admission writes 0, the sender's advance arm writes an index the executor
    already bounded with ``chain_done = step_index + 1 >= len(steps)``, the
    rewind arm writes a real ``enumerate`` index over the step list, and every
    other caller passes ``None``, which the store COALESCEs to the existing
    value. Adding a union member would cost a union entry, a sender arm, a
    facade export and a transition for a state that cannot occur.

    What it must NOT do is escape. ``_drive_one``'s caller re-raises anything
    that is not a transient lock error, so an unclassified raise here escapes
    the worker TaskGroup and kills the process; recovery then resets the row to
    ``queued`` and the next claim crashes again. That is F1's crash-loop shape,
    which is why an unreachable line still gets a guard. The sender catches
    this type alone and routes the row to ``corrupted``: the row's persisted
    index disagrees with its persisted envelope, which is row-level data
    inconsistency, and ADR-014's corrupted path is where those go.
    """


class ChainExecutor:
    """The primitive turning a :class:`ChainEnvelope` into upstream HTTP calls."""

    def __init__(
        self,
        *,
        token_cache: TokenCache,
        upstream_client: UpstreamClient,
        resolve_route: Callable[[str, InstanceCfg], ResolvedRoute],
        clock: Callable[[], datetime],
        instance: InstanceCfg,
        signer_creds: CredentialStore | None = None,
    ) -> None:
        """Construct the executor.

        Args:
            token_cache: ADR-002 cache for ``(endpoint, uid)`` lookups.
            upstream_client: Transport seam.
            resolve_route: Per-instance route policy lookup. The
                composition root binds this to
                :func:`phantom.routing.resolve_route`.
            clock: A callable returning a UTC ``datetime`` - injectable
                for deterministic tests.
            instance: The instance whose routes apply.
            signer_creds: OPTIONAL host-keyed destination-credential store for
                the ``aws_sigv4`` auth mode (the SigV4 analogue of
                ``token_cache``). ``None`` (the default) when no route uses
                ``aws_sigv4``; an ``aws_sigv4`` route resolved while this is
                ``None`` is treated as a missing credential - ``FailedAuth``
                that PARKS in ``auth_expired`` (NOT terminal). Defaulting to
                ``None`` keeps every existing construction site unchanged.
        """
        self._cache = token_cache
        self._client = upstream_client
        self._resolve_route = resolve_route
        self._clock = clock
        self._instance = instance
        self._signer_creds = signer_creds

    async def execute_one_step(
        self,
        row: UploadRow,
        body_refs: dict[str, bytes],
    ) -> ExecuteStepResult:
        """Run one step of ``row``'s chain and classify the result.

        The auth stage (c) resolves ONE provider from the route's
        ``auth_mode`` and asks it to prepare the request. A provider either
        readies it (mutating the header dict in place and reporting the URL to
        send, which the sigv4 provider rewrites) or reports that the row must
        park, carrying the status and the sanitised blocked host that
        ``FailedAuth`` persists. The same provider marks its slot bad if the
        upstream then answers 401 or 403.

        Args:
            row: The claimed upload row whose current step to run.
            body_refs: The rehydrated body bytes, keyed by ref name.

        Returns:
            One member of the :data:`ExecuteStepResult` union.

        Raises:
            ChainStepIndexError: When the row's persisted
                ``current_step_index`` points past the end of its persisted
                envelope. Unreachable through any writer of that column; the
                sender catches it and routes the row to ``corrupted`` so an
                impossible state cannot crash-loop the process (Q2).
        """
        envelope = envelope_from_persistence_json(row.chain_envelope_json)
        step_index = row.current_step_index
        if step_index >= len(envelope.steps):
            raise ChainStepIndexError(
                f"current_step_index {step_index} past end of chain (steps={len(envelope.steps)})",
            )
        step = envelope.steps[step_index]

        # (a) Capture-TTL gate (ADR-011).
        ttl_check = self._check_capture_ttl(step, row, envelope)
        if ttl_check is not None:
            return ttl_check

        # (b) Substitute placeholders in URL, headers, body. A chain marked
        # ``templated=False`` (N3) passes every template through verbatim.
        # Each text site runs the F8 type gate FIRST: outside a JSON body
        # there is no serializer to make correct output structural, so a
        # capture that cannot be spliced as text is refused rather than
        # rendered as a Python repr.
        values = self._captures_as_dict(row.captured_values)
        url_refusal = self._first_unrenderable(
            step.url,
            values,
            templated=envelope.templated,
            step_name=step.name,
            site="url",
        )
        if url_refusal is not None:
            return url_refusal
        substituted_url, ok = self._substitute_or_literal(
            step.url, values, templated=envelope.templated
        )
        if not ok:
            return TemplateUnresolved(
                step_name=step.name, site="url", unresolved=_placeholder_names(step.url)
            )
        substituted_headers: dict[str, str] = {}
        hop_by_hop = _hop_by_hop_names(step.headers)
        for name, value in step.headers.items():
            lowered = name.lower()
            # Phantom's reserved header namespace (``X-Phantom-*``)
            # must not be forwarded to upstream - transparent-proxy
            # invariant. Strip case-insensitively.
            if lowered.startswith(_PHANTOM_RESERVED_HEADER_PREFIX):
                logger.debug("stripping reserved phantom header from upstream: %s", name)
                continue
            if lowered in hop_by_hop:
                # F9: framing and connection-scoped headers describe the hop
                # Phantom terminated, not the message it forwards. A persisted
                # ``Transfer-Encoding: chunked`` would make h11 emit chunked
                # framing over a fixed-length body on EVERY retry.
                logger.debug("stripping hop-by-hop header from upstream: %s", name)
                continue
            header_refusal = self._first_unrenderable(
                value,
                values,
                templated=envelope.templated,
                step_name=step.name,
                site="header",
                header_name=name,
            )
            if header_refusal is not None:
                return header_refusal
            rendered, header_ok = self._substitute_or_literal(
                value, values, templated=envelope.templated
            )
            if not header_ok:
                return TemplateUnresolved(
                    step_name=step.name,
                    site="header",
                    unresolved=_placeholder_names(value),
                    header_name=name,
                )
            substituted_headers[name] = rendered

        try:
            body_bytes, body_content_type, sub_ok, body_refusal = self._render_body(
                step, values, body_refs, templated=envelope.templated
            )
        except InlineBodyDecodeError as exc:
            logger.warning(
                "step %r of chain_id=%s carries undecodable inline base64 (%s); "
                "terminating the row as failed",
                exc.step_name,
                row.chain_id,
                exc.reason,
            )
            return InlineBodyInvalid(step_name=exc.step_name, reason=exc.reason)
        # The refusal arm PRECEDES the unresolved arm, and the order is
        # load-bearing: a body can be both (one placeholder missing, another
        # non-scalar) and the refusal is the more specific diagnosis.
        if body_refusal is not None:
            return body_refusal
        if not sub_ok:
            body_template = self._body_as_template(step)
            return TemplateUnresolved(
                step_name=step.name,
                site="body",
                unresolved=_placeholder_names(body_template) if body_template else (),
            )
        # A CASE-INSENSITIVE presence test. The keys of ``substituted_headers``
        # carry the producer's original casing, which on every ASGI path is
        # lower-case, so ``"Content-Type" not in substituted_headers`` missed an
        # existing ``content-type: image/jpeg`` and stamped a second key beside
        # it. ``_synthesize_envelope`` builds its ``ChainBodyRef`` with no
        # content_type, so ``body_content_type`` takes the model default and
        # every raw-intake upload that declared one shipped BOTH: S3 recombines
        # them to ``image/jpeg,application/octet-stream``, answers 403
        # SignatureDoesNotMatch, and the sender terminates the row ``failed``.
        if body_content_type is not None and not has_header(substituted_headers, "Content-Type"):
            set_header(substituted_headers, "Content-Type", body_content_type)

        # (b2) Idempotency header, injected HERE rather than after the auth
        # stage. The old position was after BOTH the hop-by-hop strip above and
        # the sigv4 signer, which broke two guarantees at once. On an
        # ``aws_sigv4`` route ``provider.prepare`` has already rebuilt the map
        # from botocore's signed view, so a step declaring
        # ``idempotency_header: "x-amz-meta-idempotency"`` shipped an x-amz-*
        # header absent from ``SignedHeaders`` and S3 answered 403; the executor
        # then marked the host's credential slot bad, parking every row for that
        # destination. And on ANY route the name is producer-supplied and only
        # checked for RFC 7230 token characters, so ``Host`` and
        # ``Content-Length`` were both admissible and both bypassed the strip
        # entirely - verified emitting ``Content-Length: 3`` over an 11-byte body
        # and a rewritten ``Host``, on every attempt, because the header is
        # persisted. Injecting before the auth stage puts the header inside the
        # signature and under the same strip rule as any other.
        self._inject_idempotency_header(step, envelope, substituted_headers, hop_by_hop)

        full_url = self._absolute_url(substituted_url, envelope)

        # Resolve the route first: it decides the auth provider at (c) below,
        # and the send-deadline gate at (a') reads its deadline.
        #
        # The handler must not be able to raise out of itself. It used to call
        # ``sanitised_host_for`` on the SAME url whose parse had just raised,
        # and the old ``sanitised_host_for`` used ``urlparse``, whose
        # ``.hostname`` raises ``ValueError('Invalid IPv6 URL')``. Admission
        # route-checks only the FIRST step, so a later step's URL is never
        # validated: a step 2 URL of ``https://{{s1.host}}/obj`` with an
        # ``s1.host`` capture of ``[bad`` renders ``https://[bad/obj``,
        # ``_resolve_route`` raised, and the handler below raised again UNCAUGHT
        # into a sender that catches four storage and index types and a loop
        # that catches ``sqlite3.OperationalError``, cancelling every sender
        # worker in the TaskGroup and stopping delivery service-wide with this
        # row still in ``attempting`` holding its saturation slot.
        # ``sanitised_host_for`` is now total (it swallows every parse failure
        # into the ``<no-host>`` token), and ``resolve_route`` refuses a URL
        # with no dialable host rather than keying it by its own path, so an
        # unparseable or relative URL lands here as a clean ``RouteUnresolved``.
        try:
            resolved = self._resolve_route(full_url, self._instance)
        except ValueError:
            # Sanitised host for the persisted token. NOT ``host_key_for``: that
            # helper (``phantom.routing``) returns the WHOLE INPUT when it finds
            # no host, and a step URL legitimately can be a bare path, so
            # reusing it would splice the path and its query string into
            # ``last_error``. ``sanitised_host_for`` is the form that may be
            # persisted, and both docstrings state the split.
            unrouted_host = sanitised_host_for(full_url)
            logger.warning(
                "no route matches host %s for step %r of chain_id=%s; "
                "parking the row in 'stored' for operator repair",
                unrouted_host,
                step.name,
                row.chain_id,
            )
            return RouteUnresolved(host=unrouted_host, step_name=step.name)

        # (a') Send-deadline gate (ADR-032) - the give-up backstop, independent
        # of the retry strategy. Placed here (after ``resolved`` exists, before
        # any signing/sending work) because the capture-TTL gate (a) at the top
        # of this method runs BEFORE ``resolved`` is computed and the deadline is
        # a per-route property. Checked once per attempt against the cheapest
        # correct point that has ``resolved``.
        deadline_check = self._check_send_deadline(row, resolved)
        if deadline_check is not None:
            return deadline_check

        # (c) Inject auth via the route's provider (U8). ONE selection over
        # three providers replaces two if/elif blocks that sat 96 lines apart,
        # and the ``match`` inside the selector keeps the guarantee the two
        # blocks had: a fourth auth mode with no case is a mypy error at
        # ``assert_never``, not a silent no-auth fall-through (the pre-ADR-012
        # bare ``if`` would have behaved like ``none``).
        provider = self._auth_provider_for(resolved.auth_mode)
        outcome = await provider.prepare(
            full_url=full_url,
            uid=row.uid,
            method=step.method,
            headers=substituted_headers,
            body=body_bytes,
            chain_id=row.chain_id,
        )
        if isinstance(outcome, AuthParked):
            # The row parks in ``auth_expired`` carrying the SANITISED host the
            # provider resolved (D2/F6), never a raw URL.
            return FailedAuth(
                status=outcome.status,
                observed_at=self._clock(),
                blocked_host=outcome.blocked_host,
            )
        # REBIND: the sigv4 provider signs the presigned-stripped URL, and the
        # URL that was signed is the URL that must be sent. Every other
        # provider hands back what it was given.
        full_url = outcome.url

        # (e) Send via the transport. The resolved route may carry a
        # per-route timeout (§5.2) - pass it through; None falls back to
        # the upstream client's constructor default.
        request = UpstreamRequest(
            method=step.method,
            url=full_url,
            headers=substituted_headers,
            body=body_bytes,
            timeout_seconds=resolved.timeout_seconds,
        )
        try:
            response = await self._client.send(request)
        except httpx.HTTPError as exc:
            return FailedNetwork(error=str(exc))

        # (f) Classify.
        if 200 <= response.status < 300:
            extracted = await self._extract_captures(step, response.body, row.captured_values)
            if isinstance(extracted, CaptureNotRenderable):
                # The step SUCCEEDED upstream and a capture it declared cannot
                # be encoded onto any wire. Terminal, and refused here so the
                # value is never written to the row: persisting it is what
                # turns one bad response into a permanent crash-loop.
                logger.warning(
                    "step %r of chain_id=%s returned %d but captured a value that "
                    "cannot be encoded (%s); terminating the row rather than "
                    "persisting it",
                    step.name,
                    row.chain_id,
                    response.status,
                    extracted.token(),
                )
                return extracted
            new_captures = extracted
            # finding R7-5-B: do NOT advance on the 2xx status ALONE. A 2xx
            # with a truncated/incomplete body (D-05/D-13, a CGNAT half-close,
            # a buggy proxy that returns 2xx before the body is whole) leaves
            # this step's declared captures unextracted. If a later step
            # REFERENCES one of those captures (``{{step.capture}}``), advancing
            # would strand it on an unresolvable ``None`` template FOREVER (the
            # row wedges in ``attempting`` holding a saturation slot - invariant
            # #1 bent). Validate the required captures were actually produced;
            # a missing one is a RETRYABLE failure, not a silent advance.
            missing = self._missing_required_captures(step, envelope, new_captures)
            if missing:
                logger.warning(
                    "step %r returned %d but is MISSING required capture(s) %s "
                    "(2xx with incomplete/truncated body); treating as retryable "
                    "rather than advancing into an unresolvable downstream step",
                    step.name,
                    response.status,
                    missing,
                )
                return CaptureIncomplete(
                    upstream_status=response.status,
                    missing_captures=missing,
                )
            chain_done = step_index + 1 >= len(envelope.steps)
            return Succeeded(
                captured=new_captures,
                next_step_index=step_index + 1,
                chain_done=chain_done,
                step_name=step.name,
                upstream_status=response.status,
                upstream_headers=response.headers,
            )
        if response.status in {401, 403}:
            # The sanitised persisted host again (D2/F6). Recomputed rather
            # than reused: this return is SHARED by all three ``auth_mode``
            # arms, and the ``none`` arm computed nothing. That arm is also the
            # one worth naming: a 401 from a route Phantom never authenticated
            # records a blocked host for a slot that does not exist, so the row
            # parks and no kicker owns it. That park is pre-existing behaviour
            # which D2 makes visible in the column rather than changes.
            blocked = sanitised_host_for(full_url)
            # Mark THIS route's slot bad so the sender knows what to do. The
            # ``none`` provider holds no slot and no-ops, which is what the
            # inline chain did by having no arm for it.
            #
            # FENCED on the credential this request was actually sent with.
            # The rejection arrives a full network round trip after prepare
            # read the slot, and an operator pushes a replacement precisely when
            # requests are failing, so without the fence the push most likely to
            # be clobbered was the one meant to end the outage.
            await provider.mark_bad(
                host_key=host_key_for(full_url),
                uid=row.uid,
                slot_observed_at=outcome.slot_observed_at,
            )
            return FailedAuth(
                status=response.status, observed_at=self._clock(), blocked_host=blocked
            )
        if 400 <= response.status < 500:
            return Failed4xx(status=response.status)
        if response.status >= 500:
            return Failed5xx(status=response.status)
        return FailedNetwork(error=f"Unexpected status {response.status}")

    @staticmethod
    def _inject_idempotency_header(
        step: ChainStep,
        envelope: ChainEnvelope,
        headers: dict[str, str],
        hop_by_hop: frozenset[str],
    ) -> None:
        """Write the step's ``idempotency_header``, refusing a name that cannot carry it.

        ``idempotency_header`` is producer-supplied and admission validates only
        that it is made of RFC 7230 token characters (``_validate_step_headers``
        does not look at this field at all), so ``Host``, ``Content-Length``,
        ``Transfer-Encoding``, anything the step's own ``Connection`` names, and
        the reserved ``X-Phantom-*`` namespace are all admissible spellings.
        Each of them is a name Phantom has already decided must not reach the
        upstream, and a persisted one reproduced its damage on every attempt.

        A colliding name is DROPPED, at WARNING, which is the same verdict the
        loop above reaches for a hop-by-hop name in ``step.headers`` - one rule
        for one class of header, rather than a second rule that happens to be
        reachable through a different field. The consequence is that the step
        sends with no idempotency header, so an upstream retry may duplicate;
        the alternative was emitting a ``Content-Length`` that contradicts the
        body or a ``Host`` that redirects the request, which defeats the two
        guarantees the hop-by-hop set exists to hold. Refusing the ENVELOPE at
        admission with a 422 is the better place for this and needs an owner of
        ``routes/admission.py``.

        The write itself goes through
        :func:`~phantom.chain.headers.set_header`, so a step that both declares
        ``idempotency_header: "X-Idem"`` and carries its own ``x-idem`` header
        sends one, not two.

        Args:
            step: The step being executed.
            envelope: The chain, for ``idempotency_key``.
            headers: The outbound header map, mutated in place.
            hop_by_hop: This request's hop-by-hop set, from
                :func:`_hop_by_hop_names` (the static set plus whatever this
                step's ``Connection`` named).
        """
        name = step.idempotency_header
        if not name:
            return
        lowered = name.lower()
        if lowered in hop_by_hop or lowered.startswith(_PHANTOM_RESERVED_HEADER_PREFIX):
            logger.warning(
                "step %r declares idempotency_header %r, which is a hop-by-hop, framing "
                "or Phantom-reserved name; dropping it rather than letting it bypass the "
                "strip. The step will send with no idempotency header",
                step.name,
                name,
            )
            return
        set_header(headers, name, envelope.idempotency_key)

    def _auth_provider_for(self, auth_mode: AuthMode) -> AuthSlotProvider:
        """Pick this route's auth provider, exhaustively over ``auth_mode``.

        A ``match`` rather than a mapping, deliberately: a
        ``dict[AuthMode, AuthSlotProvider]`` gives mypy no exhaustiveness check
        (it does not verify that a literal covers every member of a Literal key
        type, and a subscript narrows nothing), so adding a fourth auth mode
        would become a runtime ``KeyError`` instead of the type error this
        dispatch has always been.

        Args:
            auth_mode: The resolved route's outbound-auth selector.

        Returns:
            The provider that prepares and marks-bad for that mode.
        """
        match auth_mode:
            case "phantom_bearer":
                return BearerAuthProvider(cache=self._cache)
            case "aws_sigv4":
                return SigV4AuthProvider(store=self._signer_creds)
            case "none":
                return NoAuthProvider()
            case _:  # pragma: no cover - exhaustive over the Literal above.
                assert_never(auth_mode)

    def _check_send_deadline(
        self,
        row: UploadRow,
        resolved: ResolvedRoute,
    ) -> SendDeadlineExpired | None:
        """Return ``SendDeadlineExpired`` when the row has been trying past its deadline.

        The deadline is wall-time since admission (``row.received_at``).
        Strategy-agnostic by design (ADR-032): ``fixed_intervals`` discards
        ``since_received`` entirely (``strategies/fixed_intervals.py``) and would
        otherwise retry forever, so this executor-side gate is the belt to the
        strategy's suspenders. ``None`` route deadline = no ceiling (the safe
        default that changes no existing behaviour).

        Args:
            row: The claimed/``attempting`` row being executed.
            resolved: The resolved route policy (carries ``send_deadline_seconds``).

        Returns:
            ``SendDeadlineExpired`` when ``now - received_at`` exceeds the route's
            ``send_deadline_seconds``; otherwise ``None``.
        """
        deadline = resolved.send_deadline_seconds
        if deadline is None:
            return None
        if (self._clock() - row.received_at).total_seconds() > deadline:
            return SendDeadlineExpired(deadline_seconds=deadline)
        return None

    def _check_capture_ttl(
        self,
        step: ChainStep,
        row: UploadRow,
        envelope: ChainEnvelope,
    ) -> CaptureExpiredStored | CaptureExpiredRewind | None:
        """Walk every placeholder this step references; gate on capture TTLs.

        If any referenced capture is missing/expired:
          - ``row.capture_reexecution_active is False`` → ``CaptureExpiredStored``.
          - True → rewind to the producing step (``CaptureExpiredRewind``).

        A chain marked ``templated=False`` (N3) returns ``None`` immediately:
        its brace spans are content, not capture references, so reading them
        as expired ones would be wrong. This gate already no-ops for every
        synthesized raw-intake chain (which carries no captured values at
        all), so the early return is about keeping the marker's meaning
        coherent, namely that no brace span is INTERPRETED anywhere.
        """
        if not envelope.templated:
            return None
        placeholders: list[tuple[str, str]] = []
        placeholders.extend(find_placeholders(step.url))
        for v in step.headers.values():
            placeholders.extend(find_placeholders(v))
        body_template = self._body_as_template(step)
        if body_template:
            placeholders.extend(find_placeholders(body_template))
        if not placeholders:
            return None
        now = self._clock()
        for producing_step, capture_name in placeholders:
            step_values = row.captured_values.steps.get(producing_step)
            if step_values is None or capture_name not in step_values.values:
                # Not yet captured - substitute will detect; not a TTL miss.
                continue
            expires_at = step_values.expires_at.get(capture_name)
            if expires_at is None:
                continue  # non-expiring capture
            if expires_at > now:
                continue  # still valid
            # Expired.
            if row.capture_reexecution_active:
                # Find the index of the producing step in the envelope.
                rewind_index = next(
                    (i for i, s in enumerate(envelope.steps) if s.name == producing_step),
                    None,
                )
                if rewind_index is None:
                    # Producing step missing from envelope - fall back to stored.
                    return CaptureExpiredStored(producing_step=producing_step)
                return CaptureExpiredRewind(
                    producing_step=producing_step,
                    rewind_to_step_index=rewind_index,
                )
            return CaptureExpiredStored(producing_step=producing_step)
        return None

    @staticmethod
    def _substitute_or_literal(
        template: str, values: dict[str, dict[str, Any]], *, templated: bool
    ) -> tuple[str, bool]:
        """Substitute placeholders, or pass the text through for a literal chain.

        The ONE place the ``ChainEnvelope.templated`` flag is honoured. A
        literal chain (N3) resolves to ``(template, True)``: a brace span in a
        raw-intake object key is content, not a capture reference, and treating
        it as one terminated a valid upload as ``failed`` with a template error
        it never had a template for.

        Takes the FLATTENED captures, like its sibling
        :meth:`_first_unrenderable` (finding SL4-8). It used to take
        :class:`CapturedValues` and re-flatten the whole structure on every
        call, so one step attempt rebuilt an identical dict-of-dicts once per
        header and once per string node of a JSON body, on the event loop,
        while every call site already held the flattened view.

        Args:
            template: The raw text, which may contain ``{{step.capture}}``.
            values: The flattened captures, ``step -> {capture: value}``.
            templated: The envelope's marker. ``False`` short-circuits.

        Returns:
            ``(rendered_or_verbatim, all_resolved)``.
        """
        if not templated:
            return template, True
        return substitute(template, values)

    @staticmethod
    def _first_unrenderable(
        template: str,
        values: dict[str, dict[str, Any]],
        *,
        templated: bool,
        step_name: str,
        site: Literal["url", "header", "body"],
        header_name: str | None = None,
    ) -> CaptureNotRenderable | None:
        """Return the first placeholder whose value cannot be spliced as TEXT.

        THE ONE type gate (F8), used by the URL site, the header site, the
        text-body arm and the JSON walker's text path. A type gate rather than
        an escaper, deliberately: outside a JSON body there is no serializer to
        make correct output structural, and percent-encoding the URL would
        break working chains (a capture holding a path segment ``a/b`` is a
        legitimate template use that ``a%2Fb`` would destroy). The producer
        wrote the template and owns its encoding; Phantom's job is to refuse
        values that cannot be spliced at all.

        A literal chain (N3) has nothing to refuse because nothing is
        substituted, so the flag is honoured HERE and the three non-JSON call
        sites need no conditional of their own.

        Args:
            template: The raw text about to be substituted.
            values: The flattened captures, ``step -> {capture: value}``.
            templated: The envelope's marker. ``False`` refuses nothing.
            step_name: The consuming step, for the returned variant.
            site: Which part of the step is being rendered.
            header_name: The header under render, for the ``header`` site.

        Returns:
            The refusal, or ``None`` when every referenced capture renders.
        """
        if not templated:
            return None
        for producing, name in find_placeholders(template):
            step_values = values.get(producing)
            if step_values is None or name not in step_values:
                continue  # unresolved is TemplateUnresolved's business, not ours
            value = step_values[name]
            reason: Literal["non_scalar", "control_character", "unencodable"]
            if not (value is None or isinstance(value, _TEXT_SCALARS)):
                reason = "non_scalar"
            elif site == "header" and isinstance(value, str) and _CONTROL_CHARS.search(value):
                reason = "control_character"
            elif isinstance(value, str) and not _wire_encodable(value, site=site):
                # The catch-up arm for rows already holding a poisoned capture.
                # ``_extract_captures`` refuses an unencodable value before it
                # is persisted, but a row admitted before that guard existed
                # still carries one, and it is exactly that row that recovery
                # resets to ``queued`` and re-claims on every restart. Refusing
                # here turns the crash-loop into one terminal row.
                reason = "unencodable"
            else:
                continue
            return CaptureNotRenderable(
                step_name=step_name,
                site=site,
                placeholder=f"{producing}.{name}",
                reason=reason,
                header_name=header_name,
            )
        return None

    def _render_json_body(
        self,
        value: dict[str, Any],
        values: dict[str, dict[str, Any]],
        *,
        templated: bool,
        step_name: str,
    ) -> tuple[dict[str, Any], bool, CaptureNotRenderable | None]:
        """Substitute placeholders INSIDE the parsed body, before serialization.

        The F8 fix. ``ChainBodyJson.value`` is already a parsed ``dict``, so
        the rule is to never leave that form until the bytes are produced. The
        CALLER serializes, which is what makes malformed output impossible:
        every string this returns is escaped by ``json.dumps``, not by us. An
        escaper has to be correct for every input; a serializer is correct by
        construction.

        Three placeholder positions, and only one of them can take structure.
        A WHOLE-VALUE node (exactly one placeholder, not a key) takes a
        captured ``dict`` or ``list`` as real structure. An EMBEDDED node (any
        other text around it, or two adjacent placeholders) must produce a
        string, so a non-scalar is refused. A KEY is a string position that can
        NEVER take structure: Python raises ``TypeError: unhashable type`` at
        dict construction, which would escape into a sender that catches only
        ``sqlite3.OperationalError`` and crash-loop the service, on an input
        that succeeds today. ``is_key`` is threaded through the walk for that
        one reason.

        Every SCALAR falls through to the text path, including in the
        whole-value position, so a captured number, boolean or null renders
        through ``str()`` byte-identically to the old path. Type preservation
        is deliberately NOT implemented: whether an upstream wants ``7`` or
        ``"7"`` is a schema question Phantom cannot see, so F8 fixes what is
        broken and leaves what works.

        Args:
            value: The parsed JSON body from the envelope.
            values: The flattened captures, ``step -> {capture: value}``,
                computed once by the caller (SL4-8).
            templated: The envelope's ``templated`` marker (N3).
            step_name: The consuming step, for any refusal raised.

        Returns:
            ``(substituted, all_resolved, refusal_or_None)``.
        """
        if not templated:
            return value, True, None  # N3's rule, applied once to the whole body
        resolved = True

        def walk(node: Any, *, is_key: bool) -> tuple[Any, CaptureNotRenderable | None]:
            """Rebuild ``node`` with every placeholder resolved in place."""
            nonlocal resolved
            if isinstance(node, dict):
                out: dict[str, Any] = {}
                for raw_key, raw_value in node.items():
                    new_key, bad = walk(raw_key, is_key=True)
                    if bad is not None:
                        return None, bad
                    new_value, bad = walk(raw_value, is_key=False)
                    if bad is not None:
                        return None, bad
                    out[new_key] = new_value
                return out, None
            if isinstance(node, list):
                items: list[Any] = []
                for item in node:
                    new_item, bad = walk(item, is_key=False)
                    if bad is not None:
                        return None, bad
                    items.append(new_item)
                return items, None
            if not isinstance(node, str):
                return node, None
            whole = whole_placeholder(node)
            if whole is not None and not is_key:
                producing, name = whole
                step_values = values.get(producing)
                if step_values is None or name not in step_values:
                    resolved = False
                    return node, None  # TemplateUnresolved is the caller's answer
                whole_value = step_values[name]
                if isinstance(whole_value, dict | list):
                    return whole_value, None  # the ONLY type that changes shape
                # Every SCALAR falls through to the text path, so a captured
                # number, boolean or null renders through str() exactly as it
                # does today. The type-preservation restraint is enforced HERE.
            bad = self._first_unrenderable(
                node, values, templated=True, step_name=step_name, site="body"
            )
            if bad is not None:
                return None, bad
            rendered, ok = self._substitute_or_literal(node, values, templated=True)
            if not ok:
                resolved = False
            return rendered, None

        substituted, refusal = walk(value, is_key=False)
        if refusal is not None:
            return {}, False, refusal
        return substituted, resolved, None

    @staticmethod
    def _captures_as_dict(
        captured: CapturedValues,
    ) -> dict[str, dict[str, Any]]:
        """Flatten :class:`CapturedValues` into the view every render site takes.

        Called ONCE per step attempt, at the top of :meth:`execute_one_step`,
        and threaded from there through the URL, header and body sites
        (SL4-8). Nothing downstream re-derives it: a step attempt rebuilding
        an identical dict-of-dicts once per header and once per string node of
        a JSON body is pure event-loop time for byte-identical output.

        Args:
            captured: The chain's captured values.

        Returns:
            ``step -> {capture: value}``.
        """
        return {name: dict(step.values) for name, step in captured.steps.items()}

    @staticmethod
    def _body_as_template(step: ChainStep) -> str | None:
        """Return the body content as a string for placeholder NAME extraction.

        NOT a rendering path. Since F8 a JSON body is rendered by walking the
        PARSED structure and serializing once at the end, so nothing sends what
        this returns. It survives for the three callers that need the set of
        placeholder NAMES a body references and do not care about its bytes:
        the capture-TTL gate, the ``TemplateUnresolved`` body arm, and
        ``_missing_required_captures``'s scan of later steps. Do not re-add a
        caller that renders from this string; that ordering is the F8 defect.
        """
        if isinstance(step.body, ChainBodyText):
            return step.body.value
        if isinstance(step.body, ChainBodyJson):
            return json.dumps(step.body.value)
        return None

    def _render_body(
        self,
        step: ChainStep,
        values: dict[str, dict[str, Any]],
        body_refs: dict[str, bytes],
        *,
        templated: bool,
    ) -> tuple[bytes, str | None, bool, CaptureNotRenderable | None]:
        """Produce the outbound body bytes + Content-Type for ``step``.

        The JSON arm substitutes into the PARSED body and serializes LAST
        (F8), which is what makes malformed output impossible rather than
        escaped. Every other arm renders text or bytes and cannot refuse
        anything structurally, so each returns ``None`` as the fourth member.

        Args:
            step: The step whose body is being rendered.
            values: The flattened captures, ``step -> {capture: value}``,
                computed once by the caller (SL4-8).
            body_refs: The rehydrated body bytes, keyed by ref name.
            templated: The envelope's ``templated`` marker. ``_render_body``
                does not receive the envelope, so the flag is passed down; a
                literal chain (N3) forwards a brace span in a text or JSON
                body verbatim.

        Returns:
            ``(bytes, content_type_or_None, all_resolved, refusal_or_None)``.
            The fourth member is a :class:`CaptureNotRenderable` when a
            captured value cannot be rendered into the position its
            placeholder sits in; the caller returns it before the
            unresolved arm, because it is the more specific diagnosis.
        """
        if step.body is None:
            return b"", None, True, None
        if isinstance(step.body, ChainBodyJson):
            substituted, ok, bad = self._render_json_body(
                step.body.value, values, templated=templated, step_name=step.name
            )
            if bad is not None:
                return b"", None, False, bad
            if not ok:
                return b"", None, False, None
            # ``json.dumps`` runs LAST and it is the whole fix: every string
            # the walker produced is escaped by the serializer, so a quote, a
            # backslash, a newline or a control character in a captured value
            # cannot produce malformed output.
            return json.dumps(substituted).encode("utf-8"), "application/json", True, None
        if isinstance(step.body, ChainBodyText):
            refusal = self._first_unrenderable(
                step.body.value,
                values,
                templated=templated,
                step_name=step.name,
                site="body",
            )
            if refusal is not None:
                return b"", None, False, refusal
            rendered, ok = self._substitute_or_literal(step.body.value, values, templated=templated)
            if not ok:
                return b"", None, False, None
            return rendered.encode("utf-8"), step.body.content_type, True, None
        if isinstance(step.body, ChainBodyBytes):
            # Raises InlineBodyDecodeError, which execute_one_step classifies.
            # The parser owns the decode rule AND its exception taxonomy, so
            # this arm does not have to know which library exceptions the
            # decoder absorbs; it catches exactly one name.
            return (
                decode_inline_body_b64(step.body.value_b64, step_name=step.name),
                step.body.content_type,
                True,
                None,
            )
        if isinstance(step.body, ChainBodyRef):
            data = body_refs.get(step.body.name)
            if data is None:
                return b"", None, False, None
            return data, step.body.content_type, True, None
        return b"", None, True, None  # pragma: no cover: exhaustive above

    def _absolute_url(self, url: str, envelope: ChainEnvelope) -> str:
        """Resolve ``url`` against ``envelope.default_target`` if needed.

        The absolute test is :func:`~phantom.routing.is_absolute_url`, anchored
        at position zero, and NOT the ``"://" in url`` substring search it
        replaces - which is the identical defect
        :func:`~phantom.routing.resolve_first_step_url` carried. A relative step
        whose QUERY holds a URL value (``/upload?callback=https://cb.example/done``)
        answered True, so ``default_target`` was skipped and the pathless URL
        went on to the route gate, was keyed there as its own path-plus-query,
        fnmatched a ``hosts: ['*']`` catch-all, collected injected auth and
        reached httpx, which raised ``UnsupportedProtocol``. That IS an
        ``httpx.HTTPError``, so it classified ``FailedNetwork`` and the row
        burned its entire retry budget on a request that could never be sent.
        """
        if is_absolute_url(url):
            return url
        if envelope.default_target is None:
            return url
        base = str(envelope.default_target).rstrip("/")
        suffix = url if url.startswith("/") else "/" + url
        return base + suffix

    @staticmethod
    def _missing_required_captures(
        step: ChainStep,
        envelope: ChainEnvelope,
        captured: CapturedValues,
    ) -> tuple[str, ...]:
        """Return this step's REQUIRED captures that did not extract (finding R7-5-B).

        A capture this step DECLARES is "required" iff some LATER step
        references it via a ``{{step.capture}}`` placeholder (in its URL,
        headers, or body). Those are exactly the captures whose absence would
        wedge a downstream step on an unresolvable template. A required capture
        that extracted to ``None`` (no JSONPath match - a truncated/incomplete
        2xx body) is "missing".

        Captures this step declares but no later step references are NOT
        required: their absence cannot wedge anything, so a ``None`` there is
        tolerated (the step still succeeds). This keeps the validation precise
        - it fires exactly when an incomplete body would otherwise strand the
        chain, and never on a legitimately-absent optional field.

        Args:
            step: The step that just returned 2xx.
            envelope: The full chain (to scan LATER steps for references).
            captured: The merged captures after ``_extract_captures``.

        Returns:
            The names of this step's required-but-missing captures, in
            declaration order (empty when every required capture extracted).
        """
        declared = {c.name for c in step.capture}
        if not declared:
            return ()
        # Names of THIS step's captures referenced by any LATER step.
        step_index = next(
            (i for i, s in enumerate(envelope.steps) if s.name == step.name),
            None,
        )
        if step_index is None:  # pragma: no cover - step came from this envelope
            return ()
        referenced: set[str] = set()
        for later in envelope.steps[step_index + 1 :]:
            templates: list[str] = [later.url, *later.headers.values()]
            body_template = ChainExecutor._body_as_template(later)
            if body_template:
                templates.append(body_template)
            for template in templates:
                for producing_step, capture_name in find_placeholders(template):
                    if producing_step == step.name and capture_name in declared:
                        referenced.add(capture_name)
        if not referenced:
            return ()
        step_values = captured.steps.get(step.name)
        produced = step_values.values if step_values is not None else {}
        # Declaration order, restricted to required-and-missing.
        return tuple(
            c.name for c in step.capture if c.name in referenced and produced.get(c.name) is None
        )

    async def _extract_captures(
        self,
        step: ChainStep,
        body: bytes,
        existing: CapturedValues,
    ) -> CapturedValues | CaptureNotRenderable:
        """Run JSONPath captures over ``body`` and merge into ``existing``.

        Emits a structured DEBUG log record carrying ``captures`` and
        ``sensitive_captures`` extras so
        :class:`phantom.observability.SensitiveCaptureRedactor` can redact
        values whose declaring :class:`ChainCapture` is ``sensitive=True``
        before the formatter sees them.

        **Refuses an unencodable value here, before it is persisted.** A lone
        UTF-16 surrogate survives every layer between an upstream response and
        a later step's URL: ``json.loads`` accepts it, ``_first_unrenderable``
        used to pass it (it is a ``str`` and ``_CONTROL_CHARS`` matches only CR,
        LF and NUL), ``json.dumps`` and SQLite round-trip it, and then
        ``httpx.Request`` raises ``UnicodeEncodeError`` - a ``ValueError``
        outside the httpx exception hierarchy that nothing on the path catches,
        so the process dies, recovery resets the row to ``queued`` and the next
        claim dies on it again, stranding the whole backlog. Rejecting at
        CAPTURE time rather than only at render time is the choice that keeps
        the value out of the database entirely, so no restart can re-arm it.
        It is applied to every captured string, not only to ones a later step
        references: a value that cannot be encoded at all is upstream data
        Phantom cannot represent, and keeping it because nothing happens to
        reference it today leaves the landmine for the next envelope revision.

        Returns:
            The merged :class:`CapturedValues`, or a
            :class:`CaptureNotRenderable` naming the first capture that cannot
            be encoded. The caller returns the refusal, which the sender routes
            to terminal ``failed``.
        """
        if not step.capture:
            return existing
        try:
            parsed_body = json.loads(body) if body else {}
        except json.JSONDecodeError:
            # Captures only apply to JSON responses; non-JSON yields empty extracts.
            parsed_body = {}
        values: dict[str, Any] = {}
        expires_at: dict[str, datetime | None] = {}
        captured_at = self._clock()
        sensitive_names: set[str] = set()
        for capture in step.capture:
            value = extract(parsed_body, capture.from_path)
            if isinstance(value, str) and not _wire_encodable(value, site="url"):
                # IDENTIFIERS ONLY, exactly as at the render-time gate: the
                # value is upstream response data and ``last_error`` reaches
                # the admin API.
                return CaptureNotRenderable(
                    step_name=step.name,
                    site="body",
                    placeholder=f"{step.name}.{capture.name}",
                    reason="unencodable",
                )
            values[capture.name] = value
            if capture.ttl_seconds is not None:
                expires_at[capture.name] = captured_at + timedelta(seconds=capture.ttl_seconds)
            else:
                expires_at[capture.name] = None
            if capture.sensitive:
                sensitive_names.add(capture.name)
        # Emit a structured DEBUG log record. The captures dict is shared
        # by reference with the formatter; the redaction filter mutates
        # the copy passed in extras (we copy first so the persisted
        # ``CapturedStepValues.values`` keeps the raw value for admin-API
        # consumers). Gated on isEnabledFor so production INFO-level
        # deployments pay zero per-step cost for the structured-extra
        # dict construction.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "chain step captured values",
                extra={
                    "captures": {step.name: dict(values)},
                    "sensitive_captures": {step.name: set(sensitive_names)},
                },
            )
        step_values = CapturedStepValues(
            values=values,
            captured_at=captured_at,
            expires_at=expires_at,
        )
        merged_steps = dict(existing.steps)
        merged_steps[step.name] = step_values
        return CapturedValues(steps=merged_steps)


def _placeholder_names(template: str) -> tuple[str, ...]:
    """Return the ``step.capture`` names a template references, in order.

    Identifiers only. Used to describe a template failure without persisting
    the template, which can carry a presigned URL since F4.

    Args:
        template: The raw template text that failed to resolve.

    Returns:
        The ``producing_step.capture_name`` pairs, in template order.
    """
    return tuple(f"{producing}.{capture}" for producing, capture in find_placeholders(template))


def default_clock() -> datetime:
    """Default UTC clock used in production wiring."""
    return datetime.now(tz=UTC)
