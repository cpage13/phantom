"""Chain executor — the load-bearing primitive.

One call to :meth:`ChainExecutor.execute_one_step` runs **one step**
of the chain identified by ``row.current_step_index``. The executor
owns:

1. The capture-TTL gate (ADR-011) — checked first.
2. ``{{step.var}}`` placeholder substitution in URL, headers, and JSON body.
3. Auth injection — looks up ``token_cache.get(endpoint, uid)`` when the
   route is ``phantom_bearer``.
4. Idempotency-header injection per the step's ``idempotency_header``.
5. The HTTP send through :class:`UpstreamClient`.
6. Capture extraction from the response (JSONPath, first-match).
7. Result classification (Succeeded / 4xx / 5xx / FailedAuth /
   FailedNetwork / TemplateUnresolved / CaptureExpiredStored /
   RouteUnresolved).
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, assert_never
from urllib.parse import urlparse

import httpx

from phantom.chain.jsonpath import extract, find_placeholders, substitute
from phantom.chain.parser import envelope_from_persistence_json
from phantom.chain.sigv4_signer import SigV4SigningError, sign_sigv4
from phantom.config.settings import InstanceCfg
from phantom.models.chain import (
    ChainBodyBytes,
    ChainBodyJson,
    ChainBodyRef,
    ChainBodyText,
    ChainEnvelope,
    ChainStep,
)
from phantom.models.credential import CredCacheRow, HostCredKey
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routing import ResolvedRoute
from phantom.storage.interface import CredentialStore, TokenCache
from phantom.transport.interface import UpstreamClient, UpstreamRequest

logger = logging.getLogger(__name__)

# Phantom's reserved header namespace. Producers use ``X-Phantom-*``
# headers at the producer→Phantom ingress boundary (X-Phantom-Uid,
# X-Phantom-Idempotency-Key, X-Phantom-Instance, X-Phantom-Group-Id,
# X-Phantom-Multifile-Id, X-Phantom-Order). These headers are
# Phantom's internal control channel and must NEVER be forwarded to
# upstream — that would violate the transparent-proxy invariant (producers
# that put X-Phantom-* into a chain step's headers would otherwise see
# them leak through).
_PHANTOM_RESERVED_HEADER_PREFIX = "x-phantom-"

# Placeholder written into ``RouteUnresolved.host`` when the step URL carries
# no parseable host. NEVER substitute the raw URL here: the URL is
# post-substitution producer data that can carry a query string with presigned
# credentials, and ``RouteUnresolved.host`` is persisted verbatim into
# ``last_error``, which the admin API surfaces.
_NO_HOST_TOKEN = "<no-host>"


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
    """Phantom-injected auth was rejected (401/403)."""

    status: int
    observed_at: datetime


@dataclass(frozen=True)
class Failed4xx:
    """Non-auth 4xx — terminal."""

    status: int
    body: bytes


@dataclass(frozen=True)
class Failed5xx:
    """5xx — retry-eligible."""

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
    """ADR-011 reexecute=True — caller rewinds to ``rewind_to_step_index`` and re-queues."""

    producing_step: str
    rewind_to_step_index: int


@dataclass(frozen=True)
class TemplateUnresolved:
    """A placeholder did not resolve at execution time."""

    placeholder: str


@dataclass(frozen=True)
class CaptureIncomplete:
    """A 2xx response was MISSING a capture a later step requires (finding R7-5-B).

    The step returned a success status, but a capture this step DECLARES — and
    that a downstream step REFERENCES via ``{{step.capture}}`` — extracted to
    ``None`` (the response body was truncated / incomplete / a buggy proxy
    returned 2xx before the body was whole — the D-05/D-13/PAR "don't-trust"
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


ExecuteStepResult = (
    Succeeded
    | FailedAuth
    | Failed4xx
    | Failed5xx
    | FailedNetwork
    | CaptureExpiredStored
    | CaptureExpiredRewind
    | TemplateUnresolved
    | CaptureIncomplete
    | SendDeadlineExpired
    | RouteUnresolved
)


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
            clock: A callable returning a UTC ``datetime`` — injectable
                for deterministic tests.
            instance: The instance whose routes apply.
            signer_creds: OPTIONAL host-keyed destination-credential store for
                the ``aws_sigv4`` auth mode (the SigV4 analogue of
                ``token_cache``). ``None`` (the default) when no route uses
                ``aws_sigv4``; an ``aws_sigv4`` route resolved while this is
                ``None`` is treated as a missing credential — ``FailedAuth``
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
        """Run one step of ``row``'s chain and classify the result."""
        envelope = envelope_from_persistence_json(row.chain_envelope_json)
        step_index = row.current_step_index
        if step_index >= len(envelope.steps):
            raise ValueError(
                f"current_step_index {step_index} past end of chain (steps={len(envelope.steps)})",
            )
        step = envelope.steps[step_index]

        # (a) Capture-TTL gate (ADR-011).
        ttl_check = self._check_capture_ttl(step, row, envelope)
        if ttl_check is not None:
            return ttl_check

        # (b) Substitute placeholders in URL, headers, body.
        substituted_url, ok = substitute(step.url, self._captures_as_dict(row.captured_values))
        if not ok:
            return TemplateUnresolved(placeholder=step.url)
        substituted_headers: dict[str, str] = {}
        for name, value in step.headers.items():
            # Phantom's reserved header namespace (``X-Phantom-*``)
            # must not be forwarded to upstream — transparent-proxy
            # invariant. Strip case-insensitively.
            if name.lower().startswith(_PHANTOM_RESERVED_HEADER_PREFIX):
                logger.debug("stripping reserved phantom header from upstream: %s", name)
                continue
            rendered, header_ok = substitute(value, self._captures_as_dict(row.captured_values))
            if not header_ok:
                return TemplateUnresolved(placeholder=f"header[{name}]={value}")
            substituted_headers[name] = rendered

        body_bytes, body_content_type, sub_ok = self._render_body(
            step, row.captured_values, body_refs
        )
        if not sub_ok:
            return TemplateUnresolved(placeholder=f"body of step {step.name!r}")
        if body_content_type is not None and "Content-Type" not in substituted_headers:
            substituted_headers["Content-Type"] = body_content_type

        full_url = self._absolute_url(substituted_url, envelope)

        # (c) Inject auth via route policy. The dispatch is exhaustive over
        # ResolvedRoute.auth_mode (if/elif/else + assert_never) so adding a new
        # auth mode without an arm is a mypy error, not a silent no-auth
        # fall-through (the prior bare ``if`` would have behaved like ``none``).
        try:
            resolved = self._resolve_route(full_url, self._instance)
        except ValueError:
            # Sanitised host for the persisted token. NOT ``_hostname``: that
            # helper returns the WHOLE INPUT when urlparse finds no host
            # (``chain/executor.py`` ``_hostname``), and a step URL legitimately
            # can be a bare path, so reusing it would splice the path and its
            # query string into ``last_error``.
            parsed_host = urlparse(full_url).hostname
            unrouted_host = parsed_host.lower() if parsed_host else _NO_HOST_TOKEN
            logger.warning(
                "no route matches host %s for step %r of chain_id=%s; "
                "parking the row in 'stored' for operator repair",
                unrouted_host,
                step.name,
                row.chain_id,
            )
            return RouteUnresolved(host=unrouted_host, step_name=step.name)

        # (a') Send-deadline gate (ADR-032) — the give-up backstop, independent
        # of the retry strategy. Placed here (after ``resolved`` exists, before
        # any signing/sending work) because the capture-TTL gate (a) at the top
        # of this method runs BEFORE ``resolved`` is computed and the deadline is
        # a per-route property. Checked once per attempt against the cheapest
        # correct point that has ``resolved``.
        deadline_check = self._check_send_deadline(row, resolved)
        if deadline_check is not None:
            return deadline_check

        if resolved.auth_mode == "phantom_bearer":
            slot = await self._cache.get(_hostname(full_url), row.uid)
            if slot is None or slot.status == "bad":
                return FailedAuth(status=401, observed_at=self._clock())
            substituted_headers["Authorization"] = slot.bearer
        elif resolved.auth_mode == "aws_sigv4":
            # SigV4 signer arm (COPY of the bearer arm above): the host-keyed
            # CredentialStore slot is the refreshable slot, the analogue of the
            # (endpoint, uid) token slot. A missing/bad credential — including
            # ``signer_creds is None`` (no store wired) and a ProfileRefCred
            # whose botocore chain yields nothing — marks the slot bad (when a
            # store exists) and returns FailedAuth, which PARKS the row in
            # ``auth_expired`` (NOT terminal) to await a credential re-push.
            dest_host = HostCredKey(_hostname(full_url))
            row_cred = await self._signer_creds_for(dest_host)
            if row_cred is None or row_cred.status == "bad":
                await self._mark_signer_creds_bad(dest_host)
                return FailedAuth(status=401, observed_at=self._clock())
            try:
                # Re-sign THIS request now (fresh X-Amz-Date) over the rehydrated
                # body. botocore mutates ``substituted_headers`` in place; the
                # body stays byte-identical (transparent-proxy invariant).
                await sign_sigv4(
                    method=step.method,
                    url=full_url,
                    headers=substituted_headers,
                    body=body_bytes,
                    credential=row_cred.credential,
                )
            except SigV4SigningError:
                logger.warning(
                    "aws_sigv4 credential resolution failed for host %s; "
                    "marking slot bad and parking (auth_expired)",
                    dest_host,
                )
                await self._mark_signer_creds_bad(dest_host)
                return FailedAuth(status=401, observed_at=self._clock())
        elif resolved.auth_mode == "none":
            pass  # forward as-is — no Phantom-injected auth.
        else:  # pragma: no cover — exhaustive over the Literal above.
            assert_never(resolved.auth_mode)

        # (d) Inject idempotency header.
        if step.idempotency_header:
            substituted_headers[step.idempotency_header] = envelope.idempotency_key

        # (e) Send via the transport. The resolved route may carry a
        # per-route timeout (§5.2) — pass it through; None falls back to
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
            new_captures = await self._extract_captures(step, response.body, row.captured_values)
            # finding R7-5-B: do NOT advance on the 2xx status ALONE. A 2xx
            # with a truncated/incomplete body (D-05/D-13, a CGNAT half-close,
            # a buggy proxy that returns 2xx before the body is whole) leaves
            # this step's declared captures unextracted. If a later step
            # REFERENCES one of those captures (``{{step.capture}}``), advancing
            # would strand it on an unresolvable ``None`` template FOREVER (the
            # row wedges in ``attempting`` holding a saturation slot — invariant
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
            if resolved.auth_mode == "phantom_bearer":
                # Mark the slot bad so the sender knows what to do.
                await self._cache.mark_bad(_hostname(full_url), row.uid)
            elif resolved.auth_mode == "aws_sigv4":
                # Symmetric to the bearer mark-bad: flip the host-keyed cred
                # slot to ``bad`` so the row stays parked until a fresh
                # credential re-push freshens it (the kicker wakes on ``fresh``).
                await self._mark_signer_creds_bad(HostCredKey(_hostname(full_url)))
            return FailedAuth(status=response.status, observed_at=self._clock())
        if 400 <= response.status < 500:
            return Failed4xx(status=response.status, body=response.body)
        if response.status >= 500:
            return Failed5xx(status=response.status)
        return FailedNetwork(error=f"Unexpected status {response.status}")

    async def _signer_creds_for(self, dest_host: HostCredKey) -> CredCacheRow | None:
        """Return the host-keyed credential row, or ``None`` when unusable.

        The SigV4 analogue of the bearer ``token_cache.get`` lookup at the inject
        site. ``None`` covers both "no credential store wired"
        (``signer_creds is None`` — no route needs ``aws_sigv4`` in this
        deployment) and "no slot for this host yet"; the caller treats both
        identically (missing credential → park).
        """
        if self._signer_creds is None:
            return None
        return await self._signer_creds.get(dest_host)

    async def _mark_signer_creds_bad(self, dest_host: HostCredKey) -> None:
        """Flip the host's credential slot to ``bad`` (no-op without a store).

        ADR-003: a bad credential stays in the store so the admin API can
        surface it; the kicker re-wakes the parked row only once a re-push
        freshens the slot. A no-op when ``signer_creds is None`` (there is no
        slot to mark) — the row still parks via the ``FailedAuth`` return.
        """
        if self._signer_creds is None:
            return
        await self._signer_creds.mark_bad(dest_host)

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
        """
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
                # Not yet captured — substitute will detect; not a TTL miss.
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
                    # Producing step missing from envelope — fall back to stored.
                    return CaptureExpiredStored(producing_step=producing_step)
                return CaptureExpiredRewind(
                    producing_step=producing_step,
                    rewind_to_step_index=rewind_index,
                )
            return CaptureExpiredStored(producing_step=producing_step)
        return None

    @staticmethod
    def _captures_as_dict(
        captured: CapturedValues,
    ) -> dict[str, dict[str, Any]]:
        """Flatten :class:`CapturedValues` for the substitute helper."""
        return {name: dict(step.values) for name, step in captured.steps.items()}

    @staticmethod
    def _body_as_template(step: ChainStep) -> str | None:
        """Return the body content as a string for placeholder detection."""
        if isinstance(step.body, ChainBodyText):
            return step.body.value
        if isinstance(step.body, ChainBodyJson):
            return json.dumps(step.body.value)
        return None

    def _render_body(
        self,
        step: ChainStep,
        captured: CapturedValues,
        body_refs: dict[str, bytes],
    ) -> tuple[bytes, str | None, bool]:
        """Produce the outbound body bytes + Content-Type for ``step``.

        Returns:
            ``(bytes, content_type_or_None, all_resolved)``.
        """
        if step.body is None:
            return b"", None, True
        if isinstance(step.body, ChainBodyJson):
            value_template = json.dumps(step.body.value)
            rendered, ok = substitute(value_template, self._captures_as_dict(captured))
            if not ok:
                return b"", None, False
            return rendered.encode("utf-8"), "application/json", True
        if isinstance(step.body, ChainBodyText):
            rendered, ok = substitute(step.body.value, self._captures_as_dict(captured))
            if not ok:
                return b"", None, False
            return rendered.encode("utf-8"), step.body.content_type, True
        if isinstance(step.body, ChainBodyBytes):
            return (
                base64.b64decode(step.body.value_b64),
                step.body.content_type,
                True,
            )
        if isinstance(step.body, ChainBodyRef):
            data = body_refs.get(step.body.name)
            if data is None:
                return b"", None, False
            return data, step.body.content_type, True
        return b"", None, True  # pragma: no cover — exhaustive above

    def _absolute_url(self, url: str, envelope: ChainEnvelope) -> str:
        """Resolve ``url`` against ``envelope.default_target`` if needed."""
        if "://" in url:
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
        that extracted to ``None`` (no JSONPath match — a truncated/incomplete
        2xx body) is "missing".

        Captures this step declares but no later step references are NOT
        required: their absence cannot wedge anything, so a ``None`` there is
        tolerated (the step still succeeds). This keeps the validation precise
        — it fires exactly when an incomplete body would otherwise strand the
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
        if step_index is None:  # pragma: no cover — step came from this envelope
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
    ) -> CapturedValues:
        """Run JSONPath captures over ``body`` and merge into ``existing``.

        Emits a structured DEBUG log record carrying ``captures`` and
        ``sensitive_captures`` extras so
        :class:`phantom.observability.SensitiveCaptureRedactor` can redact
        values whose declaring :class:`ChainCapture` is ``sensitive=True``
        before the formatter sees them.
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


def _hostname(url: str) -> str:
    """Hostname helper used by the executor for cache lookups."""
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()


def default_clock() -> datetime:
    """Default UTC clock used in production wiring."""
    return datetime.now(tz=UTC)
